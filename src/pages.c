/*-------------------------------------------------------------------------
 *
 * pages.c
 *	  The pages store (ch_table_engines.md §2.9): a committed part's files in
 *	  the relation's main fork, through the buffer manager, WAL-logged as new
 *	  pages.  ClickHouse's writer writes a part as in the files store; its
 *	  files are then copied into new pages, a file in consecutive pages, its
 *	  bytes after each page's header, and their manifest kept in the part's
 *	  catalog row.  The engine library reads them back through read_page, a
 *	  page at a time, from shared buffers.  Replicas, base backups, PITR and
 *	  checksums are PostgreSQL's own.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/relation.h"
#include "access/xloginsert.h"
#include "catalog/pg_control.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/fd.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "chte.h"

#define CH_PAGE_PAYLOAD (BLCKSZ - SizeOfPageHeaderData)
#define CH_PAGE_BATCH 32		/* a log_newpages() record's pages at most */

/* A PostgreSQL error inside read_page, raised again once the library returns */
static ErrorData *pending_error = NULL;

void
ch_pages_raise_pending(void)
{
	if (pending_error)
	{
		ErrorData  *e = pending_error;

		pending_error = NULL;
		ReThrowError(e);
	}
}

/*
 * read_page: the payload of one page of the table's main fork.  Errors are
 * caught here -- no longjmp may cross the library's frames -- and kept for
 * ch_engine_raise().
 */
static int
read_page(void *ctx, uint32_t block, void *dst)
{
	ChTable    *t = ctx;
	MemoryContext cxt = CurrentMemoryContext;
	volatile int rc = 0;

	PG_TRY();
	{
		Relation	rel = relation_open(t->relid, NoLock);
		Buffer		buf;

		CHECK_FOR_INTERRUPTS();
		buf = ReadBufferExtended(rel, MAIN_FORKNUM, block, RBM_NORMAL, NULL);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		memcpy(dst, (char *) BufferGetPage(buf) + SizeOfPageHeaderData, CH_PAGE_PAYLOAD);
		UnlockReleaseBuffer(buf);
		relation_close(rel, NoLock);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(TopMemoryContext);
		if (pending_error == NULL)
			pending_error = CopyErrorData();
		MemoryContextSwitchTo(cxt);
		FlushErrorState();
		rc = 1;
	}
	PG_END_TRY();
	return rc;
}

void
ch_pages_ops(ChTable *t, chte_pages_ops *ops)
{
	ops->ctx = t;
	ops->page_payload = CH_PAGE_PAYLOAD;
	ops->read_page = read_page;
}

/* ---- manifests: n, then each file's path length, path, size, first block, blocks ---- */

typedef struct ManifestBuf
{
	StringInfoData data;
	uint32		nfiles;
} ManifestBuf;

static void
manifest_add(ManifestBuf *m, const char *path, uint64 size, BlockNumber first, uint32 nblocks)
{
	uint16		len = strlen(path);

	appendBinaryStringInfo(&m->data, (char *) &len, sizeof(len));
	appendBinaryStringInfo(&m->data, path, len);
	appendBinaryStringInfo(&m->data, (char *) &size, sizeof(size));
	appendBinaryStringInfo(&m->data, (char *) &first, sizeof(first));
	appendBinaryStringInfo(&m->data, (char *) &nblocks, sizeof(nblocks));
	m->nfiles++;
}

static bytea *
manifest_finish(ManifestBuf *m)
{
	bytea	   *b = palloc(VARHDRSZ + sizeof(uint32) + m->data.len);

	SET_VARSIZE(b, VARHDRSZ + sizeof(uint32) + m->data.len);
	memcpy(VARDATA(b), &m->nfiles, sizeof(uint32));
	memcpy(VARDATA(b) + sizeof(uint32), m->data.data, m->data.len);
	return b;
}

/* The files a manifest names, for chte_pages_attach() */
static chte_page_file *
manifest_files(bytea *manifest, size_t *nfiles)
{
	const char *p = VARDATA_ANY(manifest);
	uint32		n;
	chte_page_file *files;

	memcpy(&n, p, sizeof(n));
	p += sizeof(n);
	files = palloc(sizeof(chte_page_file) * Max(n, 1));
	for (uint32 i = 0; i < n; i++)
	{
		uint16		len;

		memcpy(&len, p, sizeof(len));
		p += sizeof(len);
		files[i].path = pnstrdup(p, len);
		p += len;
		memcpy(&files[i].size, p, sizeof(uint64));
		p += sizeof(uint64);
		memcpy(&files[i].first_block, p, sizeof(uint32));
		p += sizeof(uint32);
		memcpy(&files[i].nblocks, p, sizeof(uint32));
		p += sizeof(uint32);
	}
	*nfiles = n;
	return files;
}

/* A part's files for the library, before it reads or merges the part */
void
ch_pages_attach(ChTable *t, const ChPart *part)
{
	size_t		n;
	chte_page_file *files;

	if (part->manifest == NULL)
		elog(ERROR, "part %s of a table of the pages store has no manifest", part->name);
	files = manifest_files(part->manifest, &n);
	CH_CALL("attaching a part", ch_engine()->pages_attach(t->handle, part->name, files, n, CH_ERR));
	pfree(files);
}

/*
 * Copies one file into new pages of the relation; returns its first block.
 * The caller holds the relation's extension lock, so the pages are consecutive.
 */
static BlockNumber
copy_file(Relation rel, const char *path, uint64 size, uint32 *nblocks_out)
{
	uint32		nblocks = (uint32) ((size + CH_PAGE_PAYLOAD - 1) / CH_PAGE_PAYLOAD);
	BlockNumber first = InvalidBlockNumber;
	uint32		done = 0;
	int			fd;
	char	   *data = palloc(CH_PAGE_PAYLOAD * CH_PAGE_BATCH);
	bool		wal = RelationNeedsWAL(rel);

	*nblocks_out = nblocks;
	if (nblocks == 0)
		return 0;
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));
	while (done < nblocks)
	{
		Buffer		buffers[CH_PAGE_BATCH];
		Page		pages[CH_PAGE_BATCH];
		Size		used[CH_PAGE_BATCH];
		uint32		want = Min(nblocks - done, CH_PAGE_BATCH);
		uint32		got;
		BlockNumber start;

		/* the batch's bytes first: nothing in the critical section may fail */
		for (uint32 i = 0; i < want; i++)
		{
			uint64		offset = (uint64) (done + i) * CH_PAGE_PAYLOAD;
			ssize_t		n = pg_pread(fd, data + (Size) i * CH_PAGE_PAYLOAD, CH_PAGE_PAYLOAD, offset);

			if (n < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read file \"%s\": %m", path)));
			used[i] = n;
		}
		start = ExtendBufferedRelBy(BMR_REL(rel), MAIN_FORKNUM, NULL, EB_SKIP_EXTENSION_LOCK, want, buffers, &got);
		if (done == 0)
			first = start;
		Assert(start == first + done);
		for (uint32 i = 0; i < got; i++)
		{
			LockBuffer(buffers[i], BUFFER_LOCK_EXCLUSIVE);
			pages[i] = BufferGetPage(buffers[i]);
		}
		/*
		 * New pages in shared buffers, logged as full-page images in one record,
		 * as log_newpage_range() logs them (xloginsert.c); the record's space is
		 * reserved before the critical section.
		 */
		if (wal)
			XLogEnsureRecordSpace(got - 1, 0);
		START_CRIT_SECTION();
		for (uint32 i = 0; i < got; i++)
		{
			PageInit(pages[i], BLCKSZ, 0);
			memcpy((char *) pages[i] + SizeOfPageHeaderData, data + (Size) i * CH_PAGE_PAYLOAD, used[i]);
			((PageHeader) pages[i])->pd_lower = SizeOfPageHeaderData + used[i];
			MarkBufferDirty(buffers[i]);
		}
		if (wal)
		{
			XLogRecPtr	recptr;

			XLogBeginInsert();
			for (uint32 i = 0; i < got; i++)
				XLogRegisterBuffer(i, buffers[i], REGBUF_FORCE_IMAGE | REGBUF_STANDARD);
			recptr = XLogInsert(RM_XLOG_ID, XLOG_FPI);
			for (uint32 i = 0; i < got; i++)
				PageSetLSN(pages[i], recptr);
		}
		END_CRIT_SECTION();
		for (uint32 i = 0; i < got; i++)
			UnlockReleaseBuffer(buffers[i]);
		done += got;
		CHECK_FOR_INTERRUPTS();
	}
	CloseTransientFile(fd);
	pfree(data);
	return first;
}

static void
copy_tree(Relation rel, const char *dir, const char *prefix, ManifestBuf *m)
{
	DIR		   *d = AllocateDir(dir);
	struct dirent *de;

	while ((de = ReadDir(d, dir)) != NULL)
	{
		char	   *path;
		char	   *rel_name;
		struct stat st;

		if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
			continue;
		path = psprintf("%s/%s", dir, de->d_name);
		rel_name = prefix[0] ? psprintf("%s/%s", prefix, de->d_name) : pstrdup(de->d_name);
		if (lstat(path, &st) < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not stat \"%s\": %m", path)));
		if (S_ISDIR(st.st_mode))
			copy_tree(rel, path, rel_name, m);
		else
		{
			uint32		nblocks;
			BlockNumber first = copy_file(rel, path, st.st_size, &nblocks);

			manifest_add(m, rel_name, st.st_size, first, nblocks);
		}
		pfree(rel_name);
		pfree(path);
	}
	FreeDir(d);
}

/*
 * A part ClickHouse wrote in part_dir, a directory of the table's: its files
 * into pages of the relation, under the table's extension lock so that each
 * file's pages are consecutive.  Returns the manifest for the part's row.
 */
bytea *
ch_pages_store_part(Relation rel, ChTable *t, const char *part_dir)
{
	ManifestBuf m;
	char	   *dir = psprintf("%s/%s%s", DataDir, t->relative_path, part_dir);

	initStringInfo(&m.data);
	m.nfiles = 0;
	LockRelationForExtension(rel, ExclusiveLock);
	PG_TRY();
	{
		copy_tree(rel, dir, "", &m);
	}
	PG_FINALLY();
	{
		UnlockRelationForExtension(rel, ExclusiveLock);
	}
	PG_END_TRY();
	pfree(dir);
	return manifest_finish(&m);
}
