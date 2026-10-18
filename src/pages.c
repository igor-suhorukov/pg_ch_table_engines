/*-------------------------------------------------------------------------
 *
 * pages.c
 *	  The pages store (ch_table_engines.md §2.9): a committed part's files in
 *	  the relation's main fork, through the buffer manager, WAL-logged as
 *	  full-page images.  ClickHouse's writer writes a part as in the files
 *	  store; its files are then copied into pages, a file in consecutive
 *	  pages, its bytes after each page's header, and their manifest kept in
 *	  the part's catalog row.  The engine library reads them back through
 *	  read_page, a page at a time, from shared buffers.  Replicas, base
 *	  backups, PITR and checksums are PostgreSQL's own.
 *
 *	  Pages no row of ch.parts names are free, and a part's files take them
 *	  before new ones (CH3, §2.11): those of a part a merge or a fold
 *	  replaced, once VACUUM of ch.parts has pruned its row -- which no
 *	  snapshot then sees, a standby's included: the prune's record resolves
 *	  the recovery conflicts of the snapshots that might (heapam's
 *	  snapshotConflictHorizon), and it comes before the pages are written
 *	  again in WAL.
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
#include "utils/hsearch.h"
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

/*
 * A part's files for the library, before it reads, analyzes or merges the
 * part; once for the table's handle, a committed part's files never moving.
 */
void
ch_pages_attach(ChTable *t, const ChPart *part)
{
	size_t		n;
	chte_page_file *files;
	bytea	   *manifest;
	bool		found;

	if (t->attached == NULL)
	{
		HASHCTL		ctl;

		ctl.keysize = NAMEDATALEN * 2;
		ctl.entrysize = NAMEDATALEN * 2;
		ctl.hcxt = GetMemoryChunkContext(t);
		t->attached = hash_create("ch attached parts", 64, &ctl, HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);
	}
	if (strlen(part->name) >= NAMEDATALEN * 2)
		elog(ERROR, "part name \"%s\" is too long", part->name);
	(void) hash_search(t->attached, part->name, HASH_FIND, &found);
	if (found)
		return;
	if (part->manifest == NULL)
		elog(ERROR, "part %s of a table of the pages store has no manifest", part->name);
	manifest = ch_part_manifest(part);
	files = manifest_files(manifest, &n);
	CH_CALL("attaching a part", ch_engine()->pages_attach(t->handle, part->name, files, n, CH_ERR));
	pfree(files);
	if (manifest != part->manifest)
		pfree(manifest);
	(void) hash_search(t->attached, part->name, HASH_ENTER, NULL);
}

/* The relation's pages no part row names: runs of them, ascending, and its end */
typedef struct FreePages
{
	BlockNumber *first;
	BlockNumber *count;
	int			n;
	BlockNumber end;			/* the relation's pages, those this copy adds counted */
} FreePages;

typedef struct Extent
{
	BlockNumber first;
	BlockNumber count;
} Extent;

static int
extent_order(const void *a, const void *b)
{
	BlockNumber x = ((const Extent *) a)->first;
	BlockNumber y = ((const Extent *) b)->first;

	return x < y ? -1 : x > y ? 1 : 0;
}

/* The free pages: the gaps between the pages the manifests of every part row name */
static void
free_pages(Relation rel, ChTable *t, FreePages *fp)
{
	List	   *manifests = ch_parts_manifests(&t->storage);
	ListCell   *lc;
	Extent	   *extents = NULL;
	int			nextents = 0;
	int			cap = 0;
	BlockNumber cursor = 0;

	foreach(lc, manifests)
	{
		size_t		nfiles;
		chte_page_file *files = manifest_files(lfirst(lc), &nfiles);

		for (size_t i = 0; i < nfiles; i++)
		{
			if (files[i].nblocks == 0)
				continue;
			if (nextents == cap)
			{
				cap = cap ? cap * 2 : 64;
				extents = extents ? repalloc_huge(extents, sizeof(Extent) * cap) :
					palloc_extended(sizeof(Extent) * cap, MCXT_ALLOC_HUGE);
			}
			extents[nextents].first = files[i].first_block;
			extents[nextents].count = files[i].nblocks;
			nextents++;
		}
		pfree(files);
	}
	if (nextents > 0)
		qsort(extents, nextents, sizeof(Extent), extent_order);
	fp->end = RelationGetNumberOfBlocks(rel);
	fp->first = palloc(sizeof(BlockNumber) * (nextents + 1));
	fp->count = palloc(sizeof(BlockNumber) * (nextents + 1));
	fp->n = 0;
	for (int i = 0; i < nextents; i++)
	{
		if (extents[i].first > cursor && cursor < fp->end)
		{
			fp->first[fp->n] = cursor;
			fp->count[fp->n++] = Min(extents[i].first, fp->end) - cursor;
		}
		cursor = Max(cursor, extents[i].first + extents[i].count);
	}
	if (cursor < fp->end)
	{
		fp->first[fp->n] = cursor;
		fp->count[fp->n++] = fp->end - cursor;
	}
	if (extents)
		pfree(extents);
	list_free_deep(manifests);
}

/*
 * n consecutive pages for a file: the first free run that holds them, or the
 * free run at the relation's end extended, or new pages after the end.
 */
static BlockNumber
take_pages(FreePages *fp, BlockNumber n)
{
	BlockNumber first;

	for (int i = 0; i < fp->n; i++)
	{
		if (fp->count[i] == 0)
			continue;
		if (fp->count[i] >= n)
		{
			first = fp->first[i];
			fp->first[i] += n;
			fp->count[i] -= n;
			return first;
		}
		if (fp->first[i] + fp->count[i] == fp->end)
		{
			first = fp->first[i];
			fp->count[i] = 0;
			fp->end = first + n;
			return first;
		}
	}
	first = fp->end;
	fp->end += n;
	return first;
}

/*
 * Copies one file into pages of the relation from block first on: free pages
 * zeroed in shared buffers and written over, then new ones past the end.  The
 * caller holds the store's lock (store_lock()), so no one else extends the
 * relation meanwhile.
 */
static void
copy_file(Relation rel, const char *path, uint64 size, BlockNumber first, uint32 nblocks)
{
	uint32		done = 0;
	int			fd;
	char	   *data;
	bool		wal = RelationNeedsWAL(rel);

	if (nblocks == 0)
		return;
	data = palloc(CH_PAGE_PAYLOAD * CH_PAGE_BATCH);
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
		start = first + done;
		if (start < RelationGetNumberOfBlocks(rel))
		{
			/* free pages: their old contents no one reads, zeroed as they are pinned */
			got = Min(want, RelationGetNumberOfBlocks(rel) - start);
			for (uint32 i = 0; i < got; i++)
				buffers[i] = ReadBufferExtended(rel, MAIN_FORKNUM, start + i, RBM_ZERO_AND_LOCK, NULL);
		}
		else
		{
			BlockNumber at PG_USED_FOR_ASSERTS_ONLY;

			at = ExtendBufferedRelBy(BMR_REL(rel), MAIN_FORKNUM, NULL, 0, want, buffers, &got);
			Assert(at == start);
			for (uint32 i = 0; i < got; i++)
				LockBuffer(buffers[i], BUFFER_LOCK_EXCLUSIVE);
		}
		for (uint32 i = 0; i < got; i++)
			pages[i] = BufferGetPage(buffers[i]);
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
}

static void
copy_tree(Relation rel, const char *dir, const char *prefix, ManifestBuf *m, FreePages *fp)
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
			copy_tree(rel, path, rel_name, m, fp);
		else
		{
			uint32		nblocks = (uint32) ((st.st_size + CH_PAGE_PAYLOAD - 1) / CH_PAGE_PAYLOAD);
			BlockNumber first = nblocks > 0 ? take_pages(fp, nblocks) : 0;

			copy_file(rel, path, st.st_size, first, nblocks);
			manifest_add(m, rel_name, st.st_size, first, nblocks);
		}
		pfree(rel_name);
		pfree(path);
	}
	FreeDir(d);
}

/*
 * The store's lock: one writer of a part at a time, from the free pages it
 * counts to the row naming those it took.  The page lock of a block no TID
 * names (InvalidBlockNumber), not the relation's extension lock, under which
 * no other lock may be taken -- the part's row is (lock.c).
 */
static void
store_lock(Relation rel)
{
	LockPage(rel, InvalidBlockNumber, ExclusiveLock);
}

static void
store_unlock(Relation rel)
{
	UnlockPage(rel, InvalidBlockNumber, ExclusiveLock);
}

/*
 * A part ClickHouse wrote in part_dir, a directory of the table's: its files
 * into pages of the relation, free ones first, and the part's row inserted
 * under cid with their manifest, all under the store's lock -- each file's
 * pages consecutive, and the next writer counting them as used.  Returns the
 * manifest, which *part takes too.
 */
bytea *
ch_pages_store_part(Relation rel, ChTable *t, const char *part_dir, ChPart *part, CommandId cid)
{
	ManifestBuf m;
	FreePages	fp;
	char	   *dir = psprintf("%s/%s%s", DataDir, t->relative_path, part_dir);

	initStringInfo(&m.data);
	m.nfiles = 0;
	store_lock(rel);
	PG_TRY();
	{
		free_pages(rel, t, &fp);
		copy_tree(rel, dir, "", &m, &fp);
		part->manifest = manifest_finish(&m);
		ch_part_insert(&t->storage, part, cid);
	}
	PG_FINALLY();
	{
		store_unlock(rel);
	}
	PG_END_TRY();
	pfree(dir);
	return part->manifest;
}
