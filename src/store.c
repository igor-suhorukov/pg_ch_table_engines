/*-------------------------------------------------------------------------
 *
 * store.c
 *	  The files store (ch_table_engines.md §2.9): a storage's directory of
 *	  part directories, ClickHouse's own files, under the database's
 *	  directory of the relation's tablespace; their lifecycle on vanilla
 *	  PostgreSQL; and their WAL, through the module's resource manager.
 *
 *	  WAL.  A part's files are synced by ClickHouse's writer before the part
 *	  is named (fsync_after_insert, fsync_part_directory); where the relation
 *	  needs WAL, their bytes are logged as FILE records before the part's
 *	  catalog row is inserted, and a removal as a REMOVE record before the
 *	  directory goes.  Replay writes each file and syncs it with its last
 *	  record, on a standby, in crash recovery and in PITR alike; the catalog
 *	  row, replayed after, makes the part visible.
 *
 *	  Lifecycle.  A new storage's directory and a new part's go at the
 *	  (sub)transaction's abort; a storage left by TRUNCATE, a rewrite or
 *	  DROP goes at commit, its catalog rows deleted in the transaction.
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

#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "catalog/pg_tablespace.h"
#include "common/file_perm.h"
#include "common/relpath.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"
#include "storage/procarray.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "chte.h"

#define CH_RMGR_ID RM_EXPERIMENTAL_ID
#define CH_RMGR_NAME "ch_table_engines"

#define XLOG_CH_FILE	0x00
#define XLOG_CH_REMOVE	0x10

#define CH_FILE_FIRST	0x01	/* create or truncate the file */
#define CH_FILE_LAST	0x02	/* sync the file and its directory */

/* the bytes of a file a FILE record carries at most */
#define CH_WAL_CHUNK	(256 * 1024)

typedef struct xl_ch_file
{
	RelFileLocator locator;
	uint64		offset;
	uint32		len;
	uint16		part_len;
	uint16		file_len;
	uint8		flags;
	/* part name, file name (relative to the part), then the bytes */
} xl_ch_file;

typedef struct xl_ch_remove
{
	RelFileLocator locator;
	uint16		part_len;		/* 0: the storage's whole directory */
	/* part name */
} xl_ch_remove;

typedef struct PendingRemoval
{
	ChStorageId storage;
	char		part[128];		/* "": the storage's directory */
	bool		at_commit;		/* else at abort */
	int			nest_level;
} PendingRemoval;

static List *pending = NIL;		/* in TopMemoryContext */

static bool
storage_needs_wal(const ChStorageId *id)
{
	return id->persistence == RELPERSISTENCE_PERMANENT && XLogIsNeeded();
}

static char *
part_path(const ChStorageId *id, const char *part)
{
	char	   *dir = ch_storage_path(id, false);

	if (part == NULL || part[0] == '\0')
		return dir;
	return psprintf("%s%s", dir, part);
}

static void
remember(const ChStorageId *id, const char *part, bool at_commit)
{
	MemoryContext old = MemoryContextSwitchTo(TopMemoryContext);
	PendingRemoval *p = palloc0(sizeof(PendingRemoval));

	p->storage = *id;
	if (part)
		strlcpy(p->part, part, sizeof(p->part));
	p->at_commit = at_commit;
	p->nest_level = GetCurrentTransactionNestLevel();
	pending = lappend(pending, p);
	MemoryContextSwitchTo(old);
}

void
ch_store_create(const ChStorageId *id)
{
	char	   *dir = ch_storage_path(id, false);
	struct stat st;

	/*
	 * A directory already there is a storage's no ch.tables row names any
	 * more: one a prepared transaction's end left, its relfilenumber taken
	 * again since -- a dropped storage goes at commit, and COMMIT PREPARED,
	 * in another session or on the port's segment (CH6), has no list of it
	 * -- or a crash's.  It goes before the new storage's parts can meet its
	 * part directories of the same names.
	 */
	if (stat(dir, &st) == 0)
		ch_store_remove_now(id, NULL, true);
	pfree(dir);
	/* the engine library makes the directory as it first opens the table */
	remember(id, NULL, false);
}

void
ch_store_drop_at_commit(const ChStorageId *id)
{
	remember(id, NULL, true);
}

void
ch_store_drop_at_abort(const ChStorageId *id)
{
	remember(id, NULL, false);
}

static void
log_remove(const ChStorageId *id, const char *part)
{
	xl_ch_remove rec;

	if (!storage_needs_wal(id))
		return;
	rec.locator = id->locator;
	rec.part_len = part ? strlen(part) : 0;
	XLogBeginInsert();
	XLogRegisterData((char *) &rec, sizeof(rec));
	if (rec.part_len)
		XLogRegisterData(part, rec.part_len);
	/* written after the commit record, it is flushed itself: a standby gets it now */
	XLogFlush(XLogInsert(CH_RMGR_ID, XLOG_CH_REMOVE));
}

void
ch_store_remove_now(const ChStorageId *id, const char *part, bool log)
{
	char	   *path = part_path(id, part);
	struct stat st;

	if (log)
		log_remove(id, part);
	if (stat(path, &st) == 0)
	{
		if (!rmtree(path, true))
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("could not remove directory \"%s\"", path)));
	}
	pfree(path);
}

/* Logs one file of a part, read back from where it was written: path; file is its name below the part */
static void
log_file(const ChStorageId *id, const char *path, const char *part, const char *file)
{
	int			fd;
	char	   *buf = palloc(CH_WAL_CHUNK);
	uint64		offset = 0;
	ssize_t		n;
	bool		first = true;
	struct stat st;

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));
	if (fstat(fd, &st) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not stat file \"%s\": %m", path)));
	do
	{
		xl_ch_file rec;

		n = pg_pread(fd, buf, CH_WAL_CHUNK, offset);
		if (n < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m", path)));
		rec.locator = id->locator;
		rec.offset = offset;
		rec.len = (uint32) n;
		rec.part_len = strlen(part);
		rec.file_len = strlen(file);
		rec.flags = (first ? CH_FILE_FIRST : 0) | (offset + n >= (uint64) st.st_size ? CH_FILE_LAST : 0);
		XLogBeginInsert();
		XLogRegisterData((char *) &rec, sizeof(rec));
		XLogRegisterData(part, rec.part_len);
		XLogRegisterData(file, rec.file_len);
		if (n > 0)
			XLogRegisterData(buf, n);
		(void) XLogInsert(CH_RMGR_ID, XLOG_CH_FILE);
		offset += n;
		first = false;
		CHECK_FOR_INTERRUPTS();
	} while (offset < (uint64) st.st_size);
	CloseTransientFile(fd);
	pfree(buf);
}

/* Logs the files of a part's directory and of its projections' directories */
static void
log_tree(const ChStorageId *id, const char *dir, const char *part, const char *prefix)
{
	DIR		   *d;
	struct dirent *de;

	d = AllocateDir(dir);
	while ((de = ReadDir(d, dir)) != NULL)
	{
		char	   *path;
		char	   *rel;
		struct stat st;

		if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
			continue;
		path = psprintf("%s/%s", dir, de->d_name);
		rel = prefix[0] ? psprintf("%s/%s", prefix, de->d_name) : pstrdup(de->d_name);
		if (lstat(path, &st) < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not stat \"%s\": %m", path)));
		if (S_ISDIR(st.st_mode))
			log_tree(id, path, part, rel);
		else
			log_file(id, path, part, rel);
		pfree(rel);
		pfree(path);
	}
	FreeDir(d);
}

/*
 * A part was written in part_dir (its temporary name) and is about to be
 * named part_name: its files go to WAL under its name, and its directory goes
 * at the (sub)transaction's abort.
 */
void
ch_store_part_written(Relation rel, const ChStorageId *id, const char *part_dir, const char *part_name)
{
	if (storage_needs_wal(id) && RelationNeedsWAL(rel))
	{
		char	   *dir = part_path(id, part_dir);

		log_tree(id, dir, part_name, "");
		pfree(dir);
	}
	remember(id, part_name, false);
}

/* Copies a file, syncing it */
static void
copy_file_synced(const char *from, const char *to)
{
	int			in;
	int			out;
	char	   *buf = palloc(CH_WAL_CHUNK);
	ssize_t		n;
	off_t		offset = 0;

	in = OpenTransientFile(from, O_RDONLY | PG_BINARY);
	if (in < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", from)));
	out = OpenTransientFile(to, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
	if (out < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", to)));
	while ((n = pg_pread(in, buf, CH_WAL_CHUNK, offset)) > 0)
	{
		if (pg_pwrite(out, buf, n, offset) != n)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write file \"%s\": %m", to)));
		offset += n;
	}
	if (n < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\": %m", from)));
	if (enableFsync && pg_fsync(out) != 0)
		ereport(data_sync_elevel(ERROR),
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m", to)));
	CloseTransientFile(out);
	CloseTransientFile(in);
	pfree(buf);
}

static void
copy_tree(const char *from, const char *to)
{
	DIR		   *d;
	struct dirent *de;

	if (pg_mkdir_p(unconstify(char *, to), pg_dir_create_mode) != 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create directory \"%s\": %m", to)));
	d = AllocateDir(from);
	while ((de = ReadDir(d, from)) != NULL)
	{
		char	   *src;
		char	   *dst;
		struct stat st;

		if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
			continue;
		src = psprintf("%s/%s", from, de->d_name);
		dst = psprintf("%s/%s", to, de->d_name);
		if (lstat(src, &st) < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not stat \"%s\": %m", src)));
		if (S_ISDIR(st.st_mode))
			copy_tree(src, dst);
		else
			copy_file_synced(src, dst);
		pfree(src);
		pfree(dst);
	}
	FreeDir(d);
	if (enableFsync)
		fsync_fname(to, true);
}

/*
 * A part copied to another storage (SET TABLESPACE): its files written and
 * synced, logged under the new storage, removed if the transaction aborts.
 */
void
ch_store_copy_part(Relation rel, const ChStorageId *from, const ChStorageId *to, const char *part)
{
	char	   *src = part_path(from, part);
	char	   *dst = part_path(to, part);

	copy_tree(src, dst);
	if (storage_needs_wal(to) && RelationNeedsWAL(rel))
		log_tree(to, dst, part, "");
	remember(to, part, false);
	pfree(src);
	pfree(dst);
}

static void
do_pending(bool commit, int nest_level)
{
	ListCell   *lc;
	List	   *keep = NIL;
	MemoryContext old;

	foreach(lc, pending)
	{
		PendingRemoval *p = lfirst(lc);

		if (p->nest_level < nest_level)
		{
			keep = lappend(keep, p);
			continue;
		}
		if (p->at_commit == commit)
		{
			PG_TRY();
			{
				ch_store_remove_now(&p->storage, p->part[0] ? p->part : NULL, true);
			}
			PG_CATCH();
			{
				/* a removal that fails leaves an orphan for the sweep */
				FlushErrorState();
			}
			PG_END_TRY();
		}
		pfree(p);
	}
	old = MemoryContextSwitchTo(TopMemoryContext);
	list_free(pending);
	pending = list_copy(keep);
	MemoryContextSwitchTo(old);
	list_free(keep);
}

void
ch_store_xact_callback(XactEvent event)
{
	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
			do_pending(true, 0);
			break;
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			do_pending(false, 0);
			break;
		case XACT_EVENT_PREPARE:
			/*
			 * A prepared transaction may end in another session, which has no
			 * list of it: what COMMIT or ROLLBACK PREPARED leaves is the sweep's
			 * (§2.9).  Nothing is removed now: its parts must outlive PREPARE.
			 */
			list_free_deep(pending);
			pending = NIL;
			break;
		default:
			break;
	}
}

void
ch_store_subxact_callback(SubXactEvent event, SubTransactionId mySubid)
{
	ListCell   *lc;
	int			level = GetCurrentTransactionNestLevel();

	switch (event)
	{
		case SUBXACT_EVENT_COMMIT_SUB:
			/* the child's removals become the parent's */
			foreach(lc, pending)
			{
				PendingRemoval *p = lfirst(lc);

				if (p->nest_level >= level)
					p->nest_level = level - 1;
			}
			break;
		case SUBXACT_EVENT_ABORT_SUB:
			do_pending(false, level);
			break;
		default:
			break;
	}
}

/* ------------------------------------------------------------------------
 * The resource manager
 * ------------------------------------------------------------------------
 */

static char *
redo_path(const RelFileLocator *locator, const char *part, uint16 part_len)
{
	ChStorageId id;

	memset(&id, 0, sizeof(id));
	id.locator = *locator;
	id.backend = -1;
	id.persistence = RELPERSISTENCE_PERMANENT;
	if (part_len == 0)
		return ch_storage_path(&id, false);
	return psprintf("%s%.*s", ch_storage_path(&id, false), (int) part_len, part);
}

static void
redo_file(XLogReaderState *record)
{
	char	   *data = XLogRecGetData(record);
	xl_ch_file *rec = (xl_ch_file *) data;
	const char *part = data + sizeof(xl_ch_file);
	const char *file = part + rec->part_len;
	const char *bytes = file + rec->file_len;
	char	   *dir = redo_path(&rec->locator, part, rec->part_len);
	char	   *path = psprintf("%s/%.*s", dir, (int) rec->file_len, file);
	char	   *slash;
	int			fd;

	/* the file's directory: the part's, or a projection's within it */
	slash = strrchr(path, '/');
	*slash = '\0';
	if (pg_mkdir_p(path, pg_dir_create_mode) != 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create directory \"%s\": %m", path)));
	*slash = '/';

	fd = OpenTransientFile(path, O_WRONLY | O_CREAT | PG_BINARY | ((rec->flags & CH_FILE_FIRST) ? O_TRUNC : 0));
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));
	if (rec->len > 0 && pg_pwrite(fd, bytes, rec->len, rec->offset) != (ssize_t) rec->len)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m", path)));
	if ((rec->flags & CH_FILE_LAST) && enableFsync && pg_fsync(fd) != 0)
		ereport(data_sync_elevel(ERROR),
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m", path)));
	CloseTransientFile(fd);
	if ((rec->flags & CH_FILE_LAST) && enableFsync)
	{
		*slash = '\0';
		fsync_fname(path, true);
	}
	pfree(path);
	pfree(dir);
}

static void
redo_remove(XLogReaderState *record)
{
	char	   *data = XLogRecGetData(record);
	xl_ch_remove *rec = (xl_ch_remove *) data;
	char	   *path = redo_path(&rec->locator, data + sizeof(xl_ch_remove), rec->part_len);
	struct stat st;

	if (stat(path, &st) == 0)
		(void) rmtree(path, true);
	pfree(path);
}

static void
ch_rmgr_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_CH_FILE:
			redo_file(record);
			break;
		case XLOG_CH_REMOVE:
			redo_remove(record);
			break;
		default:
			elog(PANIC, "ch_table_engines redo: unknown op code %u", info);
	}
}

static void
ch_rmgr_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *data = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == XLOG_CH_FILE)
	{
		xl_ch_file *rec = (xl_ch_file *) data;
		const char *part = data + sizeof(xl_ch_file);

		appendStringInfo(buf, "rel %u/%u/%u part %.*s file %.*s offset " UINT64_FORMAT " len %u%s%s",
						 rec->locator.spcOid, rec->locator.dbOid, rec->locator.relNumber,
						 (int) rec->part_len, part, (int) rec->file_len, part + rec->part_len,
						 rec->offset, rec->len,
						 (rec->flags & CH_FILE_FIRST) ? " first" : "",
						 (rec->flags & CH_FILE_LAST) ? " last" : "");
	}
	else if (info == XLOG_CH_REMOVE)
	{
		xl_ch_remove *rec = (xl_ch_remove *) data;

		appendStringInfo(buf, "rel %u/%u/%u part %.*s",
						 rec->locator.spcOid, rec->locator.dbOid, rec->locator.relNumber,
						 (int) rec->part_len, data + sizeof(xl_ch_remove));
	}
}

static const char *
ch_rmgr_identify(uint8 info)
{
	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_CH_FILE:
			return "FILE";
		case XLOG_CH_REMOVE:
			return "REMOVE";
	}
	return NULL;
}

static const RmgrData ch_rmgr = {
	.rm_name = CH_RMGR_NAME,
	.rm_redo = ch_rmgr_redo,
	.rm_desc = ch_rmgr_desc,
	.rm_identify = ch_rmgr_identify,
};

void
ch_store_register_rmgr(void)
{
	RegisterCustomRmgr(CH_RMGR_ID, &ch_rmgr);
}

/*
 * The sweep (§2.9, "Lifecycle"): a storage's directories no catalog row
 * names -- a crashed writer's part, a temporary part, what COMMIT or
 * ROLLBACK PREPARED left, a merged or folded part's predecessor -- removed.
 * The caller holds a lock that no writer of the relation holds with it
 * (ShareLock), so none is in flight; a part whose row is still in ch.parts,
 * dead or not, stays.  Returns how many went.
 */
int
ch_store_sweep(Relation rel, const ChStorageId *id)
{
	char	   *dir = ch_storage_path(id, false);
	DIR		   *d;
	struct dirent *de;
	List	   *names = NIL;
	int			removed = 0;
	ListCell   *lc;
	List	   *kept;
	struct stat st;

	if (stat(dir, &st) != 0)
		return 0;
	/*
	 * Every part row still in ch.parts, whatever its visibility: a directory
	 * goes once VACUUM of ch.parts has pruned its row, which no snapshot then
	 * sees -- a standby's included, the prune's record resolving the recovery
	 * conflicts of those that might, before this removal's record replays.
	 * Their names only: a dead row may outlive its manifest's TOAST chunks.
	 */
	kept = ch_parts_names(id);

	d = AllocateDir(dir);
	while ((de = ReadDir(d, dir)) != NULL)
	{
		bool		named = false;

		if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0 ||
			strcmp(de->d_name, "detached") == 0 || strcmp(de->d_name, "format_version.txt") == 0)
			continue;
		foreach(lc, kept)
			if (strcmp(lfirst(lc), de->d_name) == 0)
				named = true;
		if (!named)
			names = lappend(names, pstrdup(de->d_name));
	}
	FreeDir(d);

	foreach(lc, names)
	{
		ch_store_remove_now(id, lfirst(lc), true);
		removed++;
	}
	pfree(dir);
	return removed;
}

/* The storages' directories in a database's directory of a tablespace no ch.tables row names, removed */
static int
sweep_database_dir(Oid spcOid)
{
	char	   *dbpath = GetDatabasePath(MyDatabaseId, spcOid);
	DIR		   *d = AllocateDir(dbpath);
	struct dirent *de;
	List	   *orphans = NIL;
	ListCell   *lc;
	int			removed = 0;

	if (d == NULL)
	{
		pfree(dbpath);
		return 0;
	}
	while ((de = ReadDirExtended(d, dbpath, DEBUG1)) != NULL)
	{
		ChStorageId id;
		unsigned int relnumber;
		int			backend;
		char		tail;

		memset(&id, 0, sizeof(id));
		if (sscanf(de->d_name, "ch_t%d_%u%c", &backend, &relnumber, &tail) == 2)
		{
			id.backend = backend;
			id.persistence = RELPERSISTENCE_TEMP;
		}
		else if (sscanf(de->d_name, "ch_%u%c", &relnumber, &tail) == 1)
		{
			id.backend = -1;
			id.persistence = RELPERSISTENCE_PERMANENT;
		}
		else
			continue;
		id.locator.spcOid = spcOid;
		id.locator.dbOid = MyDatabaseId;
		id.locator.relNumber = relnumber;
		if (!ch_storage_named(&id))
		{
			ChStorageId *o = palloc(sizeof(ChStorageId));

			*o = id;
			orphans = lappend(orphans, o);
		}
	}
	FreeDir(d);

	/* a permanent storage's directory as its REMOVE record names it; a temporary one's unlogged */
	foreach(lc, orphans)
	{
		ChStorageId *o = lfirst(lc);

		ch_store_remove_now(o, NULL, o->persistence == RELPERSISTENCE_PERMANENT);
		removed++;
	}
	pfree(dbpath);
	return removed;
}

/*
 * The sweep of a database (CH6): the directories of the files store's
 * storages no ch.tables row names any more, in every tablespace -- what a
 * crash left, or the end of a prepared transaction, which on the port's
 * segments is the end of every distributed DDL statement and write.  A
 * storage's row is inserted before its directory is made (am.c), and a
 * dropped storage's row stays, dead, until VACUUM of ch.tables prunes it,
 * whose record resolves a standby's conflicts as ch.parts's does for a part's
 * directory (ch_store_sweep()).  Under ShareLock on the two catalogs, so
 * that no storage is made or dropped meanwhile, taken without waiting -- a
 * writer may hold one and want the other: a merge worker's sweep returns -1
 * where one could not be taken, ch.remove_orphans()'s tries again a moment
 * on.  Returns how many went.
 */
int
ch_store_sweep_database(bool wait)
{
	Oid			tables = ch_catalog_relid("tables", false);
	Oid			tables_unlogged = ch_catalog_relid("tables", true);
	DIR		   *d;
	struct dirent *de;
	int			removed;

	for (;;)
	{
		if (ConditionalLockRelationOid(tables, ShareLock))
		{
			if (ConditionalLockRelationOid(tables_unlogged, ShareLock))
				break;
			UnlockRelationOid(tables, ShareLock);
		}
		if (!wait)
			return -1;
		(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 100L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
	}
	removed = sweep_database_dir(DEFAULTTABLESPACE_OID);
	d = AllocateDir("pg_tblspc");
	while (d != NULL && (de = ReadDirExtended(d, "pg_tblspc", DEBUG1)) != NULL)
	{
		Oid			spc = atooid(de->d_name);

		if (OidIsValid(spc))
			removed += sweep_database_dir(spc);
	}
	if (d != NULL)
		FreeDir(d);
	UnlockRelationOid(tables_unlogged, ShareLock);
	UnlockRelationOid(tables, ShareLock);
	return removed;
}
