/*-------------------------------------------------------------------------
 *
 * worker.c
 *	  The merge workers (ch_table_engines.md §2.11).  A launcher, a static
 *	  background worker registered at preload, starts after recovery -- so
 *	  never on a standby -- and keeps a merge worker for each database that
 *	  allows connections, up to ch.max_merge_workers at a time; a database's
 *	  worker merges its tables while the extension is there, and exits where
 *	  it is not, to be tried again a minute later.
 *
 *	  A worker passes over its database's tables every ch.merge_naptime
 *	  seconds: each table, in a transaction of its own and under
 *	  ShareUpdateExclusiveLock taken without waiting (a fold, a merge, VACUUM
 *	  and ANALYZE hold it too), merged while ClickHouse's selector chooses
 *	  parts to merge (merge.c), on the worker's own thread; then the files
 *	  store's sweep, which VACUUM runs too; an error in one table's merge is
 *	  reported and the next table taken.
 *
 *	  DROP DATABASE and the other commands that need a database to
 *	  themselves wait for its other backends to exit (CountOtherDBBackends(),
 *	  dbcommands.c): ch_merge_workers_stop() sends its worker SIGTERM first,
 *	  from the extension's ProcessUtility hook.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/relation.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/pg_database.h"
#include "commands/extension.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#include "chte.h"

#define CH_LAUNCHER_TYPE "ch_table_engines merge launcher"
#define CH_WORKER_TYPE "ch_table_engines merge worker"

/* At most this many merges of a table in one pass, so that the others get theirs */
#define CH_MERGES_PER_PASS 16

int			ch_max_merge_workers = 2;
int			ch_merge_naptime = 10;

PGDLLEXPORT void ch_merge_launcher_main(Datum arg);
PGDLLEXPORT void ch_merge_worker_main(Datum arg);

void
ch_merge_launcher_register(void)
{
	BackgroundWorker w;

	if (ch_max_merge_workers <= 0)
		return;
	memset(&w, 0, sizeof(w));
	w.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	w.bgw_start_time = BgWorkerStart_RecoveryFinished;
	w.bgw_restart_time = 10;
	snprintf(w.bgw_library_name, MAXPGPATH, "ch_table_engines");
	snprintf(w.bgw_function_name, BGW_MAXLEN, "ch_merge_launcher_main");
	snprintf(w.bgw_name, BGW_MAXLEN, CH_LAUNCHER_TYPE);
	snprintf(w.bgw_type, BGW_MAXLEN, CH_LAUNCHER_TYPE);
	RegisterBackgroundWorker(&w);
}

/* ---- the launcher ---- */

typedef struct DbWorker
{
	Oid			dboid;
	BackgroundWorkerHandle *handle; /* NULL: none running */
	TimestampTz next_start;
	bool		seen;			/* in pg_database at the last look */
} DbWorker;

static DbWorker *db_workers;
static int	ndb_workers;
static int	db_workers_cap;

static DbWorker *
db_worker(Oid dboid)
{
	for (int i = 0; i < ndb_workers; i++)
		if (db_workers[i].dboid == dboid)
			return &db_workers[i];
	if (ndb_workers == db_workers_cap)
	{
		db_workers_cap = db_workers_cap ? db_workers_cap * 2 : 16;
		db_workers = db_workers ? repalloc(db_workers, sizeof(DbWorker) * db_workers_cap) :
			MemoryContextAlloc(TopMemoryContext, sizeof(DbWorker) * db_workers_cap);
	}
	memset(&db_workers[ndb_workers], 0, sizeof(DbWorker));
	db_workers[ndb_workers].dboid = dboid;
	return &db_workers[ndb_workers++];
}

/* The databases a worker may connect to, marked seen */
static void
look_at_databases(void)
{
	Relation	rel;
	TableScanDesc scan;
	HeapTuple	tuple;

	for (int i = 0; i < ndb_workers; i++)
		db_workers[i].seen = false;
	StartTransactionCommand();
	rel = table_open(DatabaseRelationId, AccessShareLock);
	scan = table_beginscan_catalog(rel, 0, NULL);
	while (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_database db = (Form_pg_database) GETSTRUCT(tuple);

		if (db->datallowconn && !db->datistemplate && db->datconnlimit != DATCONNLIMIT_INVALID_DB)
			db_worker(db->oid)->seen = true;
	}
	table_endscan(scan);
	table_close(rel, AccessShareLock);
	CommitTransactionCommand();
}

static void
start_worker(DbWorker *d)
{
	BackgroundWorker w;

	memset(&w, 0, sizeof(w));
	w.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	w.bgw_start_time = BgWorkerStart_RecoveryFinished;
	w.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(w.bgw_library_name, MAXPGPATH, "ch_table_engines");
	snprintf(w.bgw_function_name, BGW_MAXLEN, "ch_merge_worker_main");
	snprintf(w.bgw_name, BGW_MAXLEN, CH_WORKER_TYPE " %u", d->dboid);
	snprintf(w.bgw_type, BGW_MAXLEN, CH_WORKER_TYPE);
	w.bgw_main_arg = ObjectIdGetDatum(d->dboid);
	w.bgw_notify_pid = MyProcPid;
	if (!RegisterDynamicBackgroundWorker(&w, &d->handle))
		d->handle = NULL;		/* no slot: tried again at the next look */
}

void
ch_merge_launcher_main(Datum arg)
{
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();
	/* no database: pg_database is a shared catalog */
	BackgroundWorkerInitializeConnection(NULL, NULL, 0);

	for (;;)
	{
		TimestampTz now;
		int			running = 0;

		CHECK_FOR_INTERRUPTS();
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
		look_at_databases();
		now = GetCurrentTimestamp();
		for (int i = 0; i < ndb_workers; i++)
		{
			DbWorker   *d = &db_workers[i];
			pid_t		pid;

			if (d->handle && GetBackgroundWorkerPid(d->handle, &pid) == BGWH_STOPPED)
			{
				/* it exited: no extension there, or stopped -- looked at again a minute on */
				pfree(d->handle);
				d->handle = NULL;
				d->next_start = TimestampTzPlusSeconds(now, 60);
			}
			if (d->handle)
				running++;
		}
		for (int i = 0; i < ndb_workers && running < ch_max_merge_workers; i++)
		{
			DbWorker   *d = &db_workers[i];

			if (d->seen && d->handle == NULL && now >= d->next_start)
			{
				start_worker(d);
				if (d->handle)
					running++;
			}
		}
		(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 (long) ch_merge_naptime * 1000L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
	}
}

/* ---- a database's worker ---- */

/* The database's tables of the engines: ch.tables' and ch.tables_unlogged's, temporary ones left out */
static List *
engine_tables(void)
{
	List	   *relids = NIL;

	for (int unlogged = 0; unlogged < 2; unlogged++)
	{
		Oid			catid = ch_catalog_relid("tables", unlogged);
		Relation	catalog;
		TableScanDesc scan;
		HeapTuple	tuple;

		if (!OidIsValid(catid))
			continue;
		catalog = table_open(catid, AccessShareLock);
		scan = table_beginscan_catalog(catalog, 0, NULL);
		while (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection)))
		{
			bool		isnull;
			int32		backend = DatumGetInt32(heap_getattr(tuple, 3, RelationGetDescr(catalog), &isnull));
			Oid			relid = DatumGetObjectId(heap_getattr(tuple, 4, RelationGetDescr(catalog), &isnull));

			if (backend == -1 && !list_member_oid(relids, relid))
				relids = lappend_oid(relids, relid);
		}
		table_endscan(scan);
		table_close(catalog, AccessShareLock);
	}
	return relids;
}

/* One merge of a table, in a transaction of its own: false where none was made */
static bool
merge_once(Oid relid)
{
	bool		merged = false;
	MemoryContext cxt = CurrentMemoryContext;

	StartTransactionCommand();
	PG_TRY();
	{
		PushActiveSnapshot(GetTransactionSnapshot());
		if (ConditionalLockRelationOid(relid, ShareUpdateExclusiveLock))
		{
			Relation	rel = try_relation_open(relid, NoLock);

			if (rel)
			{
				if (ch_is_engine_rel(rel, NULL) && rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP)
				{
					pgstat_report_activity(STATE_RUNNING, RelationGetRelationName(rel));
					merged = ch_merge_parts(rel, CH_MERGE_BACKGROUND) > 0;
				}
				relation_close(rel, NoLock);
			}
		}
		PopActiveSnapshot();
		CommitTransactionCommand();
	}
	PG_CATCH();
	{
		/* reported, the transaction aborted, and the next table taken */
		MemoryContextSwitchTo(cxt);
		HOLD_INTERRUPTS();
		EmitErrorReport();
		AbortCurrentTransaction();
		FlushErrorState();
		RESUME_INTERRUPTS();
		merged = false;
	}
	PG_END_TRY();
	pgstat_report_activity(STATE_IDLE, NULL);
	return merged;
}

/*
 * The files store's sweep of a table (store.c), as VACUUM's: what merges left
 * goes once VACUUM of ch.parts has pruned its rows; under ShareLock, taken
 * without waiting, so that no writer is in flight.
 */
static void
sweep_once(Oid relid)
{
	MemoryContext cxt = CurrentMemoryContext;

	StartTransactionCommand();
	PG_TRY();
	{
		PushActiveSnapshot(GetTransactionSnapshot());
		if (ConditionalLockRelationOid(relid, ShareLock))
		{
			Relation	rel = try_relation_open(relid, NoLock);

			if (rel)
			{
				ChOptions	opts;

				ch_relation_options(rel, &opts);
				if (ch_is_engine_rel(rel, NULL) && opts.storage == CH_STORAGE_FILES &&
					rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP)
				{
					ChStorageId id;

					ch_storage_id(rel, &rel->rd_locator, &id);
					(void) ch_store_sweep(rel, &id);
				}
				relation_close(rel, NoLock);
			}
		}
		PopActiveSnapshot();
		CommitTransactionCommand();
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(cxt);
		HOLD_INTERRUPTS();
		EmitErrorReport();
		AbortCurrentTransaction();
		FlushErrorState();
		RESUME_INTERRUPTS();
	}
	PG_END_TRY();
}

void
ch_merge_worker_main(Datum arg)
{
	Oid			dboid = DatumGetObjectId(arg);
	MemoryContext pass_cxt;

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnectionByOid(dboid, InvalidOid, 0);
	pass_cxt = AllocSetContextCreate(TopMemoryContext, "ch merge pass", ALLOCSET_DEFAULT_SIZES);

	for (;;)
	{
		List	   *relids = NIL;
		ListCell   *lc;
		bool		extension;

		CHECK_FOR_INTERRUPTS();
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
		MemoryContextReset(pass_cxt);
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());
		extension = OidIsValid(get_extension_oid("ch_table_engines", true));
		if (extension)
		{
			MemoryContext old = MemoryContextSwitchTo(pass_cxt);

			relids = engine_tables();
			MemoryContextSwitchTo(old);
		}
		PopActiveSnapshot();
		CommitTransactionCommand();
		if (!extension)
			proc_exit(0);		/* the launcher looks again a minute on */

		foreach(lc, relids)
		{
			for (int i = 0; i < CH_MERGES_PER_PASS; i++)
			{
				CHECK_FOR_INTERRUPTS();
				if (!merge_once(lfirst_oid(lc)))
					break;
			}
			sweep_once(lfirst_oid(lc));
		}
		(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 (long) ch_merge_naptime * 1000L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
	}
}

/*
 * The merge workers connected to a database, sent SIGTERM: before a command
 * that needs the database to itself waits for its other backends to exit.
 */
void
ch_merge_workers_stop(Oid dboid)
{
	pid_t	   *pids = palloc(sizeof(pid_t) * ProcGlobal->allProcCount);
	int			n = 0;

	if (!OidIsValid(dboid))
		return;
	LWLockAcquire(ProcArrayLock, LW_SHARED);
	for (uint32 i = 0; i < ProcGlobal->allProcCount; i++)
	{
		PGPROC	   *proc = &ProcGlobal->allProcs[i];

		if (proc->pid != 0 && proc->databaseId == dboid && proc->backendType == B_BG_WORKER)
			pids[n++] = proc->pid;
	}
	LWLockRelease(ProcArrayLock);
	for (int i = 0; i < n; i++)
	{
		const char *type = GetBackgroundWorkerTypeByPid(pids[i]);

		if (type && strcmp(type, CH_WORKER_TYPE) == 0)
			(void) kill(pids[i], SIGTERM);
	}
	pfree(pids);
}
