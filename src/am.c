/*-------------------------------------------------------------------------
 *
 * am.c
 *	  The table access methods, one per engine over the same callbacks
 *	  (ch_table_engines.md §2.4).
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amapi.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/pg_am.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "commands/defrem.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/read_stream.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include "chte.h"
#include "tombstones.h"

static TableAmRoutine routines[CH_ENGINE_COUNT];
static TableAmRoutine external_routines[CH_EXTERNAL_COUNT];
static bool routines_made = false;

/* The external access methods' names (§2.17), as CREATE ACCESS METHOD names them */
static const char *const external_names[CH_EXTERNAL_COUNT] = {
	"parquet",
	"arrow",
	"iceberg",
};

const char *
ch_external_name(ChExternalKind kind)
{
	return external_names[kind];
}

static const char *const engine_names[CH_ENGINE_COUNT] = {
	"MergeTree",
	"ReplacingMergeTree",
	"SummingMergeTree",
	"AggregatingMergeTree",
	"CollapsingMergeTree",
	"VersionedCollapsingMergeTree",
	"CoalescingMergeTree",
};

const char *
ch_engine_name(ChEngineKind kind)
{
	return engine_names[kind];
}

bool
ch_is_engine_rel(Relation rel, ChEngineKind *kind)
{
	for (int k = 0; k < CH_ENGINE_COUNT; k++)
	{
		if (rel->rd_tableam == &routines[k])
		{
			if (kind)
				*kind = (ChEngineKind) k;
			return true;
		}
	}
	return false;
}

bool
ch_is_engine_am(Oid amoid, ChEngineKind *kind)
{
	HeapTuple	tuple;
	Form_pg_am	am;
	const TableAmRoutine *routine;

	if (!OidIsValid(amoid))
		return false;
	tuple = SearchSysCache1(AMOID, ObjectIdGetDatum(amoid));
	if (!HeapTupleIsValid(tuple))
		return false;
	am = (Form_pg_am) GETSTRUCT(tuple);
	if (am->amtype != AMTYPE_TABLE)
	{
		ReleaseSysCache(tuple);
		return false;
	}
	routine = GetTableAmRoutine(am->amhandler);
	ReleaseSysCache(tuple);
	for (int k = 0; k < CH_ENGINE_COUNT; k++)
	{
		if (routine == &routines[k])
		{
			if (kind)
				*kind = (ChEngineKind) k;
			return true;
		}
	}
	return false;
}

bool
ch_am_name_kind(const char *amname, ChEngineKind *kind)
{
	if (amname == NULL)
		return false;
	return ch_is_engine_am(get_table_am_oid(amname, true), kind);
}

bool
ch_is_external_rel(Relation rel, ChExternalKind *kind)
{
	for (int k = 0; k < CH_EXTERNAL_COUNT; k++)
	{
		if (rel->rd_tableam == &external_routines[k])
		{
			if (kind)
				*kind = (ChExternalKind) k;
			return true;
		}
	}
	return false;
}

bool
ch_is_external_am(Oid amoid, ChExternalKind *kind)
{
	HeapTuple	tuple;
	Form_pg_am	am;
	const TableAmRoutine *routine;

	if (!OidIsValid(amoid))
		return false;
	tuple = SearchSysCache1(AMOID, ObjectIdGetDatum(amoid));
	if (!HeapTupleIsValid(tuple))
		return false;
	am = (Form_pg_am) GETSTRUCT(tuple);
	if (am->amtype != AMTYPE_TABLE)
	{
		ReleaseSysCache(tuple);
		return false;
	}
	routine = GetTableAmRoutine(am->amhandler);
	ReleaseSysCache(tuple);
	for (int k = 0; k < CH_EXTERNAL_COUNT; k++)
	{
		if (routine == &external_routines[k])
		{
			if (kind)
				*kind = (ChExternalKind) k;
			return true;
		}
	}
	return false;
}

bool
ch_external_am_name_kind(const char *amname, ChExternalKind *kind)
{
	if (amname == NULL)
		return false;
	return ch_is_external_am(get_table_am_oid(amname, true), kind);
}

bool
ch_is_scan_rel(Relation rel)
{
	return ch_is_engine_rel(rel, NULL) || ch_is_external_rel(rel, NULL);
}

/* ------------------------------------------------------------------------
 * What comes in later phases
 * ------------------------------------------------------------------------
 */

static void
not_yet(const char *what, const char *phase)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("%s is not supported on tables of the ClickHouse engines yet", what),
			 errdetail("It comes in phase %s of ch_table_engines.md.", phase)));
}

/*
 * The engines' slots are virtual slots whose heap copies carry the row's TID:
 * ANALYZE sorts its sample's copies by t_self (analyze.c, compare_rows()), and
 * a trigger's tuple has it, as heap's have.  JIT declines slot types it does
 * not know (llvmjit_deform.c), and a filled virtual slot is never deformed.
 */
TupleTableSlotOps ch_slot_ops;

static HeapTuple
ch_slot_copy_heap_tuple(TupleTableSlot *slot)
{
	HeapTuple	tuple = TTSOpsVirtual.copy_heap_tuple(slot);

	tuple->t_self = slot->tts_tid;
	tuple->t_tableOid = slot->tts_tableOid;
	return tuple;
}

static const TupleTableSlotOps *
ch_slot_callbacks(Relation rel)
{
	return &ch_slot_ops;
}

static bool
ch_tuple_fetch_row_version(Relation rel, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot)
{
	return ch_fetch_row(rel, tid, snapshot, slot);
}

static bool
ch_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	return ItemPointerIsValid(tid) && ItemPointerGetOffsetNumberNoCheck(tid) <= CH_ROWS_PER_BLOCK;
}

static void
ch_tuple_insert(Relation rel, TupleTableSlot *slot, CommandId cid, uint32 options,
				BulkInsertStateData *bistate)
{
	ch_buffer_insert(rel, slot, cid);
	/* as heap_insert() counts it */
	pgstat_count_heap_insert(rel, 1);
}

static void
ch_tuple_insert_speculative(Relation rel, TupleTableSlot *slot, CommandId cid, uint32 options,
							BulkInsertStateData *bistate, uint32 specToken)
{
	not_yet("INSERT ... ON CONFLICT", "CH5");
}

static void
ch_tuple_complete_speculative(Relation rel, TupleTableSlot *slot, uint32 specToken, bool succeeded)
{
	not_yet("INSERT ... ON CONFLICT", "CH5");
}

static void
ch_multi_insert(Relation rel, TupleTableSlot **slots, int nslots, CommandId cid, uint32 options,
				BulkInsertStateData *bistate)
{
	for (int i = 0; i < nslots; i++)
		ch_buffer_insert(rel, slots[i], cid);
	/* as heap_multi_insert() counts them */
	pgstat_count_heap_insert(rel, nslots);
}

static void
ch_finish_bulk_insert(Relation rel, uint32 options)
{
	ch_buffer_flush_relation(rel);
}

/* ------------------------------------------------------------------------
 * Storage
 * ------------------------------------------------------------------------
 */

/*
 * New storage: an empty main fork, as heap's, so that its relfilenumber is
 * taken while the table lives (GetNewRelFileNumber() checks the file); the
 * storage's ch.tables row; its directory, which the engine library makes as
 * it first opens the table, removed if the transaction aborts.  The old
 * storage, where the relation had one, goes at commit (TRUNCATE, a rewrite's
 * new relfilenumber), its catalog rows deleted now.
 */
static void
ch_relation_set_new_filelocator(Relation rel, const RelFileLocator *newrlocator, char persistence,
								TransactionId *freezeXid, MultiXactId *minmulti)
{
	SMgrRelation srel;
	ChEngineKind kind = CH_ENGINE_MERGETREE;
	ChOptions	opts;
	ChStorageId newid;

	*freezeXid = InvalidTransactionId;
	*minmulti = InvalidMultiXactId;

	if (persistence == RELPERSISTENCE_PERMANENT && !ch_preloaded)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("a permanent table of the ClickHouse engines needs ch_table_engines in shared_preload_libraries"),
				 errdetail("Its WAL is replayed by the module's resource manager, which only a preloaded library registers."),
				 errhint("Add ch_table_engines to shared_preload_libraries, or make the table UNLOGGED or TEMPORARY.")));

	srel = RelationCreateStorage(*newrlocator, persistence, true);
	if (persistence == RELPERSISTENCE_UNLOGGED)
	{
		smgrcreate(srel, INIT_FORKNUM, false);
		log_smgrcreate(newrlocator, INIT_FORKNUM);
	}
	smgrclose(srel);

	if (!RelFileLocatorEquals(rel->rd_locator, *newrlocator) && RelFileNumberIsValid(rel->rd_locator.relNumber))
	{
		ChStorageId oldid;

		ch_storage_id(rel, &rel->rd_locator, &oldid);
		ch_buffer_forget_relation(RelationGetRelid(rel));
		ch_parts_delete_all(&oldid);
		ch_tombstones_delete_all(&oldid);
		ch_table_row_delete(&oldid);
		ch_store_drop_at_commit(&oldid);
	}

	(void) ch_is_engine_rel(rel, &kind);
	ch_relation_options(rel, &opts);
	ch_storage_id(rel, newrlocator, &newid);
	newid.persistence = persistence;
	newid.backend = persistence == RELPERSISTENCE_TEMP ? rel->rd_backend : -1;
	ch_table_row_create(rel, &newid, kind, opts.storage, ch_plain_columns(RelationGetDescr(rel)));
	ch_store_create(&newid);
	ch_table_forget(RelationGetRelid(rel));
}

/* TRUNCATE of a table made in the same transaction: its parts go now */
static void
ch_relation_nontransactional_truncate(Relation rel)
{
	ChStorageId id;
	List	   *parts;
	ListCell   *lc;

	ch_storage_id(rel, &rel->rd_locator, &id);
	ch_buffer_forget_relation(RelationGetRelid(rel));
	parts = ch_parts_visible(&id, NULL);
	ch_parts_delete_all(&id);
	ch_tombstones_delete_all(&id);
	foreach(lc, parts)
		ch_store_remove_now(&id, ((ChPart *) lfirst(lc))->name, true);
}

/*
 * A relation of an engine is being dropped (OAT_DROP): its storage goes at
 * commit, its catalog rows now, in the dropping transaction.
 */
void
ch_relation_dropped(Oid relid)
{
	Relation	rel = RelationIdGetRelation(relid);
	ChStorageId id;

	if (!RelationIsValid(rel))
		return;
	if (ch_is_engine_rel(rel, NULL) && RelFileNumberIsValid(rel->rd_locator.relNumber))
	{
		ch_storage_id(rel, &rel->rd_locator, &id);
		ch_buffer_forget_relation(relid);
		PG_TRY();
		{
			ch_parts_delete_all(&id);
			ch_tombstones_delete_all(&id);
			ch_table_row_delete(&id);
		}
		PG_CATCH();
		{
			/* DROP EXTENSION drops the catalogs too; the directories go anyway */
			RelationClose(rel);
			PG_RE_THROW();
		}
		PG_END_TRY();
		ch_store_drop_at_commit(&id);
	}
	RelationClose(rel);
	ch_table_forget(relid);
}

/*
 * SET TABLESPACE: the main fork as heap copies it (heapam_handler.c:541-589),
 * then each part the transaction sees copied to the new storage's directory
 * with its catalog row; the old storage goes at commit.
 */
static void
ch_relation_copy_data(Relation rel, const RelFileLocator *newrlocator)
{
	SMgrRelation dstrel;
	ChStorageId oldid;
	ChStorageId newid;
	List	   *parts;
	ListCell   *lc;

	ch_buffer_flush_relation(rel);
	FlushRelationBuffers(rel);
	dstrel = RelationCreateStorage(*newrlocator, rel->rd_rel->relpersistence, true);
	RelationCopyStorage(RelationGetSmgr(rel), dstrel, MAIN_FORKNUM, rel->rd_rel->relpersistence);
	if (smgrexists(RelationGetSmgr(rel), INIT_FORKNUM))
	{
		smgrcreate(dstrel, INIT_FORKNUM, false);
		log_smgrcreate(newrlocator, INIT_FORKNUM);
		RelationCopyStorage(RelationGetSmgr(rel), dstrel, INIT_FORKNUM, rel->rd_rel->relpersistence);
	}

	ch_storage_id(rel, &rel->rd_locator, &oldid);
	ch_storage_id(rel, newrlocator, &newid);
	ch_table_row_copy(rel, &oldid, &newid);
	ch_store_create(&newid);
	parts = ch_parts_visible(&oldid, NULL);
	foreach(lc, parts)
	{
		ChPart	   *p = lfirst(lc);

		/* the pages store's parts came with the main fork, their manifests the same */
		if (p->manifest == NULL)
			ch_store_copy_part(rel, &oldid, &newid, p->name);
		ch_part_insert(&newid, p, GetCurrentCommandId(true));
	}
	/* the rows keep their ids, and their deletions with them */
	ch_tombstones_copy(&oldid, &newid);
	ch_parts_delete_all(&oldid);
	ch_tombstones_delete_all(&oldid);
	ch_table_row_delete(&oldid);
	ch_store_drop_at_commit(&oldid);
	ch_table_forget(RelationGetRelid(rel));

	RelationDropStorage(rel);
	smgrclose(dstrel);
}

static void
ch_relation_copy_for_cluster(Relation OldTable, Relation NewTable, Relation OldIndex, bool use_sort,
							 TransactionId OldestXmin, Snapshot snapshot, TransactionId *xid_cutoff,
							 MultiXactId *multi_cutoff, double *num_tuples, double *tups_vacuumed,
							 double *tups_recently_dead)
{
	TableScanDesc scan;
	TupleTableSlot *slot;

	if (OldIndex != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("CLUSTER ... USING an index is not supported on tables of the ClickHouse engines"),
				 errhint("Their rows are kept in the order of their order_by option.")));

	*num_tuples = 0;
	*tups_vacuumed = 0;
	*tups_recently_dead = 0;
	*xid_cutoff = InvalidTransactionId;
	*multi_cutoff = InvalidMultiXactId;

	/* every row the old storage has for the snapshot, row ids kept */
	slot = table_slot_create(OldTable, NULL);
	scan = table_beginscan(OldTable, snapshot, 0, NULL, SO_NONE);
	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		CHECK_FOR_INTERRUPTS();
		ch_buffer_insert(NewTable, slot, GetCurrentCommandId(true));
		*num_tuples += 1;
	}
	table_endscan(scan);
	ExecDropSingleTupleTableSlot(slot);
	ch_buffer_flush_relation(NewTable);
}

/*
 * VACUUM: the files store's sweep, when no writer holds the relation (a
 * conditional ShareLock beside VACUUM's own).  Parts no snapshot sees come
 * with merges (CH3).
 */
static void
ch_relation_vacuum(Relation rel, const VacuumParams *params, BufferAccessStrategy bstrategy)
{
	ChStorageId id;
	int			removed;

	/* the index entries of rows every snapshot sees deleted (vacuum.c) */
	ch_vacuum_indexes(rel, params, bstrategy);
	if (!ConditionalLockRelation(rel, ShareLock))
		return;
	ch_storage_id(rel, &rel->rd_locator, &id);
	removed = ch_store_sweep(rel, &id);
	UnlockRelation(rel, ShareLock);
	if (removed > 0)
		ereport(params->options & VACOPT_VERBOSE ? INFO : DEBUG1,
				(errmsg("removed %d directories of \"%s\" no part row names", removed,
						RelationGetRelationName(rel))));
}

/* ------------------------------------------------------------------------
 * Sizes and estimates
 * ------------------------------------------------------------------------
 */

static uint64
ch_relation_size(Relation rel, ForkNumber forkNumber)
{
	uint64		bytes;
	double		rows;

	if (forkNumber != MAIN_FORKNUM && forkNumber != InvalidForkNumber)
		return 0;
	{
		ChOptions	opts;

		/* the pages store's data is the main fork */
		ch_relation_options(rel, &opts);
		if (opts.storage == CH_STORAGE_PAGES)
			return (uint64) smgrnblocks(RelationGetSmgr(rel), MAIN_FORKNUM) * BLCKSZ;
	}
	{
		ChStorageId id;

		/* the parts' rows' bytes alone, their manifests and summaries left unread (VC20's B8) */
		ch_storage_id(rel, &rel->rd_locator, &id);
		ch_parts_totals(&id, NULL, &rows, &bytes);
	}
	return bytes;
}

static void
ch_relation_estimate_size(Relation rel, int32 *attr_widths, BlockNumber *pages, double *tuples,
						  double *allvisfrac)
{
	uint64		bytes;
	double		rows;

	{
		ChStorageId id;

		/* every query's: the parts' rows and bytes alone, no manifest copied out of TOAST (VC20's B8) */
		ch_storage_id(rel, &rel->rd_locator, &id);
		ch_parts_totals(&id, NULL, &rows, &bytes);
	}
	*pages = (BlockNumber) ((bytes + BLCKSZ - 1) / BLCKSZ);
	*tuples = rows;
	*allvisfrac = 0;
}

static bool
ch_relation_needs_toast_table(Relation rel)
{
	return false;
}

static Oid
ch_relation_toast_am(Relation rel)
{
	return InvalidOid;
}

/* ------------------------------------------------------------------------
 * Samples: a later phase
 * ------------------------------------------------------------------------
 */

static bool
ch_scan_sample_next_block(TableScanDesc scan, SampleScanState *scanstate)
{
	not_yet("TABLESAMPLE", "CH5");
	return false;
}

static bool
ch_scan_sample_next_tuple(TableScanDesc scan, SampleScanState *scanstate, TupleTableSlot *slot)
{
	not_yet("TABLESAMPLE", "CH5");
	return false;
}

/* ------------------------------------------------------------------------
 * The external access methods (§2.17): their scans are the engines' (scan.c),
 * over the files of the statement's listing; nothing writes their rows
 * ------------------------------------------------------------------------
 */

pg_noreturn static void
read_only(Relation rel, const char *what)
{
	ChExternalKind kind = CH_EXTERNAL_PARQUET;

	(void) ch_is_external_rel(rel, &kind);
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot %s \"%s\"", what, RelationGetRelationName(rel)),
			 errdetail("A table of access method %s reads the files at its location, and is never written.",
					   ch_external_name(kind))));
}

static void
ext_tuple_insert(Relation rel, TupleTableSlot *slot, CommandId cid, uint32 options, BulkInsertStateData *bistate)
{
	/* a rewrite of an external table copies no row: they are its files' */
	if (!ch_external_rewrite_target(rel))
		read_only(rel, "insert into");
}

static void
ext_multi_insert(Relation rel, TupleTableSlot **slots, int nslots, CommandId cid, uint32 options,
				 BulkInsertStateData *bistate)
{
	if (!ch_external_rewrite_target(rel))
		read_only(rel, "insert into");
}

static void
ext_tuple_insert_speculative(Relation rel, TupleTableSlot *slot, CommandId cid, uint32 options,
							 BulkInsertStateData *bistate, uint32 specToken)
{
	read_only(rel, "insert into");
}

static void
ext_tuple_complete_speculative(Relation rel, TupleTableSlot *slot, uint32 specToken, bool succeeded)
{
	read_only(rel, "insert into");
}

static TM_Result
ext_tuple_delete(Relation rel, ItemPointer tid, CommandId cid, uint32 options, Snapshot snapshot, Snapshot crosscheck,
				 bool wait, TM_FailureData *tmfd)
{
	read_only(rel, "delete from");
}

static TM_Result
ext_tuple_update(Relation rel, ItemPointer otid, TupleTableSlot *slot, CommandId cid, uint32 options,
				 Snapshot snapshot, Snapshot crosscheck, bool wait, TM_FailureData *tmfd, LockTupleMode *lockmode,
				 TU_UpdateIndexes *update_indexes)
{
	read_only(rel, "update");
}

static TM_Result
ext_tuple_lock(Relation rel, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot, CommandId cid,
			   LockTupleMode mode, LockWaitPolicy wait_policy, uint8 flags, TM_FailureData *tmfd)
{
	read_only(rel, "lock rows of");
}

static void
ext_finish_bulk_insert(Relation rel, uint32 options)
{
}

/* A row never moves: its TID is its file's and its place there */
static void
ext_tuple_get_latest_tid(TableScanDesc scan, ItemPointer tid)
{
}

static bool
ext_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot, Snapshot snapshot)
{
	return true;
}

pg_noreturn static void
no_indexes(Relation rel)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("indexes are not supported on \"%s\"", RelationGetRelationName(rel)),
			 errdetail("Its rows are its files'; their statistics prune what a scan reads.")));
}

static struct IndexFetchTableData *
ext_index_fetch_begin(Relation rel, uint32 flags)
{
	no_indexes(rel);
}

static void
ext_index_fetch_reset(struct IndexFetchTableData *data)
{
}

static void
ext_index_fetch_end(struct IndexFetchTableData *data)
{
}

static bool
ext_index_fetch_tuple(struct IndexFetchTableData *scan, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot,
					  bool *call_again, bool *all_dead)
{
	no_indexes(scan->rel);
}

static TransactionId
ext_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
	no_indexes(rel);
}

static double
ext_index_build_range_scan(Relation table_rel, Relation index_rel, struct IndexInfo *index_info, bool allow_sync,
						   bool anyvisible, bool progress, BlockNumber start_blockno, BlockNumber numblocks,
						   IndexBuildCallback callback, void *callback_state, TableScanDesc scan)
{
	no_indexes(table_rel);
}

static void
ext_index_validate_scan(Relation table_rel, Relation index_rel, struct IndexInfo *index_info, Snapshot snapshot,
						struct ValidateIndexState *state)
{
	no_indexes(table_rel);
}

static bool
ext_scan_bitmap_next_tuple(TableScanDesc scan, TupleTableSlot *slot, bool *recheck, uint64 *lossy_pages,
						   uint64 *exact_pages)
{
	no_indexes(scan->rs_rd);
}

static bool
ext_scan_sample_next_block(TableScanDesc scan, SampleScanState *scanstate)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("TABLESAMPLE is not supported on \"%s\"", RelationGetRelationName(scan->rs_rd))));
	return false;
}

static bool
ext_scan_sample_next_tuple(TableScanDesc scan, SampleScanState *scanstate, TupleTableSlot *slot)
{
	return ext_scan_sample_next_block(scan, scanstate);
}

/*
 * Storage: the main fork heap's is (and an UNLOGGED table's init fork), which
 * takes the relfilenumber while the table lives and holds nothing.  TRUNCATE
 * gives the table new storage and leaves its files as they are -- pg_restore
 * -j runs it before the COPY of a table it made (§2.17) -- saying so.
 */
static void
ext_relation_set_new_filelocator(Relation rel, const RelFileLocator *newrlocator, char persistence,
								 TransactionId *freezeXid, MultiXactId *minmulti)
{
	SMgrRelation srel;

	*freezeXid = InvalidTransactionId;
	*minmulti = InvalidMultiXactId;
	srel = RelationCreateStorage(*newrlocator, persistence, true);
	if (persistence == RELPERSISTENCE_UNLOGGED)
	{
		smgrcreate(srel, INIT_FORKNUM, false);
		log_smgrcreate(newrlocator, INIT_FORKNUM);
	}
	smgrclose(srel);
	if (!RelFileLocatorEquals(rel->rd_locator, *newrlocator) && RelFileNumberIsValid(rel->rd_locator.relNumber))
		ch_external_truncated(rel);
	ch_table_forget(RelationGetRelid(rel));
}

static void
ext_relation_nontransactional_truncate(Relation rel)
{
	ch_external_truncated(rel);
}

/* SET TABLESPACE: the main fork as heap copies it (heapam_handler.c) */
static void
ext_relation_copy_data(Relation rel, const RelFileLocator *newrlocator)
{
	SMgrRelation dstrel;

	FlushRelationBuffers(rel);
	dstrel = RelationCreateStorage(*newrlocator, rel->rd_rel->relpersistence, true);
	RelationCopyStorage(RelationGetSmgr(rel), dstrel, MAIN_FORKNUM, rel->rd_rel->relpersistence);
	if (smgrexists(RelationGetSmgr(rel), INIT_FORKNUM))
	{
		smgrcreate(dstrel, INIT_FORKNUM, false);
		log_smgrcreate(newrlocator, INIT_FORKNUM);
		RelationCopyStorage(RelationGetSmgr(rel), dstrel, INIT_FORKNUM, rel->rd_rel->relpersistence);
	}
	RelationDropStorage(rel);
	smgrclose(dstrel);
	ch_table_forget(RelationGetRelid(rel));
}

/* VACUUM FULL, CLUSTER: no row to copy, the files' being the table's */
static void
ext_relation_copy_for_cluster(Relation OldTable, Relation NewTable, Relation OldIndex, bool use_sort,
							  TransactionId OldestXmin, Snapshot snapshot, TransactionId *xid_cutoff,
							  MultiXactId *multi_cutoff, double *num_tuples, double *tups_vacuumed,
							  double *tups_recently_dead)
{
	if (OldIndex != NULL)
		no_indexes(OldTable);
	*num_tuples = 0;
	*tups_vacuumed = 0;
	*tups_recently_dead = 0;
	*xid_cutoff = InvalidTransactionId;
	*multi_cutoff = InvalidMultiXactId;
}

static void
ext_relation_vacuum(Relation rel, const VacuumParams *params, BufferAccessStrategy bstrategy)
{
}

/* The files' bytes, as the statement lists them: ANALYZE's blocks (§2.17) */
static uint64
ext_relation_size(Relation rel, ForkNumber forkNumber)
{
	double		rows;
	uint64		bytes;

	if (forkNumber != MAIN_FORKNUM && forkNumber != InvalidForkNumber)
		return 0;
	ch_external_totals(rel, &rows, &bytes);
	return bytes;
}

static void
ext_relation_estimate_size(Relation rel, int32 *attr_widths, BlockNumber *pages, double *tuples,
						   double *allvisfrac)
{
	double		rows;
	uint64		bytes;

	ch_external_totals(rel, &rows, &bytes);
	*pages = (BlockNumber) Min((bytes + BLCKSZ - 1) / BLCKSZ, (uint64) MaxBlockNumber);
	*tuples = rows;
	*allvisfrac = 0;
}

static void
make_external_routines(const TableAmRoutine *base)
{
	TableAmRoutine r = *base;

	r.index_fetch_begin = ext_index_fetch_begin;
	r.index_fetch_reset = ext_index_fetch_reset;
	r.index_fetch_end = ext_index_fetch_end;
	r.index_fetch_tuple = ext_index_fetch_tuple;
	r.tuple_get_latest_tid = ext_tuple_get_latest_tid;
	r.tuple_satisfies_snapshot = ext_tuple_satisfies_snapshot;
	r.index_delete_tuples = ext_index_delete_tuples;
	r.tuple_insert = ext_tuple_insert;
	r.tuple_insert_speculative = ext_tuple_insert_speculative;
	r.tuple_complete_speculative = ext_tuple_complete_speculative;
	r.multi_insert = ext_multi_insert;
	r.tuple_delete = ext_tuple_delete;
	r.tuple_update = ext_tuple_update;
	r.tuple_lock = ext_tuple_lock;
	r.finish_bulk_insert = ext_finish_bulk_insert;
	r.relation_set_new_filelocator = ext_relation_set_new_filelocator;
	r.relation_nontransactional_truncate = ext_relation_nontransactional_truncate;
	r.relation_copy_data = ext_relation_copy_data;
	r.relation_copy_for_cluster = ext_relation_copy_for_cluster;
	r.relation_vacuum = ext_relation_vacuum;
	r.index_build_range_scan = ext_index_build_range_scan;
	r.index_validate_scan = ext_index_validate_scan;
	r.relation_size = ext_relation_size;
	r.relation_estimate_size = ext_relation_estimate_size;
	r.scan_bitmap_next_tuple = ext_scan_bitmap_next_tuple;
	r.scan_sample_next_block = ext_scan_sample_next_block;
	r.scan_sample_next_tuple = ext_scan_sample_next_tuple;
	for (int k = 0; k < CH_EXTERNAL_COUNT; k++)
		external_routines[k] = r;
}

static void
make_routines(void)
{
	TableAmRoutine r;

	memset(&r, 0, sizeof(r));
	r.type = T_TableAmRoutine;
	r.slot_callbacks = ch_slot_callbacks;
	r.scan_begin = ch_scan_begin;
	r.scan_end = ch_scan_end;
	r.scan_rescan = ch_scan_rescan;
	r.scan_getnextslot = ch_scan_getnextslot;
	r.parallelscan_estimate = ch_parallelscan_estimate;
	r.parallelscan_initialize = ch_parallelscan_initialize;
	r.parallelscan_reinitialize = ch_parallelscan_reinitialize;
	r.index_fetch_begin = ch_index_fetch_begin;
	r.index_fetch_reset = ch_index_fetch_reset;
	r.index_fetch_end = ch_index_fetch_end;
	r.index_fetch_tuple = ch_index_fetch_tuple;
	r.tuple_fetch_row_version = ch_tuple_fetch_row_version;
	r.tuple_tid_valid = ch_tuple_tid_valid;
	r.tuple_get_latest_tid = ch_tuple_get_latest_tid;
	r.tuple_satisfies_snapshot = ch_tuple_satisfies_snapshot;
	r.index_delete_tuples = ch_index_delete_tuples;
	r.tuple_insert = ch_tuple_insert;
	r.tuple_insert_speculative = ch_tuple_insert_speculative;
	r.tuple_complete_speculative = ch_tuple_complete_speculative;
	r.multi_insert = ch_multi_insert;
	r.tuple_delete = ch_tuple_delete;
	r.tuple_update = ch_tuple_update;
	r.tuple_lock = ch_tuple_lock;
	r.finish_bulk_insert = ch_finish_bulk_insert;
	r.relation_set_new_filelocator = ch_relation_set_new_filelocator;
	r.relation_nontransactional_truncate = ch_relation_nontransactional_truncate;
	r.relation_copy_data = ch_relation_copy_data;
	r.relation_copy_for_cluster = ch_relation_copy_for_cluster;
	r.relation_vacuum = ch_relation_vacuum;
	r.scan_analyze_next_block = ch_scan_analyze_next_block;
	r.scan_analyze_next_tuple = ch_scan_analyze_next_tuple;
	r.index_build_range_scan = ch_index_build_range_scan;
	r.index_validate_scan = ch_index_validate_scan;
	r.relation_size = ch_relation_size;
	r.relation_needs_toast_table = ch_relation_needs_toast_table;
	r.relation_toast_am = ch_relation_toast_am;
	r.relation_fetch_toast_slice = NULL;
	r.relation_estimate_size = ch_relation_estimate_size;
	r.scan_bitmap_next_tuple = ch_scan_bitmap_next_tuple;
	r.scan_sample_next_block = ch_scan_sample_next_block;
	r.scan_sample_next_tuple = ch_scan_sample_next_tuple;

	ch_slot_ops = TTSOpsVirtual;
	ch_slot_ops.copy_heap_tuple = ch_slot_copy_heap_tuple;

	for (int k = 0; k < CH_ENGINE_COUNT; k++)
		routines[k] = r;
	make_external_routines(&r);
	routines_made = true;
}

static Datum
handler(ChEngineKind kind)
{
	if (!routines_made)
		make_routines();
	PG_RETURN_POINTER(&routines[kind]);
}

/* An engine's routine, the key of its vexec source and sink, made by _PG_init */
const TableAmRoutine *
ch_am_routine(ChEngineKind kind)
{
	if (!routines_made)
		make_routines();
	return &routines[kind];
}

/* An external access method's routine, the key of its vexec source, made by _PG_init */
const TableAmRoutine *
ch_external_routine(ChExternalKind kind)
{
	if (!routines_made)
		make_routines();
	return &external_routines[kind];
}

static Datum
external_handler(ChExternalKind kind)
{
	if (!routines_made)
		make_routines();
	PG_RETURN_POINTER(&external_routines[kind]);
}

#define HANDLER(fn, kind) \
	PG_FUNCTION_INFO_V1(fn); \
	Datum fn(PG_FUNCTION_ARGS) { return handler(kind); }

#define EXTERNAL_HANDLER(fn, kind) \
	PG_FUNCTION_INFO_V1(fn); \
	Datum fn(PG_FUNCTION_ARGS) { return external_handler(kind); }

HANDLER(ch_mergetree_handler, CH_ENGINE_MERGETREE)
HANDLER(ch_replacingmergetree_handler, CH_ENGINE_REPLACING)
HANDLER(ch_summingmergetree_handler, CH_ENGINE_SUMMING)
HANDLER(ch_aggregatingmergetree_handler, CH_ENGINE_AGGREGATING)
HANDLER(ch_collapsingmergetree_handler, CH_ENGINE_COLLAPSING)
HANDLER(ch_versionedcollapsingmergetree_handler, CH_ENGINE_VERSIONED_COLLAPSING)
HANDLER(ch_coalescingmergetree_handler, CH_ENGINE_COALESCING)
EXTERNAL_HANDLER(ch_parquet_handler, CH_EXTERNAL_PARQUET)
EXTERNAL_HANDLER(ch_arrow_handler, CH_EXTERNAL_ARROW)
EXTERNAL_HANDLER(ch_iceberg_handler, CH_EXTERNAL_ICEBERG)
