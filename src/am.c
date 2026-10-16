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

static TableAmRoutine routines[CH_ENGINE_COUNT];
static bool routines_made = false;

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
static TupleTableSlotOps ch_slot_ops;

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

static struct IndexFetchTableData *
ch_index_fetch_begin(Relation rel, uint32 flags)
{
	not_yet("an index", "CH2");
	return NULL;
}

static void
ch_index_fetch_reset(struct IndexFetchTableData *data)
{
}

static void
ch_index_fetch_end(struct IndexFetchTableData *data)
{
}

static bool
ch_index_fetch_tuple(struct IndexFetchTableData *scan, ItemPointer tid, Snapshot snapshot,
					 TupleTableSlot *slot, bool *call_again, bool *all_dead)
{
	not_yet("an index", "CH2");
	return false;
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
ch_tuple_get_latest_tid(TableScanDesc scan, ItemPointer tid)
{
	/* rows are never updated in place, nor yet through tombstones (CH2) */
}

static bool
ch_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot, Snapshot snapshot)
{
	TupleTableSlot *probe = MakeSingleTupleTableSlot(RelationGetDescr(rel), &ch_slot_ops);
	bool		found;

	found = ch_fetch_row(rel, &slot->tts_tid, snapshot, probe);
	ExecDropSingleTupleTableSlot(probe);
	return found;
}

static TransactionId
ch_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
	not_yet("an index", "CH2");
	return InvalidTransactionId;
}

static void
ch_tuple_insert(Relation rel, TupleTableSlot *slot, CommandId cid, uint32 options,
				BulkInsertStateData *bistate)
{
	ch_buffer_insert(rel, slot, cid);
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
}

static TM_Result
ch_tuple_delete(Relation rel, ItemPointer tid, CommandId cid, uint32 options, Snapshot snapshot,
				Snapshot crosscheck, bool wait, TM_FailureData *tmfd)
{
	not_yet("DELETE", "CH2");
	return TM_Ok;
}

static TM_Result
ch_tuple_update(Relation rel, ItemPointer otid, TupleTableSlot *slot, CommandId cid, uint32 options,
				Snapshot snapshot, Snapshot crosscheck, bool wait, TM_FailureData *tmfd,
				LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes)
{
	not_yet("UPDATE", "CH2");
	return TM_Ok;
}

static TM_Result
ch_tuple_lock(Relation rel, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot, CommandId cid,
			  LockTupleMode mode, LockWaitPolicy wait_policy, uint8 flags, TM_FailureData *tmfd)
{
	not_yet("SELECT ... FOR UPDATE/SHARE", "CH2");
	return TM_Ok;
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
		ch_table_row_delete(&oldid);
		ch_store_drop_at_commit(&oldid);
	}

	(void) ch_is_engine_rel(rel, &kind);
	ch_relation_options(rel, &opts);
	ch_storage_id(rel, newrlocator, &newid);
	newid.persistence = persistence;
	newid.backend = persistence == RELPERSISTENCE_TEMP ? rel->rd_backend : -1;
	ch_table_row_create(rel, &newid, kind, opts.storage);
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
	ch_parts_delete_all(&oldid);
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
	List	   *parts;
	ListCell   *lc;
	uint64		bytes = 0;

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

		ch_storage_id(rel, &rel->rd_locator, &id);
		parts = ch_parts_visible(&id, NULL);
	}
	foreach(lc, parts)
		bytes += ((ChPart *) lfirst(lc))->bytes;
	return bytes;
}

static void
ch_relation_estimate_size(Relation rel, int32 *attr_widths, BlockNumber *pages, double *tuples,
						  double *allvisfrac)
{
	List	   *parts;
	ListCell   *lc;
	uint64		bytes = 0;
	double		rows = 0;

	{
		ChStorageId id;

		ch_storage_id(rel, &rel->rd_locator, &id);
		parts = ch_parts_visible(&id, NULL);
	}
	foreach(lc, parts)
	{
		ChPart	   *p = lfirst(lc);

		bytes += p->bytes;
		rows += p->rows;
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
 * Indexes, bitmaps, samples: later phases
 * ------------------------------------------------------------------------
 */

static double
ch_index_build_range_scan(Relation table_rel, Relation index_rel, IndexInfo *index_info, bool allow_sync,
						  bool anyvisible, bool progress, BlockNumber start_blockno, BlockNumber numblocks,
						  IndexBuildCallback callback, void *callback_state, TableScanDesc scan)
{
	not_yet("an index", "CH2");
	return 0;
}

static void
ch_index_validate_scan(Relation table_rel, Relation index_rel, IndexInfo *index_info, Snapshot snapshot,
					   ValidateIndexState *state)
{
	not_yet("an index", "CH2");
}

static bool
ch_scan_bitmap_next_tuple(TableScanDesc scan, TupleTableSlot *slot, bool *recheck,
						  uint64 *lossy_pages, uint64 *exact_pages)
{
	not_yet("a bitmap scan", "CH2");
	return false;
}

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
	routines_made = true;
}

static Datum
handler(ChEngineKind kind)
{
	if (!routines_made)
		make_routines();
	PG_RETURN_POINTER(&routines[kind]);
}

#define HANDLER(fn, kind) \
	PG_FUNCTION_INFO_V1(fn); \
	Datum fn(PG_FUNCTION_ARGS) { return handler(kind); }

HANDLER(ch_mergetree_handler, CH_ENGINE_MERGETREE)
HANDLER(ch_replacingmergetree_handler, CH_ENGINE_REPLACING)
HANDLER(ch_summingmergetree_handler, CH_ENGINE_SUMMING)
HANDLER(ch_aggregatingmergetree_handler, CH_ENGINE_AGGREGATING)
HANDLER(ch_collapsingmergetree_handler, CH_ENGINE_COLLAPSING)
HANDLER(ch_versionedcollapsingmergetree_handler, CH_ENGINE_VERSIONED_COLLAPSING)
HANDLER(ch_coalescingmergetree_handler, CH_ENGINE_COALESCING)
