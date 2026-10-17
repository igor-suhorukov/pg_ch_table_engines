/*-------------------------------------------------------------------------
 *
 * index.c
 *	  Indexes on the tables of the engines (ch_table_engines.md §2.10):
 *	  btree, hash, GiST, GIN and SP-GiST, their entries keyed by the rows'
 *	  TIDs, which are their row ids (§2.6).
 *
 *	  A build indexes the rows of the parts the latest snapshot sees, judged
 *	  by their deletions as heapam_index_build_range_scan() judges tuples by
 *	  HeapTupleSatisfiesVacuum() (heapam_handler.c): a row deleted for every
 *	  snapshot left out; one some snapshot may still see indexed, not alive;
 *	  the rest alive.  A fetch finds the row through the parts' _pg_rowid
 *	  projections (scan.c), and reports it dead to all where its deletion
 *	  is older than every running snapshot, so that the index AM may kill
 *	  its entry (all_dead); index_delete_tuples() deletes those.
 *
 *	  Not yet: unique indexes and exclusion constraints, whose checks need
 *	  the rows other backends are writing (CH5), and CREATE INDEX
 *	  CONCURRENTLY (§2.15).  BRIN is refused: it summarizes ranges of
 *	  blocks, and these tables' TIDs say which row, not where it lies -- their
 *	  parts keep min/max statistics of their own.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/tableam.h"
#include "access/transam.h"
#include "catalog/index.h"
#include "catalog/pg_am_d.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "optimizer/optimizer.h"
#include "pgstat.h"
#include "storage/procarray.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "chte.h"
#include "scan.h"
#include "tombstones.h"

typedef struct ChIndexFetch
{
	IndexFetchTableData base;
} ChIndexFetch;

struct IndexFetchTableData *
ch_index_fetch_begin(Relation rel, uint32 flags)
{
	ChIndexFetch *f = palloc0(sizeof(ChIndexFetch));

	f->base.rel = rel;
	return &f->base;
}

void
ch_index_fetch_reset(struct IndexFetchTableData *data)
{
}

void
ch_index_fetch_end(struct IndexFetchTableData *data)
{
	pfree(data);
}

/*
 * A row is dead to all when a committed deletion or update of it is older
 * than every snapshot that can still be taken (GlobalVisTestIsRemovableXid()).
 */
static bool
dead_to_all(Relation rel, uint64 rowid, TransactionId *xid)
{
	ChTable    *t = ch_table(rel);
	ChRowState	st;

	ch_row_state(&t->storage, rowid, &st);
	if (st.status != CH_ROW_MODIFIED)
		return false;
	if (xid)
		*xid = st.xid;
	return GlobalVisTestIsRemovableXid(GlobalVisTestFor(rel), st.xid, true);
}

bool
ch_index_fetch_tuple(struct IndexFetchTableData *scan, ItemPointer tid, Snapshot snapshot,
					 TupleTableSlot *slot, bool *call_again, bool *all_dead)
{
	Relation	rel = scan->rel;
	bool		found;

	*call_again = false;			/* no chains: a TID is one row */
	if (all_dead)
		*all_dead = false;
	found = ch_fetch_row(rel, tid, snapshot, slot);
	if (found)
		pgstat_count_heap_fetch(rel);
	else if (all_dead && snapshot && IsMVCCSnapshot(snapshot) && ItemPointerIsValid(tid))
		*all_dead = dead_to_all(rel, CH_TID_TO_ROWID(tid), NULL);
	return found;
}

bool
ch_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot, Snapshot snapshot)
{
	TupleTableSlot *probe = MakeSingleTupleTableSlot(RelationGetDescr(rel), &ch_slot_ops);
	bool		found;

	found = ch_fetch_row(rel, &slot->tts_tid, snapshot, probe);
	ExecDropSingleTupleTableSlot(probe);
	return found;
}

/*
 * The index entries whose rows are dead to all, among those the index AM
 * names: its known-dead ones stay deletable (they are the ones our fetches
 * reported), and the horizon is the newest of the deleters (heapam.c,
 * heap_index_delete_tuples()).
 */
TransactionId
ch_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
	TransactionId horizon = InvalidTransactionId;
	int			final = 0;

	for (int i = 0; i < delstate->ndeltids; i++)
	{
		TM_IndexDelete *d = &delstate->deltids[i];
		TM_IndexStatus *st = &delstate->status[d->id];
		TransactionId xid = InvalidTransactionId;
		bool		dead = dead_to_all(rel, CH_TID_TO_ROWID(&d->tid), &xid);

		if (dead || st->knowndeletable)
		{
			st->knowndeletable = true;
			if (TransactionIdIsNormal(xid) &&
				(!TransactionIdIsValid(horizon) || TransactionIdFollows(xid, horizon)))
				horizon = xid;
			final = i + 1;
		}
	}
	delstate->ndeltids = final;
	return horizon;
}

/* The deletion of a row among a storage's, sorted by row id: the strongest where there are several */
static const ChTombstone *
find_tombstone(const ChTombstone *tombs, int n, uint64 rowid)
{
	int			lo = 0;
	int			hi = n;
	const ChTombstone *best = NULL;

	while (lo < hi)
	{
		int			mid = lo + (hi - lo) / 2;

		if (tombs[mid].rowid < rowid)
			lo = mid + 1;
		else
			hi = mid;
	}
	for (; lo < n && tombs[lo].rowid == rowid; lo++)
	{
		const ChTombstone *t = &tombs[lo];

		if (best == NULL || (t->committed && !best->committed) ||
			(t->ours && !best->committed && !best->ours))
			best = t;
	}
	return best;
}

/*
 * The build reads the columns the index's keys and INCLUDE columns, its
 * expressions and its predicate take, and the row ids, as ChScan reads a
 * query's: every column of a part would be read where heap reads its pages
 * once.  An expression of the whole row takes them all.
 */
static void
read_index_columns(TableScanDesc scan, IndexInfo *index_info)
{
	Bitmapset  *attrs = NULL;
	AttrNumber *attnums;
	int			n = 0;
	int			x = -1;

	for (int i = 0; i < index_info->ii_NumIndexAttrs; i++)
		if (index_info->ii_IndexAttrNumbers[i] != 0)
			attrs = bms_add_member(attrs, index_info->ii_IndexAttrNumbers[i] - FirstLowInvalidHeapAttributeNumber);
	pull_varattnos((Node *) index_info->ii_Expressions, 1, &attrs);
	pull_varattnos((Node *) index_info->ii_Predicate, 1, &attrs);
	if (bms_is_member(0 - FirstLowInvalidHeapAttributeNumber, attrs))
		return;
	attnums = palloc(sizeof(AttrNumber) * (bms_num_members(attrs) + 1));
	while ((x = bms_next_member(attrs, x)) >= 0)
	{
		AttrNumber	att = x + FirstLowInvalidHeapAttributeNumber;

		if (att > 0)
			attnums[n++] = att;
	}
	ch_scan_set_columns(scan, n, attnums, true);
	pfree(attnums);
	bms_free(attrs);
}

double
ch_index_build_range_scan(Relation table_rel, Relation index_rel, IndexInfo *index_info, bool allow_sync,
						  bool anyvisible, bool progress, BlockNumber start_blockno, BlockNumber numblocks,
						  IndexBuildCallback callback, void *callback_state, TableScanDesc scan)
{
	ChTable    *t = ch_table(table_rel);
	EState	   *estate;
	ExprContext *econtext;
	ExprState  *predicate;
	TupleTableSlot *slot;
	TransactionId oldest_xmin;
	ChTombstone *tombs;
	int			ntombs;
	double		reltuples = 0;
	uint64		lo_rowid = 0;
	uint64		hi_rowid = PG_UINT64_MAX;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];

	if (index_rel->rd_rel->relam == BRIN_AM_OID)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("a BRIN index is not supported on tables of the ClickHouse engines"),
				 errdetail("BRIN summarizes ranges of blocks, and these tables' TIDs name rows, not where they lie."),
				 errhint("Their parts keep the least and greatest values of their columns, which their scans prune by.")));
	if (index_info->ii_Unique)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("a unique index is not supported on tables of the ClickHouse engines yet"),
				 errdetail("It comes in phase CH5 of ch_table_engines.md.")));
	if (index_info->ii_ExclusionOps != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("an exclusion constraint is not supported on tables of the ClickHouse engines yet"),
				 errdetail("It comes in phase CH5 of ch_table_engines.md.")));
	if (index_info->ii_Concurrent)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("CREATE INDEX CONCURRENTLY is not supported on tables of the ClickHouse engines yet"),
				 errhint("Use CREATE INDEX.")));

	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	slot = table_slot_create(table_rel, NULL);
	econtext->ecxt_scantuple = slot;
	predicate = ExecPrepareQual(index_info->ii_Predicate, estate);

	/* every row of the parts, judged here by its deletions */
	oldest_xmin = GetOldestNonRemovableTransactionId(table_rel);
	if (scan == NULL)
		scan = table_beginscan_strat(table_rel, SnapshotAny, 0, NULL, true, allow_sync);
	read_index_columns(scan, index_info);
	if (numblocks != InvalidBlockNumber)
	{
		lo_rowid = (uint64) start_blockno * CH_ROWS_PER_BLOCK;
		hi_rowid = ((uint64) start_blockno + numblocks) * CH_ROWS_PER_BLOCK - 1;
	}
	tombs = ch_tombstones_all(&t->storage, 0, PG_INT64_MAX, &ntombs);

	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		uint64		rowid = CH_TID_TO_ROWID(&slot->tts_tid);
		const ChTombstone *tomb;
		bool		alive = true;

		CHECK_FOR_INTERRUPTS();
		if (rowid < lo_rowid || rowid > hi_rowid)
			continue;
		tomb = ntombs > 0 ? find_tombstone(tombs, ntombs, rowid) : NULL;
		if (tomb == NULL)
			reltuples += 1;
		else if (tomb->committed)
		{
			/* dead to all: left out; recently dead: indexed for the snapshots that see it */
			if (TransactionIdPrecedes(tomb->xmin, oldest_xmin))
				continue;
			alive = false;
		}
		else if (tomb->ours)
			alive = false;		/* deleted by this transaction: indexed, not counted */
		else
		{
			/* a deletion in progress: the locks this build holds keep it from happening */
			elog(WARNING, "concurrent delete in progress within table \"%s\"",
				 RelationGetRelationName(table_rel));
			reltuples += 1;
			alive = false;
		}

		MemoryContextReset(econtext->ecxt_per_tuple_memory);
		if (predicate != NULL && !ExecQual(predicate, econtext))
			continue;
		FormIndexDatum(index_info, slot, estate, values, isnull);
		callback(index_rel, &slot->tts_tid, values, isnull, alive, callback_state);
	}

	/* a scan given by a parallel build is ended here too, as heap's is (heapam_index_build_range_scan()) */
	table_endscan(scan);
	if (tombs)
		pfree(tombs);
	ExecDropSingleTupleTableSlot(slot);
	FreeExecutorState(estate);
	/* these may have been pointing to the now-gone estate */
	index_info->ii_ExpressionsState = NIL;
	index_info->ii_PredicateState = NULL;
	return reltuples;
}

void
ch_index_validate_scan(Relation table_rel, Relation index_rel, IndexInfo *index_info, Snapshot snapshot,
					   ValidateIndexState *state)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("CREATE INDEX CONCURRENTLY is not supported on tables of the ClickHouse engines yet"),
			 errhint("Use CREATE INDEX.")));
}
