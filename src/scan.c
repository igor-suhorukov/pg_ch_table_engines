/*-------------------------------------------------------------------------
 *
 * scan.c
 *	  Scans of a table of an engine (ch_table_engines.md §2.8): the parts a
 *	  snapshot sees, read by the engine library a batch at a time, a row at a
 *	  time into a virtual slot; parallel scans share out the parts; a row
 *	  fetched by its TID.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/parallel.h"
#include "access/heaptoast.h"
#include "access/htup_details.h"
#include "access/relscan.h"
#include "access/table.h"
#include "access/tableam.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "chte.h"

typedef struct ChParallelScan
{
	ParallelTableScanDescData base;
	pg_atomic_uint32 next_part;
} ChParallelScan;

typedef struct ChScanDescData
{
	TableScanDescData base;
	ChTable    *table;
	List	   *parts;			/* ChPart, the snapshot's */
	int			next_part;		/* not parallel */
	chte_reader *reader;
	ResourceOwner reader_owner;
	uint32		nrows;			/* of the batch */
	uint32		row;			/* the next row of it */
	int			ncols;			/* columns read: the table's, then the row id */
	int		   *attnos;			/* per column read: the attribute's index, -1 the row id */
	chte_vector *vectors;
	bool	   *missing;		/* per column read: not in the current part */
	MemoryContext batch_cxt;	/* the slot's values of a batch */
	SnapshotData nonvacuumable;
	/* ANALYZE: block b stands for rows [b * rows / blocks, (b + 1) * rows / blocks) */
	uint64		position;		/* rows of the parts passed so far */
	uint64		block_end;		/* the current block's last row + 1 */
	uint64		total_rows;
	BlockNumber total_blocks;
	bool		fitted;			/* the table checked against the first slot */
	MemoryContext scan_cxt;		/* the scan's: where scan_begin ran */
	ChTable    *private_table;	/* where the slots' descriptor is not the relation's */
	ChTable    *pinned;			/* the cached table, pinned while the scan runs */
} ChScanDescData;

typedef ChScanDescData *ChScanDesc;

/*
 * The snapshot the catalog is read with: a scan without one (ANALYZE) reads
 * with the transaction's, and SnapshotAny with the rows no one can see any
 * more left out, so that an aborted writer's part is not read.
 */
Snapshot
ch_parts_snapshot(Relation rel, Snapshot snapshot, SnapshotData *buf)
{
	if (snapshot == NULL)
		return NULL;			/* the catalog's, registered by systable_beginscan() */
	if (snapshot->snapshot_type == SNAPSHOT_ANY)
	{
		InitNonVacuumableSnapshot(*buf, GlobalVisTestFor(rel));
		return buf;
	}
	return snapshot;
}

static void
end_reader(ChScanDesc scan)
{
	if (scan->reader)
	{
		ch_handle_closing(scan->reader_owner, true, scan->reader);
		ch_engine()->read_end(scan->reader);
		scan->reader = NULL;
	}
	scan->nrows = 0;
	scan->row = 0;
}

/* The next part to read: from the shared counter in a parallel scan */
static ChPart *
next_part(ChScanDesc scan)
{
	int			i;

	if (scan->base.rs_parallel)
	{
		ChParallelScan *ps = (ChParallelScan *) scan->base.rs_parallel;

		i = (int) pg_atomic_fetch_add_u32(&ps->next_part, 1);
	}
	else
		i = scan->next_part++;
	if (i >= list_length(scan->parts))
		return NULL;
	return list_nth(scan->parts, i);
}

/* The next batch, from this part or the next; false at the end */
static bool
next_batch(ChScanDesc scan)
{
	const ChEngine *eng = ch_engine();

	for (;;)
	{
		if (scan->reader)
		{
			uint32		n;

			CH_CALL("reading a part", eng->read_next(scan->reader, &n, CH_ERR));
			if (n > 0)
			{
				MemoryContextReset(scan->batch_cxt);
				for (int c = 0; c < scan->ncols; c++)
					CH_CALL("reading a part", eng->read_vector(scan->reader, c, &scan->vectors[c], CH_ERR));
				scan->nrows = n;
				scan->row = 0;
				return true;
			}
			end_reader(scan);
		}
		{
			ChPart	   *part = next_part(scan);

			if (part == NULL)
				return false;
			if (scan->table->options.storage == CH_STORAGE_PAGES)
				ch_pages_attach(scan->table, part);
			scan->reader_owner = ch_handle_reserve();
			CH_CALL("reading a part",
					eng->read_begin(scan->table->handle, part->name, scan->table->all_columns, &scan->reader, CH_ERR));
			ch_handle_opened(scan->reader_owner, true, scan->reader);
			/* a part written before a column was added has none: its rows take the column's missing value */
			for (int c = 0; c < scan->ncols; c++)
				scan->missing[c] = !eng->read_present(scan->reader, c);
		}
	}
}

/* The columns a scan reads: every column of its table, then the row id */
static void
setup_columns(ChScanDesc scan)
{
	ChTable    *t = scan->table;
	int			n = 0;

	for (int i = 0; i < t->natts; i++)
		if (t->columns[i].kind != CH_COL_DROPPED)
			n++;
	scan->ncols = n + 1;
	scan->attnos = palloc(sizeof(int) * scan->ncols);
	scan->vectors = palloc0(sizeof(chte_vector) * scan->ncols);
	scan->missing = palloc0(sizeof(bool) * scan->ncols);
	n = 0;
	for (int i = 0; i < t->natts; i++)
		if (t->columns[i].kind != CH_COL_DROPPED)
			scan->attnos[n++] = i;
	scan->attnos[n] = -1;
}

/*
 * The first slot tells whether the relation's table fits it: a rewrite's
 * slots have the descriptor the old storage's parts were written with.
 */
static void
fit_slot(ChScanDesc scan, TupleTableSlot *slot)
{
	MemoryContext old;

	if (scan->fitted)
		return;
	scan->fitted = true;
	if (ch_desc_matches(scan->base.rs_rd, slot->tts_tupleDescriptor))
		return;
	end_reader(scan);
	/* in the scan's memory, not the caller's: getnextslot runs in a per-tuple context */
	old = MemoryContextSwitchTo(scan->scan_cxt);
	scan->table = scan->private_table = ch_table_private(scan->base.rs_rd, &scan->table->storage,
														 slot->tts_tupleDescriptor);
	pfree(scan->attnos);
	pfree(scan->vectors);
	pfree(scan->missing);
	setup_columns(scan);
	MemoryContextSwitchTo(old);
	if (!scan->base.rs_parallel)
		scan->next_part = 0;
}

static void
fill_slot(ChScanDesc scan, TupleTableSlot *slot)
{
	uint32		row = scan->row++;
	ChTable    *t = scan->table;
	MemoryContext old;
	uint64		rowid = 0;

	ExecClearTuple(slot);
	for (int i = 0; i < slot->tts_tupleDescriptor->natts; i++)
	{
		slot->tts_values[i] = (Datum) 0;
		slot->tts_isnull[i] = true;
	}
	old = MemoryContextSwitchTo(scan->batch_cxt);
	for (int c = 0; c < scan->ncols; c++)
	{
		int			att = scan->attnos[c];

		if (att < 0)
			rowid = ((const uint64 *) scan->vectors[c].values)[row];
		else if (att >= slot->tts_tupleDescriptor->natts)
			continue;
		else if (scan->missing[c])
			slot->tts_values[att] = getmissingattr(slot->tts_tupleDescriptor, att + 1, &slot->tts_isnull[att]);
		else
			ch_vector_to_datum(&t->columns[att], &scan->vectors[c], row,
							   &slot->tts_values[att], &slot->tts_isnull[att], NULL);
	}
	MemoryContextSwitchTo(old);
	ExecStoreVirtualTuple(slot);
	ItemPointerSet(&slot->tts_tid, CH_ROWID_TO_BLOCK(rowid), CH_ROWID_TO_OFFSET(rowid));
	slot->tts_tableOid = RelationGetRelid(scan->base.rs_rd);
}

TableScanDesc
ch_scan_begin(Relation rel, Snapshot snapshot, int nkeys, ScanKeyData *key,
			  ParallelTableScanDesc pscan, uint32 flags)
{
	ChScanDesc	scan = palloc0(sizeof(ChScanDescData));
	ChTable    *t;

	scan->base.rs_rd = rel;
	scan->scan_cxt = CurrentMemoryContext;
	scan->base.rs_snapshot = snapshot;
	scan->base.rs_nkeys = nkeys;
	scan->base.rs_key = key;
	scan->base.rs_flags = flags;
	scan->base.rs_parallel = pscan;

	/* rows this backend has buffered that the snapshot sees become a part first */
	if (pscan == NULL || !IsParallelWorker())
		ch_buffer_flush_for_scan(rel, snapshot);

	scan->table = t = ch_table(rel);
	ch_table_pin(t);
	scan->pinned = t;
	scan->parts = ch_parts_visible(&t->storage, ch_parts_snapshot(rel, snapshot, &scan->nonvacuumable));
	/* SERIALIZABLE: the relation's predicate lock, and the parts' writers it cannot see */
	if (snapshot && IsMVCCSnapshot(snapshot) && CheckForSerializableConflictOutNeeded(rel, snapshot))
	{
		PredicateLockRelation(rel, snapshot);
		ch_parts_conflict_out(rel, &t->storage, snapshot);
	}
	scan->batch_cxt = AllocSetContextCreate(CurrentMemoryContext, "ch scan batch", ALLOCSET_DEFAULT_SIZES);
	setup_columns(scan);


	if (flags & SO_TYPE_ANALYZE)
	{
		ListCell   *lc;

		foreach(lc, scan->parts)
			scan->total_rows += ((ChPart *) lfirst(lc))->rows;
		scan->total_blocks = RelationGetNumberOfBlocks(rel);
	}

	if (flags & SO_TEMP_SNAPSHOT)
		Assert(snapshot != NULL);
	return &scan->base;
}

void
ch_scan_end(TableScanDesc sscan)
{
	ChScanDesc	scan = (ChScanDesc) sscan;

	end_reader(scan);
	if (scan->private_table)
		ch_table_private_close(scan->private_table);
	if (scan->pinned)
		ch_table_unpin(scan->pinned);
	MemoryContextDelete(scan->batch_cxt);
	if (scan->base.rs_flags & SO_TEMP_SNAPSHOT)
		UnregisterSnapshot(scan->base.rs_snapshot);
	pfree(scan->attnos);
	pfree(scan->vectors);
	pfree(scan);
}

void
ch_scan_rescan(TableScanDesc sscan, ScanKeyData *key, bool set_params,
			   bool allow_strat, bool allow_sync, bool allow_pagemode)
{
	ChScanDesc	scan = (ChScanDesc) sscan;

	end_reader(scan);
	scan->next_part = 0;
	scan->position = 0;
	scan->block_end = 0;
}

bool
ch_scan_getnextslot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
{
	ChScanDesc	scan = (ChScanDesc) sscan;

	if (ScanDirectionIsBackward(direction))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("backward scans are not supported on tables of the ClickHouse engines")));
	CHECK_FOR_INTERRUPTS();
	fit_slot(scan, slot);
	if (scan->row >= scan->nrows && !next_batch(scan))
	{
		ExecClearTuple(slot);
		return false;
	}
	fill_slot(scan, slot);
	return true;
}

Size
ch_parallelscan_estimate(Relation rel)
{
	return sizeof(ChParallelScan);
}

Size
ch_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	ChParallelScan *ps = (ChParallelScan *) pscan;

	ps->base.phs_locator = rel->rd_locator;
	ps->base.phs_syncscan = false;
	pg_atomic_init_u32(&ps->next_part, 0);
	return sizeof(ChParallelScan);
}

void
ch_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	ChParallelScan *ps = (ChParallelScan *) pscan;

	pg_atomic_write_u32(&ps->next_part, 0);
}

/*
 * ANALYZE: block b of the relation's size stands for the rows
 * [b * R / B, (b + 1) * R / B) of its parts in their order, R rows in all and
 * B blocks (§2.13): a sampled block's rows are read, the others' passed over,
 * a whole part at a time where they cover it.
 */
static uint64
block_first_row(ChScanDesc scan, BlockNumber b)
{
	if (scan->total_blocks == 0)
		return 0;
	return (uint64) ((uint128) b * scan->total_rows / scan->total_blocks);
}

/* Passes over rows until scan->position is target */
static void
skip_to(ChScanDesc scan, uint64 target)
{
	while (scan->position < target)
	{
		uint32		in_batch = scan->nrows - scan->row;

		if (in_batch > 0)
		{
			uint64		n = Min((uint64) in_batch, target - scan->position);

			scan->row += (uint32) n;
			scan->position += n;
			continue;
		}
		if (scan->reader == NULL && !scan->base.rs_parallel)
		{
			/* between parts: pass over whole parts the target lies beyond */
			while (scan->next_part < list_length(scan->parts))
			{
				ChPart	   *p = list_nth(scan->parts, scan->next_part);

				if (scan->position + p->rows > target)
					break;
				scan->position += p->rows;
				scan->next_part++;
			}
			if (scan->position >= target)
				break;
		}
		if (!next_batch(scan))
			break;
	}
}

bool
ch_scan_analyze_next_block(TableScanDesc sscan, ReadStream *stream)
{
	ChScanDesc	scan = (ChScanDesc) sscan;
	BufferAccessStrategy strategy;
	BlockNumber blkno = read_stream_next_block(stream, &strategy);

	if (blkno == InvalidBlockNumber)
		return false;
	skip_to(scan, block_first_row(scan, blkno));
	scan->block_end = block_first_row(scan, blkno + 1);
	return true;
}

bool
ch_scan_analyze_next_tuple(TableScanDesc sscan, double *liverows, double *deadrows, TupleTableSlot *slot)
{
	ChScanDesc	scan = (ChScanDesc) sscan;

	if (scan->position >= scan->block_end)
		return false;
	if (!ch_scan_getnextslot(sscan, ForwardScanDirection, slot))
	{
		scan->block_end = scan->position;
		return false;
	}
	scan->position++;
	*liverows += 1;
	return true;
}

/*
 * A row by its TID: from the write buffer, or from a part whose row id range
 * holds it, read until the row is found.  CH2 finds it through the _pg_rowid
 * projection instead.
 */
bool
ch_fetch_row(Relation rel, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot)
{
	uint64		rowid;
	ChTable    *t;
	SnapshotData nonvacuumable;
	List	   *parts;
	ListCell   *lc;

	if (!ItemPointerIsValid(tid) || ItemPointerGetOffsetNumberNoCheck(tid) > CH_ROWS_PER_BLOCK)
		return false;
	rowid = CH_TID_TO_ROWID(tid);
	if (snapshot && IsMVCCSnapshot(snapshot))
		PredicateLockRelation(rel, snapshot);
	if (ch_buffer_fetch(rel, rowid, snapshot, slot))
		return true;

	t = ch_table(rel);
	parts = ch_parts_visible(&t->storage, ch_parts_snapshot(rel, snapshot, &nonvacuumable));
	ch_table_pin(t);
	foreach(lc, parts)
	{
		ChPart	   *p = lfirst(lc);
		ChScanDescData scan;
		bool		found = false;

		if ((int64) rowid < p->min_rowid || (int64) rowid > p->max_rowid)
			continue;
		memset(&scan, 0, sizeof(scan));
		scan.base.rs_rd = rel;
		scan.table = t;
		scan.fitted = true;
		scan.parts = list_make1(p);
		scan.batch_cxt = AllocSetContextCreate(CurrentMemoryContext, "ch fetch", ALLOCSET_DEFAULT_SIZES);
		scan.ncols = 0;
		for (int i = 0; i < t->natts; i++)
			if (t->columns[i].kind != CH_COL_DROPPED)
				scan.ncols++;
		scan.ncols++;
		scan.attnos = palloc(sizeof(int) * scan.ncols);
		scan.vectors = palloc0(sizeof(chte_vector) * scan.ncols);
		scan.missing = palloc0(sizeof(bool) * scan.ncols);
		{
			int			n = 0;

			for (int i = 0; i < t->natts; i++)
				if (t->columns[i].kind != CH_COL_DROPPED)
					scan.attnos[n++] = i;
			scan.attnos[n] = -1;
		}
		while (!found && next_batch(&scan))
		{
			const uint64 *ids = scan.vectors[scan.ncols - 1].values;

			for (uint32 r = 0; r < scan.nrows; r++)
				if (ids[r] == rowid)
				{
					scan.row = r;
					fill_slot(&scan, slot);
					ExecMaterializeSlot(slot);
					found = true;
					break;
				}
		}
		end_reader(&scan);
		MemoryContextDelete(scan.batch_cxt);
		if (found)
		{
			ch_table_unpin(t);
			return true;
		}
	}
	ch_table_unpin(t);
	return false;
}
