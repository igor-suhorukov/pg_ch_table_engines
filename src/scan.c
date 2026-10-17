/*-------------------------------------------------------------------------
 *
 * scan.c
 *	  Scans of a table of an engine (ch_table_engines.md §2.8): the parts a
 *	  snapshot sees, read by the engine library a batch at a time, a row at a
 *	  time into a virtual slot; parallel scans share out the parts; a row
 *	  fetched by its TID.
 *
 *	  A scan reads units: a part each, or, in a parallel scan, runs of
 *	  CH_UNIT_MARKS of a part's granules, numbered in the parts' order, which
 *	  every participant derives alike from the same snapshot and takes
 *	  through an atomic counter.  A plain scan reads every column; ChScan
 *	  and vexec's source name the columns they need and may give a filter,
 *	  which ClickHouse's analysis prunes parts and granules by (filter.c): a
 *	  unit is read as the granules the filter keeps.  A scan that needs no
 *	  column reads nothing: its rows are the granules' counts.
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
#include "pgstat.h"
#include "port/atomics.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "chte.h"
#include "scan.h"

/* The granules of a unit of a parallel scan: a million rows at 8,192 rows a granule */
#define CH_UNIT_MARKS 128

typedef struct ChParallelScan
{
	ParallelTableScanDescData base;
	pg_atomic_uint32 next_unit;
} ChParallelScan;

typedef struct ChScanDescData
{
	TableScanDescData base;
	ChTable    *table;
	List	   *parts;			/* ChPart, the snapshot's */
	int			nparts;
	uint32	   *unit_start;		/* parallel: each part's first unit; NULL in a serial scan */
	uint32		nunits;
	uint32		next_unit;		/* serial */

	/* what is read: the columns, in the order set, the row id last when read */
	int			ncols;
	int		   *attnos;			/* per column read: the attribute's index, -1 the row id */
	char	   *columns;		/* their names, for the library */
	chte_vector *vectors;
	bool	   *missing;		/* per column read: not in the current part */
	bool		all_columns;	/* every column and the row id, as getnextslot reads */
	chte_filter *filter;		/* NULL: every granule */
	ResourceOwner filter_owner;
	ChUnitCheck unit_check;		/* NULL: every unit is read */
	void	   *unit_check_arg;

	/* the claimed unit */
	ChPart	   *part;			/* NULL: none */
	uint32		unit_begin;		/* marks [begin, end) of the part; end 0 its last */
	uint32		unit_end;
	bool		unit_whole;
	bool		selected;		/* its granules worked out */
	chte_mark_range *ranges;
	size_t		nranges;
	size_t		ranges_cap;
	uint64		unit_rows;		/* rows of the granules kept */
	uint64		count_left;		/* a scan of no column: rows of the unit not yet handed */
	chte_reader *reader;
	ResourceOwner reader_owner;
	uint32		nrows;			/* of the batch */
	uint32		row;			/* the next row of it */

	ChScanCounts counts;
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
		ch_handle_closing(scan->reader_owner, CH_HANDLE_READER, scan->reader);
		ch_engine()->read_end(scan->reader);
		scan->reader = NULL;
	}
	scan->nrows = 0;
	scan->row = 0;
}

/* The claimed unit is done with */
static void
release_unit(ChScanDesc scan)
{
	end_reader(scan);
	scan->part = NULL;
	scan->selected = false;
	scan->count_left = 0;
}

/* The units of a parallel scan: each part's runs of CH_UNIT_MARKS granules */
static void
setup_units(ChScanDesc scan)
{
	ListCell   *lc;
	int			i = 0;

	scan->nparts = list_length(scan->parts);
	if (scan->base.rs_parallel == NULL)
	{
		scan->nunits = scan->nparts;
		return;
	}
	scan->unit_start = palloc(sizeof(uint32) * (scan->nparts + 1));
	scan->nunits = 0;
	foreach(lc, scan->parts)
	{
		ChPart	   *p = lfirst(lc);
		uint32		n = p->marks > CH_UNIT_MARKS ? (p->marks + CH_UNIT_MARKS - 1) / CH_UNIT_MARKS : 1;

		scan->unit_start[i++] = scan->nunits;
		scan->nunits += n;
	}
	scan->unit_start[i] = scan->nunits;
}

static bool claim_one_unit(ChScanDesc scan);

/* Takes the next unit the scan's unit check keeps, passing over the others: false at the end */
static bool
claim_unit(ChScanDesc scan)
{
	while (claim_one_unit(scan))
	{
		ChScanUnit	unit;

		if (scan->unit_check == NULL)
			return true;
		unit.part = scan->part;
		unit.whole = scan->unit_whole && scan->filter == NULL;
		unit.rows = 0;
		if (scan->unit_check(scan->unit_check_arg, &unit))
			return true;
		scan->counts.units_passed++;
		release_unit(scan);
	}
	return false;
}

static bool
claim_one_unit(ChScanDesc scan)
{
	uint32		u;
	int			p;

	Assert(scan->part == NULL);
	if (scan->base.rs_parallel)
	{
		ChParallelScan *ps = (ChParallelScan *) scan->base.rs_parallel;

		u = pg_atomic_fetch_add_u32(&ps->next_unit, 1);
	}
	else
		u = scan->next_unit++;
	if (u >= scan->nunits)
		return false;

	if (scan->unit_start == NULL)
	{
		p = (int) u;
		scan->unit_begin = 0;
		scan->unit_end = 0;
		scan->unit_whole = true;
	}
	else
	{
		int			lo = 0;
		int			hi = scan->nparts - 1;
		ChPart	   *part;
		uint32		k;

		/* the part whose units hold u: the last one starting at u or before */
		while (lo < hi)
		{
			int			mid = (lo + hi + 1) / 2;

			if (scan->unit_start[mid] <= u)
				lo = mid;
			else
				hi = mid - 1;
		}
		p = lo;
		part = list_nth(scan->parts, p);
		k = u - scan->unit_start[p];
		scan->unit_whole = scan->unit_start[p + 1] - scan->unit_start[p] == 1;
		scan->unit_begin = scan->unit_whole ? 0 : k * CH_UNIT_MARKS;
		scan->unit_end = scan->unit_whole || (k + 1) * CH_UNIT_MARKS >= (uint32) part->marks ?
			0 : (k + 1) * CH_UNIT_MARKS;
	}
	scan->part = list_nth(scan->parts, p);
	scan->selected = false;
	scan->counts.units++;
	if (scan->table->options.storage == CH_STORAGE_PAGES)
		ch_pages_attach(scan->table, scan->part);
	return true;
}

/* The claimed unit's granules the filter keeps, and their rows */
static void
select_unit(ChScanDesc scan)
{
	const ChEngine *eng = ch_engine();

	if (scan->selected)
		return;
	if (scan->filter == NULL && scan->unit_whole)
	{
		/* the whole part: no call */
		scan->nranges = 0;
		scan->unit_rows = scan->part->rows;
		scan->counts.marks += scan->part->marks;
		scan->counts.marks_read += scan->part->marks;
		scan->selected = true;
		return;
	}
	for (;;)
	{
		size_t		n;

		CH_CALL("analyzing a part",
				eng->part_select(scan->table->handle, scan->part->name, scan->filter, scan->unit_begin,
								 scan->unit_end, scan->ranges, scan->ranges_cap, &n, &scan->unit_rows, CH_ERR));
		if (n <= scan->ranges_cap)
		{
			scan->nranges = n;
			break;
		}
		scan->ranges = scan->ranges ? repalloc(scan->ranges, sizeof(chte_mark_range) * n) :
			MemoryContextAlloc(scan->scan_cxt, sizeof(chte_mark_range) * n);
		scan->ranges_cap = n;
	}
	{
		uint32		end = scan->unit_end ? scan->unit_end : (uint32) scan->part->marks;

		scan->counts.marks += end - scan->unit_begin;
		for (size_t i = 0; i < scan->nranges; i++)
			scan->counts.marks_read += scan->ranges[i].end - scan->ranges[i].begin;
	}
	if (scan->unit_whole && scan->nranges == 0)
		scan->counts.parts_pruned++;
	scan->selected = true;
}

static void
begin_reader(ChScanDesc scan)
{
	const ChEngine *eng = ch_engine();

	scan->reader_owner = ch_handle_reserve();
	CH_CALL("reading a part",
			eng->read_begin(scan->table->handle, scan->part->name, scan->columns,
							scan->nranges > 0 ? scan->ranges : NULL, scan->nranges, &scan->reader, CH_ERR));
	ch_handle_opened(scan->reader_owner, CH_HANDLE_READER, scan->reader);
	/* a part written before a column was added has none: its rows take the column's missing value */
	for (int c = 0; c < scan->ncols; c++)
		scan->missing[c] = !eng->read_present(scan->reader, c);
}

/* The next batch, from this unit or the next; false at the end */
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
			release_unit(scan);
			continue;
		}
		if (scan->count_left > 0)
		{
			/* no column to read: the granules' rows, a granule's worth at a time */
			uint32		n = (uint32) Min(scan->count_left, (uint64) 8192);

			scan->count_left -= n;
			scan->nrows = n;
			scan->row = 0;
			if (scan->count_left == 0)
			{
				/* the unit's last rows: the next call takes the next unit */
				scan->part = NULL;
				scan->selected = false;
			}
			return true;
		}
		if (scan->part == NULL && !claim_unit(scan))
			return false;
		select_unit(scan);
		if (scan->unit_rows == 0)
		{
			release_unit(scan);
			continue;
		}
		if (scan->ncols == 0)
		{
			scan->count_left = scan->unit_rows;
			continue;
		}
		begin_reader(scan);
	}
}

/* The columns a scan reads, the row id among them if asked, and their names */
static void
set_columns(ChScanDesc scan, int n, const int *attnos)
{
	StringInfoData names;

	if (scan->attnos)
		pfree(scan->attnos);
	if (scan->vectors)
		pfree(scan->vectors);
	if (scan->missing)
		pfree(scan->missing);
	scan->ncols = n;
	scan->attnos = palloc(sizeof(int) * Max(n, 1));
	scan->vectors = palloc0(sizeof(chte_vector) * Max(n, 1));
	scan->missing = palloc0(sizeof(bool) * Max(n, 1));
	initStringInfo(&names);
	for (int c = 0; c < n; c++)
	{
		scan->attnos[c] = attnos[c];
		if (c > 0)
			appendStringInfoString(&names, ", ");
		if (attnos[c] < 0)
			appendStringInfoString(&names, CH_ROWID_COLUMN);
		else
			appendStringInfo(&names, "c%d", attnos[c] + 1);
	}
	if (scan->columns)
		pfree(scan->columns);
	scan->columns = names.data;
}

/* Every column of the scan's table, then the row id: what getnextslot reads */
static void
setup_all_columns(ChScanDesc scan)
{
	ChTable    *t = scan->table;
	int		   *attnos = palloc(sizeof(int) * (t->natts + 1));
	int			n = 0;

	for (int i = 0; i < t->natts; i++)
		if (t->columns[i].kind != CH_COL_DROPPED)
			attnos[n++] = i;
	attnos[n++] = -1;
	set_columns(scan, n, attnos);
	scan->all_columns = true;
	pfree(attnos);
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
	release_unit(scan);
	/* in the scan's memory, not the caller's: getnextslot runs in a per-tuple context */
	old = MemoryContextSwitchTo(scan->scan_cxt);
	scan->table = scan->private_table = ch_table_private(scan->base.rs_rd, &scan->table->storage,
														 slot->tts_tupleDescriptor);
	setup_all_columns(scan);
	MemoryContextSwitchTo(old);
	if (!scan->base.rs_parallel)
		scan->next_unit = 0;
}

static void
fill_slot(ChScanDesc scan, TupleTableSlot *slot)
{
	uint32		row = scan->row++;
	ChTable    *t = scan->table;
	MemoryContext old;
	bool		have_rowid = false;
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
		{
			rowid = ((const uint64 *) scan->vectors[c].values)[row];
			have_rowid = true;
		}
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
	if (have_rowid)
		ItemPointerSet(&slot->tts_tid, CH_ROWID_TO_BLOCK(rowid), CH_ROWID_TO_OFFSET(rowid));
	else
		ItemPointerSetInvalid(&slot->tts_tid);
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
	setup_units(scan);
	scan->counts.parts = scan->nparts;
	/* SERIALIZABLE: the relation's predicate lock, and the parts' writers it cannot see */
	if (snapshot && IsMVCCSnapshot(snapshot) && CheckForSerializableConflictOutNeeded(rel, snapshot))
	{
		PredicateLockRelation(rel, snapshot);
		ch_parts_conflict_out(rel, &t->storage, snapshot);
	}
	scan->batch_cxt = AllocSetContextCreate(CurrentMemoryContext, "ch scan batch", ALLOCSET_DEFAULT_SIZES);
	setup_all_columns(scan);

	if (flags & SO_TYPE_ANALYZE)
	{
		ListCell   *lc;

		foreach(lc, scan->parts)
			scan->total_rows += ((ChPart *) lfirst(lc))->rows;
		scan->total_blocks = RelationGetNumberOfBlocks(rel);
	}
	/* as heap's initscan() counts a sequential scan */
	if (flags & SO_TYPE_SEQSCAN)
		pgstat_count_heap_scan(rel);

	if (flags & SO_TEMP_SNAPSHOT)
		Assert(snapshot != NULL);
	return &scan->base;
}

void
ch_scan_end(TableScanDesc sscan)
{
	ChScanDesc	scan = (ChScanDesc) sscan;

	release_unit(scan);
	if (scan->filter)
	{
		ch_handle_closing(scan->filter_owner, CH_HANDLE_FILTER, scan->filter);
		ch_engine()->filter_free(scan->filter);
		scan->filter = NULL;
	}
	if (scan->private_table)
		ch_table_private_close(scan->private_table);
	if (scan->pinned)
		ch_table_unpin(scan->pinned);
	MemoryContextDelete(scan->batch_cxt);
	if (scan->base.rs_flags & SO_TEMP_SNAPSHOT)
		UnregisterSnapshot(scan->base.rs_snapshot);
	pfree(scan->attnos);
	pfree(scan->vectors);
	pfree(scan->missing);
	pfree(scan);
}

void
ch_scan_rescan(TableScanDesc sscan, ScanKeyData *key, bool set_params,
			   bool allow_strat, bool allow_sync, bool allow_pagemode)
{
	ChScanDesc	scan = (ChScanDesc) sscan;

	release_unit(scan);
	scan->next_unit = 0;
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
	if (scan->all_columns)
		fit_slot(scan, slot);
	if (scan->row >= scan->nrows && !next_batch(scan))
	{
		ExecClearTuple(slot);
		return false;
	}
	fill_slot(scan, slot);
	/* as heap's getnextslot counts each row it returns */
	pgstat_count_heap_getnext(scan->base.rs_rd);
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
	pg_atomic_init_u32(&ps->next_unit, 0);
	return sizeof(ChParallelScan);
}

void
ch_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	ChParallelScan *ps = (ChParallelScan *) pscan;

	pg_atomic_write_u32(&ps->next_unit, 0);
}

/* ---- what ChScan and vexec's source ask of a scan (scan.h) ---- */

void
ch_scan_set_columns(TableScanDesc sscan, int n, const AttrNumber *attnums, bool rowid)
{
	ChScanDesc	scan = (ChScanDesc) sscan;
	MemoryContext old = MemoryContextSwitchTo(scan->scan_cxt);
	int		   *attnos = palloc(sizeof(int) * (n + 1));
	int			k = 0;

	Assert(scan->part == NULL);
	for (int i = 0; i < n; i++)
	{
		if (attnums[i] < 1 || attnums[i] > scan->table->natts ||
			scan->table->columns[attnums[i] - 1].kind == CH_COL_DROPPED)
			elog(ERROR, "attribute %d of \"%s\" cannot be read", attnums[i],
				 RelationGetRelationName(scan->base.rs_rd));
		attnos[k++] = attnums[i] - 1;
	}
	if (rowid)
		attnos[k++] = -1;
	set_columns(scan, k, attnos);
	scan->all_columns = false;
	scan->fitted = true;
	pfree(attnos);
	MemoryContextSwitchTo(old);
}

bool
ch_scan_set_filter(TableScanDesc sscan, List *quals)
{
	ChScanDesc	scan = (ChScanDesc) sscan;

	Assert(scan->part == NULL && scan->filter == NULL);
	scan->filter_owner = ch_handle_reserve();
	scan->filter = ch_filter_make(scan->table, quals);
	if (scan->filter)
		ch_handle_opened(scan->filter_owner, CH_HANDLE_FILTER, scan->filter);
	return scan->filter != NULL;
}

bool
ch_scan_next_batch(TableScanDesc sscan, ChScanBatch *batch)
{
	ChScanDesc	scan = (ChScanDesc) sscan;

	CHECK_FOR_INTERRUPTS();
	scan->row = scan->nrows;
	if (!next_batch(scan))
		return false;
	batch->nrows = scan->nrows;
	batch->ncols = scan->ncols;
	batch->attnos = scan->attnos;
	batch->vectors = scan->vectors;
	batch->missing = scan->missing;
	batch->reader = scan->reader;
	scan->row = scan->nrows;
	return true;
}

void
ch_scan_set_unit_check(TableScanDesc sscan, ChUnitCheck check, void *arg)
{
	ChScanDesc	scan = (ChScanDesc) sscan;

	scan->unit_check = check;
	scan->unit_check_arg = arg;
}

bool
ch_scan_claim_unit(TableScanDesc sscan, ChScanUnit *unit)
{
	ChScanDesc	scan = (ChScanDesc) sscan;

	if (scan->reader || scan->count_left > 0)
		return false;			/* inside a unit: its batches come first */
	if (scan->part == NULL && !claim_unit(scan))
		return false;
	select_unit(scan);
	unit->part = scan->part;
	unit->whole = scan->unit_whole && scan->filter == NULL;
	unit->rows = scan->unit_rows;
	return true;
}

void
ch_scan_pass_unit(TableScanDesc sscan)
{
	ChScanDesc	scan = (ChScanDesc) sscan;

	Assert(scan->part != NULL && scan->reader == NULL);
	scan->counts.units_passed++;
	release_unit(scan);
}

ChTable *
ch_scan_table(TableScanDesc sscan)
{
	return ((ChScanDesc) sscan)->table;
}

const ChScanCounts *
ch_scan_counts(TableScanDesc sscan)
{
	return &((ChScanDesc) sscan)->counts;
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
		if (scan->reader == NULL && scan->part == NULL && !scan->base.rs_parallel)
		{
			/* between parts: pass over whole parts the target lies beyond */
			while (scan->next_unit < scan->nunits)
			{
				ChPart	   *p = list_nth(scan->parts, scan->next_unit);

				if (scan->position + p->rows > target)
					break;
				scan->position += p->rows;
				scan->next_unit++;
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
		scan.scan_cxt = CurrentMemoryContext;
		setup_units(&scan);
		scan.batch_cxt = AllocSetContextCreate(CurrentMemoryContext, "ch fetch", ALLOCSET_DEFAULT_SIZES);
		setup_all_columns(&scan);
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
		release_unit(&scan);
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

/* ---- estimates (scan.h) ---- */

void
ch_estimate(Relation rel, Snapshot snapshot, int n, const AttrNumber *attnums, bool rowid, List *quals,
			double *rows, double *bytes, double *raw)
{
	const ChEngine *eng;
	ChTable    *t = ch_table(rel);
	SnapshotData nonvacuumable;
	List	   *parts;
	ListCell   *lc;
	StringInfoData names;
	chte_filter *filter = NULL;
	ResourceOwner owner = NULL;

	*rows = 0;
	*bytes = 0;
	if (raw)
		*raw = 0;
	parts = ch_parts_visible(&t->storage, ch_parts_snapshot(rel, snapshot, &nonvacuumable));
	if (parts == NIL)
		return;
	eng = ch_engine();
	initStringInfo(&names);
	for (int i = 0; i < n; i++)
		if (attnums[i] >= 1 && attnums[i] <= t->natts && t->columns[attnums[i] - 1].kind != CH_COL_DROPPED)
			appendStringInfo(&names, "%sc%d", names.len > 0 ? ", " : "", attnums[i]);
	if (rowid)
		appendStringInfo(&names, "%s" CH_ROWID_COLUMN, names.len > 0 ? ", " : "");

	ch_table_pin(t);
	if (quals != NIL)
	{
		owner = ch_handle_reserve();
		filter = ch_filter_make(t, quals);
		if (filter)
			ch_handle_opened(owner, CH_HANDLE_FILTER, filter);
	}
	foreach(lc, parts)
	{
		ChPart	   *p = lfirst(lc);
		uint64		kept = (uint64) p->rows;

		if (t->options.storage == CH_STORAGE_PAGES)
			ch_pages_attach(t, p);
		if (filter)
		{
			size_t		nranges;

			CH_CALL("analyzing a part",
					eng->part_select(t->handle, p->name, filter, 0, 0, NULL, 0, &nranges, &kept, CH_ERR));
		}
		*rows += kept;
		if (names.len > 0 && kept > 0 && p->rows > 0)
		{
			uint64		compressed;
			uint64		uncompressed;

			CH_CALL("sizing a part's columns",
					eng->part_column_bytes(t->handle, p->name, names.data, &compressed, &uncompressed, CH_ERR));
			*bytes += (double) compressed * kept / p->rows;
			if (raw)
				*raw += (double) uncompressed * kept / p->rows;
		}
	}
	if (filter)
	{
		ch_handle_closing(owner, CH_HANDLE_FILTER, filter);
		eng->filter_free(filter);
	}
	ch_table_unpin(t);
	pfree(names.data);
}
