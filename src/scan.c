/*-------------------------------------------------------------------------
 *
 * scan.c
 *	  Scans of a table of an engine (ch_table_engines.md §2.8): the parts a
 *	  snapshot sees, read by the engine library a batch at a time, a row at a
 *	  time into a virtual slot; parallel scans share out the parts; a row
 *	  fetched by its TID.
 *
 *	  A scan reads units: a part each, or, in a parallel scan, runs of a
 *	  part's granules, numbered in the parts' order, which every participant
 *	  derives alike from the same snapshot and takes through an atomic
 *	  counter.  A run is the parts' granules over CH_UNITS, between
 *	  CH_UNIT_MIN_MARKS and CH_UNIT_MAX_MARKS, so that the participants end
 *	  near one another: with a million rows a unit, 10M rows made 11 units,
 *	  and of a Gather's 7 participants the last ran twice as long as the
 *	  average.  A plain scan reads every column; ChScan
 *	  and vexec's source name the columns they need and may give a filter,
 *	  which ClickHouse's analysis prunes parts and granules by (filter.c): a
 *	  unit is read as the granules the filter keeps.  A scan that needs no
 *	  column reads nothing: its rows are the granules' counts.
 *
 *	  Deleted rows (§2.10): a scan takes the row ids of the deletions and
 *	  updates its snapshot sees once, as it begins (tombstones.c), and passes
 *	  over the rows they name; a unit of a part with any of them in its row
 *	  id range reads the row ids with its columns to find them.  SnapshotAny
 *	  sees every row, as heap's does: index builds and rewrites judge each
 *	  row's tombstones themselves.
 *
 *	  A row by its TID: the parts that may hold it asked for its offset
 *	  through their _pg_rowid projection, and the granule holding it read
 *	  (the engine library's chte_part_locate and chte_read_rows).  A bitmap
 *	  scan's TID block is read the same way, its row ids a part at a time.
 *	  Where a running scan that reads every column has just returned the
 *	  row -- an UPDATE's old row, which ModifyTable fetches by TID, a DELETE's
 *	  for RETURNING and triggers, a lock's -- the row is taken from its batch.
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
#include "nodes/tidbitmap.h"
#include "pgstat.h"
#include "lib/ilist.h"
#include "port/atomics.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "chte.h"
#include "scan.h"
#include "tombstones.h"

/*
 * The units of a parallel scan: about CH_UNITS of them, each a run of at
 * least CH_UNIT_MIN_MARKS granules -- 64K rows at 8,192 rows a granule, so
 * that a unit's reader is begun no more often than that -- and at most
 * CH_UNIT_MAX_MARKS, a million rows
 */
#define CH_UNITS 64
#define CH_UNIT_MIN_MARKS 8
#define CH_UNIT_MAX_MARKS 128

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
	uint32		unit_marks;		/* parallel: the granules of a unit */
	uint32		nunits;
	uint32		next_unit;		/* serial */

	/* what is read: the columns, in the order set, the row id last when read */
	int			ncols;
	int		   *attnos;			/* per column read: the attribute's index, -1 the row id */
	char	   *columns;		/* their names, for the library */
	char	   *columns_rowid;	/* the same, and the row id where it is not among them */
	int			asked_rowid;	/* the row id's column among them, or -1 */
	chte_vector *vectors;		/* ncols + 1: the row id last where the reader adds it */
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

	/* deletions the snapshot sees (tombstones.c), and the claimed unit's share */
	bool		apply_deleted;	/* not for SnapshotAny */
	uint64	   *dead;			/* their row ids, ascending */
	int			ndead;
	int			dead_lo;		/* the claimed unit's part's: dead[dead_lo, dead_hi) */
	int			dead_hi;
	int			rowid_col;		/* the reader's row id column, or -1 */
	uint64		*visible;		/* ch_scan_next_batch(): the batch's live rows as bits, or NULL */

	/* a bitmap scan: the rows of the current TID block, materialized */
	MemoryContext bm_cxt;
	HeapTuple  *bm_rows;
	int			bm_nrows;
	int			bm_pos;

	/* among the running scans, for a fetch of the row it has just returned */
	struct RunningScan *running;
	bool		whole_row;		/* it reads every column and the row id */

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
 * The running scans of this backend (fetch_from_scan()), each entry in
 * TopMemoryContext and owned by the resource owner the scan began under: a
 * statement's error frees its portal's executor memory, the scan's, before
 * the resource owners are released (portalmem.c, AtAbort_Portals()), so the
 * release touches the entry alone, never the scan.
 */
typedef struct RunningScan
{
	dlist_node	node;
	ChScanDescData *scan;
	ResourceOwner owner;
} RunningScan;

static dlist_head running_scans = DLIST_STATIC_INIT(running_scans);

static void
scan_running(ChScanDesc scan)
{
	RunningScan *r = MemoryContextAlloc(TopMemoryContext, sizeof(RunningScan));

	r->scan = scan;
	r->owner = ch_handle_reserve();
	dlist_push_head(&running_scans, &r->node);
	scan->running = r;
	ch_handle_opened(r->owner, CH_HANDLE_SCAN, r);
}

static void
scan_done(ChScanDesc scan)
{
	RunningScan *r = scan->running;

	if (r == NULL)
		return;
	ch_handle_closing(r->owner, CH_HANDLE_SCAN, r);
	dlist_delete(&r->node);
	pfree(r);
	scan->running = NULL;
}

/* A scan that never ended, at its resource owner's release: its entry goes, the scan untouched */
void
ch_scan_forget(void *ptr)
{
	RunningScan *r = ptr;

	dlist_delete(&r->node);
	pfree(r);
}

/*
 * The snapshot the parts are read with: a scan without one (ANALYZE) and
 * SnapshotAny read the parts the latest snapshot sees, the catalog's.  An
 * aborted writer's part is not among them; one another transaction is
 * writing cannot be either, under the locks a scan of every row takes
 * (index builds, rewrites) or for a row the caller has already seen (a
 * fetch); and a part folded away (VACUUM) holds no row that its successor,
 * which is among them, does not, but those deleted for every snapshot.
 */
Snapshot
ch_parts_snapshot(Relation rel, Snapshot snapshot, SnapshotData *buf)
{
	if (snapshot == NULL || snapshot->snapshot_type == SNAPSHOT_ANY)
		return NULL;			/* the catalog's, registered by systable_beginscan() */
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
	scan->dead_lo = scan->dead_hi = 0;
}

/* The first of the sorted ids at or above v */
static int
lower_bound(const uint64 *ids, int n, uint64 v)
{
	int			lo = 0;
	int			hi = n;

	while (lo < hi)
	{
		int			mid = lo + (hi - lo) / 2;

		if (ids[mid] < v)
			lo = mid + 1;
		else
			hi = mid;
	}
	return lo;
}

/* The deletions within the claimed part's row id range */
static void
unit_deletions(ChScanDesc scan)
{
	if (scan->ndead == 0)
	{
		scan->dead_lo = scan->dead_hi = 0;
		return;
	}
	scan->dead_lo = lower_bound(scan->dead, scan->ndead, (uint64) scan->part->min_rowid);
	scan->dead_hi = lower_bound(scan->dead, scan->ndead, (uint64) scan->part->max_rowid + 1);
}

/* Whether row r of the batch is deleted for the scan's snapshot */
static inline bool
row_dead(ChScanDesc scan, uint32 r)
{
	uint64		rowid;

	if (scan->dead_lo == scan->dead_hi)
		return false;
	rowid = ((const uint64 *) scan->vectors[scan->rowid_col].values)[r];
	return ch_rowid_in(scan->dead + scan->dead_lo, scan->dead_hi - scan->dead_lo, rowid);
}

/*
 * The deletions and updates the scan's snapshot sees, as it begins: a scan
 * without a snapshot (ANALYZE) takes the catalog's, and SnapshotAny none.
 */
static void
load_deletions(ChScanDesc scan, Snapshot snapshot)
{
	scan->apply_deleted = snapshot == NULL || snapshot->snapshot_type != SNAPSHOT_ANY;
	scan->dead = NULL;
	scan->ndead = 0;
	if (scan->apply_deleted)
		scan->dead = ch_tombstones_visible(&scan->table->storage, snapshot, 0, PG_INT64_MAX, &scan->ndead);
}

/*
 * The units of a parallel scan: each part's runs of unit_marks granules, the
 * parts' granules over CH_UNITS within its bounds -- the same in every
 * participant, whose snapshot sees the same parts
 */
static void
setup_units(ChScanDesc scan)
{
	ListCell   *lc;
	uint64		marks = 0;
	int			i = 0;

	scan->nparts = list_length(scan->parts);
	if (scan->base.rs_parallel == NULL)
	{
		scan->nunits = scan->nparts;
		return;
	}
	foreach(lc, scan->parts)
		marks += ((ChPart *) lfirst(lc))->marks;
	scan->unit_marks = (uint32) Min(Max((marks + CH_UNITS - 1) / CH_UNITS, CH_UNIT_MIN_MARKS),
									CH_UNIT_MAX_MARKS);
	scan->unit_start = palloc(sizeof(uint32) * (scan->nparts + 1));
	scan->nunits = 0;
	foreach(lc, scan->parts)
	{
		ChPart	   *p = lfirst(lc);
		uint32		n = p->marks > scan->unit_marks ?
			(p->marks + scan->unit_marks - 1) / scan->unit_marks : 1;

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
		unit.deletions = scan->dead_lo < scan->dead_hi;
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
		scan->unit_begin = scan->unit_whole ? 0 : k * scan->unit_marks;
		scan->unit_end = scan->unit_whole || (k + 1) * scan->unit_marks >= (uint32) part->marks ?
			0 : (k + 1) * scan->unit_marks;
	}
	scan->part = list_nth(scan->parts, p);
	scan->selected = false;
	scan->counts.units++;
	unit_deletions(scan);
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
	else if (!scan->unit_whole && scan->unit_begin == 0 && scan->filter != NULL)
	{
		/*
		 * A part a parallel scan reads as several units: the participant
		 * that claims its first one asks whether the filter keeps any of
		 * the part's granules, so that the parts pruned are counted once,
		 * as a serial scan counts them
		 */
		size_t		n;
		uint64		rows;

		CH_CALL("analyzing a part",
				eng->part_select(scan->table->handle, scan->part->name, scan->filter, 0, 0,
								 NULL, 0, &n, &rows, CH_ERR));
		if (n == 0)
			scan->counts.parts_pruned++;
	}
	scan->selected = true;
}

static void
begin_reader(ChScanDesc scan)
{
	const ChEngine *eng = ch_engine();
	const char *columns = scan->columns;

	/* a unit with deletions reads the row ids, to know its rows; one of no column, to count them */
	scan->rowid_col = scan->asked_rowid;
	if ((scan->dead_lo < scan->dead_hi || scan->ncols == 0) && scan->asked_rowid < 0)
	{
		columns = scan->columns_rowid;
		scan->rowid_col = scan->ncols;
	}
	scan->reader_owner = ch_handle_reserve();
	CH_CALL("reading a part",
			eng->read_begin(scan->table->handle, scan->part->name, columns,
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
				int			nvec = scan->rowid_col == scan->ncols ? scan->ncols + 1 : scan->ncols;

				MemoryContextReset(scan->batch_cxt);
				for (int c = 0; c < nvec; c++)
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
		/* no column to read: the granules' rows, where no deletion leaves some out */
		if (scan->ncols == 0 && scan->dead_lo == scan->dead_hi)
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
	scan->vectors = palloc0(sizeof(chte_vector) * (n + 1));
	scan->missing = palloc0(sizeof(bool) * Max(n, 1));
	scan->asked_rowid = -1;
	initStringInfo(&names);
	for (int c = 0; c < n; c++)
	{
		scan->attnos[c] = attnos[c];
		if (c > 0)
			appendStringInfoString(&names, ", ");
		if (attnos[c] < 0)
		{
			appendStringInfoString(&names, CH_ROWID_COLUMN);
			scan->asked_rowid = c;
		}
		else
			appendStringInfo(&names, "c%d", attnos[c] + 1);
	}
	if (scan->columns)
		pfree(scan->columns);
	if (scan->columns_rowid)
		pfree(scan->columns_rowid);
	scan->columns = names.data;
	scan->columns_rowid = scan->asked_rowid >= 0 ? pstrdup(names.data) :
		psprintf("%s%s" CH_ROWID_COLUMN, names.data, n > 0 ? ", " : "");
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
	scan->whole_row = true;
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

/* Row row of the batch into slot, its values' memory in cxt */
static void
fill_row(ChScanDesc scan, uint32 row, TupleTableSlot *slot, MemoryContext cxt)
{
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
	old = MemoryContextSwitchTo(cxt);
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

static void
fill_slot(ChScanDesc scan, TupleTableSlot *slot)
{
	fill_row(scan, scan->row++, slot, scan->batch_cxt);
}

/*
 * The row a running scan of the relation has just returned, where the scan
 * reads every column: an UPDATE's old row, fetched by TID right after its
 * scan returned it (nodeModifyTable.c, ExecModifyTable()), and a DELETE's
 * or a lock's, needs no read of the parts.
 */
static bool
fetch_from_scan(Relation rel, uint64 rowid, TupleTableSlot *slot)
{
	dlist_iter	it;

	dlist_foreach(it, &running_scans)
	{
		ChScanDesc	s = dlist_container(RunningScan, node, it.cur)->scan;
		uint32		r;

		if (RelationGetRelid(s->base.rs_rd) != RelationGetRelid(rel) || !s->whole_row ||
			s->private_table != NULL || s->part == NULL || s->row == 0 || s->row > s->nrows ||
			s->rowid_col < 0)
			continue;
		r = s->row - 1;
		if (((const uint64 *) s->vectors[s->rowid_col].values)[r] != rowid)
			continue;
		fill_row(s, r, slot, CurrentMemoryContext);
		ExecMaterializeSlot(slot);
		return true;
	}
	return false;
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
	load_deletions(scan, snapshot);
	/* SERIALIZABLE: the relation's predicate lock, and the writers of parts and deletions it cannot see */
	if (snapshot && IsMVCCSnapshot(snapshot) && CheckForSerializableConflictOutNeeded(rel, snapshot))
	{
		PredicateLockRelation(rel, snapshot);
		ch_parts_conflict_out(rel, &t->storage, snapshot);
		ch_tombstones_conflict_out(rel, &t->storage, snapshot);
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
	scan_running(scan);
	return &scan->base;
}

void
ch_scan_end(TableScanDesc sscan)
{
	ChScanDesc	scan = (ChScanDesc) sscan;

	scan_done(scan);
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
	if (scan->bm_cxt)
		MemoryContextDelete(scan->bm_cxt);
	if (scan->base.rs_flags & SO_TEMP_SNAPSHOT)
		UnregisterSnapshot(scan->base.rs_snapshot);
	if (scan->dead)
		pfree(scan->dead);
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
	scan->bm_nrows = scan->bm_pos = 0;
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
	for (;;)
	{
		if (scan->row >= scan->nrows && !next_batch(scan))
		{
			ExecClearTuple(slot);
			return false;
		}
		/* deleted for the snapshot: passed over */
		if (row_dead(scan, scan->row))
		{
			scan->row++;
			continue;
		}
		break;
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
	/* every column and the row id: a fetch of its rows may take them from it */
	{
		int			live = 0;

		for (int i = 0; i < scan->table->natts; i++)
			if (scan->table->columns[i].kind != CH_COL_DROPPED)
				live++;
		scan->whole_row = rowid && n == live;
	}
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
	batch->visible = NULL;
	batch->nvisible = scan->nrows;
	if (scan->dead_lo < scan->dead_hi)
	{
		uint64	   *bits = MemoryContextAllocZero(scan->batch_cxt, sizeof(uint64) * ((scan->nrows + 63) / 64));
		uint32		live = 0;

		for (uint32 r = 0; r < scan->nrows; r++)
			if (!row_dead(scan, r))
			{
				bits[r / 64] |= UINT64CONST(1) << (r % 64);
				live++;
			}
		if (live < scan->nrows)
		{
			batch->visible = bits;
			batch->nvisible = live;
		}
	}
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
	unit->deletions = scan->dead_lo < scan->dead_hi;
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

	if (scan->all_columns)
		fit_slot(scan, slot);
	while (scan->position < scan->block_end)
	{
		if (scan->row >= scan->nrows && !next_batch(scan))
		{
			scan->block_end = scan->position;
			return false;
		}
		scan->position++;
		if (row_dead(scan, scan->row))
		{
			scan->row++;
			*deadrows += 1;
			continue;
		}
		fill_slot(scan, slot);
		*liverows += 1;
		return true;
	}
	return false;
}

/*
 * The rows of a part with the given row ids (ascending), those it holds:
 * their offsets through its _pg_rowid projection, then the granules holding
 * them read, each row found materialized into rows[] in its offset's order,
 * its row id into ids[].  Returns how many.
 */
static int
read_rows_by_id(Relation rel, ChTable *t, ChPart *part, const uint64 *rowids, int n, TupleDesc desc,
				HeapTuple *rows, uint64 *ids)
{
	const ChEngine *eng = ch_engine();
	uint64	   *offsets = palloc(sizeof(uint64) * n);
	uint64	   *found = palloc(sizeof(uint64) * n);
	int			nfound = 0;
	ChScanDescData scan;
	ResourceOwner owner;
	TupleTableSlot *slot;
	int			out = 0;

	if (t->options.storage == CH_STORAGE_PAGES)
		ch_pages_attach(t, part);
	CH_CALL("finding rows by their TIDs", eng->part_locate(t->handle, part->name, rowids, n, offsets, CH_ERR));
	for (int i = 0; i < n; i++)
		if (offsets[i] != PG_UINT64_MAX)
			found[nfound++] = offsets[i];
	if (nfound == 0)
	{
		pfree(offsets);
		pfree(found);
		return 0;
	}
	/* the offsets ascending, as the reader wants them */
	qsort(found, nfound, sizeof(uint64), ch_compare_uint64);

	memset(&scan, 0, sizeof(scan));
	scan.base.rs_rd = rel;
	scan.table = t;
	scan.fitted = true;
	scan.scan_cxt = CurrentMemoryContext;
	scan.batch_cxt = AllocSetContextCreate(CurrentMemoryContext, "ch fetch", ALLOCSET_DEFAULT_SIZES);
	setup_all_columns(&scan);
	scan.part = part;
	scan.rowid_col = scan.asked_rowid;
	owner = ch_handle_reserve();
	CH_CALL("reading rows by their TIDs",
			eng->read_rows(t->handle, part->name, scan.columns, found, nfound, &scan.reader, CH_ERR));
	ch_handle_opened(owner, CH_HANDLE_READER, scan.reader);
	scan.reader_owner = owner;
	for (int c = 0; c < scan.ncols; c++)
		scan.missing[c] = !eng->read_present(scan.reader, c);

	slot = MakeSingleTupleTableSlot(desc, &ch_slot_ops);
	for (;;)
	{
		uint32		nrows;

		CH_CALL("reading rows by their TIDs", eng->read_next(scan.reader, &nrows, CH_ERR));
		if (nrows == 0)
			break;
		MemoryContextReset(scan.batch_cxt);
		for (int c = 0; c < scan.ncols; c++)
			CH_CALL("reading rows by their TIDs", eng->read_vector(scan.reader, c, &scan.vectors[c], CH_ERR));
		scan.nrows = nrows;
		for (scan.row = 0; scan.row < nrows && out < nfound;)
		{
			ids[out] = ((const uint64 *) scan.vectors[scan.asked_rowid].values)[scan.row];
			fill_slot(&scan, slot);
			rows[out++] = ExecCopySlotHeapTuple(slot);
		}
	}
	ExecDropSingleTupleTableSlot(slot);
	end_reader(&scan);
	MemoryContextDelete(scan.batch_cxt);
	pfree(scan.attnos);
	pfree(scan.vectors);
	pfree(scan.missing);
	pfree(offsets);
	pfree(found);
	return out;
}

/*
 * Whether the deletions of a row leave it to the snapshot: SnapshotAny sees
 * every row; SnapshotDirty a row being deleted, its deleter's xid in xmax, as
 * HeapTupleSatisfiesDirty() does; others a row no deletion they see names.
 */
static bool
row_survives(ChTable *t, uint64 rowid, Snapshot snapshot)
{
	if (snapshot && snapshot->snapshot_type == SNAPSHOT_ANY)
		return true;
	if (snapshot && snapshot->snapshot_type == SNAPSHOT_DIRTY)
	{
		ChRowState	st;

		snapshot->xmin = snapshot->xmax = InvalidTransactionId;
		ch_row_state(&t->storage, rowid, &st);
		if (st.status == CH_ROW_MODIFIED || st.status == CH_ROW_SELF_MODIFIED)
			return false;
		if (st.status == CH_ROW_BEING_MODIFIED && !st.lock_only)
			snapshot->xmax = st.xid;
		return true;
	}
	return !ch_tombstone_visible(&t->storage, snapshot, rowid, NULL);
}

/*
 * The row the last fetch by TID read from a part, for a fetch of it with
 * SnapshotAny right after: an UPDATE's or DELETE's of the row an index scan
 * has just returned (ExecModifyTable(), nodeModifyTable.c:4744-4747), a row
 * lock's.  Reading a row is reading a granule of every column, so the second
 * fetch would cost as much as the first.  A row id's values never change once
 * it is given (§2.6) -- a fold or a merge writes them again as they were --
 * so the copy is the row's for its relation, storage and descriptor.
 */
static struct
{
	Oid			relid;
	RelFileNumber relfilenumber;
	int			natts;
	uint64		rowid;
	HeapTuple	tuple;			/* in TopMemoryContext */
} last_fetch;

static void
remember_fetch(Relation rel, uint64 rowid, TupleDesc desc, HeapTuple row)
{
	MemoryContext old = MemoryContextSwitchTo(TopMemoryContext);

	if (last_fetch.tuple)
		heap_freetuple(last_fetch.tuple);
	last_fetch.tuple = heap_copytuple(row);
	MemoryContextSwitchTo(old);
	last_fetch.relid = RelationGetRelid(rel);
	last_fetch.relfilenumber = rel->rd_locator.relNumber;
	last_fetch.natts = desc->natts;
	last_fetch.rowid = rowid;
}

static bool
fetch_from_last(Relation rel, uint64 rowid, TupleTableSlot *slot)
{
	if (last_fetch.tuple == NULL || last_fetch.rowid != rowid || last_fetch.relid != RelationGetRelid(rel) ||
		last_fetch.relfilenumber != rel->rd_locator.relNumber ||
		last_fetch.natts != slot->tts_tupleDescriptor->natts)
		return false;
	ExecForceStoreHeapTuple(heap_copytuple(last_fetch.tuple), slot, true);
	return true;
}

/*
 * A row by its TID, visible to the snapshot: from the write buffer, or from
 * the part the snapshot sees that holds it (§2.8 rule 7).
 */
bool
ch_fetch_row(Relation rel, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot)
{
	uint64		rowid;
	ChTable    *t;
	SnapshotData nonvacuumable;
	List	   *parts;
	ListCell   *lc;
	bool		found = false;

	if (!ItemPointerIsValid(tid) || ItemPointerGetOffsetNumberNoCheck(tid) > CH_ROWS_PER_BLOCK)
		return false;
	rowid = CH_TID_TO_ROWID(tid);
	if (snapshot && IsMVCCSnapshot(snapshot))
		PredicateLockRelation(rel, snapshot);
	if (ch_buffer_fetch(rel, rowid, snapshot, slot))
		return true;
	/* SnapshotAny: the row whatever its deletions, which a running scan or the last fetch may have at hand */
	if (snapshot && snapshot->snapshot_type == SNAPSHOT_ANY)
	{
		if (fetch_from_scan(rel, rowid, slot))
			return true;
		if (fetch_from_last(rel, rowid, slot))
		{
			slot->tts_tid = *tid;
			slot->tts_tableOid = RelationGetRelid(rel);
			return true;
		}
	}

	t = ch_table(rel);
	ch_table_pin(t);
	PG_TRY();
	{
		if (row_survives(t, rowid, snapshot))
		{
			parts = ch_parts_visible(&t->storage, ch_parts_snapshot(rel, snapshot, &nonvacuumable));
			foreach(lc, parts)
			{
				ChPart	   *p = lfirst(lc);
				HeapTuple	row;
				uint64		id;

				if ((int64) rowid < p->min_rowid || (int64) rowid > p->max_rowid)
					continue;
				if (read_rows_by_id(rel, t, p, &rowid, 1, slot->tts_tupleDescriptor, &row, &id) == 1)
				{
					remember_fetch(rel, rowid, slot->tts_tupleDescriptor, row);
					ExecForceStoreHeapTuple(row, slot, true);
					slot->tts_tid = *tid;
					slot->tts_tableOid = RelationGetRelid(rel);
					found = true;
					break;
				}
			}
		}
	}
	PG_FINALLY();
	{
		ch_table_unpin(t);
	}
	PG_END_TRY();
	return found;
}

/*
 * A bitmap scan's next row: the TIDs of the bitmap's next block, exact or
 * the whole block where it is lossy, those no deletion the snapshot sees
 * names, read from the parts holding them, materialized for the block.
 */
bool
ch_scan_bitmap_next_tuple(TableScanDesc sscan, TupleTableSlot *slot, bool *recheck, uint64 *lossy_pages,
						  uint64 *exact_pages)
{
	ChScanDesc	scan = (ChScanDesc) sscan;

	while (scan->bm_pos >= scan->bm_nrows)
	{
		TBMIterateResult res;
		OffsetNumber offsets[TBM_MAX_TUPLES_PER_PAGE];
		uint64		rowids[TBM_MAX_TUPLES_PER_PAGE];
		uint64		ids[TBM_MAX_TUPLES_PER_PAGE];
		int			n = 0;
		ListCell   *lc;
		MemoryContext old;

		CHECK_FOR_INTERRUPTS();
		if (!tbm_iterate(&scan->base.st.rs_tbmiterator, &res))
			return false;
		*recheck = res.recheck;
		if (res.lossy)
		{
			(*lossy_pages)++;
			for (int i = 0; i < CH_ROWS_PER_BLOCK; i++)
				rowids[n++] = (uint64) res.blockno * CH_ROWS_PER_BLOCK + i;
		}
		else
		{
			int			noff = tbm_extract_page_tuple(&res, offsets, TBM_MAX_TUPLES_PER_PAGE);

			(*exact_pages)++;
			for (int i = 0; i < noff; i++)
				if (offsets[i] >= 1 && offsets[i] <= CH_ROWS_PER_BLOCK)
					rowids[n++] = (uint64) res.blockno * CH_ROWS_PER_BLOCK + offsets[i] - 1;
		}
		/* the rows no deletion the snapshot sees names */
		if (scan->ndead > 0)
		{
			int			k = 0;

			for (int i = 0; i < n; i++)
				if (!ch_rowid_in(scan->dead, scan->ndead, rowids[i]))
					rowids[k++] = rowids[i];
			n = k;
		}
		if (scan->bm_cxt == NULL)
			scan->bm_cxt = AllocSetContextCreate(scan->scan_cxt, "ch bitmap block", ALLOCSET_DEFAULT_SIZES);
		MemoryContextReset(scan->bm_cxt);
		old = MemoryContextSwitchTo(scan->bm_cxt);
		scan->bm_rows = palloc(sizeof(HeapTuple) * TBM_MAX_TUPLES_PER_PAGE);
		scan->bm_nrows = 0;
		scan->bm_pos = 0;
		foreach(lc, scan->parts)
		{
			ChPart	   *p = lfirst(lc);
			uint64		lo;
			uint64		hi;
			int			first;
			int			last;
			int			got;

			if (n == 0)
				break;
			lo = (uint64) p->min_rowid;
			hi = (uint64) p->max_rowid;
			if (rowids[n - 1] < lo || rowids[0] > hi)
				continue;
			first = lower_bound(rowids, n, lo);
			last = lower_bound(rowids, n, hi + 1);
			if (first >= last)
				continue;
			got = read_rows_by_id(scan->base.rs_rd, scan->table, p, rowids + first, last - first,
								  RelationGetDescr(scan->base.rs_rd), scan->bm_rows + scan->bm_nrows, ids);
			/* rows found leave the list: no other part holds them */
			if (got > 0)
			{
				int			k = 0;

				qsort(ids, got, sizeof(uint64), ch_compare_uint64);
				for (int i = 0; i < n; i++)
					if (!ch_rowid_in(ids, got, rowids[i]))
						rowids[k++] = rowids[i];
				n = k;
			}
			scan->bm_nrows += got;
		}
		MemoryContextSwitchTo(old);
	}
	ExecForceStoreHeapTuple(scan->bm_rows[scan->bm_pos], slot, false);
	slot->tts_tid = scan->bm_rows[scan->bm_pos]->t_self;
	slot->tts_tableOid = RelationGetRelid(scan->base.rs_rd);
	scan->bm_pos++;
	pgstat_count_heap_fetch(scan->base.rs_rd);
	return true;
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
	/* pinned from the start: the catalog's read may accept an invalidation of the table */
	ch_table_pin(t);
	parts = ch_parts_visible(&t->storage, ch_parts_snapshot(rel, snapshot, &nonvacuumable));
	if (parts == NIL)
	{
		ch_table_unpin(t);
		return;
	}
	eng = ch_engine();
	initStringInfo(&names);
	for (int i = 0; i < n; i++)
		if (attnums[i] >= 1 && attnums[i] <= t->natts && t->columns[attnums[i] - 1].kind != CH_COL_DROPPED)
			appendStringInfo(&names, "%sc%d", names.len > 0 ? ", " : "", attnums[i]);
	if (rowid)
		appendStringInfo(&names, "%s" CH_ROWID_COLUMN, names.len > 0 ? ", " : "");

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
