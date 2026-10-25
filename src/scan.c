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
 *	  A fetch reads only the columns its slot's node reads, where fetch.c
 *	  knows them, and an index scan's fetches keep what they read for the
 *	  next ones: the snapshot's parts and deletions, located row ids, the
 *	  last granule decoded (VC16).
 *
 *	  A serial scan may read its parts in the order of the table's key,
 *	  where their ranges of its first column are apart, and so hand its rows
 *	  in that order (VC4).
 *
 *	  vexec's source may read its columns in steps (VC2): each granule's
 *	  first step's columns -- the row ids among them -- as it comes, and a
 *	  later step's only where the source asks, the engine library leaving
 *	  the granules it is not asked for undecoded (chte_read_begin_steps).
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
#include "catalog/pg_type.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "nodes/tidbitmap.h"
#include "pgstat.h"
#include "common/hashfn.h"
#include "lib/ilist.h"
#include "port/atomics.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/typcache.h"
#include "utils/varlena.h"

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
	/* an external table's (§2.17): the leader's listing of its files, which every participant reads */
	uint32		listing_size;
	char		listing[FLEXIBLE_ARRAY_MEMBER];
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
	bool	   *keep_dict;		/* per column read: handed as its dictionary (VC5), or NULL */
	chte_dictionary *dicts;		/* per column read: the batch's dictionary, where encoded */
	int			nsteps;			/* VC2: the steps they are read in; 0, all at once */
	uint8	   *col_step;		/* per column read: its step, the row id's 0 */
	uint8	   *step_read;		/* per step: read of the current batch */
	bool		all_columns;	/* every column and the row id, as getnextslot reads */
	chte_filter *filter;		/* NULL: every granule */
	ResourceOwner filter_owner;
	List	   *filter_quals;	/* the quals it was made of */
	int			njoin_keys;		/* VC3: a hash join's keys, ANDed with them */
	ScanKeyData *join_keys;
	MemoryContext join_cxt;
	bool		filter_stale;	/* made anew before the next unit is claimed */
	ChUnitCheck unit_check;		/* NULL: every unit is read */
	void	   *unit_check_arg;
	ChGranuleSkip granule_skip; /* F7: NULL, every granule the filter keeps is read */
	void	   *granule_skip_arg;

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
	bool		one_unit;		/* V8e: the claimed unit's batches alone, then an end */
	bool		skip_done;		/* W21: the unit's granules the caller passes over are cut already (ch_scan_granules_cut()) */
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
	int			dead_at;		/* row_dead()'s place among them, the last row's */
	int			rowid_col;		/* the reader's row id column, or -1 */
	uint64		*visible;		/* ch_scan_next_batch(): the batch's live rows as bits, or NULL */

	/* a bitmap scan: the rows of the current TID block, materialized */
	MemoryContext bm_cxt;
	HeapTuple  *bm_rows;
	int			bm_nrows;
	int			bm_pos;
	struct FetchCols *bm_cols;	/* the columns they read (VC16), once known */

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
	ChListing  *listing;		/* an external table's files, pinned while the scan runs (§2.17) */
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
	scan->skip_done = false;
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
	scan->dead_at = scan->dead_lo;
}

/*
 * Whether row r of the batch is deleted for the scan's snapshot: the dead
 * ids searched from the last row's place, galloping forward where the rows
 * ascend -- a table of its own files' part's always (§2.18), a MergeTree
 * part's runs of a key -- and from the range's start where they do not
 */
static inline bool
row_dead(ChScanDesc scan, uint32 r)
{
	const uint64 *dead = scan->dead;
	uint64		rowid;
	int			d;

	if (scan->dead_lo == scan->dead_hi)
		return false;
	rowid = ((const uint64 *) scan->vectors[scan->rowid_col].values)[r];
	d = scan->dead_at;
	if (d < scan->dead_lo || d > scan->dead_hi || (d > scan->dead_lo && dead[d - 1] >= rowid))
		d = scan->dead_lo + lower_bound(dead + scan->dead_lo, scan->dead_hi - scan->dead_lo, rowid);
	else
	{
		int			hi = d;
		int			step = 1;

		/* every one before d is below rowid: the first at or above it, between d and the gallop's last probe */
		while (hi < scan->dead_hi && dead[hi] < rowid)
		{
			d = hi + 1;
			hi += step;
			step *= 2;
		}
		hi = Min(hi, scan->dead_hi);
		d += lower_bound(dead + d, hi - d, rowid);
	}
	scan->dead_at = d;
	return d < scan->dead_hi && dead[d] == rowid;
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
static void refresh_filter(ChScanDesc scan);
static void skip_granules(ChScanDesc scan);

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
		unit.deletions = scan->dead_lo < scan->dead_hi || scan->part->deletes;
		if (scan->unit_check(scan->unit_check_arg, &unit))
			return true;
		scan->counts.units_passed++;
		release_unit(scan);
	}
	return false;
}

/*
 * A table of its own files (§2.18): its parts' files listed, each part read
 * through its file as an external table's is -- every participant of a
 * parallel scan lists the same parts' files.  As the scan first reads, by
 * its table then: a rewrite's scan reads by the old descriptor (fit_slot()),
 * whose files hold the types the new one may not take.
 */
static void
own_listing(ChScanDesc scan)
{
	if (scan->listing != NULL || !scan->table->own || scan->parts == NIL)
		return;
	scan->listing = ch_own_listing(scan->table, scan->parts);
	ch_listing_pin(scan->listing);
}

static bool
claim_one_unit(ChScanDesc scan)
{
	uint32		u;
	int			p;

	Assert(scan->part == NULL);
	own_listing(scan);
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
	return true;
}

/*
 * What the filter keeps of the claimed unit's part by the part's summary
 * (VC20's B7), as chte_part_select() would by its statistics and its key:
 * CHTE_SELECT_NONE or CHTE_SELECT_ALL, the part then neither attached nor
 * loaded, or CHTE_SELECT_RANGES, chte_part_select()'s to tell.
 */
static int
summary_verdict(ChScanDesc scan)
{
	const ChEngine *eng = ch_engine();
	bytea	   *summary = NULL;
	int			verdict;

	if (scan->filter == NULL || !ch_enable_summary_pruning || eng->filter_summary_select == NULL ||
		scan->part->listing != NULL)
		return CHTE_SELECT_RANGES;
	if (ch_enable_part_summaries)
		summary = ch_part_summary(scan->part);
	CH_CALL("analyzing a part",
			eng->filter_summary_select(scan->filter, scan->part->name, summary ? VARDATA_ANY(summary) : NULL,
									   summary ? VARSIZE_ANY_EXHDR(summary) : 0, &verdict, CH_ERR));
	return verdict;
}

/* The claimed unit's granules the filter keeps, and their rows */
static void
select_unit(ChScanDesc scan)
{
	int			verdict;

	if (scan->selected)
		return;
	verdict = scan->filter == NULL ? CHTE_SELECT_ALL : summary_verdict(scan);
	/*
	 * The whole part: no call -- where the filter keeps every granule by its
	 * summary too (VC29, pg_accel's §3.28 W16), so that a GPU's unit, whose
	 * streams are kept in shared memory, needs no load of the part here
	 */
	if (scan->unit_whole && verdict == CHTE_SELECT_ALL)
	{
		scan->nranges = 0;
		scan->unit_rows = scan->part->rows;
		scan->counts.marks += scan->part->marks;
		scan->counts.marks_read += scan->part->marks;
		scan->selected = true;
		return;
	}
	if (verdict == CHTE_SELECT_NONE)
	{
		/* none of its granules, counted as chte_part_select()'s none would be below */
		scan->nranges = 0;
		scan->unit_rows = 0;
		scan->counts.marks += (scan->unit_end ? scan->unit_end : (uint32) scan->part->marks) - scan->unit_begin;
		if (scan->unit_whole || scan->unit_begin == 0)
			scan->counts.parts_pruned++;
		scan->selected = true;
		return;
	}
	if (scan->table->options.storage == CH_STORAGE_PAGES)
		ch_pages_attach(scan->table, scan->part);
	for (;;)
	{
		size_t		n;

		(void) ch_unit_select(scan->table, scan->part, scan->filter, scan->unit_begin, scan->unit_end,
							  scan->ranges, scan->ranges_cap, &n, &scan->unit_rows);
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

		if (!ch_unit_select(scan->table, scan->part, scan->filter, 0, 0, NULL, 0, &n, &rows))
			scan->counts.parts_pruned++;
	}
	scan->selected = true;
}

/*
 * VC3: the filter made again of the quals and the join's keys given since,
 * for the units claimed from now on
 */
static void
refresh_filter(ChScanDesc scan)
{
	MemoryContext old;

	if (!scan->filter_stale)
		return;
	scan->filter_stale = false;
	if (scan->filter)
	{
		ch_handle_closing(scan->filter_owner, CH_HANDLE_FILTER, scan->filter);
		ch_engine()->filter_free(scan->filter);
		scan->filter = NULL;
	}
	old = MemoryContextSwitchTo(scan->scan_cxt);
	scan->filter_owner = ch_handle_reserve();
	scan->filter = ch_filter_make_keys(scan->table, scan->filter_quals, scan->njoin_keys, scan->join_keys);
	if (scan->filter)
		ch_handle_opened(scan->filter_owner, CH_HANDLE_FILTER, scan->filter);
	MemoryContextSwitchTo(old);
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
	if (scan->table->options.storage == CH_STORAGE_PAGES)
		ch_pages_attach(scan->table, scan->part);
	scan->reader_owner = ch_handle_reserve();
	if (scan->part->listing)
		/* an external table's file, its units; the filter the reader's own pruning's too */
		CH_CALL("reading a file",
				eng->external_read_begin(scan->table->handle, scan->part->listing->handle, (size_t) scan->part->file,
										 scan->filter, columns, scan->nranges > 0 ? scan->ranges : NULL,
										 scan->nranges, &scan->reader, CH_ERR));
	else if (scan->nsteps > 0)
	{
		/* VC2: each column's step, the row id the reader adds in the first */
		uint8	   *steps = palloc0(scan->ncols + 1);

		memcpy(steps, scan->col_step, scan->ncols);
		CH_CALL("reading a part",
				eng->read_begin_steps(scan->table->handle, scan->part->name, columns, steps,
									  scan->nranges > 0 ? scan->ranges : NULL, scan->nranges, &scan->reader,
									  CH_ERR));
		pfree(steps);
	}
	else
		CH_CALL("reading a part",
				eng->read_begin(scan->table->handle, scan->part->name, columns,
								scan->nranges > 0 ? scan->ranges : NULL, scan->nranges, &scan->reader, CH_ERR));
	ch_handle_opened(scan->reader_owner, CH_HANDLE_READER, scan->reader);
	/* a part written before a column was added has none: its rows take the column's missing value */
	for (int c = 0; c < scan->ncols; c++)
		scan->missing[c] = !eng->read_present(scan->reader, c);
	/* the columns left LowCardinality, the row id the reader may add never among them */
	if (scan->keep_dict)
	{
		uint8	   *keep = palloc0(scan->ncols + 1);

		for (int c = 0; c < scan->ncols; c++)
			keep[c] = scan->keep_dict[c] && !scan->missing[c];
		CH_CALL("reading a part", eng->read_keep_dictionaries(scan->reader, keep, scan->ncols + 1, CH_ERR));
		pfree(keep);
	}
}

/* What a scan's reader reads, for its errors: a part, or an external table's file (§2.17) */
static const char *
reading(ChScanDesc scan)
{
	return scan->part && scan->part->listing ? "reading a file" : "reading a part";
}

/* Column c of the reader's current batch: its vector, or its dictionary where kept */
static void
read_column(ChScanDesc scan, int c)
{
	const ChEngine *eng = ch_engine();

	if (c < scan->ncols && scan->keep_dict && scan->keep_dict[c])
	{
		CH_CALL(reading(scan), eng->read_dictionary(scan->reader, c, &scan->dicts[c], CH_ERR));
		if (scan->dicts[c].encoded)
			return;
	}
	CH_CALL(reading(scan), eng->read_vector(scan->reader, c, &scan->vectors[c], CH_ERR));
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

			CH_CALL(reading(scan), eng->read_next(scan->reader, &n, CH_ERR));
			if (n > 0)
			{
				int			nvec = scan->rowid_col == scan->ncols ? scan->ncols + 1 : scan->ncols;

				MemoryContextReset(scan->batch_cxt);
				for (int c = 0; c < nvec; c++)
				{
					/* VC2: a later step's columns are read where it is asked for */
					if (c < scan->ncols && scan->nsteps > 0 && scan->col_step[c] > 0)
						continue;
					read_column(scan, c);
				}
				if (scan->nsteps > 0)
				{
					memset(scan->step_read, 0, scan->nsteps);
					scan->step_read[0] = 1;
				}
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
		if (scan->part == NULL)
		{
			/* V8e: the one unit asked for is read: an end, once */
			if (scan->one_unit)
			{
				scan->one_unit = false;
				return false;
			}
			refresh_filter(scan);
			if (!claim_unit(scan))
				return false;
		}
		select_unit(scan);
		/* F7: a CPU's batches of the unit, the granules the caller passes over left out */
		skip_granules(scan);
		if (scan->unit_rows == 0)
		{
			release_unit(scan);
			continue;
		}
		/*
		 * no column to read: the granules' rows, where no deletion leaves some
		 * out -- nor an Iceberg data file's delete files (§2.17)
		 */
		if (scan->ncols == 0 && scan->dead_lo == scan->dead_hi && !scan->part->deletes)
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
	if (scan->keep_dict)
		pfree(scan->keep_dict);
	if (scan->dicts)
		pfree(scan->dicts);
	if (scan->col_step)
		pfree(scan->col_step);
	if (scan->step_read)
		pfree(scan->step_read);
	scan->keep_dict = NULL;
	scan->dicts = NULL;
	scan->nsteps = 0;
	scan->col_step = NULL;
	scan->step_read = NULL;
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
		else if (scan->dicts && scan->dicts[c].encoded)
			ch_dictionary_to_datum(&t->columns[att], &scan->dicts[c], row,
								   &slot->tts_values[att], &slot->tts_isnull[att]);
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

	if (ch_is_external_rel(rel, NULL) && !ch_is_own_rel(rel))
	{
		/*
		 * An external table's files (§2.17): the statement's listing, or a
		 * parallel scan's leader's, which every participant reads; no row
		 * of them is PostgreSQL's to delete, and SSI has none to track
		 */
		ChParallelScan *ps = (ChParallelScan *) pscan;

		scan->table = t = ch_table(rel);
		ch_table_pin(t);
		scan->pinned = t;
		scan->listing = ps ? ch_external_listing_load(t, ps->listing, ps->listing_size) :
			ch_external_listing(rel, t);
		ch_listing_pin(scan->listing);
		scan->parts = scan->listing->parts;
		setup_units(scan);
		scan->counts.parts = scan->nparts;
	}
	else
	{
		/* rows this backend has buffered that the snapshot sees become a part first */
		if (pscan == NULL || !IsParallelWorker())
			ch_buffer_flush_for_scan(rel, snapshot);

		scan->table = t = ch_table(rel);
		ch_table_pin(t);
		scan->pinned = t;
		scan->parts = ch_parts_to_scan(&t->storage, ch_parts_snapshot(rel, snapshot, &scan->nonvacuumable));
		/* a table of its own files' parts' files are listed as the scan first reads (own_listing()) */
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
	if (scan->listing)
		ch_listing_unpin(scan->listing);
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
	if (scan->keep_dict)
		pfree(scan->keep_dict);
	if (scan->dicts)
		pfree(scan->dicts);
	if (scan->col_step)
		pfree(scan->col_step);
	if (scan->step_read)
		pfree(scan->step_read);
	pfree(scan);
}

void
ch_scan_rescan(TableScanDesc sscan, ScanKeyData *key, bool set_params,
			   bool allow_strat, bool allow_sync, bool allow_pagemode)
{
	ChScanDesc	scan = (ChScanDesc) sscan;

	release_unit(scan);
	scan->next_unit = 0;
	scan->one_unit = false;
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

/* An external table's listing for the running statement, as bytes for a parallel scan's descriptor */
static void
listing_bytes(Relation rel, const void **bytes, size_t *size)
{
	ChTable    *t = ch_table(rel);

	ch_table_pin(t);
	PG_TRY();
	{
		ChListing  *l = ch_external_listing(rel, t);

		CH_CALL("listing a table's files", ch_engine()->external_listing_bytes(l->handle, bytes, size, CH_ERR));
	}
	PG_FINALLY();
	{
		ch_table_unpin(t);
	}
	PG_END_TRY();
}

/* A parallel scan's descriptor: an external table's with its listing (§2.17) */
static Size
parallel_size(Relation rel, const void **bytes, size_t *size)
{
	*bytes = NULL;
	*size = 0;
	if (!ch_is_external_rel(rel, NULL) || ch_is_own_rel(rel))
		return MAXALIGN(sizeof(ChParallelScan));
	listing_bytes(rel, bytes, size);
	return MAXALIGN(add_size(offsetof(ChParallelScan, listing), *size));
}

Size
ch_parallelscan_estimate(Relation rel)
{
	const void *bytes;
	size_t		size;

	return parallel_size(rel, &bytes, &size);
}

Size
ch_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	ChParallelScan *ps = (ChParallelScan *) pscan;
	const void *bytes;
	size_t		size;
	Size		total = parallel_size(rel, &bytes, &size);

	ps->base.phs_locator = rel->rd_locator;
	ps->base.phs_syncscan = false;
	pg_atomic_init_u32(&ps->next_unit, 0);
	ps->listing_size = (uint32) size;
	if (size > 0)
		memcpy(ps->listing, bytes, size);
	return total;
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
	scan->filter_quals = list_copy(quals);
	scan->filter_owner = ch_handle_reserve();
	scan->filter = ch_filter_make(scan->table, quals);
	if (scan->filter)
		ch_handle_opened(scan->filter_owner, CH_HANDLE_FILTER, scan->filter);
	return scan->filter != NULL;
}

/*
 * VC3: a hash join's keys, lent to the scan on its outer side (vexec's
 * set_join_keys()), ANDed with the quals into the filter the units claimed
 * from now on are selected by -- replacing the last given, none where nkeys
 * is 0 -- so that ClickHouse's analysis passes over the parts and granules
 * no inner row meets: by the parts' summaries and statistics, and by the
 * primary key where the keys' column leads it.  A unit claimed before keeps
 * its granules.
 */
void
ch_scan_set_join_keys(TableScanDesc sscan, int nkeys, const ScanKeyData *keys)
{
	ChScanDesc	scan = (ChScanDesc) sscan;
	MemoryContext old;

	if (nkeys <= 0 && scan->njoin_keys == 0)
		return;
	if (scan->join_cxt == NULL)
		scan->join_cxt = AllocSetContextCreate(scan->scan_cxt, "ch join keys", ALLOCSET_SMALL_SIZES);
	MemoryContextReset(scan->join_cxt);
	scan->join_keys = NULL;
	scan->njoin_keys = 0;
	if (nkeys > 0)
	{
		old = MemoryContextSwitchTo(scan->join_cxt);
		scan->join_keys = palloc(sizeof(ScanKeyData) * nkeys);
		for (int k = 0; k < nkeys; k++)
		{
			scan->join_keys[k] = keys[k];
			if (keys[k].sk_flags & SK_SEARCHARRAY)
				scan->join_keys[k].sk_argument = PointerGetDatum(DatumGetArrayTypePCopy(keys[k].sk_argument));
		}
		scan->njoin_keys = nkeys;
		MemoryContextSwitchTo(old);
	}
	scan->filter_stale = true;
}

/*
 * Before the first batch, after the columns: those of keep[] -- LowCardinality
 * columns, by their ChColumn -- handed as their dictionaries.  False where
 * the engine library cannot: every column is then handed as its values.
 */
bool
ch_scan_set_dictionaries(TableScanDesc sscan, const bool *keep)
{
	ChScanDesc	scan = (ChScanDesc) sscan;
	const ChEngine *eng = ch_engine();
	MemoryContext old;
	bool		any = false;

	Assert(scan->part == NULL);
	if (eng->read_keep_dictionaries == NULL)
		return false;
	for (int c = 0; c < scan->ncols; c++)
		if (keep[c] && scan->attnos[c] >= 0 && scan->table->columns[scan->attnos[c]].low_cardinality)
			any = true;
	if (!any)
		return false;
	old = MemoryContextSwitchTo(scan->scan_cxt);
	scan->keep_dict = palloc0(sizeof(bool) * Max(scan->ncols, 1));
	scan->dicts = palloc0(sizeof(chte_dictionary) * Max(scan->ncols, 1));
	for (int c = 0; c < scan->ncols; c++)
		scan->keep_dict[c] = keep[c] && scan->attnos[c] >= 0 &&
			scan->table->columns[scan->attnos[c]].low_cardinality;
	MemoryContextSwitchTo(old);
	return true;
}

/*
 * VC2: before the first batch, after the columns -- and their dictionaries
 * -- the step each attribute set is in, step_of[i] (0 the first, below
 * nsteps), read in steps: each batch the first step's columns and the row
 * ids, ch_scan_read_step() a later one's of the batch.  False where the
 * engine library reads no step: every column is then read with each batch.
 */
bool
ch_scan_set_steps(TableScanDesc sscan, const int *step_of, int nsteps)
{
	ChScanDesc	scan = (ChScanDesc) sscan;
	MemoryContext old;
	int			i = 0;

	Assert(scan->part == NULL);
	/*
	 * Steps are a MergeTree part's granules': an external table's files and a
	 * table of its own files' (§2.18) are read whole by CH7's readers -- the
	 * latter's listing made as the scan first reads, so not here yet
	 */
	if (ch_engine()->read_begin_steps == NULL || nsteps < 2 || scan->all_columns || scan->listing != NULL ||
		scan->table->own)
		return false;
	old = MemoryContextSwitchTo(scan->scan_cxt);
	scan->col_step = palloc0(Max(scan->ncols, 1));
	scan->step_read = palloc0(nsteps);
	for (int c = 0; c < scan->ncols; c++)
		scan->col_step[c] = c == scan->asked_rowid ? 0 : (uint8) step_of[i++];
	scan->nsteps = nsteps;
	MemoryContextSwitchTo(old);
	return true;
}

/* VC2: step `step`'s columns of the batch ch_scan_next_batch() handed last, into it */
void
ch_scan_read_step(TableScanDesc sscan, int step, ChScanBatch *batch)
{
	ChScanDesc	scan = (ChScanDesc) sscan;

	Assert(scan->reader != NULL && step > 0 && step < scan->nsteps);
	if (scan->step_read[step])
		return;
	CH_CALL("reading a part", ch_engine()->read_step(scan->reader, (uint32) step, CH_ERR));
	for (int c = 0; c < scan->ncols; c++)
		if (scan->col_step[c] == step)
			read_column(scan, c);
	scan->step_read[step] = 1;
	batch->vectors = scan->vectors;
	batch->dicts = scan->keep_dict ? scan->dicts : NULL;
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
	batch->dicts = scan->keep_dict ? scan->dicts : NULL;
	batch->reader = scan->reader;
	batch->visible = NULL;
	batch->nvisible = scan->nrows;
	batch->part = scan->part;
	batch->granule = -1;
	batch->granule_last = false;
	/* F7: a part's granule, as the library read it */
	if (scan->reader != NULL && scan->part->listing == NULL && ch_engine()->read_granule != NULL)
	{
		int64_t		g;
		int32_t		last;

		ch_engine()->read_granule(scan->reader, &g, &last);
		batch->granule = g;
		batch->granule_last = last != 0;
	}
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

void
ch_scan_set_granule_skip(TableScanDesc sscan, ChGranuleSkip skip, void *arg)
{
	ChScanDesc	scan = (ChScanDesc) sscan;

	scan->granule_skip = skip;
	scan->granule_skip_arg = arg;
}

/* W21: granules of the claimed unit the caller passes over for the device (pg_accel's W21), as skip_granules() counts its own */
void
ch_scan_note_skipped(TableScanDesc sscan, int64 n)
{
	ChScanDesc	scan = (ChScanDesc) sscan;

	scan->counts.marks_skipped += n;
	scan->counts.marks_read -= n;
}

void
ch_scan_granules_cut(TableScanDesc sscan)
{
	ChScanDesc	scan = (ChScanDesc) sscan;

	Assert(scan->part != NULL);
	scan->skip_done = true;
}

void
ch_scan_drop_unit(TableScanDesc sscan)
{
	ChScanDesc	scan = (ChScanDesc) sscan;

	Assert(scan->part != NULL && scan->reader == NULL);
	release_unit(scan);
}

void
ch_part_unit_id(const ChTable *t, const ChPart *part, uint64 *id)
{
	struct
	{
		Oid			db;
		Oid			spc;
		RelFileNumber rel;
		int64		rows;
		TimestampTz created;
	}			k;

	id[0] = id[1] = 0;
	if (t->storage.backend >= 0 || part->listing != NULL)
		return;
	memset(&k, 0, sizeof(k));
	k.db = t->storage.locator.dbOid;
	k.spc = t->storage.locator.spcOid;
	k.rel = t->storage.locator.relNumber;
	k.rows = part->rows;
	k.created = part->created;
	id[0] = hash_bytes_extended((const unsigned char *) &k, sizeof(k), 0) ^
		hash_bytes_extended((const unsigned char *) part->name, strlen(part->name), 1);
	id[1] = hash_bytes_extended((const unsigned char *) &k, sizeof(k), 2) ^
		hash_bytes_extended((const unsigned char *) part->name, strlen(part->name), 3);
	if (id[0] == 0 && id[1] == 0)
		id[1] = 1;
}

/*
 * F7 (VC30): the claimed unit's granules the filter kept, less those its
 * granule skip passes over -- whole runs of them where none is passed -- and
 * the rows left none where none is left
 */
static void
skip_granules(ChScanDesc scan)
{
	const uint64 *skip;
	uint64		id[2];
	uint32		begin = scan->unit_begin;
	uint32		end = scan->unit_end ? scan->unit_end : (uint32) scan->part->marks;
	chte_mark_range whole;
	const chte_mark_range *from;
	size_t		nfrom;
	chte_mark_range *out;
	size_t		nout = 0;
	int64		passed = 0;

	if (scan->granule_skip == NULL || scan->part->listing != NULL || scan->unit_rows == 0 ||
		scan->part->marks <= 0 || scan->skip_done)
		return;
	ch_part_unit_id(scan->table, scan->part, id);
	if (id[0] == 0 && id[1] == 0)
		return;
	skip = scan->granule_skip(scan->granule_skip_arg, id, (uint32) scan->part->marks, begin, end);
	if (skip == NULL)
		return;
	whole.begin = begin;
	whole.end = end;
	from = scan->nranges > 0 ? scan->ranges : &whole;
	nfrom = scan->nranges > 0 ? scan->nranges : 1;
	/* at most a range for each granule kept, in a copy: the ranges are rewritten in place */
	out = palloc(sizeof(chte_mark_range) * Max(end - begin, 1));
	for (size_t i = 0; i < nfrom; i++)
		for (uint32 m = from[i].begin; m < from[i].end; m++)
		{
			if (skip[m / 64] & (UINT64CONST(1) << (m % 64)))
			{
				passed++;
				continue;
			}
			if (nout > 0 && out[nout - 1].end == m)
				out[nout - 1].end = m + 1;
			else
			{
				out[nout].begin = m;
				out[nout].end = m + 1;
				nout++;
			}
		}
	if (passed > 0)
	{
		if (nout > scan->ranges_cap)
		{
			scan->ranges = scan->ranges ? repalloc(scan->ranges, sizeof(chte_mark_range) * nout) :
				MemoryContextAlloc(scan->scan_cxt, sizeof(chte_mark_range) * nout);
			scan->ranges_cap = nout;
		}
		if (nout > 0)
			memcpy(scan->ranges, out, sizeof(chte_mark_range) * nout);
		scan->nranges = nout;
		/* the rows kept an upper bound, none where no granule is left */
		if (nout == 0)
			scan->unit_rows = 0;
		scan->counts.marks_skipped += passed;
		scan->counts.marks_read -= passed;
	}
	pfree(out);
}

bool
ch_scan_claim_unit(TableScanDesc sscan, ChScanUnit *unit)
{
	ChScanDesc	scan = (ChScanDesc) sscan;

	if (scan->reader || scan->count_left > 0)
		return false;			/* inside a unit: its batches come first */
	if (scan->part == NULL)
	{
		refresh_filter(scan);
		if (!claim_unit(scan))
			return false;
	}
	select_unit(scan);
	unit->part = scan->part;
	unit->whole = scan->unit_whole && scan->filter == NULL;
	unit->rows = scan->unit_rows;
	unit->deletions = scan->dead_lo < scan->dead_hi || scan->part->deletes;
	return true;
}

/* CH8 (§2.18): the deletions within the claimed unit's part's row ids, ascending: how many, *dead the first */
int
ch_scan_unit_dead(TableScanDesc sscan, const uint64 **dead)
{
	ChScanDesc	scan = (ChScanDesc) sscan;

	Assert(scan->part != NULL);
	*dead = scan->dead ? scan->dead + scan->dead_lo : NULL;
	return scan->dead_hi - scan->dead_lo;
}

void
ch_scan_pass_unit(TableScanDesc sscan)
{
	ChScanDesc	scan = (ChScanDesc) sscan;

	Assert(scan->part != NULL && scan->reader == NULL);
	scan->counts.units_passed++;
	release_unit(scan);
}

/*
 * V8e (pg_vector_executor.md §3.25, N2): the claimed unit read by the next
 * batches alone, which then end -- ch_scan_next_batch() returns false once,
 * at the unit's end -- the scan going on at the next call.
 */
void
ch_scan_one_unit(TableScanDesc sscan)
{
	ChScanDesc	scan = (ChScanDesc) sscan;

	Assert(scan->part != NULL && scan->reader == NULL);
	scan->one_unit = true;
}

/*
 * V8e: the claimed unit's units of its part -- an external table's file's
 * row groups or record batches -- that the filter keeps, ascending, into
 * *parts (palloc'd); and its granules' ranges, which ch_scan_read_unit()
 * takes back.
 */
int
ch_scan_unit_parts(TableScanDesc sscan, int32 **parts, chte_mark_range **ranges, int *nranges)
{
	ChScanDesc	scan = (ChScanDesc) sscan;
	int			n = 0;

	Assert(scan->part != NULL && scan->selected);
	if (scan->nranges == 0)
	{
		uint32		end = scan->unit_end ? scan->unit_end : (uint32) scan->part->marks;

		*parts = palloc(sizeof(int32) * Max(end - scan->unit_begin, 1));
		for (uint32 u = scan->unit_begin; u < end; u++)
			(*parts)[n++] = (int32) u;
		*ranges = palloc(sizeof(chte_mark_range));
		(*ranges)[0].begin = scan->unit_begin;
		(*ranges)[0].end = end;
		*nranges = 1;
		return n;
	}
	{
		int			total = 0;

		for (size_t i = 0; i < scan->nranges; i++)
			total += scan->ranges[i].end - scan->ranges[i].begin;
		*parts = palloc(sizeof(int32) * Max(total, 1));
		for (size_t i = 0; i < scan->nranges; i++)
			for (uint32 u = scan->ranges[i].begin; u < scan->ranges[i].end; u++)
				(*parts)[n++] = (int32) u;
	}
	*ranges = palloc(sizeof(chte_mark_range) * scan->nranges);
	memcpy(*ranges, scan->ranges, sizeof(chte_mark_range) * scan->nranges);
	*nranges = (int) scan->nranges;
	return n;
}

/*
 * V8e: a unit the scan claimed and passed over, read by the next batches
 * alone: its part's granules' ranges and their rows, as
 * ch_scan_unit_parts() gave them.  For a device that lost them, or refused
 * them, so that the reader raises what it raises.
 */
void
ch_scan_read_unit(TableScanDesc sscan, const ChPart *part, const chte_mark_range *ranges, int nranges, uint64 rows)
{
	ChScanDesc	scan = (ChScanDesc) sscan;

	if (scan->reader != NULL || scan->part != NULL || scan->count_left > 0)
		elog(ERROR, "a unit read again while another is being read");
	/* one of the scan's own parts, which ch_scan_claim_unit() handed out */
	scan->part = unconstify(ChPart *, part);
	scan->unit_whole = false;
	scan->unit_begin = 0;
	scan->unit_end = 0;
	if (scan->ranges_cap < (size_t) nranges)
	{
		scan->ranges = scan->ranges ? repalloc(scan->ranges, sizeof(chte_mark_range) * nranges) :
			MemoryContextAlloc(scan->scan_cxt, sizeof(chte_mark_range) * Max(nranges, 1));
		scan->ranges_cap = nranges;
	}
	memcpy(scan->ranges, ranges, sizeof(chte_mark_range) * nranges);
	scan->nranges = nranges;
	scan->unit_rows = rows;
	scan->selected = true;
	unit_deletions(scan);
	scan->one_unit = true;
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

/* ---- rows by their TIDs (VC16) ---- */

/*
 * What fetches by TID read in this backend, for ch.fetch_stats(): the rows
 * read from a part, the columns decoded for them, those of every column,
 * the rows taken from a granule kept from a fetch before, and the row ids
 * found among offsets located before.
 */
ChFetchStats ch_fetch_counts;

/* Row ids located at once, where a fetch's row id is near the one before */
#define CH_FETCH_LOCATE_IDS 8192

/* Row ids whose deletions a cache reads at once */
#define CH_FETCH_DEAD_IDS 65536

/*
 * The columns a fetch reads: their attribute indexes, from 0, the row id
 * (-1) last, as set_columns() takes them; whole where they are every column
 */
typedef struct FetchCols
{
	int			n;
	int		   *attnos;
	bool		whole;
} FetchCols;

/*
 * The columns of attnums[0 .. n), 1-based, ascending, that the table has, or
 * every one where n < 0; then the row id
 */
static void
fetch_cols(ChTable *t, int n, const AttrNumber *attnums, MemoryContext cxt, FetchCols *cols)
{
	int			live = 0;
	int			k = 0;

	cols->attnos = MemoryContextAlloc(cxt, sizeof(int) * (t->natts + 1));
	for (int i = 0; i < t->natts; i++)
	{
		bool		want = n < 0;

		if (t->columns[i].kind == CH_COL_DROPPED)
			continue;
		live++;
		for (int j = 0; j < n && !want; j++)
			want = attnums[j] == i + 1;
		if (want)
			cols->attnos[k++] = i;
	}
	cols->whole = k == live;
	cols->attnos[k++] = -1;
	cols->n = k;
}

/* The library's names of the columns, as set_columns() makes them */
static char *
cols_names(const FetchCols *cols)
{
	StringInfoData names;

	initStringInfo(&names);
	for (int c = 0; c < cols->n; c++)
	{
		if (c > 0)
			appendStringInfoString(&names, ", ");
		if (cols->attnos[c] < 0)
			appendStringInfoString(&names, CH_ROWID_COLUMN);
		else
			appendStringInfo(&names, "c%d", cols->attnos[c] + 1);
	}
	return names.data;
}

static void
count_fetch(const FetchCols *cols, int rows)
{
	ch_fetch_counts.fetches += rows;
	ch_fetch_counts.values += (int64) rows * (cols->n - 1);
	if (cols->whole)
		ch_fetch_counts.whole += rows;
}

/*
 * The rows of a part at the given offsets (ascending), those it holds, the
 * granules holding them read: each row materialized into rows[] in its
 * offset's order, of the columns cols names, the others NULL, its row id
 * into ids[].  Returns how many.
 */
static int
read_rows_at(Relation rel, ChTable *t, ChPart *part, const uint64 *offsets, int n, TupleDesc desc,
			 const FetchCols *cols, HeapTuple *rows, uint64 *ids)
{
	const ChEngine *eng = ch_engine();
	ChScanDescData scan;
	ResourceOwner owner;
	TupleTableSlot *slot;
	int			out = 0;

	memset(&scan, 0, sizeof(scan));
	scan.base.rs_rd = rel;
	scan.table = t;
	scan.fitted = true;
	scan.scan_cxt = CurrentMemoryContext;
	scan.batch_cxt = AllocSetContextCreate(CurrentMemoryContext, "ch fetch", ALLOCSET_DEFAULT_SIZES);
	set_columns(&scan, cols->n, cols->attnos);
	scan.part = part;
	scan.rowid_col = scan.asked_rowid;
	owner = ch_handle_reserve();
	if (part->listing)
		CH_CALL("reading rows by their TIDs",
				eng->external_read_rows(t->handle, part->listing->handle, (size_t) part->file, scan.columns, offsets,
										n, &scan.reader, CH_ERR));
	else
		CH_CALL("reading rows by their TIDs",
				eng->read_rows(t->handle, part->name, scan.columns, offsets, n, &scan.reader, CH_ERR));
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
		for (scan.row = 0; scan.row < nrows && out < n;)
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
	pfree(scan.columns);
	pfree(scan.columns_rowid);
	count_fetch(cols, out);
	return out;
}

/*
 * The rows of a part with the given row ids (ascending), those it holds:
 * their offsets through its _pg_rowid projection, then the rows at them
 * read (read_rows_at()).  Returns how many.
 */
static int
read_rows_by_id(Relation rel, ChTable *t, ChPart *part, const uint64 *rowids, int n, TupleDesc desc,
				const FetchCols *cols, HeapTuple *rows, uint64 *ids)
{
	const ChEngine *eng = ch_engine();
	uint64	   *offsets = palloc(sizeof(uint64) * n);
	uint64	   *found = palloc(sizeof(uint64) * n);
	int			nfound = 0;
	int			out;

	if (t->options.storage == CH_STORAGE_PAGES)
		ch_pages_attach(t, part);
	if (t->own)
		/* a table of its own files' part (§2.18): its rows' ids, its file's */
		CH_CALL("finding rows by their TIDs",
				eng->own_locate(t->handle, part->listing->handle, (size_t) part->file, rowids, n, offsets, CH_ERR));
	else
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
	out = read_rows_at(rel, t, part, found, nfound, desc, cols, rows, ids);
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
 * so the copy is the row's for its relation, storage and descriptor.  Only a
 * fetch of every column is kept: one of fewer columns left the others NULL.
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
 * A row by its TID, visible to the snapshot, of the columns cols names: from
 * the part the snapshot sees that holds it (§2.8 rule 7), the parts and the
 * deletions read from the catalogs for this row alone.
 */
static bool
fetch_uncached(Relation rel, ItemPointer tid, uint64 rowid, Snapshot snapshot, TupleTableSlot *slot,
			   const FetchCols *cols)
{
	ChTable    *t;
	SnapshotData nonvacuumable;
	List	   *parts;
	ListCell   *lc;
	bool		found = false;
	ChListing  *volatile own_listing = NULL;

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
		bool		external = t->external && !t->own;

		if (external || row_survives(t, rowid, snapshot))
		{
			/* an external table's: the statement's files, a row's id its file's first and its place there */
			parts = external ? ch_external_listing(rel, t)->parts :
				ch_parts_to_scan(&t->storage, ch_parts_snapshot(rel, snapshot, &nonvacuumable));
			if (t->own)
			{
				List	   *holding = NIL;

				/* a table of its own files' parts whose row ids span the row's, their files listed (§2.18) */
				foreach(lc, parts)
				{
					ChPart	   *p = lfirst(lc);

					if ((int64) rowid >= p->min_rowid && (int64) rowid <= p->max_rowid)
						holding = lappend(holding, p);
				}
				parts = holding;
				if (parts != NIL)
				{
					own_listing = ch_own_listing(t, parts);
					ch_listing_pin(own_listing);
				}
			}
			foreach(lc, parts)
			{
				ChPart	   *p = lfirst(lc);
				HeapTuple	row;
				uint64		id;
				uint64		offset = rowid - (uint64) p->min_rowid;

				if ((int64) rowid < p->min_rowid || (int64) rowid > p->max_rowid)
					continue;
				if (external ?
					read_rows_at(rel, t, p, &offset, 1, slot->tts_tupleDescriptor, cols, &row, &id) == 1 :
					read_rows_by_id(rel, t, p, &rowid, 1, slot->tts_tupleDescriptor, cols, &row, &id) == 1)
				{
					if (cols->whole)
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
		if (own_listing)
			ch_listing_unpin(own_listing);
		ch_table_unpin(t);
	}
	PG_END_TRY();
	return found;
}

/* The TID's row id, where it names a row at all */
static bool
tid_rowid(ItemPointer tid, uint64 *rowid)
{
	if (!ItemPointerIsValid(tid) || ItemPointerGetOffsetNumberNoCheck(tid) > CH_ROWS_PER_BLOCK)
		return false;
	*rowid = CH_TID_TO_ROWID(tid);
	return true;
}

/*
 * A row by its TID, visible to the snapshot, of the columns attnums[0 .. n)
 * (1-based), or of every one where n < 0, the slot's other attributes NULL:
 * from the write buffer, or from the part the snapshot sees that holds it.
 */
bool
ch_fetch_row_columns(Relation rel, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot, int n,
					 const AttrNumber *attnums)
{
	uint64		rowid;
	FetchCols	cols;
	ChTable    *t;
	bool		found;

	if (!tid_rowid(tid, &rowid))
		return false;
	if (ch_is_mvcc_rel(rel))
	{
		if (snapshot && IsMVCCSnapshot(snapshot))
			PredicateLockRelation(rel, snapshot);
		if (ch_buffer_fetch(rel, rowid, snapshot, slot))
			return true;
	}
	t = ch_table(rel);
	fetch_cols(t, n, attnums, CurrentMemoryContext, &cols);
	found = fetch_uncached(rel, tid, rowid, snapshot, slot, &cols);
	pfree(cols.attnos);
	return found;
}

/*
 * A row by its TID into the slot: of the columns fetch.c knows the slot to
 * need (ch_fetch_cache_for()), through that slot's cache, or of every column.
 */
bool
ch_fetch_row(Relation rel, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot)
{
	ChFetchCache *cache;

	/* an external table's row: its file's, read again, every column (a TID scan's) */
	if (ch_is_external_rel(rel, NULL) && !ch_is_own_rel(rel))
		return ch_fetch_row_columns(rel, tid, snapshot, slot, -1, NULL);
	/* a table of its own files' (§2.18): the columns fetch.c knows the slot to need, uncached */
	if (ch_is_own_rel(rel))
	{
		int			n;
		const AttrNumber *attnums;

		if (!ch_fetch_slot_columns(rel, slot, &n, &attnums))
			n = -1;
		return ch_fetch_row_columns(rel, tid, snapshot, slot, n, n < 0 ? NULL : attnums);
	}
	cache = ch_fetch_cache_for(rel, slot);

	if (cache != NULL)
		return ch_fetch_row_cached(rel, tid, snapshot, slot, cache);
	return ch_fetch_row_columns(rel, tid, snapshot, slot, -1, NULL);
}

/*
 * The caches of fetches by TID (VC16).  An index scan's fetches -- a
 * nested loop's inner index scan makes one a probe -- and those into a
 * slot fetch.c knows, each read the snapshot's parts and the row's
 * deletions from the catalogs, its offset through the part's _pg_rowid
 * projection, and a granule of each column it decodes, keeping nothing:
 * the rows of one key, adjacent in a part, cost a granule each.  A cache
 * keeps, for one MVCC snapshot -- by its identity, the transactions it
 * found completed among them -- the parts it sees, the deletions it sees of
 * a range of row ids, and for each part the offsets of a range of row ids
 * located at once, where a fetch's row id is near the one before, and the
 * granule of the last row read: its columns as the library decoded them,
 * the whole granule read at the cost of one row of it, where the part's
 * granules are of index_granularity rows, and kept where they take no more
 * than work_mem.  A row's values never change once its row id is given
 * (§2.6), and a kept granule is the library's columns, not the part's files,
 * which a merge may remove.  Any other snapshot reads as fetch_uncached().
 */
typedef struct KeptBatch
{
	uint64		first;			/* the offset of its first row */
	uint32		nrows;
	chte_vector *vectors;		/* one a column read */
	chte_batch *keep;			/* the library's columns they point into */
} KeptBatch;

typedef struct FetchPart
{
	ChPart	   *part;
	/* the row ids [loc_lo, loc_hi) located, their offsets, PG_UINT64_MAX where the part has none */
	uint64		loc_lo;
	uint64		loc_hi;			/* 0: none */
	uint64	   *loc_offsets;
	uint64		last_rowid;		/* the row id located last, + 1; 0: none */
	/* the offsets [win_lo, win_hi) kept, a batch or more of them */
	uint64		win_lo;
	uint64		win_hi;			/* 0: none */
	int			nkept;
	int			kept_cap;
	KeptBatch  *kept;
	bool	   *missing;		/* per column read: the part has none */
} FetchPart;

struct ChFetchCache
{
	MemoryContext cxt;			/* its own */
	FetchCols	cols;
	char	   *names;			/* the columns', for the library */
	bool		no_windows;		/* a granule larger than work_mem was met */
	/*
	 * one snapshot's parts and deletions, in snap_cxt, whose reset or
	 * deletion -- before its parent's, which deletes its children first --
	 * releases the library's columns it keeps (snapshot_gone())
	 */
	MemoryContext snap_cxt;
	MemoryContextCallback *snap_cb;
	bool		snap_valid;
	Snapshot	snapshot;
	TransactionId xmin;
	TransactionId xmax;
	CommandId	curcid;
	uint64		completions;
	RelFileNumber relfilenumber;
	List	   *parts;
	FetchPart  *fparts;			/* per part of parts, in their order */
	int			nparts;
	uint64		dead_lo;		/* the deletions of row ids [dead_lo, dead_hi) */
	uint64		dead_hi;		/* 0: none read */
	uint64	   *dead;
	int			ndead;
	MemoryContext row_cxt;		/* a row's values, as the slot is filled */
};

/* The granule a part keeps let go: the library's columns, and its batches' arrays */
static void
release_kept(FetchPart *fp)
{
	for (int i = 0; i < fp->nkept; i++)
	{
		if (fp->kept[i].keep)
			ch_engine()->batch_release(fp->kept[i].keep);
		fp->kept[i].keep = NULL;
		if (fp->kept[i].vectors)
			pfree(fp->kept[i].vectors);
		fp->kept[i].vectors = NULL;
	}
	fp->nkept = 0;
	fp->win_lo = fp->win_hi = 0;
}

/*
 * A snapshot's context is going, reset for another or deleted with the
 * cache: its memory is still there, the library's columns it kept go
 */
static void
snapshot_gone(void *arg)
{
	ChFetchCache *c = arg;

	for (int i = 0; i < c->nparts; i++)
		release_kept(&c->fparts[i]);
	c->nparts = 0;
	c->fparts = NULL;
	c->snap_valid = false;
}

/*
 * A cache of fetches of the columns attnums[0 .. n) of the relation (1-based),
 * every column where n < 0, in a context under parent, whose reset or
 * deletion releases it and the library's columns it keeps.
 */
ChFetchCache *
ch_fetch_cache_create(MemoryContext parent, Relation rel, int n, const AttrNumber *attnums)
{
	MemoryContext cxt = AllocSetContextCreate(parent, "ch fetch cache", ALLOCSET_SMALL_SIZES);
	ChFetchCache *c = MemoryContextAllocZero(cxt, sizeof(ChFetchCache));
	MemoryContext old;

	c->cxt = cxt;
	c->snap_cxt = AllocSetContextCreate(cxt, "ch fetch snapshot", ALLOCSET_DEFAULT_SIZES);
	c->row_cxt = AllocSetContextCreate(cxt, "ch fetch row", ALLOCSET_DEFAULT_SIZES);
	fetch_cols(ch_table(rel), n, attnums, cxt, &c->cols);
	old = MemoryContextSwitchTo(cxt);
	c->names = cols_names(&c->cols);
	MemoryContextSwitchTo(old);
	return c;
}

void
ch_fetch_cache_free(ChFetchCache *cache)
{
	MemoryContextDelete(cache->cxt);
}

/* The parts and deletions a cache holds are the snapshot's */
static bool
snapshot_cached(ChFetchCache *c, Relation rel, Snapshot snapshot)
{
	return c->snap_valid && c->snapshot == snapshot && c->xmin == snapshot->xmin &&
		c->xmax == snapshot->xmax && c->curcid == snapshot->curcid &&
		c->completions == snapshot->snapXactCompletionCount &&
		c->relfilenumber == rel->rd_locator.relNumber;
}

static void
cache_snapshot(ChFetchCache *c, Relation rel, ChTable *t, Snapshot snapshot)
{
	MemoryContext old;
	ListCell   *lc;
	int			i = 0;

	MemoryContextReset(c->snap_cxt);	/* snapshot_gone() runs */
	c->parts = NIL;
	c->fparts = NULL;
	c->nparts = 0;
	c->dead = NULL;
	c->ndead = 0;
	c->dead_lo = c->dead_hi = 0;
	c->snap_cb = MemoryContextAllocZero(c->snap_cxt, sizeof(MemoryContextCallback));
	c->snap_cb->func = snapshot_gone;
	c->snap_cb->arg = c;
	MemoryContextRegisterResetCallback(c->snap_cxt, c->snap_cb);
	old = MemoryContextSwitchTo(c->snap_cxt);
	c->parts = ch_parts_to_scan(&t->storage, snapshot);
	c->fparts = palloc0(sizeof(FetchPart) * Max(list_length(c->parts), 1));
	foreach(lc, c->parts)
		c->fparts[i++].part = lfirst(lc);
	MemoryContextSwitchTo(old);
	c->nparts = i;
	c->snapshot = snapshot;
	c->xmin = snapshot->xmin;
	c->xmax = snapshot->xmax;
	c->curcid = snapshot->curcid;
	c->completions = snapshot->snapXactCompletionCount;
	c->relfilenumber = rel->rd_locator.relNumber;
	c->snap_valid = true;
}

/* Whether the snapshot sees a deletion or update of the row, from the range of deletions read */
static bool
cached_row_deleted(ChFetchCache *c, ChTable *t, Snapshot snapshot, uint64 rowid)
{
	if (c->dead_hi == 0 || rowid < c->dead_lo || rowid >= c->dead_hi)
	{
		MemoryContext old = MemoryContextSwitchTo(c->snap_cxt);
		uint64		lo = rowid - rowid % CH_FETCH_DEAD_IDS;

		if (c->dead)
			pfree(c->dead);
		c->dead = NULL;
		c->dead_hi = 0;
		c->dead = ch_tombstones_visible(&t->storage, snapshot, lo, lo + CH_FETCH_DEAD_IDS - 1, &c->ndead);
		c->dead_lo = lo;
		c->dead_hi = lo + CH_FETCH_DEAD_IDS;
		MemoryContextSwitchTo(old);
	}
	return c->ndead > 0 && ch_rowid_in(c->dead, c->ndead, rowid);
}

/*
 * The row id's offset in the part, PG_UINT64_MAX where it has none: from the
 * ids located before, or through its projection -- those of a range of
 * CH_FETCH_LOCATE_IDS row ids at once where the row id located last is in
 * the same range, as an index's rows of one key are.
 */
static uint64
locate(ChTable *t, ChFetchCache *c, FetchPart *fp, uint64 rowid)
{
	const ChEngine *eng = ch_engine();
	uint64		lo = rowid - rowid % CH_FETCH_LOCATE_IDS;
	uint64		offset;

	if (fp->loc_hi > 0 && rowid >= fp->loc_lo && rowid < fp->loc_hi)
	{
		ch_fetch_counts.located++;
		return fp->loc_offsets[rowid - fp->loc_lo];
	}
	if (t->options.storage == CH_STORAGE_PAGES)
		ch_pages_attach(t, fp->part);
	if (fp->last_rowid > 0 && (fp->last_rowid - 1) / CH_FETCH_LOCATE_IDS == rowid / CH_FETCH_LOCATE_IDS)
	{
		uint64		first = Max(lo, (uint64) fp->part->min_rowid);
		uint64		end = Min(lo + CH_FETCH_LOCATE_IDS, (uint64) fp->part->max_rowid + 1);
		uint64	   *ids = palloc(sizeof(uint64) * (end - first));

		if (fp->loc_offsets == NULL)
			fp->loc_offsets = MemoryContextAlloc(c->snap_cxt, sizeof(uint64) * CH_FETCH_LOCATE_IDS);
		fp->loc_hi = 0;
		for (uint64 i = first; i < end; i++)
			ids[i - first] = i;
		CH_CALL("finding rows by their TIDs",
				eng->part_locate(t->handle, fp->part->name, ids, end - first, fp->loc_offsets, CH_ERR));
		pfree(ids);
		fp->loc_lo = first;
		fp->loc_hi = end;
		fp->last_rowid = rowid + 1;
		return fp->loc_offsets[rowid - first];
	}
	CH_CALL("finding rows by their TIDs", eng->part_locate(t->handle, fp->part->name, &rowid, 1, &offset, CH_ERR));
	fp->last_rowid = rowid + 1;
	return offset;
}

/* The bytes of a kept batch's columns as decoded */
static Size
kept_bytes(ChTable *t, const FetchCols *cols, const KeptBatch *kb)
{
	Size		bytes = 0;

	for (int c = 0; c < cols->n; c++)
	{
		const chte_vector *v = &kb->vectors[c];

		if (cols->attnos[c] >= 0 && v->offsets != NULL)
			bytes += (Size) (v->offsets[kb->nrows] - v->offsets[0]) + sizeof(uint64) * kb->nrows;
		else if (cols->attnos[c] >= 0)
			bytes += (Size) Max(t->columns[cols->attnos[c]].width, 1) * kb->nrows;
		else
			bytes += sizeof(uint64) * kb->nrows;
	}
	return bytes;
}

/*
 * The granule of the part holding offsets [lo, hi), read and kept; false
 * where it was not kept -- not all its rows came, or they take more than
 * work_mem, which no granule of the cache's is kept for again.
 */
static bool
keep_window(ChTable *t, ChFetchCache *c, FetchPart *fp, uint64 lo, uint64 hi)
{
	const ChEngine *eng = ch_engine();
	uint64	   *offsets = palloc(sizeof(uint64) * (hi - lo));
	chte_reader *reader;
	ResourceOwner owner;
	uint64		total = 0;
	Size		bytes = 0;

	release_kept(fp);
	for (uint64 i = lo; i < hi; i++)
		offsets[i - lo] = i;
	if (t->options.storage == CH_STORAGE_PAGES)
		ch_pages_attach(t, fp->part);
	owner = ch_handle_reserve();
	CH_CALL("reading rows by their TIDs",
			eng->read_rows(t->handle, fp->part->name, c->names, offsets, hi - lo, &reader, CH_ERR));
	ch_handle_opened(owner, CH_HANDLE_READER, reader);
	pfree(offsets);
	if (fp->missing == NULL)
		fp->missing = MemoryContextAlloc(c->snap_cxt, sizeof(bool) * c->cols.n);
	for (int col = 0; col < c->cols.n; col++)
		fp->missing[col] = !eng->read_present(reader, col);
	for (;;)
	{
		uint32		nrows;
		KeptBatch  *kb;
		chte_vector *vectors;

		CH_CALL("reading rows by their TIDs", eng->read_next(reader, &nrows, CH_ERR));
		if (nrows == 0)
			break;
		if (fp->nkept == fp->kept_cap)
		{
			fp->kept_cap = Max(fp->kept_cap * 2, 4);
			fp->kept = fp->kept == NULL ?
				MemoryContextAlloc(c->snap_cxt, sizeof(KeptBatch) * fp->kept_cap) :
				repalloc(fp->kept, sizeof(KeptBatch) * fp->kept_cap);
		}
		vectors = MemoryContextAlloc(c->snap_cxt, sizeof(chte_vector) * c->cols.n);
		kb = &fp->kept[fp->nkept++];
		kb->first = lo + total;
		kb->nrows = nrows;
		kb->vectors = vectors;
		kb->keep = NULL;
		for (int col = 0; col < c->cols.n; col++)
			CH_CALL("reading rows by their TIDs", eng->read_vector(reader, col, &vectors[col], CH_ERR));
		kb->keep = eng->read_retain(reader);
		if (kb->keep == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory keeping rows of the engine library")));
		total += nrows;
		bytes += kept_bytes(t, &c->cols, kb);
	}
	ch_handle_closing(owner, CH_HANDLE_READER, reader);
	eng->read_end(reader);
	if (total != hi - lo || bytes > (Size) work_mem * 1024)
	{
		if (bytes > (Size) work_mem * 1024)
			c->no_windows = true;
		release_kept(fp);
		return false;
	}
	fp->win_lo = lo;
	fp->win_hi = hi;
	return true;
}

/* The kept row at offset, into the slot */
static void
fill_from_kept(Relation rel, ChTable *t, ChFetchCache *c, FetchPart *fp, uint64 offset, TupleTableSlot *slot)
{
	ChScanDescData scan;
	KeptBatch  *kb = NULL;

	for (int i = 0; i < fp->nkept; i++)
		if (offset >= fp->kept[i].first && offset < fp->kept[i].first + fp->kept[i].nrows)
		{
			kb = &fp->kept[i];
			break;
		}
	Assert(kb != NULL);
	memset(&scan, 0, sizeof(scan));
	scan.base.rs_rd = rel;
	scan.table = t;
	scan.fitted = true;
	scan.ncols = c->cols.n;
	scan.attnos = c->cols.attnos;
	scan.vectors = kb->vectors;
	scan.missing = fp->missing;
	MemoryContextReset(c->row_cxt);
	fill_row(&scan, (uint32) (offset - kb->first), slot, c->row_cxt);
	ExecMaterializeSlot(slot);
	count_fetch(&c->cols, 1);
}

/* The row at offset of the part into the slot, from its kept granule, a granule kept now, or alone */
static bool
fetch_at(Relation rel, ChTable *t, ChFetchCache *c, FetchPart *fp, uint64 rowid, uint64 offset,
		 TupleTableSlot *slot)
{
	ChPart	   *part = fp->part;
	int			g = t->options.index_granularity;
	HeapTuple	row;
	uint64		id;

	if (fp->win_hi > 0 && offset >= fp->win_lo && offset < fp->win_hi)
	{
		ch_fetch_counts.kept++;
		fill_from_kept(rel, t, c, fp, offset, slot);
		return true;
	}
	/* granules of index_granularity rows each: the row's, read whole */
	if (!c->no_windows && ch_enable_fetch_cache && g > 0 && (uint64) offset < (uint64) part->rows &&
		(int64) part->marks == (part->rows + g - 1) / g)
	{
		uint64		lo = offset - offset % g;
		uint64		hi = Min(lo + g, (uint64) part->rows);

		if (keep_window(t, c, fp, lo, hi))
		{
			fill_from_kept(rel, t, c, fp, offset, slot);
			return true;
		}
	}
	if (read_rows_at(rel, t, part, &offset, 1, slot->tts_tupleDescriptor, &c->cols, &row, &id) != 1)
		return false;
	if (c->cols.whole)
		remember_fetch(rel, rowid, slot->tts_tupleDescriptor, row);
	ExecForceStoreHeapTuple(row, slot, true);
	return true;
}

/*
 * A row by its TID into the slot, of the cache's columns, the slot's other
 * attributes NULL, through the cache where the snapshot is an MVCC one.
 */
bool
ch_fetch_row_cached(Relation rel, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot,
					ChFetchCache *c)
{
	uint64		rowid;
	ChTable    *t;
	bool		found = false;

	if (!tid_rowid(tid, &rowid))
		return false;
	if (snapshot && IsMVCCSnapshot(snapshot))
		PredicateLockRelation(rel, snapshot);
	if (ch_buffer_fetch(rel, rowid, snapshot, slot))
		return true;
	if (snapshot == NULL || snapshot->snapshot_type != SNAPSHOT_MVCC || !ch_enable_fetch_cache)
		return fetch_uncached(rel, tid, rowid, snapshot, slot, &c->cols);

	t = ch_table(rel);
	ch_table_pin(t);
	PG_TRY();
	{
		if (!snapshot_cached(c, rel, snapshot))
			cache_snapshot(c, rel, t, snapshot);
		if (!cached_row_deleted(c, t, snapshot, rowid))
		{
			for (int i = 0; i < c->nparts && !found; i++)
			{
				FetchPart  *fp = &c->fparts[i];
				uint64		offset;

				if ((int64) rowid < fp->part->min_rowid || (int64) rowid > fp->part->max_rowid)
					continue;
				offset = locate(t, c, fp, rowid);
				if (offset != PG_UINT64_MAX)
					found = fetch_at(rel, t, c, fp, rowid, offset, slot);
			}
		}
	}
	PG_FINALLY();
	{
		ch_table_unpin(t);
	}
	PG_END_TRY();
	if (found)
	{
		slot->tts_tid = *tid;
		slot->tts_tableOid = RelationGetRelid(rel);
	}
	return found;
}

/*
 * A bitmap scan's next row: the TIDs of the bitmap's next block, exact or
 * the whole block where it is lossy, those no deletion the snapshot sees
 * names, read from the parts holding them, materialized for the block, of
 * the columns its node reads (fetch.c), the others NULL.
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
		/* the columns the slot's node reads (fetch.c), or every one */
		if (scan->bm_cols == NULL)
		{
			int			ncols;
			const AttrNumber *attnums = NULL;

			scan->bm_cols = MemoryContextAlloc(scan->scan_cxt, sizeof(FetchCols));
			if (!ch_fetch_slot_columns(scan->base.rs_rd, slot, &ncols, &attnums))
				ncols = -1;
			fetch_cols(scan->table, ncols, attnums, scan->scan_cxt, scan->bm_cols);
		}
		own_listing(scan);
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
								  RelationGetDescr(scan->base.rs_rd), scan->bm_cols,
								  scan->bm_rows + scan->bm_nrows, ids);
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
ch_estimate_units(Relation rel, Snapshot snapshot, double *units, bool *deleted)
{
	ChTable    *t = ch_table(rel);
	SnapshotData nonvacuumable;

	if (t->external && !t->own)
	{
		/* the statement's files, none of whose rows is deleted (§2.17) */
		ch_table_pin(t);
		PG_TRY();
		{
			*units = list_length(ch_external_listing(rel, t)->parts);
		}
		PG_FINALLY();
		{
			ch_table_unpin(t);
		}
		PG_END_TRY();
		*deleted = false;
		return;
	}
	ch_table_pin(t);
	*units = list_length(ch_parts_to_scan(&t->storage, ch_parts_snapshot(rel, snapshot, &nonvacuumable)));
	*deleted = true;
	/* whether any row is deleted, not which: the first deletion found tells */
	if (snapshot != NULL)
		*deleted = ch_tombstones_visible_any(&t->storage, snapshot);
	ch_table_unpin(t);
}

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
	ChListing  *own_listing = NULL;

	*rows = 0;
	*bytes = 0;
	if (raw)
		*raw = 0;
	/* pinned from the start: the catalog's read may accept an invalidation of the table */
	ch_table_pin(t);
	parts = t->external && !t->own ? ch_external_listing(rel, t)->parts :
		ch_parts_to_scan(&t->storage, ch_parts_snapshot(rel, snapshot, &nonvacuumable));
	if (parts == NIL)
	{
		ch_table_unpin(t);
		return;
	}
	/* a table of its own files' parts, each read through its file (§2.18) */
	if (t->own)
	{
		own_listing = ch_own_listing(t, parts);
		ch_listing_pin(own_listing);
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
	/*
	 * Each part's rows the quals keep and its needed columns' bytes, from its
	 * summary where it has one (VC20's B6, B7), else from the part itself,
	 * loaded -- and, in the pages store, attached -- only then.
	 */
	foreach(lc, parts)
	{
		ChPart	   *p = lfirst(lc);
		uint64		kept = (uint64) p->rows;
		bytea	   *summary = NULL;
		int			verdict = CHTE_SELECT_RANGES;
		int			found = 0;

		if (ch_enable_part_summaries && eng->summary_column_bytes != NULL && p->listing == NULL)
			summary = ch_part_summary(p);
		if (filter)
		{
			if (ch_enable_summary_pruning && eng->filter_summary_select != NULL && p->listing == NULL)
				CH_CALL("analyzing a part",
						eng->filter_summary_select(filter, p->name, summary ? VARDATA_ANY(summary) : NULL,
												   summary ? VARSIZE_ANY_EXHDR(summary) : 0, &verdict, CH_ERR));
			if (verdict == CHTE_SELECT_NONE)
				kept = 0;
			else if (verdict == CHTE_SELECT_RANGES)
			{
				size_t		nranges;

				if (t->options.storage == CH_STORAGE_PAGES)
					ch_pages_attach(t, p);
				(void) ch_unit_select(t, p, filter, 0, 0, NULL, 0, &nranges, &kept);
			}
		}
		*rows += kept;
		if (names.len > 0 && kept > 0 && p->rows > 0)
		{
			uint64		compressed;
			uint64		uncompressed;

			if (summary)
				CH_CALL("sizing a part's columns",
						eng->summary_column_bytes(t->handle, p->name, VARDATA_ANY(summary), VARSIZE_ANY_EXHDR(summary),
												  names.data, &compressed, &uncompressed, &found, CH_ERR));
			if (!found)
			{
				if (t->options.storage == CH_STORAGE_PAGES)
					ch_pages_attach(t, p);
				ch_unit_column_bytes(t, p, names.data, &compressed, &uncompressed);
			}
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
	if (own_listing)
		ch_listing_unpin(own_listing);
	ch_table_unpin(t);
	pfree(names.data);
}

/* ---- the order of a serial scan's rows (VC4) ---- */

/*
 * Whether rows of a column's equal values lie together wherever ClickHouse
 * sorted them: its order's equality is the type's default equality --
 * integers, dates and times by their values, floats by theirs (-0 = 0, NaN
 * = NaN, as both have them), numeric(p, s) as Decimal, text, varchar and
 * bytea of a deterministic collation by their bytes, uuid and name by
 * theirs.  Not bpchar (its trailing spaces), unconstrained numeric (kept as
 * its bytes, 1.0 and 1.00 two), interval and the rest.
 */
static bool
key_equality_kept(Relation rel, AttrNumber attnum, const ChColumn *col)
{
	Form_pg_attribute att;
	Oid			typid;

	if (attnum < 1 || attnum > RelationGetNumberOfAttributes(rel))
		return false;
	att = TupleDescAttr(RelationGetDescr(rel), attnum - 1);
	if (att->attisdropped)
		return false;
	typid = getBaseType(att->atttypid);
	switch (col->kind)
	{
		case CH_COL_FIXED:
			return typid == BOOLOID || typid == INT2OID || typid == INT4OID || typid == INT8OID ||
				typid == OIDOID || typid == CHAROID || typid == DATEOID || typid == TIMEOID ||
				typid == TIMESTAMPOID || typid == TIMESTAMPTZOID || typid == FLOAT4OID ||
				typid == FLOAT8OID || typid == UUIDOID || typid == NAMEOID;
		case CH_COL_NUMERIC:
			return true;
		case CH_COL_VARLENA:
			if (typid == BYTEAOID)
				return true;
			return (typid == TEXTOID || typid == VARCHAROID) &&
				(!OidIsValid(att->attcollation) || get_collation_isdeterministic(att->attcollation));
		default:
			return false;
	}
}

/*
 * The attributes the table's rows are sorted by in each part -- its
 * order_by -- as far as their order's equality is their types' own, at most
 * max of them into attnums; how many.
 */
int
ch_table_ordering(Relation rel, int max, AttrNumber *attnums)
{
	ChTable    *t = ch_table(rel);
	char	   *copy;
	List	   *names;
	ListCell   *lc;
	int			n = 0;

	if (t->options.order_by == NULL)
		return 0;
	copy = pstrdup(t->options.order_by);
	if (!SplitIdentifierString(copy, ',', &names))
		return 0;
	foreach(lc, names)
	{
		AttrNumber	attnum = get_attnum(RelationGetRelid(rel), lfirst(lc));

		if (n >= max || attnum < 1 || attnum > t->natts || !key_equality_kept(rel, attnum, &t->columns[attnum - 1]))
			break;
		attnums[n++] = attnum;
	}
	return n;
}

typedef struct PartRange
{
	ChPart	   *part;
	bool		has_values;
	Datum		min;
	Datum		max;
	int64		nulls;
} PartRange;

static FmgrInfo *parts_cmp;
static Oid	parts_collation;

static int
compare_ranges(const void *a, const void *b)
{
	const PartRange *x = a;
	const PartRange *y = b;

	/* a part of NULLs alone after the others */
	if (x->has_values != y->has_values)
		return x->has_values ? -1 : 1;
	if (!x->has_values)
		return 0;
	return DatumGetInt32(FunctionCall2Coll(parts_cmp, parts_collation, x->min, y->min));
}

/*
 * How far the parts, read one after another in *ordered, hand their rows in
 * the order of the table's key, by their least and greatest values of its
 * first column, first_key: 2, its every column -- one part, or parts whose
 * ranges are apart; 1, its first column alone -- the ranges meet at their
 * ends, where rows of the same first value end one part and begin the next;
 * 0, no order -- they overlap, a part's statistics keep no least and
 * greatest values, or a part's NULLs, which sort last, are not the last
 * rows read.
 */
int
ch_parts_order(Relation rel, ChTable *t, List *parts, AttrNumber first_key, List **ordered)
{
	int			n = list_length(parts);
	PartRange  *ranges;
	TypeCacheEntry *tc;
	ListCell   *lc;
	int			level = 2;
	int			i = 0;

	*ordered = parts;
	if (n <= 1)
		return 2;
	if (first_key < 1 || first_key > t->natts)
		return 0;
	ranges = palloc(sizeof(PartRange) * n);
	foreach(lc, parts)
	{
		PartRange  *r = &ranges[i++];

		r->part = lfirst(lc);
		if (!ch_part_minmax(t, r->part, first_key, &r->has_values, &r->min, &r->max, &r->nulls))
			return 0;
	}
	tc = lookup_type_cache(TupleDescAttr(RelationGetDescr(rel), first_key - 1)->atttypid, TYPECACHE_CMP_PROC_FINFO);
	if (!OidIsValid(tc->cmp_proc_finfo.fn_oid))
		return 0;
	parts_cmp = &tc->cmp_proc_finfo;
	parts_collation = TupleDescAttr(RelationGetDescr(rel), first_key - 1)->attcollation;
	qsort(ranges, n, sizeof(PartRange), compare_ranges);
	for (i = 0; i < n - 1; i++)
	{
		int			c;

		/* only the last part read may hold NULLs */
		if (ranges[i].nulls > 0 || !ranges[i].has_values || !ranges[i + 1].has_values)
			return 0;
		c = DatumGetInt32(FunctionCall2Coll(parts_cmp, parts_collation, ranges[i].max, ranges[i + 1].min));
		if (c > 0)
			return 0;
		if (c == 0)
			level = 1;
	}
	*ordered = NIL;
	for (i = 0; i < n; i++)
		*ordered = lappend(*ordered, ranges[i].part);
	return level;
}

/*
 * Before a serial scan's first batch: its parts in the order of the table's
 * key, where they can be (ch_parts_order()); how far it hands its rows in
 * that order.  A parallel scan's participants claim their units in any
 * order: none.
 */
int
ch_scan_order_parts(TableScanDesc sscan, AttrNumber first_key)
{
	ChScanDesc	scan = (ChScanDesc) sscan;
	List	   *ordered;
	int			level;

	if (scan->base.rs_parallel != NULL || scan->part != NULL || scan->next_unit > 0)
		return 0;
	level = ch_parts_order(scan->base.rs_rd, scan->table, scan->parts, first_key, &ordered);
	if (level > 0)
	{
		MemoryContext old = MemoryContextSwitchTo(scan->scan_cxt);

		scan->parts = list_copy(ordered);
		MemoryContextSwitchTo(old);
	}
	return level;
}
