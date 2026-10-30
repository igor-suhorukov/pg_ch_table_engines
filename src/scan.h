/*-------------------------------------------------------------------------
 *
 * scan.h
 *	  What ChScan and vexec's source ask of a scan of a table of an engine
 *	  (scan.c), beyond the table access method's own calls: the columns it
 *	  reads, a filter to prune by, its batches as the engine library hands
 *	  them, and its units, for aggregates and bounds.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#ifndef CHTE_SCAN_H
#define CHTE_SCAN_H

#include "access/relscan.h"

#include "chte.h"

/* A batch as read: valid until the next call on the scan */
typedef struct ChScanBatch
{
	uint32		nrows;
	int			ncols;			/* the columns set; none for a scan of no column */
	const int  *attnos;			/* per column: the attribute's index, -1 the row id */
	const chte_vector *vectors;
	const bool *missing;		/* per column: the part has none, its rows the missing value */
	const chte_dictionary *dicts;	/* per column, where ch_scan_set_dictionaries() asked:
									 * encoded, its dictionary in place of its vector; or NULL */
	chte_reader *reader;		/* NULL for a scan of no column */
	const uint64 *visible;		/* NULL: every row; else nrows bits, 1 = not deleted */
	uint32		nvisible;		/* the rows not deleted */
	const ChPart *part;			/* whose rows they are: a part, or an external table's file */
	/*
	 * F7 (VC30): the part's granule every row of the batch is of, -1 where
	 * not known (a file's, a library's without chte_read_granule), and
	 * whether the batch holds the granule's last row
	 */
	int64		granule;
	bool		granule_last;
} ChScanBatch;

/* A unit, claimed and not yet read */
typedef struct ChScanUnit
{
	const ChPart *part;
	bool		whole;			/* the whole part, unfiltered: its statistics are the unit's */
	uint64		rows;			/* the rows it holds, deleted ones too */
	bool		deletions;		/* deletions the snapshot sees may name some of them */
} ChScanUnit;

/* What a scan did, for EXPLAIN ANALYZE */
typedef struct ChScanCounts
{
	int64		parts;			/* the snapshot's */
	int64		parts_pruned;	/* none of whose granules was kept */
	int64		units;			/* taken by this participant */
	int64		units_passed;	/* answered from statistics or passed over by a bound */
	int64		marks;			/* granules of the units taken */
	int64		marks_read;		/* of them, those read */
	int64		marks_skipped;	/* F7: of those the filter kept, those a granule skip passed over */
	int64		marks_bound;	/* VC43: of those, those a running bound passed over */
	int64		units_cut;		/* VC44: the units, where cut over the granules the filter keeps; else 0 */
	int64		sparse_passed;	/* VC45: rows of a sparse column's default its quals leave out */
} ChScanCounts;

/*
 * Before the first batch: the attributes read (1..natts), the row id if asked;
 * none at all reads no column, the rows only.
 */
extern void ch_scan_set_columns(TableScanDesc scan, int n, const AttrNumber *attnums, bool rowid);

/* Before the first batch: quals to prune by (filter.c); false when none could be */
extern bool ch_scan_set_filter(TableScanDesc scan, List *quals);
/*
 * VC45: before the first batch, the quals the scan's rows must meet: where one is false of a column's
 * default and NULL, a part's sparse column's rows of the default are left out of its batches' visible rows
 */
extern void ch_scan_set_sparse_quals(TableScanDesc scan, List *quals);
extern void ch_scan_set_join_keys(TableScanDesc scan, int nkeys, const ScanKeyData *keys);

extern bool ch_scan_set_dictionaries(TableScanDesc scan, const bool *keep);

/*
 * VC2: before the first batch, the attributes set read in steps, step_of[i]
 * each's (0 the first, the row id's); false where they cannot be.  A batch then holds the first
 * step's columns, and ch_scan_read_step() reads a later one's of it.
 */
extern bool ch_scan_set_steps(TableScanDesc scan, const int *step_of, int nsteps);
extern void ch_scan_read_step(TableScanDesc scan, int step, ChScanBatch *batch);
extern bool ch_scan_next_batch(TableScanDesc scan, ChScanBatch *batch);

/*
 * A check of each unit as it is claimed, before anything of it is read: false
 * passes it over (unit->rows is not set).
 */
typedef bool (*ChUnitCheck) (void *arg, const ChScanUnit *unit);
extern void ch_scan_set_unit_check(TableScanDesc scan, ChUnitCheck check, void *arg);

/*
 * VC35: a parallel scan's participant that alone claims its units from now
 * on: a part's units it claims at once, where no other participant claimed
 * one between.  Nothing for a serial scan, whose units are whole parts.
 */
extern void ch_scan_claim_alone(TableScanDesc scan);

/*
 * F7 (VC30): asked as a unit's granules are selected, of a part of a table of
 * an engine whose storage is not temporary, by its identity (ch_part_unit_id())
 * and its granules' count: NULL passes none over, else a bit a granule, set
 * for one the scan need not read -- no row of it is true of the quals of the
 * caller's that keeps the bits.  Valid until the next call.
 */
typedef const uint64 *(*ChGranuleSkip) (void *arg, const uint64 *unit_id, uint32 ngranules, uint32 begin,
										uint32 end);
extern void ch_scan_set_granule_skip(TableScanDesc scan, ChGranuleSkip skip, void *arg);

/*
 * VC43 (pg_accel's §3.38, F25): a running bound's check of a part's granules
 * [begin, end): keep's bit m - begin set for a granule the bound may leave a
 * row of, cleared for one it leaves none of, by its least or greatest value;
 * false where it knows none of them, every granule read.  The scan applies it
 * to each unit it claims, and -- told the bound moved -- to the granules of the
 * unit it reads that its reader has not begun.
 */
typedef bool (*ChGranuleCheck) (void *arg, const ChPart *part, uint32 begin, uint32 end, uint64 *keep);
extern void ch_scan_set_granule_check(TableScanDesc scan, ChGranuleCheck check, void *arg);
extern void ch_scan_bound_moved(TableScanDesc scan);
/* W21: granules of a unit passed over for the device by the caller's granule skip, counted */
extern void ch_scan_note_skipped(TableScanDesc scan, int64 n);
/* W21: the unit being read here has the caller's granule skip applied already: skip_granules() asks no more */
extern void ch_scan_granules_cut(TableScanDesc scan);
/* W21: the claimed unit let go unread -- every granule of it passed over -- not counted as passed to the device */
extern void ch_scan_drop_unit(TableScanDesc scan);
/* A part's identity, never another's: of its storage, name, rows and time of writing; zeros for a temporary table's */
extern void ch_part_unit_id(const ChTable *t, const ChPart *part, uint64 *id);

/* Between batches: the next unit, claimed, which the next batch reads unless passed */
extern bool ch_scan_claim_unit(TableScanDesc scan, ChScanUnit *unit);
extern void ch_scan_pass_unit(TableScanDesc scan);
/* CH8: the claimed unit's part's deletions, ascending */
extern int	ch_scan_unit_dead(TableScanDesc scan, const uint64 **dead);

/*
 * V8e: the claimed unit's batches alone, then an end; its units of its part,
 * the filter's; and a unit passed over read again by the next batches alone
 */
extern void ch_scan_one_unit(TableScanDesc scan);
extern int	ch_scan_unit_parts(TableScanDesc scan, int32 **parts, chte_mark_range **ranges, int *nranges);
extern void ch_scan_read_unit(TableScanDesc scan, const ChPart *part, const chte_mark_range *ranges, int nranges,
							  uint64 rows);

extern ChTable *ch_scan_table(TableScanDesc scan);
extern const ChScanCounts *ch_scan_counts(TableScanDesc scan);

/*
 * The rows and the compressed bytes a scan of these columns, the row id if
 * asked, would read in the granules the quals keep, over the snapshot's parts
 * (NULL: the catalog's); and, where raw is not NULL, those columns'
 * uncompressed bytes there.
 */
extern void ch_estimate(Relation rel, Snapshot snapshot, int n, const AttrNumber *attnums, bool rowid,
						List *quals, double *rows, double *bytes, double *raw);
extern void ch_estimate_parallel(Relation rel, Snapshot snapshot, int n, const AttrNumber *attnums, bool rowid,
								 List *quals, double *rows, double *bytes, double *raw, double *punits);

/* filter.c: ANDed quals over a table's Vars as a filter of the library's, or NULL */
extern chte_filter *ch_filter_make(ChTable *t, List *quals, ChFilterKey *key);
extern chte_filter *ch_filter_make_keys(ChTable *t, List *quals, int nkeys, const ScanKeyData *keys,
										ChFilterKey *key);
extern List *ch_filter_quals(ChTable *t, List *quals);

/*
 * A part's column by its statistics: its NULLs, and the least and greatest of
 * its values that are not NULL where it has any (*has_values); false where the
 * part keeps no such statistics of it.
 */
extern bool ch_part_minmax(ChTable *t, const ChPart *part, AttrNumber attnum, bool *has_values,
						   Datum *min, Datum *max, int64 *nulls);
extern bool ch_part_nulls(ChTable *t, const ChPart *part, AttrNumber attnum, int64 *nulls);
extern bool ch_column_minmax_kept(const ChColumn *col);
/* The parts a serial scan reads, its units, and whether the snapshot sees a deleted row */
extern void ch_estimate_units(Relation rel, Snapshot snapshot, double *units, bool *deleted);
extern double ch_kept_parallel_units(int nparts, const int32 *marks, const int64 *rows, const uint64 *kept_rows,
									  const double *gbytes);

/*
 * Rows by their TIDs (VC16): of the columns attnums[0 .. n), 1-based, or of
 * every one where n < 0, the slot's other attributes NULL; through a cache
 * of the snapshot's parts and deletions, located row ids and a granule kept
 * (ch_fetch_cache_create()), which its context's reset or deletion releases.
 */
typedef struct ChFetchCache ChFetchCache;
extern ChFetchCache *ch_fetch_cache_create(MemoryContext parent, Relation rel, int n, const AttrNumber *attnums);
extern void ch_fetch_cache_free(ChFetchCache *cache);
extern bool ch_fetch_row_cached(Relation rel, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot,
								ChFetchCache *cache);
extern bool ch_fetch_row_columns(Relation rel, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot,
								 int n, const AttrNumber *attnums);

/*
 * The order a serial scan's rows come in (VC4): the table's key, as far as
 * its order's equality is its columns' types' own (ch_table_ordering()), and
 * how far parts read one after another keep it -- 2 every key column, 1 the
 * first, 0 none (ch_parts_order()); a serial scan's parts put in that order
 * before its first batch (ch_scan_order_parts()).
 */
extern int	ch_table_ordering(Relation rel, int max, AttrNumber *attnums);
extern int	ch_parts_order(Relation rel, ChTable *t, List *parts, AttrNumber first_key, List **ordered);
extern int	ch_scan_order_parts(TableScanDesc scan, AttrNumber first_key);

/* What fetches by TID read in this backend (ch.fetch_stats()) */
typedef struct ChFetchStats
{
	int64		fetches;		/* rows read from a part by their TIDs */
	int64		values;			/* the columns decoded for them, summed */
	int64		whole;			/* those rows read of every column */
	int64		kept;			/* rows taken from a granule kept from a fetch before */
	int64		located;		/* row ids found among offsets located before */
	int64		pointed;		/* VC34: rows read where they lie */
} ChFetchStats;
extern ChFetchStats ch_fetch_counts;

/*
 * fetch.c: the columns a fetch into a slot reads, where they are known --
 * the scans of the running statements' plans, and a batch source's caller's
 * (fetch_columns()) -- and the slot's cache, made at its first fetch
 */
extern bool ch_fetch_slot_columns(Relation rel, TupleTableSlot *slot, int *n, const AttrNumber **attnums);
extern ChFetchCache *ch_fetch_cache_for(Relation rel, TupleTableSlot *slot);
extern void ch_fetch_set_columns(Relation rel, TupleTableSlot *slot, int n, const AttrNumber *attnums);

#endif							/* CHTE_SCAN_H */
