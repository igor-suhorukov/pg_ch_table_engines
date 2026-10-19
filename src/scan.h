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
} ChScanCounts;

/*
 * Before the first batch: the attributes read (1..natts), the row id if asked;
 * none at all reads no column, the rows only.
 */
extern void ch_scan_set_columns(TableScanDesc scan, int n, const AttrNumber *attnums, bool rowid);

/* Before the first batch: quals to prune by (filter.c); false when none could be */
extern bool ch_scan_set_filter(TableScanDesc scan, List *quals);

extern bool ch_scan_set_dictionaries(TableScanDesc scan, const bool *keep);
extern bool ch_scan_next_batch(TableScanDesc scan, ChScanBatch *batch);

/*
 * A check of each unit as it is claimed, before anything of it is read: false
 * passes it over (unit->rows is not set).
 */
typedef bool (*ChUnitCheck) (void *arg, const ChScanUnit *unit);
extern void ch_scan_set_unit_check(TableScanDesc scan, ChUnitCheck check, void *arg);

/* Between batches: the next unit, claimed, which the next batch reads unless passed */
extern bool ch_scan_claim_unit(TableScanDesc scan, ChScanUnit *unit);
extern void ch_scan_pass_unit(TableScanDesc scan);

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

/* filter.c: ANDed quals over a table's Vars as a filter of the library's, or NULL */
extern chte_filter *ch_filter_make(ChTable *t, List *quals);
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
