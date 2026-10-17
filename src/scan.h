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
	chte_reader *reader;		/* NULL for a scan of no column */
} ChScanBatch;

/* A unit, claimed and not yet read */
typedef struct ChScanUnit
{
	const ChPart *part;
	bool		whole;			/* the whole part, unfiltered: its statistics are the unit's */
	uint64		rows;			/* the rows it holds */
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

#endif							/* CHTE_SCAN_H */
