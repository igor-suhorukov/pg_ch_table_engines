/*-------------------------------------------------------------------------
 *
 * fold.h
 *	  The rows every snapshot sees deleted, as a fold and a merge leave them
 *	  out of the parts they write (vacuum.c, merge.c).
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#ifndef CHTE_FOLD_H
#define CHTE_FOLD_H

#include "chte.h"
#include "tombstones.h"

/* The row ids of the deletions every snapshot sees, ascending, and their tombstones */
typedef struct ChDeadRows
{
	uint64	   *ids;
	int			n;
	ChTombstone *tombs;			/* the tombstones of those rows */
	int			ntombs;
} ChDeadRows;

extern void ch_dead_rows(Relation rel, ChTable *t, ChDeadRows *dead);
/* The dead rows a part holds, ascending, into *mine (palloc'd); each found marked in found[] */
extern int	ch_dead_rows_of_part(ChTable *t, const ChPart *p, const ChDeadRows *dead, uint64 **mine, bool *found);
/* The indexes' entries of the rows ids[0 .. n), ascending, removed, each index cleaned up */
extern void ch_dead_rows_unindex(Relation rel, const uint64 *ids, int n);
/* The tombstones of the rows ids[0 .. n), ascending, deleted */
extern void ch_dead_rows_forget(ChTable *t, const ChDeadRows *dead, const uint64 *ids, int n);
/* The part written again without its dead rows: a writer of its parts (none: every row dead) */
extern chte_writer *ch_fold_part(ChTable *t, const ChPart *p, const uint64 *mine, int nmine, size_t *nparts,
								 ResourceOwner *owner);

#endif							/* CHTE_FOLD_H */
