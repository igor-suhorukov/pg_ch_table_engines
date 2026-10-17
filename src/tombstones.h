/*-------------------------------------------------------------------------
 *
 * tombstones.h
 *	  ch.tombstones (tombstones.c): rows' deletions, updates and locks.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#ifndef CHTE_TOMBSTONES_H
#define CHTE_TOMBSTONES_H

#include "chte.h"

#define CH_TOMB_DELETED 'd'
#define CH_TOMB_UPDATED 'u'
#define CH_TOMB_LOCKED 'l'

/* What a writer of a row finds, as HeapTupleSatisfiesUpdate() tells it of a tuple */
typedef enum ChRowStatus
{
	CH_ROW_LIVE,				/* not deleted, nor updated, nor locked by another */
	CH_ROW_SELF_MODIFIED,		/* deleted or updated by the running transaction */
	CH_ROW_BEING_MODIFIED,		/* deleted, updated or locked by one in progress */
	CH_ROW_MODIFIED				/* deleted or updated by one that committed */
} ChRowStatus;

typedef struct ChRowState
{
	ChRowStatus status;
	TransactionId xid;			/* the writer, but for LIVE */
	CommandId	cid;			/* SELF_MODIFIED: its command */
	bool		lock_only;		/* BEING_MODIFIED: a lock only */
	bool		updated;		/* an update, its new version successor */
	uint64		successor;
	bool		locked_by_us;	/* the running transaction holds a lock on it */
} ChRowState;

/* A deletion or update, with its writer's state */
typedef struct ChTombstone
{
	uint64		rowid;
	char		kind;
	uint64		successor;
	TransactionId xmin;
	ItemPointerData tid;		/* its catalog row's */
	bool		ours;			/* the running transaction's */
	bool		in_progress;	/* another's in progress */
	bool		committed;
} ChTombstone;

extern void ch_row_state(const ChStorageId *id, uint64 rowid, ChRowState *state);
extern void ch_tombstone_insert(const ChStorageId *id, uint64 rowid, char kind, uint64 successor, CommandId cid);
extern uint64 *ch_tombstones_visible(const ChStorageId *id, Snapshot snapshot, uint64 lo, uint64 hi, int *n);
extern bool ch_tombstone_visible(const ChStorageId *id, Snapshot snapshot, uint64 rowid, uint64 *successor);
extern bool ch_tombstones_any(const ChStorageId *id);
extern ChTombstone *ch_tombstones_all(const ChStorageId *id, uint64 lo, uint64 hi, int *n);
extern void ch_tombstones_delete_all(const ChStorageId *id);
extern void ch_tombstones_delete(const ChStorageId *id, const ChTombstone *tombs, int n);
extern void ch_tombstones_copy(const ChStorageId *from, const ChStorageId *to);
extern void ch_tombstones_conflict_out(Relation rel, const ChStorageId *id, Snapshot snapshot);
extern void ch_tombstones_xact_callback(XactEvent event);

/* qsort's order of row ids */
static inline int
ch_compare_uint64(const void *a, const void *b)
{
	uint64		x = *(const uint64 *) a;
	uint64		y = *(const uint64 *) b;

	return x < y ? -1 : x > y ? 1 : 0;
}

/* Whether a sorted array of row ids holds one */
static inline bool
ch_rowid_in(const uint64 *ids, int n, uint64 rowid)
{
	int			lo = 0;
	int			hi = n;

	while (lo < hi)
	{
		int			mid = lo + (hi - lo) / 2;

		if (ids[mid] < rowid)
			lo = mid + 1;
		else
			hi = mid;
	}
	return lo < n && ids[lo] == rowid;
}

#endif							/* CHTE_TOMBSTONES_H */
