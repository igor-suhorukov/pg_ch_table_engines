/*-------------------------------------------------------------------------
 *
 * dml.c
 *	  DELETE, UPDATE and row locks on the tables of the engines
 *	  (ch_table_engines.md §2.10), as heap_delete(), heap_update() and
 *	  heap_lock_tuple() do them (heapam.c), a row's deletion, update or lock
 *	  being a row of ch.tombstones (tombstones.c) where heap sets a tuple's
 *	  xmax.
 *
 *	  A writer of a row reads the row's state and inserts its own row under
 *	  the page lock of the row's TID block, as heap does both under the
 *	  buffer's content lock: no two writers of a row insert at once.  One
 *	  that finds another transaction in progress deleting, updating or
 *	  locking the row lets the page lock go, takes the row's tuple lock --
 *	  every mode as exclusive -- and waits for that transaction holding it,
 *	  as heap_acquire_tuplock() makes waiters queue in order; a writer that
 *	  need not wait, such as the row's own locker, takes no tuple lock, so
 *	  it never waits behind a waiter for its own transaction.
 *
 *	  An update is a deletion naming the row id of the new version, which
 *	  goes into the write buffer as an inserted row does, and becomes a part
 *	  with the statement's other rows; READ COMMITTED's EvalPlanQual follows
 *	  the name as heap follows t_ctid (tuple_lock with
 *	  TUPLE_LOCK_FLAG_FIND_LAST_VERSION, heapam_handler.c's
 *	  heapam_tuple_lock()).
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/tableam.h"
#include "access/xact.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "storage/predicate.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "chte.h"
#include "tombstones.h"

/* Every mode is exclusive (§2.10): the lock heap takes for LockTupleExclusive (heapam.c, tupleLockExtraInfo) */
#define CH_TUPLE_LOCK AccessExclusiveLock

/* What a row's state is read and a row of its own inserted under: its TID block's page lock */
static void
guard_row(Relation rel, ItemPointer tid)
{
	LockPage(rel, ItemPointerGetBlockNumber(tid), ExclusiveLock);
}

static void
unguard_row(Relation rel, ItemPointer tid)
{
	UnlockPage(rel, ItemPointerGetBlockNumber(tid), ExclusiveLock);
}

/*
 * The relation's storage, which its tombstones are keyed by, from the
 * relation itself: a table of the library's cache (table.c) goes at an
 * invalidation of it, which a lock taken below -- a catalog's, a page's --
 * may accept.
 */
static void
row_storage(Relation rel, ChStorageId *id)
{
	ch_storage_id(rel, &rel->rd_locator, id);
}

static void
set_tid(ItemPointer tid, uint64 rowid)
{
	ItemPointerSet(tid, CH_ROWID_TO_BLOCK(rowid), CH_ROWID_TO_OFFSET(rowid));
}

/* Takes the row's tuple lock by the wait policy: false where it would have to wait and may not */
static bool
lock_row(Relation rel, ItemPointer tid, LockWaitPolicy policy)
{
	switch (policy)
	{
		case LockWaitBlock:
			LockTuple(rel, tid, CH_TUPLE_LOCK);
			return true;
		case LockWaitSkip:
			return ConditionalLockTuple(rel, tid, CH_TUPLE_LOCK, false);
		case LockWaitError:
			if (!ConditionalLockTuple(rel, tid, CH_TUPLE_LOCK, log_lock_failures))
				ereport(ERROR,
						(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
						 errmsg("could not obtain lock on row in relation \"%s\"",
								RelationGetRelationName(rel))));
			return true;
	}
	return false;
}

/* Waits for a writer in progress by the wait policy: false where it may not */
static bool
wait_for(Relation rel, ItemPointer tid, TransactionId xid, LockWaitPolicy policy, XLTW_Oper oper)
{
	switch (policy)
	{
		case LockWaitBlock:
			XactLockTableWait(xid, rel, tid, oper);
			return true;
		case LockWaitSkip:
			return ConditionalXactLockTableWait(xid, false);
		case LockWaitError:
			if (!ConditionalXactLockTableWait(xid, log_lock_failures))
				ereport(ERROR,
						(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
						 errmsg("could not obtain lock on row in relation \"%s\"",
								RelationGetRelationName(rel))));
			return true;
	}
	return false;
}

static void
fill_failure(TM_FailureData *tmfd, const ChRowState *st, ItemPointer tid)
{
	if (st->updated)
		set_tid(&tmfd->ctid, st->successor);
	else
		tmfd->ctid = *tid;
	tmfd->xmax = st->xid;
	tmfd->cmax = st->status == CH_ROW_SELF_MODIFIED ? st->cid : InvalidCommandId;
}

/*
 * Whether the row is one the snapshot sees: in a part the snapshot sees, and
 * deleted by no one it sees -- the crosscheck of a REPEATABLE READ RI
 * action (heap_delete()'s and heap_update()'s).
 */
static bool
row_visible(Relation rel, uint64 rowid, Snapshot snapshot)
{
	TupleTableSlot *slot = MakeSingleTupleTableSlot(RelationGetDescr(rel), &ch_slot_ops);
	ItemPointerData tid;
	bool		found;

	set_tid(&tid, rowid);
	found = ch_fetch_row(rel, &tid, snapshot, slot);
	ExecDropSingleTupleTableSlot(slot);
	return found;
}

/*
 * A deletion of the row at tid, or an update's (kind, successor): its state
 * read under the page lock of its TID block, a writer in progress waited for
 * under the row's tuple lock, and the tombstone inserted under cid where
 * nothing is in the way.  The results are heap_delete()'s.
 */
static TM_Result
modify_row(Relation rel, ItemPointer tid, CommandId cid, Snapshot crosscheck, bool wait,
		   TM_FailureData *tmfd, char kind, uint64 successor)
{
	ChStorageId storage;
	uint64		rowid = CH_TID_TO_ROWID(tid);
	ChRowState	st;
	TM_Result	result;
	XLTW_Oper	oper = kind == CH_TOMB_UPDATED ? XLTW_Update : XLTW_Delete;
	bool		have_tuple_lock = false;

	if (IsInParallelMode())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("cannot %s tuples during a parallel operation",
						kind == CH_TOMB_UPDATED ? "update" : "delete")));

	row_storage(rel, &storage);
	for (;;)
	{
		guard_row(rel, tid);
		ch_row_state(&storage, rowid, &st);
		if (st.status == CH_ROW_BEING_MODIFIED && wait)
		{
			/* our place among the row's waiters, held until done (heap_acquire_tuplock()) */
			unguard_row(rel, tid);
			if (!have_tuple_lock)
			{
				LockTuple(rel, tid, CH_TUPLE_LOCK);
				have_tuple_lock = true;
			}
			wait_for(rel, tid, st.xid, LockWaitBlock, oper);
			continue;
		}
		break;
	}
	switch (st.status)
	{
		case CH_ROW_LIVE:
			result = TM_Ok;
			break;
		case CH_ROW_SELF_MODIFIED:
			/* by a later command, or this one, of the running transaction; by an earlier one, invisible */
			if (st.cid < cid)
			{
				unguard_row(rel, tid);
				if (have_tuple_lock)
					UnlockTuple(rel, tid, CH_TUPLE_LOCK);
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("attempted to %s invisible tuple",
								kind == CH_TOMB_UPDATED ? "update" : "delete")));
			}
			result = TM_SelfModified;
			break;
		case CH_ROW_BEING_MODIFIED:
			result = TM_BeingModified;
			break;
		case CH_ROW_MODIFIED:
		default:
			result = st.updated ? TM_Updated : TM_Deleted;
			break;
	}
	if (crosscheck != InvalidSnapshot && result == TM_Ok && !row_visible(rel, rowid, crosscheck))
	{
		/* in a part the crosscheck snapshot does not see: newer than it */
		result = TM_Updated;
		st.status = CH_ROW_MODIFIED;
		st.updated = false;
		st.xid = InvalidTransactionId;
	}
	if (result == TM_Ok)
	{
		/* SERIALIZABLE: a write of the relation, at the relation's grain (§2.6) */
		CheckForSerializableConflictIn(rel, NULL, InvalidBlockNumber);
		ch_tombstone_insert(&storage, rowid, kind, successor, cid);
	}
	else
		fill_failure(tmfd, &st, tid);
	unguard_row(rel, tid);
	if (have_tuple_lock)
		UnlockTuple(rel, tid, CH_TUPLE_LOCK);
	return result;
}

TM_Result
ch_tuple_delete(Relation rel, ItemPointer tid, CommandId cid, uint32 options, Snapshot snapshot,
				Snapshot crosscheck, bool wait, TM_FailureData *tmfd)
{
	TM_Result	result = modify_row(rel, tid, cid, crosscheck, wait, tmfd, CH_TOMB_DELETED, 0);

	/* as heap_delete() counts it */
	if (result == TM_Ok)
		pgstat_count_heap_delete(rel);
	return result;
}

/*
 * An update: the new version's row id taken first, which its tombstone
 * names, and the new row buffered under it once the old one is the
 * update's.  The TID changes, so every index takes the new row (TU_All).
 */
TM_Result
ch_tuple_update(Relation rel, ItemPointer otid, TupleTableSlot *slot, CommandId cid, uint32 options,
				Snapshot snapshot, Snapshot crosscheck, bool wait, TM_FailureData *tmfd,
				LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes)
{
	uint64		newid = ch_buffer_reserve_rowid(rel, cid);
	TM_Result	result;

	*lockmode = LockTupleExclusive;
	*update_indexes = TU_None;
	result = modify_row(rel, otid, cid, crosscheck, wait, tmfd, CH_TOMB_UPDATED, newid);
	if (result != TM_Ok)
		return result;
	ch_buffer_insert_rowid(rel, slot, cid, newid);
	*update_indexes = TU_All;
	/* as heap_update() counts it: never HOT, the row a new one */
	pgstat_count_heap_update(rel, false, false);
	return TM_Ok;
}

/*
 * A row lock (SELECT ... FOR UPDATE and the like, EvalPlanQual's lock of the
 * latest version): a lock row under cid, every mode as exclusive, the row in
 * *slot.  With TUPLE_LOCK_FLAG_FIND_LAST_VERSION an update by a committed
 * transaction is followed to its new version, which is locked instead, and
 * so on to the end of the chain, as heapam_tuple_lock() follows one: through
 * the running transaction's own updates too, its newest version, where a
 * command not earlier than cid wrote it, being TM_SelfModified with that
 * command's id, and one it deleted TM_Deleted.
 */
TM_Result
ch_tuple_lock(Relation rel, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot, CommandId cid,
			  LockTupleMode mode, LockWaitPolicy wait_policy, uint8 flags, TM_FailureData *tmfd)
{
	ChStorageId storage;
	ChRowState	st;
	bool		own_version = false;	/* the row a version the running transaction wrote */
	CommandId	own_cid = InvalidCommandId; /* by that command */

	tmfd->traversed = false;
	row_storage(rel, &storage);
	for (;;)
	{
		uint64		rowid = CH_TID_TO_ROWID(tid);
		bool		have_tuple_lock = false;
		bool		lock = false;

		for (;;)
		{
			guard_row(rel, tid);
			ch_row_state(&storage, rowid, &st);
			if (st.status == CH_ROW_BEING_MODIFIED)
			{
				unguard_row(rel, tid);
				if (!have_tuple_lock)
				{
					if (!lock_row(rel, tid, wait_policy))
						return TM_WouldBlock;
					have_tuple_lock = true;
				}
				if (!wait_for(rel, tid, st.xid, wait_policy, XLTW_Lock))
				{
					UnlockTuple(rel, tid, CH_TUPLE_LOCK);
					return TM_WouldBlock;
				}
				continue;
			}
			break;
		}
		/* a version this transaction's command at or after cid wrote is not the caller's to lock */
		lock = st.status == CH_ROW_LIVE && !(own_version && own_cid >= cid);
		if (lock && !st.locked_by_us)
			ch_tombstone_insert(&storage, rowid, CH_TOMB_LOCKED, 0, cid);
		unguard_row(rel, tid);
		if (have_tuple_lock)
			UnlockTuple(rel, tid, CH_TUPLE_LOCK);
		switch (st.status)
		{
			case CH_ROW_LIVE:
				if (!lock)
				{
					tmfd->ctid = *tid;
					tmfd->xmax = GetCurrentTransactionId();
					tmfd->cmax = own_cid;
					return TM_SelfModified;
				}
				if (!ch_fetch_row(rel, tid, SnapshotAny, slot))
					elog(ERROR, "row (%u,%u) of \"%s\" is in no part",
						 ItemPointerGetBlockNumber(tid), ItemPointerGetOffsetNumber(tid),
						 RelationGetRelationName(rel));
				return TM_Ok;
			case CH_ROW_SELF_MODIFIED:
				if (tmfd->traversed)
				{
					/* further along a chain: the running transaction's own update followed, its deletion the end */
					if (!st.updated)
						return TM_Deleted;
					own_version = true;
					own_cid = st.cid;
					set_tid(tid, st.successor);
					continue;
				}
				if (st.cid < cid)
					return TM_Invisible;
				fill_failure(tmfd, &st, tid);
				return TM_SelfModified;
			case CH_ROW_MODIFIED:
			default:
				if (st.updated && (flags & TUPLE_LOCK_FLAG_FIND_LAST_VERSION))
				{
					/* its new version, which the committed updater wrote: locked in turn */
					set_tid(tid, st.successor);
					tmfd->traversed = true;
					own_version = false;
					continue;
				}
				fill_failure(tmfd, &st, tid);
				return st.updated ? TM_Updated : TM_Deleted;
		}
	}
}

/*
 * The latest version of the row at tid the scan's snapshot sees: updates the
 * snapshot sees followed to their new rows (heap_get_latest_tid()).
 */
void
ch_tuple_get_latest_tid(TableScanDesc scan, ItemPointer tid)
{
	Relation	rel = scan->rs_rd;
	ChStorageId storage;
	Snapshot	snapshot = scan->rs_snapshot;

	row_storage(rel, &storage);
	for (;;)
	{
		uint64		successor;

		if (!ch_tombstone_visible(&storage, snapshot, CH_TID_TO_ROWID(tid), &successor) || successor == 0)
			return;
		set_tid(tid, successor);
	}
}
