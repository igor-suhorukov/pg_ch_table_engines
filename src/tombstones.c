/*-------------------------------------------------------------------------
 *
 * tombstones.c
 *	  ch.tombstones and its UNLOGGED twin (ch_table_engines.md §2.10): a
 *	  row's deletion, an update's -- naming the row id of the new version --
 *	  or a lock on the row, each a catalog row keyed by the row's id, whose
 *	  visibility is heap's.
 *
 *	  A row of a visible part is visible to a snapshot when no deletion or
 *	  update the snapshot sees names it (§2.6, MVCC rule 2).  A deletion or
 *	  an update is inserted under its command's id, so the command itself
 *	  still sees the row and later ones do not, as heap's xmax and cmax
 *	  have it.
 *
 *	  What a writer of a row asks -- whether another transaction is deleting,
 *	  updating or locking it, has done so and committed, or the running one
 *	  has -- is ch_row_state()'s answer, read with SnapshotAny and each
 *	  catalog row's writer judged as HeapTupleSatisfiesUpdate() judges a
 *	  tuple's xmax (heapam_visibility.c).  Writers of a row take its tuple
 *	  lock first (dml.c), so that no two of them insert at once.
 *
 *	  A lock holds while its transaction runs, and means nothing after:
 *	  a committed or aborted locker's row is passed over, and the running
 *	  transaction deletes its own lock rows before it commits.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "miscadmin.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "chte.h"
#include "tombstones.h"

/* ch.tombstones's columns */
#define Natts_tombstones 6
#define Anum_tombstones_tablespace 1
#define Anum_tombstones_relfilenumber 2
#define Anum_tombstones_backend 3
#define Anum_tombstones_rowid 4
#define Anum_tombstones_kind 5
#define Anum_tombstones_successor 6

/* The storage's keys, and the row id's when one is given */
static int
row_keys(const ChStorageId *id, ScanKeyData *keys, StrategyNumber strategy, uint64 rowid)
{
	int			n = ch_catalog_storage_keys(id, keys);

	if (strategy != InvalidStrategy)
		ScanKeyInit(&keys[n++], Anum_tombstones_rowid, strategy,
					strategy == BTEqualStrategyNumber ? F_INT8EQ :
					strategy == BTGreaterEqualStrategyNumber ? F_INT8GE : F_INT8LE,
					Int64GetDatum((int64) rowid));
	return n;
}

static void
deform(Relation catalog, HeapTuple tuple, uint64 *rowid, char *kind, uint64 *successor)
{
	Datum		values[Natts_tombstones];
	bool		nulls[Natts_tombstones];

	heap_deform_tuple(tuple, RelationGetDescr(catalog), values, nulls);
	*rowid = (uint64) DatumGetInt64(values[Anum_tombstones_rowid - 1]);
	*kind = DatumGetChar(values[Anum_tombstones_kind - 1]);
	*successor = nulls[Anum_tombstones_successor - 1] ? 0 :
		(uint64) DatumGetInt64(values[Anum_tombstones_successor - 1]);
}

/*
 * Whether a catalog row is gone: deleted by the running transaction or by
 * one that committed.  Only lock rows are deleted while their table lives.
 */
static bool
row_deleted(HeapTupleHeader h)
{
	TransactionId xmax;

	if (h->t_infomask & HEAP_XMAX_INVALID || HEAP_XMAX_IS_LOCKED_ONLY(h->t_infomask))
		return false;
	if (h->t_infomask & HEAP_XMAX_IS_MULTI)
		return false;			/* only ever locked by a multixact, never deleted by one */
	xmax = HeapTupleHeaderGetRawXmax(h);
	if (!TransactionIdIsValid(xmax))
		return false;
	if (TransactionIdIsCurrentTransactionId(xmax))
		return true;
	if (TransactionIdIsInProgress(xmax))
		return false;
	return TransactionIdDidCommit(xmax);
}

typedef enum WriterState
{
	WRITER_ABORTED,
	WRITER_US,
	WRITER_IN_PROGRESS,
	WRITER_COMMITTED
} WriterState;

/* The state of a catalog row's inserter, checked in progress before committed (heapam_visibility.c) */
static WriterState
writer_state(TransactionId xmin, HeapTupleHeader h)
{
	if (HeapTupleHeaderXminFrozen(h))
		return WRITER_COMMITTED;
	if (h->t_infomask & HEAP_XMIN_INVALID)
		return WRITER_ABORTED;
	if (TransactionIdIsCurrentTransactionId(xmin))
		return WRITER_US;
	if (h->t_infomask & HEAP_XMIN_COMMITTED)
		return WRITER_COMMITTED;
	if (TransactionIdIsInProgress(xmin))
		return WRITER_IN_PROGRESS;
	if (TransactionIdDidCommit(xmin))
		return WRITER_COMMITTED;
	return WRITER_ABORTED;		/* aborted, or crashed */
}

void
ch_row_state(const ChStorageId *id, uint64 rowid, ChRowState *state)
{
	Relation	catalog;
	SysScanDesc scan;
	ScanKeyData keys[4];
	HeapTuple	tuple;
	int			nkeys;

	memset(state, 0, sizeof(*state));
	state->status = CH_ROW_LIVE;
	catalog = ch_catalog_open("tombstones", id, AccessShareLock);
	nkeys = row_keys(id, keys, BTEqualStrategyNumber, rowid);
	scan = systable_beginscan(catalog, ch_catalog_index(catalog), true, SnapshotAny, nkeys, keys);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		HeapTupleHeader h = tuple->t_data;
		TransactionId xmin = HeapTupleHeaderGetRawXmin(h);
		uint64		rid;
		char		kind;
		uint64		successor;
		WriterState ws;

		if (row_deleted(h))
			continue;
		ws = writer_state(xmin, h);
		if (ws == WRITER_ABORTED)
			continue;
		deform(catalog, tuple, &rid, &kind, &successor);
		if (kind == CH_TOMB_LOCKED)
		{
			if (ws == WRITER_US)
				state->locked_by_us = true;
			else if (ws == WRITER_IN_PROGRESS && state->status == CH_ROW_LIVE)
			{
				state->status = CH_ROW_BEING_MODIFIED;
				state->xid = xmin;
				state->lock_only = true;
			}
			/* a committed locker's row means nothing */
			continue;
		}
		switch (ws)
		{
			case WRITER_COMMITTED:
				/* a committed deletion or update outranks anything else */
				state->status = CH_ROW_MODIFIED;
				state->xid = xmin;
				state->updated = kind == CH_TOMB_UPDATED;
				state->successor = successor;
				state->lock_only = false;
				break;
			case WRITER_IN_PROGRESS:
				if (state->status != CH_ROW_MODIFIED)
				{
					state->status = CH_ROW_BEING_MODIFIED;
					state->xid = xmin;
					state->lock_only = false;
					state->updated = kind == CH_TOMB_UPDATED;
					state->successor = successor;
				}
				break;
			case WRITER_US:
				if (state->status == CH_ROW_LIVE ||
					(state->status == CH_ROW_BEING_MODIFIED && state->lock_only))
				{
					state->status = CH_ROW_SELF_MODIFIED;
					state->xid = xmin;
					state->cid = HeapTupleHeaderGetCmin(h);
					state->updated = kind == CH_TOMB_UPDATED;
					state->successor = successor;
					state->lock_only = false;
				}
				break;
			case WRITER_ABORTED:
				break;
		}
	}
	systable_endscan(scan);
	table_close(catalog, AccessShareLock);
}

/* The running transaction's lock rows, deleted before it commits */
typedef struct OwnLock
{
	Oid			catalog;
	ItemPointerData tid;
} OwnLock;

static List *own_locks = NIL;	/* in TopTransactionContext */

void
ch_tombstone_insert(const ChStorageId *id, uint64 rowid, char kind, uint64 successor, CommandId cid)
{
	Relation	catalog;
	Relation	index;
	Datum		values[Natts_tombstones];
	bool		nulls[Natts_tombstones];
	Datum		ivalues[INDEX_MAX_KEYS];
	bool		inulls[INDEX_MAX_KEYS];
	HeapTuple	tuple;
	IndexInfo  *ii;

	catalog = ch_catalog_open("tombstones", id, RowExclusiveLock);
	memset(nulls, false, sizeof(nulls));
	values[Anum_tombstones_tablespace - 1] = ObjectIdGetDatum(id->locator.spcOid);
	values[Anum_tombstones_relfilenumber - 1] = ObjectIdGetDatum(id->locator.relNumber);
	values[Anum_tombstones_backend - 1] = Int32GetDatum(id->backend);
	values[Anum_tombstones_rowid - 1] = Int64GetDatum((int64) rowid);
	values[Anum_tombstones_kind - 1] = CharGetDatum(kind);
	if (kind == CH_TOMB_UPDATED)
		values[Anum_tombstones_successor - 1] = Int64GetDatum((int64) successor);
	else
		nulls[Anum_tombstones_successor - 1] = true;
	tuple = heap_form_tuple(RelationGetDescr(catalog), values, nulls);
	heap_insert(catalog, tuple, cid, 0, NULL);

	/* the index's columns are the table's first four */
	index = index_open(ch_catalog_index(catalog), RowExclusiveLock);
	ii = BuildIndexInfo(index);
	for (int i = 0; i < 4; i++)
	{
		ivalues[i] = values[i];
		inulls[i] = false;
	}
	(void) index_insert(index, ivalues, inulls, &tuple->t_self, catalog, UNIQUE_CHECK_NO, false, ii);
	index_close(index, RowExclusiveLock);

	if (kind == CH_TOMB_LOCKED)
	{
		MemoryContext old = MemoryContextSwitchTo(TopTransactionContext);
		OwnLock    *l = palloc(sizeof(OwnLock));

		l->catalog = RelationGetRelid(catalog);
		l->tid = tuple->t_self;
		own_locks = lappend(own_locks, l);
		MemoryContextSwitchTo(old);
	}
	heap_freetuple(tuple);
	table_close(catalog, RowExclusiveLock);
}

/*
 * Before the transaction commits: its lock rows go, those of subtransactions
 * that aborted being gone already.  A prepared transaction keeps its locks.
 */
static void
release_own_locks(void)
{
	ListCell   *lc;
	Relation	catalog = NULL;

	/* the last command's lock rows were inserted under its id: deleted under the next */
	CommandCounterIncrement();
	foreach(lc, own_locks)
	{
		OwnLock    *l = lfirst(lc);
		HeapTupleData tuple;
		Buffer		buffer;
		bool		ours;

		if (catalog == NULL || RelationGetRelid(catalog) != l->catalog)
		{
			if (catalog)
				table_close(catalog, RowExclusiveLock);
			catalog = table_open(l->catalog, RowExclusiveLock);
		}
		tuple.t_self = l->tid;
		if (!heap_fetch(catalog, SnapshotAny, &tuple, &buffer, false))
			continue;
		ours = TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple.t_data)) &&
			(tuple.t_data->t_infomask & HEAP_XMAX_INVALID);
		ReleaseBuffer(buffer);
		if (ours)
			simple_heap_delete(catalog, &l->tid);
	}
	if (catalog)
		table_close(catalog, RowExclusiveLock);
	own_locks = NIL;
}

void
ch_tombstones_xact_callback(XactEvent event)
{
	switch (event)
	{
		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
			if (own_locks != NIL)
				release_own_locks();
			break;
		case XACT_EVENT_PRE_PREPARE:
			/* a prepared transaction keeps its locks until it ends */
			break;
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
		case XACT_EVENT_PREPARE:
			/* TopTransactionContext takes the list with it */
			own_locks = NIL;
			break;
	}
}

/* Row ids of deletions and updates in [lo, hi] the snapshot sees, ascending, without repeats */
uint64 *
ch_tombstones_visible(const ChStorageId *id, Snapshot snapshot, uint64 lo, uint64 hi, int *n)
{
	Relation	catalog;
	SysScanDesc scan;
	ScanKeyData keys[5];
	HeapTuple	tuple;
	int			nkeys;
	int			cap = 0;
	uint64	   *ids = NULL;

	*n = 0;
	catalog = ch_catalog_open("tombstones", id, AccessShareLock);
	nkeys = ch_catalog_storage_keys(id, keys);
	ScanKeyInit(&keys[nkeys++], Anum_tombstones_rowid, BTGreaterEqualStrategyNumber, F_INT8GE, Int64GetDatum((int64) lo));
	ScanKeyInit(&keys[nkeys++], Anum_tombstones_rowid, BTLessEqualStrategyNumber, F_INT8LE, Int64GetDatum((int64) hi));
	scan = systable_beginscan(catalog, ch_catalog_index(catalog), true, snapshot, nkeys, keys);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		uint64		rowid;
		char		kind;
		uint64		successor;

		deform(catalog, tuple, &rowid, &kind, &successor);
		if (kind == CH_TOMB_LOCKED)
			continue;
		if (*n > 0 && ids[*n - 1] == rowid)
			continue;
		if (*n == cap)
		{
			cap = cap ? cap * 2 : 64;
			ids = ids ? repalloc_huge(ids, sizeof(uint64) * cap) : palloc_extended(sizeof(uint64) * cap, MCXT_ALLOC_HUGE);
		}
		ids[(*n)++] = rowid;
	}
	systable_endscan(scan);
	table_close(catalog, AccessShareLock);
	return ids;
}

/* Whether the snapshot sees any deletion or update of the storage's rows: the first one found ends the scan */
bool
ch_tombstones_visible_any(const ChStorageId *id, Snapshot snapshot)
{
	Relation	catalog;
	SysScanDesc scan;
	ScanKeyData keys[3];
	HeapTuple	tuple;
	int			nkeys;
	bool		found = false;

	catalog = ch_catalog_open("tombstones", id, AccessShareLock);
	nkeys = ch_catalog_storage_keys(id, keys);
	scan = systable_beginscan(catalog, ch_catalog_index(catalog), true, snapshot, nkeys, keys);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		uint64		rowid;
		char		kind;
		uint64		successor;

		deform(catalog, tuple, &rowid, &kind, &successor);
		if (kind == CH_TOMB_LOCKED)
			continue;
		found = true;
		break;
	}
	systable_endscan(scan);
	table_close(catalog, AccessShareLock);
	return found;
}

/* Whether the snapshot sees a deletion or update of the row: an update's new row id into *successor */
bool
ch_tombstone_visible(const ChStorageId *id, Snapshot snapshot, uint64 rowid, uint64 *successor)
{
	Relation	catalog;
	SysScanDesc scan;
	ScanKeyData keys[4];
	HeapTuple	tuple;
	int			nkeys;
	bool		found = false;

	if (successor)
		*successor = 0;
	catalog = ch_catalog_open("tombstones", id, AccessShareLock);
	nkeys = row_keys(id, keys, BTEqualStrategyNumber, rowid);
	scan = systable_beginscan(catalog, ch_catalog_index(catalog), true, snapshot, nkeys, keys);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		uint64		rid;
		char		kind;
		uint64		succ;

		deform(catalog, tuple, &rid, &kind, &succ);
		if (kind == CH_TOMB_LOCKED)
			continue;
		found = true;
		if (successor && kind == CH_TOMB_UPDATED)
			*successor = succ;
		break;
	}
	systable_endscan(scan);
	table_close(catalog, AccessShareLock);
	return found;
}

/* Whether a storage has any deletion or update at all, visible to anyone or not */
bool
ch_tombstones_any(const ChStorageId *id)
{
	Relation	catalog;
	SysScanDesc scan;
	ScanKeyData keys[3];
	HeapTuple	tuple;
	int			nkeys;
	bool		any = false;

	catalog = ch_catalog_open("tombstones", id, AccessShareLock);
	nkeys = ch_catalog_storage_keys(id, keys);
	scan = systable_beginscan(catalog, ch_catalog_index(catalog), true, SnapshotAny, nkeys, keys);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		uint64		rid;
		char		kind;
		uint64		succ;

		deform(catalog, tuple, &rid, &kind, &succ);
		if (kind != CH_TOMB_LOCKED)
		{
			any = true;
			break;
		}
	}
	systable_endscan(scan);
	table_close(catalog, AccessShareLock);
	return any;
}

/*
 * Every deletion and update of a storage in [lo, hi] (SnapshotAny), with its
 * writer's state, ascending: index builds and VACUUM judge each row by them.
 */
ChTombstone *
ch_tombstones_all(const ChStorageId *id, uint64 lo, uint64 hi, int *n)
{
	Relation	catalog;
	SysScanDesc scan;
	ScanKeyData keys[5];
	HeapTuple	tuple;
	int			nkeys;
	int			cap = 0;
	ChTombstone *all = NULL;

	*n = 0;
	catalog = ch_catalog_open("tombstones", id, AccessShareLock);
	nkeys = ch_catalog_storage_keys(id, keys);
	ScanKeyInit(&keys[nkeys++], Anum_tombstones_rowid, BTGreaterEqualStrategyNumber, F_INT8GE, Int64GetDatum((int64) lo));
	ScanKeyInit(&keys[nkeys++], Anum_tombstones_rowid, BTLessEqualStrategyNumber, F_INT8LE, Int64GetDatum((int64) hi));
	scan = systable_beginscan(catalog, ch_catalog_index(catalog), true, SnapshotAny, nkeys, keys);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		HeapTupleHeader h = tuple->t_data;
		TransactionId xmin = HeapTupleHeaderGetRawXmin(h);
		ChTombstone *t;
		uint64		rowid;
		char		kind;
		uint64		successor;
		WriterState ws;

		if (row_deleted(h))
			continue;
		deform(catalog, tuple, &rowid, &kind, &successor);
		if (kind == CH_TOMB_LOCKED)
			continue;
		ws = writer_state(xmin, h);
		if (ws == WRITER_ABORTED)
			continue;
		if (*n == cap)
		{
			cap = cap ? cap * 2 : 64;
			all = all ? repalloc_huge(all, sizeof(ChTombstone) * cap) :
				palloc_extended(sizeof(ChTombstone) * cap, MCXT_ALLOC_HUGE);
		}
		t = &all[(*n)++];
		t->rowid = rowid;
		t->kind = kind;
		t->successor = successor;
		t->xmin = HeapTupleHeaderXminFrozen(h) ? FrozenTransactionId : xmin;
		t->tid = tuple->t_self;
		t->ours = ws == WRITER_US;
		t->in_progress = ws == WRITER_IN_PROGRESS;
		t->committed = ws == WRITER_COMMITTED;
	}
	systable_endscan(scan);
	table_close(catalog, AccessShareLock);
	return all;
}

/* Deletes every tombstone row of a storage the transaction can see (TRUNCATE, DROP, a rewrite) */
void
ch_tombstones_delete_all(const ChStorageId *id)
{
	Relation	catalog;
	SysScanDesc scan;
	ScanKeyData keys[3];
	HeapTuple	tuple;
	int			nkeys;

	catalog = ch_catalog_open("tombstones", id, RowExclusiveLock);
	nkeys = ch_catalog_storage_keys(id, keys);
	scan = systable_beginscan(catalog, ch_catalog_index(catalog), true, NULL, nkeys, keys);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
		simple_heap_delete(catalog, &tuple->t_self);
	systable_endscan(scan);
	table_close(catalog, RowExclusiveLock);
}

/* Deletes the given tombstone rows (VACUUM, once folded into their parts) */
void
ch_tombstones_delete(const ChStorageId *id, const ChTombstone *tombs, int n)
{
	Relation	catalog = ch_catalog_open("tombstones", id, RowExclusiveLock);

	for (int i = 0; i < n; i++)
		simple_heap_delete(catalog, (ItemPointer) &tombs[i].tid);
	table_close(catalog, RowExclusiveLock);
}

/*
 * A storage's deletions and updates for its copy (SET TABLESPACE, a rewrite
 * that keeps row ids): the committed ones the copy's rows need, written by
 * the copying transaction.
 */
void
ch_tombstones_copy(const ChStorageId *from, const ChStorageId *to)
{
	int			n;
	ChTombstone *all = ch_tombstones_all(from, 0, PG_INT64_MAX, &n);
	CommandId	cid = GetCurrentCommandId(true);

	for (int i = 0; i < n; i++)
		if (all[i].committed || all[i].ours)
			ch_tombstone_insert(to, all[i].rowid, all[i].kind, all[i].successor, cid);
	if (all)
		pfree(all);
}

/*
 * SERIALIZABLE (§2.6): every writer of a storage's deletions and updates,
 * visible to the snapshot or not, reported to SSI's conflict-out check, which
 * passes over the ones that are not concurrent.
 */
void
ch_tombstones_conflict_out(Relation rel, const ChStorageId *id, Snapshot snapshot)
{
	Relation	catalog;
	SysScanDesc scan;
	ScanKeyData keys[3];
	HeapTuple	tuple;
	int			nkeys;
	TransactionId last = InvalidTransactionId;

	catalog = ch_catalog_open("tombstones", id, AccessShareLock);
	nkeys = ch_catalog_storage_keys(id, keys);
	scan = systable_beginscan(catalog, ch_catalog_index(catalog), true, SnapshotAny, nkeys, keys);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		TransactionId xmin = HeapTupleHeaderGetXmin(tuple->t_data);

		if (TransactionIdIsNormal(xmin) && !TransactionIdEquals(xmin, last))
		{
			CheckForSerializableConflictOut(rel, xmin, snapshot);
			last = xmin;
		}
	}
	systable_endscan(scan);
	table_close(catalog, AccessShareLock);
}
