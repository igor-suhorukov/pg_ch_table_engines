/*-------------------------------------------------------------------------
 *
 * catalog.c
 *	  The extension's catalogs, ch.tables and ch.parts, and their UNLOGGED
 *	  twins for UNLOGGED and temporary tables (ch_table_engines.md §2.6).
 *
 *	  A part is visible to a snapshot when its ch.parts row is.  The rows of
 *	  a statement's part are inserted under the command id their rows were
 *	  inserted with, so a command never sees its own rows and later commands
 *	  do (MVCC rule 3).
 *
 *	  A storage's row id and block counters move in place, as a sequence's
 *	  do: never back, gaps allowed.  An in-place update refuses a row the
 *	  running transaction inserted (heapam.c, heap_inplace_lock()), so the
 *	  counters of a storage made in the running transaction are kept in the
 *	  backend and written before it commits.
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
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "chte.h"

/* ch.tables's columns */
#define Natts_tables 9
#define Anum_tables_tablespace 1
#define Anum_tables_relfilenumber 2
#define Anum_tables_backend 3
#define Anum_tables_relid 4
#define Anum_tables_engine 5
#define Anum_tables_storage 6
#define Anum_tables_next_rowid 7
#define Anum_tables_next_block 8
#define Anum_tables_library 9

/* ch.parts's columns */
#define Natts_parts 10
#define Anum_parts_tablespace 1
#define Anum_parts_relfilenumber 2
#define Anum_parts_backend 3
#define Anum_parts_name 4
#define Anum_parts_rows 5
#define Anum_parts_bytes 6
#define Anum_parts_min_rowid 7
#define Anum_parts_max_rowid 8
#define Anum_parts_level 9
#define Anum_parts_manifest 10

static bool
twin(const ChStorageId *id)
{
	return id->persistence != RELPERSISTENCE_PERMANENT;
}

static Oid
catalog_relid(const char *name, bool unlogged)
{
	Oid			nsp = get_namespace_oid("ch", false);
	char		buf[NAMEDATALEN];
	Oid			relid;

	snprintf(buf, sizeof(buf), "%s%s", name, unlogged ? "_unlogged" : "");
	relid = get_relname_relid(buf, nsp);
	if (!OidIsValid(relid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("catalog ch.%s of extension ch_table_engines is missing", buf)));
	return relid;
}

static Relation
open_catalog(const char *name, const ChStorageId *id, LOCKMODE lockmode)
{
	return table_open(catalog_relid(name, twin(id)), lockmode);
}

/* The unique index of a catalog: its only index */
static Oid
catalog_index(Relation catalog)
{
	List	   *indexes = RelationGetIndexList(catalog);

	if (list_length(indexes) != 1)
		elog(ERROR, "catalog %s has %d indexes", RelationGetRelationName(catalog), list_length(indexes));
	return linitial_oid(indexes);
}

static int
storage_keys(const ChStorageId *id, ScanKeyData *keys)
{
	ScanKeyInit(&keys[0], 1, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(id->locator.spcOid));
	ScanKeyInit(&keys[1], 2, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(id->locator.relNumber));
	ScanKeyInit(&keys[2], 3, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(id->backend));
	return 3;
}

/* Inserts a catalog row and its index entry under the given command id */
static void
insert_with_cid(Relation catalog, HeapTuple tuple, CommandId cid)
{
	Relation	index;
	IndexInfo  *ii;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	TupleTableSlot *slot;
	EState	   *estate;

	heap_insert(catalog, tuple, cid, 0, NULL);

	index = index_open(catalog_index(catalog), RowExclusiveLock);
	ii = BuildIndexInfo(index);
	estate = CreateExecutorState();
	slot = MakeSingleTupleTableSlot(RelationGetDescr(catalog), &TTSOpsHeapTuple);
	ExecStoreHeapTuple(tuple, slot, false);
	GetPerTupleExprContext(estate)->ecxt_scantuple = slot;
	FormIndexDatum(ii, slot, estate, values, isnull);
	(void) index_insert(index, values, isnull, &tuple->t_self, catalog, UNIQUE_CHECK_YES, false, ii);
	ExecDropSingleTupleTableSlot(slot);
	FreeExecutorState(estate);
	index_close(index, RowExclusiveLock);
}

/* Updates a catalog row, and its index entry when the update is not HOT */
static void
update_with_index(Relation catalog, ItemPointer otid, HeapTuple newtup)
{
	TU_UpdateIndexes update_indexes;

	simple_heap_update(catalog, otid, newtup, &update_indexes);
	if (update_indexes != TU_None)
	{
		Relation	index = index_open(catalog_index(catalog), RowExclusiveLock);
		IndexInfo  *ii = BuildIndexInfo(index);
		EState	   *estate = CreateExecutorState();
		TupleTableSlot *slot = MakeSingleTupleTableSlot(RelationGetDescr(catalog), &TTSOpsHeapTuple);
		Datum		values[INDEX_MAX_KEYS];
		bool		isnull[INDEX_MAX_KEYS];

		ExecStoreHeapTuple(newtup, slot, false);
		GetPerTupleExprContext(estate)->ecxt_scantuple = slot;
		FormIndexDatum(ii, slot, estate, values, isnull);
		(void) index_insert(index, values, isnull, &newtup->t_self, catalog, UNIQUE_CHECK_NO, false, ii);
		ExecDropSingleTupleTableSlot(slot);
		FreeExecutorState(estate);
		index_close(index, RowExclusiveLock);
	}
}

/*
 * The parts of a storage a snapshot sees.  A scan without a snapshot (ANALYZE)
 * and SnapshotAny take the parts no one can see anymore out (scan.c).
 */
List *
ch_parts_visible(const ChStorageId *id, Snapshot snapshot)
{
	Relation	catalog;
	SysScanDesc scan;
	ScanKeyData keys[3];
	HeapTuple	tuple;
	List	   *parts = NIL;
	int			nkeys;

	catalog = open_catalog("parts", id, AccessShareLock);
	nkeys = storage_keys(id, keys);
	scan = systable_beginscan(catalog, catalog_index(catalog), true, snapshot, nkeys, keys);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Datum		values[Natts_parts];
		bool		nulls[Natts_parts];
		ChPart	   *p = palloc(sizeof(ChPart));

		heap_deform_tuple(tuple, RelationGetDescr(catalog), values, nulls);
		p->name = TextDatumGetCString(values[Anum_parts_name - 1]);
		p->rows = DatumGetInt64(values[Anum_parts_rows - 1]);
		p->bytes = DatumGetInt64(values[Anum_parts_bytes - 1]);
		p->min_rowid = DatumGetInt64(values[Anum_parts_min_rowid - 1]);
		p->max_rowid = DatumGetInt64(values[Anum_parts_max_rowid - 1]);
		p->level = DatumGetInt32(values[Anum_parts_level - 1]);
		p->manifest = nulls[Anum_parts_manifest - 1] ? NULL :
			DatumGetByteaPCopy(values[Anum_parts_manifest - 1]);
		parts = lappend(parts, p);
	}
	systable_endscan(scan);
	table_close(catalog, AccessShareLock);
	return parts;
}

void
ch_part_insert(const ChStorageId *id, const ChPart *part, CommandId cid)
{
	Relation	catalog;
	Datum		values[Natts_parts];
	bool		nulls[Natts_parts];
	HeapTuple	tuple;

	catalog = open_catalog("parts", id, RowExclusiveLock);
	memset(nulls, false, sizeof(nulls));
	values[Anum_parts_tablespace - 1] = ObjectIdGetDatum(id->locator.spcOid);
	values[Anum_parts_relfilenumber - 1] = ObjectIdGetDatum(id->locator.relNumber);
	values[Anum_parts_backend - 1] = Int32GetDatum(id->backend);
	values[Anum_parts_name - 1] = CStringGetTextDatum(part->name);
	values[Anum_parts_rows - 1] = Int64GetDatum(part->rows);
	values[Anum_parts_bytes - 1] = Int64GetDatum(part->bytes);
	values[Anum_parts_min_rowid - 1] = Int64GetDatum(part->min_rowid);
	values[Anum_parts_max_rowid - 1] = Int64GetDatum(part->max_rowid);
	values[Anum_parts_level - 1] = Int32GetDatum(part->level);
	if (part->manifest)
		values[Anum_parts_manifest - 1] = PointerGetDatum(part->manifest);
	else
		nulls[Anum_parts_manifest - 1] = true;
	tuple = heap_form_tuple(RelationGetDescr(catalog), values, nulls);
	insert_with_cid(catalog, tuple, cid);
	heap_freetuple(tuple);
	table_close(catalog, RowExclusiveLock);
}

/* Deletes every part row of a storage the transaction can see (TRUNCATE of a new table) */
void
ch_parts_delete_all(const ChStorageId *id)
{
	Relation	catalog;
	SysScanDesc scan;
	ScanKeyData keys[3];
	HeapTuple	tuple;
	int			nkeys;

	catalog = open_catalog("parts", id, RowExclusiveLock);
	nkeys = storage_keys(id, keys);
	scan = systable_beginscan(catalog, catalog_index(catalog), true, NULL, nkeys, keys);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
		simple_heap_delete(catalog, &tuple->t_self);
	systable_endscan(scan);
	table_close(catalog, RowExclusiveLock);
}

void
ch_table_row_create(Relation rel, const ChStorageId *id, ChEngineKind kind, ChStorage storage)
{
	Relation	catalog;
	Datum		values[Natts_tables];
	bool		nulls[Natts_tables];
	HeapTuple	tuple;

	catalog = open_catalog("tables", id, RowExclusiveLock);
	memset(nulls, false, sizeof(nulls));
	values[Anum_tables_tablespace - 1] = ObjectIdGetDatum(id->locator.spcOid);
	values[Anum_tables_relfilenumber - 1] = ObjectIdGetDatum(id->locator.relNumber);
	values[Anum_tables_backend - 1] = Int32GetDatum(id->backend);
	values[Anum_tables_relid - 1] = ObjectIdGetDatum(RelationGetRelid(rel));
	values[Anum_tables_engine - 1] = CStringGetTextDatum(ch_engine_name(kind));
	values[Anum_tables_storage - 1] = CStringGetTextDatum(storage == CH_STORAGE_PAGES ? "pages" : "files");
	values[Anum_tables_next_rowid - 1] = Int64GetDatum(1);
	values[Anum_tables_next_block - 1] = Int64GetDatum(1);
	values[Anum_tables_library - 1] = CStringGetTextDatum("v26.8.8.8-lts");
	tuple = heap_form_tuple(RelationGetDescr(catalog), values, nulls);
	insert_with_cid(catalog, tuple, GetCurrentCommandId(true));
	heap_freetuple(tuple);
	table_close(catalog, RowExclusiveLock);
}

/* A storage's row for its copy (SET TABLESPACE), the counters carried over */
void
ch_table_row_copy(Relation rel, const ChStorageId *from, const ChStorageId *to)
{
	Relation	catalog;
	SysScanDesc scan;
	ScanKeyData keys[3];
	HeapTuple	tuple;
	int			nkeys;
	Datum		values[Natts_tables];
	bool		nulls[Natts_tables];
	uint64		rowid;
	uint64		block;

	/* the counters as this transaction has them, where it made the storage */
	ch_counters_allocate(from, 0, 0, &rowid, &block);

	catalog = open_catalog("tables", from, RowExclusiveLock);
	nkeys = storage_keys(from, keys);
	scan = systable_beginscan(catalog, catalog_index(catalog), true, NULL, nkeys, keys);
	tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "no ch.tables row for storage %u/%u/%u",
			 from->locator.spcOid, from->locator.dbOid, from->locator.relNumber);
	heap_deform_tuple(tuple, RelationGetDescr(catalog), values, nulls);
	systable_endscan(scan);
	table_close(catalog, RowExclusiveLock);

	catalog = open_catalog("tables", to, RowExclusiveLock);
	values[Anum_tables_tablespace - 1] = ObjectIdGetDatum(to->locator.spcOid);
	values[Anum_tables_relfilenumber - 1] = ObjectIdGetDatum(to->locator.relNumber);
	values[Anum_tables_backend - 1] = Int32GetDatum(to->backend);
	values[Anum_tables_relid - 1] = ObjectIdGetDatum(RelationGetRelid(rel));
	values[Anum_tables_next_rowid - 1] = Int64GetDatum(rowid);
	values[Anum_tables_next_block - 1] = Int64GetDatum(block);
	tuple = heap_form_tuple(RelationGetDescr(catalog), values, nulls);
	insert_with_cid(catalog, tuple, GetCurrentCommandId(true));
	heap_freetuple(tuple);
	table_close(catalog, RowExclusiveLock);
}

void
ch_table_row_delete(const ChStorageId *id)
{
	Relation	catalog;
	SysScanDesc scan;
	ScanKeyData keys[3];
	HeapTuple	tuple;
	int			nkeys;

	catalog = open_catalog("tables", id, RowExclusiveLock);
	nkeys = storage_keys(id, keys);
	scan = systable_beginscan(catalog, catalog_index(catalog), true, NULL, nkeys, keys);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
		simple_heap_delete(catalog, &tuple->t_self);
	systable_endscan(scan);
	table_close(catalog, RowExclusiveLock);
}

/*
 * Counters of storages made in the running transaction, written at its
 * pre-commit (see the head comment).
 */
typedef struct LocalCounters
{
	ChStorageId id;				/* hash key */
	uint64		next_rowid;
	uint64		next_block;
	bool		dirty;
} LocalCounters;

static HTAB *local_counters = NULL;

static LocalCounters *
local_entry(const ChStorageId *id, bool create, bool *found)
{
	if (local_counters == NULL)
	{
		HASHCTL		ctl;

		if (!create)
			return NULL;
		ctl.keysize = sizeof(ChStorageId);
		ctl.entrysize = sizeof(LocalCounters);
		ctl.hcxt = TopTransactionContext;
		local_counters = hash_create("ch counters", 16, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	return hash_search(local_counters, id, create ? HASH_ENTER : HASH_FIND, found);
}

/*
 * Takes nrowids row ids and nblocks block numbers of a storage, returning
 * the first of each.
 */
void
ch_counters_allocate(const ChStorageId *id, uint64 nrowids, uint64 nblocks,
					 uint64 *first_rowid, uint64 *first_block)
{
	Relation	catalog;
	ScanKeyData keys[3];
	HeapTuple	oldtup;
	void	   *state;
	int			nkeys;
	bool		found;
	LocalCounters *local = local_entry(id, false, &found);
	Datum		values[Natts_tables];
	bool		nulls[Natts_tables];

	if (local && found)
	{
		*first_rowid = local->next_rowid;
		*first_block = local->next_block;
		local->next_rowid += nrowids;
		local->next_block += nblocks;
		local->dirty = true;
		if (local->next_rowid > CH_MAX_ROWID)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("the table has used all its row ids")));
		return;
	}

	catalog = open_catalog("tables", id, RowExclusiveLock);
	nkeys = storage_keys(id, keys);

	/*
	 * A row the running transaction inserted cannot be updated in place: its
	 * counters move in the backend until pre-commit.  The row is found with
	 * the latest snapshot that sees the running command's own rows.
	 */
	{
		SysScanDesc scan = systable_beginscan(catalog, catalog_index(catalog), true, SnapshotSelf, nkeys, keys);
		HeapTuple	tuple = systable_getnext(scan);

		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "no ch.tables row for storage %u/%u/%u",
				 id->locator.spcOid, id->locator.dbOid, id->locator.relNumber);
		if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmin(tuple->t_data)))
		{
			heap_deform_tuple(tuple, RelationGetDescr(catalog), values, nulls);
			systable_endscan(scan);
			table_close(catalog, RowExclusiveLock);
			local = local_entry(id, true, &found);
			local->next_rowid = DatumGetInt64(values[Anum_tables_next_rowid - 1]);
			local->next_block = DatumGetInt64(values[Anum_tables_next_block - 1]);
			local->dirty = false;
			ch_counters_allocate(id, nrowids, nblocks, first_rowid, first_block);
			return;
		}
		systable_endscan(scan);
	}

	systable_inplace_update_begin(catalog, catalog_index(catalog), true, NULL, nkeys, keys, &oldtup, &state);
	if (!HeapTupleIsValid(oldtup))
		elog(ERROR, "no ch.tables row for storage %u/%u/%u",
			 id->locator.spcOid, id->locator.dbOid, id->locator.relNumber);
	heap_deform_tuple(oldtup, RelationGetDescr(catalog), values, nulls);
	*first_rowid = DatumGetInt64(values[Anum_tables_next_rowid - 1]);
	*first_block = DatumGetInt64(values[Anum_tables_next_block - 1]);
	if (*first_rowid + nrowids > CH_MAX_ROWID)
	{
		systable_inplace_update_cancel(state);
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("the table has used all its row ids")));
	}
	/* the fixed-width columns change in place, in the tuple's own bytes */
	{
		HeapTuple	newtup = heap_copytuple(oldtup);
		Datum		v[Natts_tables];
		bool		n[Natts_tables];
		bool		repl[Natts_tables];

		memset(repl, false, sizeof(repl));
		memset(n, false, sizeof(n));
		v[Anum_tables_next_rowid - 1] = Int64GetDatum(*first_rowid + nrowids);
		v[Anum_tables_next_block - 1] = Int64GetDatum(*first_block + nblocks);
		repl[Anum_tables_next_rowid - 1] = true;
		repl[Anum_tables_next_block - 1] = true;
		newtup = heap_modify_tuple(oldtup, RelationGetDescr(catalog), v, n, repl);
		systable_inplace_update_finish(state, newtup);
		heap_freetuple(newtup);
	}
	heap_freetuple(oldtup);
	table_close(catalog, RowExclusiveLock);

	/*
	 * A block number names a part's directory before the transaction commits,
	 * so it is durable first: a crash must not hand it out again while a dead
	 * writer's directory has the name.
	 */
	if (nblocks > 0 && !twin(id))
		XLogFlush(GetXLogInsertRecPtr());
}

/* Writes the counters kept in the backend before the transaction commits */
static void
write_local_counters(void)
{
	HASH_SEQ_STATUS status;
	LocalCounters *local;

	if (local_counters == NULL)
		return;
	CommandCounterIncrement();
	hash_seq_init(&status, local_counters);
	while ((local = hash_seq_search(&status)) != NULL)
	{
		Relation	catalog;
		SysScanDesc scan;
		ScanKeyData keys[3];
		HeapTuple	tuple;
		int			nkeys;

		if (!local->dirty)
			continue;
		catalog = open_catalog("tables", &local->id, RowExclusiveLock);
		nkeys = storage_keys(&local->id, keys);
		scan = systable_beginscan(catalog, catalog_index(catalog), true, NULL, nkeys, keys);
		tuple = systable_getnext(scan);
		if (HeapTupleIsValid(tuple))
		{
			Datum		v[Natts_tables];
			bool		n[Natts_tables];
			bool		repl[Natts_tables];
			HeapTuple	newtup;

			memset(repl, false, sizeof(repl));
			memset(n, false, sizeof(n));
			v[Anum_tables_next_rowid - 1] = Int64GetDatum(local->next_rowid);
			v[Anum_tables_next_block - 1] = Int64GetDatum(local->next_block);
			repl[Anum_tables_next_rowid - 1] = true;
			repl[Anum_tables_next_block - 1] = true;
			newtup = heap_modify_tuple(tuple, RelationGetDescr(catalog), v, n, repl);
			update_with_index(catalog, &tuple->t_self, newtup);
			heap_freetuple(newtup);
		}
		systable_endscan(scan);
		table_close(catalog, RowExclusiveLock);
	}
}

void
ch_counters_xact_callback(XactEvent event)
{
	switch (event)
	{
		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_PREPARE:
			write_local_counters();
			break;
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
		case XACT_EVENT_PREPARE:
			/* the hash is in TopTransactionContext, gone with it */
			local_counters = NULL;
			break;
	}
}

/*
 * SERIALIZABLE over the parts (§2.6): every writer of a storage's part rows,
 * visible to the snapshot or not, reported to SSI's conflict-out check, which
 * ignores the ones that are not concurrent.
 */
void
ch_parts_conflict_out(Relation rel, const ChStorageId *id, Snapshot snapshot)
{
	Relation	catalog;
	SysScanDesc scan;
	ScanKeyData keys[3];
	HeapTuple	tuple;
	int			nkeys;

	catalog = open_catalog("parts", id, AccessShareLock);
	nkeys = storage_keys(id, keys);
	scan = systable_beginscan(catalog, catalog_index(catalog), true, SnapshotAny, nkeys, keys);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		TransactionId xmin = HeapTupleHeaderGetXmin(tuple->t_data);
		TransactionId xmax = HeapTupleHeaderGetRawXmax(tuple->t_data);

		if (TransactionIdIsNormal(xmin))
			CheckForSerializableConflictOut(rel, xmin, snapshot);
		if (TransactionIdIsNormal(xmax) && !(tuple->t_data->t_infomask & HEAP_XMAX_IS_MULTI))
			CheckForSerializableConflictOut(rel, xmax, snapshot);
	}
	systable_endscan(scan);
	table_close(catalog, AccessShareLock);
}
