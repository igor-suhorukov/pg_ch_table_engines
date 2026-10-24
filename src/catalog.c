/*-------------------------------------------------------------------------
 *
 * catalog.c
 *	  The extension's catalogs, ch.tables and ch.parts, and their UNLOGGED
 *	  twins for UNLOGGED and temporary tables (ch_table_engines.md §2.6);
 *	  ch.tombstones has a file of its own (tombstones.c).
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

#include "access/detoast.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/heaptoast.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/toast_internals.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "varatt.h"

#include "chte.h"

/* ch.tables's columns */
#define Natts_tables 10
#define Anum_tables_tablespace 1
#define Anum_tables_relfilenumber 2
#define Anum_tables_backend 3
#define Anum_tables_relid 4
#define Anum_tables_engine 5
#define Anum_tables_storage 6
#define Anum_tables_next_rowid 7
#define Anum_tables_next_block 8
#define Anum_tables_library 9
#define Anum_tables_plain 10

/* ch.parts's columns */
#define Natts_parts 13
#define Anum_parts_tablespace 1
#define Anum_parts_relfilenumber 2
#define Anum_parts_backend 3
#define Anum_parts_name 4
#define Anum_parts_rows 5
#define Anum_parts_bytes 6
#define Anum_parts_min_rowid 7
#define Anum_parts_max_rowid 8
#define Anum_parts_level 9
#define Anum_parts_marks 10
#define Anum_parts_manifest 11
#define Anum_parts_created 12
#define Anum_parts_summary 13

static bool
twin(const ChStorageId *id)
{
	return id->persistence != RELPERSISTENCE_PERMANENT;
}

Oid
ch_catalog_relid(const char *name, bool unlogged)
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

Relation
ch_catalog_open(const char *name, const ChStorageId *id, LOCKMODE lockmode)
{
	return table_open(ch_catalog_relid(name, twin(id)), lockmode);
}

#define open_catalog ch_catalog_open
#define catalog_index ch_catalog_index
#define storage_keys ch_catalog_storage_keys
#define insert_with_cid ch_catalog_insert

/* The index of a catalog: its only one */
Oid
ch_catalog_index(Relation catalog)
{
	List	   *indexes = RelationGetIndexList(catalog);

	if (list_length(indexes) != 1)
		elog(ERROR, "catalog %s has %d indexes", RelationGetRelationName(catalog), list_length(indexes));
	return linitial_oid(indexes);
}

int
ch_catalog_storage_keys(const ChStorageId *id, ScanKeyData *keys)
{
	ScanKeyInit(&keys[0], 1, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(id->locator.spcOid));
	ScanKeyInit(&keys[1], 2, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(id->locator.relNumber));
	ScanKeyInit(&keys[2], 3, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(id->backend));
	return 3;
}

/* Inserts a catalog row and its index entry under the given command id */
void
ch_catalog_insert(Relation catalog, HeapTuple tuple, CommandId cid)
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
	(void) index_insert(index, values, isnull, &tuple->t_self, catalog,
						index->rd_index->indisunique ? UNIQUE_CHECK_YES : UNIQUE_CHECK_NO, false, ii);
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

/* Parts in the order of their rows: their first row ids, which rows get as they enter */
static int
part_order(const ListCell *a, const ListCell *b)
{
	const ChPart *pa = lfirst(a);
	const ChPart *pb = lfirst(b);

	if (pa->min_rowid != pb->min_rowid)
		return pa->min_rowid < pb->min_rowid ? -1 : 1;
	return strcmp(pa->name, pb->name);
}

/*
 * The parts of a storage a snapshot sees, in the order their rows entered --
 * the catalog's index orders them by name, where all_10_10_0 comes before
 * all_2_2_0 -- so that a scan reads the rows of a table never merged in the
 * order they were inserted, as heap's does.  A scan without a snapshot
 * (ANALYZE) and SnapshotAny take the parts no one can see anymore out
 * (scan.c).  Never SnapshotAny itself: a dead row's manifest may have lost
 * its TOAST chunks (manifest_kept()).
 *
 * keep_toasted: an MVCC snapshot's manifests are left in TOAST, their
 * pointers copied, until a part is attached (ch_part_manifest()), so that
 * each participant of a scan reads only the manifests of the parts it
 * claims units of (VC12).  The snapshot, registered for the scan, keeps the
 * chunks of every row it sees.
 */
static List *
parts_visible(const ChStorageId *id, Snapshot snapshot, bool keep_toasted)
{
	Relation	catalog;
	SysScanDesc scan;
	ScanKeyData keys[3];
	HeapTuple	tuple;
	List	   *parts = NIL;
	int			nkeys;

	Assert(snapshot == NULL || snapshot->snapshot_type != SNAPSHOT_ANY);
	catalog = open_catalog("parts", id, AccessShareLock);
	nkeys = storage_keys(id, keys);
	scan = systable_beginscan(catalog, catalog_index(catalog), true, snapshot, nkeys, keys);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Datum		values[Natts_parts];
		bool		nulls[Natts_parts];
		ChPart	   *p = palloc0(sizeof(ChPart));

		heap_deform_tuple(tuple, RelationGetDescr(catalog), values, nulls);
		p->xmin = HeapTupleHeaderGetXmin(tuple->t_data);
		p->name = TextDatumGetCString(values[Anum_parts_name - 1]);
		p->rows = DatumGetInt64(values[Anum_parts_rows - 1]);
		p->bytes = DatumGetInt64(values[Anum_parts_bytes - 1]);
		p->min_rowid = DatumGetInt64(values[Anum_parts_min_rowid - 1]);
		p->max_rowid = DatumGetInt64(values[Anum_parts_max_rowid - 1]);
		p->level = DatumGetInt32(values[Anum_parts_level - 1]);
		p->marks = DatumGetInt32(values[Anum_parts_marks - 1]);
		if (nulls[Anum_parts_manifest - 1])
			p->manifest = NULL;
		else if (keep_toasted && VARATT_IS_EXTERNAL_ONDISK(DatumGetPointer(values[Anum_parts_manifest - 1])))
		{
			struct varlena *pointer = (struct varlena *) DatumGetPointer(values[Anum_parts_manifest - 1]);

			p->manifest = palloc(VARSIZE_EXTERNAL(pointer));
			memcpy(p->manifest, pointer, VARSIZE_EXTERNAL(pointer));
		}
		else
			p->manifest = DatumGetByteaPCopy(values[Anum_parts_manifest - 1]);
		p->created = DatumGetTimestampTz(values[Anum_parts_created - 1]);
		/* a catalog made before the column was: no part has a summary */
		if (RelationGetDescr(catalog)->natts < Anum_parts_summary || nulls[Anum_parts_summary - 1])
			p->summary = NULL;
		else
		{
			struct varlena *value = (struct varlena *) DatumGetPointer(values[Anum_parts_summary - 1]);

			/* as stored -- compressed, or a TOAST pointer an MVCC snapshot keeps -- until read */
			if (VARATT_IS_EXTERNAL_ONDISK(value) && !keep_toasted)
				p->summary = (bytea *) detoast_attr(value);
			else
			{
				p->summary = palloc(VARSIZE_ANY(value));
				memcpy(p->summary, value, VARSIZE_ANY(value));
			}
		}
		parts = lappend(parts, p);
	}
	systable_endscan(scan);
	table_close(catalog, AccessShareLock);
	list_sort(parts, part_order);
	return parts;
}

List *
ch_parts_visible(const ChStorageId *id, Snapshot snapshot)
{
	return parts_visible(id, snapshot, false);
}

/* The same, for a scan: its manifests read as its parts are attached */
List *
ch_parts_to_scan(const ChStorageId *id, Snapshot snapshot)
{
	return parts_visible(id, snapshot, snapshot != NULL && IsMVCCSnapshot(snapshot));
}

/* A part's manifest, fetched from TOAST where its list left it there; the part's own else */
bytea *
ch_part_manifest(const ChPart *part)
{
	if (part->manifest == NULL || !VARATT_IS_EXTERNAL_ONDISK(part->manifest))
		return part->manifest;
	return (bytea *) detoast_attr((struct varlena *) part->manifest);
}

/*
 * A part's summary (VC20's B6), decompressed or fetched from TOAST the first
 * time it is read and kept so in the part, in the part's memory context;
 * NULL where its row has none.
 */
bytea *
ch_part_summary(ChPart *part)
{
	if (part->summary != NULL && VARATT_IS_EXTENDED(part->summary))
	{
		MemoryContext old = MemoryContextSwitchTo(GetMemoryChunkContext(part));
		bytea	   *plain = (bytea *) detoast_attr((struct varlena *) part->summary);

		MemoryContextSwitchTo(old);
		if (plain != part->summary)
			pfree(part->summary);
		part->summary = plain;
	}
	return part->summary;
}

/*
 * The rows and bytes of the parts of a storage a snapshot sees, summed from
 * those two columns of their rows alone, no manifest or summary copied or
 * fetched from TOAST (VC20's B8): the planner's size of the relation, at
 * every query.
 */
void
ch_parts_totals(const ChStorageId *id, Snapshot snapshot, double *rows, uint64 *bytes)
{
	Relation	catalog;
	SysScanDesc scan;
	ScanKeyData keys[3];
	HeapTuple	tuple;
	int			nkeys;

	*rows = 0;
	*bytes = 0;
	catalog = open_catalog("parts", id, AccessShareLock);
	nkeys = storage_keys(id, keys);
	scan = systable_beginscan(catalog, catalog_index(catalog), true, snapshot, nkeys, keys);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		bool		isnull;

		*rows += DatumGetInt64(heap_getattr(tuple, Anum_parts_rows, RelationGetDescr(catalog), &isnull));
		*bytes += DatumGetInt64(heap_getattr(tuple, Anum_parts_bytes, RelationGetDescr(catalog), &isnull));
	}
	systable_endscan(scan);
	table_close(catalog, AccessShareLock);
}

/*
 * A manifest read with SnapshotAny, or NULL once its TOAST chunks are gone.
 * A row's deletion deletes its chunks in the same transaction, and the TOAST
 * table is pruned and vacuumed apart from ch.parts -- a read prunes its full
 * pages, where ch.parts's, with room, stay -- so a dead row may outlive them.
 * Its part is then one no snapshot sees, a standby's included: the record
 * that removed the chunks resolved the conflicts of those that might, as
 * ch.parts's own would.  The chunks are read as heap_fetch_toast_slice()
 * reads them, which raises an error at a missing one.
 */
static bytea *
manifest_kept(Datum value)
{
	varlena    *attr = (varlena *) DatumGetPointer(value);
	varatt_external pointer;
	int32		size;
	int32		nchunks;
	int32		next = 0;
	varlena    *result;
	Relation	toastrel;
	Relation   *toastidxs;
	int			nindexes;
	int			valid;
	ScanKeyData key;
	SysScanDesc scan;
	HeapTuple	chunk_tuple;

	if (!VARATT_IS_EXTERNAL_ONDISK(attr))
		return DatumGetByteaPCopy(value);	/* in the row, no chunk to lose */
	VARATT_EXTERNAL_GET_POINTER(pointer, attr);
	size = VARATT_EXTERNAL_GET_EXTSIZE(pointer);
	nchunks = size > 0 ? (size - 1) / TOAST_MAX_CHUNK_SIZE + 1 : 0;
	result = palloc(size + VARHDRSZ);
	if (VARATT_EXTERNAL_IS_COMPRESSED(pointer))
		SET_VARSIZE_COMPRESSED(result, size + VARHDRSZ);
	else
		SET_VARSIZE(result, size + VARHDRSZ);

	toastrel = table_open(pointer.va_toastrelid, AccessShareLock);
	valid = toast_open_indexes(toastrel, AccessShareLock, &toastidxs, &nindexes);
	ScanKeyInit(&key, 1, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(pointer.va_valueid));
	scan = systable_beginscan_ordered(toastrel, toastidxs[valid], get_toast_snapshot(), 1, &key);
	while ((chunk_tuple = systable_getnext_ordered(scan, ForwardScanDirection)) != NULL)
	{
		bool		isnull;
		int32		seq = DatumGetInt32(fastgetattr(chunk_tuple, 2, RelationGetDescr(toastrel), &isnull));
		Pointer		chunk = DatumGetPointer(fastgetattr(chunk_tuple, 3, RelationGetDescr(toastrel), &isnull));
		int32		len = VARATT_IS_SHORT(chunk) ? VARSIZE_SHORT(chunk) - VARHDRSZ_SHORT : VARSIZE(chunk) - VARHDRSZ;
		int32		expected = seq < nchunks - 1 ? TOAST_MAX_CHUNK_SIZE : size - (nchunks - 1) * TOAST_MAX_CHUNK_SIZE;

		if (seq > next)
			break;				/* one before it is gone */
		if (seq < next || seq >= nchunks || len != expected ||
			(VARATT_IS_EXTENDED(chunk) && !VARATT_IS_SHORT(chunk)))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg_internal("unexpected chunk %d of %d, %d bytes, for toast value %u in %s",
									 seq, nchunks, len, pointer.va_valueid,
									 RelationGetRelationName(toastrel))));
		memcpy(VARDATA(result) + seq * TOAST_MAX_CHUNK_SIZE,
			   VARATT_IS_SHORT(chunk) ? VARDATA_SHORT(chunk) : VARDATA(chunk), len);
		next++;
	}
	systable_endscan_ordered(scan);
	toast_close_indexes(toastidxs, nindexes, AccessShareLock);
	table_close(toastrel, AccessShareLock);

	if (next < nchunks)
	{
		pfree(result);
		return NULL;
	}
	if (VARATT_IS_COMPRESSED(result))
	{
		varlena    *plain = detoast_attr(result);

		pfree(result);
		result = plain;
	}
	return (bytea *) result;
}

/*
 * The manifests of every part row of a storage still in ch.parts, whatever
 * its visibility -- a writer's in progress, an aborted one's, one a merge
 * deleted -- until VACUUM of ch.parts prunes it or its manifest's chunks go
 * (SnapshotAny, manifest_kept()): the pages store's pages they name are not
 * free (pages.c).
 */
List *
ch_parts_manifests(const ChStorageId *id)
{
	Relation	catalog;
	SysScanDesc scan;
	ScanKeyData keys[3];
	HeapTuple	tuple;
	List	   *manifests = NIL;
	int			nkeys;

	catalog = open_catalog("parts", id, AccessShareLock);
	nkeys = storage_keys(id, keys);
	scan = systable_beginscan(catalog, catalog_index(catalog), true, SnapshotAny, nkeys, keys);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		bool		isnull;
		Datum		manifest = heap_getattr(tuple, Anum_parts_manifest, RelationGetDescr(catalog), &isnull);
		bytea	   *kept = isnull ? NULL : manifest_kept(manifest);

		if (kept)
			manifests = lappend(manifests, kept);
	}
	systable_endscan(scan);
	table_close(catalog, AccessShareLock);
	return manifests;
}

/*
 * The names of every part row of a storage still in ch.parts, whatever its
 * visibility, for the sweep (store.c): SnapshotAny, the name alone, never the
 * manifest, whose chunks a dead row may outlive (manifest_kept()).
 */
List *
ch_parts_names(const ChStorageId *id)
{
	Relation	catalog;
	SysScanDesc scan;
	ScanKeyData keys[3];
	HeapTuple	tuple;
	List	   *names = NIL;
	int			nkeys;

	catalog = open_catalog("parts", id, AccessShareLock);
	nkeys = storage_keys(id, keys);
	scan = systable_beginscan(catalog, catalog_index(catalog), true, SnapshotAny, nkeys, keys);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		bool		isnull;
		Datum		name = heap_getattr(tuple, Anum_parts_name, RelationGetDescr(catalog), &isnull);

		names = lappend(names, TextDatumGetCString(name));
	}
	systable_endscan(scan);
	table_close(catalog, AccessShareLock);
	return names;
}

/*
 * The horizon of a storage's part rows: an inserter older than it committed
 * or aborted before every snapshot still in use began -- on a node of the
 * port's cluster, every distributed snapshot's too, gp_core holding the
 * horizon back while one may still see a transaction in progress that has
 * committed here (gp_dtx.c).
 */
TransactionId
ch_parts_horizon(const ChStorageId *id)
{
	Relation	catalog = open_catalog("parts", id, AccessShareLock);
	TransactionId horizon = GetOldestNonRemovableTransactionId(catalog);

	table_close(catalog, AccessShareLock);
	return horizon;
}

/*
 * Whether a row of ch.tables or ch.tables_unlogged names a storage, whatever
 * its visibility, as a part's directory stays while a ch.parts row names it
 * (store.c): the files store's sweep of a database leaves its directory while
 * one does, and removes it once VACUUM of the catalog has pruned the last.
 */
bool
ch_storage_named(const ChStorageId *id)
{
	bool		named = false;

	for (int unlogged = 0; unlogged < 2 && !named; unlogged++)
	{
		Relation	catalog = table_open(ch_catalog_relid("tables", unlogged), AccessShareLock);
		SysScanDesc scan;
		ScanKeyData keys[3];
		int			nkeys = storage_keys(id, keys);

		scan = systable_beginscan(catalog, catalog_index(catalog), true, SnapshotAny, nkeys, keys);
		named = HeapTupleIsValid(systable_getnext(scan));
		systable_endscan(scan);
		table_close(catalog, AccessShareLock);
	}
	return named;
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
	values[Anum_parts_marks - 1] = Int32GetDatum(part->marks);
	if (part->manifest)
		values[Anum_parts_manifest - 1] = PointerGetDatum(part->manifest);
	else
		nulls[Anum_parts_manifest - 1] = true;
	values[Anum_parts_created - 1] = TimestampTzGetDatum(part->created ? part->created : GetCurrentTimestamp());
	if (part->summary)
		values[Anum_parts_summary - 1] = PointerGetDatum(part->summary);
	else
		nulls[Anum_parts_summary - 1] = true;
	tuple = heap_form_tuple(RelationGetDescr(catalog), values, nulls);
	insert_with_cid(catalog, tuple, cid);
	heap_freetuple(tuple);
	table_close(catalog, RowExclusiveLock);
}

/*
 * A writer's part i's summary for its row (VC20's B6), the library's bytes
 * of its columns' sizes and statistics; NULL where the library keeps none.
 */
bytea *
ch_part_summary_written(chte_writer *writer, size_t i)
{
	const ChEngine *eng = ch_engine();
	const void *data;
	size_t		size;
	bytea	   *summary;

	if (eng->write_part_summary == NULL)
		return NULL;
	CH_CALL("summarizing a part", eng->write_part_summary(writer, i, &data, &size, CH_ERR));
	if (size == 0)
		return NULL;
	summary = palloc(VARHDRSZ + size);
	SET_VARSIZE(summary, VARHDRSZ + size);
	memcpy(VARDATA(summary), data, size);
	return summary;
}

/* Deletes a part's row (its next version replacing it: tombstones folded into it) */
void
ch_part_delete(const ChStorageId *id, const char *name)
{
	Relation	catalog;
	SysScanDesc scan;
	ScanKeyData keys[4];
	HeapTuple	tuple;
	int			nkeys;
	bool		found = false;

	catalog = open_catalog("parts", id, RowExclusiveLock);
	nkeys = storage_keys(id, keys);
	ScanKeyInit(&keys[nkeys++], Anum_parts_name, BTEqualStrategyNumber, F_TEXTEQ, CStringGetTextDatum(name));
	scan = systable_beginscan(catalog, catalog_index(catalog), true, NULL, nkeys, keys);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		simple_heap_delete(catalog, &tuple->t_self);
		found = true;
	}
	systable_endscan(scan);
	table_close(catalog, RowExclusiveLock);
	if (!found)
		elog(ERROR, "no ch.parts row for part %s", name);
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

/* A set of attribute numbers as ch.tables keeps it: int2[], in order */
static Datum
attnums_datum(const Bitmapset *attnums)
{
	int			n = bms_num_members(attnums);
	Datum	   *elems = palloc(sizeof(Datum) * Max(n, 1));
	int			i = -1;
	int			k = 0;

	while ((i = bms_next_member(attnums, i)) >= 0)
		elems[k++] = Int16GetDatum((int16) i);
	return PointerGetDatum(construct_array_builtin(elems, n, INT2OID));
}

static Bitmapset *
datum_attnums(Datum datum)
{
	ArrayType  *array = DatumGetArrayTypeP(datum);
	Datum	   *elems;
	int			n;
	Bitmapset  *attnums = NULL;

	deconstruct_array_builtin(array, INT2OID, &elems, NULL, &n);
	for (int k = 0; k < n; k++)
		attnums = bms_add_member(attnums, DatumGetInt16(elems[k]));
	return attnums;
}

void
ch_table_row_create(Relation rel, const ChStorageId *id, ChEngineKind kind, ChStorage storage,
					const Bitmapset *plain)
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
	{
		ChExternalKind ext_kind;

		/* a table of its own files (§2.18): its access method's name, its parts' format */
		values[Anum_tables_engine - 1] =
			CStringGetTextDatum(ch_is_external_rel(rel, &ext_kind) ? ch_external_name(ext_kind) : ch_engine_name(kind));
	}
	values[Anum_tables_storage - 1] = CStringGetTextDatum(storage == CH_STORAGE_PAGES ? "pages" : "files");
	values[Anum_tables_next_rowid - 1] = Int64GetDatum(1);
	values[Anum_tables_next_block - 1] = Int64GetDatum(1);
	values[Anum_tables_library - 1] = CStringGetTextDatum("v26.8.8.8-lts");
	values[Anum_tables_plain - 1] = attnums_datum(plain);
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

/*
 * The columns a storage keeps as ClickHouse's plain types (VC11), found with
 * the latest snapshot that sees the running command's own rows, as the
 * counters are: a CREATE opens its table in the command that made the row.
 */
Bitmapset *
ch_table_row_plain(const ChStorageId *id)
{
	Relation	catalog;
	SysScanDesc scan;
	ScanKeyData keys[3];
	HeapTuple	tuple;
	int			nkeys;
	Bitmapset  *plain = NULL;

	catalog = open_catalog("tables", id, AccessShareLock);
	/* a catalog made before the column was: every column Nullable, as then */
	if (RelationGetDescr(catalog)->natts < Anum_tables_plain)
	{
		table_close(catalog, AccessShareLock);
		return NULL;
	}
	nkeys = storage_keys(id, keys);
	scan = systable_beginscan(catalog, catalog_index(catalog), true, SnapshotSelf, nkeys, keys);
	tuple = systable_getnext(scan);
	if (HeapTupleIsValid(tuple))
	{
		bool		isnull;
		Datum		datum = heap_getattr(tuple, Anum_tables_plain, RelationGetDescr(catalog), &isnull);

		if (!isnull)
			plain = datum_attnums(datum);
	}
	systable_endscan(scan);
	table_close(catalog, AccessShareLock);
	return plain;
}

/*
 * A storage's plain columns replaced (VC11): an update of its row, under
 * the lock of the command that changes the table, so that no writer of the
 * table moves its counters meanwhile.
 */
void
ch_table_row_set_plain(const ChStorageId *id, const Bitmapset *plain)
{
	Relation	catalog;
	SysScanDesc scan;
	ScanKeyData keys[3];
	HeapTuple	tuple;
	int			nkeys;

	catalog = open_catalog("tables", id, RowExclusiveLock);
	if (RelationGetDescr(catalog)->natts < Anum_tables_plain)
	{
		table_close(catalog, RowExclusiveLock);
		return;
	}
	nkeys = storage_keys(id, keys);
	scan = systable_beginscan(catalog, catalog_index(catalog), true, SnapshotSelf, nkeys, keys);
	tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "no ch.tables row for storage %u/%u/%u",
			 id->locator.spcOid, id->locator.dbOid, id->locator.relNumber);
	{
		Datum		v[Natts_tables];
		bool		n[Natts_tables];
		bool		repl[Natts_tables];
		HeapTuple	newtup;

		memset(repl, false, sizeof(repl));
		memset(n, false, sizeof(n));
		v[Anum_tables_plain - 1] = attnums_datum(plain);
		repl[Anum_tables_plain - 1] = true;
		newtup = heap_modify_tuple(tuple, RelationGetDescr(catalog), v, n, repl);
		update_with_index(catalog, &tuple->t_self, newtup);
		heap_freetuple(newtup);
	}
	systable_endscan(scan);
	table_close(catalog, RowExclusiveLock);
	CommandCounterIncrement();
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
	 * writer's directory has the name.  The in-place update's record is the
	 * last this backend inserted: its end is flushed, not the insert
	 * position, which past a page's end is the next record's start, beyond
	 * what can be flushed ("xlog flush request ... is not satisfied").
	 */
	if (nblocks > 0 && !twin(id))
		XLogFlush(XactLastRecEnd);
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
