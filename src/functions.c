/*-------------------------------------------------------------------------
 *
 * functions.c
 *	  The extension's SQL functions (ch_table_engines.md §2.4, "Functions").
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/relation.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/tuplestore.h"

#include "chte.h"

static Relation
open_engine_relation(Oid relid, ChEngineKind *kind)
{
	Relation	rel = relation_open(relid, AccessShareLock);

	/* a table of its own files (§2.18) is MergeTree's as its parts and tombstones go */
	if (kind)
		*kind = CH_ENGINE_MERGETREE;
	if (!ch_is_engine_rel(rel, kind) && !ch_is_own_rel(rel))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a table of the ClickHouse engines", RelationGetRelationName(rel))));
	return rel;
}

PG_FUNCTION_INFO_V1(ch_parts);
Datum
ch_parts(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Relation	rel;
	ChStorageId id;
	List	   *parts;
	ListCell   *lc;

	InitMaterializedSRF(fcinfo, 0);
	rel = open_engine_relation(PG_GETARG_OID(0), NULL);
	ch_buffer_flush_for_scan(rel, GetActiveSnapshot());
	ch_storage_id(rel, &rel->rd_locator, &id);
	parts = ch_parts_visible(&id, GetActiveSnapshot());
	foreach(lc, parts)
	{
		ChPart	   *p = lfirst(lc);
		Datum		values[6];
		bool		nulls[6] = {false};

		values[0] = CStringGetTextDatum(p->name);
		values[1] = Int64GetDatum(p->rows);
		values[2] = Int64GetDatum(p->bytes);
		values[3] = Int64GetDatum(p->min_rowid);
		values[4] = Int64GetDatum(p->max_rowid);
		values[5] = Int32GetDatum(p->level);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}
	relation_close(rel, AccessShareLock);
	return (Datum) 0;
}

PG_FUNCTION_INFO_V1(ch_definition);
Datum
ch_definition(PG_FUNCTION_ARGS)
{
	ChEngineKind kind;
	Relation	rel = open_engine_relation(PG_GETARG_OID(0), &kind);
	ChOptions	opts;
	char	   *def;

	ch_relation_options(rel, &opts);
	def = ch_table_definition(rel, &opts, kind);
	relation_close(rel, AccessShareLock);
	PG_RETURN_TEXT_P(cstring_to_text(def));
}

/*
 * The ClickHouse settings an external table, or one of its own files, is
 * read with, as the session's process opens it now (VC26): the extension's
 * defaults, then the table's own; NULL where there are none.
 */
PG_FUNCTION_INFO_V1(ch_read_settings);
Datum
ch_read_settings(PG_FUNCTION_ARGS)
{
	Relation	rel = relation_open(PG_GETARG_OID(0), AccessShareLock);
	ChTable    *t;
	char	   *settings;

	if (!ch_is_external_rel(rel, NULL))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an external table or a table of its own files",
						RelationGetRelationName(rel))));
	t = ch_table(rel);
	settings = t->read_settings ? pstrdup(t->read_settings) : NULL;
	relation_close(rel, AccessShareLock);
	if (settings == NULL)
		PG_RETURN_NULL();
	PG_RETURN_TEXT_P(cstring_to_text(settings));
}

PG_FUNCTION_INFO_V1(ch_table_size);
Datum
ch_table_size(PG_FUNCTION_ARGS)
{
	Relation	rel = open_engine_relation(PG_GETARG_OID(0), NULL);
	ChStorageId id;
	List	   *parts;
	ListCell   *lc;
	int64		bytes = 0;

	ch_storage_id(rel, &rel->rd_locator, &id);
	parts = ch_parts_visible(&id, GetActiveSnapshot());
	foreach(lc, parts)
		bytes += ((ChPart *) lfirst(lc))->bytes;
	relation_close(rel, AccessShareLock);
	PG_RETURN_INT64(bytes);
}

PG_FUNCTION_INFO_V1(ch_engine_info);
Datum
ch_engine_info(PG_FUNCTION_ARGS)
{
	TupleDesc	desc;
	Datum		values[3];
	bool		nulls[3] = {false};
	const ChEngine *eng = ch_engine();

	if (get_call_result_type(fcinfo, NULL, &desc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	values[0] = CStringGetTextDatum("ClickHouse v26.8.8.8-lts");
	values[1] = Int32GetDatum(eng->thread_count());
	/* VC12's: a library built before it has every one */
	if (eng->function_count)
		values[2] = Int32GetDatum(eng->function_count());
	else
		nulls[2] = true;
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(BlessTupleDesc(desc), values, nulls)));
}

/*
 * The Parquet and Arrow footers the session's process has read from their
 * files (VC25), for the tests that check a plan reads none; NULL where the
 * engine library predates the count.
 */
PG_FUNCTION_INFO_V1(ch_footers_read);
Datum
ch_footers_read(PG_FUNCTION_ARGS)
{
	const ChEngine *eng = ch_engine();

	if (eng->footers_read == NULL)
		PG_RETURN_NULL();
	PG_RETURN_INT64((int64) eng->footers_read());
}

/*
 * The rows the session's process has read alone of an Arrow file's record
 * batches (VC27), for the tests; NULL where the engine library predates it.
 */
PG_FUNCTION_INFO_V1(ch_rows_read_alone);
Datum
ch_rows_read_alone(PG_FUNCTION_ARGS)
{
	const ChEngine *eng = ch_engine();

	if (eng->rows_read_alone == NULL)
		PG_RETURN_NULL();
	PG_RETURN_INT64((int64) eng->rows_read_alone());
}

/*
 * Folds the deletions every snapshot sees into their parts (vacuum.c): the
 * parts written again without their rows, the tombstones gone.  Under
 * ShareUpdateExclusiveLock, as VACUUM's: DML goes on beside it, a second
 * fold waits.
 */
PG_FUNCTION_INFO_V1(ch_fold);
Datum
ch_fold(PG_FUNCTION_ARGS)
{
	Relation	rel = relation_open(PG_GETARG_OID(0), ShareUpdateExclusiveLock);
	int64		folded;

	if (!ch_is_mvcc_rel(rel))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a table of the ClickHouse engines", RelationGetRelationName(rel))));
	if (!object_ownercheck(RelationRelationId, RelationGetRelid(rel), GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE, RelationGetRelationName(rel));
	folded = ch_is_own_rel(rel) ? ch_own_fold(rel) : ch_fold_deletions(rel);
	relation_close(rel, NoLock);
	PG_RETURN_INT64(folded);
}

PG_FUNCTION_INFO_V1(ch_remove_orphans);
Datum
ch_remove_orphans(PG_FUNCTION_ARGS)
{
	Relation	rel = relation_open(PG_GETARG_OID(0), ShareLock);
	ChStorageId id;
	int			removed;

	if (!ch_is_mvcc_rel(rel))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a table of the ClickHouse engines", RelationGetRelationName(rel))));
	if (!object_ownercheck(RelationRelationId, RelationGetRelid(rel), GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE, RelationGetRelationName(rel));
	ch_storage_id(rel, &rel->rd_locator, &id);
	removed = ch_store_sweep(rel, &id);
	relation_close(rel, NoLock);
	PG_RETURN_INT32(removed);
}

/*
 * ch.remove_orphans(): the files store's sweep of the database (store.c), as
 * the merge workers' passes end with: the directories of storages no
 * ch.tables row names any more.  The database's owner's.
 */
PG_FUNCTION_INFO_V1(ch_remove_orphans_database);
Datum
ch_remove_orphans_database(PG_FUNCTION_ARGS)
{
	if (!object_ownercheck(DatabaseRelationId, MyDatabaseId, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_DATABASE, get_database_name(MyDatabaseId));
	PG_RETURN_INT32(ch_store_sweep_database(true));
}
