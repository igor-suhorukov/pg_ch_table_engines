/*-------------------------------------------------------------------------
 *
 * table.c
 *	  A relation's table in the engine library: the ClickHouse CREATE TABLE
 *	  its columns and options make, and its handle, cached per backend until
 *	  the relation changes (ch_table_engines.md §2.2, §2.5).
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "catalog/pg_type.h"
#include "catalog/pg_tablespace.h"
#include "common/relpath.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/procnumber.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/formatting.h"
#include "utils/varlena.h"

#include "chte.h"

typedef struct ChTableEntry
{
	Oid			relid;			/* hash key */
	ChTable    *table;
	MemoryContext cxt;
} ChTableEntry;

static HTAB *tables = NULL;

/*
 * The relation a table is being built for (open_table()), whose catalog reads
 * may take invalidations: the build is outside the cache's entry, which an
 * invalidation may take away meanwhile, and its memory -- the current context
 * -- is left alone.  One of the relation itself marks the build to be done
 * again.  A reset of every cache does not: it names no change of the
 * relation, whose definition the caller's lock holds, and with
 * debug_discard_caches every catalog read is one, so that the build would
 * never end (relcache.c's RelationBuildDesc() leaves those out likewise).
 */
static Oid	building_relid = InvalidOid;
static bool building_invalidated = false;

/*
 * Tables an invalidation took out of the cache while a scan or a write buffer
 * still had them pinned: freed as the last pin goes, or at the transaction's
 * end, which no scan or buffer outlives.
 */
static List *stale = NIL;		/* of ChTable, in TopMemoryContext */

static void
table_free(ChTable *t)
{
	if (t->handle)
		ch_engine()->table_close(t->handle);
	MemoryContextDelete(t->own_cxt);
}

static void
entry_release(ChTableEntry *entry)
{
	ChTable    *t = entry->table;

	if (t)
	{
		if (t->refcount > 0)
		{
			MemoryContext old = MemoryContextSwitchTo(TopMemoryContext);

			t->stale = true;
			stale = lappend(stale, t);
			MemoryContextSwitchTo(old);
		}
		else
			table_free(t);
	}
	else if (entry->cxt)
		MemoryContextDelete(entry->cxt);
	entry->table = NULL;
	entry->cxt = NULL;
}

void
ch_table_pin(ChTable *t)
{
	t->refcount++;
}

void
ch_table_unpin(ChTable *t)
{
	Assert(t->refcount > 0);
	if (--t->refcount == 0 && t->stale)
	{
		stale = list_delete_ptr(stale, t);
		table_free(t);
	}
}

/* At a transaction's end no scan or buffer is left: every pin goes */
void
ch_tables_xact_end(void)
{
	HASH_SEQ_STATUS status;
	ChTableEntry *entry;
	ListCell   *lc;

	foreach(lc, stale)
		table_free(lfirst(lc));
	list_free(stale);
	stale = NIL;
	if (tables == NULL)
		return;
	hash_seq_init(&status, tables);
	while ((entry = hash_seq_search(&status)) != NULL)
		if (entry->table)
			entry->table->refcount = 0;
}

void
ch_table_forget(Oid relid)
{
	ChTableEntry *entry;

	if (tables == NULL)
		return;
	entry = hash_search(tables, &relid, HASH_FIND, NULL);
	if (entry)
	{
		entry_release(entry);
		hash_search(tables, &relid, HASH_REMOVE, NULL);
	}
}

static void
relcache_callback(Datum arg, Oid relid)
{
	HASH_SEQ_STATUS status;
	ChTableEntry *entry;

	if (OidIsValid(building_relid) && relid == building_relid)
		building_invalidated = true;
	if (tables == NULL)
		return;
	if (OidIsValid(relid))
	{
		ch_table_forget(relid);
		return;
	}
	hash_seq_init(&status, tables);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		entry_release(entry);
		hash_search(tables, &entry->relid, HASH_REMOVE, NULL);
	}
}

static void
init_cache(void)
{
	HASHCTL		ctl;

	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(ChTableEntry);
	ctl.hcxt = CacheMemoryContext;
	tables = hash_create("ch tables", 64, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	CacheRegisterRelcacheCallback(relcache_callback, (Datum) 0);
}

void
ch_storage_id(Relation rel, const RelFileLocator *locator, ChStorageId *id)
{
	/* a hash key: its padding zeroed */
	memset(id, 0, sizeof(*id));
	id->locator = *locator;
	id->persistence = rel->rd_rel->relpersistence;
	id->backend = id->persistence == RELPERSISTENCE_TEMP ? rel->rd_backend : -1;
}

/*
 * The directory of a storage's parts, relative to DataDir or absolute: in
 * the database's directory of its tablespace, ch_<relfilenumber>, or for a
 * temporary relation ch_t<procnumber>_<relfilenumber>, as its files are named.
 */
char *
ch_storage_path(const ChStorageId *id, bool relative)
{
	char	   *dbpath = GetDatabasePath(id->locator.dbOid, id->locator.spcOid);
	char	   *path;

	if (id->backend >= 0)
		path = psprintf("%s%s/ch_t%d_%u/", relative ? "" : psprintf("%s/", DataDir), dbpath,
						id->backend, id->locator.relNumber);
	else
		path = psprintf("%s%s/ch_%u/", relative ? "" : psprintf("%s/", DataDir), dbpath,
						id->locator.relNumber);
	pfree(dbpath);
	return path;
}

static char *
column_name(int attnum)
{
	return psprintf("c%d", attnum);
}

/* A column list option's PostgreSQL names as ClickHouse's */
static char *
translate_columns(Relation rel, const char *option, const char *list, bool tuple)
{
	char	   *copy = pstrdup(list);
	List	   *names;
	ListCell   *lc;
	StringInfoData buf;
	bool		first = true;

	if (!SplitIdentifierString(copy, ',', &names))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid value for option \"%s\": \"%s\"", option, list)));
	initStringInfo(&buf);
	if (tuple)
		appendStringInfoChar(&buf, '(');
	foreach(lc, names)
	{
		char	   *name = lfirst(lc);
		AttrNumber	attnum = get_attnum(RelationGetRelid(rel), name);

		if (attnum == InvalidAttrNumber)
		{
			/* a relation the running command makes is not in the syscache yet */
			TupleDesc	desc = RelationGetDescr(rel);

			for (int i = 0; i < desc->natts; i++)
			{
				Form_pg_attribute att = TupleDescAttr(desc, i);

				if (!att->attisdropped && strcmp(NameStr(att->attname), name) == 0)
				{
					attnum = att->attnum;
					break;
				}
			}
		}
		if (attnum <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" named in option \"%s\" does not exist", name, option)));
		if (!first)
			appendStringInfoString(&buf, ", ");
		appendStringInfoString(&buf, column_name(attnum));
		first = false;
	}
	if (tuple)
		appendStringInfoChar(&buf, ')');
	return buf.data;
}

static bool
names_column(const char *option, const char *name)
{
	char	   *copy;
	List	   *names;

	if (option == NULL)
		return false;
	copy = pstrdup(option);
	return SplitIdentifierString(copy, ',', &names) && list_length(names) == 1 &&
		strcmp(linitial(names), name) == 0;
}

bool
ch_is_sign_column(const ChOptions *opts, const char *name)
{
	return names_column(opts->sign, name);
}

/* Whether a column list option names a column, at any place */
static bool
lists_column(const char *option, const char *name)
{
	char	   *copy;
	List	   *names;
	ListCell   *lc;

	if (option == NULL)
		return false;
	copy = pstrdup(option);
	if (!SplitIdentifierString(copy, ',', &names))
		return false;
	foreach(lc, names)
		if (strcmp(lfirst(lc), name) == 0)
			return true;
	return false;
}

/*
 * A column as a table keeps it: a collapsing engine's sign is ClickHouse's
 * Int8 (its engines check it), from any of PostgreSQL's integers; a column
 * option low_cardinality names, ClickHouse's LowCardinality of its String.
 */
void
ch_column_for_table(Relation rel, const ChOptions *opts, Form_pg_attribute att, ChColumn *col)
{
	ch_column_for_attribute(att, col);
	if (col->kind != CH_COL_DROPPED && lists_column(opts->low_cardinality, NameStr(att->attname)))
	{
		if (col->kind != CH_COL_VARLENA)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("column \"%s\" named in option \"low_cardinality\" is of type %s, which is not kept as a string",
							NameStr(att->attname), format_type_be(att->atttypid)),
					 errdetail("Only a column of a variable-length type is kept as ClickHouse's String, which LowCardinality takes.")));
		col->low_cardinality = true;
	}
	if (col->kind == CH_COL_DROPPED || !ch_is_sign_column(opts, NameStr(att->attname)))
		return;
	if (att->atttypid == INT2OID || att->atttypid == INT4OID || att->atttypid == INT8OID)
	{
		col->kind = CH_COL_SIGN;
		col->width = 1;
		strcpy(col->chtype, "Int8");
	}
	else if (att->atttypid != CHAROID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("the sign column \"%s\" must be of an integer type", NameStr(att->attname))));
}

/* Whether a column is one an engine's option names: ver, is_deleted or sign */
bool
ch_is_engine_column(Relation rel, const ChOptions *opts, const char *name)
{
	const char *const options[] = {opts->ver, opts->is_deleted, opts->sign};

	for (int i = 0; i < lengthof(options); i++)
	{
		char	   *copy;
		List	   *names;

		if (options[i] == NULL)
			continue;
		copy = pstrdup(options[i]);
		if (SplitIdentifierString(copy, ',', &names) && list_length(names) == 1 &&
			strcmp(linitial(names), name) == 0)
			return true;
	}
	return false;
}

static const char *
codec(const char *compression)
{
	int			level;

	if (pg_strcasecmp(compression, "none") == 0)
		return "CODEC(NONE)";
	if (pg_strcasecmp(compression, "zstd") == 0)
		return "CODEC(ZSTD(1))";
	if (pg_strcasecmp(compression, "lz4hc") == 0)
		return "CODEC(LZ4HC(9))";
	if (sscanf(compression, "zstd(%d)", &level) == 1)
		return psprintf("CODEC(ZSTD(%d))", level);
	if (sscanf(compression, "lz4hc(%d)", &level) == 1)
		return psprintf("CODEC(LZ4HC(%d))", level);
	return "CODEC(LZ4)";
}

/* Whether settings, "name = value, ...", name a setting, as a word of its own */
static bool
mentions_setting(const char *settings, const char *name)
{
	char	   *lower = asc_tolower(settings, strlen(settings));
	char	   *at = strstr(lower, name);

	while (at)
	{
		char		before = at == lower ? ' ' : at[-1];
		char		after = at[strlen(name)];

		if (!isalnum((unsigned char) before) && before != '_' &&
			!isalnum((unsigned char) after) && after != '_')
			return true;
		at = strstr(at + 1, name);
	}
	return false;
}

/* MergeTree settings a table may not set: where its data lives is the extension's */
static void
check_user_settings(const char *settings)
{
	static const char *const refused[] = {"storage_policy", "disk", "table_disk"};

	for (int i = 0; i < lengthof(refused); i++)
		if (mentions_setting(settings, refused[i]))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("setting \"%s\" cannot be given to a table of the ClickHouse engines", refused[i])));
}

/*
 * Option low_cardinality's columns, as the option is given: each one of the
 * relation's, kept as a string.  Not as a table opens, where a column
 * dropped since is passed over.
 */
void
ch_low_cardinality_check(Relation rel, const char *list)
{
	TupleDesc	desc = RelationGetDescr(rel);

	(void) translate_columns(rel, "low_cardinality", list, false);
	for (int i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);
		ChColumn	col;

		if (!att->attisdropped && lists_column(list, NameStr(att->attname)))
		{
			ch_column_for_attribute(att, &col);
			if (col.kind != CH_COL_VARLENA)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("column \"%s\" named in option \"low_cardinality\" is of type %s, which is not kept as a string",
								NameStr(att->attname), format_type_be(att->atttypid)),
						 errdetail("Only a column of a variable-length type is kept as ClickHouse's String, which LowCardinality takes.")));
		}
	}
}

/* The ClickHouse CREATE TABLE of a relation, as the engine library compiles it */
char *
ch_table_definition(Relation rel, const ChOptions *opts, ChEngineKind kind)
{
	if (opts->low_cardinality)
		ch_low_cardinality_check(rel, opts->low_cardinality);
	return ch_table_definition_for(rel, RelationGetDescr(rel), opts, kind);
}

char *
ch_table_definition_for(Relation rel, TupleDesc desc, const ChOptions *opts, ChEngineKind kind)
{
	StringInfoData buf;
	const char *c = codec(opts->compression);

	initStringInfo(&buf);
	appendStringInfoString(&buf, "CREATE TABLE t (");
	for (int i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);
		ChColumn	col;

		ch_column_for_table(rel, opts, att, &col);
		if (col.kind == CH_COL_DROPPED)
			continue;
		if (ch_is_engine_column(rel, opts, NameStr(att->attname)))
		{
			/* ClickHouse's engines take no Nullable version, sign or is_deleted column */
			if (!att->attnotnull)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("column \"%s\" named by an option of %s must be NOT NULL",
								NameStr(att->attname), ch_engine_name(kind))));
			appendStringInfo(&buf, "%s %s %s, ", column_name(att->attnum), col.chtype, c);
		}
		else if (col.low_cardinality)
			appendStringInfo(&buf, "%s LowCardinality(Nullable(%s)) %s, ", column_name(att->attnum), col.chtype, c);
		else
			appendStringInfo(&buf, "%s Nullable(%s) %s, ", column_name(att->attnum), col.chtype, c);
	}
	/* the row id, and the projection that finds a row by it (Appendix E) */
	appendStringInfoString(&buf, CH_ROWID_COLUMN " UInt64 CODEC(T64, LZ4), ");
	appendStringInfoString(&buf, "PROJECTION " CH_ROWID_PROJECTION " (SELECT " CH_ROWID_COLUMN ", _part_offset ORDER BY " CH_ROWID_COLUMN ")");
	appendStringInfoString(&buf, ") ENGINE = ");
	appendStringInfoString(&buf, ch_engine_name(kind));
	switch (kind)
	{
		case CH_ENGINE_REPLACING:
			if (opts->ver)
			{
				appendStringInfo(&buf, "(%s", translate_columns(rel, "ver", opts->ver, false));
				if (opts->is_deleted)
					appendStringInfo(&buf, ", %s", translate_columns(rel, "is_deleted", opts->is_deleted, false));
				appendStringInfoChar(&buf, ')');
			}
			else if (opts->is_deleted)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("option \"is_deleted\" needs option \"ver\"")));
			break;
		case CH_ENGINE_SUMMING:
		case CH_ENGINE_COALESCING:
			if (opts->sum_columns)
				appendStringInfo(&buf, "(%s)", translate_columns(rel, "sum_columns", opts->sum_columns, true));
			break;
		case CH_ENGINE_COLLAPSING:
			if (!opts->sign)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("%s needs option \"sign\"", ch_engine_name(kind))));
			appendStringInfo(&buf, "(%s)", translate_columns(rel, "sign", opts->sign, false));
			break;
		case CH_ENGINE_VERSIONED_COLLAPSING:
			if (!opts->sign || !opts->ver)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("%s needs options \"sign\" and \"ver\"", ch_engine_name(kind))));
			appendStringInfo(&buf, "(%s, %s)", translate_columns(rel, "sign", opts->sign, false),
							 translate_columns(rel, "ver", opts->ver, false));
			break;
		default:
			break;
	}
	appendStringInfo(&buf, " ORDER BY %s",
					 opts->order_by ? translate_columns(rel, "order_by", opts->order_by, true) : "tuple()");
	if (opts->primary_key)
		appendStringInfo(&buf, " PRIMARY KEY %s", translate_columns(rel, "primary_key", opts->primary_key, true));
	appendStringInfo(&buf, " SETTINGS index_granularity = %d, allow_nullable_key = 1", opts->index_granularity);
	/* the parts are synced before their catalog rows commit, as PostgreSQL syncs */
	appendStringInfo(&buf, ", fsync_after_insert = %d, fsync_part_directory = %d",
					 enableFsync ? 1 : 0, enableFsync ? 1 : 0);
	/*
	 * ClickHouse's statistics of every column, "basic" alone: exact least and
	 * greatest values and NULLs of each part, which part pruning and the
	 * source's aggregates read (§2.8); its default adds uniq_v2's sketches,
	 * which nothing of the extension's reads.
	 */
	if (!opts->settings || !mentions_setting(opts->settings, "auto_statistics_types"))
		appendStringInfoString(&buf, ", auto_statistics_types = 'basic'");
	if (opts->settings && opts->settings[0])
	{
		check_user_settings(opts->settings);
		appendStringInfo(&buf, ", %s", opts->settings);
	}
	return buf.data;
}

static ChTable *
open_table(Relation rel, const ChStorageId *id, TupleDesc desc, MemoryContext cxt)
{
	ChTable    *t;
	MemoryContext old = MemoryContextSwitchTo(cxt);
	chte_table_def def;
	StringInfoData cols;

	t = palloc0(sizeof(ChTable));
	t->relid = RelationGetRelid(rel);
	t->storage = *id;
	(void) ch_is_engine_rel(rel, &t->kind);
	ch_relation_options(rel, &t->options);
	t->relative_path = ch_storage_path(id, true);
	t->definition = ch_table_definition_for(rel, desc, &t->options, t->kind);
	t->natts = desc->natts;
	t->columns = palloc0(sizeof(ChColumn) * Max(desc->natts, 1));
	initStringInfo(&cols);
	for (int i = 0; i < desc->natts; i++)
	{
		ch_column_for_table(rel, &t->options, TupleDescAttr(desc, i), &t->columns[i]);
		if (t->columns[i].kind != CH_COL_DROPPED)
			appendStringInfo(&cols, "%s, ", column_name(TupleDescAttr(desc, i)->attnum));
	}
	appendStringInfoString(&cols, CH_ROWID_COLUMN);
	t->all_columns = cols.data;
	MemoryContextSwitchTo(old);

	def.name = RelationGetRelationName(rel);
	def.relative_path = t->relative_path;
	def.create_query = t->definition;
	def.pages = NULL;
	if (t->options.storage == CH_STORAGE_PAGES)
	{
		ch_pages_ops(t, &t->pages_ops);
		def.pages = &t->pages_ops;
	}
	CH_CALL("opening the table in the engine library", ch_engine()->table_open(&def, &t->handle, CH_ERR));
	return t;
}

/* The engine library's table for a relation's current storage */
ChTable *
ch_table(Relation rel)
{
	ChStorageId id;

	ch_storage_id(rel, &rel->rd_locator, &id);
	return ch_table_for_storage(rel, &id);
}

ChTable *
ch_table_for_storage(Relation rel, const ChStorageId *id)
{
	ChTableEntry *entry;
	bool		found;
	Oid			relid = RelationGetRelid(rel);

	if (tables == NULL)
		init_cache();
	entry = hash_search(tables, &relid, HASH_ENTER, &found);
	if (found && entry->table &&
		(!RelFileLocatorEquals(entry->table->storage.locator, id->locator) ||
		 entry->table->storage.backend != id->backend ||
		 entry->table->natts != RelationGetDescr(rel)->natts))
		entry_release(entry);
	if (!found || entry->table == NULL)
	{
		ChTable    *t = NULL;
		MemoryContext cxt;

		entry->table = NULL;
		entry->cxt = NULL;
		/* built outside the entry, which an invalidation may take away meanwhile; again if one came */
		Assert(!OidIsValid(building_relid));
		do
		{
			cxt = AllocSetContextCreate(CacheMemoryContext, "ch table", ALLOCSET_SMALL_SIZES);
			building_relid = relid;
			building_invalidated = false;
			PG_TRY();
			{
				t = open_table(rel, id, RelationGetDescr(rel), cxt);
				t->own_cxt = cxt;
			}
			PG_CATCH();
			{
				building_relid = InvalidOid;
				MemoryContextDelete(cxt);
				hash_search(tables, &relid, HASH_REMOVE, NULL);
				PG_RE_THROW();
			}
			PG_END_TRY();
			building_relid = InvalidOid;
			if (building_invalidated)
				table_free(t);
		} while (building_invalidated);
		entry = hash_search(tables, &relid, HASH_ENTER, &found);
		if (found && entry->table)
			entry_release(entry);
		entry->table = t;
		entry->cxt = cxt;
	}
	return entry->table;
}

/*
 * A table of the library for a tuple descriptor other than the relation's:
 * a rewrite reads the old storage into slots of the descriptor its parts were
 * written with (ATRewriteTable()), while the relation has its new one.  Not
 * cached; closed with ch_table_private_close().
 */
ChTable *
ch_table_private(Relation rel, const ChStorageId *id, TupleDesc desc)
{
	MemoryContext cxt = AllocSetContextCreate(CurrentMemoryContext, "ch private table", ALLOCSET_SMALL_SIZES);
	ChTable    *t = NULL;

	PG_TRY();
	{
		t = open_table(rel, id, desc, cxt);
	}
	PG_CATCH();
	{
		MemoryContextDelete(cxt);
		PG_RE_THROW();
	}
	PG_END_TRY();
	t->private_cxt = cxt;
	return t;
}

void
ch_table_private_close(ChTable *t)
{
	if (t->handle)
		ch_engine()->table_close(t->handle);
	MemoryContextDelete(t->private_cxt);
}

/* Whether slots of desc can be filled from the relation's own table */
bool
ch_desc_matches(Relation rel, TupleDesc desc)
{
	TupleDesc	rdesc = RelationGetDescr(rel);

	if (desc == rdesc)
		return true;
	if (desc->natts != rdesc->natts)
		return false;
	for (int i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute a = TupleDescAttr(desc, i);
		Form_pg_attribute b = TupleDescAttr(rdesc, i);

		if (a->attisdropped != b->attisdropped || a->atttypid != b->atttypid || a->atttypmod != b->atttypmod)
			return false;
	}
	return true;
}
