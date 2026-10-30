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

#include "access/relation.h"
#include "access/table.h"
#include "access/xlog.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_type.h"
#include "catalog/pg_tablespace.h"
#include "common/relpath.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/lmgr.h"
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
	if (t->listing)
		ch_listing_unpin(t->listing);
	t->listing = NULL;
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
	if (tables != NULL)
	{
		hash_seq_init(&status, tables);
		while ((entry = hash_seq_search(&status)) != NULL)
			if (entry->table)
			{
				entry->table->refcount = 0;
				/* an external table's listing is its last statement's: gone below */
				entry->table->listing = NULL;
			}
	}
	ch_external_xact_end();
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

/* A column list option's PostgreSQL names as ClickHouse's: *n of them, palloc'd */
static const char **
translated_columns(Relation rel, const char *option, const char *list, size_t *n)
{
	char	   *copy = pstrdup(list);
	List	   *names;
	ListCell   *lc;
	const char **out;

	if (!SplitIdentifierString(copy, ',', &names))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid value for option \"%s\": \"%s\"", option, list)));
	out = palloc(sizeof(char *) * Max(list_length(names), 1));
	*n = 0;
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
		out[(*n)++] = column_name(attnum);
	}
	return out;
}

/* Names joined by ", ", and in parentheses where tuple */
static char *
joined(const char *const *names, size_t n, bool tuple)
{
	StringInfoData buf;

	initStringInfo(&buf);
	if (tuple)
		appendStringInfoChar(&buf, '(');
	for (size_t i = 0; i < n; i++)
		appendStringInfo(&buf, "%s%s", i > 0 ? ", " : "", names[i]);
	if (tuple)
		appendStringInfoChar(&buf, ')');
	return buf.data;
}

/* A column list option's PostgreSQL names as ClickHouse's, "c1, c3" or "(c1, c3)" */
static char *
translate_columns(Relation rel, const char *option, const char *list, bool tuple)
{
	size_t		n;
	const char **names = translated_columns(rel, option, list, &n);

	return joined(names, n, tuple);
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

/* The compression option as CODEC()'s arguments */
static const char *
codec(const char *compression)
{
	int			level;

	if (pg_strcasecmp(compression, "none") == 0)
		return "NONE";
	if (pg_strcasecmp(compression, "zstd") == 0)
		return "ZSTD(1)";
	if (pg_strcasecmp(compression, "lz4hc") == 0)
		return "LZ4HC(9)";
	if (sscanf(compression, "zstd(%d)", &level) == 1)
		return psprintf("ZSTD(%d)", level);
	if (sscanf(compression, "lz4hc(%d)", &level) == 1)
		return psprintf("LZ4HC(%d)", level);
	return "LZ4";
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

/*
 * The columns of a descriptor that a storage made now may keep as
 * ClickHouse's plain types (§2.5, VC11): those NOT NULL by a constraint not
 * known to be invalid.  A running CREATE's own descriptor says no more than
 * that a NOT NULL exists, and its NOT NULLs are valid, as relcache takes
 * them (RelationBuildTupleDesc()); a rewrite's new storage gets the old
 * relation's validity with its NOT NULLs (RelationBuildLocalRelation()).
 */
Bitmapset *
ch_plain_columns(TupleDesc desc)
{
	Bitmapset  *plain = NULL;

	for (int i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);

		if (!att->attisdropped && att->attnotnull &&
			TupleDescCompactAttr(desc, i)->attnullability != ATTNULLABLE_INVALID)
			plain = bms_add_member(plain, att->attnum);
	}
	return plain;
}

/*
 * Whether a column is ClickHouse's plain type in a storage's table, not
 * Nullable (VC11): one of the storage's plain columns, NOT NULL still, and
 * neither a key column -- ClickHouse reads a part's index by the table's
 * types, so its type never changes -- nor a low_cardinality one, whose NULL
 * is an entry of its dictionary, not a null map.  A column that loses NOT
 * NULL is Nullable from then on, its plain parts read through ClickHouse's
 * conversion of T into Nullable(T); the storage's set loses it before the
 * next ALTER TABLE (ch_plain_revoke()), so that it is not plain again while
 * a part may hold a NULL of it.
 */
static bool
stored_plain(const ChOptions *opts, const Bitmapset *plain, TupleDesc desc, int i, const ChColumn *col)
{
	Form_pg_attribute att = TupleDescAttr(desc, i);

	return bms_is_member(att->attnum, plain) && att->attnotnull &&
		TupleDescCompactAttr(desc, i)->attnullability != ATTNULLABLE_INVALID &&
		!col->low_cardinality &&
		!lists_column(opts->order_by, NameStr(att->attname)) &&
		!lists_column(opts->primary_key, NameStr(att->attname));
}

/* The ClickHouse CREATE TABLE of a relation, as the engine library compiles it */
char *
ch_table_definition(Relation rel, const ChOptions *opts, ChEngineKind kind)
{
	ChStorageId id;

	if (opts->low_cardinality)
		ch_low_cardinality_check(rel, opts->low_cardinality);
	ch_storage_id(rel, &rel->rd_locator, &id);
	return ch_table_definition_for(rel, RelationGetDescr(rel), opts, kind, ch_table_row_plain(&id));
}

/*
 * A relation's ClickHouse table as its definition's parts (VC21), from which
 * the engine library builds it without ClickHouse's parser
 * (chte_table_open_spec()), and its CREATE TABLE, the same table, which
 * ch.definition() shows and a library built before VC21 compiles.  Its
 * members point into memory of the current context.
 */
#define CH_SPEC_SETTINGS 7

typedef struct ChTableSpec
{
	chte_table_spec spec;
	chte_table_setting settings[CH_SPEC_SETTINGS];
	const char *projections[1];
} ChTableSpec;

static void
table_spec_for(Relation rel, TupleDesc desc, const ChOptions *opts, ChEngineKind kind, const Bitmapset *plain,
			   ChTableSpec *ts)
{
	const char *c = codec(opts->compression);
	chte_table_column *columns = palloc0(sizeof(chte_table_column) * (desc->natts + 1));
	size_t		n = 0;
	int			nsettings = 0;

	memset(ts, 0, sizeof(*ts));
	ts->spec.size = sizeof(chte_table_spec);
	for (int i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);
		ChColumn	col;
		chte_table_column *out = &columns[n];

		ch_column_for_table(rel, opts, att, &col);
		if (col.kind == CH_COL_DROPPED)
			continue;
		out->name = column_name(att->attnum);
		out->codec = c;
		if (ch_is_engine_column(rel, opts, NameStr(att->attname)))
		{
			/* ClickHouse's engines take no Nullable version, sign or is_deleted column */
			if (!att->attnotnull)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("column \"%s\" named by an option of %s must be NOT NULL",
								NameStr(att->attname), ch_engine_name(kind))));
			out->type = pstrdup(col.chtype);
		}
		else if (col.low_cardinality)
			out->type = psprintf("LowCardinality(Nullable(%s))", col.chtype);
		else if (stored_plain(opts, plain, desc, i, &col))
			out->type = pstrdup(col.chtype);
		else
			out->type = psprintf("Nullable(%s)", col.chtype);
		n++;
	}
	/* the row id, and the projection that finds a row by it (Appendix E) */
	columns[n].name = CH_ROWID_COLUMN;
	columns[n].type = "UInt64";
	columns[n].codec = "T64, LZ4";
	n++;
	ts->spec.columns = columns;
	ts->spec.ncolumns = n;
	ts->projections[0] = CH_ROWID_PROJECTION " (SELECT " CH_ROWID_COLUMN ", _part_offset ORDER BY " CH_ROWID_COLUMN ")";
	ts->spec.projections = ts->projections;
	ts->spec.nprojections = 1;

	ts->spec.engine = ch_engine_name(kind);
	switch (kind)
	{
		case CH_ENGINE_REPLACING:
			if (opts->ver)
			{
				size_t		nver;
				size_t		ndeleted = 0;
				const char **ver = translated_columns(rel, "ver", opts->ver, &nver);
				const char **deleted = opts->is_deleted ?
					translated_columns(rel, "is_deleted", opts->is_deleted, &ndeleted) : NULL;
				const char **args = palloc(sizeof(char *) * (nver + ndeleted));

				memcpy(args, ver, sizeof(char *) * nver);
				if (ndeleted > 0)
					memcpy(args + nver, deleted, sizeof(char *) * ndeleted);
				ts->spec.engine_args = args;
				ts->spec.nengine_args = nver + ndeleted;
			}
			else if (opts->is_deleted)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("option \"is_deleted\" needs option \"ver\"")));
			break;
		case CH_ENGINE_SUMMING:
		case CH_ENGINE_COALESCING:
			if (opts->sum_columns)
			{
				ts->spec.engine_args = translated_columns(rel, "sum_columns", opts->sum_columns, &ts->spec.nengine_args);
				ts->spec.engine_tuple = 1;
			}
			break;
		case CH_ENGINE_COLLAPSING:
			if (!opts->sign)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("%s needs option \"sign\"", ch_engine_name(kind))));
			ts->spec.engine_args = translated_columns(rel, "sign", opts->sign, &ts->spec.nengine_args);
			break;
		case CH_ENGINE_VERSIONED_COLLAPSING:
			if (!opts->sign || !opts->ver)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("%s needs options \"sign\" and \"ver\"", ch_engine_name(kind))));
			{
				size_t		nsign;
				size_t		nver;
				const char **sign = translated_columns(rel, "sign", opts->sign, &nsign);
				const char **ver = translated_columns(rel, "ver", opts->ver, &nver);
				const char **args = palloc(sizeof(char *) * (nsign + nver));

				memcpy(args, sign, sizeof(char *) * nsign);
				memcpy(args + nsign, ver, sizeof(char *) * nver);
				ts->spec.engine_args = args;
				ts->spec.nengine_args = nsign + nver;
			}
			break;
		default:
			break;
	}
	if (opts->order_by)
		ts->spec.order_by = translated_columns(rel, "order_by", opts->order_by, &ts->spec.norder_by);
	if (opts->primary_key)
		ts->spec.primary_key = translated_columns(rel, "primary_key", opts->primary_key, &ts->spec.nprimary_key);

	ts->settings[nsettings++] = (chte_table_setting) {"index_granularity", NULL, (uint64) opts->index_granularity};
	ts->settings[nsettings++] = (chte_table_setting) {"allow_nullable_key", NULL, 1};
	/* the parts are synced before their catalog rows commit, as PostgreSQL syncs */
	ts->settings[nsettings++] = (chte_table_setting) {"fsync_after_insert", NULL, enableFsync ? 1 : 0};
	ts->settings[nsettings++] = (chte_table_setting) {"fsync_part_directory", NULL, enableFsync ? 1 : 0};
	/*
	 * ClickHouse's statistics of every column, "basic" alone: exact least and
	 * greatest values and NULLs of each part, which part pruning and the
	 * source's aggregates read (§2.8); its default adds uniq_v2's sketches,
	 * which nothing of the extension's reads.
	 */
	if (!opts->settings || !mentions_setting(opts->settings, "auto_statistics_types"))
		ts->settings[nsettings++] = (chte_table_setting) {"auto_statistics_types", "basic", 0};
	/*
	 * VC43 (pg_accel's §3.38; §8, decided): ClickHouse's min/max skip index
	 * of every numeric and temporal column, written with each part and
	 * merge, which a scan's quals and a running bound choose granules by
	 * (engine/src/select.cpp); a text column's none, its least nearly
	 * always the empty string.  A table's own settings over them.
	 */
	if (!opts->settings || !mentions_setting(opts->settings, "add_minmax_index_for_numeric_columns"))
		ts->settings[nsettings++] = (chte_table_setting) {"add_minmax_index_for_numeric_columns", NULL, 1};
	if (!opts->settings || !mentions_setting(opts->settings, "add_minmax_index_for_temporal_columns"))
		ts->settings[nsettings++] = (chte_table_setting) {"add_minmax_index_for_temporal_columns", NULL, 1};
	Assert(nsettings <= CH_SPEC_SETTINGS);
	ts->spec.settings = ts->settings;
	ts->spec.nsettings = nsettings;
	if (opts->settings && opts->settings[0])
	{
		check_user_settings(opts->settings);
		ts->spec.user_settings = opts->settings;
	}
}

/* The CREATE TABLE of a table's definition's parts */
static char *
table_spec_sql(const ChTableSpec *ts)
{
	const chte_table_spec *s = &ts->spec;
	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfoString(&buf, "CREATE TABLE t (");
	for (size_t i = 0; i < s->ncolumns; i++)
		appendStringInfo(&buf, "%s %s CODEC(%s), ", s->columns[i].name, s->columns[i].type, s->columns[i].codec);
	for (size_t i = 0; i < s->nprojections; i++)
		appendStringInfo(&buf, "%sPROJECTION %s", i > 0 ? ", " : "", s->projections[i]);
	appendStringInfo(&buf, ") ENGINE = %s", s->engine);
	if (s->nengine_args > 0)
		appendStringInfo(&buf, "(%s)", joined(s->engine_args, s->nengine_args, s->engine_tuple != 0));
	appendStringInfo(&buf, " ORDER BY %s", s->norder_by > 0 ? joined(s->order_by, s->norder_by, true) : "tuple()");
	if (s->nprimary_key > 0)
		appendStringInfo(&buf, " PRIMARY KEY %s", joined(s->primary_key, s->nprimary_key, true));
	for (size_t i = 0; i < s->nsettings; i++)
	{
		const chte_table_setting *x = &s->settings[i];

		appendStringInfoString(&buf, i == 0 ? " SETTINGS " : ", ");
		if (x->string)
			appendStringInfo(&buf, "%s = '%s'", x->name, x->string);
		else
			appendStringInfo(&buf, "%s = " UINT64_FORMAT, x->name, x->number);
	}
	if (s->user_settings)
		appendStringInfo(&buf, ", %s", s->user_settings);
	return buf.data;
}

char *
ch_table_definition_for(Relation rel, TupleDesc desc, const ChOptions *opts, ChEngineKind kind,
						const Bitmapset *plain)
{
	ChTableSpec ts;

	table_spec_for(rel, desc, opts, kind, plain, &ts);
	return table_spec_sql(&ts);
}

/*
 * The ClickHouse settings an external table is read with: the table's own,
 * after the extension's defaults for a table of its own files (VC26) --
 * Parquet's dictionary filter limited to ch.own_parquet_dictionary_filter
 * bytes a dictionary page, where ClickHouse's reader takes 1 MB: it hashes a
 * row group's dictionary for each filtered column, 68% of a ClickBench Q19
 * worker's cycles over a dictionary of UserID's, and finds nothing for a
 * filter like "<> ''" (ch_table_engines.md §3, "ClickBench after V8g, the
 * extension's share").  Left out where the table's settings or
 * ch.engine_settings name the setting, or ch.own_parquet_dictionary_filter
 * is -1: the context the table is read in is ch.engine_settings', and the
 * table's settings are applied over it.
 */
static char *
external_read_settings(const ChTable *t)
{
	static const char *const dictionary_filter = "input_format_parquet_dictionary_filter_push_down";
	const char *own = t->options.settings;

	if (!t->own || t->ext_kind != CH_EXTERNAL_PARQUET || ch_own_parquet_dictionary_filter < 0 ||
		(own != NULL && mentions_setting(own, dictionary_filter)) ||
		(ch_engine_settings != NULL && mentions_setting(ch_engine_settings, dictionary_filter)))
		return t->options.settings;
	if (own != NULL && own[0] != '\0')
		return psprintf("%s = %d, %s", dictionary_filter, ch_own_parquet_dictionary_filter, own);
	return psprintf("%s = %d", dictionary_filter, ch_own_parquet_dictionary_filter);
}

/*
 * An external table's (§2.17): its columns, as the engine library reads them
 * from the files, and their kinds as an engine's table keeps them (§2.5),
 * which scan.c, ChScan and vexec's source read alike
 */
static ChTable *
open_external(Relation rel, const ChStorageId *id, TupleDesc desc, MemoryContext cxt, ChExternalKind kind)
{
	static const int32 formats[CH_EXTERNAL_COUNT] = {
		CHTE_EXTERNAL_PARQUET, CHTE_EXTERNAL_ARROW, CHTE_EXTERNAL_ICEBERG,
	};
	const ChEngine *eng = ch_engine();
	MemoryContext old = MemoryContextSwitchTo(cxt);
	ChTable    *t;
	chte_external_def def;
	chte_external_column *columns;
	int			ncolumns;
	StringInfoData cols;

	t = palloc0(sizeof(ChTable));
	t->relid = RelationGetRelid(rel);
	t->storage = *id;
	t->external = true;
	t->ext_kind = kind;
	ch_relation_options(rel, &t->options);
	t->natts = desc->natts;
	t->columns = palloc0(sizeof(ChColumn) * Max(desc->natts, 1));
	initStringInfo(&cols);
	for (int i = 0; i < desc->natts; i++)
	{
		ch_column_for_attribute(TupleDescAttr(desc, i), &t->columns[i]);
		if (t->columns[i].kind != CH_COL_DROPPED)
			appendStringInfo(&cols, "%s, ", column_name(TupleDescAttr(desc, i)->attnum));
	}
	appendStringInfoString(&cols, CH_ROWID_COLUMN);
	t->all_columns = cols.data;
	ch_external_columns_for(rel, desc, cxt, &columns, &ncolumns);
	t->own = ch_is_own_rel(rel);
	if (t->own)
	{
		/* a table of its own files (§2.18): its parts' columns c<attnum>, which a renamed column keeps */
		t->relative_path = ch_storage_path(id, true);
		ch_own_table_setup(rel, desc, t);
	}
	t->read_settings = external_read_settings(t);
	MemoryContextSwitchTo(old);

	if (eng->external_open == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("the engine library reads no external files"),
				 errdetail("It was built before CH7 (ch_table_engines.md §2.17).")));
	if (t->own && eng->own_open == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("the engine library keeps no table of its own files"),
				 errdetail("It was built before CH8 (ch_table_engines.md §2.18)."),
				 errhint("Give the table a location, or install an engine library built with CH8.")));
	if (t->options.location == NULL && !t->own)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("table \"%s\" has no location", RelationGetRelationName(rel))));
	memset(&def, 0, sizeof(def));
	def.format = formats[kind];
	def.name = RelationGetRelationName(rel);
	def.location = t->options.location;
	def.settings = t->read_settings;
	def.columns = columns;
	def.ncolumns = ncolumns;
	if (t->own)
		CH_CALL("opening the table in the engine library", eng->own_open(&def, &t->handle, CH_ERR));
	else
		CH_CALL("opening the table in the engine library", eng->external_open(&def, &t->handle, CH_ERR));
	return t;
}

static ChTable *
open_table(Relation rel, const ChStorageId *id, TupleDesc desc, MemoryContext cxt)
{
	ChTable    *t;
	MemoryContext old;
	chte_table_def def;
	StringInfoData cols;
	ChExternalKind ext_kind;
	ChTableSpec ts;
	const ChEngine *eng;

	if (ch_is_external_rel(rel, &ext_kind))
		return open_external(rel, id, desc, cxt, ext_kind);
	old = MemoryContextSwitchTo(cxt);

	t = palloc0(sizeof(ChTable));
	t->relid = RelationGetRelid(rel);
	t->storage = *id;
	(void) ch_is_engine_rel(rel, &t->kind);
	ch_relation_options(rel, &t->options);
	t->relative_path = ch_storage_path(id, true);
	/* the definition's parts in the caller's memory, the library's for the call alone */
	MemoryContextSwitchTo(old);
	table_spec_for(rel, desc, &t->options, t->kind, ch_table_row_plain(id), &ts);
	MemoryContextSwitchTo(cxt);
	t->definition = table_spec_sql(&ts);
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
	/* VC21: its parts, where the library builds a table of them, without ClickHouse's parser */
	eng = ch_engine();
	if (eng->table_open_spec)
	{
		ts.spec.def = &def;
		CH_CALL("opening the table in the engine library", eng->table_open_spec(&ts.spec, &t->handle, CH_ERR));
	}
	else
		CH_CALL("opening the table in the engine library", eng->table_open(&def, &t->handle, CH_ERR));
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
	if (t->listing)
		ch_listing_unpin(t->listing);
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

/*
 * Before an ALTER TABLE (the option hook's): each storage of the relation,
 * and of its descendants and a partition it attaches, loses from its plain
 * columns those NOT NULL no longer (VC11).  A column leaves NOT NULL only
 * by an ALTER TABLE, and becomes NOT NULL again only by one, so that this
 * runs before any command that could make a column plain whose parts may
 * hold a NULL.  The update takes the table's ACCESS EXCLUSIVE lock, as the
 * commands that change NOT NULL do, and only where there is something to
 * revoke: a column that lost NOT NULL by an earlier command.
 */
void
ch_plain_revoke(Oid relid)
{
	List	   *relids = find_all_inheritors(relid, NoLock, NULL);
	ListCell   *lc;

	foreach(lc, relids)
	{
		Oid			child = lfirst_oid(lc);
		Relation	rel = RelationIdGetRelation(child);
		ChStorageId id;
		Bitmapset  *plain;
		Bitmapset  *now;

		if (!RelationIsValid(rel))
			continue;
		/*
		 * Read without a lock -- one weaker than the command's own, taken
		 * first, would deadlock two of them -- and read again under it
		 */
		if (!RELKIND_HAS_STORAGE(rel->rd_rel->relkind) || !ch_is_engine_rel(rel, NULL))
		{
			RelationClose(rel);
			continue;
		}
		ch_storage_id(rel, &rel->rd_locator, &id);
		plain = ch_table_row_plain(&id);
		now = ch_plain_columns(RelationGetDescr(rel));
		RelationClose(rel);
		if (bms_is_subset(plain, now))
			continue;

		LockRelationOid(child, AccessExclusiveLock);
		rel = relation_open(child, NoLock);
		ch_storage_id(rel, &rel->rd_locator, &id);
		plain = bms_intersect(ch_table_row_plain(&id), ch_plain_columns(RelationGetDescr(rel)));
		ch_table_row_set_plain(&id, plain);
		CacheInvalidateRelcache(rel);
		relation_close(rel, NoLock);
	}
}

/*
 * After an ALTER TABLE: a column it added NOT NULL is plain in the storage
 * the column was added to (VC11), since no part of it holds the column; a
 * relation of natts attributes before.  A rewrite made new storage with
 * them already.
 */
void
ch_plain_grow(Oid relid, int natts)
{
	Relation	rel = RelationIdGetRelation(relid);
	ChStorageId id;
	Bitmapset  *plain;
	Bitmapset  *now;
	Bitmapset  *added = NULL;
	int			i = -1;

	/*
	 * Read without a lock: a relation the command added no column to -- a
	 * partition beside the one ATTACH PARTITION takes -- it may not have
	 * locked; one it added columns to, it holds
	 */
	if (!RelationIsValid(rel))
		return;
	if (!RELKIND_HAS_STORAGE(rel->rd_rel->relkind) || !ch_is_engine_rel(rel, NULL) ||
		RelationGetNumberOfAttributes(rel) <= natts)
	{
		RelationClose(rel);
		return;
	}
	RelationClose(rel);
	rel = relation_open(relid, NoLock);
	ch_storage_id(rel, &rel->rd_locator, &id);
	plain = ch_table_row_plain(&id);
	now = ch_plain_columns(RelationGetDescr(rel));
	while ((i = bms_next_member(now, i)) >= 0)
		if (i > natts && !bms_is_member(i, plain))
			added = bms_add_member(added, i);
	if (added != NULL)
	{
		ch_table_row_set_plain(&id, bms_union(plain, added));
		CacheInvalidateRelcache(rel);
	}
	relation_close(rel, NoLock);
}
