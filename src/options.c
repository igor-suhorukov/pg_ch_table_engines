/*-------------------------------------------------------------------------
 *
 * options.c
 *	  The engines' table options, in pg_class.reloptions (ch_table_engines.md
 *	  §2.4, "How options pass").
 *
 * Vanilla PostgreSQL 19 has no table-AM option parser: CREATE TABLE and
 * ALTER TABLE ... SET validate a table's options as heap's, which refuse a
 * name they do not know (tablecmds.c:963, reloptions.c:1576-1588), and an
 * extension cannot add one to heap's set without every heap table's options
 * failing their parse table (reloptions.c:1962-1964).  So the
 * ProcessUtility hook takes the engines' options out of CREATE TABLE,
 * CREATE TABLE AS, SELECT INTO, CREATE MATERIALIZED VIEW and ALTER TABLE
 * SET/RESET, lets PostgreSQL do the rest with heap's alone, and writes the
 * engines' into pg_class.reloptions itself once the command has made the
 * relation.  The relcache parses reloptions without validation and ignores
 * them (reloptions.c:1576-1588); this file parses them for the engines.
 *
 * A relation made by the running command has its options bound in memory
 * until they are written: CREATE TABLE AS writes its rows, and so asks for
 * the options, before its command ends.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/relation.h"
#include "access/reloptions.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_inherits.h"
#include "catalog/toasting.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "common/relpath.h"
#include "storage/fd.h"
#include "commands/tablecmds.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "storage/lmgr.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/varlena.h"

#include "chte.h"

static ProcessUtility_hook_type prev_ProcessUtility = NULL;
static object_access_hook_type prev_object_access = NULL;

/* Names of the engines' options, and whether a change needs an empty table */
typedef struct ChOptionName
{
	const char *name;
	bool		defining;		/* fixed while the table has parts */
} ChOptionName;

static const ChOptionName option_names[] = {
	{"storage", true},
	{"order_by", true},
	{"primary_key", true},
	{"index_granularity", true},
	{"compression", false},
	{"settings", false},
	{"merges", false},
	{"ver", true},
	{"is_deleted", true},
	{"sign", true},
	{"sum_columns", true},
	{"low_cardinality", true},
	/* an external table's (§2.17): where it reads, which changes at any time */
	{"location", false},
	/* a table of its own files' (§2.18): its parts' codec, the next part's and merge's */
	{"codec", false},
};

/*
 * Relations the running utility command made with an engine's access method,
 * and the options they get: from OAT_POST_CREATE until the command returns.
 */
typedef struct BoundOptions
{
	Oid			relid;
	List	   *defs;			/* DefElems, in CacheMemoryContext's child */
} BoundOptions;

static MemoryContext bound_cxt = NULL;
static List *bound = NIL;		/* of BoundOptions */
static List *pending_defs = NIL;	/* the options of the CREATE being run */
static bool pending_create = false;
static int	utility_depth = 0;

static void ch_ProcessUtility(PlannedStmt *pstmt, const char *queryString, bool readOnlyTree,
							  ProcessUtilityContext context, ParamListInfo params,
							  QueryEnvironment *queryEnv, DestReceiver *dest, QueryCompletion *qc);
static void ch_object_access(ObjectAccessType access, Oid classId, Oid objectId, int subId, void *arg);

void
ch_options_init(void)
{
	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = ch_ProcessUtility;
	prev_object_access = object_access_hook;
	object_access_hook = ch_object_access;
}

bool
ch_is_option_name(const char *name)
{
	for (int i = 0; i < lengthof(option_names); i++)
		if (strcmp(option_names[i].name, name) == 0)
			return true;
	return false;
}

static bool
option_is_defining(const char *name)
{
	for (int i = 0; i < lengthof(option_names); i++)
		if (strcmp(option_names[i].name, name) == 0)
			return option_names[i].defining;
	return false;
}

/* A column list option: names separated by commas, each an identifier */
static void
check_column_list(const char *option, const char *value)
{
	char	   *copy = pstrdup(value);
	List	   *names;

	if (!SplitIdentifierString(copy, ',', &names))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid value for option \"%s\": \"%s\"", option, value),
				 errhint("Give a comma-separated list of column names.")));
}

static bool
parse_bool_value(const char *option, const char *value)
{
	bool		result;

	if (!parse_bool(value, &result))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid value for boolean option \"%s\": \"%s\"", option, value)));
	return result;
}

/* "name(N)" with N in [lo, hi] */
static bool
codec_with_level(const char *value, const char *name, int lo, int hi)
{
	size_t		len = strlen(name);
	char	   *end;
	long		level;

	if (pg_strncasecmp(value, name, len) != 0 || value[len] != '(')
		return false;
	level = strtol(value + len + 1, &end, 10);
	return end != value + len + 1 && end[0] == ')' && end[1] == '\0' && level >= lo && level <= hi;
}

static void
check_compression(const char *value)
{
	if (pg_strcasecmp(value, "lz4") == 0 || pg_strcasecmp(value, "zstd") == 0 ||
		pg_strcasecmp(value, "none") == 0 || pg_strcasecmp(value, "lz4hc") == 0)
		return;
	if (codec_with_level(value, "zstd", 1, 22) || codec_with_level(value, "lz4hc", 1, 12))
		return;
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("invalid value for option \"compression\": \"%s\"", value),
			 errhint("Valid values are \"lz4\", \"lz4hc\", \"lz4hc(N)\", \"zstd\", \"zstd(N)\" and \"none\".")));
}

static const char *
def_string(DefElem *def)
{
	if (def->arg == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("option \"%s\" needs a value", def->defname)));
	return defGetString(def);
}

/* Checks the engines' options of a CREATE or ALTER TABLE */
void
ch_options_validate(List *defs, ChEngineKind kind)
{
	ListCell   *lc;

	foreach(lc, defs)
	{
		DefElem    *def = lfirst_node(DefElem, lc);
		const char *name = def->defname;

		if (def->defaction == DEFELEM_DROP)
			continue;
		if (strcmp(name, "storage") == 0)
		{
			const char *v = def_string(def);

			if (strcmp(v, "files") != 0 && strcmp(v, "pages") != 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid value for option \"storage\": \"%s\"", v),
						 errhint("Valid values are \"pages\" and \"files\".")));

		}
		else if (strcmp(name, "order_by") == 0 || strcmp(name, "primary_key") == 0 ||
				 strcmp(name, "sum_columns") == 0 || strcmp(name, "low_cardinality") == 0)
			check_column_list(name, def_string(def));
		else if (strcmp(name, "ver") == 0 || strcmp(name, "is_deleted") == 0 || strcmp(name, "sign") == 0)
		{
			char	   *copy = pstrdup(def_string(def));
			List	   *names;

			if (!SplitIdentifierString(copy, ',', &names) || list_length(names) != 1)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("option \"%s\" names one column", name)));
		}
		else if (strcmp(name, "index_granularity") == 0)
		{
			int			v = (int) defGetInt64(def);

			if (v < 1 || v > 1048576)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("index_granularity must be between 1 and 1048576")));
		}
		else if (strcmp(name, "compression") == 0)
			check_compression(def_string(def));
		else if (strcmp(name, "merges") == 0)
			(void) parse_bool_value(name, def_string(def));
		else if (strcmp(name, "settings") == 0)
			(void) def_string(def);
		else if (strcmp(name, "location") == 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("option \"location\" does not apply to %s", ch_engine_name(kind)),
					 errhint("A table that reads files is made with an access method of its format: parquet, arrow or iceberg.")));
		else
			elog(ERROR, "unexpected option \"%s\"", name);

		/* the engines' own options belong to their engines */
		if ((strcmp(name, "ver") == 0 && kind != CH_ENGINE_REPLACING && kind != CH_ENGINE_VERSIONED_COLLAPSING) ||
			(strcmp(name, "is_deleted") == 0 && kind != CH_ENGINE_REPLACING) ||
			(strcmp(name, "sign") == 0 && kind != CH_ENGINE_COLLAPSING && kind != CH_ENGINE_VERSIONED_COLLAPSING) ||
			(strcmp(name, "sum_columns") == 0 && kind != CH_ENGINE_SUMMING && kind != CH_ENGINE_COALESCING))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("option \"%s\" does not apply to %s", name, ch_engine_name(kind))));
	}
}

/*
 * The options of an external table (§2.17): location -- an absolute path,
 * set only by a role that may read the server's files, as COPY FROM a file
 * and file_fdw require -- needed where the table is made; settings,
 * ClickHouse's for its format.  A table of parquet or arrow made without a
 * location is one of its own files (§2.18, CH8), the kind fixed then: its
 * options settings and codec, its parts' -- lz4, zstd or none, by default
 * lz4 for Parquet and none for Arrow.  The engines' others do not apply.
 * own says which kind an existing table is; where it is made, its options do.
 */
void
ch_external_options_validate(List *defs, ChExternalKind kind, bool creating, bool own)
{
	ListCell   *lc;
	bool		has_location = false;

	if (creating)
	{
		own = kind != CH_EXTERNAL_ICEBERG;
		foreach(lc, defs)
			if (strcmp(lfirst_node(DefElem, lc)->defname, "location") == 0)
				own = false;
	}
	foreach(lc, defs)
	{
		DefElem    *def = lfirst_node(DefElem, lc);
		const char *name = def->defname;

		if (strcmp(name, "location") == 0 && own)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("option \"location\" does not apply to a table of its own files"),
					 errdetail("A table of access method %s made without a location keeps its rows in files of its own, "
							   "and stays so.", ch_external_name(kind)),
					 errhint("Read files elsewhere with another table: CREATE TABLE ... USING %s WITH (location = '...').",
							 ch_external_name(kind))));
		if (strcmp(name, "codec") == 0)
		{
			const char *v;

			if (!own)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("option \"codec\" does not apply to a table of access method %s that reads files at a location",
								ch_external_name(kind)),
						 errdetail("Its files' codecs are their writers'.")));
			if (def->defaction == DEFELEM_DROP)
				continue;
			v = def_string(def);
			if (strcmp(v, "lz4") != 0 && strcmp(v, "zstd") != 0 && strcmp(v, "none") != 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("codec must be \"lz4\", \"zstd\" or \"none\"")));
			continue;
		}
		if (strcmp(name, "location") == 0)
		{
			const char *v;

			if (def->defaction == DEFELEM_DROP)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("option \"location\" of a table of access method %s cannot be reset", ch_external_name(kind)),
						 errhint("Set another location with ALTER TABLE ... SET (location = '...').")));
			v = def_string(def);
			if (v[0] != '/')
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("location \"%s\" is not an absolute path", v)));
			if (!has_privs_of_role(GetUserId(), ROLE_PG_READ_SERVER_FILES))
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("permission denied to set the location of a table of access method %s", ch_external_name(kind)),
						 errdetail("Only roles with privileges of the \"%s\" role may read the server's files.",
								   "pg_read_server_files")));
			has_location = true;
		}
		else if (strcmp(name, "settings") == 0)
		{
			if (def->defaction != DEFELEM_DROP)
				(void) def_string(def);
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("option \"%s\" does not apply to a table of access method %s", name, ch_external_name(kind)),
					 own ? errhint("Its options are \"codec\" and \"settings\".") :
					 errhint("Its options are \"location\" and \"settings\".")));
	}
	if (creating && !has_location && !own)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("a table of access method %s needs option \"location\"", ch_external_name(kind)),
				 errhint("Name its files: CREATE TABLE ... USING %s WITH (location = '/path/to/files').",
						 ch_external_name(kind))));
}

static void
set_defaults(ChOptions *opts)
{
	memset(opts, 0, sizeof(*opts));
	opts->storage = CH_STORAGE_FILES;
	opts->index_granularity = 8192;
	opts->compression = "lz4";
	opts->merges = true;
}

static void
apply_option(ChOptions *opts, const char *name, const char *value)
{
	if (strcmp(name, "storage") == 0)
		opts->storage = strcmp(value, "pages") == 0 ? CH_STORAGE_PAGES : CH_STORAGE_FILES;
	else if (strcmp(name, "order_by") == 0)
		opts->order_by = pstrdup(value);
	else if (strcmp(name, "primary_key") == 0)
		opts->primary_key = pstrdup(value);
	else if (strcmp(name, "index_granularity") == 0)
		opts->index_granularity = atoi(value);
	else if (strcmp(name, "compression") == 0)
		opts->compression = pstrdup(value);
	else if (strcmp(name, "settings") == 0)
		opts->settings = pstrdup(value);
	else if (strcmp(name, "merges") == 0)
	{
		bool		b = true;

		(void) parse_bool(value, &b);
		opts->merges = b;
	}
	else if (strcmp(name, "ver") == 0)
		opts->ver = pstrdup(value);
	else if (strcmp(name, "is_deleted") == 0)
		opts->is_deleted = pstrdup(value);
	else if (strcmp(name, "sign") == 0)
		opts->sign = pstrdup(value);
	else if (strcmp(name, "sum_columns") == 0)
		opts->sum_columns = pstrdup(value);
	else if (strcmp(name, "low_cardinality") == 0)
		opts->low_cardinality = pstrdup(value);
	else if (strcmp(name, "location") == 0)
		opts->location = pstrdup(value);
	else if (strcmp(name, "codec") == 0)
		opts->codec = pstrdup(value);
}

/* The engines' options in a reloptions array; the others are heap's */
void
ch_options_parse(Datum reloptions, ChOptions *opts)
{
	List	   *defs;
	ListCell   *lc;

	set_defaults(opts);
	if (reloptions == (Datum) 0)
		return;
	defs = untransformRelOptions(reloptions);
	foreach(lc, defs)
	{
		DefElem    *def = lfirst_node(DefElem, lc);

		if (def->defnamespace == NULL && ch_is_option_name(def->defname))
			apply_option(opts, def->defname, defGetString(def));
	}
}

static List *
bound_defs(Oid relid)
{
	ListCell   *lc;

	foreach(lc, bound)
	{
		BoundOptions *b = lfirst(lc);

		if (b->relid == relid)
			return b->defs;
	}
	return NIL;
}

static bool
is_bound(Oid relid)
{
	ListCell   *lc;

	foreach(lc, bound)
		if (((BoundOptions *) lfirst(lc))->relid == relid)
			return true;
	return false;
}

/* The options of a relation of an engine's AM */
void
ch_relation_options(Relation rel, ChOptions *opts)
{
	HeapTuple	tuple;
	Datum		datum;
	bool		isnull;

	if (is_bound(RelationGetRelid(rel)))
	{
		ListCell   *lc;

		set_defaults(opts);
		foreach(lc, bound_defs(RelationGetRelid(rel)))
		{
			DefElem    *def = lfirst_node(DefElem, lc);

			apply_option(opts, def->defname, defGetString(def));
		}
		return;
	}

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(RelationGetRelid(rel)));
	if (!HeapTupleIsValid(tuple))
	{
		ListCell   *lc;

		/*
		 * made by the running command, before its options were bound: the
		 * CREATE's own -- whether a table of parquet or arrow has its own
		 * files (§2.18) is known so as its storage is made
		 */
		set_defaults(opts);
		if (pending_create)
			foreach(lc, pending_defs)
			{
				DefElem    *def = lfirst_node(DefElem, lc);

				apply_option(opts, def->defname, defGetString(def));
			}
		return;
	}
	datum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &isnull);
	ch_options_parse(isnull ? (Datum) 0 : datum, opts);
	ReleaseSysCache(tuple);
}

/* Remembers the options of a relation the running command makes */
void
ch_options_bind(Oid relid, List *defs)
{
	BoundOptions *b;
	MemoryContext old;

	if (bound_cxt == NULL)
		bound_cxt = AllocSetContextCreate(TopMemoryContext, "ch bound options", ALLOCSET_SMALL_SIZES);
	old = MemoryContextSwitchTo(bound_cxt);
	b = palloc(sizeof(BoundOptions));
	b->relid = relid;
	b->defs = copyObject(defs);
	bound = lappend(bound, b);
	MemoryContextSwitchTo(old);
}

static void
forget_bound(void)
{
	bound = NIL;
	if (bound_cxt != NULL)
		MemoryContextReset(bound_cxt);
}

/* Splits a statement's options into the engines' and the rest */
static List *
take_engine_options(List **options)
{
	List	   *ours = NIL;
	List	   *rest = NIL;
	ListCell   *lc;

	foreach(lc, *options)
	{
		DefElem    *def = lfirst_node(DefElem, lc);

		if (def->defnamespace == NULL && ch_is_option_name(def->defname))
			ours = lappend(ours, def);
		else
			rest = lappend(rest, def);
	}
	*options = rest;
	return ours;
}

/* Turns DefElems of any value type into string ones, as reloptions keep them */
static List *
stringify_defs(List *defs)
{
	List	   *result = NIL;
	ListCell   *lc;

	foreach(lc, defs)
	{
		DefElem    *def = lfirst_node(DefElem, lc);

		if (def->arg == NULL || IsA(def->arg, String))
			result = lappend(result, def);
		else
			result = lappend(result, makeDefElem(def->defname, (Node *) makeString(defGetString(def)), def->location));
	}
	return result;
}

/* Writes options into pg_class.reloptions; the relation must be visible */
static void
store_reloptions(Oid relid, List *defs, bool reset, bool keep_others)
{
	Relation	classrel;
	HeapTuple	tuple;
	HeapTuple	newtuple;
	Datum		old;
	Datum		newopts;
	bool		isnull;
	Datum		repl_val[Natts_pg_class];
	bool		repl_null[Natts_pg_class];
	bool		repl_repl[Natts_pg_class];

	classrel = table_open(RelationRelationId, RowExclusiveLock);
	tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", relid);
	old = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &isnull);
	if (isnull || !keep_others)
		old = (Datum) 0;
	newopts = transformRelOptions(old, defs, NULL, NULL, false, reset);

	memset(repl_val, 0, sizeof(repl_val));
	memset(repl_null, false, sizeof(repl_null));
	memset(repl_repl, false, sizeof(repl_repl));
	if (newopts != (Datum) 0)
		repl_val[Anum_pg_class_reloptions - 1] = newopts;
	else
		repl_null[Anum_pg_class_reloptions - 1] = true;
	repl_repl[Anum_pg_class_reloptions - 1] = true;
	newtuple = heap_modify_tuple(tuple, RelationGetDescr(classrel), repl_val, repl_null, repl_repl);
	CatalogTupleUpdate(classrel, &newtuple->t_self, newtuple);
	heap_freetuple(newtuple);
	heap_freetuple(tuple);
	table_close(classrel, RowExclusiveLock);
}

/* Removes the engines' options from a relation that left their AMs */
static void
strip_engine_options(Oid relid)
{
	HeapTuple	tuple;
	Datum		datum;
	bool		isnull;
	List	   *keep = NIL;
	ListCell   *lc;
	bool		had = false;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		return;
	datum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &isnull);
	if (!isnull)
	{
		foreach(lc, untransformRelOptions(datum))
		{
			DefElem    *def = lfirst_node(DefElem, lc);

			if (def->defnamespace == NULL && ch_is_option_name(def->defname))
				had = true;
			else
				keep = lappend(keep, def);
		}
	}
	ReleaseSysCache(tuple);
	if (had)
		store_reloptions(relid, keep, false, false);
}

/* The options of a new relation, defaults filled where the CREATE had none */
static List *
with_defaults(List *defs)
{
	bool		has_storage = false;
	ListCell   *lc;

	foreach(lc, defs)
		if (strcmp(lfirst_node(DefElem, lc)->defname, "storage") == 0)
			has_storage = true;
	if (!has_storage)
		defs = lappend(defs, makeDefElem("storage",
										 (Node *) makeString(ch_default_storage == CH_STORAGE_PAGES ? "pages" : "files"), -1));
	return defs;
}

/* The access method a CREATE names, or the one it gets by default */
static const char *
create_access_method(const char *am)
{
	if (am != NULL)
		return am;
	return default_table_access_method;
}

static PlannedStmt *
copy_with_utility(PlannedStmt *pstmt, Node *stmt)
{
	PlannedStmt *copy = makeNode(PlannedStmt);

	memcpy(copy, pstmt, sizeof(PlannedStmt));
	copy->utilityStmt = stmt;
	return copy;
}

static void
run_utility(PlannedStmt *pstmt, const char *queryString, bool readOnlyTree,
			ProcessUtilityContext context, ParamListInfo params,
			QueryEnvironment *queryEnv, DestReceiver *dest, QueryCompletion *qc)
{
	if (prev_ProcessUtility)
		prev_ProcessUtility(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc);
	else
		standard_ProcessUtility(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc);
}

/* Writes the options of the relations the command made, once it has made them */
static void
store_bound(void)
{
	ListCell   *lc;

	if (bound == NIL)
		return;
	CommandCounterIncrement();
	foreach(lc, bound)
	{
		BoundOptions *b = lfirst(lc);

		if (SearchSysCacheExists1(RELOID, ObjectIdGetDatum(b->relid)))
			store_reloptions(b->relid, b->defs, false, true);
	}
	CommandCounterIncrement();
}

/* Whether a relation has parts in the current snapshot */
static bool
relation_has_parts(Relation rel)
{
	ChStorageId id;

	ch_buffer_flush_relation(rel);
	ch_storage_id(rel, &rel->rd_locator, &id);
	return ch_parts_visible(&id, GetActiveSnapshot()) != NIL;
}

/*
 * ALTER TABLE ... SET/RESET (...) on a table of an engine's AM: done here for
 * heap's options and the engines' alike, since PostgreSQL would validate the
 * whole merged list as heap's.
 */
static void
alter_table_options(Oid relid, AlterTableCmd *cmd)
{
	Relation	rel;
	List	   *defs = (List *) cmd->def;
	List	   *ours;
	List	   *heaps;
	ChEngineKind kind;
	bool		defining = false;
	ListCell   *lc;
	LOCKMODE	lockmode;
	HeapTuple	tuple;
	Datum		datum;
	bool		isnull;
	Datum		merged;
	List	   *heap_only = NIL;

	ChExternalKind ext_kind;
	bool		external;

	heaps = list_copy(defs);
	ours = take_engine_options(&heaps);
	foreach(lc, ours)
		if (option_is_defining(lfirst_node(DefElem, lc)->defname))
			defining = true;

	/*
	 * as tablecmds.c would take for heap's, and all of it for a defining
	 * change -- or for where an external table reads, which a running query
	 * of it finishes reading first
	 */
	{
		Relation	r = RelationIdGetRelation(relid);

		external = RelationIsValid(r) && ch_is_external_rel(r, &ext_kind);
		if (RelationIsValid(r))
			RelationClose(r);
	}
	lockmode = defining || (external && ours != NIL) ? AccessExclusiveLock :
		Max(AlterTableGetRelOptionsLockLevel(heaps), ShareUpdateExclusiveLock);
	rel = table_open(relid, lockmode);
	if (external)
	{
		/* §2.17: location and settings, location set at any time and never reset */
		if (!ch_is_external_rel(rel, &ext_kind))
			elog(ERROR, "relation \"%s\" is not of an external access method", RelationGetRelationName(rel));
		if (cmd->subtype == AT_ResetRelOptions)
			foreach(lc, ours)
				lfirst_node(DefElem, lc)->defaction = DEFELEM_DROP;
		ch_external_options_validate(ours, ext_kind, false, ch_is_own_rel(rel));
		defining = false;
	}
	else if (!ch_is_engine_rel(rel, &kind))
		elog(ERROR, "relation \"%s\" is not of an engine's access method", RelationGetRelationName(rel));
	else if (cmd->subtype == AT_SetRelOptions)
	{
		ch_options_validate(ours, kind);
		foreach(lc, ours)
			if (strcmp(lfirst_node(DefElem, lc)->defname, "low_cardinality") == 0)
				ch_low_cardinality_check(rel, defGetString(lfirst_node(DefElem, lc)));
	}
	if (defining && relation_has_parts(rel))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot change the storage, keys or engine options of \"%s\" while it has data",
						RelationGetRelationName(rel)),
				 errhint("TRUNCATE the table first, or make a new table with CREATE TABLE AS.")));

	/* heap's options, validated as heap's alone */
	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	datum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &isnull);
	if (!isnull)
		foreach(lc, untransformRelOptions(datum))
		{
			DefElem    *def = lfirst_node(DefElem, lc);

			if (!(def->defnamespace == NULL && ch_is_option_name(def->defname)))
				heap_only = lappend(heap_only, def);
		}
	ReleaseSysCache(tuple);
	merged = transformRelOptions(heap_only == NIL ? (Datum) 0 :
								 transformRelOptions((Datum) 0, heap_only, NULL, NULL, false, false),
								 heaps, NULL, NULL, false, cmd->subtype == AT_ResetRelOptions);
	(void) heap_reloptions(RELKIND_RELATION, merged, true);

	store_reloptions(relid, stringify_defs(defs), cmd->subtype == AT_ResetRelOptions, true);
	ch_table_forget(relid);
	table_close(rel, NoLock);
}

/* Whether a database's directory in a tablespace holds a storage of the files store */
static bool
dir_has_storage(const char *dir)
{
	DIR		   *d = AllocateDir(dir);
	struct dirent *de;
	bool		found = false;

	if (d == NULL)
		return false;
	while ((de = ReadDirExtended(d, dir, DEBUG1)) != NULL)
		if (strncmp(de->d_name, "ch_", 3) == 0)
			found = true;
	FreeDir(d);
	return found;
}

/*
 * CREATE DATABASE copies a template's relation files alone (dbcommands.c:79-89,
 * 745): a template with tables of the engines would give the new database
 * catalog rows of parts it does not have.
 */
static void
check_template(CreatedbStmt *stmt)
{
	const char *template = "template1";
	ListCell   *lc;
	Oid			dboid;
	char	   *path;
	DIR		   *d;
	struct dirent *de;
	bool		found;

	foreach(lc, stmt->options)
	{
		DefElem    *def = lfirst_node(DefElem, lc);

		if (strcmp(def->defname, "template") == 0 && def->arg)
			template = defGetString(def);
	}
	dboid = get_database_oid(template, true);
	if (!OidIsValid(dboid))
		return;
	path = psprintf("base/%u", dboid);
	found = dir_has_storage(path);
	d = AllocateDir("pg_tblspc");
	while (!found && d && (de = ReadDirExtended(d, "pg_tblspc", DEBUG1)) != NULL)
	{
		if (de->d_name[0] == '.')
			continue;
		found = dir_has_storage(psprintf("pg_tblspc/%s/%s/%u", de->d_name, TABLESPACE_VERSION_DIRECTORY, dboid));
	}
	if (d)
		FreeDir(d);
	if (found)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("template database \"%s\" has tables of the ClickHouse engines", template),
				 errdetail("CREATE DATABASE copies a template's relation files, not their parts' directories."),
				 errhint("Drop those tables from the template, or use another template.")));
}

static void
ch_ProcessUtility(PlannedStmt *pstmt, const char *queryString, bool readOnlyTree,
				  ProcessUtilityContext context, ParamListInfo params,
				  QueryEnvironment *queryEnv, DestReceiver *dest, QueryCompletion *qc)
{
	Node	   *parsetree = pstmt->utilityStmt;
	ChEngineKind kind;
	bool		outermost = (utility_depth == 0);
	List	   *after_alter_strip = NIL;
	List	   *grow_relids = NIL;	/* VC11: the tables an ALTER may add columns to */
	List	   *grow_natts = NIL;	/* and their attributes before it */
	List	   *external_checks = NIL;	/* §2.17: the external tables an ALTER changes */

	/*
	 * A command that needs a database to itself waits for its other backends
	 * to exit (dbcommands.c, CountOtherDBBackends()): the database's merge
	 * worker is told to first (worker.c).
	 */
	if (IsA(parsetree, DropdbStmt))
		ch_merge_workers_stop(get_database_oid(((DropdbStmt *) parsetree)->dbname, true));
	else if (IsA(parsetree, RenameStmt) && ((RenameStmt *) parsetree)->renameType == OBJECT_DATABASE)
		ch_merge_workers_stop(get_database_oid(((RenameStmt *) parsetree)->subname, true));
	else if (IsA(parsetree, AlterDatabaseStmt))
	{
		ListCell   *lc;

		foreach(lc, ((AlterDatabaseStmt *) parsetree)->options)
			if (strcmp(((DefElem *) lfirst(lc))->defname, "tablespace") == 0)
				ch_merge_workers_stop(get_database_oid(((AlterDatabaseStmt *) parsetree)->dbname, true));
	}
	else if (IsA(parsetree, CreatedbStmt))
	{
		ListCell   *lc;

		foreach(lc, ((CreatedbStmt *) parsetree)->options)
			if (strcmp(((DefElem *) lfirst(lc))->defname, "template") == 0)
				ch_merge_workers_stop(get_database_oid(defGetString(lfirst(lc)), true));
	}

	if (IsA(parsetree, CreateStmt))
	{
		CreateStmt *stmt = (CreateStmt *) parsetree;
		ChExternalKind ext_kind;

		if (ch_external_am_name_kind(create_access_method(stmt->accessMethod), &ext_kind))
		{
			CreateStmt *copy;
			List	   *ours;

			/* §2.17: not a partition in CH7, nor partitioned (§8 item 17) */
			if (stmt->partspec != NULL || stmt->partbound != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("a table of access method %s cannot be partitioned or a partition", ch_external_name(ext_kind))));
			copy = copyObject(stmt);
			ours = take_engine_options(&copy->options);
			ch_external_options_validate(ours, ext_kind, true, false);
			pending_defs = stringify_defs(ours);
			pending_create = true;
			pstmt = copy_with_utility(pstmt, (Node *) copy);
			readOnlyTree = false;
		}
		else if (stmt->partspec == NULL &&
				 ch_am_name_kind(create_access_method(stmt->accessMethod), &kind))
		{
			CreateStmt *copy = copyObject(stmt);
			List	   *ours = take_engine_options(&copy->options);

			ch_options_validate(ours, kind);
			pending_defs = with_defaults(stringify_defs(ours));
			pending_create = true;
			pstmt = copy_with_utility(pstmt, (Node *) copy);
			readOnlyTree = false;
		}
	}
	else if (IsA(parsetree, CreateTableAsStmt))
	{
		CreateTableAsStmt *stmt = (CreateTableAsStmt *) parsetree;
		ChExternalKind ext_kind;

		if (stmt->into && ch_external_am_name_kind(create_access_method(stmt->into->accessMethod), &ext_kind))
		{
			/* WITH NO DATA makes one; rows the access method refuses */
			CreateTableAsStmt *copy = copyObject(stmt);
			List	   *ours = take_engine_options(&copy->into->options);

			ch_external_options_validate(ours, ext_kind, true, false);
			pending_defs = stringify_defs(ours);
			pending_create = true;
			pstmt = copy_with_utility(pstmt, (Node *) copy);
			readOnlyTree = false;
		}
		else if (stmt->into && ch_am_name_kind(create_access_method(stmt->into->accessMethod), &kind))
		{
			CreateTableAsStmt *copy = copyObject(stmt);
			List	   *ours = take_engine_options(&copy->into->options);

			ch_options_validate(ours, kind);
			pending_defs = with_defaults(stringify_defs(ours));
			pending_create = true;
			pstmt = copy_with_utility(pstmt, (Node *) copy);
			readOnlyTree = false;
		}
	}
	else if (IsA(parsetree, AlterTableStmt))
	{
		AlterTableStmt *stmt = (AlterTableStmt *) parsetree;
		Oid			relid;

		relid = RangeVarGetRelid(stmt->relation, NoLock, true);
		if (OidIsValid(relid) && stmt->objtype == OBJECT_TABLE)
		{
			Relation	rel = RelationIdGetRelation(relid);
			bool		ours = RelationIsValid(rel) && ch_is_engine_rel(rel, &kind);
			bool		external = RelationIsValid(rel) && ch_is_external_rel(rel, NULL);
			AlterTableStmt *copy = NULL;
			List	   *rest = NIL;
			ListCell   *lc;

			if (RelationIsValid(rel))
				RelationClose(rel);
			/* its columns or its location changed: checked against its files as the command ends */
			if (external)
				external_checks = lappend_oid(external_checks, relid);

			/*
			 * VC11: the storages' plain columns that lost NOT NULL by an
			 * earlier command are revoked before this one can give it back,
			 * the table's, its descendants' and a partition's it attaches;
			 * and the tables' attributes are counted, so that a column it
			 * adds NOT NULL is made plain after it (ch_plain_grow()).
			 */
			ch_plain_revoke(relid);
			foreach(lc, stmt->cmds)
			{
				AlterTableCmd *cmd = lfirst_node(AlterTableCmd, lc);

				if (cmd->subtype == AT_AttachPartition && IsA(cmd->def, PartitionCmd))
				{
					Oid			part = RangeVarGetRelid(((PartitionCmd *) cmd->def)->name, NoLock, true);
					ChExternalKind ext_kind;

					/* §2.17: an external table is no partition in CH7 */
					if (OidIsValid(part) && ch_is_external_am(get_rel_relam(part), &ext_kind))
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("a table of access method %s cannot be a partition", ch_external_name(ext_kind))));
					if (OidIsValid(part))
						ch_plain_revoke(part);
				}
			}
			foreach(lc, find_all_inheritors(relid, NoLock, NULL))
			{
				Relation	r = RelationIdGetRelation(lfirst_oid(lc));

				if (!RelationIsValid(r))
					continue;
				if (ch_is_engine_rel(r, NULL))
				{
					grow_relids = lappend_oid(grow_relids, lfirst_oid(lc));
					grow_natts = lappend_int(grow_natts, RelationGetNumberOfAttributes(r));
				}
				RelationClose(r);
			}
			foreach(lc, stmt->cmds)
			{
				AlterTableCmd *cmd = lfirst_node(AlterTableCmd, lc);

				if ((ours || external) && (cmd->subtype == AT_SetRelOptions || cmd->subtype == AT_ResetRelOptions))
				{
					alter_table_options(relid, cmd);
					continue;
				}

				if (ours && cmd->subtype == AT_DropNotNull)
				{
					Relation	r = relation_open(relid, AccessShareLock);
					ChOptions	opts;

					ch_relation_options(r, &opts);
					if (ch_is_engine_column(r, &opts, cmd->name))
						ereport(ERROR,
								(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
								 errmsg("column \"%s\" is named by an option of %s and stays NOT NULL",
										cmd->name, ch_engine_name(kind))));
					relation_close(r, AccessShareLock);
				}
				if (cmd->subtype == AT_SetAccessMethod)
				{
					ChEngineKind newkind;
					ChExternalKind ext_kind;
					bool		to_ours = ch_am_name_kind(create_access_method(cmd->name), &newkind);

					/*
					 * §2.17: a table that reads files is made so; one leaves
					 * them for another access method, its rows read into it
					 */
					if (ch_external_am_name_kind(create_access_method(cmd->name), &ext_kind))
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("cannot change the access method of a table to %s", ch_external_name(ext_kind)),
								 errhint("Make the table with CREATE TABLE ... USING %s WITH (location = '...').",
										 ch_external_name(ext_kind))));
					if (external)
					{
						after_alter_strip = lappend_oid(after_alter_strip, relid);
						external_checks = list_delete_oid(external_checks, relid);
					}
					if (ours && !to_ours)
						after_alter_strip = lappend_oid(after_alter_strip, relid);
					else if (!ours && to_ours)
					{
						/*
						 * The rewrite writes the rows into a transient relation the
						 * command makes (make_new_heap()): it gets the same options,
						 * bound as it is made, and the table gets them after the swap.
						 */
						pending_defs = with_defaults(NIL);
						ch_options_bind(relid, pending_defs);
						pending_create = true;
					}
				}
				rest = lappend(rest, cmd);
			}
			if (list_length(rest) != list_length(stmt->cmds))
			{
				if (rest == NIL)
				{
					CommandCounterIncrement();
					return;
				}
				copy = copyObject(stmt);
				copy->cmds = copyObject(rest);
				pstmt = copy_with_utility(pstmt, (Node *) copy);
				readOnlyTree = false;
			}
		}
	}

	else if (IsA(parsetree, CreatedbStmt))
		check_template((CreatedbStmt *) parsetree);
	else if (IsA(parsetree, CopyStmt))
	{
		CopyStmt   *stmt = (CopyStmt *) parsetree;

		/*
		 * §2.17, the user's rule: the table form of COPY ... TO of an
		 * external table runs as its query form under WHERE false, so that
		 * pg_dump's dump holds no row of it
		 */
		if (!stmt->is_from && stmt->relation != NULL && stmt->query == NULL)
		{
			Oid			relid = RangeVarGetRelid(stmt->relation, NoLock, true);
			Node	   *query_form = OidIsValid(relid) ? ch_external_copy_to(stmt, relid) : NULL;

			if (query_form)
			{
				pstmt = copy_with_utility(pstmt, query_form);
				readOnlyTree = false;
			}
		}
	}

	utility_depth++;
	PG_TRY();
	{
		run_utility(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc);
	}
	PG_CATCH();
	{
		utility_depth--;
		if (outermost)
		{
			pending_defs = NIL;
			pending_create = false;
			/* the relations it bound are rolled back with it */
			forget_bound();
		}
		PG_RE_THROW();
	}
	PG_END_TRY();
	utility_depth--;
	if (outermost)
	{
		pending_defs = NIL;
		pending_create = false;
	}
	{
		ListCell   *lr;
		ListCell   *ln;

		forboth(lr, grow_relids, ln, grow_natts)
			ch_plain_grow(lfirst_oid(lr), lfirst_int(ln));
	}
	if (external_checks != NIL)
	{
		ListCell   *lc;

		/* the columns and location the ALTER left, against the files the location names now */
		CommandCounterIncrement();
		foreach(lc, external_checks)
		{
			Relation	r = RelationIdGetRelation(lfirst_oid(lc));

			if (!RelationIsValid(r))
				continue;
			PG_TRY();
			{
				if (ch_is_external_rel(r, NULL) && !ch_is_own_rel(r))
					ch_external_check(r);
			}
			PG_FINALLY();
			{
				RelationClose(r);
			}
			PG_END_TRY();
		}
	}

	/*
	 * A utility statement's rows -- COPY's, which no ExecutorFinish ends --
	 * become parts as it ends (§2.7 rule 3); after COMMIT or ROLLBACK there is
	 * nothing left to flush.
	 */
	if (IsTransactionState())
		ch_buffer_flush_all();

	if (outermost && IsTransactionState())
	{
		ListCell   *lc;

		store_bound();
		forget_bound();
		foreach(lc, after_alter_strip)
		{
			strip_engine_options(lfirst_oid(lc));
			/*
			 * make_new_heap() gives the new heap a TOAST table only where the old
			 * relation had one (repack.c), and ours have none.
			 */
			CommandCounterIncrement();
			AlterTableCreateToastTable(lfirst_oid(lc), (Datum) 0, AccessExclusiveLock);
		}
	}
}

/*
 * OAT_POST_CREATE of a relation the running CREATE makes with an engine's AM
 * binds it to the CREATE's options.  The relation's pg_class row is not yet
 * visible to a catalog lookup (the command has not ended), but its relcache
 * entry exists.
 */
static void
ch_object_access(ObjectAccessType access, Oid classId, Oid objectId, int subId, void *arg)
{
	if (prev_object_access)
		prev_object_access(access, classId, objectId, subId, arg);

	if (access == OAT_POST_CREATE && classId == RelationRelationId && subId == 0 && pending_create &&
		pending_defs != NIL && !is_bound(objectId))
	{
		Relation	rel = RelationIdGetRelation(objectId);
		ChEngineKind kind;

		if (RelationIsValid(rel))
		{
			if ((rel->rd_rel->relkind == RELKIND_RELATION || rel->rd_rel->relkind == RELKIND_MATVIEW) &&
				ch_is_external_rel(rel, NULL))
			{
				ch_options_bind(objectId, pending_defs);
				/*
				 * §2.17: its columns' types the engine library takes, and the
				 * files its location names now, each one's columns checked; a
				 * table of its own files (§2.18), its columns alone
				 */
				PG_TRY();
				{
					if (ch_is_own_rel(rel))
						(void) ch_table(rel);
					else
						ch_external_check(rel);
				}
				PG_CATCH();
				{
					RelationClose(rel);
					PG_RE_THROW();
				}
				PG_END_TRY();
			}
			else if ((rel->rd_rel->relkind == RELKIND_RELATION || rel->rd_rel->relkind == RELKIND_MATVIEW) &&
					 ch_is_engine_rel(rel, &kind))
			{
				ChOptions	opts;

				ch_options_bind(objectId, pending_defs);
				/*
				 * Column names in the options, an engine's required ones, what
				 * ClickHouse itself checks as it makes the table: refused now,
				 * not at the first insert.
				 */
				ch_relation_options(rel, &opts);
				PG_TRY();
				{
					(void) ch_table_definition(rel, &opts, kind);
					(void) ch_table(rel);
				}
				PG_CATCH();
				{
					RelationClose(rel);
					PG_RE_THROW();
				}
				PG_END_TRY();
			}
			RelationClose(rel);
		}
	}
	else if (access == OAT_DROP && classId == RelationRelationId && subId == 0)
		ch_relation_dropped(objectId);
}
