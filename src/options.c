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
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
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
				 strcmp(name, "sum_columns") == 0)
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
		/* made by the running command, before its options were bound */
		set_defaults(opts);
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

	heaps = list_copy(defs);
	ours = take_engine_options(&heaps);
	foreach(lc, ours)
		if (option_is_defining(lfirst_node(DefElem, lc)->defname))
			defining = true;

	/* as tablecmds.c would take for heap's, and all of it for a defining change */
	lockmode = defining ? AccessExclusiveLock : Max(AlterTableGetRelOptionsLockLevel(heaps), ShareUpdateExclusiveLock);
	rel = table_open(relid, lockmode);
	if (!ch_is_engine_rel(rel, &kind))
		elog(ERROR, "relation \"%s\" is not of an engine's access method", RelationGetRelationName(rel));

	if (cmd->subtype == AT_SetRelOptions)
		ch_options_validate(ours, kind);
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

	if (IsA(parsetree, CreateStmt))
	{
		CreateStmt *stmt = (CreateStmt *) parsetree;

		if (stmt->partspec == NULL &&
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

		if (stmt->into && ch_am_name_kind(create_access_method(stmt->into->accessMethod), &kind))
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
			AlterTableStmt *copy = NULL;
			List	   *rest = NIL;
			ListCell   *lc;

			if (RelationIsValid(rel))
				RelationClose(rel);
			foreach(lc, stmt->cmds)
			{
				AlterTableCmd *cmd = lfirst_node(AlterTableCmd, lc);

				if (ours && (cmd->subtype == AT_SetRelOptions || cmd->subtype == AT_ResetRelOptions))
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
					bool		to_ours = ch_am_name_kind(create_access_method(cmd->name), &newkind);

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
