/*-------------------------------------------------------------------------
 *
 * external.c
 *	  Tables of files PostgreSQL does not own (ch_table_engines.md §2.17,
 *	  CH7): the read-only access methods parquet, arrow and iceberg, whose
 *	  rows the engine library's readers decode from the files their
 *	  location names.
 *
 *	  A table's columns are the user's, each one a column of the files of
 *	  the same name and of a type §2.5 keeps exactly: the engine library
 *	  converts the files' values to the type ClickHouse keeps it as -- a
 *	  date's days and a timestamp's microseconds moved to PostgreSQL's
 *	  epoch -- so that scan.c, ChScan and vexec's source read an external
 *	  table's batches as an engine's.  A column NOT NULL refuses a NULL as
 *	  it is read; a text, varchar or json column a value that is not the
 *	  database's encoding.
 *
 *	  The rows are not under PostgreSQL's MVCC: a statement reads the files
 *	  as they are listed for it -- once a statement, kept with the table and
 *	  pinned by the scans that read them -- each file a "part" of scan.c's
 *	  whose units are its row groups, its rows' ids the rows of the files
 *	  before it and the row's place in it, and so its TIDs.  A parallel
 *	  scan hands its listing to its workers in its descriptor, so that every
 *	  participant reads the same files.  The listings go at the
 *	  transaction's end, which no scan outlives.
 *
 *	  pg_dump (the user's rule): the table form of COPY ... TO of an external
 *	  table runs as its query form under WHERE false, the hook's
 *	  (options.c), so that a dump holds none of its rows and its restore
 *	  reads its location again; the query form reads them.  TRUNCATE leaves
 *	  the files as they are, and says so.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/detoast.h"
#include "access/relation.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "executor/tuptable.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "storage/proc.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/tuplestore.h"
#include "varatt.h"

#include "chte.h"

/* Every listing of the transaction, freed at its end (ch_external_xact_end()) */
static List *listings = NIL;

static const int32 external_formats[CH_EXTERNAL_COUNT] = {
	CHTE_EXTERNAL_PARQUET,
	CHTE_EXTERNAL_ARROW,
	CHTE_EXTERNAL_ICEBERG,
};

static const ChEngine *
external_engine(void)
{
	const ChEngine *eng = ch_engine();

	if (eng->external_open == NULL || eng->external_list == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("the engine library reads no external files"),
				 errdetail("It was built before CH7 (ch_table_engines.md §2.17)."),
				 errhint("Install an engine library built with Parquet.")));
	return eng;
}

/*
 * A column's description for the engine library: the ClickHouse type §2.5
 * keeps its values as, and how the files' are converted to it.  The types
 * whose values a file can hold exactly: bool, the integers and floats,
 * numeric(p, s), date, timestamp and timestamptz, text, varchar without a
 * length, json and bytea -- a file's strings, their bytes -- and uuid.
 */
static void
external_column(Relation rel, Form_pg_attribute att, chte_external_column *out)
{
	ChColumn	col;
	const char *name = NameStr(att->attname);
	bool		text_like = false;

	memset(out, 0, sizeof(*out));
	ch_column_for_attribute(att, &col);
	out->name = psprintf("c%d", att->attnum);
	out->file_name = pstrdup(name);
	out->type = pstrdup(col.chtype);
	out->not_null = att->attnotnull ? 1 : 0;
	out->convert = CHTE_CONVERT_NONE;

	switch (att->atttypid)
	{
		case BOOLOID:
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case FLOAT4OID:
		case FLOAT8OID:
		case BYTEAOID:
		case UUIDOID:
			return;
		case NUMERICOID:
			if (col.kind == CH_COL_NUMERIC)
				return;
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("column \"%s\" of \"%s\" must be numeric(p, s) with p at most 38", name,
							RelationGetRelationName(rel)),
					 errdetail("A file's decimals are read as numeric(p, s) of the precision and scale that hold them.")));
			break;
		case DATEOID:
			out->convert = CHTE_CONVERT_DATE;
			return;
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			out->convert = CHTE_CONVERT_TIMESTAMP;
			return;
		case VARCHAROID:
			if (att->atttypmod >= 0)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("column \"%s\" of \"%s\" cannot be of type %s", name, RelationGetRelationName(rel),
								format_type_with_typemod(att->atttypid, att->atttypmod)),
						 errdetail("A file's strings are of any length."),
						 errhint("Use text or varchar without a length.")));
			text_like = true;
			break;
		case TEXTOID:
		case JSONOID:
			text_like = true;
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("column \"%s\" of \"%s\" cannot be of type %s", name, RelationGetRelationName(rel),
							format_type_with_typemod(att->atttypid, att->atttypmod)),
					 errdetail("A table of files reads bool, smallint, integer, bigint, real, double precision, "
							   "numeric(p, s), date, timestamp, timestamptz, text, varchar, json, bytea and uuid.")));
	}
	if (text_like)
	{
		/* a file's strings are UTF-8, as Parquet's and Arrow's are; SQL_ASCII checks nothing */
		if (GetDatabaseEncoding() == PG_UTF8)
			out->check_utf8 = 1;
		else if (GetDatabaseEncoding() != PG_SQL_ASCII)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("column \"%s\" of \"%s\" cannot be of type %s in a database of encoding %s", name,
							RelationGetRelationName(rel), format_type_be(att->atttypid), GetDatabaseEncodingName()),
					 errdetail("A file's strings are UTF-8."),
					 errhint("Use bytea, or a database of encoding UTF8.")));
	}
}

/*
 * A column of a table of its own files (§2.18, CH8): its files' are
 * PostgreSQL's, written as the library keeps the column -- §2.5's type of it,
 * or its bytes -- a date and a timestamp as a file's Date32 and DateTime64,
 * named c<attnum> there as in the library
 */
static void
own_column(Form_pg_attribute att, chte_external_column *out)
{
	ChColumn	col;

	memset(out, 0, sizeof(*out));
	ch_column_for_attribute(att, &col);
	out->name = psprintf("c%d", att->attnum);
	out->file_name = out->name;
	out->type = pstrdup(col.chtype);
	out->not_null = att->attnotnull ? 1 : 0;
	out->convert = att->atttypid == DATEOID ? CHTE_CONVERT_DATE :
		att->atttypid == TIMESTAMPOID || att->atttypid == TIMESTAMPTZOID ? CHTE_CONVERT_TIMESTAMP :
		CHTE_CONVERT_NONE;
}

/* The columns of a descriptor of an external table, as the engine library takes them */
void
ch_external_columns_for(Relation rel, TupleDesc desc, MemoryContext cxt, chte_external_column **columns,
						int *ncolumns)
{
	MemoryContext old = MemoryContextSwitchTo(cxt);
	int			n = 0;
	bool		own = ch_is_own_rel(rel);

	*columns = palloc0(sizeof(chte_external_column) * Max(desc->natts, 1));
	for (int i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);

		if (att->attisdropped)
			continue;
		if (own)
		{
			own_column(att, &(*columns)[n++]);
			continue;
		}
		if (get_typtype(att->atttypid) == TYPTYPE_DOMAIN)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("column \"%s\" of \"%s\" cannot be of a domain", NameStr(att->attname),
							RelationGetRelationName(rel)),
					 errdetail("A domain's constraints are checked as rows are written, and a table of files is never written.")));
		external_column(rel, att, &(*columns)[n++]);
	}
	*ncolumns = n;
	MemoryContextSwitchTo(old);
}

/* ---- listings ---- */

/* A listing of the engine library's, as scan.c's parts: a file each */
static ChListing *
listing_make(chte_external_listing *handle)
{
	const ChEngine *eng = ch_engine();
	MemoryContext cxt = AllocSetContextCreate(TopMemoryContext, "ch listing", ALLOCSET_SMALL_SIZES);
	ChListing  *l;

	PG_TRY();
	{
		size_t		nfiles = eng->external_files(handle);
		MemoryContext old = MemoryContextSwitchTo(cxt);

		l = palloc0(sizeof(ChListing));
		l->cxt = cxt;
		l->handle = handle;
		for (size_t i = 0; i < nfiles; i++)
		{
			chte_external_file info;
			ChPart	   *p = palloc0(sizeof(ChPart));

			CH_CALL("listing a table's files", eng->external_file_info(handle, i, &info, CH_ERR));
			p->name = pstrdup(info.path);
			p->rows = (int64) info.rows;
			p->bytes = (int64) info.size;
			p->min_rowid = (int64) info.first_row;
			p->max_rowid = (int64) (info.first_row + info.rows) - 1;
			p->marks = (int32) info.units;
			p->listing = l;
			p->file = (int32) i;
			p->deletes = info.deletes != 0;
			l->parts = lappend(l->parts, p);
		}
		MemoryContextSwitchTo(TopMemoryContext);
		listings = lappend(listings, l);
		MemoryContextSwitchTo(old);
	}
	PG_CATCH();
	{
		eng->external_listing_free(handle);
		MemoryContextDelete(cxt);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return l;
}

static void
listing_free(ChListing *l)
{
	listings = list_delete_ptr(listings, l);
	if (ch_engine_loaded())
		ch_engine()->external_listing_free(l->handle);
	MemoryContextDelete(l->cxt);
}

void
ch_listing_pin(ChListing *l)
{
	l->refcount++;
}

void
ch_listing_unpin(ChListing *l)
{
	Assert(l->refcount > 0);
	if (--l->refcount == 0)
		listing_free(l);
}

/*
 * The files the running statement reads (§2.17): listed as it first asks,
 * then kept with the table for the rest of the statement -- its planning
 * and its scans -- so that it reads the files as they were at its start
 */
ChListing *
ch_external_listing(Relation rel, ChTable *t)
{
	const ChEngine *eng = external_engine();
	TimestampTz stmt = GetCurrentStatementStartTimestamp();
	LocalTransactionId lxid = MyProc->vxid.lxid;
	chte_external_listing *handle;
	ChListing  *l;

	if (t->listing && t->listing->stmt == stmt && t->listing->lxid == lxid)
		return t->listing;
	CH_CALL("listing a table's files", eng->external_list(t->handle, &handle, CH_ERR));
	l = listing_make(handle);
	l->stmt = stmt;
	l->lxid = lxid;
	if (t->listing)
		ch_listing_unpin(t->listing);
	t->listing = l;
	ch_listing_pin(l);
	return l;
}

/* A parallel scan's listing, from the bytes its leader put in its descriptor */
ChListing *
ch_external_listing_load(ChTable *t, const void *bytes, Size size)
{
	const ChEngine *eng = external_engine();
	chte_external_listing *handle;

	CH_CALL("listing a table's files", eng->external_listing_load(t->handle, bytes, size, &handle, CH_ERR));
	return listing_make(handle);
}

/* A part's file of a table of its own files (§2.18): <storage>/<part>/data.<format>, an absolute path */
char *
ch_own_part_file(ChTable *t, const char *part)
{
	char	   *dir = ch_storage_path(&t->storage, false);
	char	   *path = psprintf("%s%s/%s", dir, part, t->own_file);

	pfree(dir);
	return path;
}

/*
 * The own parts' summaries (VC25) a backend's listings handed the engine
 * library, kept as read from TOAST: each listing -- twice a plan, the
 * planner's estimate and vexec's -- read every part's summary from TOAST
 * and decompressed it again, two thirds of a warm backend's planning of
 * ClickBench's count(*) over its Parquet table, 0.02 -> 0.07 ms.  Keyed by
 * the TOAST value, which a part's row keeps for the part's life, its size
 * with it; emptied before a listing once it holds OWN_SUMMARIES_MAX bytes,
 * so that the summaries of a listing stay while the library reads them.
 */
#define OWN_SUMMARIES_MAX	(16 * 1024 * 1024)

typedef struct OwnSummaryKey
{
	Oid			toastrelid;
	Oid			valueid;
	int32		rawsize;
} OwnSummaryKey;

typedef struct OwnSummary
{
	OwnSummaryKey key;
	bytea	   *summary;
} OwnSummary;

static MemoryContext own_summaries_cxt = NULL;
static HTAB *own_summaries = NULL;
static Size own_summaries_bytes = 0;

/* A part's summary, from the backend's own summaries where its row's is in TOAST, else ch_part_summary()'s */
static bytea *
own_summary(ChPart *part)
{
	struct varatt_external toast;
	OwnSummaryKey key;
	OwnSummary *entry;

	if (part->summary == NULL || !VARATT_IS_EXTERNAL_ONDISK(part->summary))
		return ch_part_summary(part);
	VARATT_EXTERNAL_GET_POINTER(toast, part->summary);
	memset(&key, 0, sizeof(key));
	key.toastrelid = toast.va_toastrelid;
	key.valueid = toast.va_valueid;
	key.rawsize = toast.va_rawsize;
	if (own_summaries == NULL)
	{
		HASHCTL		ctl;

		if (own_summaries_cxt == NULL)
			own_summaries_cxt = AllocSetContextCreate(TopMemoryContext, "ch_table_engines own summaries",
													  ALLOCSET_DEFAULT_SIZES);
		ctl.keysize = sizeof(OwnSummaryKey);
		ctl.entrysize = sizeof(OwnSummary);
		ctl.hcxt = own_summaries_cxt;
		own_summaries = hash_create("ch_table_engines own summaries", 64, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	entry = hash_search(own_summaries, &key, HASH_FIND, NULL);
	if (entry == NULL)
	{
		MemoryContext old = MemoryContextSwitchTo(own_summaries_cxt);
		bytea	   *plain = (bytea *) detoast_attr((struct varlena *) part->summary);

		MemoryContextSwitchTo(old);
		entry = hash_search(own_summaries, &key, HASH_ENTER, NULL);
		entry->summary = plain;
		own_summaries_bytes += VARSIZE_ANY(plain);
	}
	return entry->summary;
}

/*
 * A listing of a table of its own files (§2.18): the files of the parts
 * given -- a snapshot's, of the catalog -- each part's attached to it
 * (part->listing, part->file) for the readers of external tables to read.
 * Its units are the part's marks, as the part was written.  A part of a
 * summary (VC25) is listed from it, its file opened by none but the readers
 * of its units.
 */
ChListing *
ch_own_listing(ChTable *t, List *parts)
{
	const ChEngine *eng = ch_engine();
	chte_external_listing *handle;
	const char **paths;
	const void **summaries = NULL;
	size_t	   *sizes = NULL;
	ChListing  *l;
	ListCell   *lc;
	int			i = 0;

	Assert(t->own);
	paths = palloc(sizeof(char *) * Max(list_length(parts), 1));
	if (eng->own_list_summaries != NULL && ch_enable_part_summaries)
	{
		summaries = palloc0(sizeof(void *) * Max(list_length(parts), 1));
		sizes = palloc0(sizeof(size_t) * Max(list_length(parts), 1));
		if (own_summaries_bytes >= OWN_SUMMARIES_MAX)
		{
			MemoryContextReset(own_summaries_cxt);
			own_summaries = NULL;
			own_summaries_bytes = 0;
		}
	}
	foreach(lc, parts)
	{
		ChPart	   *p = lfirst(lc);
		bytea	   *summary = summaries != NULL ? own_summary(p) : NULL;

		if (summary != NULL)
		{
			summaries[i] = VARDATA_ANY(summary);
			sizes[i] = VARSIZE_ANY_EXHDR(summary);
		}
		paths[i++] = ch_own_part_file(t, p->name);
	}
	if (summaries != NULL)
		CH_CALL("listing a table's files",
				eng->own_list_summaries(t->handle, paths, summaries, sizes, (size_t) i, &handle, CH_ERR));
	else
		CH_CALL("listing a table's files", eng->own_list(t->handle, paths, (size_t) i, &handle, CH_ERR));
	l = listing_make(handle);
	i = 0;
	foreach(lc, parts)
	{
		ChPart	   *p = lfirst(lc);
		ChPart	   *file = list_nth(l->parts, i);

		if (file->marks != p->marks || file->rows != p->rows)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("part \"%s\" of \"%s\" holds %lld rows in %d units, where its catalog row has %lld in %d",
							p->name, get_rel_name(t->relid), (long long) file->rows, file->marks,
							(long long) p->rows, p->marks)));
		p->listing = l;
		p->file = i++;
	}
	pfree(paths);
	if (summaries != NULL)
	{
		pfree(summaries);
		pfree(sizes);
	}
	return l;
}

/* At the transaction's end every listing goes: no scan outlives it (table.c clears the tables' first) */
void
ch_external_xact_end(void)
{
	while (listings != NIL)
		listing_free(linitial(listings));
}

/*
 * A table's columns against the files its location names now: each file
 * listed and its columns checked by the engine library, at CREATE TABLE and
 * after an ALTER TABLE (options.c)
 */
void
ch_external_check(Relation rel)
{
	const ChEngine *eng = external_engine();
	ChTable    *t = ch_table(rel);
	chte_external_listing *handle;
	chte_error	err;

	ch_table_pin(t);
	PG_TRY();
	{
		if (eng->external_list(t->handle, &handle, &err) != 0)
			ch_engine_raise(&err, psprintf("checking the files of \"%s\"", RelationGetRelationName(rel)));
		eng->external_listing_free(handle);
	}
	PG_FINALLY();
	{
		ch_table_unpin(t);
	}
	PG_END_TRY();
}

/* The rows and bytes of the files the running statement reads */
void
ch_external_totals(Relation rel, double *rows, uint64 *bytes)
{
	ChTable    *t = ch_table(rel);
	ListCell   *lc;

	*rows = 0;
	*bytes = 0;
	ch_table_pin(t);
	PG_TRY();
	{
		ChListing  *l = ch_external_listing(rel, t);

		foreach(lc, l->parts)
		{
			ChPart	   *p = lfirst(lc);

			*rows += p->rows;
			*bytes += (uint64) p->bytes;
		}
	}
	PG_FINALLY();
	{
		ch_table_unpin(t);
	}
	PG_END_TRY();
}

/* ---- a unit of a part, or of a file ---- */

/* CHTE_SPEC_CHECK in the environment, the tests': set, neither empty nor 0 */
bool
ch_spec_check(void)
{
	static int	check = -1;

	if (check < 0)
	{
		const char *v = getenv("CHTE_SPEC_CHECK");

		check = v != NULL && *v != '\0' && strcmp(v, "0") != 0;
	}
	return check;
}

/*
 * The units of a part or a file a filter keeps, as chte_part_select() selects
 * them; true where any.  VC31: a part's, under a filter of a key, from those
 * kept in shared memory (selections.c), the part neither attached nor
 * loaded; else the library's, kept there.
 */
bool
ch_unit_select(ChTable *t, ChPart *p, chte_filter *filter, const ChFilterKey *key, uint32 begin, uint32 end,
			   chte_mark_range *ranges, size_t cap, size_t *nranges, uint64 *rows)
{
	const ChEngine *eng = ch_engine();
	chte_mark_range kept[CH_SELECTION_RANGES];
	chte_mark_range made[CH_SELECTION_RANGES];
	chte_mark_range *into = ranges;
	size_t		room = cap;
	size_t		found_n = 0;
	uint64		found_rows = 0;
	bool		found;

	if (p->listing)
	{
		CH_CALL("analyzing a file",
				eng->external_select(t->handle, p->listing->handle, (size_t) p->file, filter, begin, end, ranges,
									 cap, nranges, rows, CH_ERR));
		return *nranges > 0;
	}
	found = filter != NULL &&
		ch_selections_find(t, p, key, begin, end, kept, CH_SELECTION_RANGES, &found_n, &found_rows);
	if (found && !ch_spec_check())
	{
		*nranges = found_n;
		*rows = found_rows;
		if (ranges != NULL)
			memcpy(ranges, kept, sizeof(chte_mark_range) * Min(found_n, cap));
		return found_n > 0;
	}
	/* the library's answer, into ranges an entry keeps it from where the caller's are fewer */
	if (filter != NULL && (cap < CH_SELECTION_RANGES || ranges == NULL))
	{
		into = made;
		room = CH_SELECTION_RANGES;
	}
	if (t->options.storage == CH_STORAGE_PAGES)
		ch_pages_attach(t, p);
	CH_CALL("analyzing a part",
			eng->part_select(t->handle, p->name, filter, begin, end, into, room, nranges, rows, CH_ERR));
	if (found)
		/* CHTE_SPEC_CHECK: the answer found, as the library's */
		ch_selections_check(p, begin, end, kept, found_n, found_rows, into, *nranges, *rows);
	else if (filter != NULL && *nranges <= room)
		ch_selections_keep(t, p, key, begin, end, into, *nranges, *rows);
	if (into != ranges && ranges != NULL)
		memcpy(ranges, into, sizeof(chte_mark_range) * Min(*nranges, cap));
	return *nranges > 0;
}

void
ch_unit_column_bytes(ChTable *t, ChPart *p, const char *columns, uint64 *compressed, uint64 *uncompressed)
{
	const ChEngine *eng = ch_engine();

	if (p->listing)
		CH_CALL("sizing a file's columns",
				eng->external_column_bytes(t->handle, p->listing->handle, (size_t) p->file, columns, compressed,
										   uncompressed, CH_ERR));
	else
		CH_CALL("sizing a part's columns",
				eng->part_column_bytes(t->handle, p->name, columns, compressed, uncompressed, CH_ERR));
}

void
ch_unit_stats(ChTable *t, const ChPart *p, const char *column, chte_column_stats *stats)
{
	const ChEngine *eng = ch_engine();

	if (p->listing)
		CH_CALL("reading a file's statistics",
				eng->external_stats(t->handle, p->listing->handle, (size_t) p->file, column, stats, CH_ERR));
	else
		CH_CALL("reading a part's statistics", eng->part_stats(t->handle, p->name, column, stats, CH_ERR));
}

/* ---- what the access methods and the hook ask ---- */

/*
 * Whether a relation is the transient table of a rewrite of an external
 * table -- ALTER TABLE's (make_new_heap(): "pg_temp_<oid>" beside the
 * table, made in this transaction) -- whose rows are the files', so that it
 * copies none of them; ch_rewrite_source(), the table it rewrites, of the
 * same access method, or InvalidOid
 */
Oid
ch_rewrite_source(Relation rel)
{
	const char *name = RelationGetRelationName(rel);
	unsigned int old;
	char		tail;

	if (rel->rd_createSubid == InvalidSubTransactionId ||
		sscanf(name, "pg_temp_%u%c", &old, &tail) != 1)
		return InvalidOid;
	if (get_rel_relam((Oid) old) != rel->rd_rel->relam || get_rel_namespace((Oid) old) != RelationGetNamespace(rel))
		return InvalidOid;
	return (Oid) old;
}

bool
ch_external_rewrite_target(Relation rel)
{
	Oid			source = ch_rewrite_source(rel);

	/* a table of its own files' rows are its parts', which its rewrite copies (§2.18) */
	return OidIsValid(source) && !ch_is_own_relid(source);
}

void
ch_external_truncated(Relation rel)
{
	ereport(NOTICE,
			(errmsg("TRUNCATE leaves the files of \"%s\" as they are", RelationGetRelationName(rel)),
			 errdetail("The table reads them still: its rows are the files' at its location.")));
}

/*
 * The table form of COPY ... TO of an external table (§2.17): its query form
 * over the same columns -- those it lists, or those COPY would name, neither
 * dropped nor generated (copy.c, CopyGetAttnums()) -- under WHERE false, the
 * statement's options kept.  NULL for any other relation.
 */
Node *
ch_external_copy_to(CopyStmt *stmt, Oid relid)
{
	ChExternalKind kind;
	CopyStmt   *copy;
	SelectStmt *select;
	RangeVar   *from;
	A_Const    *no;
	Relation	rel;
	char	   *relname;

	/* a table of its own files (§2.18) is dumped with its rows, as any table */
	if (!ch_is_external_am(get_rel_relam(relid), &kind) || ch_is_own_relid(relid))
		return NULL;
	select = makeNode(SelectStmt);
	if (stmt->attlist != NIL)
	{
		ListCell   *lc;

		foreach(lc, stmt->attlist)
		{
			ResTarget  *target = makeNode(ResTarget);
			ColumnRef  *ref = makeNode(ColumnRef);

			ref->fields = list_make1(makeString(pstrdup(strVal(lfirst(lc)))));
			ref->location = -1;
			target->val = (Node *) ref;
			target->location = -1;
			select->targetList = lappend(select->targetList, target);
		}
	}
	else
	{
		TupleDesc	desc;

		rel = relation_open(relid, AccessShareLock);
		desc = RelationGetDescr(rel);
		for (int i = 0; i < desc->natts; i++)
		{
			Form_pg_attribute att = TupleDescAttr(desc, i);
			ResTarget  *target;
			ColumnRef  *ref;

			if (att->attisdropped || att->attgenerated)
				continue;
			target = makeNode(ResTarget);
			ref = makeNode(ColumnRef);
			ref->fields = list_make1(makeString(pstrdup(NameStr(att->attname))));
			ref->location = -1;
			target->val = (Node *) ref;
			target->location = -1;
			select->targetList = lappend(select->targetList, target);
		}
		relation_close(rel, NoLock);
	}
	from = copyObject(stmt->relation);
	from->inh = false;
	select->fromClause = list_make1(from);
	no = makeNode(A_Const);
	no->val.boolval.type = T_Boolean;
	no->val.boolval.boolval = false;
	no->location = -1;
	select->whereClause = (Node *) no;

	copy = copyObject(stmt);
	copy->relation = NULL;
	copy->attlist = NIL;
	copy->query = (Node *) select;

	relname = stmt->relation->schemaname ?
		psprintf("%s.%s", quote_identifier(stmt->relation->schemaname), quote_identifier(stmt->relation->relname)) :
		pstrdup(quote_identifier(stmt->relation->relname));
	ereport(NOTICE,
			(errmsg("COPY of %s returns no rows: its rows are those of the files at its location", relname),
			 errhint("COPY (SELECT * FROM %s) TO ... reads them.", relname)));
	return (Node *) copy;
}

/* ---- SQL functions ---- */

static int
external_kind_of_am(const char *amname)
{
	ChExternalKind kind;

	if (!ch_external_am_name_kind(amname, &kind))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"%s\" is not an access method of files", amname),
				 errhint("The access methods of files are parquet, arrow and iceberg.")));
	return (int) kind;
}

/* PostgreSQL's type for a column of ClickHouse's type, as a table of files reads it; NULL where none */
static const char *
suggested_type(const char *chtype, bool *not_null)
{
	int			p;
	int			s;
	char		tail;

	*not_null = true;
	if (strncmp(chtype, "Nullable(", 9) == 0)
	{
		size_t		len = strlen(chtype);

		*not_null = false;
		return suggested_type(pnstrdup(chtype + 9, len - 10), &(bool) {false});
	}
	if (strncmp(chtype, "LowCardinality(", 15) == 0)
	{
		size_t		len = strlen(chtype);
		bool		inner;
		const char *t = suggested_type(pnstrdup(chtype + 15, len - 16), &inner);

		*not_null = inner;
		return t;
	}
	if (strcmp(chtype, "Bool") == 0)
		return "boolean";
	if (strcmp(chtype, "Int8") == 0 || strcmp(chtype, "UInt8") == 0 || strcmp(chtype, "Int16") == 0)
		return "smallint";
	if (strcmp(chtype, "UInt16") == 0 || strcmp(chtype, "Int32") == 0)
		return "integer";
	if (strcmp(chtype, "UInt32") == 0 || strcmp(chtype, "Int64") == 0)
		return "bigint";
	if (strcmp(chtype, "UInt64") == 0)
		return "numeric(20, 0)";
	if (strcmp(chtype, "Float32") == 0)
		return "real";
	if (strcmp(chtype, "Float64") == 0)
		return "double precision";
	if (sscanf(chtype, "Decimal(%d, %d)%c", &p, &s, &tail) == 2 && p <= 38)
		return psprintf("numeric(%d, %d)", p, s);
	if (strcmp(chtype, "String") == 0)
		return "text";
	if (strncmp(chtype, "FixedString(", 12) == 0)
		return "bytea";
	if (strcmp(chtype, "Date") == 0 || strcmp(chtype, "Date32") == 0)
		return "date";
	/* a timestamp of a time zone -- Parquet's adjusted to UTC -- or one of none, as Iceberg's timestamp */
	if (strncmp(chtype, "DateTime", 8) == 0)
		return strchr(chtype, '\'') ? "timestamp with time zone" : "timestamp without time zone";
	return NULL;
}

/*
 * ch.external_columns(am, location, settings): the columns of the first file
 * a location names, as a column list to start a CREATE TABLE from; a column
 * of a type no table of files reads, a comment.  Its caller may read the
 * server's files.
 */
PG_FUNCTION_INFO_V1(ch_external_columns);
Datum
ch_external_columns(PG_FUNCTION_ARGS)
{
	const ChEngine *eng;
	int			kind = external_kind_of_am(text_to_cstring(PG_GETARG_TEXT_PP(0)));
	char	   *location = text_to_cstring(PG_GETARG_TEXT_PP(1));
	chte_external_def def;
	chte_table *table;
	const chte_external_field *fields;
	size_t		nfields;
	chte_error	err;
	StringInfoData out;

	if (!has_privs_of_role(GetUserId(), ROLE_PG_READ_SERVER_FILES))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to read the server's files"),
				 errdetail("Only roles with privileges of the \"%s\" role may read the server's files.",
						   "pg_read_server_files")));
	eng = external_engine();
	memset(&def, 0, sizeof(def));
	def.format = external_formats[kind];
	def.name = location;
	def.location = location;
	def.settings = PG_ARGISNULL(2) ? NULL : text_to_cstring(PG_GETARG_TEXT_PP(2));
	CH_CALL("opening the files", eng->external_open(&def, &table, CH_ERR));
	if (eng->external_schema(table, &fields, &nfields, &err) != 0)
	{
		eng->table_close(table);
		ch_engine_raise(&err, "reading the files' schema");
	}
	initStringInfo(&out);
	for (size_t i = 0; i < nfields; i++)
	{
		bool		not_null;
		const char *type = suggested_type(fields[i].type, &not_null);

		if (out.len > 0)
			appendStringInfoString(&out, ",\n");
		if (type)
			appendStringInfo(&out, "%s %s%s", quote_identifier(fields[i].name), type, not_null ? " NOT NULL" : "");
		else
			appendStringInfo(&out, "-- %s %s: not read by a table of files", quote_identifier(fields[i].name),
							 fields[i].type);
	}
	eng->table_close(table);
	PG_RETURN_TEXT_P(cstring_to_text(out.data));
}

/* ch.external_files(rel): the files the statement reads, as it lists them */
PG_FUNCTION_INFO_V1(ch_external_files);
Datum
ch_external_files(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Relation	rel;
	ChTable    *t;
	ListCell   *lc;

	InitMaterializedSRF(fcinfo, 0);
	rel = relation_open(PG_GETARG_OID(0), AccessShareLock);
	if (!ch_is_external_rel(rel, NULL))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a table of files", RelationGetRelationName(rel))));
	t = ch_table(rel);
	ch_table_pin(t);
	PG_TRY();
	{
		ChListing  *l = ch_external_listing(rel, t);

		foreach(lc, l->parts)
		{
			ChPart	   *p = lfirst(lc);
			Datum		values[5];
			bool		nulls[5] = {false};

			values[0] = CStringGetTextDatum(p->name);
			values[1] = Int64GetDatum(p->bytes);
			values[2] = Int64GetDatum(p->rows);
			values[3] = Int32GetDatum(p->marks);
			values[4] = Int64GetDatum(p->min_rowid);
			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		}
	}
	PG_FINALLY();
	{
		ch_table_unpin(t);
	}
	PG_END_TRY();
	relation_close(rel, AccessShareLock);
	return (Datum) 0;
}

/* A column list's columns, "name type, ...": split at the commas outside parentheses and quotes */
static List *
split_columns(const char *list)
{
	List	   *out = NIL;
	int			depth = 0;
	char		quote = 0;
	const char *start = list;

	for (const char *c = list;; c++)
	{
		if (*c == '\0' || (*c == ',' && depth == 0 && quote == 0))
		{
			char	   *item = pnstrdup(start, c - start);

			item = pstrdup(item + strspn(item, " \t\n"));
			if (item[0] != '\0')
				out = lappend(out, item);
			if (*c == '\0')
				break;
			start = c + 1;
		}
		else if (quote)
		{
			if (*c == quote)
				quote = 0;
		}
		else if (*c == '\'' || *c == '"' || *c == '`')
			quote = *c;
		else if (*c == '(')
			depth++;
		else if (*c == ')')
			depth--;
	}
	return out;
}

/* A file's type, by default, for a column of a query: §2.5's type of it, Nullable, its date or timestamp ClickHouse's */
static char *
default_file_type(const ChColumn *col, int convert)
{
	if (convert == CHTE_CONVERT_DATE)
		return "Nullable(Date32)";
	if (convert == CHTE_CONVERT_TIMESTAMP)
		return "Nullable(DateTime64(6, 'UTC'))";
	return psprintf("Nullable(%s)", col->chtype);
}

/* A given column, "name type": its name, unquoted as ClickHouse quotes one ("..." or `...`, a quote doubled), and its type */
static void
given_column(const char *item, int i, chte_file_column *column)
{
	const char *end;

	if (item[0] == '"' || item[0] == '`')
	{
		StringInfoData name;
		char		quote = item[0];

		initStringInfo(&name);
		for (end = item + 1; *end && !(*end == quote && end[1] != quote); end++)
		{
			if (*end == quote)
				end++;
			appendStringInfoChar(&name, *end);
		}
		if (*end != quote)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("column %d's name is not closed: %s", i + 1, item)));
		column->name = name.data;
		end++;
	}
	else
	{
		end = item + strcspn(item, " \t\n");
		column->name = pnstrdup(item, end - item);
	}
	column->type = end + strspn(end, " \t\n");
	if (column->type[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("column \"%s\" is given no type", column->name)));
}

/* Whether the caller may write the server's files, as COPY TO a file requires */
static void
check_write_privilege(void)
{
	if (!has_privs_of_role(GetUserId(), ROLE_PG_WRITE_SERVER_FILES))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to write the server's files"),
				 errdetail("Only roles with privileges of the \"%s\" role may write the server's files.",
						   "pg_write_server_files")));
}

/* Where ch.write_file's and ch.write_iceberg's rows go: a file of a format, or an Iceberg table by an action */
typedef struct WriteTarget
{
	const char *function;
	const char *format;			/* a file's */
	const char *path;			/* the file's, or the Iceberg table's location */
	int			action;			/* the Iceberg table's CHTE_ICEBERG_*; -1: a file */
	const char *spec;			/* a new Iceberg table's partition spec, or NULL */
} WriteTarget;

/*
 * The rows of a query written by the engine library's writer of the target, in
 * batches of the query's columns: each column's name and type, the given
 * ones' or -- by default -- the query's own and, for a file, a Nullable of
 * §2.5's type, for an Iceberg table its own.  Its caller may write the
 * server's files, as COPY TO a file requires.  Returns the bytes written.
 */
static int64
write_rows(const WriteTarget *target, const char *query, List *given, const char *settings)
{
	const ChEngine *eng;
	Portal		portal = NULL;
	SPIParseOpenOptions open_options;
	TupleDesc	desc = NULL;
	ChTable		table;
	chte_file_column *columns;
	chte_file_writer *writer;
	ResourceOwner owner;
	ChVectorBuilder *builder = NULL;
	MemoryContext cxt;
	TupleTableSlot *slot = NULL;
	uint64		bytes = 0;
	int			n = 0;
	int			ncolumns;

	check_write_privilege();
	eng = external_engine();
	if (target->action < 0 ? eng->file_write_begin == NULL : eng->iceberg_write_begin == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("the engine library writes no %s", target->action < 0 ? "files" : "Iceberg tables")));

	/* the builder's, the caller's: SPI_finish() deletes what SPI's procedure context holds */
	cxt = AllocSetContextCreate(CurrentMemoryContext, "ch write_rows", ALLOCSET_DEFAULT_SIZES);
	if (query != NULL)
	{
		if (SPI_connect() != SPI_OK_CONNECT)
			elog(ERROR, "SPI_connect failed");
		memset(&open_options, 0, sizeof(open_options));
		open_options.read_only = true;
		portal = SPI_cursor_parse_open(NULL, query, &open_options);
		desc = portal->tupDesc;
		if (desc == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("the query of %s returns no rows", target->function)));
		if (given != NIL && list_length(given) != desc->natts)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%d columns given for a query of %d", list_length(given), desc->natts)));
	}
	ncolumns = desc ? desc->natts : list_length(given);

	/* the query's columns as a table's, for the builder of the library's vectors */
	memset(&table, 0, sizeof(table));
	table.natts = desc ? desc->natts : 0;
	table.columns = MemoryContextAllocZero(cxt, sizeof(ChColumn) * Max(ncolumns, 1));
	columns = MemoryContextAllocZero(cxt, sizeof(chte_file_column) * Max(ncolumns, 1));
	for (int i = 0; i < ncolumns; i++)
	{
		Form_pg_attribute att;
		ChColumn   *col = &table.columns[i];
		int			convert = CHTE_CONVERT_NONE;

		if (desc == NULL)
		{
			/* columns alone: an Iceberg table made of them */
			given_column(list_nth(given, i), i, &columns[i]);
			n++;
			continue;
		}
		att = TupleDescAttr(desc, i);
		ch_column_for_attribute(att, col);
		switch (getBaseType(att->atttypid))
		{
			case BOOLOID:
			case INT2OID:
			case INT4OID:
			case INT8OID:
			case FLOAT4OID:
			case FLOAT8OID:
			case BYTEAOID:
			case UUIDOID:
			case TEXTOID:
			case VARCHAROID:
			case BPCHAROID:
			case JSONOID:
				break;
			case NUMERICOID:
				if (col->kind == CH_COL_NUMERIC)
					break;
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("column %d of the query is numeric without a precision of at most 38", i + 1),
						 errhint("Cast it to numeric(p, s).")));
				break;
			case DATEOID:
				convert = CHTE_CONVERT_DATE;
				break;
			case TIMESTAMPOID:
			case TIMESTAMPTZOID:
				convert = CHTE_CONVERT_TIMESTAMP;
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("column %d of the query is of type %s, which is not written to a file", i + 1,
								format_type_be(att->atttypid))));
		}
		columns[i].convert = convert;
		columns[i].source = col->chtype;
		if (given != NIL)
			given_column(list_nth(given, i), i, &columns[i]);
		else
		{
			columns[i].name = pstrdup(NameStr(att->attname));
			/* an Iceberg table's own type of the column, the engine library's to find */
			columns[i].type = target->action < 0 ? default_file_type(col, convert) : NULL;
		}
		n++;
	}

	owner = ch_handle_reserve();
	if (target->action < 0)
		CH_CALL("writing a file",
				eng->file_write_begin(target->format, target->path, columns, n, settings, &writer, CH_ERR));
	else
		CH_CALL("writing an Iceberg table",
				eng->iceberg_write_begin(target->path, target->action, target->spec, columns, n, settings, &writer, CH_ERR));
	ch_handle_opened(owner, CH_HANDLE_FILE_WRITER, writer);
	if (portal != NULL)
	{
		builder = ch_builder_create(&table, cxt);
		slot = MakeSingleTupleTableSlot(desc, &TTSOpsHeapTuple);
		for (;;)
		{
			SPI_cursor_fetch(portal, true, 8192);
			if (SPI_processed == 0)
				break;
			for (uint64 r = 0; r < SPI_processed; r++)
			{
				ExecStoreHeapTuple(SPI_tuptable->vals[r], slot, false);
				ch_builder_add(builder, slot, 0);
			}
			SPI_freetuptable(SPI_tuptable);
			CH_CALL("writing a file",
					eng->file_write_batch(writer, ch_builder_rows(builder), ch_builder_vectors(builder), CH_ERR));
			ch_builder_reset(builder);
			CHECK_FOR_INTERRUPTS();
		}
	}
	CH_CALL("writing a file", eng->file_write_finish(writer, &bytes, CH_ERR));
	ch_handle_closing(owner, CH_HANDLE_FILE_WRITER, writer);
	eng->file_write_close(writer);
	if (portal != NULL)
	{
		ExecDropSingleTupleTableSlot(slot);
		SPI_cursor_close(portal);
		SPI_finish();
	}
	MemoryContextDelete(cxt);
	return (int64) bytes;
}

/*
 * ch.write_file(format, path, query, columns, settings): the rows of a query
 * written to a file by ClickHouse's own output format -- "Parquet", "Arrow" --
 * as the tests make their tables' files (§2.17): columns, ClickHouse's "name
 * type, ..." of the file, by default the query's columns as Nullable of §2.5's
 * types.  The file is written whole under a temporary name, then renamed.
 * Returns the file's bytes.
 */
PG_FUNCTION_INFO_V1(ch_write_file);
Datum
ch_write_file(PG_FUNCTION_ARGS)
{
	WriteTarget target = {"ch.write_file", NULL, NULL, -1, NULL};

	target.format = text_to_cstring(PG_GETARG_TEXT_PP(0));
	target.path = text_to_cstring(PG_GETARG_TEXT_PP(1));
	PG_RETURN_INT64(write_rows(&target, text_to_cstring(PG_GETARG_TEXT_PP(2)),
							   PG_ARGISNULL(3) ? NIL : split_columns(text_to_cstring(PG_GETARG_TEXT_PP(3))),
							   PG_ARGISNULL(4) ? NULL : text_to_cstring(PG_GETARG_TEXT_PP(4))));
}

/*
 * ch.write_iceberg(location, action, query, columns, settings): an Iceberg
 * table on the server's disk written by ClickHouse's own Iceberg writers, as
 * the tests make theirs (§2.17), a snapshot an action:
 *   'create'            a table of columns ("name type, ...", ClickHouse's),
 *                       partitioned where the third argument is a partition
 *                       spec as ClickHouse's PARTITION BY names one
 *   'append'            the query's rows, a column each of the table's, by name
 *   'position deletes'  the rows at the positions the query names: its file_path
 *                       (a data file's path as the table's metadata names it) and pos
 *   'equality deletes'  the rows whose columns the query names hold its values
 *   'alter'             its schema changed by the third argument, a command of
 *                       ClickHouse's ALTER: ADD, DROP, MODIFY or RENAME COLUMN
 * settings, ClickHouse's: a table's data files' rows
 * (iceberg_insert_max_rows_in_data_file), their row groups'
 * (output_format_parquet_row_group_size).  Returns the bytes of the files added.
 */
PG_FUNCTION_INFO_V1(ch_write_iceberg);
Datum
ch_write_iceberg(PG_FUNCTION_ARGS)
{
	WriteTarget target = {"ch.write_iceberg", NULL, NULL, -1, NULL};
	char	   *action;
	char	   *query = PG_ARGISNULL(2) ? NULL : text_to_cstring(PG_GETARG_TEXT_PP(2));
	List	   *given = PG_ARGISNULL(3) ? NIL : split_columns(text_to_cstring(PG_GETARG_TEXT_PP(3)));

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("ch.write_iceberg needs a location and an action")));
	target.path = text_to_cstring(PG_GETARG_TEXT_PP(0));
	action = text_to_cstring(PG_GETARG_TEXT_PP(1));
	if (strcmp(action, "alter") == 0)
	{
		const ChEngine *eng;

		check_write_privilege();
		eng = external_engine();
		if (query == NULL || given != NIL)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("ch.write_iceberg's 'alter' takes a command and no columns")));
		if (eng->iceberg_alter == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("the engine library writes no Iceberg tables")));
		CH_CALL("changing an Iceberg table",
				eng->iceberg_alter(target.path, query, PG_ARGISNULL(4) ? NULL : text_to_cstring(PG_GETARG_TEXT_PP(4)), CH_ERR));
		PG_RETURN_INT64(0);
	}
	if (strcmp(action, "create") == 0)
		target.action = CHTE_ICEBERG_CREATE;
	else if (strcmp(action, "append") == 0)
		target.action = CHTE_ICEBERG_APPEND;
	else if (strcmp(action, "position deletes") == 0)
		target.action = CHTE_ICEBERG_DELETE_POSITIONS;
	else if (strcmp(action, "equality deletes") == 0)
		target.action = CHTE_ICEBERG_DELETE_EQUAL;
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("ch.write_iceberg has no action \"%s\"", action),
				 errhint("Its actions are 'create', 'append', 'position deletes', 'equality deletes' and 'alter'.")));
	if (target.action == CHTE_ICEBERG_CREATE)
	{
		/* a new table's columns, and its partition spec where given in the query's place */
		if (given == NIL)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("ch.write_iceberg's 'create' takes columns")));
		target.spec = query;
		query = NULL;
	}
	else if (query == NULL || given != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("ch.write_iceberg's actions but 'create' take a query and no columns")));
	PG_RETURN_INT64(write_rows(&target, query, given, PG_ARGISNULL(4) ? NULL : text_to_cstring(PG_GETARG_TEXT_PP(4))));
}
