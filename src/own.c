/*-------------------------------------------------------------------------
 *
 * own.c
 *	  Tables of their own files (ch_table_engines.md §2.18, CH8): the access
 *	  methods parquet and arrow made without a location keep their rows in
 *	  files PostgreSQL writes, a part's one file each -- data.parquet or
 *	  data.arrow in its directory of the files store (§2.9) -- written by
 *	  ClickHouse's own output formats at the layout §2.18 fixes: Parquet's row
 *	  groups of 131,072 rows, its page index, bloom filters and dictionaries,
 *	  the codec the table's option names, lz4 by default; Arrow's record
 *	  batches of 131,072 rows, uncompressed by default.  A part's rows are in
 *	  their ids' order, each id in its file's _pg_rowid, and its columns are
 *	  named as the library names them, c<attnum>.
 *
 *	  Everything else is an engine's (§2.6, §2.10): a part's row in ch.parts,
 *	  its rows visible as that row is; DELETE and UPDATE as tombstones; the
 *	  part's file logged as the files store logs a part's files, under its
 *	  name, before its directory gets it.  Scans read the parts through CH7's
 *	  readers, over a listing of the snapshot's parts (scan.c), the
 *	  tombstones applied; a fetch by TID finds a row by its id among its
 *	  file's (the engine library's chte_own_locate).
 *
 *	  Merges (§2.11's workers, ch.optimize()): parts below CH_OWN_MERGE_ROWS
 *	  rows, adjacent in row-id order, become one -- the engine library reads
 *	  their files through the same readers and writes one, the rows every
 *	  snapshot sees deleted left out, in their ids' order -- the merged part's
 *	  row replacing the inputs' in one transaction (§2.6, rule 4).  ch.fold()
 *	  writes a part with dead rows again without them.  ClickHouse's
 *	  MergeTree merges do not apply: a part is no MergeTree part.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/htup_details.h"
#include "access/relation.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "common/file_utils.h"
#include "executor/tuptable.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/tuplestore.h"

#include "chte.h"
#include "fold.h"

/* A part's units -- Parquet's row groups, Arrow's record batches -- as its writer cuts them (§2.18) */
#define CH_OWN_UNIT_ROWS 131072

/* The rows below which a part merges, and the most a merge writes (§2.18, the user's word: 8M rows) */
#define CH_OWN_MERGE_ROWS (8 * 1024 * 1024)

/* The most parts a merge takes, as §2.11's */
#define CH_OWN_MERGE_PARTS 100

/*
 * A table of its own files' parts, as the engine library writes them: its
 * format, its parts' file name, their columns -- the builder's vectors', each
 * c<attnum> of §2.5's type, a date Date32 and a timestamp DateTime64(6, 'UTC'),
 * Nullable unless the column is NOT NULL, then _pg_rowid -- and the writer's
 * settings: the table's own, then the layout §2.18 fixes.
 */
void
ch_own_table_setup(Relation rel, TupleDesc desc, ChTable *t)
{
	StringInfoData settings;
	const char *codec = t->options.codec;
	int			n = 0;

	t->own_format = t->ext_kind == CH_EXTERNAL_ARROW ? "Arrow" : "Parquet";
	t->own_file = t->ext_kind == CH_EXTERNAL_ARROW ? "data.arrow" : "data.parquet";
	t->file_columns = palloc0(sizeof(chte_file_column) * (desc->natts + 1));
	for (int i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);
		const ChColumn *col = &t->columns[i];
		chte_file_column *fc;
		const char *type = col->chtype;
		int			convert = CHTE_CONVERT_NONE;

		if (col->kind == CH_COL_DROPPED)
			continue;
		switch (att->atttypid)
		{
			case DATEOID:
				type = "Date32";
				convert = CHTE_CONVERT_DATE;
				break;
			case TIMESTAMPOID:
			case TIMESTAMPTZOID:
				type = "DateTime64(6, 'UTC')";
				convert = CHTE_CONVERT_TIMESTAMP;
				break;
			default:
				break;
		}
		fc = &t->file_columns[n++];
		fc->name = psprintf("c%d", att->attnum);
		fc->type = att->attnotnull ? pstrdup(type) : psprintf("Nullable(%s)", type);
		fc->source = pstrdup(col->chtype);
		fc->convert = convert;
	}
	t->file_columns[n].name = CH_ROWID_COLUMN;
	t->file_columns[n].type = "UInt64";
	t->file_columns[n].source = "UInt64";
	t->file_columns[n].convert = CHTE_CONVERT_NONE;
	t->nfile_columns = n + 1;

	initStringInfo(&settings);
	if (t->options.settings && t->options.settings[0])
		appendStringInfo(&settings, "%s, ", t->options.settings);
	if (t->ext_kind == CH_EXTERNAL_ARROW)
		appendStringInfo(&settings,
						 "output_format_arrow_compression_method = '%s', output_format_arrow_record_batch_size = %d",
						 codec == NULL || strcmp(codec, "none") == 0 ? "none" :
						 strcmp(codec, "lz4") == 0 ? "lz4_frame" : "zstd",
						 CH_OWN_UNIT_ROWS);
	else
		appendStringInfo(&settings,
						 "output_format_parquet_row_group_size = %d, output_format_parquet_compression_method = '%s', "
						 "output_format_parquet_write_page_index = 1, output_format_parquet_write_bloom_filter = 1",
						 CH_OWN_UNIT_ROWS, codec ? codec : "lz4");
	t->write_settings = settings.data;
}

/* ---- a part's file ---- */

/* A directory made, where it is not there yet */
static void
make_dir(const char *path)
{
	if (MakePGDirectory(path) < 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create directory \"%s\": %m", path)));
}

/* A part's temporary directory in its storage's, made empty: its absolute path */
static char *
tmp_part_dir(const ChStorageId *id, const char *tmp)
{
	char	   *storage = ch_storage_path(id, false);
	char	   *path = psprintf("%s%s", storage, tmp);
	struct stat st;

	make_dir(storage);
	if (stat(path, &st) == 0 && !rmtree(path, true))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not remove directory \"%s\"", path)));
	make_dir(path);
	pfree(storage);
	return path;
}

/*
 * A file's rows, units and bytes, its footer read (and kept, for the scans
 * that read it next), and its summary (VC25) for its part's row, by which the
 * part is listed without its file read; NULL where the library makes none
 */
static void
file_info(ChTable *t, const char *path, int64 *rows, int32 *units, int64 *bytes, bytea **summary)
{
	const ChEngine *eng = ch_engine();
	chte_external_listing *handle;
	chte_external_file info;
	chte_error	err;
	const void *data = NULL;
	size_t		size = 0;

	CH_CALL("reading a part's file", eng->own_list(t->handle, &path, 1, &handle, CH_ERR));
	if (eng->external_file_info(handle, 0, &info, &err) != 0 ||
		(eng->own_summary != NULL && eng->own_summary(t->handle, handle, 0, &data, &size, &err) != 0))
	{
		eng->external_listing_free(handle);
		ch_engine_raise(&err, "reading a part's file");
	}
	*rows = (int64) info.rows;
	*units = (int32) info.units;
	*bytes = (int64) info.size;
	*summary = NULL;
	if (data != NULL && size > 0)
	{
		*summary = palloc(VARHDRSZ + size);
		SET_VARSIZE(*summary, VARHDRSZ + size);
		memcpy(VARDATA(*summary), data, size);
	}
	eng->external_listing_free(handle);
}

/*
 * A part written in its temporary directory tmp of the storage, its file
 * there: its rows, units and bytes from its file, its files logged under its
 * name, its directory renamed to its name and synced, and its catalog row
 * inserted under cid.  The part's directory goes if the transaction aborts.
 */
static void
store_own_part(Relation rel, ChTable *t, const char *tmp, ChPart *part, CommandId cid)
{
	char	   *storage = ch_storage_path(&t->storage, false);
	char	   *from = psprintf("%s%s", storage, tmp);
	char	   *to = psprintf("%s%s", storage, part->name);
	char	   *file = psprintf("%s/%s", from, t->own_file);
	struct stat st;

	fsync_fname(from, true);
	file_info(t, file, &part->rows, &part->marks, &part->bytes, &part->summary);
	/* the files store logs a part's files before they get the part's name (§2.9) */
	ch_store_part_written(rel, &t->storage, tmp, part->name);
	if (stat(to, &st) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("the directory of part \"%s\" of \"%s\" is there already", part->name,
						RelationGetRelationName(rel))));
	if (rename(from, to) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not rename directory \"%s\" to \"%s\": %m", from, to)));
	fsync_fname(to, true);
	fsync_fname(storage, true);
	ch_part_insert(&t->storage, part, cid);
	pfree(file);
	pfree(to);
	pfree(from);
	pfree(storage);
}

/*
 * The write buffer's rows as a part (buffer.c's flush): written by the
 * format's writer into tmp_insert_<part>/, its rows as they came, in their
 * ids' order, then stored.  The writer, on an error, removes its file.
 */
void
ch_own_write_part(Relation rel, ChTable *t, const ChStorageId *id, ChVectorBuilder *builder,
				  uint64 first_rowid, uint64 last_rowid, CommandId cid)
{
	const ChEngine *eng = ch_engine();
	uint64		block;
	uint64		ignored;
	char	   *tmp;
	char	   *dir;
	char	   *file;
	chte_file_writer *writer;
	ResourceOwner owner;
	uint64		bytes;
	ChPart		part;

	ch_counters_allocate(id, 0, 1, &ignored, &block);
	memset(&part, 0, sizeof(part));
	part.name = psprintf("all_" UINT64_FORMAT "_" UINT64_FORMAT "_0", block, block);
	part.min_rowid = (int64) first_rowid;
	part.max_rowid = (int64) last_rowid;
	tmp = psprintf("tmp_insert_%s", part.name);
	dir = tmp_part_dir(id, tmp);
	file = psprintf("%s/%s", dir, t->own_file);

	owner = ch_handle_reserve();
	CH_CALL("writing a part", eng->own_write_begin(t->own_format, file, t->file_columns, t->nfile_columns,
												   t->write_settings, &writer, CH_ERR));
	ch_handle_opened(owner, CH_HANDLE_FILE_WRITER, writer);
	CH_CALL("writing a part", eng->file_write_batch(writer, ch_builder_rows(builder), ch_builder_vectors(builder),
													CH_ERR));
	CH_CALL("writing a part", eng->file_write_finish(writer, &bytes, CH_ERR));
	ch_handle_closing(owner, CH_HANDLE_FILE_WRITER, writer);
	eng->file_write_close(writer);
	store_own_part(rel, t, tmp, &part, cid);
}

/* ---- merges ---- */

/*
 * Each column's missing value (getmissingattr()), a row of the library's
 * vectors: what a merge writes for a column a part's file lacks, added after
 * the part was written
 */
static chte_vector *
missing_values(Relation rel, ChTable *t)
{
	TupleDesc	desc = RelationGetDescr(rel);
	TupleTableSlot *slot = MakeSingleTupleTableSlot(desc, &TTSOpsVirtual);
	ChVectorBuilder *b = ch_builder_create(t, CurrentMemoryContext);

	ExecClearTuple(slot);
	for (int i = 0; i < desc->natts; i++)
		slot->tts_values[i] = getmissingattr(desc, i + 1, &slot->tts_isnull[i]);
	ExecStoreVirtualTuple(slot);
	ch_builder_add(b, slot, 0);
	ExecDropSingleTupleTableSlot(slot);
	return ch_builder_vectors(b);
}

/* A part's blocks and level, from its name: all_<min>_<max>_<level> */
static bool
own_part_place(const ChPart *p, int64 *min_block, int64 *max_block, int64 *level)
{
	long long	a;
	long long	b;
	long long	l;

	if (sscanf(p->name, "all_%lld_%lld_%lld", &a, &b, &l) != 3)
		return false;
	*min_block = a;
	*max_block = b;
	*level = l;
	return true;
}

/*
 * The chosen parts merged into one (or a part alone written again without its
 * dead rows): its file written by the engine library from theirs, their rows
 * every snapshot sees deleted left out; its row inserted and theirs deleted,
 * the rows folded out unindexed and their tombstones deleted.  Returns how
 * many parts it took, 0 where nothing changed, and in *nfolded the rows it
 * folded out.
 */
static int
merge_own(Relation rel, ChTable *t, List *chosen, bool fold_only, int *nfolded)
{
	const ChEngine *eng = ch_engine();
	ChDeadRows	dead;
	ChListing  *listing;
	int			n = list_length(chosen);
	size_t	   *files = palloc(sizeof(size_t) * n);
	uint64	   *mine_all = NULL;
	int			nmine_all = 0;
	uint8	   *dropped = NULL;
	int64		min_block = PG_INT64_MAX;
	int64		max_block = PG_INT64_MIN;
	int64		level = 0;
	ChPart		next;
	ListCell   *lc;
	int			i = 0;
	char	   *tmp;
	char	   *dir;
	char	   *file;
	uint64		rows;
	uint64		bytes;

	ch_dead_rows(rel, t, &dead);
	listing = ch_own_listing(t, chosen);
	ch_listing_pin(listing);
	memset(&next, 0, sizeof(next));
	next.min_rowid = PG_INT64_MAX;
	next.max_rowid = PG_INT64_MIN;
	foreach(lc, chosen)
	{
		ChPart	   *p = lfirst(lc);
		int64		a;
		int64		b;
		int64		l;
		uint64	   *mine;
		int			nmine = 0;

		if (!own_part_place(p, &a, &b, &l))
			elog(ERROR, "part \"%s\" of \"%s\" is not named as a part of a table of its own files", p->name,
				 RelationGetRelationName(rel));
		min_block = Min(min_block, a);
		max_block = Max(max_block, b);
		level = Max(level, l);
		next.min_rowid = Min(next.min_rowid, p->min_rowid);
		next.max_rowid = Max(next.max_rowid, p->max_rowid);
		files[i++] = (size_t) p->file;
		if (dead.n > 0)
			nmine = ch_dead_rows_of_part(t, p, &dead, &mine, NULL);
		if (nmine > 0)
		{
			mine_all = mine_all ? repalloc_huge(mine_all, sizeof(uint64) * (nmine_all + nmine)) :
				palloc_extended(sizeof(uint64) * nmine, MCXT_ALLOC_HUGE);
			memcpy(mine_all + nmine_all, mine, sizeof(uint64) * nmine);
			nmine_all += nmine;
		}
	}
	*nfolded = nmine_all;
	if (fold_only && nmine_all == 0)
	{
		ch_listing_unpin(listing);
		return 0;
	}
	if (nmine_all > 1)
		qsort(mine_all, nmine_all, sizeof(uint64), ch_compare_uint64);
	if (nmine_all > 0)
		dropped = palloc0(nmine_all);

	/* a merge's level one above its inputs', a fold's the part's own, its version a block number of the storage's */
	if (fold_only)
	{
		uint64		version;
		uint64		ignored;

		ch_counters_allocate(&t->storage, 0, 1, &ignored, &version);
		next.name = psprintf("all_" INT64_FORMAT "_" INT64_FORMAT "_" INT64_FORMAT "_" UINT64_FORMAT,
							 min_block, max_block, level, version);
	}
	else
		next.name = psprintf("all_" INT64_FORMAT "_" INT64_FORMAT "_" INT64_FORMAT, min_block, max_block, level + 1);
	next.level = (int32) (fold_only ? level : level + 1);
	tmp = psprintf("tmp_merge_%s", next.name);
	dir = tmp_part_dir(&t->storage, tmp);
	file = psprintf("%s/%s", dir, t->own_file);
	CH_CALL("merging parts",
			eng->own_merge(t->handle, listing->handle, files, (size_t) n, mine_all, (size_t) nmine_all, dropped,
						   t->own_format, file, t->file_columns, t->nfile_columns, missing_values(rel, t),
						   t->write_settings, &rows, &bytes, CH_ERR));
	for (int k = 0; k < nmine_all; k++)
		if (!dropped[k])
			elog(ERROR, "row " UINT64_FORMAT " of \"%s\", deleted, was in no part a merge read", mine_all[k],
				 RelationGetRelationName(rel));
	ch_listing_unpin(listing);

	if (rows > 0)
		store_own_part(rel, t, tmp, &next, GetCurrentCommandId(true));
	else
	{
		/* every row dead: no part at all */
		if (!rmtree(dir, true))
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("could not remove directory \"%s\"", dir)));
	}
	foreach(lc, chosen)
		ch_part_delete(&t->storage, ((ChPart *) lfirst(lc))->name);

	/* the rows folded out: their index entries, then their tombstones */
	if (nmine_all > 0)
	{
		ch_dead_rows_unindex(rel, mine_all, nmine_all);
		ch_dead_rows_forget(t, &dead, mine_all, nmine_all);
	}
	return n;
}

/*
 * A table of its own files' merge (merge.c's ch_merge_parts() for it): the
 * first run, in row-id order, of two or more adjacent parts below
 * CH_OWN_MERGE_ROWS rows whose rows together are no more; with
 * CH_MERGE_FINAL, every part into one.  Under ShareUpdateExclusiveLock, as
 * merge.c's.  Returns how many parts were merged.
 */
int
ch_own_merge_parts(Relation rel, ChMergeMode mode)
{
	ChTable    *t = ch_table(rel);
	List	   *parts;
	List	   *chosen = NIL;
	ListCell   *lc;
	int			merged = 0;
	int			nfolded;

	Assert(CheckRelationLockedByMe(rel, ShareUpdateExclusiveLock, true));
	/* pinned before the flush: it, and the catalogs' reads, may accept an invalidation of the table */
	ch_table_pin(t);
	PG_TRY();
	{
		int64		sum = 0;
		int			nparts;

		ch_buffer_flush_relation(rel);
		parts = ch_parts_visible(&t->storage, NULL);
		/* on a node of the port's cluster (CH6), the parts before the first not older than the horizon (merge.c) */
		if (ch_cluster_node() && parts != NIL)
		{
			TransactionId horizon = ch_parts_horizon(&t->storage);
			List	   *settled = NIL;

			foreach(lc, parts)
			{
				if (!TransactionIdPrecedes(((ChPart *) lfirst(lc))->xmin, horizon))
					break;
				settled = lappend(settled, lfirst(lc));
			}
			parts = settled;
		}
		nparts = list_length(parts);
		if (mode == CH_MERGE_FINAL)
		{
			/* every part, or the one there is where rows of it are dead */
			if (nparts > 1)
				merged = merge_own(rel, t, parts, false, &nfolded);
			else if (nparts == 1)
				merged = merge_own(rel, t, parts, true, &nfolded);
		}
		else
		{
			foreach(lc, parts)
			{
				ChPart	   *p = lfirst(lc);

				if (p->rows >= CH_OWN_MERGE_ROWS || sum + p->rows > CH_OWN_MERGE_ROWS ||
					list_length(chosen) >= CH_OWN_MERGE_PARTS)
				{
					if (list_length(chosen) >= 2)
						break;
					/* a run too short: the next begins at this part, if it is small enough */
					list_free(chosen);
					chosen = NIL;
					sum = 0;
					if (p->rows >= CH_OWN_MERGE_ROWS)
						continue;
				}
				chosen = lappend(chosen, p);
				sum += p->rows;
			}
			if (list_length(chosen) >= 2)
				merged = merge_own(rel, t, chosen, false, &nfolded);
		}
	}
	PG_FINALLY();
	{
		ch_table_unpin(t);
	}
	PG_END_TRY();
	return merged;
}

/*
 * ch.fold() of a table of its own files: each part holding rows every
 * snapshot sees deleted written again without them.  Returns how many rows
 * were folded.
 */
int64
ch_own_fold(Relation rel)
{
	ChTable    *t = ch_table(rel);
	int64		folded = 0;

	ch_table_pin(t);
	PG_TRY();
	{
		ChDeadRows	dead;
		List	   *parts;
		ListCell   *lc;
		TransactionId horizon;

		ch_buffer_flush_relation(rel);
		ch_dead_rows(rel, t, &dead);
		if (dead.n > 0)
		{
			parts = ch_parts_visible(&t->storage, NULL);
			horizon = ch_cluster_node() ? ch_parts_horizon(&t->storage) : InvalidTransactionId;
			foreach(lc, parts)
			{
				ChPart	   *p = lfirst(lc);
				int			first = 0;
				int			nfolded = 0;

				if (TransactionIdIsValid(horizon) && !TransactionIdPrecedes(p->xmin, horizon))
					continue;
				/* a part whose row ids span none of the dead rows is left as it is */
				while (first < dead.n && dead.ids[first] < (uint64) p->min_rowid)
					first++;
				if (first == dead.n || dead.ids[first] > (uint64) p->max_rowid)
					continue;
				(void) merge_own(rel, t, list_make1(p), true, &nfolded);
				folded += nfolded;
			}
		}
	}
	PG_FINALLY();
	{
		ch_table_unpin(t);
	}
	PG_END_TRY();
	return folded;
}

/* ---- ch.own_files() ---- */

PG_FUNCTION_INFO_V1(ch_own_files);
/*
 * ch.own_files(rel): the parts of a table of its own files the caller's
 * snapshot sees, each one's file and its layout read back from the file
 * (§2.18): its units -- row groups or record batches -- and their rows, its
 * codec, and whether a Parquet file has a page index, bloom filters and
 * dictionaries.
 */
Datum
ch_own_files(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	const ChEngine *eng = ch_engine();
	Relation	rel = relation_open(PG_GETARG_OID(0), AccessShareLock);
	ChTable    *t;

	InitMaterializedSRF(fcinfo, 0);
	if (!ch_is_own_rel(rel))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a table of its own files", RelationGetRelationName(rel))));
	ch_buffer_flush_for_scan(rel, GetActiveSnapshot());
	t = ch_table(rel);
	ch_table_pin(t);
	PG_TRY();
	{
		List	   *parts = ch_parts_to_scan(&t->storage, GetActiveSnapshot());
		ChListing  *listing = ch_own_listing(t, parts);
		ListCell   *lc;

		ch_listing_pin(listing);
		foreach(lc, parts)
		{
			ChPart	   *p = lfirst(lc);
			chte_file_layout layout;
			Datum		values[12];
			bool		nulls[12] = {0};

			CH_CALL("reading a part's file",
					eng->own_file_layout(t->handle, listing->handle, (size_t) p->file, &layout, CH_ERR));
			values[0] = CStringGetTextDatum(p->name);
			values[1] = CStringGetTextDatum(ch_own_part_file(t, p->name));
			values[2] = Int64GetDatum(p->bytes);
			values[3] = Int64GetDatum((int64) layout.rows);
			values[4] = Int64GetDatum(p->min_rowid);
			values[5] = Int64GetDatum(p->max_rowid);
			values[6] = Int32GetDatum((int32) layout.units);
			values[7] = Int64GetDatum((int64) layout.min_unit_rows);
			values[8] = Int64GetDatum((int64) layout.max_unit_rows);
			values[9] = CStringGetTextDatum(layout.codec);
			values[10] = BoolGetDatum(layout.page_index != 0);
			values[11] = BoolGetDatum(layout.bloom_filters != 0);
			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		}
		ch_listing_unpin(listing);
	}
	PG_FINALLY();
	{
		ch_table_unpin(t);
	}
	PG_END_TRY();
	relation_close(rel, AccessShareLock);
	return (Datum) 0;
}
