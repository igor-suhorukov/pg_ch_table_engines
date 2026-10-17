/*-------------------------------------------------------------------------
 *
 * merge.c
 *	  Merges (ch_table_engines.md §2.11): a table's parts merged by
 *	  ClickHouse's MergeTask into one, chosen by ClickHouse's merge selector,
 *	  in a transaction under ShareUpdateExclusiveLock -- the merge workers'
 *	  (worker.c), ch.optimize()'s.
 *
 *	  The rows keep their row ids, a column of theirs, so the merged part
 *	  holds the inputs' rows under the same TIDs: indexes keep their entries,
 *	  tombstones go on naming their rows.  The merged part's row replaces
 *	  the inputs' rows in one transaction, so a snapshot sees the inputs or
 *	  the merged part, the same rows either way (§2.6, rule 4).
 *
 *	  An input's rows every snapshot sees deleted are folded out first, into
 *	  a part of the merge's own beside the input (vacuum.c's ch_fold_part):
 *	  committed in the storage's directory, it has no catalog row and no WAL,
 *	  and goes after the merge, or with the sweep after a crash -- the merged
 *	  part's files are what a standby and recovery need.  Their tombstones go
 *	  with them, their index entries first, as a fold's.
 *
 *	  Only MergeTree's tables merge (CH3).  The other engines' merges drop
 *	  rows -- replaced, collapsed, summed -- that a running transaction may
 *	  still reach by their TIDs, an EvalPlanQual's fetch of the latest parts
 *	  among them, and whose index entries no deletion names: they come with
 *	  their engines' FINAL in CH4.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relation.h"
#include "access/xact.h"
#include "catalog/pg_class.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/timestamp.h"

#include "chte.h"
#include "fold.h"

/* A part's place among the others: its name's blocks, level and version, all_<min>_<max>_<level>[_<version>] */
typedef struct PartPlace
{
	ChPart	   *part;
	int64		min_block;
	int64		max_block;
	int64		level;
	int64		version;
} PartPlace;

static bool
part_place(ChPart *p, PartPlace *place)
{
	long long	a;
	long long	b;
	long long	l;
	long long	v = 0;
	char		partition[64];
	int			n;

	place->part = p;
	n = sscanf(p->name, "%63[^_]_%lld_%lld_%lld_%lld", partition, &a, &b, &l, &v);
	if (n < 4)
		return false;
	place->min_block = a;
	place->max_block = b;
	place->level = l;
	place->version = n == 5 ? v : 0;
	return true;
}

/* ClickHouse's order of parts (MergeTreePartInfo::toTuple()): blocks, then level, then version */
static int
place_order(const void *x, const void *y)
{
	const PartPlace *a = x;
	const PartPlace *b = y;

	if (a->min_block != b->min_block)
		return a->min_block < b->min_block ? -1 : 1;
	if (a->max_block != b->max_block)
		return a->max_block < b->max_block ? -1 : 1;
	if (a->level != b->level)
		return a->level < b->level ? -1 : 1;
	if (a->version != b->version)
		return a->version < b->version ? -1 : 1;
	return 0;
}

/* A part's rows: in pages, or a directory logged as the files store logs a part's */
static void
store_part(Relation rel, ChTable *t, const char *dir, ChPart *part)
{
	if (t->options.storage == CH_STORAGE_PAGES)
	{
		ch_pages_store_part(rel, t, dir, part, GetCurrentCommandId(true));
		ch_pages_attach(t, part);
	}
	else
	{
		ch_store_part_written(rel, &t->storage, dir, part->name);
		ch_part_insert(&t->storage, part, GetCurrentCommandId(true));
	}
}

/*
 * Merges the chosen parts, in ClickHouse's order: returns how many were
 * merged or folded, 0 where nothing changed.
 */
static int
merge_chosen(Relation rel, ChTable *t, const PartPlace *chosen, int n)
{
	const ChEngine *eng = ch_engine();
	ChDeadRows	dead;
	const char **names = palloc(sizeof(char *) * n);
	int		   *from = palloc(sizeof(int) * n);	/* the chosen part each input is of */
	chte_part_info *local = palloc0(sizeof(chte_part_info) * n);	/* an input's folded part, its name "" if none */
	int			ninputs = 0;
	uint64	   *folded = NULL;
	int			nfolded = 0;
	int64		min_rowid = PG_INT64_MAX;
	int64		max_rowid = PG_INT64_MIN;
	bool		keep_local = false;

	ch_dead_rows(rel, t, &dead);
	for (int i = 0; i < n; i++)
	{
		ChPart	   *p = chosen[i].part;
		uint64	   *mine = NULL;
		int			nmine = 0;
		chte_writer *writer;
		size_t		nparts;
		ResourceOwner owner;

		min_rowid = Min(min_rowid, p->min_rowid);
		max_rowid = Max(max_rowid, p->max_rowid);
		if (t->options.storage == CH_STORAGE_PAGES)
			ch_pages_attach(t, p);
		if (dead.n > 0)
			nmine = ch_dead_rows_of_part(t, p, &dead, &mine, NULL);
		if (nmine == 0)
		{
			from[ninputs] = i;
			names[ninputs++] = p->name;
			continue;
		}
		/* the input's dead rows folded out first, into a part of the merge's own */
		folded = folded ? repalloc_huge(folded, sizeof(uint64) * (nfolded + nmine)) :
			palloc_extended(sizeof(uint64) * nmine, MCXT_ALLOC_HUGE);
		memcpy(folded + nfolded, mine, sizeof(uint64) * nmine);
		nfolded += nmine;
		writer = ch_fold_part(t, p, mine, nmine, &nparts, &owner);
		if (nparts > 0)
		{
			CH_CALL("folding a part's deletions out", eng->write_part(writer, 0, &local[ninputs], CH_ERR));
			CH_CALL("folding a part's deletions out", eng->write_commit(writer, CH_ERR));
			from[ninputs] = i;
			names[ninputs] = local[ninputs].name;
			ninputs++;
		}
		ch_handle_closing(owner, CH_HANDLE_WRITER, writer);
		eng->write_close(writer);
	}
	if (ninputs == n && nfolded == 0 && n < 2)
		return 0;				/* one part, no row of it dead: nothing to do */

	if (ninputs >= 2)
	{
		chte_writer *writer;
		chte_part_info info;
		ChPart		next;
		ResourceOwner owner;
		uint64		version;
		uint64		ignored;

		/* the merged part's version a block number of the storage's: no other part ever has its name */
		ch_counters_allocate(&t->storage, 0, 1, &ignored, &version);
		owner = ch_handle_reserve();
		CH_CALL("merging parts", eng->merge(t->handle, names, ninputs, version, &writer, CH_ERR));
		ch_handle_opened(owner, CH_HANDLE_WRITER, writer);
		CH_CALL("merging parts", eng->write_part(writer, 0, &info, CH_ERR));
		next.name = pstrdup(info.name);
		next.rows = (int64) info.rows;
		next.bytes = (int64) info.bytes_on_disk;
		next.min_rowid = min_rowid;
		next.max_rowid = max_rowid;
		next.level = (int32) info.level;
		next.marks = (int32) info.marks;
		next.manifest = NULL;
		next.created = 0;
		store_part(rel, t, info.temp_dir, &next);
		/* a part of the pages store is in pages now: its directory goes, uncommitted */
		if (t->options.storage != CH_STORAGE_PAGES)
			CH_CALL("committing a part", eng->write_commit(writer, CH_ERR));
		ch_handle_closing(owner, CH_HANDLE_WRITER, writer);
		eng->write_close(writer);
	}
	else if (ninputs == 1 && local[0].name[0])
	{
		/* one input left, the others' rows all dead: its folded part the result */
		ChPart		next = *chosen[from[0]].part;

		next.name = pstrdup(local[0].name);
		next.rows = (int64) local[0].rows;
		next.bytes = (int64) local[0].bytes_on_disk;
		next.level = (int32) local[0].level;
		next.marks = (int32) local[0].marks;
		next.manifest = NULL;
		next.created = 0;
		store_part(rel, t, local[0].name, &next);
		/* the files store's part stays where it is, its directory the part's */
		keep_local = t->options.storage != CH_STORAGE_PAGES;
	}

	/* the inputs' rows go, the merged part's row in their place; an input alone left unchanged keeps its own */
	for (int i = 0; i < n; i++)
		if (!(ninputs == 1 && !local[0].name[0] && from[0] == i))
			ch_part_delete(&t->storage, chosen[i].part->name);
	for (int i = 0; i < ninputs; i++)
		if (local[i].name[0] && !(keep_local && i == 0))
			CH_CALL("removing a merge's folded part", eng->drop_part(t->handle, local[i].name, CH_ERR));

	/* the rows folded out: their index entries, then their tombstones */
	if (nfolded > 0)
	{
		qsort(folded, nfolded, sizeof(uint64), ch_compare_uint64);
		ch_dead_rows_unindex(rel, folded, nfolded);
		ch_dead_rows_forget(t, &dead, folded, nfolded);
	}
	return n;
}

int
ch_merge_parts(Relation rel, ChMergeMode mode)
{
	const ChEngine *eng = ch_engine();
	ChTable    *t = ch_table(rel);
	List	   *parts;
	ListCell   *lc;
	PartPlace  *places;
	int			nplaces = 0;
	size_t		first = 0;
	size_t		count = 0;
	int			merged = 0;

	Assert(CheckRelationLockedByMe(rel, ShareUpdateExclusiveLock, true));
	if (t->kind != CH_ENGINE_MERGETREE)
	{
		if (mode == CH_MERGE_BACKGROUND)
			return 0;
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("merges of the %s engine's tables come in phase CH4 of ch_table_engines.md",
						ch_engine_name(t->kind)),
				 errdetail("Their merges drop rows that running transactions may still reach by their TIDs.")));
	}
	/* pinned before the flush: it, and the catalogs' reads, may accept an invalidation of the table */
	ch_table_pin(t);
	PG_TRY();
	{
		ch_buffer_flush_relation(rel);
		parts = ch_parts_visible(&t->storage, NULL);
		places = palloc(sizeof(PartPlace) * Max(list_length(parts), 1));
		foreach(lc, parts)
		{
			if (!part_place(lfirst(lc), &places[nplaces]))
				elog(ERROR, "part \"%s\" of \"%s\" is not named as a part of a table without partitions",
					 ((ChPart *) lfirst(lc))->name, RelationGetRelationName(rel));
			nplaces++;
		}
		if (nplaces > 1)
			qsort(places, nplaces, sizeof(PartPlace), place_order);

		if (mode == CH_MERGE_FINAL)
		{
			/* every part, or the one there is where rows of it are dead */
			first = 0;
			count = nplaces;
		}
		else if (nplaces > 1)
		{
			chte_merge_candidate *c = palloc(sizeof(chte_merge_candidate) * nplaces);
			TimestampTz now = GetCurrentTimestamp();

			for (int i = 0; i < nplaces; i++)
			{
				ChPart	   *p = places[i].part;

				c[i].name = p->name;
				c[i].bytes = (uint64) Max(p->bytes, 0);
				c[i].rows = (uint64) Max(p->rows, 0);
				c[i].age_seconds = now > p->created ? (uint64) ((now - p->created) / USECS_PER_SEC) : 0;
			}
			CH_CALL("choosing parts to merge",
					eng->merge_select(t->handle, c, nplaces, mode == CH_MERGE_OPTIMIZE, &first, &count, CH_ERR));
		}
		if (count >= 2 || (count == 1 && mode == CH_MERGE_FINAL))
			merged = merge_chosen(rel, t, places + first, (int) count);
	}
	PG_FINALLY();
	{
		ch_table_unpin(t);
	}
	PG_END_TRY();
	return merged;
}

PG_FUNCTION_INFO_V1(ch_optimize);
/*
 * ch.optimize(rel, final): merges in the calling session, as OPTIMIZE TABLE
 * does -- the parts the selector chooses for a merge now, or with final every
 * part into one.  Returns how many parts were merged.
 */
Datum
ch_optimize(PG_FUNCTION_ARGS)
{
	Relation	rel = relation_open(PG_GETARG_OID(0), ShareUpdateExclusiveLock);
	bool		final = PG_GETARG_BOOL(1);
	int			merged;

	if (!ch_is_engine_rel(rel, NULL))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a table of the ClickHouse engines", RelationGetRelationName(rel))));
	if (!object_ownercheck(RelationRelationId, RelationGetRelid(rel), GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE, RelationGetRelationName(rel));
	merged = ch_merge_parts(rel, final ? CH_MERGE_FINAL : CH_MERGE_OPTIMIZE);
	relation_close(rel, NoLock);
	PG_RETURN_INT32(merged);
}
