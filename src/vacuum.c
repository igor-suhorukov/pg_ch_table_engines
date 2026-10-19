/*-------------------------------------------------------------------------
 *
 * vacuum.c
 *	  What deletions leave behind (ch_table_engines.md §2.10, §2.13): the
 *	  index entries of rows deleted for every snapshot, and the tombstones
 *	  themselves.
 *
 *	  VACUUM removes the indexes' entries of rows whose deletion every
 *	  snapshot sees, as heap's VACUUM removes those of dead tuples
 *	  (vacuumlazy.c): index_bulk_delete() where there are such rows, and
 *	  index_vacuum_cleanup() always -- GIN flushes its pending list there --
 *	  unless INDEX_CLEANUP is off, an index's exact counts written to its
 *	  pg_class row as heap's VACUUM writes them; none of it is transactional.
 *	  It cannot fold the tombstones: a lazy VACUUM must not write rows of its
 *	  own -- other backends' snapshots leave out the xid of a backend in
 *	  PROC_IN_VACUUM (procarray.c, GetSnapshotData(); vacuum.c,
 *	  vac_update_relstats()) -- and a fold is a part's row and its
 *	  tombstones' changed.
 *
 *	  A fold (ch.fold()), in a transaction of its own: each part holding rows
 *	  deleted for every snapshot is written again without them, as a new part
 *	  named as a mutation of it (the engine library's chte_rewrite_part),
 *	  whose catalog row replaces the part's, the folded tombstones deleted
 *	  with it; the index entries of those rows are removed first.  A merge
 *	  (merge.c) folds its inputs' dead rows out the same way.  The old part's
 *	  storage goes once no row of ch.parts names it (store.c, pages.c).
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/multixact.h"
#include "access/transam.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "chte.h"
#include "fold.h"
#include "tombstones.h"

void
ch_dead_rows(Relation rel, ChTable *t, ChDeadRows *dead)
{
	TransactionId oldest = GetOldestNonRemovableTransactionId(rel);
	int			n;
	ChTombstone *all = ch_tombstones_all(&t->storage, 0, PG_INT64_MAX, &n);

	memset(dead, 0, sizeof(*dead));
	if (n == 0)
		return;
	dead->ids = palloc_extended(sizeof(uint64) * n, MCXT_ALLOC_HUGE);
	dead->tombs = palloc_extended(sizeof(ChTombstone) * n, MCXT_ALLOC_HUGE);
	for (int i = 0; i < n; i++)
	{
		if (!all[i].committed || !TransactionIdPrecedes(all[i].xmin, oldest))
			continue;
		dead->tombs[dead->ntombs++] = all[i];
		if (dead->n == 0 || dead->ids[dead->n - 1] != all[i].rowid)
			dead->ids[dead->n++] = all[i].rowid;
	}
	pfree(all);
}

/* The row ids an index's entries are dead for, ascending */
typedef struct DeadIds
{
	const uint64 *ids;
	int			n;
} DeadIds;

static bool
entry_dead(ItemPointer itemptr, void *state)
{
	DeadIds    *dead = state;

	return ch_rowid_in(dead->ids, dead->n, CH_TID_TO_ROWID(itemptr));
}

/*
 * Removes the indexes' entries of the dead rows, and cleans each index up
 * (lazy_vacuum_one_index(), lazy_cleanup_one_index()); in VACUUM, an index's
 * exact counts go to its pg_class row (update_relstats_all_indexes()).
 */
static void
clean_indexes(Relation rel, const uint64 *ids, int n, BufferAccessStrategy strategy, int message_level, bool vacuum)
{
	Relation   *indexes;
	int			nindexes;
	DeadIds		dead = {ids, n};

	vac_open_indexes(rel, RowExclusiveLock, &nindexes, &indexes);
	for (int i = 0; i < nindexes; i++)
	{
		IndexVacuumInfo ivinfo;
		IndexBulkDeleteResult *stats = NULL;

		memset(&ivinfo, 0, sizeof(ivinfo));
		ivinfo.index = indexes[i];
		ivinfo.heaprel = rel;
		ivinfo.analyze_only = false;
		ivinfo.report_progress = false;
		ivinfo.estimated_count = true;
		ivinfo.message_level = message_level;
		ivinfo.num_heap_tuples = rel->rd_rel->reltuples;
		ivinfo.strategy = strategy;
		if (n > 0)
			stats = index_bulk_delete(&ivinfo, NULL, entry_dead, &dead);
		stats = index_vacuum_cleanup(&ivinfo, stats);
		if (stats && vacuum && !stats->estimated_count)
			vac_update_relstats(indexes[i], stats->num_pages, stats->num_index_tuples, 0, 0, false,
								InvalidTransactionId, InvalidMultiXactId, NULL, NULL, false);
		if (stats)
			pfree(stats);
	}
	vac_close_indexes(nindexes, indexes, NoLock);
}

/* VACUUM (relation_vacuum): the dead rows' index entries, and every index's cleanup */
void
ch_vacuum_indexes(Relation rel, const VacuumParams *params, BufferAccessStrategy strategy)
{
	ChTable    *t;
	ChDeadRows	dead;

	if (!RelationGetForm(rel)->relhasindex || params->index_cleanup == VACOPTVALUE_DISABLED)
		return;
	/* pinned: the catalogs' reads may accept an invalidation of the table */
	t = ch_table(rel);
	ch_table_pin(t);
	ch_dead_rows(rel, t, &dead);
	ch_table_unpin(t);
	clean_indexes(rel, dead.ids, dead.n, strategy, params->options & VACOPT_VERBOSE ? INFO : DEBUG2, true);
}

void
ch_dead_rows_unindex(Relation rel, const uint64 *ids, int n)
{
	if (RelationGetForm(rel)->relhasindex && n > 0)
		clean_indexes(rel, ids, n, NULL, DEBUG2, false);
}

/* The sorted ids among [lo, hi]: their index range [*first, *last) */
static void
id_range(const uint64 *ids, int n, uint64 lo, uint64 hi, int *first, int *last)
{
	int			a = 0;
	int			b = n;

	while (a < b)
	{
		int			mid = a + (b - a) / 2;

		if (ids[mid] < lo)
			a = mid + 1;
		else
			b = mid;
	}
	*first = a;
	b = n;
	while (a < b)
	{
		int			mid = a + (b - a) / 2;

		if (ids[mid] <= hi)
			a = mid + 1;
		else
			b = mid;
	}
	*last = a;
}

int
ch_dead_rows_of_part(ChTable *t, const ChPart *p, const ChDeadRows *dead, uint64 **mine, bool *found)
{
	const ChEngine *eng = ch_engine();
	int			first;
	int			last;
	uint64	   *offsets;
	int			nmine = 0;

	*mine = NULL;
	id_range(dead->ids, dead->n, (uint64) p->min_rowid, (uint64) p->max_rowid, &first, &last);
	if (first >= last)
		return 0;
	if (t->options.storage == CH_STORAGE_PAGES)
		ch_pages_attach(t, p);
	/* the part's own rows among them: its row ids may span others' (a merge's) */
	offsets = palloc(sizeof(uint64) * (last - first));
	*mine = palloc(sizeof(uint64) * (last - first));
	CH_CALL("finding a part's deleted rows",
			eng->part_locate(t->handle, p->name, dead->ids + first, last - first, offsets, CH_ERR));
	for (int i = 0; i < last - first; i++)
		if (offsets[i] != PG_UINT64_MAX)
		{
			(*mine)[nmine++] = dead->ids[first + i];
			if (found)
				found[first + i] = true;
		}
	pfree(offsets);
	return nmine;
}

chte_writer *
ch_fold_part(ChTable *t, const ChPart *p, const uint64 *mine, int nmine, size_t *nparts, ResourceOwner *owner)
{
	const ChEngine *eng = ch_engine();
	uint64		block;
	uint64		ignored;
	chte_writer *writer;

	/* its version a block number of the storage's, as a new part's number is */
	ch_counters_allocate(&t->storage, 0, 1, &ignored, &block);
	*owner = ch_handle_reserve();
	CH_CALL("folding deletions into a part",
			eng->rewrite_part(t->handle, p->name, mine, nmine, block, &writer, nparts, CH_ERR));
	ch_handle_opened(*owner, CH_HANDLE_WRITER, writer);
	return writer;
}

void
ch_dead_rows_forget(ChTable *t, const ChDeadRows *dead, const uint64 *ids, int n)
{
	ChTombstone *gone = palloc_extended(sizeof(ChTombstone) * Max(dead->ntombs, 1), MCXT_ALLOC_HUGE);
	int			ngone = 0;

	for (int i = 0; i < dead->ntombs; i++)
		if (ch_rowid_in(ids, n, dead->tombs[i].rowid))
			gone[ngone++] = dead->tombs[i];
	ch_tombstones_delete(&t->storage, gone, ngone);
	pfree(gone);
}

/*
 * Folds the deletions every snapshot sees into the parts holding their rows:
 * returns how many rows were folded.
 */
int64
ch_fold_deletions(Relation rel)
{
	const ChEngine *eng = ch_engine();
	ChTable    *t;
	ChDeadRows	dead;
	List	   *parts;
	ListCell   *lc;
	uint64	   *folded;
	int			nfolded = 0;
	bool	   *handled;
	ChTombstone *gone;
	int			ngone = 0;

	/* pinned throughout: the flush, the catalogs' reads and writes may accept an invalidation of the table */
	t = ch_table(rel);
	ch_table_pin(t);
	ch_buffer_flush_relation(rel);
	ch_dead_rows(rel, t, &dead);
	if (dead.n == 0)
	{
		ch_table_unpin(t);
		return 0;
	}
	ch_dead_rows_unindex(rel, dead.ids, dead.n);

	folded = palloc_extended(sizeof(uint64) * dead.n, MCXT_ALLOC_HUGE);
	handled = palloc_extended(sizeof(bool) * dead.n, MCXT_ALLOC_HUGE | MCXT_ALLOC_ZERO);
	parts = ch_parts_visible(&t->storage, NULL);
	foreach(lc, parts)
	{
		ChPart	   *p = lfirst(lc);
		uint64	   *mine;
		int			nmine;
		chte_writer *writer;
		size_t		nparts;
		ResourceOwner owner;

		nmine = ch_dead_rows_of_part(t, p, &dead, &mine, handled);
		if (nmine == 0)
			continue;
		writer = ch_fold_part(t, p, mine, nmine, &nparts, &owner);
		for (size_t i = 0; i < nparts; i++)
		{
			chte_part_info info;
			ChPart		next;

			CH_CALL("folding deletions into a part", eng->write_part(writer, i, &info, CH_ERR));
			next = *p;
			next.name = pstrdup(info.name);
			next.rows = (int64) info.rows;
			next.bytes = (int64) info.bytes_on_disk;
			next.level = (int32) info.level;
			next.marks = (int32) info.marks;
			next.manifest = NULL;
			next.created = 0;
			next.summary = ch_part_summary_written(writer, i);
			if (t->options.storage == CH_STORAGE_PAGES)
				next.manifest = ch_pages_store_part(rel, t, info.temp_dir, &next, GetCurrentCommandId(true));
			else
			{
				ch_store_part_written(rel, &t->storage, info.temp_dir, info.name);
				ch_part_insert(&t->storage, &next, GetCurrentCommandId(true));
			}
			if (next.manifest)
				ch_pages_attach(t, &next);
		}
		ch_part_delete(&t->storage, p->name);
		/* a part of the pages store is in pages now: its directory goes, uncommitted */
		if (t->options.storage != CH_STORAGE_PAGES)
			CH_CALL("committing a part", eng->write_commit(writer, CH_ERR));
		ch_handle_closing(owner, CH_HANDLE_WRITER, writer);
		eng->write_close(writer);
		memcpy(folded + nfolded, mine, sizeof(uint64) * nmine);
		nfolded += nmine;
	}

	/* the tombstones folded, and those of rows no part holds, go */
	qsort(folded, nfolded, sizeof(uint64), ch_compare_uint64);
	gone = palloc_extended(sizeof(ChTombstone) * dead.ntombs, MCXT_ALLOC_HUGE);
	for (int i = 0; i < dead.ntombs; i++)
	{
		uint64		rowid = dead.tombs[i].rowid;
		int			at;
		int			ignored;

		id_range(dead.ids, dead.n, rowid, rowid, &at, &ignored);
		if (ch_rowid_in(folded, nfolded, rowid) || !handled[at])
			gone[ngone++] = dead.tombs[i];
	}
	ch_tombstones_delete(&t->storage, gone, ngone);
	ch_table_unpin(t);
	return nfolded;
}
