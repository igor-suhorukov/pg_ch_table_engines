/*-------------------------------------------------------------------------
 *
 * buffer.c
 *	  The write buffer (ch_table_engines.md §2.7): rows inserted into a table
 *	  of an engine, columnar, until they become a part.
 *
 *	  A buffer holds the rows of one relation, one subtransaction and one
 *	  command id; a row of another command or subtransaction flushes it
 *	  first.  It becomes a part (§2.7 rule 3) as the query that wrote ends
 *	  (ExecutorFinish), as COPY, CREATE TABLE AS and a rewrite end
 *	  (finish_bulk_insert), before commit and PREPARE, before its
 *	  subtransaction ends, as a scan begins whose snapshot would see its
 *	  rows, and at ch.insert_buffer_rows or ch.insert_buffer_bytes.  A part's
 *	  catalog row is inserted under the buffer's command id.
 *
 *	  Each row gets its row id, its TID, as it enters: from a range taken
 *	  from the storage's counter, in chunks that grow to the buffer's limit.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relation.h"
#include "access/xact.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "chte.h"

typedef struct ChBuffer
{
	Oid			relid;
	ChStorageId storage;
	SubTransactionId subid;
	CommandId	cid;
	MemoryContext cxt;
	ChTable    *table;			/* pinned: the builder points into it */
	ChVectorBuilder *builder;
	uint64		next_rowid;		/* the taken range, [next_rowid, end_rowid) */
	uint64		end_rowid;
	uint64		chunk;			/* the next range's size */
	uint64		first_rowid;	/* of the buffered rows */
} ChBuffer;

/* The buffers of the running transaction, in TopTransactionContext */
static List *buffers = NIL;

static ChBuffer *
find_buffer(Oid relid)
{
	ListCell   *lc;

	foreach(lc, buffers)
	{
		ChBuffer   *b = lfirst(lc);

		if (b->relid == relid)
			return b;
	}
	return NULL;
}

static void
drop_buffer(ChBuffer *b)
{
	buffers = list_delete_ptr(buffers, b);
	ch_table_unpin(b->table);
	MemoryContextDelete(b->cxt);
	pfree(b);
}

/* The buffered rows become parts; the buffer stays, empty */
static void
flush(ChBuffer *b)
{
	const ChEngine *eng;
	Relation	rel;
	ChTable    *table;
	uint32		nrows = ch_builder_rows(b->builder);
	uint64		block;
	uint64		ignored;
	chte_writer *writer;
	size_t		nparts;
	ResourceOwner owner;

	if (nrows == 0)
		return;

	eng = ch_engine();
	rel = relation_open(b->relid, NoLock);
	table = b->table;
	ch_counters_allocate(&b->storage, 0, 1, &ignored, &block);

	owner = ch_handle_reserve();
	CH_CALL("starting a part", eng->write_begin(table->handle, block, &writer, CH_ERR));
	ch_handle_opened(owner, false, writer);
	PG_TRY();
	{
		CH_CALL("writing a part", eng->write_batch(writer, nrows, ch_builder_vectors(b->builder), CH_ERR));
		CH_CALL("writing a part", eng->write_finish(writer, &nparts, CH_ERR));
		for (size_t i = 0; i < nparts; i++)
		{
			chte_part_info info;
			ChPart		part;
			char	   *tmp_dir;

			CH_CALL("writing a part", eng->write_part(writer, i, &info, CH_ERR));
			tmp_dir = psprintf("tmp_insert_%s", info.name);
			part.manifest = NULL;
			if (table->options.storage == CH_STORAGE_PAGES)
				/* the pages store copies the part into pages; its directory goes with the writer */
				part.manifest = ch_pages_store_part(rel, table, tmp_dir);
			else
				/* the files store logs the part's files before they get the part's name */
				ch_store_part_written(rel, &b->storage, tmp_dir, info.name);
			part.name = info.name;
			part.rows = info.rows;
			part.bytes = info.bytes_on_disk;
			part.min_rowid = b->first_rowid;
			part.max_rowid = ch_builder_rowid(b->builder, nrows - 1);
			part.level = info.level;
			ch_part_insert(&b->storage, &part, b->cid);
			if (part.manifest)
				ch_pages_attach(table, &part);
		}
		/* a part of the pages store is in pages now: its directory goes, uncommitted */
		if (table->options.storage != CH_STORAGE_PAGES)
			CH_CALL("committing a part", eng->write_commit(writer, CH_ERR));
	}
	PG_CATCH();
	{
		/* the resource owner's release closes it, removing the part's directory */
		PG_RE_THROW();
	}
	PG_END_TRY();
	ch_handle_closing(owner, false, writer);
	eng->write_close(writer);

	relation_close(rel, NoLock);
	ch_builder_reset(b->builder);
}

void
ch_buffer_insert(Relation rel, TupleTableSlot *slot, CommandId cid)
{
	ChBuffer   *b = find_buffer(RelationGetRelid(rel));
	SubTransactionId subid = GetCurrentSubTransactionId();
	uint64		rowid;
	ChStorageId storage;

	ch_storage_id(rel, &rel->rd_locator, &storage);
	if (b && (b->cid != cid || b->subid != subid ||
			  memcmp(&b->storage, &storage, sizeof(storage)) != 0))
	{
		flush(b);
		drop_buffer(b);
		b = NULL;
	}
	if (b == NULL)
	{
		MemoryContext old;

		/* SERIALIZABLE: a statement's writes to the relation, as one relation-level write (§2.6) */
		CheckForSerializableConflictIn(rel, NULL, InvalidBlockNumber);
		old = MemoryContextSwitchTo(TopTransactionContext);

		b = palloc0(sizeof(ChBuffer));
		b->relid = RelationGetRelid(rel);
		b->storage = storage;
		b->subid = subid;
		b->cid = cid;
		b->cxt = AllocSetContextCreate(TopTransactionContext, "ch write buffer", ALLOCSET_DEFAULT_SIZES);
		b->table = ch_table_for_storage(rel, &storage);
		ch_table_pin(b->table);
		b->builder = ch_builder_create(b->table, b->cxt);
		b->chunk = 1024;
		buffers = lappend(buffers, b);
		MemoryContextSwitchTo(old);
	}

	if (b->next_rowid == b->end_rowid)
	{
		uint64		ignored;

		ch_counters_allocate(&b->storage, b->chunk, 0, &b->next_rowid, &ignored);
		b->end_rowid = b->next_rowid + b->chunk;
		b->chunk = Min(b->chunk * 2, (uint64) ch_insert_buffer_rows);
	}
	rowid = b->next_rowid++;
	if (ch_builder_rows(b->builder) == 0)
		b->first_rowid = rowid;
	ch_builder_add(b->builder, slot, rowid);
	ItemPointerSet(&slot->tts_tid, CH_ROWID_TO_BLOCK(rowid), CH_ROWID_TO_OFFSET(rowid));
	slot->tts_tableOid = RelationGetRelid(rel);

	if (ch_builder_rows(b->builder) >= (uint32) ch_insert_buffer_rows ||
		ch_builder_bytes(b->builder) >= (Size) ch_insert_buffer_bytes * 1024)
		flush(b);
}

void
ch_buffer_flush_relation(Relation rel)
{
	ChBuffer   *b = find_buffer(RelationGetRelid(rel));

	if (b)
		flush(b);
}

/* Before a scan whose snapshot would see buffered rows: those rows become a part */
void
ch_buffer_flush_for_scan(Relation rel, Snapshot snapshot)
{
	ChBuffer   *b = find_buffer(RelationGetRelid(rel));

	if (b == NULL || ch_builder_rows(b->builder) == 0)
		return;
	if (snapshot == NULL || !IsMVCCSnapshot(snapshot) || snapshot->curcid > b->cid)
		flush(b);
}

void
ch_buffer_flush_all(void)
{
	ListCell   *lc;
	bool		pushed = false;

	if (buffers == NIL)
		return;
	/* at pre-commit no snapshot is active, and the catalog's inserts want one */
	if (!ActiveSnapshotSet())
	{
		PushActiveSnapshot(GetTransactionSnapshot());
		pushed = true;
	}
	foreach(lc, buffers)
		flush(lfirst(lc));
	if (pushed)
		PopActiveSnapshot();
}

/* A buffered row by its row id, for a fetch by TID before its part is written */
bool
ch_buffer_fetch(Relation rel, uint64 rowid, Snapshot snapshot, TupleTableSlot *slot)
{
	ChBuffer   *b = find_buffer(RelationGetRelid(rel));
	uint32		nrows;
	uint64		first;

	if (b == NULL || (nrows = ch_builder_rows(b->builder)) == 0)
		return false;
	if (snapshot && IsMVCCSnapshot(snapshot) && snapshot->curcid <= b->cid)
		return false;
	first = b->first_rowid;
	if (rowid < first || rowid > ch_builder_rowid(b->builder, nrows - 1))
		return false;
	/* row ids of a buffer ascend with its rows, gaps allowed between ranges */
	{
		uint32		lo = 0;
		uint32		hi = nrows;

		while (lo < hi)
		{
			uint32		mid = lo + (hi - lo) / 2;
			uint64		v = ch_builder_rowid(b->builder, mid);

			if (v < rowid)
				lo = mid + 1;
			else
				hi = mid;
		}
		if (lo < nrows && ch_builder_rowid(b->builder, lo) == rowid)
		{
			ch_builder_fetch(b->builder, lo, slot);
			ItemPointerSet(&slot->tts_tid, CH_ROWID_TO_BLOCK(rowid), CH_ROWID_TO_OFFSET(rowid));
			slot->tts_tableOid = RelationGetRelid(rel);
			return true;
		}
	}
	return false;
}

/* A relation whose storage changed (TRUNCATE, a rewrite) loses its buffered rows */
void
ch_buffer_forget_relation(Oid relid)
{
	ChBuffer   *b = find_buffer(relid);

	if (b)
		drop_buffer(b);
}

void
ch_buffer_xact_callback(XactEvent event)
{
	switch (event)
	{
		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_PREPARE:
			ch_buffer_flush_all();
			break;
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
		case XACT_EVENT_PREPARE:
			/* TopTransactionContext takes the buffers with it */
			buffers = NIL;
			break;
	}
}

void
ch_buffer_subxact_callback(SubXactEvent event, SubTransactionId mySubid)
{
	ListCell   *lc;

	switch (event)
	{
		case SUBXACT_EVENT_START_SUB:
			/*
			 * The parent's rows become parts now, under the parent's xid: a
			 * buffer only ever holds rows of the running subtransaction.
			 */
			ch_buffer_flush_all();
			break;
		case SUBXACT_EVENT_PRE_COMMIT_SUB:
			ch_buffer_flush_all();
			break;
		case SUBXACT_EVENT_ABORT_SUB:
			foreach(lc, buffers)
			{
				ChBuffer   *b = lfirst(lc);

				if (b->subid == mySubid)
				{
					drop_buffer(b);
					/* the list changed; start over */
					ch_buffer_subxact_callback(event, mySubid);
					return;
				}
			}
			break;
		default:
			break;
	}
}
