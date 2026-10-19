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
 *	  vexec's sink's batches go straight into the engine library's writer
 *	  where it takes them (VC18): ClickHouse's columns filled from the
 *	  batch's own arrays, the buffer holding only their row ids -- where the
 *	  rows of tuple_insert are copied into the builder's buffers, and from
 *	  them into ClickHouse's columns as the part is made.  A buffer holds
 *	  rows of one of the two kinds at a time: a row of the other makes the
 *	  part first.  A pages-store part is written into memory, where the
 *	  library can, and copied into its pages from there, with no temporary
 *	  files.
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
#include "vexec/vexec_sink.h"

typedef struct ChBuffer
{
	Oid			relid;
	ChStorageId storage;
	SubTransactionId subid;
	CommandId	cid;
	MemoryContext cxt;
	ChTable    *table;			/* pinned: the builder points into it */
	ChVectorBuilder *builder;
	/* VC18: a sink's rows, in the library's writer; their row ids, ascending */
	chte_writer *writer;
	ResourceOwner writer_owner;
	uint32		wrows;
	Size		wbytes;
	uint64	   *wrowids;
	uint32		wcap;
	uint32		reserve_rows;	/* the rows of a part made at the limits: the next writer's room */
	MemoryContext batch_cxt;	/* a batch's conversions (ch_builder_columns_in()) */
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

/* The buffer's writer of a sink's rows closed, its rows gone with it */
static void
close_writer(ChBuffer *b)
{
	if (b->writer == NULL)
		return;
	ch_handle_closing(b->writer_owner, CH_HANDLE_WRITER, b->writer);
	ch_engine()->write_close(b->writer);
	b->writer = NULL;
	b->wrows = 0;
	b->wbytes = 0;
}

/* The buffer's rows, of either kind */
static uint32
buffered_rows(ChBuffer *b)
{
	return b->writer != NULL ? b->wrows : ch_builder_rows(b->builder);
}

/* The row id of buffered row i, of either kind */
static uint64
buffer_rowid(ChBuffer *b, uint32 i)
{
	return b->writer != NULL ? b->wrowids[i] : ch_builder_rowid(b->builder, i);
}

static void
drop_buffer(ChBuffer *b)
{
	buffers = list_delete_ptr(buffers, b);
	close_writer(b);
	ch_table_unpin(b->table);
	MemoryContextDelete(b->cxt);
	pfree(b);
}

/*
 * Back-pressure (§2.7 rule 5): with merges behind, a part's insert waits, as
 * ClickHouse's inserts wait from parts_to_delay_insert active parts on, and
 * fails at parts_to_throw_insert (the shim's chte_insert_delay, the table's
 * settings).  Not for a temporary table, which no merge worker reaches, nor
 * for the engines whose merges come in CH4: nothing would ever merge them.
 * The parts are counted as they stand now (SnapshotSelf), not as the
 * transaction's snapshot sees them: an MVCC snapshot's scan of ch.parts would
 * take predicate locks and conflicts out, and so serialization failures,
 * that no read of the table's rows asked for.
 */
static void
wait_for_merges(Relation rel, ChBuffer *b)
{
	const ChEngine *eng = ch_engine();
	List	   *parts;
	ListCell   *lc;
	uint64		bytes = 0;
	uint64		delay;

	if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP || b->table->kind != CH_ENGINE_MERGETREE)
		return;
	parts = ch_parts_visible(&b->storage, SnapshotSelf);
	foreach(lc, parts)
		bytes += (uint64) Max(((ChPart *) lfirst(lc))->bytes, 0);
	CH_CALL("inserting a part", eng->insert_delay(b->table->handle, list_length(parts), bytes, &delay, CH_ERR));
	list_free_deep(parts);
	while (delay > 0)
	{
		uint64		step = Min(delay, 100);

		CHECK_FOR_INTERRUPTS();
		pg_usleep((long) step * 1000L);
		delay -= step;
	}
}

/* The buffered rows become parts; the buffer stays, empty */
static void
flush(ChBuffer *b)
{
	const ChEngine *eng;
	Relation	rel;
	ChTable    *table;
	uint32		nrows = buffered_rows(b);
	bool		sink = b->writer != NULL;
	uint64		block;
	uint64		ignored;
	chte_writer *writer;
	size_t		nparts;
	ResourceOwner owner;
	uint64		last_rowid;

	b->reserve_rows = 0;
	if (nrows == 0)
	{
		close_writer(b);
		return;
	}

	eng = ch_engine();
	rel = relation_open(b->relid, NoLock);
	table = b->table;
	wait_for_merges(rel, b);
	ch_counters_allocate(&b->storage, 0, 1, &ignored, &block);

	if (sink)
	{
		/* VC18: the sink's rows, in the writer since its first batch, numbered now */
		writer = b->writer;
		owner = b->writer_owner;
		last_rowid = b->wrowids[nrows - 1];
		CH_CALL("writing a part", eng->write_block(writer, block, CH_ERR));
	}
	else
	{
		last_rowid = ch_builder_rowid(b->builder, nrows - 1);
		owner = ch_handle_reserve();
		CH_CALL("starting a part", eng->write_begin(table->handle, block, &writer, CH_ERR));
		ch_handle_opened(owner, CH_HANDLE_WRITER, writer);
	}
	PG_TRY();
	{
		if (!sink)
			CH_CALL("writing a part", eng->write_batch(writer, nrows, ch_builder_vectors(b->builder), CH_ERR));
		/* VC18: a pages-store part into memory, for its pages, where the library can */
		if (table->options.storage == CH_STORAGE_PAGES && eng->write_in_memory != NULL && ch_enable_memory_parts)
			CH_CALL("writing a part", eng->write_in_memory(writer, CH_ERR));
		CH_CALL("writing a part", eng->write_finish(writer, &nparts, CH_ERR));
		for (size_t i = 0; i < nparts; i++)
		{
			chte_part_info info;
			ChPart		part;
			char	   *tmp_dir;

			CH_CALL("writing a part", eng->write_part(writer, i, &info, CH_ERR));
			tmp_dir = psprintf("tmp_insert_%s", info.name);
			part.name = info.name;
			part.rows = info.rows;
			part.bytes = info.bytes_on_disk;
			part.min_rowid = b->first_rowid;
			part.max_rowid = last_rowid;
			part.level = info.level;
			part.marks = (int32) info.marks;
			part.manifest = NULL;
			part.created = 0;
			if (table->options.storage == CH_STORAGE_PAGES)
			{
				const chte_part_file *files = NULL;
				size_t		nfiles = 0;

				/*
				 * the pages store copies the part into pages -- from memory, where
				 * the library wrote it there -- and inserts its row; its
				 * directory goes with the writer
				 */
				if (eng->write_part_files != NULL)
					CH_CALL("writing a part", eng->write_part_files(writer, i, &files, &nfiles, CH_ERR));
				if (nfiles > 0)
					ch_pages_store_part_files(rel, table, files, nfiles, &part, b->cid);
				else
					ch_pages_store_part(rel, table, tmp_dir, &part, b->cid);
				ch_pages_attach(table, &part);
			}
			else
			{
				/* the files store logs the part's files before they get the part's name */
				ch_store_part_written(rel, &b->storage, tmp_dir, info.name);
				ch_part_insert(&b->storage, &part, b->cid);
			}
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
	ch_handle_closing(owner, CH_HANDLE_WRITER, writer);
	eng->write_close(writer);
	if (sink)
	{
		b->writer = NULL;
		b->wrows = 0;
		b->wbytes = 0;
	}

	relation_close(rel, NoLock);
	ch_builder_reset(b->builder);
}

/* The buffer of (rel, cid) in this subtransaction: another's flushed first */
static ChBuffer *
buffer_for(Relation rel, CommandId cid)
{
	ChBuffer   *b = find_buffer(RelationGetRelid(rel));
	SubTransactionId subid = GetCurrentSubTransactionId();
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
		b->batch_cxt = AllocSetContextCreate(b->cxt, "ch write buffer batch", ALLOCSET_SMALL_SIZES);
		b->chunk = 1024;
		buffers = lappend(buffers, b);
		MemoryContextSwitchTo(old);
	}
	return b;
}

/* The next row id, from the range taken, a new range taken when it is used up */
static uint64
take_rowid(ChBuffer *b)
{
	uint64		rowid;

	if (b->next_rowid == b->end_rowid)
	{
		uint64		ignored;

		ch_counters_allocate(&b->storage, b->chunk, 0, &b->next_rowid, &ignored);
		b->end_rowid = b->next_rowid + b->chunk;
		b->chunk = Min(b->chunk * 2, (uint64) ch_insert_buffer_rows);
	}
	rowid = b->next_rowid++;
	return rowid;
}

/*
 * At the buffer's limits its rows become a part; more are coming, as many,
 * which the next writer makes room for (VC18)
 */
static void
check_limits(ChBuffer *b)
{
	uint32		nrows = buffered_rows(b);

	if (nrows >= (uint32) ch_insert_buffer_rows ||
		(b->writer != NULL ? b->wbytes : ch_builder_bytes(b->builder)) >= (Size) ch_insert_buffer_bytes * 1024)
	{
		flush(b);
		b->reserve_rows = nrows;
	}
}

/* The buffer of (rel, cid), for a row of tuple_insert: a sink's rows made a part first */
static ChBuffer *
buffer_for_row(Relation rel, CommandId cid)
{
	ChBuffer   *b = buffer_for(rel, cid);

	if (b->writer != NULL)
		flush(b);
	return b;
}

void
ch_buffer_insert(Relation rel, TupleTableSlot *slot, CommandId cid)
{
	ChBuffer   *b = buffer_for_row(rel, cid);
	uint64		rowid = take_rowid(b);

	if (ch_builder_rows(b->builder) == 0)
		b->first_rowid = rowid;
	ch_builder_add(b->builder, slot, rowid);
	ItemPointerSet(&slot->tts_tid, CH_ROWID_TO_BLOCK(rowid), CH_ROWID_TO_OFFSET(rowid));
	slot->tts_tableOid = RelationGetRelid(rel);
	check_limits(b);
}

/*
 * An update's new row (dml.c): its row id taken first, which the old row's
 * tombstone names, then the row buffered under it.  Nothing enters the
 * buffer between the two, so its row ids still ascend.
 */
uint64
ch_buffer_reserve_rowid(Relation rel, CommandId cid)
{
	return take_rowid(buffer_for_row(rel, cid));
}

void
ch_buffer_insert_rowid(Relation rel, TupleTableSlot *slot, CommandId cid, uint64 rowid)
{
	ChBuffer   *b = buffer_for_row(rel, cid);

	if (ch_builder_rows(b->builder) == 0)
		b->first_rowid = rowid;
	ch_builder_add(b->builder, slot, rowid);
	ItemPointerSet(&slot->tts_tid, CH_ROWID_TO_BLOCK(rowid), CH_ROWID_TO_OFFSET(rowid));
	slot->tts_tableOid = RelationGetRelid(rel);
	check_limits(b);
}

/*
 * A batch of rows by column, from vexec's sink (vexec.c), into the buffer of
 * (rel, cid), the batch's row ids taken in rowids[]: straight into the
 * library's writer, opened at the buffer's first batch, where it takes them
 * so (VC18) -- else appended to the builder, as rows are.
 */
void
ch_buffer_put_columns(Relation rel, CommandId cid, const VexecColumn *columns, uint32 nrows, uint64 *rowids)
{
	ChBuffer   *b = buffer_for(rel, cid);
	const ChEngine *eng = ch_engine();
	chte_column_in *in;

	if (eng->write_columns == NULL || !ch_enable_sink_columns)
	{
		if (b->writer != NULL)
			flush(b);
		for (uint32 i = 0; i < nrows; i++)
			rowids[i] = take_rowid(b);
		if (nrows > 0 && ch_builder_rows(b->builder) == 0)
			b->first_rowid = rowids[0];
		ch_builder_add_columns(b->builder, columns, nrows, rowids);
		check_limits(b);
		return;
	}
	if (ch_builder_rows(b->builder) > 0)
		flush(b);
	if (b->writer == NULL)
	{
		/*
		 * the transaction's, not the statement's: a buffer outlives the
		 * statement whose rows it took until it is made a part
		 */
		ResourceOwner save = CurrentResourceOwner;

		if (TopTransactionResourceOwner != NULL)
			CurrentResourceOwner = TopTransactionResourceOwner;
		PG_TRY();
		{
			b->writer_owner = ch_handle_reserve();
			CH_CALL("starting a part", eng->write_begin(b->table->handle, 0, &b->writer, CH_ERR));
			ch_handle_opened(b->writer_owner, CH_HANDLE_WRITER, b->writer);
			if (b->reserve_rows > 0)
				CH_CALL("starting a part", eng->write_reserve(b->writer, b->reserve_rows, CH_ERR));
		}
		PG_FINALLY();
		{
			CurrentResourceOwner = save;
		}
		PG_END_TRY();
		b->wrows = 0;
		b->wbytes = 0;
	}
	for (uint32 i = 0; i < nrows; i++)
		rowids[i] = take_rowid(b);
	if (nrows > 0 && b->wrows == 0)
		b->first_rowid = rowids[0];
	in = MemoryContextAlloc(b->batch_cxt, sizeof(chte_column_in) * ch_builder_ncolumns(b->builder));
	b->wbytes += ch_builder_columns_in(b->builder, columns, nrows, rowids, b->batch_cxt, in);
	CH_CALL("writing a part", eng->write_columns(b->writer, nrows, in, CH_ERR));
	MemoryContextReset(b->batch_cxt);
	if (b->wrows + nrows > b->wcap)
	{
		uint32		cap = Max(b->wcap * 2, Max(b->wrows + nrows, 1024));

		b->wrowids = b->wrowids ? repalloc_huge(b->wrowids, sizeof(uint64) * cap) :
			MemoryContextAllocHuge(b->cxt, sizeof(uint64) * cap);
		b->wcap = cap;
	}
	memcpy(b->wrowids + b->wrows, rowids, sizeof(uint64) * nrows);
	b->wrows += nrows;
	check_limits(b);
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

	if (b == NULL || buffered_rows(b) == 0)
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

	if (b == NULL || (nrows = buffered_rows(b)) == 0)
		return false;
	if (snapshot && IsMVCCSnapshot(snapshot) && snapshot->curcid <= b->cid)
		return false;
	first = b->first_rowid;
	if (rowid < first || rowid > buffer_rowid(b, nrows - 1))
		return false;
	/* row ids of a buffer ascend with its rows, gaps allowed between ranges */
	{
		uint32		lo = 0;
		uint32		hi = nrows;

		while (lo < hi)
		{
			uint32		mid = lo + (hi - lo) / 2;
			uint64		v = buffer_rowid(b, mid);

			if (v < rowid)
				lo = mid + 1;
			else
				hi = mid;
		}
		if (lo < nrows && buffer_rowid(b, lo) == rowid)
		{
			if (b->writer != NULL)
			{
				/* VC18: the row from the writer's columns */
				int			n = ch_builder_ncolumns(b->builder);
				chte_vector *vectors = palloc(sizeof(chte_vector) * n);

				for (int c = 0; c < n; c++)
					CH_CALL("reading a buffered row",
							ch_engine()->write_vector(b->writer, (uint32) c, &vectors[c], CH_ERR));
				ch_builder_fetch_vectors(b->builder, vectors, lo, slot);
				pfree(vectors);
			}
			else
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
