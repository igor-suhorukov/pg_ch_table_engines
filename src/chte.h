/*-------------------------------------------------------------------------
 *
 * chte.h
 *	  ch_table_engines: ClickHouse's MergeTree family as PostgreSQL 19 table
 *	  access methods, over the engine library (ch_table_engines.md).
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#ifndef CHTE_H
#define CHTE_H

#include "access/skey.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "datatype/timestamp.h"
#include "nodes/parsenodes.h"
#include "storage/lockdefs.h"
#include "storage/relfilelocator.h"
#include "utils/rel.h"

#include "engine.h"

/* The engines, one access method each (§2.4) */
typedef enum ChEngineKind
{
	CH_ENGINE_MERGETREE,
	CH_ENGINE_REPLACING,
	CH_ENGINE_SUMMING,
	CH_ENGINE_AGGREGATING,
	CH_ENGINE_COLLAPSING,
	CH_ENGINE_VERSIONED_COLLAPSING,
	CH_ENGINE_COALESCING,
	CH_ENGINE_COUNT
} ChEngineKind;

typedef enum ChStorage
{
	CH_STORAGE_FILES,
	CH_STORAGE_PAGES
} ChStorage;

/* A table's options, from pg_class.reloptions (§2.4) */
typedef struct ChOptions
{
	ChStorage	storage;
	char	   *order_by;		/* column names, comma-separated; NULL: none */
	char	   *primary_key;	/* NULL: order_by */
	int			index_granularity;
	char	   *compression;	/* "lz4", "zstd", "zstd(N)", "lz4hc", "none" */
	char	   *settings;		/* further MergeTree settings, or NULL */
	bool		merges;
	/* the engines' own */
	char	   *ver;
	char	   *is_deleted;
	char	   *sign;
	char	   *sum_columns;
	/* columns kept as ClickHouse's LowCardinality: a dictionary a part's granules share */
	char	   *low_cardinality;
} ChOptions;

/*
 * A relation's storage, as the catalogs and the files store name it: its
 * locator, and for a temporary relation its backend, since a temporary
 * relfilenumber is unique only with it.
 */
typedef struct ChStorageId
{
	RelFileLocator locator;
	int			backend;		/* ProcNumber, or -1 */
	char		persistence;	/* RELPERSISTENCE_* */
} ChStorageId;

/* The hidden row id and the projection that finds a row by it (Appendix E) */
#define CH_ROWID_COLUMN "_pg_rowid"
#define CH_ROWID_PROJECTION "_pg_rowid_index"

/* TIDs from row ids (§2.6): 291 = MaxHeapTuplesPerPage, a TID bitmap's limit */
#define CH_ROWS_PER_BLOCK 291
#define CH_ROWID_TO_BLOCK(id) ((BlockNumber) ((id) / CH_ROWS_PER_BLOCK))
#define CH_ROWID_TO_OFFSET(id) ((OffsetNumber) ((id) % CH_ROWS_PER_BLOCK + 1))
#define CH_TID_TO_ROWID(tid) \
	((uint64) ItemPointerGetBlockNumberNoCheck(tid) * CH_ROWS_PER_BLOCK + \
	 ItemPointerGetOffsetNumberNoCheck(tid) - 1)
#define CH_MAX_ROWID ((uint64) MaxBlockNumber * CH_ROWS_PER_BLOCK)

/* module.c */
extern bool ch_enable_chscan;
extern int	ch_insert_buffer_rows;
extern int	ch_insert_buffer_bytes;
extern int	ch_default_storage;
extern bool ch_preloaded;

/* am.c */
extern PGDLLIMPORT TupleTableSlotOps ch_slot_ops;
extern const TableAmRoutine *ch_am_routine(ChEngineKind kind);
extern bool ch_is_engine_am(Oid amoid, ChEngineKind *kind);
extern bool ch_is_engine_rel(Relation rel, ChEngineKind *kind);
extern const char *ch_engine_name(ChEngineKind kind);
extern bool ch_am_name_kind(const char *amname, ChEngineKind *kind);
extern void ch_relation_dropped(Oid relid);

/* options.c */
extern void ch_options_init(void);
extern bool ch_is_option_name(const char *name);
extern void ch_options_validate(List *defs, ChEngineKind kind);
extern void ch_options_parse(Datum reloptions, ChOptions *opts);
extern void ch_relation_options(Relation rel, ChOptions *opts);
extern void ch_options_bind(Oid relid, List *defs);

/* types.c */
typedef enum ChColumnKind
{
	CH_COL_FIXED,				/* by value or fixed-length by reference: its bytes */
	CH_COL_VARLENA,				/* a varlena's payload as String */
	CH_COL_CSTRING,				/* a cstring (typlen -2) as String */
	CH_COL_NUMERIC,				/* numeric(p, s) as Decimal(p, s) */
	CH_COL_SIGN,				/* an integer column a collapsing engine's sign: Int8 */
	CH_COL_DROPPED
} ChColumnKind;

typedef struct ChColumn
{
	ChColumnKind kind;
	Oid			typid;
	int32		typmod;
	int16		typlen;
	bool		typbyval;
	int			width;			/* bytes of a value in the library's column */
	int			precision;		/* CH_COL_NUMERIC */
	int			scale;
	bool		low_cardinality;	/* LowCardinality(chtype), option low_cardinality */
	char		chtype[64];		/* ClickHouse's type, without Nullable and LowCardinality */
} ChColumn;

extern void ch_column_for_attribute(Form_pg_attribute att, ChColumn *col);
extern void ch_column_for_table(Relation rel, const ChOptions *opts, Form_pg_attribute att, ChColumn *col);
extern void ch_low_cardinality_check(Relation rel, const char *list);

/* table.c: a relation's table in the engine library */
typedef struct ChTable
{
	Oid			relid;
	ChStorageId storage;
	ChEngineKind kind;
	ChOptions	options;
	char	   *relative_path;	/* of the parts' directory, under DataDir */
	char	   *definition;		/* the CREATE TABLE the library compiled */
	chte_table *handle;
	int			natts;			/* the relation's attributes, dropped included */
	ChColumn   *columns;		/* per attribute */
	char	   *all_columns;	/* "c1, c2, ..., _pg_rowid": what a full scan reads */
	MemoryContext private_cxt;	/* a private table's (ch_table_private) */
	MemoryContext own_cxt;		/* a cached table's: where it lives */
	chte_pages_ops pages_ops;	/* the pages store's read_page, its ctx this table */
	struct HTAB *attached;		/* the pages store's: parts whose files the library has */
	int			refcount;		/* scans and write buffers using it */
	bool		stale;			/* out of the cache, freed with its last pin */
} ChTable;

extern void ch_storage_id(Relation rel, const RelFileLocator *locator, ChStorageId *id);
extern char *ch_storage_path(const ChStorageId *id, bool relative);
/*
 * A relation's cached table goes at the next invalidation of the relation,
 * which any lock taken or catalog read may accept: a caller that holds it
 * across one pins it (ch_table_pin(); every pin goes at the transaction's end).
 */
extern ChTable *ch_table(Relation rel);
extern ChTable *ch_table_for_storage(Relation rel, const ChStorageId *id);
extern char *ch_table_definition(Relation rel, const ChOptions *opts, ChEngineKind kind);
extern char *ch_table_definition_for(Relation rel, TupleDesc desc, const ChOptions *opts, ChEngineKind kind);
extern ChTable *ch_table_private(Relation rel, const ChStorageId *id, TupleDesc desc);
extern void ch_table_private_close(ChTable *t);
extern bool ch_desc_matches(Relation rel, TupleDesc desc);
extern bool ch_is_engine_column(Relation rel, const ChOptions *opts, const char *name);
extern bool ch_is_sign_column(const ChOptions *opts, const char *name);
extern void ch_table_forget(Oid relid);
extern void ch_table_pin(ChTable *t);
extern void ch_table_unpin(ChTable *t);
extern void ch_tables_xact_end(void);

/* catalog.c: ch.tables and ch.parts */
typedef struct ChPart
{
	char	   *name;
	int64		rows;
	int64		bytes;
	int64		min_rowid;
	int64		max_rowid;
	int32		level;
	int32		marks;			/* granules */
	bytea	   *manifest;		/* the pages store's: the files' pages, a TOAST pointer where
								 * ch_parts_to_scan() left it unread (ch_part_manifest()); NULL
								 * in the files store */
	TimestampTz created;		/* when it was written; 0 at an insert: now */
} ChPart;

extern Oid	ch_catalog_relid(const char *name, bool unlogged);
extern Relation ch_catalog_open(const char *name, const ChStorageId *id, LOCKMODE lockmode);
extern Oid	ch_catalog_index(Relation catalog);
extern int	ch_catalog_storage_keys(const ChStorageId *id, ScanKeyData *keys);
extern void ch_catalog_insert(Relation catalog, HeapTuple tuple, CommandId cid);
extern List *ch_parts_visible(const ChStorageId *id, Snapshot snapshot);
extern List *ch_parts_to_scan(const ChStorageId *id, Snapshot snapshot);
extern bytea *ch_part_manifest(const ChPart *part);
extern void ch_part_insert(const ChStorageId *id, const ChPart *part, CommandId cid);
extern List *ch_parts_manifests(const ChStorageId *id);
extern List *ch_parts_names(const ChStorageId *id);
extern void ch_part_delete(const ChStorageId *id, const char *name);
extern void ch_parts_delete_all(const ChStorageId *id);
extern void ch_table_row_create(Relation rel, const ChStorageId *id, ChEngineKind kind, ChStorage storage);
extern void ch_table_row_delete(const ChStorageId *id);
extern void ch_table_row_copy(Relation rel, const ChStorageId *from, const ChStorageId *to);
extern void ch_counters_allocate(const ChStorageId *id, uint64 nrowids, uint64 nblocks,
								 uint64 *first_rowid, uint64 *first_block);
extern void ch_counters_xact_callback(XactEvent event);
extern void ch_parts_conflict_out(Relation rel, const ChStorageId *id, Snapshot snapshot);

/* convert.c: datums and the library's vectors */
typedef struct ChVectorBuilder ChVectorBuilder;
extern ChVectorBuilder *ch_builder_create(ChTable *table, MemoryContext cxt);
extern void ch_builder_add(ChVectorBuilder *b, TupleTableSlot *slot, uint64 rowid);
extern uint32 ch_builder_rows(ChVectorBuilder *b);
extern Size ch_builder_bytes(ChVectorBuilder *b);
extern chte_vector *ch_builder_vectors(ChVectorBuilder *b);
extern uint64 ch_builder_rowid(ChVectorBuilder *b, uint32 row);
extern bool ch_builder_fetch(ChVectorBuilder *b, uint32 row, TupleTableSlot *slot);
extern void ch_builder_reset(ChVectorBuilder *b);
extern uint64 ch_dictionary_index(const chte_dictionary *d, uint32 row);
extern void ch_dictionary_to_datum(const ChColumn *col, const chte_dictionary *d, uint32 row,
								   Datum *value, bool *isnull);
extern void ch_vector_to_datum(const ChColumn *col, const chte_vector *v, uint32 row,
							   Datum *value, bool *isnull, char **arena);
struct VexecColumn;
extern void ch_builder_add_columns(ChVectorBuilder *b, const struct VexecColumn *columns, uint32 nrows,
								   const uint64 *rowids);

/* buffer.c: the write buffer (§2.7) */
extern void ch_buffer_insert(Relation rel, TupleTableSlot *slot, CommandId cid);
extern uint64 ch_buffer_reserve_rowid(Relation rel, CommandId cid);
extern void ch_buffer_insert_rowid(Relation rel, TupleTableSlot *slot, CommandId cid, uint64 rowid);
extern ChVectorBuilder *ch_buffer_batch_begin(Relation rel, CommandId cid, uint32 nrows, uint64 *rowids);
extern void ch_buffer_batch_done(Relation rel);
extern void ch_buffer_flush_relation(Relation rel);
extern void ch_buffer_flush_for_scan(Relation rel, Snapshot snapshot);
extern void ch_buffer_flush_all(void);
extern bool ch_buffer_fetch(Relation rel, uint64 rowid, Snapshot snapshot, TupleTableSlot *slot);
extern void ch_buffer_xact_callback(XactEvent event);
extern void ch_buffer_subxact_callback(SubXactEvent event, SubTransactionId mySubid);
extern void ch_buffer_forget_relation(Oid relid);

/* store.c: the files store, its lifecycle and its WAL (§2.9) */
extern void ch_store_create(const ChStorageId *id);
extern void ch_store_drop_at_commit(const ChStorageId *id);
extern void ch_store_drop_at_abort(const ChStorageId *id);
extern void ch_store_part_written(Relation rel, const ChStorageId *id, const char *part_dir,
								  const char *part_name);
extern void ch_store_xact_callback(XactEvent event);
extern void ch_store_subxact_callback(SubXactEvent event, SubTransactionId mySubid);
extern void ch_store_register_rmgr(void);
extern void ch_store_remove_now(const ChStorageId *id, const char *part, bool log);
extern void ch_store_copy_part(Relation rel, const ChStorageId *from, const ChStorageId *to, const char *part);
extern int	ch_store_sweep(Relation rel, const ChStorageId *id);

/* pages.c: the pages store */
extern void ch_pages_ops(ChTable *t, chte_pages_ops *ops);
extern void ch_pages_attach(ChTable *t, const ChPart *part);
extern bytea *ch_pages_store_part(Relation rel, ChTable *t, const char *part_dir, ChPart *part, CommandId cid);
extern void ch_pages_raise_pending(void);

/* scan.c */
extern TableScanDesc ch_scan_begin(Relation rel, Snapshot snapshot, int nkeys, ScanKeyData *key,
								   ParallelTableScanDesc pscan, uint32 flags);
extern void ch_scan_end(TableScanDesc scan);
extern void ch_scan_rescan(TableScanDesc scan, ScanKeyData *key, bool set_params,
						   bool allow_strat, bool allow_sync, bool allow_pagemode);
extern bool ch_scan_getnextslot(TableScanDesc scan, ScanDirection direction, TupleTableSlot *slot);
extern Size ch_parallelscan_estimate(Relation rel);
extern Size ch_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan);
extern void ch_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan);
extern bool ch_fetch_row(Relation rel, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot);
extern void ch_scan_forget(void *scan);
extern Snapshot ch_parts_snapshot(Relation rel, Snapshot snapshot, SnapshotData *buf);
extern bool ch_scan_analyze_next_block(TableScanDesc scan, struct ReadStream *stream);
extern bool ch_scan_analyze_next_tuple(TableScanDesc scan, double *liverows, double *deadrows,
									   TupleTableSlot *slot);
extern bool ch_scan_bitmap_next_tuple(TableScanDesc scan, TupleTableSlot *slot, bool *recheck,
									  uint64 *lossy_pages, uint64 *exact_pages);

/* dml.c: DELETE, UPDATE and row locks (§2.10) */
extern TM_Result ch_tuple_delete(Relation rel, ItemPointer tid, CommandId cid, uint32 options, Snapshot snapshot,
								 Snapshot crosscheck, bool wait, TM_FailureData *tmfd);
extern TM_Result ch_tuple_update(Relation rel, ItemPointer otid, TupleTableSlot *slot, CommandId cid,
								 uint32 options, Snapshot snapshot, Snapshot crosscheck, bool wait,
								 TM_FailureData *tmfd, LockTupleMode *lockmode,
								 TU_UpdateIndexes *update_indexes);
extern TM_Result ch_tuple_lock(Relation rel, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot,
							   CommandId cid, LockTupleMode mode, LockWaitPolicy wait_policy, uint8 flags,
							   TM_FailureData *tmfd);
extern void ch_tuple_get_latest_tid(TableScanDesc scan, ItemPointer tid);

/* vacuum.c: what deletions leave behind (§2.10, §2.13) */
struct VacuumParams;
extern void ch_vacuum_indexes(Relation rel, const struct VacuumParams *params, BufferAccessStrategy strategy);
extern int64 ch_fold_deletions(Relation rel);

/* merge.c (§2.11) */
typedef enum ChMergeMode
{
	CH_MERGE_BACKGROUND,		/* a merge worker's: what the selector chooses now, if anything */
	CH_MERGE_OPTIMIZE,			/* ch.optimize(): the selector asked aggressively */
	CH_MERGE_FINAL				/* ch.optimize(rel, true): every part into one */
} ChMergeMode;

extern int	ch_merge_parts(Relation rel, ChMergeMode mode);

/* worker.c */
extern int	ch_max_merge_workers;
extern int	ch_merge_naptime;
extern void ch_merge_launcher_register(void);
extern void ch_merge_workers_stop(Oid dboid);

/* index.c: indexes (§2.10) */
extern struct IndexFetchTableData *ch_index_fetch_begin(Relation rel, uint32 flags);
extern void ch_index_fetch_reset(struct IndexFetchTableData *data);
extern void ch_index_fetch_end(struct IndexFetchTableData *data);
extern bool ch_index_fetch_tuple(struct IndexFetchTableData *scan, ItemPointer tid, Snapshot snapshot,
								 TupleTableSlot *slot, bool *call_again, bool *all_dead);
extern bool ch_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot, Snapshot snapshot);
extern TransactionId ch_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate);
extern double ch_index_build_range_scan(Relation table_rel, Relation index_rel, struct IndexInfo *index_info,
										bool allow_sync, bool anyvisible, bool progress,
										BlockNumber start_blockno, BlockNumber numblocks,
										IndexBuildCallback callback, void *callback_state, TableScanDesc scan);
extern void ch_index_validate_scan(Relation table_rel, Relation index_rel, struct IndexInfo *index_info,
								   Snapshot snapshot, struct ValidateIndexState *state);

/* chscan.c: ChScan (§2.8) */
extern void ch_chscan_init(void);

/* vexec.c: vexec's batch source and sink (§3) */
extern void ch_vexec_register(void);

#endif							/* CHTE_H */
