/*
 * chte_engine.h — the C API of libchte_engine.so, the core of ClickHouse v26.8.8.8-lts
 * compiled into one shared library (ch_table_engines.md §2.2).
 *
 * The extension dlopen()s the library on first use in a backend, or in the postmaster as
 * it preloads -- mapped there, its static initializers run, chte_preload called, chte_init
 * never -- and talks to it only through these functions: no C++ type, exception or thread crosses
 * this boundary.  Every call runs on the calling thread.  A call that fails returns
 * nonzero and fills *error; the caller turns that into ereport(ERROR) after the call has
 * returned, so ClickHouse's stack is always unwound by ClickHouse.
 *
 * Tables are described in ClickHouse's own syntax and compiled by ClickHouse's parsers.
 * Parts are named the way ClickHouse names them (<partition>_<min>_<max>_<level>); which
 * parts exist and which a snapshot sees is PostgreSQL's business, not the library's.
 */
#ifndef CHTE_ENGINE_H
#define CHTE_ENGINE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define CHTE_ABI_VERSION 4

/* The library is built -fvisibility=hidden; these are the only symbols it exports. */
#if defined(__GNUC__)
#define CHTE_API __attribute__((visibility("default")))
#else
#define CHTE_API
#endif

typedef struct chte_table chte_table;
typedef struct chte_writer chte_writer;
typedef struct chte_reader chte_reader;
typedef struct chte_filter chte_filter;
typedef struct chte_batch chte_batch;

typedef struct chte_error
{
	int			code;			/* ClickHouse ErrorCodes value; 0 = no error */
	char		message[2048];
} chte_error;

typedef struct chte_callbacks
{
	void	   *arg;
	/* Polled between blocks; nonzero stops the call with QUERY_WAS_CANCELLED. */
	int			(*interrupted) (void *arg);
	/* ClickHouse's log messages at or above the configured level; caller's thread only. */
	void		(*log) (void *arg, int level, const char *message);
} chte_callbacks;

typedef struct chte_init_options
{
	uint32_t	abi_version;	/* CHTE_ABI_VERSION */
	const char *data_path;		/* root of the parts' directories, ends with '/' */
	const char *work_path;		/* ClickHouse's own directory, ends with '/'; NULL: data_path */
	const char *tmp_path;		/* the process's own directory of temporary files -- a vertical
								 * merge's rows' sources -- which it clears; ends with '/' */
	const char *log_level;		/* "error", "warning", "information", "debug", "trace" */
	const char *settings;		/* ClickHouse settings "name = value, ...", or NULL */
	chte_callbacks callbacks;
} chte_init_options;

/* Initializes the process-wide ClickHouse context; later calls are no-ops. */
CHTE_API int			chte_init(const chte_init_options *options, chte_error *error);

/*
 * In the postmaster, after its dlopen(): builds what every process's chte_init would build
 * alike and that starts no thread -- ClickHouse's date tables of its default time zone, UTC
 * -- so that each process forked after it inherits them.  Optional: a library built before
 * it builds them in each process's chte_init, which still sets the zone where it was not.
 */
CHTE_API int			chte_preload(chte_error *error);

/*
 * Shuts the process's ClickHouse down: its background executors' idle threads end, so
 * that exit() can join them.  Idempotent; registered with atexit() by chte_init as a
 * fallback, and called by the extension from on_proc_exit, before static destructors run.
 */
CHTE_API void		chte_shutdown(void);

/* Number of threads of the process, for the tests that check no thread is started. */
CHTE_API int			chte_thread_count(void);

/*
 * ClickHouse's functions in the process (VC12).  chte_init registers those the filters
 * use; a table whose definition names others, a write, a merge and a rewrite register the
 * rest, and the aggregate functions, as they first need them -- or the caller, at once.
 * chte_function_count counts those registered, aggregate functions included, for the
 * tests.  Both are optional: a library built before them registers every function in
 * chte_init.
 */
CHTE_API int			chte_register_all_functions(chte_error *error);
CHTE_API int			chte_function_count(void);

/*
 * A table is a CREATE TABLE statement in ClickHouse's syntax — columns with codecs,
 * indexes and projections, then ENGINE = <MergeTree family>(...) with PARTITION BY,
 * ORDER BY, PRIMARY KEY, SAMPLE BY and SETTINGS — so the same text also works in
 * clickhouse-local.  The table's name in the statement is ignored.
 */
/*
 * The pages store (ch_table_engines.md §2.9): a committed part's files live in
 * pages of the relation, page_payload bytes of each page after its header, a
 * file in consecutive pages.  The library reads them a page at a time through
 * read_page, on the calling thread; a part is written as in the files store,
 * then copied into pages by the caller, which hands the library its manifest.
 */
typedef struct chte_pages_ops
{
	void	   *ctx;
	uint32_t	page_payload;
	/* copies page `block`'s payload to dst; nonzero on failure, the caller's error kept by it */
	int			(*read_page) (void *ctx, uint32_t block, void *dst);
} chte_pages_ops;

typedef struct chte_page_file
{
	const char *path;			/* below the part's directory: "columns.txt", "p.proj/data.bin" */
	uint64_t	size;
	uint32_t	first_block;
	uint32_t	nblocks;
} chte_page_file;

typedef struct chte_table_def
{
	const char *name;			/* for messages */
	const char *relative_path;	/* directory of the parts under data_path, ends with '/' */
	const char *create_query;
	const chte_pages_ops *pages;	/* the pages store's, or NULL for the files store */
} chte_table_def;

CHTE_API int			chte_table_open(const chte_table_def *def, chte_table **table, chte_error *error);
CHTE_API void		chte_table_close(chte_table *table);

/*
 * One column of a batch, in table-definition order.  Fixed-width values are packed
 * native-endian in ClickHouse's in-memory representation; strings are bytes plus
 * nrows + 1 offsets.
 */
typedef struct chte_vector
{
	const void *values;
	const uint64_t *offsets;	/* strings only, else NULL */
	const uint8_t *nulls;		/* nrows bytes, 1 = NULL; NULL when there are none */
} chte_vector;

typedef struct chte_part_info
{
	char		name[128];
	char		partition_id[64];
	uint64_t	min_block;
	uint64_t	max_block;
	uint32_t	level;
	uint64_t	rows;
	uint64_t	bytes_on_disk;
	uint64_t	marks;
	char		temp_dir[160];	/* where it is until committed */
} chte_part_info;

/*
 * Writing: rows of one or more batches become parts numbered block_number, one per
 * partition -- 0 numbers them later, by chte_write_block (VC18).  finish writes and syncs
 * them under temporary names; commit gives them their final names; abort (or close without
 * commit) removes them.
 */
CHTE_API int			chte_write_begin(chte_table *table, uint64_t block_number, chte_writer **writer,
							 chte_error *error);
CHTE_API int			chte_write_batch(chte_writer *writer, uint32_t nrows, const chte_vector *columns,
							 chte_error *error);
CHTE_API int			chte_write_finish(chte_writer *writer, size_t *nparts, chte_error *error);
CHTE_API int			chte_write_part(chte_writer *writer, size_t i, chte_part_info *part, chte_error *error);
CHTE_API int			chte_write_commit(chte_writer *writer, chte_error *error);
CHTE_API void		chte_write_close(chte_writer *writer);

/*
 * A writer's rows from vexec's batches (ch_table_engines.md §3, VC18), each column read once
 * into ClickHouse's own: a column of nrows rows, in table-definition order as chte_write_batch
 * takes them, is fixed-width values `width` bytes wide, `stride` bytes apart -- 8-byte scaled
 * integers narrowed where the column's Decimal is 4 bytes -- or a string's bytes and its
 * nrows + 1 offsets into them, int32 (offsets32) or 64-bit (offsets64), from any start;
 * validity a bit a row, 1 = a value, least significant first, or NULL for none.  A NULL row's
 * value is the type's default, as chte_write_batch makes it.
 */
typedef struct chte_column_in
{
	const void *values;
	const int32_t *offsets32;
	const uint64_t *offsets64;
	const uint64_t *validity;
	uint32_t	width;
	uint32_t	stride;
} chte_column_in;

CHTE_API int			chte_write_columns(chte_writer *writer, uint32_t nrows, const chte_column_in *columns,
								   chte_error *error);

/*
 * The block number of a writer begun with 0, before chte_write_finish: a writer whose rows
 * arrive before its parts are numbered (VC18).
 */
CHTE_API int			chte_write_block(chte_writer *writer, uint64_t block_number, chte_error *error);

/*
 * Room in a writer's columns for nrows rows, before its first: a string's bytes as many a row
 * as the table's last part an insert made had -- the caller asks where more rows are coming,
 * as when a part was made at its buffer's limit, so that ClickHouse's columns are not grown
 * again and again as the batches arrive (VC18).
 */
CHTE_API int			chte_write_reserve(chte_writer *writer, uint64_t nrows, chte_error *error);

/*
 * Column i of a writer's rows so far, in chte_vector's layout, valid until its next write or
 * this call again: a row not yet in a part, fetched by its TID (VC18).
 */
CHTE_API int			chte_write_vector(chte_writer *writer, uint32_t column, chte_vector *vector,
								  chte_error *error);

/*
 * The pages store's parts written into memory, not temporary files (VC18): before
 * chte_write_finish, then the files of part i, each its path below the part's directory, its
 * bytes and size, valid until the writer is closed, for the caller to copy into its pages; a
 * part in memory is never committed.  The six calls are optional: a library built before
 * them lacks them.
 */
typedef struct chte_part_file
{
	const char *path;
	const void *data;
	uint64_t	size;
} chte_part_file;

CHTE_API int			chte_write_in_memory(chte_writer *writer, chte_error *error);
CHTE_API int			chte_write_part_files(chte_writer *writer, size_t i, const chte_part_file **files,
									  size_t *nfiles, chte_error *error);

/*
 * Filters (ch_table_engines.md §2.8): conditions on a table's columns that
 * ClickHouse's own analysis -- the primary key's KeyCondition and the parts'
 * statistics -- prunes parts and granules by.  A filter only prunes: a part or a
 * granule it keeps may hold rows it would reject, and the caller evaluates its
 * own conditions on every row read.  Nodes are in prefix order: AND and OR are
 * followed by their nargs subtrees, NOT by one; the others are leaves on one
 * column, comparing it with constants of the column's own type (without
 * Nullable), laid out as chte_vector lays out values, with no NULL.
 */
typedef enum chte_filter_op
{
	CHTE_FILTER_AND,
	CHTE_FILTER_OR,
	CHTE_FILTER_NOT,
	CHTE_FILTER_EQ,				/* column = value */
	CHTE_FILTER_NE,
	CHTE_FILTER_LT,
	CHTE_FILTER_LE,
	CHTE_FILTER_GT,
	CHTE_FILTER_GE,
	CHTE_FILTER_IN,				/* column = one of nvalues values */
	CHTE_FILTER_IS_NULL,
	CHTE_FILTER_IS_NOT_NULL
} chte_filter_op;

typedef struct chte_filter_node
{
	int32_t		op;				/* chte_filter_op */
	uint32_t	nargs;			/* AND, OR: the subtrees that follow */
	const char *column;			/* leaves: the column's name */
	uint32_t	nvalues;		/* comparisons 1, IN at least 1, IS [NOT] NULL 0 */
	chte_vector values;
} chte_filter_node;

CHTE_API int			chte_filter_create(chte_table *table, const chte_filter_node *nodes, size_t nnodes,
							   chte_filter **filter, chte_error *error);
CHTE_API void		chte_filter_free(chte_filter *filter);

/* A range of a part's granules: marks [begin, end) */
typedef struct chte_mark_range
{
	uint32_t	begin;
	uint32_t	end;
} chte_mark_range;

/*
 * The granules of marks [begin, end) of a part that a filter keeps (filter NULL:
 * all of them; end 0: the part's last mark), as at most max_ranges ranges into
 * ranges[], and the rows they hold.  *nranges larger than max_ranges asks for a
 * call with room for that many; none when the part's statistics or its key rule
 * every row out.
 */
CHTE_API int			chte_part_select(chte_table *table, const char *part_name, chte_filter *filter,
							 uint32_t begin, uint32_t end, chte_mark_range *ranges, size_t max_ranges,
							 size_t *nranges, uint64_t *rows, chte_error *error);

/* The compressed and uncompressed bytes of a part's named columns (comma-separated) */
CHTE_API int			chte_part_column_bytes(chte_table *table, const char *part_name, const char *columns,
								   uint64_t *compressed, uint64_t *uncompressed, chte_error *error);

/*
 * The statistics ClickHouse keeps of a part's column ("basic", exact): its rows,
 * NULLs, and least and greatest values that are not NULL, numeric and temporal
 * types only.  min and max hold one value each, in the column's own type, valid
 * until the next call on the table.
 */
typedef struct chte_column_stats
{
	int32_t		has_stats;
	int32_t		has_null_count;
	int32_t		has_minmax;		/* none: every value NULL, or no min/max kept */
	uint64_t	rows;
	uint64_t	null_count;
	chte_vector min;
	chte_vector max;
} chte_column_stats;

CHTE_API int			chte_part_stats(chte_table *table, const char *part_name, const char *column,
							chte_column_stats *stats, chte_error *error);

/*
 * A part's summary (ch_table_engines.md §3, VC20's B6): what chte_part_column_bytes(),
 * chte_part_stats() and a filter's pruning by statistics read of a loaded part -- each
 * column's compressed and uncompressed bytes and its statistics -- in bytes of the
 * library's own, which the caller keeps with the part and hands back, with its name,
 * to the calls below, which load no part.  chte_write_part_summary makes them for a
 * writer's part i, valid until the writer is closed.  *found 0: the bytes are of
 * another format, or the part was written without a column asked for -- the loaded
 * part's to tell.
 */
CHTE_API int			chte_write_part_summary(chte_writer *writer, size_t i, const void **summary, size_t *size,
									chte_error *error);
CHTE_API int			chte_summary_column_bytes(chte_table *table, const char *part_name, const void *summary,
									  size_t size, const char *columns, uint64_t *compressed,
									  uint64_t *uncompressed, int *found, chte_error *error);
CHTE_API int			chte_summary_stats(chte_table *table, const char *part_name, const void *summary,
							   size_t size, const char *column, chte_column_stats *stats, int *found,
							   chte_error *error);

/*
 * What chte_part_select() would keep of a part, as far as the filter and the part's
 * summary tell without the part (VC20's B7): CHTE_SELECT_NONE, no row -- the filter's
 * key is always false, or the summary's statistics rule every row out;
 * CHTE_SELECT_ALL, every granule -- the statistics leave a row, and the filter's key
 * condition narrows no part's granules; CHTE_SELECT_RANGES, chte_part_select()'s to
 * tell.  A summary NULL, or of another format, prunes nothing by statistics.
 */
#define CHTE_SELECT_NONE	0
#define CHTE_SELECT_ALL		1
#define CHTE_SELECT_RANGES	2
CHTE_API int			chte_filter_summary_select(chte_filter *filter, const char *part_name, const void *summary,
									   size_t size, int *verdict, chte_error *error);

/*
 * Reading one part: columns is a comma-separated list of names (virtual columns such as
 * _part_offset allowed); ranges, when nranges is not 0, the granules to read, in order.
 * next returns 0 rows at the end; vectors stay valid until the following next or end,
 * or, for a batch retained, until it is released.
 */
CHTE_API int			chte_read_begin(chte_table *table, const char *part_name, const char *columns,
							const chte_mark_range *ranges, size_t nranges,
							chte_reader **reader, chte_error *error);
CHTE_API int			chte_read_next(chte_reader *reader, uint32_t *nrows, chte_error *error);
CHTE_API int			chte_read_vector(chte_reader *reader, uint32_t column, chte_vector *vector,
							 chte_error *error);
CHTE_API void		chte_read_end(chte_reader *reader);

/*
 * Whether the part has a column it was asked for, or was written before the
 * column was added (the library then reads its default); virtual columns are present.
 */
CHTE_API int			chte_read_present(chte_reader *reader, uint32_t column);

/* The current batch's columns kept past the next chte_read_next(), until released */
CHTE_API chte_batch *chte_read_retain(chte_reader *reader);
CHTE_API void		chte_batch_release(chte_batch *batch);

/*
 * A LowCardinality column of a batch as ClickHouse keeps it (ch_table_engines.md §3,
 * VC5): its dictionary's nvalues values, of the column's type without LowCardinality
 * and Nullable, with no NULL, and the rows' indexes into them, index_width bytes each
 * (1, 2, 4 or 8), native-endian.  Where the type is LowCardinality(Nullable(T)), index
 * null_index is NULL, its value the type's default.  id names the dictionary: the
 * batches of a reader's column with the same id share it -- ClickHouse's dictionary of
 * a part's granules -- and the library never gives one id to two dictionaries in a
 * process.  encoded is 0 where the column of this batch is not LowCardinality, and is
 * then read by chte_read_vector.
 */
typedef struct chte_dictionary
{
	int32_t		encoded;
	int32_t		has_null;
	uint64_t	null_index;
	chte_vector values;
	uint64_t	nvalues;
	const void *indexes;
	uint32_t	index_width;
	uint64_t	id;
} chte_dictionary;

/*
 * Before the first chte_read_next: the columns, keep[c] nonzero, that the reader's
 * batches leave LowCardinality, for chte_read_dictionary; every other column is
 * expanded into its values, as without the call.  Both calls are optional: a library
 * built before them lacks them, and its columns are always expanded.
 */
CHTE_API int			chte_read_keep_dictionaries(chte_reader *reader, const uint8_t *keep, size_t ncolumns,
										chte_error *error);
CHTE_API int			chte_read_dictionary(chte_reader *reader, uint32_t column, chte_dictionary *dictionary,
								 chte_error *error);

/*
 * Reading in steps (ch_table_engines.md §3, VC2): a reader of a part's columns as
 * chte_read_begin opens one, each column of a step, steps[c] -- 0 the first, one byte a
 * column, every step up to the greatest having one.  chte_read_next reads the first step's
 * columns of the next granule, a granule a call, and chte_read_step a later step's columns
 * of the granule it read last, once, before the next chte_read_next: a step not asked for
 * leaves that granule's columns of it undecoded, as ClickHouse's PREWHERE leaves a granule
 * no row of passed.  A column of a later step is read by chte_read_vector or
 * chte_read_dictionary only once its step is; chte_read_retain keeps the steps read by
 * then.  Both calls are optional: a library built before them lacks them.
 */
CHTE_API int			chte_read_begin_steps(chte_table *table, const char *part_name, const char *columns,
									  const uint8_t *steps, const chte_mark_range *ranges, size_t nranges,
									  chte_reader **reader, chte_error *error);
CHTE_API int			chte_read_step(chte_reader *reader, uint32_t step, chte_error *error);

/*
 * Rows by their row ids (Appendix E): the offsets in a part of the rows whose
 * _pg_rowid are rowids[0 .. n), ascending, found through the part's _pg_rowid
 * projection; offsets[i] is UINT64_MAX where the part has no such row.
 */
CHTE_API int			chte_part_locate(chte_table *table, const char *part_name, const uint64_t *rowids,
							 size_t n, uint64_t *offsets, chte_error *error);

/*
 * A reader of the rows of a part at offsets[0 .. n), ascending, and of no other:
 * the granules holding them read, as chte_read_begin reads, the rest left out.
 */
CHTE_API int			chte_read_rows(chte_table *table, const char *part_name, const char *columns,
						   const uint64_t *offsets, size_t n, chte_reader **reader,
						   chte_error *error);

/*
 * A part written again without the rows whose _pg_rowid are dead[0 .. n),
 * ascending -- deletions every snapshot sees, folded into it -- as a new part
 * named as a mutation of it, block_number its version (all_<min>_<max>_<level>_<block_number>),
 * through the writer an insert uses: *nparts is how many parts it made (none
 * where every row was dead), which chte_write_part describes, chte_write_commit
 * names and chte_write_close removes uncommitted.
 */
CHTE_API int			chte_rewrite_part(chte_table *table, const char *part_name, const uint64_t *dead,
							  size_t n, uint64_t block_number, chte_writer **writer, size_t *nparts,
							  chte_error *error);

/*
 * Merges parts, as ClickHouse's MergeTask merges them with the table's merging mode, into one
 * part under a temporary name, as a writer that chte_write_finish has finished:
 * chte_write_part describes it, chte_write_commit gives it its final name, chte_write_close
 * removes it uncommitted.  The part is named for the inputs' blocks and level as ClickHouse
 * names a merged part, version (a block number of the table's) its mutation version, so that
 * no other part ever has its name.
 */
CHTE_API int			chte_merge(chte_table *table, const char *const *part_names, size_t nparts,
					   uint64_t version, chte_writer **writer, chte_error *error);

/* A part the merge selector may choose, as the caller lists them: in block order. */
typedef struct chte_merge_candidate
{
	const char *name;
	uint64_t	bytes;
	uint64_t	rows;
	uint64_t	age_seconds;
} chte_merge_candidate;

/*
 * The parts to merge next, as ClickHouse's merge selector chooses them for the table's
 * settings: parts[*first] and the *count - 1 after it, *count 0 where none should merge yet.
 * aggressive asks for a merge of whatever may merge, as OPTIMIZE does.
 */
CHTE_API int			chte_merge_select(chte_table *table, const chte_merge_candidate *parts, size_t n,
							  int aggressive, size_t *first, size_t *count, chte_error *error);

/*
 * How long an insert of a part waits, with the table holding parts of bytes in all: none
 * below parts_to_delay_insert, up to max_delay_to_insert seconds; an error (TOO_MANY_PARTS)
 * at parts_to_throw_insert, as MergeTreeData::delayInsertOrThrowIfNeeded() decides.
 */
CHTE_API int			chte_insert_delay(chte_table *table, size_t parts, uint64_t bytes, uint64_t *delay_ms,
							  chte_error *error);

/* The files of a part in pages, before it is read or merged; again replaces them. */
CHTE_API int			chte_pages_attach(chte_table *table, const char *part_name, const chte_page_file *files,
								  size_t nfiles, chte_error *error);
CHTE_API void		chte_pages_detach(chte_table *table, const char *part_name);

/* Removes a part's directory (after PostgreSQL no longer needs it). */
CHTE_API int			chte_drop_part(chte_table *table, const char *part_name, chte_error *error);

/*
 * External tables (ch_table_engines.md §2.17, CH7): files PostgreSQL does not own, read by
 * ClickHouse's own readers -- Parquet's v3 reader, its Arrow IPC reader -- and never written.  A
 * table is its format, its location and its columns: each the caller's name for a column of
 * the files, the ClickHouse type the caller takes its values as (§2.5's, without Nullable), and
 * how they are converted to it as they are read, a filter's constants converted back.  Which
 * files a statement reads is a listing, made as it begins and kept by the caller: its files
 * numbered from 0, each file's units -- Parquet's row groups, Arrow's record batches -- from 0,
 * as a part's marks are, and its rows' ids the rows of the files before it and the row's place
 * in its file.  An external table is a chte_table: chte_filter_create takes it, and
 * chte_table_close closes it.  The calls are optional: a library built before them lacks them.
 */
#define CHTE_EXTERNAL_PARQUET	0
#define CHTE_EXTERNAL_ARROW		1
#define CHTE_EXTERNAL_ICEBERG	2

/* How a column's values are converted from the files' */
#define CHTE_CONVERT_NONE		0	/* the files' values cast to the type, as ClickHouse casts */
#define CHTE_CONVERT_DATE		1	/* Date, Date32: days from 2000-01-01, Int32 */
#define CHTE_CONVERT_TIMESTAMP	2	/* DateTime, DateTime64: microseconds from 2000-01-01, Int64 */

typedef struct chte_external_column
{
	const char *name;			/* the caller's, which filters and reads name */
	const char *file_name;		/* the files': matched exactly, else regardless of case where one
								 * column alone matches so */
	const char *type;			/* ClickHouse's type the caller takes it as, without Nullable */
	int32_t		convert;		/* CHTE_CONVERT_* */
	int32_t		not_null;		/* a NULL read is an error */
	int32_t		check_utf8;		/* a string that is not UTF-8, or holds a zero byte, is an error */
} chte_external_column;

typedef struct chte_external_def
{
	int32_t		format;			/* CHTE_EXTERNAL_* */
	const char *name;			/* for messages */
	const char *location;		/* an absolute path: a file, a glob or a directory */
	const char *settings;		/* ClickHouse settings, "name = value, ...", or NULL */
	const chte_external_column *columns;
	size_t		ncolumns;
} chte_external_def;

CHTE_API int			chte_external_open(const chte_external_def *def, chte_table **table, chte_error *error);

/*
 * The files' columns, as ClickHouse's schema inference reads the first file of a listing made
 * now: each one's name and type, Nullable(T) where it may hold NULLs; none where no file is
 * listed.  Valid until the next call on the table.
 */
typedef struct chte_external_field
{
	const char *name;
	const char *type;
} chte_external_field;

CHTE_API int			chte_external_schema(chte_table *table, const chte_external_field **fields, size_t *nfields,
									 chte_error *error);

/*
 * A listing: the files the table's location names now, in their paths' order, each one's
 * footer read -- kept in the process by path, size and modification time -- and its columns
 * checked against the table's: a column the file lacks, or of a type the caller's cannot take,
 * fails the listing.  bytes and load hand a listing to a parallel scan's other processes, which
 * then read the same files; a file found changed as it is read fails the read.
 */
typedef struct chte_external_listing chte_external_listing;

typedef struct chte_external_file
{
	const char *path;
	uint64_t	size;
	int64_t		mtime;			/* nanoseconds since 1970 */
	uint64_t	rows;			/* an Iceberg data file's deleted rows among them */
	uint32_t	units;			/* an Iceberg data file is one */
	uint64_t	first_row;		/* its first row's id: the rows of the files before it */
	int32_t		deletes;		/* an Iceberg data file's delete files apply to it: not all its rows are live */
} chte_external_file;

CHTE_API int			chte_external_list(chte_table *table, chte_external_listing **listing, chte_error *error);
CHTE_API int			chte_external_listing_bytes(chte_external_listing *listing, const void **bytes, size_t *size,
											chte_error *error);
CHTE_API int			chte_external_listing_load(chte_table *table, const void *bytes, size_t size,
										   chte_external_listing **listing, chte_error *error);
CHTE_API void		chte_external_listing_free(chte_external_listing *listing);
CHTE_API size_t		chte_external_files(chte_external_listing *listing);
CHTE_API int			chte_external_file_info(chte_external_listing *listing, size_t file, chte_external_file *info,
										chte_error *error);

/*
 * As chte_part_select, of a listed file's units [begin, end) (end 0: its last): the ranges of
 * units a filter keeps by the files' statistics, and their rows.
 */
CHTE_API int			chte_external_select(chte_table *table, chte_external_listing *listing, size_t file,
									 chte_filter *filter, uint32_t begin, uint32_t end, chte_mark_range *ranges,
									 size_t max_ranges, size_t *nranges, uint64_t *rows, chte_error *error);

/* As chte_part_column_bytes, of a listed file */
CHTE_API int			chte_external_column_bytes(chte_table *table, chte_external_listing *listing, size_t file,
										   const char *columns, uint64_t *compressed, uint64_t *uncompressed,
										   chte_error *error);

/*
 * As chte_part_stats, of a listed file: its column's rows and NULLs where every unit counts
 * them, and its least and greatest values, converted, where every unit keeps them exactly.
 */
CHTE_API int			chte_external_stats(chte_table *table, chte_external_listing *listing, size_t file,
									const char *column, chte_column_stats *stats, chte_error *error);

/*
 * As chte_read_begin, of a listed file's units: the table's columns by the caller's names, and
 * _pg_rowid, each row's id.  A filter, where given, lets the reader pass over what else the
 * files' indexes rule out -- Parquet's pages by their page index, its row groups by their bloom
 * filters and dictionaries -- every row of what it reads handed.  And as chte_read_rows, of the
 * rows at offsets in the file.
 */
CHTE_API int			chte_external_read_begin(chte_table *table, chte_external_listing *listing, size_t file,
										 chte_filter *filter, const char *columns, const chte_mark_range *ranges,
										 size_t nranges, chte_reader **reader, chte_error *error);
CHTE_API int			chte_external_read_rows(chte_table *table, chte_external_listing *listing, size_t file,
										const char *columns, const uint64_t *offsets, size_t n, chte_reader **reader,
										chte_error *error);

/*
 * A file written by ClickHouse's own output format -- "Parquet", "Arrow" (Arrow IPC's file
 * format) -- under its settings, as tests make their files (§2.17): each batch's vectors in
 * chte_write_batch's layout of the columns' source types -- a date's an Int32 of days from
 * 2000-01-01 and a timestamp's an Int64 of microseconds, where their convert kinds say so --
 * cast to the file's types as ClickHouse casts.  The file is written under a temporary name and
 * renamed to path by finish; close removes what finish did not.
 */
typedef struct chte_file_writer chte_file_writer;

typedef struct chte_file_column
{
	const char *name;
	const char *type;			/* the file's, ClickHouse's: Nullable(T) where it may hold NULLs */
	const char *source;			/* the type of the batches' vectors, without Nullable */
	int32_t		convert;		/* CHTE_CONVERT_*: a source of days or microseconds */
} chte_file_column;

CHTE_API int			chte_file_write_begin(const char *format, const char *path, const chte_file_column *columns,
									  size_t ncolumns, const char *settings, chte_file_writer **writer,
									  chte_error *error);
CHTE_API int			chte_file_write_batch(chte_file_writer *writer, uint32_t nrows, const chte_vector *columns,
									  chte_error *error);
CHTE_API int			chte_file_write_finish(chte_file_writer *writer, uint64_t *bytes, chte_error *error);
CHTE_API void		chte_file_write_close(chte_file_writer *writer);

/*
 * Tables of their own files (ch_table_engines.md §2.18, CH8), each part one file the extension
 * writes, of rows in their ids' order.  The calls are optional: a library built before them lacks
 * them, and the extension then makes no such table.
 *
 * chte_own_open opens one as chte_external_open opens an external table, of Parquet or Arrow, its
 * location not read (NULL): its rows carry their ids, _pg_rowid a column UInt64 of every file,
 * ascending in it, read as it is rather than as a row's place.  Its caller lists its parts' files
 * itself (chte_external_listing_load); chte_external_list and chte_external_schema refuse it.
 *
 * chte_own_write_begin is chte_file_write_begin with each batch written in slices of 65,536 rows,
 * so that a writer that gathers its rows to a target -- Parquet's row groups, Arrow's record
 * batches, at the settings the caller gives -- cuts them at it, not at a batch's size.
 */
CHTE_API int			chte_own_open(const chte_external_def *def, chte_table **table, chte_error *error);

/*
 * A listing of a table of its own files: the files given, absolute paths -- a snapshot's parts' --
 * in their order, each one's identity taken now and its footer read or found kept, as
 * chte_external_list lists a location's.
 */
CHTE_API int			chte_own_list(chte_table *table, const char *const *paths, size_t npaths,
							  chte_external_listing **listing, chte_error *error);
CHTE_API int			chte_own_write_begin(const char *format, const char *path, const chte_file_column *columns,
									 size_t ncolumns, const char *settings, chte_file_writer **writer,
									 chte_error *error);

/*
 * The places in a listed file of the rows of ids: offsets[i] the place of ids[i], UINT64_MAX
 * where the file holds none.  A file's ids are read once in the table, for its identity.
 */
CHTE_API int			chte_own_locate(chte_table *table, chte_external_listing *listing, size_t file,
								const uint64_t *ids, size_t n, uint64_t *offsets, chte_error *error);

/*
 * The ids of the rows of a listed file's units -- ascending, as its units are -- in their order:
 * *ids valid until the next call on the table.
 */
CHTE_API int			chte_own_unit_ids(chte_table *table, chte_external_listing *listing, size_t file,
								  const int32_t *units, size_t nunits, const uint64_t **ids, size_t *n,
								  chte_error *error);

/*
 * A merge: the rows of listed files -- their columns by the columns' names, _pg_rowid among them
 * -- but those whose ids dead names (ascending), in their ids' order, written as chte_own_write_*
 * writes a part's file at path; a column a file lacks, added after it was written, its rows the
 * column's missing value, missing[c] a vector of one row in chte_write_batch's layout of the
 * column's source type; dropped[i], where given, set where a row of dead[i] was among them.
 * Returns the rows and the bytes written.
 */
CHTE_API int			chte_own_merge(chte_table *table, chte_external_listing *listing, const size_t *files,
							   size_t nfiles, const uint64_t *dead, size_t ndead, uint8_t *dropped,
							   const char *format, const char *path, const chte_file_column *columns,
							   size_t ncolumns, const chte_vector *missing, const char *settings, uint64_t *rows,
							   uint64_t *bytes, chte_error *error);

/*
 * A listed file's layout, as the tests read it back: its units and their rows, the codec of its
 * first column chunk or record batch ("NONE" where none), and whether every column chunk of a
 * Parquet file has a page index, and some a bloom filter or a dictionary.  codec is valid until
 * the next call on the table.
 */
typedef struct chte_file_layout
{
	uint32_t	units;
	uint64_t	rows;
	uint64_t	min_unit_rows;
	uint64_t	max_unit_rows;
	const char *codec;
	int32_t		page_index;
	int32_t		bloom_filters;
	int32_t		dictionaries;
} chte_file_layout;

CHTE_API int			chte_own_file_layout(chte_table *table, chte_external_listing *listing, size_t file,
									 chte_file_layout *layout, chte_error *error);

/*
 * The tests' Iceberg tables (§2.17), on a local disk, by ClickHouse's own Iceberg writers: a table made by
 * IcebergMetadata::createInitial(), its rows appended by IcebergStorageSink, and its deletes -- which ClickHouse
 * writes only through its interpreter's mutations, and equality deletes never -- by its Parquet writer, its
 * manifests' and metadata's generators, an equality delete's manifest given its equality_ids.  The writer takes
 * chte_file_write_batch's rows and commits them as a snapshot at chte_file_write_finish, which returns the bytes
 * of the files it added.  CREATE's columns are the table's, each with its type, and its spec, where given, the
 * table's partitions as ClickHouse's PARTITION BY names them (identity: a column; icebergBucket(n, c), ...); the
 * others' columns name the table's, a NULL type the table's own: APPEND's every column, DELETE_EQUAL's those whose
 * values delete a row, DELETE_POSITIONS' file_path (a data file's path in the table's metadata) and pos.
 */
#define CHTE_ICEBERG_CREATE				0
#define CHTE_ICEBERG_APPEND				1
#define CHTE_ICEBERG_DELETE_POSITIONS	2
#define CHTE_ICEBERG_DELETE_EQUAL		3

CHTE_API int			chte_iceberg_write_begin(const char *location, int action, const char *spec,
										 const chte_file_column *columns, size_t ncolumns, const char *settings,
										 chte_file_writer **writer, chte_error *error);

/* The tests' change of an Iceberg table's schema, by IcebergMetadata::alter(): one command of ClickHouse's ALTER */
CHTE_API int			chte_iceberg_alter(const char *location, const char *command, const char *settings,
								   chte_error *error);

#ifdef __cplusplus
}
#endif

#endif							/* CHTE_ENGINE_H */
