/*
 * chte_engine.h — the C API of libchte_engine.so, the core of ClickHouse v26.8.8.8-lts
 * compiled into one shared library (ch_table_engines.md §2.2).
 *
 * The extension dlopen()s the library on first use in a backend (never in the postmaster)
 * and talks to it only through these functions: no C++ type, exception or thread crosses
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

#define CHTE_ABI_VERSION 3

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
	const char *log_level;		/* "error", "warning", "information", "debug", "trace" */
	const char *settings;		/* ClickHouse settings "name = value, ...", or NULL */
	chte_callbacks callbacks;
} chte_init_options;

/* Initializes the process-wide ClickHouse context; later calls are no-ops. */
CHTE_API int			chte_init(const chte_init_options *options, chte_error *error);

/*
 * Shuts the process's ClickHouse down: its background executors' idle threads end, so
 * that exit() can join them.  Idempotent; registered with atexit() by chte_init as a
 * fallback, and called by the extension from on_proc_exit, before static destructors run.
 */
CHTE_API void		chte_shutdown(void);

/* Number of threads of the process, for the tests that check no thread is started. */
CHTE_API int			chte_thread_count(void);

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
 * partition.  finish writes and syncs them under temporary names; commit gives them their
 * final names; abort (or close without commit) removes them.
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
 * numbered block_number, through the writer an insert uses: *nparts is how
 * many parts it made (none where every row was dead), which chte_write_part
 * describes, chte_write_commit names and chte_write_close removes uncommitted.
 */
CHTE_API int			chte_rewrite_part(chte_table *table, const char *part_name, const uint64_t *dead,
							  size_t n, uint64_t block_number, chte_writer **writer, size_t *nparts,
							  chte_error *error);

/* Merges parts with the table's merging mode into one part, committed under its final name. */
CHTE_API int			chte_merge(chte_table *table, const char *const *part_names, size_t nparts,
					   chte_part_info *result, chte_error *error);

/* The files of a part in pages, before it is read or merged; again replaces them. */
CHTE_API int			chte_pages_attach(chte_table *table, const char *part_name, const chte_page_file *files,
								  size_t nfiles, chte_error *error);
CHTE_API void		chte_pages_detach(chte_table *table, const char *part_name);

/* Removes a part's directory (after PostgreSQL no longer needs it). */
CHTE_API int			chte_drop_part(chte_table *table, const char *part_name, chte_error *error);

#ifdef __cplusplus
}
#endif

#endif							/* CHTE_ENGINE_H */
