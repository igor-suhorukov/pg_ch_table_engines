/*-------------------------------------------------------------------------
 *
 * engine.h
 *	  The engine library, libchte_engine.so (ch_table_engines.md §2.2):
 *	  mapped in the postmaster, initialized on first use, and its errors
 *	  raised as PostgreSQL's.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#ifndef CHTE_ENGINE_LOADER_H
#define CHTE_ENGINE_LOADER_H

#include "chte_engine.h"

/* The library's entry points, resolved by dlsym. */
typedef struct ChEngine
{
	int			(*init) (const chte_init_options *, chte_error *);
	void		(*shutdown) (void);
	int			(*thread_count) (void);
	int			(*table_open) (const chte_table_def *, chte_table **, chte_error *);
	void		(*table_close) (chte_table *);
	int			(*write_begin) (chte_table *, uint64_t, chte_writer **, chte_error *);
	int			(*write_batch) (chte_writer *, uint32_t, const chte_vector *, chte_error *);
	int			(*write_finish) (chte_writer *, size_t *, chte_error *);
	int			(*write_part) (chte_writer *, size_t, chte_part_info *, chte_error *);
	int			(*write_commit) (chte_writer *, chte_error *);
	void		(*write_close) (chte_writer *);
	int			(*read_begin) (chte_table *, const char *, const char *, const chte_mark_range *, size_t,
							   chte_reader **, chte_error *);
	int			(*read_next) (chte_reader *, uint32_t *, chte_error *);
	int			(*read_vector) (chte_reader *, uint32_t, chte_vector *, chte_error *);
	void		(*read_end) (chte_reader *);
	int			(*read_present) (chte_reader *, uint32_t);
	int			(*merge) (chte_table *, const char *const *, size_t, uint64_t, chte_writer **, chte_error *);
	int			(*merge_select) (chte_table *, const chte_merge_candidate *, size_t, int, size_t *, size_t *,
								 chte_error *);
	int			(*insert_delay) (chte_table *, size_t, uint64_t, uint64_t *, chte_error *);
	int			(*drop_part) (chte_table *, const char *, chte_error *);
	int			(*pages_attach) (chte_table *, const char *, const chte_page_file *, size_t, chte_error *);
	void		(*pages_detach) (chte_table *, const char *);
	int			(*filter_create) (chte_table *, const chte_filter_node *, size_t, chte_filter **, chte_error *);
	void		(*filter_free) (chte_filter *);
	int			(*part_select) (chte_table *, const char *, chte_filter *, uint32_t, uint32_t,
								chte_mark_range *, size_t, size_t *, uint64_t *, chte_error *);
	int			(*part_column_bytes) (chte_table *, const char *, const char *, uint64_t *, uint64_t *,
									  chte_error *);
	/* W6: a part's columns as its files hold them, and its files' bytes; both, or neither */
	int			(*part_streams_of) (chte_table *, const char *, const char *, uint32_t, uint32_t,
									chte_part_streams *, chte_error *);
	int			(*part_file_read) (chte_table *, const char *, const char *, uint64_t, uint64_t, void *,
								   chte_error *);
	int			(*part_stats) (chte_table *, const char *, const char *, chte_column_stats *, chte_error *);
	chte_batch *(*read_retain) (chte_reader *);
	void		(*batch_release) (chte_batch *);
	int			(*part_locate) (chte_table *, const char *, const uint64_t *, size_t, uint64_t *, chte_error *);
	int			(*read_rows) (chte_table *, const char *, const char *, const uint64_t *, size_t, chte_reader **,
							  chte_error *);
	int			(*rewrite_part) (chte_table *, const char *, const uint64_t *, size_t, uint64_t, chte_writer **,
								 size_t *, chte_error *);
	/* optional (VC5): NULL where the library lacks them */
	int			(*read_keep_dictionaries) (chte_reader *, const uint8_t *, size_t, chte_error *);
	int			(*read_dictionary) (chte_reader *, uint32_t, chte_dictionary *, chte_error *);
	/* optional (VC12) */
	int			(*register_all_functions) (chte_error *);
	int			(*function_count) (void);
	/* optional (VC2) */
	int			(*read_begin_steps) (chte_table *, const char *, const char *, const uint8_t *,
									 const chte_mark_range *, size_t, chte_reader **, chte_error *);
	int			(*read_step) (chte_reader *, uint32_t, chte_error *);
	/* optional (VC18) */
	int			(*write_columns) (chte_writer *, uint32_t, const chte_column_in *, chte_error *);
	int			(*write_block) (chte_writer *, uint64_t, chte_error *);
	int			(*write_reserve) (chte_writer *, uint64_t, chte_error *);
	int			(*write_vector) (chte_writer *, uint32_t, chte_vector *, chte_error *);
	int			(*write_in_memory) (chte_writer *, chte_error *);
	int			(*write_part_files) (chte_writer *, size_t, const chte_part_file **, size_t *, chte_error *);
	/* optional (VC20's B6, B7) */
	int			(*write_part_summary) (chte_writer *, size_t, const void **, size_t *, chte_error *);
	int			(*summary_column_bytes) (chte_table *, const char *, const void *, size_t, const char *,
										 uint64_t *, uint64_t *, int *, chte_error *);
	int			(*summary_stats) (chte_table *, const char *, const void *, size_t, const char *,
								  chte_column_stats *, int *, chte_error *);
	int			(*filter_summary_select) (chte_filter *, const char *, const void *, size_t, int *, chte_error *);
	/* optional (CH7): external tables, and the tests' writer of their files */
	int			(*external_open) (const chte_external_def *, chte_table **, chte_error *);
	int			(*external_schema) (chte_table *, const chte_external_field **, size_t *, chte_error *);
	int			(*external_list) (chte_table *, chte_external_listing **, chte_error *);
	int			(*external_listing_bytes) (chte_external_listing *, const void **, size_t *, chte_error *);
	int			(*external_listing_load) (chte_table *, const void *, size_t, chte_external_listing **,
										  chte_error *);
	void		(*external_listing_free) (chte_external_listing *);
	size_t		(*external_files) (chte_external_listing *);
	int			(*external_file_info) (chte_external_listing *, size_t, chte_external_file *, chte_error *);
	int			(*external_select) (chte_table *, chte_external_listing *, size_t, chte_filter *, uint32_t,
									uint32_t, chte_mark_range *, size_t, size_t *, uint64_t *, chte_error *);
	int			(*external_column_bytes) (chte_table *, chte_external_listing *, size_t, const char *, uint64_t *,
										  uint64_t *, chte_error *);
	int			(*external_stats) (chte_table *, chte_external_listing *, size_t, const char *, chte_column_stats *,
								   chte_error *);
	int			(*external_read_begin) (chte_table *, chte_external_listing *, size_t, chte_filter *, const char *,
										const chte_mark_range *, size_t, chte_reader **, chte_error *);
	int			(*external_read_rows) (chte_table *, chte_external_listing *, size_t, const char *, const uint64_t *,
									   size_t, chte_reader **, chte_error *);
	int			(*file_write_begin) (const char *, const char *, const chte_file_column *, size_t, const char *,
									 chte_file_writer **, chte_error *);
	int			(*iceberg_write_begin) (const char *, int, const char *, const chte_file_column *, size_t,
										const char *, chte_file_writer **, chte_error *);
	int			(*iceberg_alter) (const char *, const char *, const char *, chte_error *);
	int			(*file_write_batch) (chte_file_writer *, uint32_t, const chte_vector *, chte_error *);
	int			(*file_write_finish) (chte_file_writer *, uint64_t *, chte_error *);
	void		(*file_write_close) (chte_file_writer *);
	/* CH8's (§2.18), optional: tables of their own files */
	int			(*own_open) (const chte_external_def *, chte_table **, chte_error *);
	int			(*own_list) (chte_table *, const char *const *, size_t, chte_external_listing **, chte_error *);
	int			(*own_write_begin) (const char *, const char *, const chte_file_column *, size_t, const char *,
									chte_file_writer **, chte_error *);
	int			(*own_locate) (chte_table *, chte_external_listing *, size_t, const uint64_t *, size_t, uint64_t *,
							   chte_error *);
	int			(*own_unit_ids) (chte_table *, chte_external_listing *, size_t, const int32_t *, size_t,
								 const uint64_t **, size_t *, chte_error *);
	int			(*own_merge) (chte_table *, chte_external_listing *, const size_t *, size_t, const uint64_t *, size_t,
							  uint8_t *, const char *, const char *, const chte_file_column *, size_t,
							  const chte_vector *, const char *, uint64_t *, uint64_t *, chte_error *);
	int			(*own_file_layout) (chte_table *, chte_external_listing *, size_t, chte_file_layout *, chte_error *);
	/* optional (VC21): a table opened from its definition's parts */
	int			(*table_open_spec) (const chte_table_spec *, chte_table **, chte_error *);
	/* optional (VC25): a table of its own files' parts summarized, and listed from their summaries; both, or neither */
	int			(*own_summary) (chte_table *, chte_external_listing *, size_t, const void **, size_t *, chte_error *);
	int			(*own_list_summaries) (chte_table *, const char *const *, const void *const *, const size_t *, size_t,
									   chte_external_listing **, chte_error *);
	/* optional (VC29): the parts the process has loaded, for the tests */
	uint64_t	(*parts_loaded) (void);
	/* optional (VC25): the files' footers the process has read, for the tests */
	uint64_t	(*footers_read) (void);
} ChEngine;

/* GUCs */
extern char *ch_engine_library;
extern char *ch_engine_log_level;
extern char *ch_engine_settings;
extern bool ch_engine_preload;
extern int	ch_engine_functions;

/* ch.engine_functions */
typedef enum ChEngineFunctions
{
	CH_FUNCTIONS_USED,
	CH_FUNCTIONS_ALL
} ChEngineFunctions;

/* The library, loaded and initialized on the first call. */
extern const ChEngine *ch_engine(void);

/* The postmaster's: the library mapped, nothing of it called (VC12) */
extern void ch_engine_map(void);

/* The library's objects, owned by the resource owner they are made under */
#include "utils/resowner.h"
typedef enum ChHandleKind
{
	CH_HANDLE_READER,
	CH_HANDLE_WRITER,
	CH_HANDLE_FILTER,
	CH_HANDLE_BATCH,
	CH_HANDLE_SCAN,				/* a running scan's place among them (scan.c) */
	CH_HANDLE_FILE_WRITER		/* a file the tests' writer has not finished (external.c) */
} ChHandleKind;
extern ResourceOwner ch_handle_reserve(void);
extern void ch_handle_opened(ResourceOwner owner, ChHandleKind kind, void *ptr);
extern void ch_handle_closing(ResourceOwner owner, ChHandleKind kind, void *ptr);

/* Whether this process has loaded it. */
extern bool ch_engine_loaded(void);

/*
 * Raises the error a call returned: QUERY_WAS_CANCELLED as PostgreSQL's own
 * interrupt, ClickHouse's checksum and corruption errors as data corruption.
 */
pg_noreturn extern void ch_engine_raise(const chte_error *err, const char *what);

/* Runs a call of the library and raises what it returns. */
#define CH_CALL(what, call) \
	do { \
		chte_error ch_err_; \
		if ((call) != 0) \
			ch_engine_raise(&ch_err_, (what)); \
	} while (0)
#define CH_ERR (&ch_err_)

#endif							/* CHTE_ENGINE_LOADER_H */
