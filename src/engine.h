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
	CH_HANDLE_SCAN				/* a running scan's place among them (scan.c) */
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
