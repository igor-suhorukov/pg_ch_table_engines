/*-------------------------------------------------------------------------
 *
 * engine.h
 *	  The engine library, libchte_engine.so (ch_table_engines.md §2.2):
 *	  loaded on first use, and its errors raised as PostgreSQL's.
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
	int			(*read_begin) (chte_table *, const char *, const char *, chte_reader **, chte_error *);
	int			(*read_next) (chte_reader *, uint32_t *, chte_error *);
	int			(*read_vector) (chte_reader *, uint32_t, chte_vector *, chte_error *);
	void		(*read_end) (chte_reader *);
	int			(*read_present) (chte_reader *, uint32_t);
	int			(*merge) (chte_table *, const char *const *, size_t, chte_part_info *, chte_error *);
	int			(*drop_part) (chte_table *, const char *, chte_error *);
	int			(*pages_attach) (chte_table *, const char *, const chte_page_file *, size_t, chte_error *);
	void		(*pages_detach) (chte_table *, const char *);
} ChEngine;

/* GUCs */
extern char *ch_engine_library;
extern char *ch_engine_log_level;
extern char *ch_engine_settings;

/* The library, loaded and initialized on the first call. */
extern const ChEngine *ch_engine(void);

/* Readers and writers, owned by the resource owner they are opened under */
#include "utils/resowner.h"
extern ResourceOwner ch_handle_reserve(void);
extern void ch_handle_opened(ResourceOwner owner, bool reader, void *ptr);
extern void ch_handle_closing(ResourceOwner owner, bool reader, void *ptr);

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
