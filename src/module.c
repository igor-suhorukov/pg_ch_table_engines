/*-------------------------------------------------------------------------
 *
 * module.c
 *	  ch_table_engines' module: its settings (ch_table_engines.md §2.14),
 *	  hooks and callbacks.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <execinfo.h>
#include <signal.h>
#include <unistd.h>

#include "access/xact.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/guc.h"

#include "chte.h"
#include "tombstones.h"

PG_MODULE_MAGIC_EXT(.name = "ch_table_engines", .version = "0.1");

int			ch_insert_buffer_rows = 1048576;
int			ch_insert_buffer_bytes = 256 * 1024;	/* kB */
int			ch_default_storage = CH_STORAGE_PAGES;
bool		ch_preloaded = false;

static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;

static const struct config_enum_entry storage_options[] = {
	{"files", CH_STORAGE_FILES, false},
	{"pages", CH_STORAGE_PAGES, false},
	{NULL, 0, false}
};


/*
 * The rows the query wrote become parts as it finishes (§2.7 rule 3).  Its
 * AFTER triggers have run by then; one that fetched a row by TID was answered
 * from the buffer.
 */
static void
ch_ExecutorFinish(QueryDesc *queryDesc)
{
	if (prev_ExecutorFinish)
		prev_ExecutorFinish(queryDesc);
	else
		standard_ExecutorFinish(queryDesc);
	ch_buffer_flush_all();
}

static void
ch_xact_callback(XactEvent event, void *arg)
{
	/* the buffers' parts take row ids and blocks; the lock rows go; then the counters are written */
	ch_buffer_xact_callback(event);
	ch_tombstones_xact_callback(event);
	ch_counters_xact_callback(event);
	ch_store_xact_callback(event);
	if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_ABORT || event == XACT_EVENT_PREPARE ||
		event == XACT_EVENT_PARALLEL_COMMIT || event == XACT_EVENT_PARALLEL_ABORT)
		ch_tables_xact_end();
}

static void
ch_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
					SubTransactionId parentSubid, void *arg)
{
	ch_buffer_subxact_callback(event, mySubid);
	ch_store_subxact_callback(event, mySubid);
}

/*
 * A test leg's aid (test/isolation.sh): where CHTE_BACKTRACE_ON_CRASH is in
 * the server's environment, a backend that faults writes its stack to the
 * server's log before it dies as it would have.
 */
static void
crash_backtrace(int signo)
{
	void	   *frames[64];
	int			n = backtrace(frames, lengthof(frames));
	static const char head[] = "ch_table_engines: a backend faulted; its stack:\n";

	(void) write(STDERR_FILENO, head, sizeof(head) - 1);
	backtrace_symbols_fd(frames, n, STDERR_FILENO);
	signal(signo, SIG_DFL);
	raise(signo);
}

void
_PG_init(void)
{
	ch_preloaded = process_shared_preload_libraries_in_progress;
	if (getenv("CHTE_BACKTRACE_ON_CRASH") != NULL)
	{
		void	   *frame[1];

		(void) backtrace(frame, 1);	/* libgcc loaded now, not in the handler */
		signal(SIGSEGV, crash_backtrace);
		signal(SIGBUS, crash_backtrace);
	}

	DefineCustomIntVariable("ch.insert_buffer_rows",
							"Rows a write buffer holds before they become a part.",
							NULL,
							&ch_insert_buffer_rows,
							1048576, 1, INT_MAX,
							PGC_USERSET, 0,
							NULL, NULL, NULL);
	DefineCustomIntVariable("ch.insert_buffer_bytes",
							"Bytes a write buffer holds before its rows become a part.",
							NULL,
							&ch_insert_buffer_bytes,
							256 * 1024, 64, INT_MAX / 1024,
							PGC_USERSET, GUC_UNIT_KB,
							NULL, NULL, NULL);
	DefineCustomEnumVariable("ch.default_storage",
							 "The store of a table whose CREATE names none.",
							 NULL,
							 &ch_default_storage,
							 CH_STORAGE_PAGES, storage_options,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);
	DefineCustomStringVariable("ch.engine_library",
							   "The engine library's file.",
							   NULL,
							   &ch_engine_library,
							   "$libdir/libchte_engine.so",
							   PGC_SUSET, 0,
							   NULL, NULL, NULL);
	DefineCustomStringVariable("ch.engine_log_level",
							   "The least level of ClickHouse's messages passed to the server's log.",
							   "One of ClickHouse's: fatal, critical, error, warning, notice, information, debug, trace.",
							   &ch_engine_log_level,
							   "warning",
							   PGC_SUSET, 0,
							   NULL, NULL, NULL);
	DefineCustomStringVariable("ch.engine_settings",
							   "ClickHouse settings of the process's engine, as name = value, ...",
							   "Read once, as a process first loads the engine library.",
							   &ch_engine_settings,
							   "",
							   PGC_SUSET, 0,
							   NULL, NULL, NULL);
	DefineCustomBoolVariable("ch.enable_chscan",
							 "Whether the planner offers ChScan: a scan that reads only the columns it needs, "
							 "and only the granules its quals keep.",
							 NULL,
							 &ch_enable_chscan,
							 true,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);
	MarkGUCPrefixReserved("ch");

	ch_options_init();

	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = ch_ExecutorFinish;

	RegisterXactCallback(ch_xact_callback, NULL);
	RegisterSubXactCallback(ch_subxact_callback, NULL);

	ch_chscan_init();

	if (ch_preloaded)
	{
		ch_store_register_rmgr();
		/* without vexec, unused entries of its registries */
		ch_vexec_register();
	}
}
