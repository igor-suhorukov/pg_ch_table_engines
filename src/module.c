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
bool		ch_enable_fetch_columns = true;
bool		ch_enable_sink_columns = true;
bool		ch_enable_memory_parts = true;
bool		ch_enable_fetch_cache = true;
bool		ch_enable_part_summaries = true;
bool		ch_enable_summary_pruning = true;
double		ch_fetch_cost = 55.0;
double		ch_fetch_value_cost = 0.0025;
bool		ch_preloaded = false;

static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;

static const struct config_enum_entry storage_options[] = {
	{"files", CH_STORAGE_FILES, false},
	{"pages", CH_STORAGE_PAGES, false},
	{NULL, 0, false}
};

static const struct config_enum_entry functions_options[] = {
	{"used", CH_FUNCTIONS_USED, false},
	{"all", CH_FUNCTIONS_ALL, false},
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
	if (getenv("CHTE_BACKTRACE_ON_CRASH") != NULL && getenv("CHTE_BACKTRACE_ON_CRASH")[0] != '\0')
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
	/* the postmaster's alone: a library loaded later may not define it */
	if (process_shared_preload_libraries_in_progress)
		DefineCustomBoolVariable("ch.engine_preload",
								 "Whether the postmaster maps the engine library as it preloads the extension.",
								 "Every process forked from it then finds the library mapped and relocated; "
								 "ClickHouse's engine still starts in each process as it first needs it.",
								 &ch_engine_preload,
								 true,
								 PGC_POSTMASTER, 0,
								 NULL, NULL, NULL);
	DefineCustomEnumVariable("ch.engine_functions",
							 "Which of ClickHouse's functions a process registers as it first loads the engine library.",
							 "used: those the filters use, the others as a table's definition, a write, a merge "
							 "or a rewrite first needs them; all: every one at once.",
							 &ch_engine_functions,
							 CH_FUNCTIONS_USED, functions_options,
							 PGC_SUSET, 0,
							 NULL, NULL, NULL);
	DefineCustomIntVariable("ch.max_merge_workers",
							"Merge workers at most, one for a database, beside their launcher.",
							"0 at the server's start: no launcher, which a reload does not start.",
							&ch_max_merge_workers,
							2, 0, 64,
							PGC_SIGHUP, 0,
							NULL, NULL, NULL);
	DefineCustomIntVariable("ch.merge_naptime",
							"How long a merge worker sleeps between its passes over its database's tables.",
							NULL,
							&ch_merge_naptime,
							10, 1, 3600,
							PGC_SIGHUP, GUC_UNIT_S,
							NULL, NULL, NULL);
	DefineCustomBoolVariable("ch.enable_chscan",
							 "Whether the planner offers ChScan: a scan that reads only the columns it needs, "
							 "and only the granules its quals keep.",
							 NULL,
							 &ch_enable_chscan,
							 true,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);
	DefineCustomBoolVariable("ch.enable_fetch_columns",
							 "Whether a fetch of a row by its TID decodes only the columns its scan reads.",
							 "Off, a fetch decodes a granule of every column of the table, whatever the query "
							 "reads of it.",
							 &ch_enable_fetch_columns,
							 true,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);
	DefineCustomBoolVariable("ch.enable_sink_columns",
							 "Whether vexec's sink fills ClickHouse's columns from its batches directly.",
							 "Off, a batch's columns are copied into the write buffer's, and from them into "
							 "ClickHouse's as the part is made, as rows are.",
							 &ch_enable_sink_columns,
							 true,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);
	DefineCustomBoolVariable("ch.enable_memory_parts",
							 "Whether a part of the pages store is written into memory, then into its pages.",
							 "Off, ClickHouse writes it into temporary files in the table's directory, which "
							 "are then copied into its pages and removed.",
							 &ch_enable_memory_parts,
							 true,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);
	DefineCustomBoolVariable("ch.enable_fetch_cache",
							 "Whether an index scan's fetches by TID keep the snapshot's parts and deletions, "
							 "located row ids and the last granule read.",
							 NULL,
							 &ch_enable_fetch_cache,
							 true,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);
	DefineCustomBoolVariable("ch.enable_part_summaries",
							 "Whether the planner and a scan take a part's columns' sizes and statistics from "
							 "its summary in ch.parts.",
							 "Off, a part is loaded through ClickHouse, in each process, the first time they are "
							 "asked of it; its summary is written either way.",
							 &ch_enable_part_summaries,
							 true,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);
	DefineCustomBoolVariable("ch.enable_summary_pruning",
							 "Whether the planner asks a filter what a part's summary rules out before it "
							 "loads the part to select its granules.",
							 "Off, the estimate of a scan with quals loads every part.",
							 &ch_enable_summary_pruning,
							 true,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);
	DefineCustomRealVariable("ch.fetch_cost",
							 "The planner's cost of finding a row by its TID, beside its columns' granules.",
							 "Added to each index scan's cost a row it fetches; 0 with ch.fetch_value_cost "
							 "prices a fetch as PostgreSQL's random page alone.",
							 &ch_fetch_cost,
							 55.0, 0.0, 1e10,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);
	DefineCustomRealVariable("ch.fetch_value_cost",
							 "The planner's cost of decoding a value of a granule a fetch by TID reads.",
							 "A fetch decodes a granule, index_granularity rows, of each column it reads.",
							 &ch_fetch_value_cost,
							 0.0025, 0.0, 1e10,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);
	MarkGUCPrefixReserved("ch");

	ch_options_init();

	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = ch_ExecutorFinish;

	RegisterXactCallback(ch_xact_callback, NULL);
	RegisterSubXactCallback(ch_subxact_callback, NULL);

	ch_chscan_init();
	ch_fetch_init();

	if (ch_preloaded)
	{
		/* the postmaster's, which its children inherit (VC12) */
		if (ch_engine_preload && !IsUnderPostmaster && IsPostmasterEnvironment)
			ch_engine_map();
		ch_store_register_rmgr();
		/* the merge workers' launcher (worker.c), never on a standby */
		ch_merge_launcher_register();
		/* without vexec, unused entries of its registries */
		ch_vexec_register();
	}
}
