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
bool		ch_enable_point_reads = true;
bool		ch_enable_scan_marks = true;
bool		ch_enable_skip_indexes = true;
bool		ch_parallel_part_affinity = false;
bool		ch_parallel_key_units = true;
int			ch_parallel_unit_min_bytes = 4096;
bool		ch_enable_sparse_quals = true;
bool		ch_enable_count_from_defaults = true;
bool		ch_enable_char_counts = true;
uint64		ch_bound_granules_passed = 0;
uint64		ch_sparse_rows_passed = 0;
bool		ch_enable_part_summaries = true;
bool		ch_enable_summary_pruning = true;
double		ch_fetch_cost = 55.0;
double		ch_fetch_value_cost = 0.0025;
int			ch_own_parquet_dictionary_filter = 64 * 1024;	/* bytes */
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
 * On a node of the Cloudberry port's cluster (ch_table_engines.md CH6),
 * gp_core sends a DDL statement to the segments as its own ProcessUtility
 * hook receives it, before it runs it here.  This library's hook takes the
 * engines' options out of a statement (options.c), and so must run inside
 * gp_core's: listed before gp_core in shared_preload_libraries.  Listed
 * after it, or loaded later, it would send the segments the statement
 * without the options, and every segment would make its share of a table
 * without them.  gp_core publishes its rendezvous variable as its _PG_init
 * ends, and gp.cluster_config names the cluster's nodes; on a single node
 * nothing is sent, and either order does.
 */
static bool
cluster_node(void)
{
	void	  **core = (void **) find_rendezvous_variable("Cloudberry/gp_core");
	const char *nodes;

	if (*core == NULL)
		return false;
	nodes = GetConfigOption("gp.cluster_config", true, false);
	return nodes != NULL && nodes[0] != '\0';
}

/*
 * Whether this server is a node of the port's cluster, gp_core loaded and its
 * nodes named: known in a process forked once every library is loaded, and
 * asked afresh in the postmaster, where gp_core may load after this library.
 */
bool
ch_cluster_node(void)
{
	static int	known = -1;

	if (!IsUnderPostmaster)
		return cluster_node();
	if (known < 0)
		known = cluster_node() ? 1 : 0;
	return known == 1;
}

static void
check_cluster_order(void)
{
	if (!cluster_node())
		return;
	ereport(ERROR,
			(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			 errmsg("ch_table_engines must come before gp_core in shared_preload_libraries on a node of a cluster"),
			 errdetail("gp_core sends each DDL statement to the segments as its own hook receives it; "
					   "the hook that takes the engines' options out of a statement must run inside it."),
			 errhint("List ch_table_engines first in shared_preload_libraries on every node.")));
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
	ssize_t		rc;

	/*
	 * Nothing to be done here if the log cannot be written; the result kept,
	 * as glibc's _FORTIFY_SOURCE (PGDG's builds) wants it used.
	 */
	rc = write(STDERR_FILENO, head, sizeof(head) - 1);
	(void) rc;
	backtrace_symbols_fd(frames, n, STDERR_FILENO);
	signal(signo, SIG_DFL);
	raise(signo);
}

void
_PG_init(void)
{
	check_cluster_order();
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
	/* the parts' streams for a GPU's units, in the postmaster's shared memory (VC29) */
	if (process_shared_preload_libraries_in_progress)
		ch_streams_init();
	/* the parts' granules filters keep, in the postmaster's shared memory (VC31) */
	if (process_shared_preload_libraries_in_progress)
		ch_selections_init();
	/* the parts' loaded metadata, in the postmaster's shared memory (VC42) */
	if (process_shared_preload_libraries_in_progress)
		ch_partmeta_init();
	else
		DefineCustomBoolVariable("ch.enable_shared_part_metadata",
								 "Whether a process makes a MergeTree part of the metadata another process loaded.",
								 NULL, &ch_enable_shared_part_metadata, true, PGC_USERSET, 0, NULL, NULL, NULL);
	/* the parts' decompressed marks, in the postmaster's shared memory (VC42) */
	if (process_shared_preload_libraries_in_progress)
		ch_partmarks_init();
	else
		DefineCustomBoolVariable("ch.enable_shared_marks",
								 "Whether a process takes a MergeTree part's marks another process decompressed.",
								 NULL, &ch_enable_shared_marks, false, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("ch.enable_part_selections",
							 "Whether the granules a filter keeps of a MergeTree part are kept in shared memory.",
							 "Kept, a process plans and claims the part under the same filter without loading it; "
							 "off, every part is asked of the engine library, and nothing is kept.",
							 &ch_enable_part_selections,
							 true,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);
	/* the exits' counts, in the postmaster's shared memory (VC23) */
	if (process_shared_preload_libraries_in_progress)
		ch_engine_shmem_init();
	DefineCustomEnumVariable("ch.engine_functions",
							 "Which of ClickHouse's functions a process registers as it first loads the engine library.",
							 "used: those the filters use, the others as a table's definition, a write, a merge "
							 "or a rewrite first needs them; all: every one at once.",
							 &ch_engine_functions,
							 CH_FUNCTIONS_USED, functions_options,
							 PGC_SUSET, 0,
							 NULL, NULL, NULL);
	DefineCustomBoolVariable("ch.fast_exit",
							 "Whether a process that started the engine ends by _exit() once PostgreSQL's own "
							 "exits have run.",
							 "Off, the engine is torn down and the library's static destructors run as the "
							 "process exits, a millisecond or two of each parallel worker.",
							 &ch_fast_exit,
							 true,
							 PGC_SUSET, 0,
							 NULL, NULL, NULL);
	DefineCustomBoolVariable("ch.keep_freed_memory",
							 "Whether a process that starts the engine keeps the memory it frees, rather than "
							 "handing it back to the system and faulting it in again.",
							 "Read as the process starts the engine: glibc's malloc then serves requests of up "
							 "to 32 MB from its heap and trims the heap's top only past 1 GB free.",
							 &ch_keep_freed_memory,
							 true,
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
	/*
	 * VC34 (pg_accel's §3.29): a fetch's rows of a granule decoded where they
	 * lie, where ClickHouse's reader made a whole granule of every column --
	 * ClickBench's Q23 fetched each of its ten rows' 104 late columns so
	 */
	DefineCustomBoolVariable("ch.enable_point_reads",
							 "Whether a fetch by TID reads a mergetree part's rows of a granule where they lie.",
							 "On, each column's value is decoded from the granule's streams, their marks kept in "
							 "shared memory for every process, a granule read whole only for a second row of it; "
							 "off, ClickHouse's reader makes the row's granule of every column.",
							 &ch_enable_point_reads,
							 true,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);

	/*
	 * VC41 (pg_accel's §3.38): a scan's part readers take a merge's reader
	 * settings, which save no marks in the process's mark cache, so that
	 * each decompressed its part's marks again -- a reader a run where the
	 * condition cache cut a unit into runs, 30% of ClickBench's Q1 at 100M
	 * rows
	 */
	DefineCustomBoolVariable("ch.enable_scan_marks",
							 "Whether a scan's readers of a mergetree part take its marks from the process's mark cache.",
							 "On, a scan loads the part's marks of its columns into the mark cache before its "
							 "readers, which then find them there, each part's marks decompressed once in the "
							 "process; off, each reader -- a run's, a step's -- decompresses them itself.",
							 &ch_enable_scan_marks,
							 true,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);

	/*
	 * VC43 (pg_accel's §3.38, F25): a mergetree table's min/max skip indexes
	 * -- every numeric and temporal column's, made by default -- read where a
	 * scan's granules are chosen, by its quals and by a running bound
	 */
	/*
	 * VC42 (pg_accel's §3.38): a parallel scan's participants each claim the
	 * units of a part of their own first -- off by default: at 100M rows Q1
	 * and Q7 gained 6 to 10%, the scans of URL and Title lost 5 to 15%, the
	 * participants following the units left, not the work left
	 */
	DefineCustomBoolVariable("ch.parallel_part_affinity",
							 "Whether a parallel scan's participants claim the units of a part of their own first.",
							 "On, each participant claims its part's units until it runs out, then the part with "
							 "the most left, so that each of a query's processes loads, attaches and reads the "
							 "marks of a few parts; off, every participant the next unit of all, every part's.",
							 &ch_parallel_part_affinity,
							 false,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);
	DefineCustomBoolVariable("ch.parallel_key_units",
							 "Whether a parallel scan's units are cut over the granules its filter keeps.",
							 "On, a parallel scan whose quals keep some of the parts' granules cuts its units of "
							 "those granules alone, by the sizes it cuts a whole scan's by, so that a narrow range "
							 "of the key has units for its participants; off, over every part's granules.",
							 &ch_parallel_key_units,
							 true,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);
	DefineCustomIntVariable("ch.parallel_unit_min_bytes",
							"The least of a parallel scan's units cut over the granules its filter keeps, by its "
							"columns' bytes decoded.",
							"A unit within a compressed block decodes the block whole: a scan of a few narrow "
							"columns, cut finely, decodes each block once a unit.  At least VC36's least of rows "
							"too; 0: by rows alone.",
							&ch_parallel_unit_min_bytes,
							4096, 0, INT_MAX / 1024,
							PGC_USERSET, GUC_UNIT_KB,
							NULL, NULL, NULL);
	DefineCustomBoolVariable("ch.enable_char_counts",
							 "Reads a text column's character counts in place of its values where vexec needs no more.",
							 "A storage's NOT NULL text columns keep each value's characters in a hidden column of "
							 "the parts (the table's option char_counts, on by default for a table made now); a "
							 "scan that only takes their length() or compares them with '' reads that alone, no "
							 "value's bytes (pg_accel's pg_vector_executor.md §3.38, F35 and F37).",
							 &ch_enable_char_counts,
							 true,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);
	DefineCustomBoolVariable("ch.enable_count_from_defaults",
							 "Answers count(*) under a column's <> its default from each part's exact figures.",
							 "A vector aggregate's count(*) over a scan whose one qual is exactly a column's not "
							 "being its type's default -- a NOT NULL integer's <> 0, a text's <> '', a boolean -- "
							 "takes each whole part's rows less its rows of the default, ClickHouse's exact count, "
							 "kept with the part's shared metadata; a part with neither is read (pg_accel's "
							 "pg_vector_executor.md §3.38, F36).",
							 &ch_enable_count_from_defaults,
							 true,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);
	DefineCustomBoolVariable("ch.enable_sparse_quals",
							 "Whether a scan leaves a sparse column's rows of its default out by the quals false of it.",
							 "On, a part's column of the sparse serialization -- its rows of other than the type's "
							 "default listed apart -- has its rows of the default left out of a batch's visible "
							 "rows where one of the scan's quals is false of the default and of NULL; off, every "
							 "row is handed and the quals judge it.",
							 &ch_enable_sparse_quals,
							 true,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);
	DefineCustomBoolVariable("ch.enable_skip_indexes",
							 "Whether a scan chooses a mergetree part's granules by its min/max skip indexes too.",
							 "On, a scan's quals and a running bound vexec's sort gives it pass over the granules "
							 "whose least and greatest values leave them no row, after the primary key; off, by "
							 "the primary key alone, as before.  The indexes are made either way.",
							 &ch_enable_skip_indexes,
							 true,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);
	DefineCustomBoolVariable("ch.enable_part_summaries",
							 "Whether the planner and a scan take a part's columns' sizes and statistics from "
							 "its summary in ch.parts, and a table of its own files' part its file's footer.",
							 "Off, a part is loaded through ClickHouse, in each process, the first time they are "
							 "asked of it, and a part's file's footer read; its summary is written either way.",
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
	DefineCustomIntVariable("ch.own_parquet_dictionary_filter",
							"The largest dictionary page of a Parquet column a filter is checked against to pass "
							"a row group of a table of its own files over.",
							"ClickHouse's input_format_parquet_dictionary_filter_push_down for these tables, "
							"where neither ch.engine_settings nor the table's settings name it, read as a "
							"process opens the table; 0 checks none, -1 leaves ClickHouse's own limit, 1 MB.",
							&ch_own_parquet_dictionary_filter,
							64 * 1024, -1, INT_MAX,
							PGC_USERSET, GUC_UNIT_BYTE,
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
