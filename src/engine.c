/*-------------------------------------------------------------------------
 *
 * engine.c
 *	  Loading libchte_engine.so: mapped in the postmaster as it preloads the
 *	  extension (VC12), initialized in a backend or worker on first use,
 *	  never in the postmaster; and raising its errors as PostgreSQL's
 *	  (ch_table_engines.md §2.2, "How it is loaded", "Its errors").
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <dlfcn.h>

#include "access/xact.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "common/file_utils.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/elog.h"

#include "chte.h"
#include "engine.h"

char	   *ch_engine_library = NULL;
char	   *ch_engine_log_level = NULL;
char	   *ch_engine_settings = NULL;
bool		ch_engine_preload = true;
int			ch_engine_functions = CH_FUNCTIONS_USED;

static ChEngine engine;
static bool loaded = false;

/* the postmaster's mapping of the library, which its children inherit */
static void *mapped_handle = NULL;
static char mapped_path[MAXPGPATH];

/* ClickHouse's error codes this file tells apart (src/Common/ErrorCodes.cpp at the tag) */
#define CH_CANNOT_PARSE_TEXT			6
#define CH_NO_SUCH_COLUMN_IN_TABLE		16
#define CH_CANNOT_READ_ALL_DATA			33
#define CH_BAD_ARGUMENTS				36
#define CH_CHECKSUM_DOESNT_MATCH		40
#define CH_ILLEGAL_TYPE_OF_ARGUMENT		43
#define CH_ILLEGAL_COLUMN				44
#define CH_UNKNOWN_IDENTIFIER			47
#define CH_NOT_IMPLEMENTED				48
#define CH_UNKNOWN_TYPE					50
#define CH_TYPE_MISMATCH				53
#define CH_UNKNOWN_STORAGE				56
#define CH_SYNTAX_ERROR					62
#define CH_INCORRECT_QUERY				80
#define CH_UNKNOWN_SETTING				115
#define CH_INCORRECT_DATA				117
#define CH_CANNOT_ALLOCATE_MEMORY		173
#define CH_MEMORY_LIMIT_EXCEEDED		241
#define CH_NOT_ENOUGH_SPACE				243
#define CH_CORRUPTED_DATA				246
#define CH_CANNOT_DECOMPRESS			271
#define CH_QUERY_WAS_CANCELLED			394
#define CH_INVALID_SETTING_VALUE		471
#define CH_DATA_TYPE_CANNOT_BE_USED_IN_KEY 549
#define CH_THERE_IS_NO_COLUMN			8
#define CH_CANNOT_OPEN_FILE				76
#define CH_FILE_DOESNT_EXIST			107
#define CH_CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN 349
#define CH_CANNOT_STAT					400
#define CH_CANNOT_FSTAT					531
#define CH_UNICODE_ERROR				760
#define CH_FILE_CHANGED_DURING_READ		1015

/*
 * Polled by ClickHouse's code between blocks.  Only the interrupts that end a
 * query stop a call; CHECK_FOR_INTERRUPTS() then raises them once the call
 * has returned and ClickHouse's stack is unwound.
 */
static int
engine_interrupted(void *arg)
{
	return QueryCancelPending || ProcDiePending;
}

/*
 * ClickHouse's log messages, on the calling thread.  elog() at these levels
 * does not raise, but a failure inside it must not longjmp through C++
 * frames, so it is caught here.
 */
static void
engine_log(void *arg, int level, const char *message)
{
	int			pglevel;
	MemoryContext oldcxt = CurrentMemoryContext;

	/* Poco: 1 fatal, 2 critical, 3 error, 4 warning, 5 notice, 6 information, 7 debug, 8 trace */
	if (level <= 3)
		pglevel = LOG;
	else if (level <= 5)
		pglevel = DEBUG1;
	else if (level == 6)
		pglevel = DEBUG2;
	else
		pglevel = DEBUG3;

	PG_TRY();
	{
		elog(pglevel, "engine: %s", message);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(oldcxt);
		FlushErrorState();
	}
	PG_END_TRY();
}

static void
engine_on_exit(int code, Datum arg)
{
	if (loaded)
		engine.shutdown();
}

#define LOAD(field, symbol) \
	do { \
		*(void **) &engine.field = dlsym(handle, symbol); \
		if (engine.field == NULL) \
			ereport(ERROR, \
					(errcode(ERRCODE_UNDEFINED_FUNCTION), \
					 errmsg("engine library \"%s\" has no symbol %s", path, symbol))); \
	} while (0)

/* The process's temporary directory, as it exits */
static void
remove_tmp_dir(int code, Datum arg)
{
	(void) rmtree(DatumGetPointer(arg), true);
}

/* The library's file, as ch.engine_library names it */
static void
engine_path(char *path)
{
	if (ch_engine_library == NULL || ch_engine_library[0] == '\0' ||
		strcmp(ch_engine_library, "$libdir/libchte_engine.so") == 0)
		snprintf(path, MAXPGPATH, "%s/libchte_engine.so", pkglib_path);
	else
		strlcpy(path, ch_engine_library, MAXPGPATH);
}

/*
 * In the postmaster, as it preloads the extension (VC12): the library mapped
 * and relocated once, its static initializers run -- which start no thread
 * and leave nothing a child could not inherit (§2.2) -- so that every
 * process forked from it finds it so, where each mapped it again before, 4
 * to 11 ms a parallel worker.  Nothing of it is called: ClickHouse's context,
 * executors and their threads are each process's own, made as it first needs
 * them (ch_engine()).  A library that cannot be loaded here is left to the
 * processes, which raise their error as they first need it.
 */
void
ch_engine_map(void)
{
	char		path[MAXPGPATH];

	Assert(!IsUnderPostmaster);
	engine_path(path);
	mapped_handle = dlopen(path, RTLD_NOW | RTLD_LOCAL);
	if (mapped_handle == NULL)
	{
		ereport(WARNING,
				(errcode(ERRCODE_UNDEFINED_FILE),
				 errmsg("could not map the engine library \"%s\" in the postmaster: %s", path, dlerror()),
				 errdetail("Each process loads it as it first needs it.")));
		return;
	}
	strlcpy(mapped_path, path, sizeof(mapped_path));
}

const ChEngine *
ch_engine(void)
{
	char		path[MAXPGPATH];
	char		work[MAXPGPATH];
	static char tmp[MAXPGPATH];
	void	   *handle;
	chte_init_options options;
	chte_error	err;

	if (loaded)
		return &engine;

	if (!IsUnderPostmaster && !IsBootstrapProcessingMode() && IsPostmasterEnvironment)
		elog(ERROR, "the engine library is never initialized in the postmaster");

	engine_path(path);
	/* the postmaster's mapping, where the setting still names its file */
	if (mapped_handle != NULL && strcmp(path, mapped_path) == 0)
		handle = mapped_handle;
	else
		handle = dlopen(path, RTLD_NOW | RTLD_LOCAL);
	if (handle == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FILE),
				 errmsg("could not load the engine library \"%s\": %s", path, dlerror())));

	LOAD(init, "chte_init");
	LOAD(shutdown, "chte_shutdown");
	LOAD(thread_count, "chte_thread_count");
	LOAD(table_open, "chte_table_open");
	LOAD(table_close, "chte_table_close");
	LOAD(write_begin, "chte_write_begin");
	LOAD(write_batch, "chte_write_batch");
	LOAD(write_finish, "chte_write_finish");
	LOAD(write_part, "chte_write_part");
	LOAD(write_commit, "chte_write_commit");
	LOAD(write_close, "chte_write_close");
	LOAD(read_begin, "chte_read_begin");
	LOAD(read_next, "chte_read_next");
	LOAD(read_vector, "chte_read_vector");
	LOAD(read_end, "chte_read_end");
	LOAD(read_present, "chte_read_present");
	LOAD(merge, "chte_merge");
	LOAD(merge_select, "chte_merge_select");
	LOAD(insert_delay, "chte_insert_delay");
	LOAD(drop_part, "chte_drop_part");
	LOAD(pages_attach, "chte_pages_attach");
	LOAD(pages_detach, "chte_pages_detach");
	LOAD(filter_create, "chte_filter_create");
	LOAD(filter_free, "chte_filter_free");
	LOAD(part_select, "chte_part_select");
	LOAD(part_column_bytes, "chte_part_column_bytes");
	LOAD(part_stats, "chte_part_stats");
	LOAD(read_retain, "chte_read_retain");
	LOAD(batch_release, "chte_batch_release");
	LOAD(part_locate, "chte_part_locate");
	LOAD(read_rows, "chte_read_rows");
	LOAD(rewrite_part, "chte_rewrite_part");
	/* optional: a library built before VC5 hands no dictionary, and its columns are expanded */
	*(void **) &engine.read_keep_dictionaries = dlsym(handle, "chte_read_keep_dictionaries");
	*(void **) &engine.read_dictionary = dlsym(handle, "chte_read_dictionary");
	if (engine.read_keep_dictionaries == NULL || engine.read_dictionary == NULL)
	{
		engine.read_keep_dictionaries = NULL;
		engine.read_dictionary = NULL;
	}
	/* optional: a library built before VC12 registers every function in chte_init */
	*(void **) &engine.register_all_functions = dlsym(handle, "chte_register_all_functions");
	*(void **) &engine.function_count = dlsym(handle, "chte_function_count");
	/* optional: a library built before VC2 reads no step, its columns all at once */
	*(void **) &engine.read_begin_steps = dlsym(handle, "chte_read_begin_steps");
	*(void **) &engine.read_step = dlsym(handle, "chte_read_step");
	if (engine.read_begin_steps == NULL || engine.read_step == NULL)
	{
		engine.read_begin_steps = NULL;
		engine.read_step = NULL;
	}
	/*
	 * optional: a library built before VC18 takes a sink's rows through
	 * chte_write_batch, and writes a pages-store part into files first
	 */
	*(void **) &engine.write_columns = dlsym(handle, "chte_write_columns");
	*(void **) &engine.write_block = dlsym(handle, "chte_write_block");
	*(void **) &engine.write_vector = dlsym(handle, "chte_write_vector");
	*(void **) &engine.write_reserve = dlsym(handle, "chte_write_reserve");
	if (engine.write_columns == NULL || engine.write_block == NULL || engine.write_vector == NULL ||
		engine.write_reserve == NULL)
	{
		engine.write_columns = NULL;
		engine.write_block = NULL;
		engine.write_vector = NULL;
		engine.write_reserve = NULL;
	}
	*(void **) &engine.write_in_memory = dlsym(handle, "chte_write_in_memory");
	*(void **) &engine.write_part_files = dlsym(handle, "chte_write_part_files");
	if (engine.write_in_memory == NULL || engine.write_part_files == NULL)
	{
		engine.write_in_memory = NULL;
		engine.write_part_files = NULL;
	}
	/*
	 * optional: a library built before VC20's B6 keeps no part's summary, and
	 * every part is loaded where its sizes and statistics are asked
	 */
	*(void **) &engine.write_part_summary = dlsym(handle, "chte_write_part_summary");
	*(void **) &engine.summary_column_bytes = dlsym(handle, "chte_summary_column_bytes");
	*(void **) &engine.summary_stats = dlsym(handle, "chte_summary_stats");
	*(void **) &engine.filter_summary_select = dlsym(handle, "chte_filter_summary_select");

	/*
	 * CH7's, optional: a library built before them lacks them, and the
	 * external access methods refuse their tables (external.c)
	 */
	*(void **) &engine.external_open = dlsym(handle, "chte_external_open");
	*(void **) &engine.external_schema = dlsym(handle, "chte_external_schema");
	*(void **) &engine.external_list = dlsym(handle, "chte_external_list");
	*(void **) &engine.external_listing_bytes = dlsym(handle, "chte_external_listing_bytes");
	*(void **) &engine.external_listing_load = dlsym(handle, "chte_external_listing_load");
	*(void **) &engine.external_listing_free = dlsym(handle, "chte_external_listing_free");
	*(void **) &engine.external_files = dlsym(handle, "chte_external_files");
	*(void **) &engine.external_file_info = dlsym(handle, "chte_external_file_info");
	*(void **) &engine.external_select = dlsym(handle, "chte_external_select");
	*(void **) &engine.external_column_bytes = dlsym(handle, "chte_external_column_bytes");
	*(void **) &engine.external_stats = dlsym(handle, "chte_external_stats");
	*(void **) &engine.external_read_begin = dlsym(handle, "chte_external_read_begin");
	*(void **) &engine.external_read_rows = dlsym(handle, "chte_external_read_rows");
	*(void **) &engine.file_write_begin = dlsym(handle, "chte_file_write_begin");
	*(void **) &engine.file_write_batch = dlsym(handle, "chte_file_write_batch");
	*(void **) &engine.file_write_finish = dlsym(handle, "chte_file_write_finish");
	*(void **) &engine.file_write_close = dlsym(handle, "chte_file_write_close");
	if (engine.write_part_summary == NULL || engine.summary_column_bytes == NULL ||
		engine.summary_stats == NULL || engine.filter_summary_select == NULL)
	{
		engine.write_part_summary = NULL;
		engine.summary_column_bytes = NULL;
		engine.summary_stats = NULL;
		engine.filter_summary_select = NULL;
	}

	memset(&options, 0, sizeof(options));
	options.abi_version = CHTE_ABI_VERSION;
	snprintf(path, sizeof(path), "%s/", DataDir);
	options.data_path = path;
	snprintf(work, sizeof(work), "%s/pg_chte/", DataDir);
	options.work_path = work;
	/*
	 * The process's own temporary directory, removed as it exits; one a crash
	 * leaves goes at the next start, its name PostgreSQL's temporary files'
	 * (fd.c, RemovePgTempFiles()).
	 */
	snprintf(tmp, sizeof(tmp), "%s/base/%s", DataDir, PG_TEMP_FILES_DIR);
	(void) MakePGDirectory(tmp);
	snprintf(tmp, sizeof(tmp), "%s/base/%s/%schte_%d/", DataDir, PG_TEMP_FILES_DIR, PG_TEMP_FILE_PREFIX, MyProcPid);
	if (MakePGDirectory(tmp) < 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create directory \"%s\": %m", tmp)));
	on_proc_exit(remove_tmp_dir, PointerGetDatum(tmp));
	options.tmp_path = tmp;
	options.log_level = ch_engine_log_level;
	options.settings = ch_engine_settings;
	options.callbacks.interrupted = engine_interrupted;
	options.callbacks.log = engine_log;

	if (engine.init(&options, &err) != 0)
		ch_engine_raise(&err, "initializing the engine library");

	loaded = true;
	/* before exit() runs static destructors, which would wait for the executors' threads */
	on_proc_exit(engine_on_exit, (Datum) 0);
	if (ch_engine_functions == CH_FUNCTIONS_ALL && engine.register_all_functions != NULL &&
		engine.register_all_functions(&err) != 0)
		ch_engine_raise(&err, "registering ClickHouse's functions");
	return &engine;
}

bool
ch_engine_loaded(void)
{
	return loaded;
}

void
ch_engine_raise(const chte_error *err, const char *what)
{
	int			sqlstate;

	/* a PostgreSQL error the pages store's read_page caught, as it was */
	ch_pages_raise_pending();
	switch (err->code)
	{
		case CH_QUERY_WAS_CANCELLED:
			CHECK_FOR_INTERRUPTS();
			sqlstate = ERRCODE_QUERY_CANCELED;
			break;
		case CH_CHECKSUM_DOESNT_MATCH:
		case CH_CORRUPTED_DATA:
		case CH_CANNOT_READ_ALL_DATA:
		case CH_CANNOT_DECOMPRESS:
		case CH_INCORRECT_DATA:
			sqlstate = ERRCODE_DATA_CORRUPTED;
			break;
		case CH_MEMORY_LIMIT_EXCEEDED:
		case CH_CANNOT_ALLOCATE_MEMORY:
			sqlstate = ERRCODE_OUT_OF_MEMORY;
			break;
		case CH_NOT_ENOUGH_SPACE:
			sqlstate = ERRCODE_DISK_FULL;
			break;
		case CH_NOT_IMPLEMENTED:
			sqlstate = ERRCODE_FEATURE_NOT_SUPPORTED;
			break;
		case CH_SYNTAX_ERROR:
		case CH_CANNOT_PARSE_TEXT:
			sqlstate = ERRCODE_SYNTAX_ERROR;
			break;
		case CH_TYPE_MISMATCH:
			sqlstate = ERRCODE_DATATYPE_MISMATCH;
			break;
		case CH_NO_SUCH_COLUMN_IN_TABLE:
		case CH_UNKNOWN_IDENTIFIER:
		case CH_THERE_IS_NO_COLUMN:
			sqlstate = ERRCODE_UNDEFINED_COLUMN;
			break;
		case CH_FILE_DOESNT_EXIST:
			sqlstate = ERRCODE_UNDEFINED_FILE;
			break;
		case CH_CANNOT_OPEN_FILE:
		case CH_CANNOT_STAT:
		case CH_CANNOT_FSTAT:
			sqlstate = ERRCODE_IO_ERROR;
			break;
		case CH_FILE_CHANGED_DURING_READ:
			sqlstate = ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE;
			break;
		case CH_CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN:
			sqlstate = ERRCODE_NOT_NULL_VIOLATION;
			break;
		case CH_UNICODE_ERROR:
			sqlstate = ERRCODE_CHARACTER_NOT_IN_REPERTOIRE;
			break;
		case CH_BAD_ARGUMENTS:
		case CH_ILLEGAL_TYPE_OF_ARGUMENT:
		case CH_ILLEGAL_COLUMN:
		case CH_UNKNOWN_TYPE:
		case CH_UNKNOWN_STORAGE:
		case CH_INCORRECT_QUERY:
		case CH_UNKNOWN_SETTING:
		case CH_INVALID_SETTING_VALUE:
		case CH_DATA_TYPE_CANNOT_BE_USED_IN_KEY:
			sqlstate = ERRCODE_INVALID_PARAMETER_VALUE;
			break;
		default:
			sqlstate = ERRCODE_INTERNAL_ERROR;
			break;
	}
	ereport(ERROR,
			(errcode(sqlstate),
			 errmsg("%s: %s", what, err->message),
			 errdetail("ClickHouse error code %d.", err->code)));
	pg_unreachable();
}

/*
 * The engine library's readers, writers, filters and retained batches belong
 * to the resource owner they were made under -- a portal's, for a scan -- and
 * are freed when it is released: an error or an aborted subtransaction leaves
 * no ClickHouse object or open file behind, and a cursor's reader outlives a
 * savepoint rolled back inside the cursor's transaction.
 */
static void
reader_release(Datum res)
{
	if (loaded)
		engine.read_end((chte_reader *) DatumGetPointer(res));
}

static void
writer_release(Datum res)
{
	if (loaded)
		engine.write_close((chte_writer *) DatumGetPointer(res));
}

static void
filter_release(Datum res)
{
	if (loaded)
		engine.filter_free((chte_filter *) DatumGetPointer(res));
}

static void
batch_release(Datum res)
{
	if (loaded)
		engine.batch_release((chte_batch *) DatumGetPointer(res));
}

/* a scan whose end did not come: off the running scans, its memory freed with its context */
static void
scan_release(Datum res)
{
	ch_scan_forget(DatumGetPointer(res));
}

/* a file the tests' writer did not finish: removed */
static void
file_writer_release(Datum res)
{
	if (loaded)
		engine.file_write_close((chte_file_writer *) DatumGetPointer(res));
}

static const ResourceOwnerDesc handle_descs[] = {
	[CH_HANDLE_READER] = {
		.name = "ch_table_engines reader",
		.release_phase = RESOURCE_RELEASE_AFTER_LOCKS,
		.release_priority = RELEASE_PRIO_FIRST,
		.ReleaseResource = reader_release,
	},
	[CH_HANDLE_WRITER] = {
		.name = "ch_table_engines writer",
		.release_phase = RESOURCE_RELEASE_AFTER_LOCKS,
		.release_priority = RELEASE_PRIO_FIRST,
		.ReleaseResource = writer_release,
	},
	[CH_HANDLE_FILTER] = {
		.name = "ch_table_engines filter",
		.release_phase = RESOURCE_RELEASE_AFTER_LOCKS,
		.release_priority = RELEASE_PRIO_FIRST,
		.ReleaseResource = filter_release,
	},
	[CH_HANDLE_BATCH] = {
		.name = "ch_table_engines batch",
		.release_phase = RESOURCE_RELEASE_AFTER_LOCKS,
		.release_priority = RELEASE_PRIO_FIRST,
		.ReleaseResource = batch_release,
	},
	[CH_HANDLE_SCAN] = {
		.name = "ch_table_engines scan",
		.release_phase = RESOURCE_RELEASE_BEFORE_LOCKS,
		.release_priority = RELEASE_PRIO_FIRST,
		.ReleaseResource = scan_release,
	},
	[CH_HANDLE_FILE_WRITER] = {
		.name = "ch_table_engines file writer",
		.release_phase = RESOURCE_RELEASE_AFTER_LOCKS,
		.release_priority = RELEASE_PRIO_FIRST,
		.ReleaseResource = file_writer_release,
	},
};

/* Before a handle is made: the owner then has room for it */
ResourceOwner
ch_handle_reserve(void)
{
	ResourceOwnerEnlarge(CurrentResourceOwner);
	return CurrentResourceOwner;
}

void
ch_handle_opened(ResourceOwner owner, ChHandleKind kind, void *ptr)
{
	ResourceOwnerRemember(owner, PointerGetDatum(ptr), &handle_descs[kind]);
}

void
ch_handle_closing(ResourceOwner owner, ChHandleKind kind, void *ptr)
{
	ResourceOwnerForget(owner, PointerGetDatum(ptr), &handle_descs[kind]);
}
