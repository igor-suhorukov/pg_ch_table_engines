/*-------------------------------------------------------------------------
 *
 * engine.c
 *	  Loading libchte_engine.so in a backend or worker on first use, never in
 *	  the postmaster, and raising its errors as PostgreSQL's
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
#include "storage/ipc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/elog.h"

#include "chte.h"
#include "engine.h"

char	   *ch_engine_library = NULL;
char	   *ch_engine_log_level = NULL;
char	   *ch_engine_settings = NULL;

static ChEngine engine;
static bool loaded = false;

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

const ChEngine *
ch_engine(void)
{
	char		path[MAXPGPATH];
	char		work[MAXPGPATH];
	void	   *handle;
	chte_init_options options;
	chte_error	err;

	if (loaded)
		return &engine;

	if (!IsUnderPostmaster && !IsBootstrapProcessingMode() && IsPostmasterEnvironment)
		elog(ERROR, "the engine library is never loaded in the postmaster");

	if (ch_engine_library == NULL || ch_engine_library[0] == '\0' ||
		strcmp(ch_engine_library, "$libdir/libchte_engine.so") == 0)
		snprintf(path, sizeof(path), "%s/libchte_engine.so", pkglib_path);
	else
		strlcpy(path, ch_engine_library, sizeof(path));

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
	LOAD(drop_part, "chte_drop_part");
	LOAD(pages_attach, "chte_pages_attach");
	LOAD(pages_detach, "chte_pages_detach");

	memset(&options, 0, sizeof(options));
	options.abi_version = CHTE_ABI_VERSION;
	snprintf(path, sizeof(path), "%s/", DataDir);
	options.data_path = path;
	snprintf(work, sizeof(work), "%s/pg_chte/", DataDir);
	options.work_path = work;
	options.log_level = ch_engine_log_level;
	options.settings = ch_engine_settings;
	options.callbacks.interrupted = engine_interrupted;
	options.callbacks.log = engine_log;

	if (engine.init(&options, &err) != 0)
		ch_engine_raise(&err, "initializing the engine library");

	loaded = true;
	/* before exit() runs static destructors, which would wait for the executors' threads */
	on_proc_exit(engine_on_exit, (Datum) 0);
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
		case CH_NO_SUCH_COLUMN_IN_TABLE:
		case CH_UNKNOWN_IDENTIFIER:
			sqlstate = ERRCODE_UNDEFINED_COLUMN;
			break;
		case CH_BAD_ARGUMENTS:
		case CH_ILLEGAL_TYPE_OF_ARGUMENT:
		case CH_ILLEGAL_COLUMN:
		case CH_UNKNOWN_TYPE:
		case CH_TYPE_MISMATCH:
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
 * Readers and writers of the engine library belong to the resource owner they
 * were opened under -- a portal's, for a scan -- and are closed when it is
 * released: an error or an aborted subtransaction leaves no ClickHouse object
 * or open file behind, and a cursor's reader outlives a savepoint rolled back
 * inside the cursor's transaction.
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

static const ResourceOwnerDesc reader_desc = {
	.name = "ch_table_engines reader",
	.release_phase = RESOURCE_RELEASE_AFTER_LOCKS,
	.release_priority = RELEASE_PRIO_FIRST,
	.ReleaseResource = reader_release,
};

static const ResourceOwnerDesc writer_desc = {
	.name = "ch_table_engines writer",
	.release_phase = RESOURCE_RELEASE_AFTER_LOCKS,
	.release_priority = RELEASE_PRIO_FIRST,
	.ReleaseResource = writer_release,
};

/* Before a handle is opened: the owner then has room for it */
ResourceOwner
ch_handle_reserve(void)
{
	ResourceOwnerEnlarge(CurrentResourceOwner);
	return CurrentResourceOwner;
}

void
ch_handle_opened(ResourceOwner owner, bool reader, void *ptr)
{
	ResourceOwnerRemember(owner, PointerGetDatum(ptr), reader ? &reader_desc : &writer_desc);
}

void
ch_handle_closing(ResourceOwner owner, bool reader, void *ptr)
{
	ResourceOwnerForget(owner, PointerGetDatum(ptr), reader ? &reader_desc : &writer_desc);
}
