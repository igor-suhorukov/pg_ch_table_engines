/*
 * The process-wide ClickHouse context of libchte_engine.so (ch_table_engines.md §2.2):
 * what clickhouse-local sets up (programs/local/LocalServer.cpp at the tag), minus everything
 * that would start threads, listen, or read configuration files.
 */
#include "internal.h"

#include <sys/resource.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Compression/CompressionFactory.h>
#include <Core/ServerSettings.h>
#include <DataTypes/DataTypeFactory.h>
#include <Parsers/ParserCreateQuery.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Common/checkStackSize.h>
#include <Common/DateLUT.h>
#include <Common/Exception.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/register_objects.h>
#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/StoragePolicy.h>
#include <Common/ThreadStatus.h>
#include <Core/Settings.h>
#include <Functions/FunctionFactory.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/parseQuery.h>

#include <Poco/Channel.h>
#include <Poco/Logger.h>
#include <Poco/Message.h>
#include <Poco/Util/MapConfiguration.h>
#include <Common/Config/ConfigProcessor.h>

#include <dirent.h>
#include <filesystem>
#include <pthread.h>
#include <signal.h>
#include <string_view>
#include <unordered_set>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
}

namespace fs = std::filesystem;

namespace chte
{

namespace
{

Engine * state = nullptr;
pthread_t main_thread;
chte_callbacks callbacks{};

/// VC12: ClickHouse registers its functions and aggregate functions as a process starts
/// (registerFunctions.cpp, registerAggregateFunctions.cpp), 4.3 of chte_init's 5.2 ms; here first only
/// the registrations -- REGISTER_FUNCTION's names (Common/register_objects.h) -- of the functions the
/// filters name (select.cpp, FilterBuilder) and ClickHouse's key analysis adds to them
/// (KeyCondition.cpp:747-749, 1323), and a sorting key's tuple.
constexpr std::string_view used_registrations[] = {
    "Equals", "NotEquals", "Less", "LessOrEquals", "Greater", "GreaterOrEquals",
    "Logical", "IsNull", "IsNotNull", "Tuple",
};
std::unordered_set<std::string_view> registered;
bool all_registered = false;
bool used_registered = false;	/* VC22: in the postmaster, inherited */

void registerUsedFunctions()
{
    if (used_registered)
        return;
    used_registered = true;
    /// as registerFunctions() does: what registration allocates is not a call's
    MemoryTrackerBlockerInThread blocker;
    auto & factory = FunctionFactory::instance();
    const auto & registrations = FunctionRegisterMap::instance();
    for (auto name : used_registrations)
    {
        auto it = registrations.find(name);
        if (it == registrations.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ClickHouse registers no functions as {}", name);
        it->second(factory);
        registered.insert(it->first);
    }
}

/// ClickHouse's log messages go to the caller's log callback, and only from the thread that
/// called into the library: PostgreSQL's elog is not thread-safe.
class CallbackChannel final : public Poco::Channel
{
public:
    void log(const Poco::Message & msg) override
    {
        if (callbacks.log && pthread_equal(pthread_self(), main_thread))
            callbacks.log(callbacks.arg, msg.getPriority(), msg.getText().c_str());
    }
};

}

/// Settings that keep a query, an insert and a merge on the calling thread.
void applySingleThreadedSettings(ContextMutablePtr & context)
{
    context->setSetting("max_threads", Field(UInt64(1)));
    context->setSetting("max_insert_threads", Field(UInt64(1)));
    context->setSetting("max_final_threads", Field(UInt64(1)));
    context->setSetting("max_threads_for_indexes", Field(UInt64(1)));
    context->setSetting("max_download_threads", Field(UInt64(1)));
    context->setSetting("max_parsing_threads", Field(UInt64(1)));
    context->setSetting("local_filesystem_read_method", Field(String("pread")));
    context->setSetting("remote_filesystem_read_method", Field(String("read")));
    context->setSetting("local_filesystem_read_prefetch", Field(false));
    context->setSetting("remote_filesystem_read_prefetch", Field(false));
    context->setSetting("allow_prefetched_read_pool_for_local_filesystem", Field(false));
    context->setSetting("allow_prefetched_read_pool_for_remote_filesystem", Field(false));
}

void applySettings(ContextMutablePtr & context, const char * settings)
{
    if (!settings || !*settings)
        return;
    String text = settings;
    ParserSetQuery parser(/* parse_only_internals= */ true);
    ASTPtr ast = parseQuery(parser, text, "settings", DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH,
                            DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    context->applySettingsChanges(ast->as<ASTSetQuery &>().changes);
}

void registerAllFunctions()
{
    if (all_registered)
        return;
    MemoryTrackerBlockerInThread blocker;
    auto & factory = FunctionFactory::instance();
    for (const auto & [name, registration] : FunctionRegisterMap::instance())
        if (!registered.contains(name))
        {
            registration(factory);
            registered.insert(name);
        }
    registerAggregateFunctions();
    all_registered = true;
}

bool allFunctionsRegistered()
{
    return all_registered;
}

bool functionsRegistered(const Names & names)
{
    for (const auto & name : names)
        if (!FunctionFactory::instance().has(name) && !AggregateFunctionFactory::instance().isAggregateFunctionName(name))
            return false;
    return true;
}

Engine & engine()
{
    if (!state)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "chte_init has not been called");
    return *state;
}

void checkInterrupted()
{
    if (callbacks.interrupted && callbacks.interrupted(callbacks.arg))
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "canceled by the server");
}

void setError(chte_error * error, int code, const std::string & message) noexcept
{
    if (!error)
        return;
    error->code = code;
    size_t n = std::min(message.size(), sizeof(error->message) - 1);
    memcpy(error->message, message.data(), n);
    error->message[n] = '\0';
}

}

using namespace chte;

extern "C" int chte_init(const chte_init_options * options, chte_error * error)
{
    return guarded(error, [&]
    {
        if (!options || options->abi_version != CHTE_ABI_VERSION)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_init: ABI version {} expected", CHTE_ABI_VERSION);
        if (state)
            return;
        if (!options->data_path || !*options->data_path)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_init: data_path is required");

        main_thread = pthread_self();
        callbacks = options->callbacks;

        /// The open files' soft limit raised to the hard one, as ClickHouse's server raises it
        /// (Server.cpp, max_open_files): a merge of wide parts of the files store opens every column's
        /// data and marks of every part at once.  PostgreSQL's own budget of descriptors, set at the
        /// postmaster's start (fd.c, max_safe_fds), stays as it is.
        {
            rlimit rlim{};
            if (getrlimit(RLIMIT_NOFILE, &rlim) == 0 && rlim.rlim_cur < rlim.rlim_max)
            {
                rlim.rlim_cur = rlim.rlim_max;
                (void) setrlimit(RLIMIT_NOFILE, &rlim);
            }
        }
        MainThreadStatus::getInstance();

        Poco::AutoPtr<CallbackChannel> channel(new CallbackChannel);
        int level = Poco::Logger::parseLevel(options->log_level && *options->log_level ? options->log_level : "warning");
        Poco::Logger::root().setChannel(channel);
        Poco::Logger::root().setLevel(level);
        Poco::Logger::setChannel("", channel);
        Poco::Logger::setLevel("", level);

        registerUsedFunctions();

        auto engine = std::make_unique<Engine>();
        engine->shared = Context::createShared();
        engine->global = Context::createGlobal(engine->shared.get());
        engine->global->makeGlobalContext();
        /// Before the application type, which loads the server settings from the context's config,
        /// or else from Poco's Application, which this process does not have (Context.cpp:7705-7712).
        engine->global->setConfig(ConfigurationPtr(new Poco::Util::MapConfiguration));
        engine->global->setApplicationType(Context::ApplicationType::LOCAL);
        /// ClickHouse's own state (user_defined/, tmp/, metadata/) in its work directory; an
        /// empty user_defined/ there spares a stat() of it on every function lookup
        /// (UserDefinedSQLObjectsDiskStorage.cpp:133).
        String work_path = options->work_path && *options->work_path ? options->work_path : options->data_path;
        fs::create_directories(fs::path(work_path) / "user_defined");
        engine->global->setPath(work_path);
        engine->parts_disk = std::make_shared<DiskLocal>("chte", options->data_path);
        engine->policy = std::make_shared<StoragePolicy>(
            "chte", Volumes{std::make_shared<SingleDiskVolume>("chte", engine->parts_disk)}, 0.0);

        /// a vertical merge keeps its rows' sources in a temporary file (MergeTask.cpp, RowsSourcesTemporaryFile),
        /// in a directory the process alone uses: setting it up clears the temporary files in it
        if (options->tmp_path && *options->tmp_path)
            engine->global->setTemporaryStoragePath(options->tmp_path, /* max_size= */ 0);
        engine->global->setMarkCache("SLRU", 64 << 20, 0.5);
        engine->global->setIndexMarkCache("SLRU", 16 << 20, 0.5);
        engine->global->setPrimaryIndexCache("SLRU", 16 << 20, 0.5);

        applySingleThreadedSettings(engine->global);
        /// ClickHouse's time zone where a type names none, UTC, whatever the host's TZ or /etc/localtime says:
        /// the shim's timestamps are UTC's, and an Iceberg table's day, month and hour partitions of a
        /// timestamp without a zone are UTC's too, which the manifests' pruning computes in it (CH7).
        /// Its tables built by chte_preload in the postmaster, where it preloads the library, and found
        /// here; built here otherwise
        DateLUT::setDefaultTimezone("UTC");
        applySettings(engine->global, options->settings);

        /// The background executors' sizes, for startBackgroundExecutors(): one thread each
        for (const char * name : {"background_pool_size", "background_move_pool_size",
                                  "background_fetches_pool_size", "background_common_pool_size"})
            engine->global->setServerSetting(name, Field(UInt64(1)));
        engine->global->setServerSetting("background_merges_mutations_concurrency_ratio", Field(Float64(1)));

        state = engine.release();
        /// Static destructors join the global thread pool's threads (ThreadPool.cpp:700-743),
        /// so the executors must have ended theirs first; see chte_shutdown.
        atexit(chte_shutdown);
    });
}

namespace
{
bool executors_started = false;
}

/// MergeTreeData's constructor initializes the global background executors (MergeTreeData.cpp:775),
/// each of which starts its threads at once (MergeTreeBackgroundExecutor.cpp:90-91).  Nothing is ever
/// scheduled on them here; they get one thread each (chte_init), started with every signal blocked so that
/// the server's signals keep going to the thread that handles them.  pg_accel's §3.32 F12 (VC38): started
/// as the process makes its first MergeTree table (makeTable()), where chte_init started them -- a process
/// that reads tables of their own files alone starts none, 0.47 Mcycles and four threads a process
void chte::startBackgroundExecutors()
{
    if (executors_started)
        return;
    sigset_t all;
    sigset_t old;
    sigfillset(&all);
    pthread_sigmask(SIG_BLOCK, &all, &old);
    try
    {
        engine().global->initializeBackgroundExecutorsIfNeeded();
    }
    catch (...)
    {
        pthread_sigmask(SIG_SETMASK, &old, nullptr);
        throw;
    }
    pthread_sigmask(SIG_SETMASK, &old, nullptr);
    executors_started = true;
}

extern "C" void chte_shutdown(void)
{
    static bool done = false;
    if (!state || done)
        return;
    done = true;
    auto & global = state->global;
    try
    {
        /// The executors first, where they were started: their threads are the ones exit() would wait for.
        if (executors_started)
        {
            global->getMergeMutateExecutor()->wait();
            global->getMovesExecutor()->wait();
            global->getFetchesExecutor()->wait();
            global->getCommonExecutor()->wait();
        }
    }
    catch (...)
    {
        tryLogCurrentException("chte_shutdown");
    }
    try
    {
        global->shutdown();
    }
    catch (...)
    {
        tryLogCurrentException("chte_shutdown");
    }
    /// An executor's threads are its own pool's, idle until the pool is destroyed with the
    /// shared context; the global context goes first, as clickhouse-local's cleanup has it
    /// (programs/local/LocalServer.cpp:961-967).  A table still open keeps the global context,
    /// and is never used again: the process is exiting.
    try
    {
        global.reset();
        state->shared.reset();
    }
    catch (...)
    {
        tryLogCurrentException("chte_shutdown");
    }
}

/// In the postmaster as it preloads the library (the extension's ch_engine_map()): ClickHouse's date
/// tables of the default time zone chte_init sets, UTC.  DateLUT's constructor builds the table of the
/// zone the host names -- TZ, else /etc/localtime, here Etc/UTC -- and setDefaultTimezone() UTC's under
/// its own name, each the days of 1900 to 2299, about 4.4 ms (DateLUT.h:43-48, DateLUT.cpp:194-232):
/// built in every process's chte_init, the backends' before they plan, every parallel worker's before
/// it reads.  Built here, they are each forked process's, inherited, and chte_init's
/// setDefaultTimezone("UTC") finds them.  DateLUT starts no thread and keeps only its tables and a
/// mutex, which no thread holds as the postmaster forks.
/// VC22 (ch_table_engines.md §8 item 24): and ClickHouse's other statics that start no thread and that
/// no thread holds a lock of as the postmaster forks -- it has none but this one -- made here once,
/// each forked process's then, where every backend and worker made them in its chte_init and its first
/// table's open: the used functions' registrations (VC12), the settings' accessors -- a query's, the
/// server's and MergeTree's, which the global context and a table's settings read -- the factories of
/// data types and codecs a definition's columns are made by, and the parser's of a definition: a fresh
/// backend's chte_init 1.0 -> 0.4 ms, its first table's columns 0.3 -> 0.19.  No context is made here:
/// ClickHouse makes one shared context a process (Context.cpp, ContextSharedPart's constructor).  The
/// threads are counted before and after.
/// pg_accel's §3.32 F12 (VC38), at the user's word: and the main thread's stack bounds that
/// checkStackSize() finds once a thread (Common/checkStackSize.cpp:41, 133-140) -- glibc's
/// pthread_getattr_np() reading /proc/self/maps, 0.37 Mcycles of each process's first table's open --
/// thread-local, constinit, inherited by the main thread of every process forked from here, whose stack
/// is where the postmaster's is.
extern "C" int chte_preload(chte_error * error)
{
    return guarded(error, [&]
    {
        int threads = chte_thread_count();

        checkStackSize();
        DateLUT::setDefaultTimezone("UTC");
        registerUsedFunctions();
        {
            MemoryTrackerBlockerInThread blocker;
            Settings settings;
            ServerSettings server;
            MergeTreeSettings tree;
            (void) DataTypeFactory::instance().get("Nullable(String)");
            (void) CompressionCodecFactory::instance().getDefaultCodec();
            String query = "CREATE TABLE t (a Int32 CODEC(LZ4), b Nullable(String)) ENGINE = MergeTree ORDER BY a";
            ParserCreateQuery parser;
            (void) parseQuery(parser, query, "a definition", DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH,
                              DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        }
        if (chte_thread_count() != threads)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "the preload started {} threads", chte_thread_count() - threads);
    });
}

/// pg_accel's §3.32 F12 (VC38): the process's own directory of temporary files, set where a merge first
/// needs it -- a vertical merge's rows' sources (MergeTask.cpp, RowsSourcesTemporaryFile) -- not by every
/// process's chte_init, which made it and removed it again; setting it up clears the temporary files in it
extern "C" int chte_set_temporary_path(const char * path, chte_error * error)
{
    return guarded(error, [&]
    {
        static String set;
        if (!path || !*path)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_set_temporary_path: a path is required");
        if (set == path)
            return;
        engine().global->setTemporaryStoragePath(path, /* max_size= */ 0);
        set = path;
    });
}

extern "C" int chte_register_all_functions(chte_error * error)
{
    return guarded(error, [&]
    {
        engine();
        registerAllFunctions();
    });
}

extern "C" int chte_function_count(void)
{
    try
    {
        return static_cast<int>(FunctionFactory::instance().getAllRegisteredNames().size()
                                + AggregateFunctionFactory::instance().getAllRegisteredNames().size());
    }
    catch (...)
    {
        return -1;
    }
}

extern "C" uint64_t chte_parts_loaded(void)
{
    return chte::partsLoaded();
}

extern "C" int chte_thread_count(void)
{
    int n = 0;
    if (DIR * dir = opendir("/proc/self/task"))
    {
        while (struct dirent * entry = readdir(dir))
            if (entry->d_name[0] != '.')
                ++n;
        closedir(dir);
    }
    return n;
}
