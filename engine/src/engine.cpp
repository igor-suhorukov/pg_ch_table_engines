/*
 * The process-wide ClickHouse context of libchte_engine.so (ch_table_engines.md §2.2):
 * what clickhouse-local sets up (programs/local/LocalServer.cpp at the tag), minus everything
 * that would start threads, listen, or read configuration files.
 */
#include "internal.h"

#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Common/Exception.h>
#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/StoragePolicy.h>
#include <Common/ThreadStatus.h>
#include <Core/Settings.h>
#include <Functions/registerFunctions.h>
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
        MainThreadStatus::getInstance();

        Poco::AutoPtr<CallbackChannel> channel(new CallbackChannel);
        int level = Poco::Logger::parseLevel(options->log_level && *options->log_level ? options->log_level : "warning");
        Poco::Logger::root().setChannel(channel);
        Poco::Logger::root().setLevel(level);
        Poco::Logger::setChannel("", channel);
        Poco::Logger::setLevel("", level);

        registerFunctions();
        registerAggregateFunctions();

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

        engine->global->setMarkCache("SLRU", 64 << 20, 0.5);
        engine->global->setIndexMarkCache("SLRU", 16 << 20, 0.5);
        engine->global->setPrimaryIndexCache("SLRU", 16 << 20, 0.5);

        applySingleThreadedSettings(engine->global);
        applySettings(engine->global, options->settings);

        /// MergeTreeData's constructor initializes the global background executors
        /// (MergeTreeData.cpp:775), each of which starts its threads at once
        /// (MergeTreeBackgroundExecutor.cpp:90-91).  Nothing is ever scheduled on them here;
        /// they get one thread each, started with every signal blocked so that the server's
        /// signals keep going to the thread that handles them.
        for (const char * name : {"background_pool_size", "background_move_pool_size",
                                  "background_fetches_pool_size", "background_common_pool_size"})
            engine->global->setServerSetting(name, Field(UInt64(1)));
        engine->global->setServerSetting("background_merges_mutations_concurrency_ratio", Field(Float64(1)));

        sigset_t all;
        sigset_t old;
        sigfillset(&all);
        pthread_sigmask(SIG_BLOCK, &all, &old);
        try
        {
            engine->global->initializeBackgroundExecutorsIfNeeded();
        }
        catch (...)
        {
            pthread_sigmask(SIG_SETMASK, &old, nullptr);
            throw;
        }
        pthread_sigmask(SIG_SETMASK, &old, nullptr);

        state = engine.release();
        /// Static destructors join the global thread pool's threads (ThreadPool.cpp:700-743),
        /// so the executors must have ended theirs first; see chte_shutdown.
        atexit(chte_shutdown);
    });
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
        /// The executors first: their threads are the ones exit() would wait for.
        global->getMergeMutateExecutor()->wait();
        global->getMovesExecutor()->wait();
        global->getFetchesExecutor()->wait();
        global->getCommonExecutor()->wait();
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
