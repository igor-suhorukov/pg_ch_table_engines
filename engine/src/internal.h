/*
 * Internals of libchte_engine.so (ch_table_engines.md §2.2): the process-wide ClickHouse
 * context, the MergeTreeData subclass that holds no part set, and the guard that keeps
 * exceptions on this side of the C API.
 */
#pragma once

#include <chte_engine.h>

#include <Disks/DiskLocal.h>
#include <Disks/IDisk.h>
#include <Disks/StoragePolicy.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/MarkCache.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/MergeTreeReadersChain.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>

#include <cstring>
#include <functional>
#include <map>
#include <mutex>
#include <set>
#include <string>

namespace DB::ErrorCodes
{
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
    extern const int UNKNOWN_FUNCTION;
}

namespace chte
{

using namespace DB;

/*
 * The pages store's disk: a DiskLocal for what is being written -- parts are
 * written and named as in the files store -- and, for the paths no local file
 * has, the files of committed parts in the relation's pages, from their
 * manifests, read through the caller's read_page.
 */
class PagesDisk final : public DiskLocal
{
public:
    struct File
    {
        UInt64 size;
        UInt32 first_block;
        UInt32 nblocks;
    };

    /// A directory's entries: names, a trailing '/' for a subdirectory, in order.
    using Entries = std::shared_ptr<const std::vector<String>>;

    PagesDisk(const String & name_, const String & path_, const chte_pages_ops & ops_);

    /// A part's files, by their paths below its directory as its manifest has them; each directory's
    /// entries listed here, once a part, not at each listing of the directory (VC20).
    void attachPart(const String & part_dir, const chte_page_file * part_files, size_t nfiles);
    void detachPart(const String & part_dir);
    void readPage(UInt32 block, char * dst) const;
    UInt32 pagePayload() const { return ops.page_payload; }

    /// VC18: while on, a part directory created -- a tmp_insert_ one, an insert's -- and everything
    /// below it are kept in memory, not in the files of the disk's directory: the parts the caller
    /// copies into pages and never commits.  A part in memory goes as its directory is removed.
    void setMemoryWrites(bool on) { memory_writes = on; }
    /// The files below a directory in memory, by their paths below it, in their paths' order.
    std::vector<std::pair<String, std::shared_ptr<const std::string>>> memoryFiles(const String & dir) const;

    void createDirectory(const String & path) override;
    void createDirectories(const String & path) override;
    void moveDirectory(const String & from_path, const String & to_path) override;
    void createFile(const String & path) override;
    void moveFile(const String & from_path, const String & to_path) override;
    void replaceFile(const String & from_path, const String & to_path) override;
    std::unique_ptr<WriteBufferFromFileBase> writeFile(const String & path, size_t buf_size, WriteMode mode,
                                                       const WriteSettings & settings) override;
    void removeFile(const String & path) override;
    void removeFileIfExists(const String & path) override;
    void removeDirectory(const String & path) override;
    void removeDirectoryIfExists(const String & path) override;
    void removeRecursive(const String & path) override;
    void setLastModified(const String & path, const Poco::Timestamp & timestamp) override;
    void setReadOnly(const String & path) override;
    SyncGuardPtr getDirectorySyncGuard(const String & path) const override;

    bool existsFile(const String & path) const override;
    bool existsDirectory(const String & path) const override;
    bool existsFileOrDirectory(const String & path) const override;
    size_t getFileSize(const String & path) const override;
    DirectoryIteratorPtr iterateDirectory(const String & path) const override;
    void listFiles(const String & path, std::vector<String> & file_names) const override;
    Poco::Timestamp getLastModified(const String & path) const override;
    time_t getLastChanged(const String & path) const override;
    void prepareRead(const String & path, const ReadSettings & settings, std::optional<size_t> read_hint,
                     ReadPipeline & pipeline) const override;

private:
    bool hasLocal(const String & path) const;
    const File * findFile(const String & path) const;
    /// The entries of a directory of a part in pages; none where it is not one.
    Entries entries(const String & dir) const;
    /// VC18: whether a path is a directory in memory or below one; the entries of one
    bool inMemory(const String & path) const;
    String memoryKey(const String & path) const;
    std::vector<String> memoryEntries(const String & dir) const;

    chte_pages_ops ops;
    /// full relative path of each file of the parts in pages, and their directories' entries
    std::map<String, File> files;
    std::map<String, Entries> dirs;

    bool memory_writes = false;
    mutable std::mutex memory_mutex;
    std::set<String> memory_dirs;
    std::map<String, std::shared_ptr<std::string>> memory_files;
};

struct Engine
{
    SharedContextHolder shared;
    ContextMutablePtr global;
    chte_callbacks callbacks{};
    /// The disk every table's parts are on, rooted at data_path, and its one-volume policy.
    DiskPtr parts_disk;
    StoragePolicyPtr policy;
};

/// The initialized engine; throws if chte_init has not succeeded.
Engine & engine();

/// Throws QUERY_WAS_CANCELLED when the caller's interrupted() callback says so.
void checkInterrupted();

/// ClickHouse's functions a process has (VC12): chte_init registers those the filters and ClickHouse's
/// analysis of them use; the others, and the aggregate functions, as a table's definition, a write, a
/// merge or a rewrite first needs them.  Each is a no-op once done.  The others are the functions the
/// library links, not all of ClickHouse's (engine.cmake, engine.cpp).
void registerAllFunctions();
bool allFunctionsRegistered();
/// Whether the function factory knows every name, an aggregate function's included.
bool functionsRegistered(const Names & names);

void setError(chte_error * error, int code, const std::string & message) noexcept;

/// Runs f; an exception becomes *error and a nonzero result.
template <typename F>
int guarded(chte_error * error, F && f) noexcept
{
    try
    {
        f();
        if (error)
        {
            error->code = 0;
            error->message[0] = '\0';
        }
        return 0;
    }
    catch (...)
    {
        int code = -1;
        std::string message;
        try
        {
            code = getCurrentExceptionCode();
            message = getCurrentExceptionMessage(/* with_stacktrace= */ false);
        }
        catch (...)
        {
            message = "unknown error";
        }
        if (code == 0)
            code = -1;
        /// a function looked up where the first registrations did not foresee it (VC12): every
        /// one from now on, so that the statement succeeds when it is run again -- unless the
        /// library does not link it, when it fails again with the same error
        if ((code == ErrorCodes::UNKNOWN_FUNCTION || code == ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION)
            && !allFunctionsRegistered())
        {
            try
            {
                registerAllFunctions();
            }
            catch (...)
            {
            }
        }
        setError(error, code, message);
        return code;
    }
}

/// A MergeTree table whose parts PostgreSQL keeps track of.  MergeTreeData's own part set
/// stays empty: parts are built from their directories when a call needs them.
class PgMergeTree final : public MergeTreeData
{
public:
    PgMergeTree(
        const StorageID & table_id,
        const String & relative_path,
        const StorageInMemoryMetadata & metadata,
        ContextMutablePtr context_,
        const MergingParams & merging_params,
        std::unique_ptr<MergeTreeSettings> settings,
        std::shared_ptr<PagesDisk> pages_disk_);

    std::string getName() const override { return "PgMergeTree"; }

    /// Parts live under the data path, not the context's (where ClickHouse keeps its own state);
    /// a table of the pages store has a disk of its own.
    StoragePolicyPtr getStoragePolicy() const override { return own_policy ? own_policy : engine().policy; }

    StoragePolicyPtr own_policy;
    std::shared_ptr<PagesDisk> pages_disk;

    /// Part object for an existing directory under the table's path: what a read of it needs
    /// (VC12), without the check of every file's size against its checksums, which a merge and a
    /// rewrite, reading every file, make (checkedPart), and without its projections, which only a
    /// search by row id, a merge and a rewrite read (VC20).
    MutableDataPartPtr loadPart(const String & part_name) const;

    /// A committed part, loaded once: a part's directory never changes once committed, and
    /// its name is never reused in a table's storage.  The pages store's files are attached
    /// before a part is first loaded.
    DataPartPtr getPart(const String & part_name) const;
    /// The same, its projections loaded (VC20): a search by row id's.
    DataPartPtr partWithProjections(const String & part_name) const;
    /// The same, its files checked against its checksums, as ClickHouse checks a part it loads.
    DataPartPtr checkedPart(const String & part_name) const;
    void forgetPart(const String & part_name) const;

    /// While this process loads a part of the table, the table's metadata without its projections,
    /// so that IMergeTreeDataPart::loadProjections() finds none to load (VC20).
    StorageMetadataHandle getInMemoryMetadataPtr(ContextPtr query_context, bool bypass_metadata_cache) const override;

    /// A part's statistics of the named columns, loaded once a column (VC12), where ClickHouse's
    /// getEstimates() loads every column's.  A column without statistics is not in the result.
    Estimates partEstimates(const DataPartPtr & part, const Names & columns) const;
    /// A part's bytes of the named columns, each column's worked out once (VC20), where a part's first
    /// getColumnSize() works out every column's (calculateColumnsSizesOnDisk).
    ColumnSize columnsSize(const String & part_name, const Names & columns) const;

    MergeTreeDataWriter writer;
    MergeTreeDataMergerMutator merger;

    MutationCounters getMutationCounters() const override { return {}; }
    std::map<std::string, MutationCommands> getUnfinishedMutationCommands() const override { return {}; }
    std::vector<MergeTreeMutationStatus> getMutationsStatus() const override { return {}; }
    MutationsSnapshotPtr getMutationsSnapshot(const IMutationsSnapshot::Params & params) const override;
    CursorPromotersMap buildPromoters() override { return {}; }
    void dropPartNoWaitNoThrow(const String &) override { }
    void dropPart(const String &, bool, ContextPtr) override { unsupported("DROP PART"); }
    void dropPartition(const ASTPtr &, bool, ContextPtr) override { unsupported("DROP PARTITION"); }
    PartitionCommandsResultInfo attachPartition(const PartitionCommand &, const StorageMetadataPtr &, ContextPtr) override
    {
        unsupported("ATTACH PARTITION");
    }
    void replacePartitionFrom(const StoragePtr &, const ASTPtr &, bool, ContextPtr) override { unsupported("REPLACE PARTITION"); }
    void movePartitionToTable(const StoragePtr &, const ASTPtr &, ContextPtr) override { unsupported("MOVE PARTITION"); }
    bool partIsAssignedToBackgroundOperation(const DataPartPtr &) const override { return false; }
    void attachRestoredParts(MutableDataPartsVector &&, const std::optional<ZooKeeperRetriesInfo> &) override
    {
        unsupported("RESTORE");
    }
    void startBackgroundMovesIfNeeded() override { }
    std::unique_ptr<MergeTreeSettings> getDefaultSettings() const override;

    /// PostgreSQL runs merges and moves; nothing is ever scheduled in the background.
    bool scheduleDataProcessingJob(BackgroundJobsAssignee &) override { return false; }
    bool scheduleDataMovingJob(BackgroundJobsAssignee &) override { return false; }

private:
    [[noreturn]] static void unsupported(const char * what);

    struct LoadedPart
    {
        MutableDataPartPtr part;
        bool projections = false;    /// its projections loaded
        std::map<String, ColumnSize> column_sizes;    /// columnsSize()'s, so far
    };
    mutable std::map<String, LoadedPart> parts;
    LoadedPart & loadedPart(const String & part_name) const;
    /// the metadata loadPart() loads a part by, and the table's metadata it is a copy of
    mutable StorageMetadataPtr without_projections;
    mutable StorageMetadataPtr without_projections_of;
    /// per part, each column's statistics asked for so far: nullopt where it has none
    mutable std::map<String, std::map<String, std::optional<Estimate>>> estimates;
};

/// The names of a comma-separated list; throws when there is none.
Names columnNames(const char * columns);

}

struct chte_reader;
struct chte_table;

namespace chte
{

/// A reader of a part's named columns over mark_ranges (none: every granule), as chte_read_begin opens.
std::unique_ptr<chte_reader> openReader(chte_table & table, const MergeTreeData::DataPartPtr & part, const Names & names,
                                        std::optional<MarkRanges> mark_ranges);

/// Mark ranges, ascending, as runs of adjacent granules, each for a sequential source of its own; none: the
/// whole part, one run.  MergeTreeSequentialSource takes the rows of a read from the granule after the one
/// it read last (MergeTreeSequentialSource.cpp:205-214, 233): past a gap between ranges that is another
/// granule's count, and a compact part's reader, which reads whole granules only, then hands more rows than
/// asked where the granule read is the longer last one a compact part may end with
/// (MergeTreeDataPartWriterCompact.cpp:514-521).
std::vector<std::optional<MarkRanges>> markRuns(const std::optional<MarkRanges> & mark_ranges);

/// One step of a reader in steps (chte_read_begin_steps, VC2): a reader of its columns over the
/// reader's granules, and the chain that reads a granule of them where the step is asked for.
struct ReadStep
{
    Names names;
    std::vector<size_t> columns;    /// their places among the reader's columns
    MergeTreeReadTaskInfoPtr info;
    MergeTreeReadTask::Readers readers;
    std::optional<MergeTreeReadersChain> chain;
    /// the granule the chain's stream stands at, past one it read last, where it reads on without a
    /// seek; none: the next read seeks
    std::optional<size_t> next_mark;
};

/// The writer's rows as parts, one a partition, under temporary names (chte_write_finish).
void writeParts(chte_writer & writer, ContextPtr context);
/// A part's description for the caller, its directory as it is now.
void fillPartInfo(const IMergeTreeDataPart & part, chte_part_info & info);
/// Gives a finished part its final directory name.
void commitPart(IMergeTreeDataPart & part);

/// Appends value i of a vector of the caller's layout (chte_engine.h) to a ClickHouse column.
void appendValue(IColumn & column, const chte_vector & vector, size_t i);
/// Appends nrows values.
void appendVector(IColumn & column, uint32_t nrows, const chte_vector & vector);
/// A column's values in the caller's layout, pointing into the column.
chte_vector vectorOf(const IColumn & column);

}

struct chte_writer
{
    chte_table * table = nullptr;
    UInt64 block_number = 0;
    DB::Block header;
    DB::MutableColumns columns;
    std::vector<DB::MergeTreeTemporaryPartPtr> parts;
    bool finished = false;
    bool committed = false;

    /// chte_write_vector: a LowCardinality column's values, made full, and the rows it was made of
    std::vector<std::pair<DB::ColumnPtr, size_t>> full_columns;
    /// chte_write_in_memory (VC18): the parts written into memory, and each one's files
    bool in_memory = false;
    struct PartFiles
    {
        std::vector<std::pair<std::string, std::shared_ptr<const std::string>>> files;
        std::vector<chte_part_file> listed;
    };
    std::vector<PartFiles> part_files;
};

struct chte_reader
{
    chte_table * table = nullptr;
    /// the runs of granules to read (chte::markRuns), each by a source of its own, opened in turn
    std::function<DB::Pipe(std::optional<DB::MarkRanges>)> open_run;
    std::vector<std::optional<DB::MarkRanges>> runs;
    size_t next_run = 0;
    DB::QueryPipeline pipeline;
    std::unique_ptr<DB::PullingPipelineExecutor> executor;
    DB::Columns columns;
    std::vector<uint8_t> present;
    /// chte_read_rows: the offsets of the rows wanted, ascending; the blocks' last column is
    /// their _part_offset, by which the other rows are left out, and is not handed on
    std::vector<UInt64> wanted;
    size_t wanted_pos = 0;
    bool by_offset = false;

    /// chte_read_keep_dictionaries: the columns left LowCardinality, and per column the last
    /// dictionary handed and its id, held so that no other dictionary is made at its address
    /// while the reader compares with it
    std::vector<uint8_t> keep_dictionary;
    struct Dictionary
    {
        DB::ColumnPtr column;
        uint64_t id = 0;
    };
    std::vector<Dictionary> dictionaries;

    /// chte_read_begin_steps (VC2): the steps, the first read by chte_read_next; the granules to read,
    /// ranges of marks in order, and the place of the next; the granule read last, its rows, and
    /// whether each step of it is read
    std::vector<chte::ReadStep> steps;
    DB::MarkRanges step_ranges;
    size_t range_at = 0;
    size_t mark_at = 0;
    std::optional<size_t> granule;
    size_t granule_rows = 0;
    size_t granule_run_end = 0;
    std::vector<uint8_t> step_read;
    DB::MarkCachePtr mark_cache;
    DB::ReadStepPerformanceCountersPtr counters;

    /// The next block of the runs, the next run's source opened when one is done: false at the end
    bool pull(DB::Block & block);
};

struct chte_table
{
    std::shared_ptr<chte::PgMergeTree> storage;
    DB::StorageMetadataPtr metadata;
    DB::ContextMutablePtr context;
    /// what a call's result points into, until the next such call
    DB::Columns scratch;
    /// the last part an insert's writer made: per column, its bytes a row, a string's chars
    /// alone (chte_write_reserve, VC18)
    std::vector<double> last_bytes_per_row;
};
