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
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>

#include <cstring>
#include <map>
#include <set>
#include <string>

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

    PagesDisk(const String & name_, const String & path_, const chte_pages_ops & ops_);

    void attachPart(const String & part_dir, std::map<String, File> files);
    void detachPart(const String & part_dir);
    void readPage(UInt32 block, char * dst) const;
    UInt32 pagePayload() const { return ops.page_payload; }

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
    /// The entries of a directory of a part in pages: names, a trailing '/' for a subdirectory.
    std::vector<String> entries(const String & dir) const;

    chte_pages_ops ops;
    /// full relative path of each file of the parts in pages, and their directories
    std::map<String, File> files;
    std::set<String> dirs;
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

    /// Part object for an existing directory under the table's path.
    MutableDataPartPtr loadPart(const String & part_name) const;

    /// A committed part, loaded once: a part's directory never changes once committed, and
    /// its name is never reused in a table's storage.  The pages store's files are attached
    /// before a part is first loaded.
    DataPartPtr getPart(const String & part_name) const;
    void forgetPart(const String & part_name) const;

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

    mutable std::map<String, DataPartPtr> parts;
};

/// Appends value i of a vector of the caller's layout (chte_engine.h) to a ClickHouse column.
void appendValue(IColumn & column, const chte_vector & vector, size_t i);
/// Appends nrows values.
void appendVector(IColumn & column, uint32_t nrows, const chte_vector & vector);
/// A column's values in the caller's layout, pointing into the column.
chte_vector vectorOf(const IColumn & column);

}

struct chte_table
{
    std::shared_ptr<chte::PgMergeTree> storage;
    DB::StorageMetadataPtr metadata;
    DB::ContextMutablePtr context;
    /// what a call's result points into, until the next such call
    DB::Columns scratch;
};
