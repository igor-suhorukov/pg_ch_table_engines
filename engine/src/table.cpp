/*
 * Tables of libchte_engine.so (ch_table_engines.md §2.2): a CREATE TABLE statement in
 * ClickHouse's syntax, compiled by ClickHouse's parsers and description builders into the
 * metadata of a PgMergeTree, the MergeTreeData subclass whose parts PostgreSQL tracks.
 */
#include "internal.h"

#include <Common/Exception.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/StoragePolicy.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/IndicesDescription.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/ProjectionsDescription.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <base/scope_guard.h>

namespace DB::MergeTreeSetting
{
    extern const MergeTreeSettingsString auto_statistics_types;
}

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
    extern const int UNKNOWN_FUNCTION;
    extern const int UNKNOWN_STORAGE;
}

namespace chte
{

namespace
{

/// The mutations snapshot of a table that has none: PostgreSQL applies its deletes itself.
struct NoMutations final : public MergeTreeData::MutationsSnapshotBase
{
    using MutationsSnapshotBase::MutationsSnapshotBase;

    MutationCommands getOnFlyMutationCommandsForPart(const MergeTreeData::DataPartPtr &) const override { return {}; }
    std::shared_ptr<MergeTreeData::IMutationsSnapshot> cloneEmpty() const override { return std::make_shared<NoMutations>(); }
    NameSet getAllUpdatedColumns() const override { return getColumnsUpdatedInPatches(); }
};

String identifierArgument(const ASTs & args, size_t i, const String & engine, const char * what)
{
    String name;
    if (i >= args.size() || !tryGetIdentifierNameInto(args[i], name))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}: argument {} must be the {} column", engine, i + 1, what);
    return name;
}

Names columnListArgument(const ASTPtr & arg)
{
    if (const auto * tuple = arg->as<ASTFunction>(); tuple && tuple->name == "tuple")
    {
        Names names;
        for (const auto & element : tuple->arguments->children)
            names.push_back(getIdentifierName(element));
        return names;
    }
    return {getIdentifierName(arg)};
}

/// The engine clause, as registerStorageMergeTree.cpp:440-672 at the tag reads it for the
/// extended syntax (no Replicated*, no Graphite).
MergeTreeData::MergingParams mergingParams(const ASTFunction & engine, std::optional<String> & key_column)
{
    using Mode = MergeTreeData::MergingParams::Mode;
    MergeTreeData::MergingParams params;
    const String & name = engine.name;
    ASTs args = engine.arguments ? engine.arguments->children : ASTs{};
    size_t max_args = 0;

    if (name == "MergeTree")
        params.mode = Mode::Ordinary;
    else if (name == "ReplacingMergeTree")
    {
        params.mode = Mode::Replacing;
        max_args = 2;
        if (!args.empty())
            params.version_column = identifierArgument(args, 0, name, "version");
        if (args.size() > 1)
            params.is_deleted_column = identifierArgument(args, 1, name, "is_deleted");
    }
    else if (name == "SummingMergeTree" || name == "CoalescingMergeTree")
    {
        params.mode = name == "SummingMergeTree" ? Mode::Summing : Mode::Coalescing;
        max_args = 1;
        if (!args.empty())
            params.columns_to_sum = columnListArgument(args[0]);
    }
    else if (name == "AggregatingMergeTree")
        params.mode = Mode::Aggregating;
    else if (name == "CollapsingMergeTree")
    {
        params.mode = Mode::Collapsing;
        max_args = 1;
        params.sign_column = identifierArgument(args, 0, name, "sign");
    }
    else if (name == "VersionedCollapsingMergeTree")
    {
        params.mode = Mode::VersionedCollapsing;
        max_args = 2;
        params.sign_column = identifierArgument(args, 0, name, "sign");
        params.version_column = identifierArgument(args, 1, name, "version");
        /// The only engine that adds a column to the sorting key.
        key_column = params.version_column;
    }
    else if (name.starts_with("Replicated") || name == "GraphiteMergeTree")
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "engine {} is not supported", name);
    else
        throw Exception(ErrorCodes::UNKNOWN_STORAGE, "unknown engine {}", name);

    if (args.size() > max_args)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} takes at most {} arguments", name, max_args);
    return params;
}

}

PgMergeTree::PgMergeTree(
    const StorageID & table_id,
    const String & relative_path,
    const StorageInMemoryMetadata & metadata,
    ContextMutablePtr context_,
    const MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> settings,
    std::shared_ptr<PagesDisk> pages_disk_)
    : MergeTreeData(table_id, metadata, context_, /* date_column_name= */ "", merging_params_, std::move(settings),
                    /* require_part_metadata= */ false, LoadingStrictnessLevel::ATTACH)
    , writer(*this)
    , merger(*this)
{
    pages_disk = std::move(pages_disk_);
    if (pages_disk)
        own_policy = std::make_shared<StoragePolicy>(
            "chte_pages", Volumes{std::make_shared<SingleDiskVolume>("chte_pages", pages_disk)}, 0.0);
    initializeDirectoriesAndFormatVersion(relative_path, /* attach= */ true, /* date_column_name= */ "");
}

MergeTreeData::MutationsSnapshotPtr PgMergeTree::getMutationsSnapshot(const IMutationsSnapshot::Params & params) const
{
    return std::make_shared<NoMutations>(params, MutationCounters{}, DataPartsVector{});
}

std::unique_ptr<MergeTreeSettings> PgMergeTree::getDefaultSettings() const
{
    return std::make_unique<MergeTreeSettings>(getContext()->getMergeTreeSettings());
}

void PgMergeTree::unsupported(const char * what)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is done by PostgreSQL, not by the engine", what);
}

namespace
{

/// The table whose part this thread is loading, for getInMemoryMetadataPtr()
thread_local const PgMergeTree * loading_part_of = nullptr;

}

StorageMetadataHandle PgMergeTree::getInMemoryMetadataPtr(ContextPtr query_context, bool bypass_metadata_cache) const
{
    StorageMetadataHandle handle = MergeTreeData::getInMemoryMetadataPtr(query_context, bypass_metadata_cache);
    if (loading_part_of != this)
        return handle;
    StorageMetadataPtr metadata = handle;
    if (metadata->projections.empty())
        return handle;
    if (without_projections_of != metadata)
    {
        auto copy = std::make_shared<StorageInMemoryMetadata>(*metadata);
        copy->projections = ProjectionsDescription{};
        without_projections = std::move(copy);
        without_projections_of = metadata;
    }
    return without_projections;
}

MergeTreeData::MutableDataPartPtr PgMergeTree::loadPart(const String & part_name) const
{
    auto info = MergeTreePartInfo::fromPartName(part_name, format_version);
    auto disk = getStoragePolicy()->getDisks().at(0);
    auto volume = std::make_shared<SingleDiskVolume>("volume_" + part_name, disk, 0);
    auto part = getDataPartBuilder(part_name, volume, part_name, getReadSettings(), PartDirIntent::OpenExisting)
        .withPartInfo(info)
        .withPartFormatFromDisk()
        .build();
    /// a read finds a missing or short file as it reads it, as a checksum's mismatch; the projections
    /// as they are first needed (partWithProjections), the row id's a part of its own to load
    loading_part_of = this;
    SCOPE_EXIT({ loading_part_of = nullptr; });
    part->loadColumnsChecksumsIndexes(/* require_columns_checksums= */ true, /* check_consistency= */ false);
    part->setState(DataPartState::Active);
    return part;
}

PgMergeTree::LoadedPart & PgMergeTree::loadedPart(const String & part_name) const
{
    auto it = parts.find(part_name);
    if (it == parts.end())
        it = parts.emplace(part_name, LoadedPart{loadPart(part_name), false, {}}).first;
    return it->second;
}

MergeTreeData::DataPartPtr PgMergeTree::getPart(const String & part_name) const
{
    return loadedPart(part_name).part;
}

MergeTreeData::DataPartPtr PgMergeTree::partWithProjections(const String & part_name) const
{
    auto & loaded = loadedPart(part_name);
    if (!loaded.projections)
    {
        /// as loadColumnsChecksumsIndexes() loads them: one that fails to load kept as broken
        bool broken = false;
        loaded.part->loadProjections(/* require_columns_checksums= */ true, /* check_consistency= */ false, broken,
                                     /* if_not_loaded= */ true);
        loaded.projections = true;
    }
    return loaded.part;
}

MergeTreeData::DataPartPtr PgMergeTree::checkedPart(const String & part_name) const
{
    DataPartPtr part = partWithProjections(part_name);
    part->checkConsistencyWithProjections(/* require_part_metadata= */ true);
    return part;
}

void PgMergeTree::forgetPart(const String & part_name) const
{
    parts.erase(part_name);
    estimates.erase(part_name);
    summaries.erase(part_name);
}

std::shared_ptr<const PartSummary> PgMergeTree::partSummary(const String & part_name, const void * data, size_t size) const
{
    auto it = summaries.find(part_name);
    if (it != summaries.end() && it->second.first == size)
        return it->second.second;
    auto summary = parseSummary(data, size);
    summaries[part_name] = {size, summary};
    return summary;
}

/// A column's bytes in a part: its streams' files in the part's checksums, as
/// MergeTreeDataPartWide::getColumnSizeImpl adds them up.  A compact part's, whose columns share one
/// file, and a column's of dynamic subcolumns, as ClickHouse has them.
ColumnSize columnSize(const IMergeTreeDataPart & part, const String & name)
{
    const auto & checksums = part.checksums;
    auto column = part.tryGetColumn(name);
    if (part.getType() != MergeTreeDataPartType::Wide || !column || column->type->hasDynamicSubcolumns() || checksums.empty())
        return part.getColumnSize(name);
    ColumnSize size;
    String marks = part.getMarksFileExtension();
    auto settings = part.storage.getSettings();
    part.getSerialization(name)->enumerateStreams([&](const ISerialization::SubstreamPath & path)
    {
        auto stream = IMergeTreeDataPart::getStreamNameForColumn(*column, path, IMergeTreeDataPart::DATA_FILE_EXTENSION,
                                                                 checksums, settings);
        if (!stream)
            return;
        if (auto it = checksums.files.find(*stream + IMergeTreeDataPart::DATA_FILE_EXTENSION); it != checksums.files.end())
        {
            size.data_compressed += it->second.file_size;
            size.data_uncompressed += it->second.uncompressed_size;
        }
        if (auto it = checksums.files.find(*stream + marks); it != checksums.files.end())
            size.marks += it->second.file_size;
    }, column->type, part.getColumnSample(*column));
    return size;
}

ColumnSize PgMergeTree::columnsSize(const String & part_name, const Names & columns) const
{
    auto & loaded = loadedPart(part_name);
    ColumnSize total;
    for (const auto & name : columns)
    {
        auto it = loaded.column_sizes.find(name);
        if (it == loaded.column_sizes.end())
            it = loaded.column_sizes.emplace(name, columnSize(*loaded.part, name)).first;
        total.add(it->second);
    }
    return total;
}

Estimates PgMergeTree::partEstimates(const DataPartPtr & part, const Names & columns) const
{
    auto & known = estimates[part->name];
    Names missing;
    for (const auto & name : columns)
        if (!known.contains(name))
            missing.push_back(name);
    if (!missing.empty())
    {
        auto statistics = part->loadStatistics(missing);
        for (const auto & name : missing)
        {
            auto it = statistics.find(name);
            known[name] = it == statistics.end() ? std::nullopt : std::optional<Estimate>(it->second->getEstimate());
        }
    }
    Estimates result;
    for (const auto & name : columns)
        if (const auto & estimate = known[name])
            result.emplace(name, *estimate);
    return result;
}

}

using namespace chte;

namespace
{

void collectFunctionNames(const IAST * ast, Names & names)
{
    if (!ast)
        return;
    if (const auto * function = ast->as<ASTFunction>())
        names.push_back(function->name);
    for (const auto & child : ast->children)
        collectFunctionNames(child.get(), names);
}

/// The functions a definition's expressions name: its columns' defaults, statistics and TTLs, its
/// indexes, projections and constraints, its keys and TTL -- not its types, codecs or engine.
Names definitionFunctions(const ASTCreateQuery & create)
{
    Names names;
    for (const auto & child : create.columns_list->columns->children)
        if (const auto * column = child->as<ASTColumnDeclaration>())
        {
            collectFunctionNames(column->getDefaultExpression().get(), names);
            collectFunctionNames(column->getStatisticsDesc().get(), names);
            collectFunctionNames(column->getTTL().get(), names);
        }
    collectFunctionNames(create.columns_list->indices, names);
    collectFunctionNames(create.columns_list->projections, names);
    collectFunctionNames(create.columns_list->constraints, names);
    const auto & storage = *create.storage;
    for (const IAST * clause : {storage.partition_by, storage.primary_key, storage.order_by, storage.sample_by, storage.ttl_table})
        collectFunctionNames(clause, names);
    return names;
}

/// Statistics other than the least and greatest values and NULLs: uniq's read and build through
/// aggregate functions (StatisticsUniq.cpp:18).
bool statisticsNeedFunctions(const ColumnsDescription & columns)
{
    for (const auto & column : columns)
        for (const auto & [type, desc] : column.statistics.types_to_desc)
            if (type != StatisticsType::Basic && type != StatisticsType::MinMax)
                return true;
    return false;
}

chte_table * openTable(const chte_table_def * def)
{
    auto & e = engine();
    if (!def || !def->create_query || !def->relative_path)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_table_open: create_query and relative_path are required");

    auto context = Context::createCopy(e.global);
    ParserCreateQuery parser;
    String query = def->create_query;
    ASTPtr ast = parseQuery(parser, query, "table definition", DBMS_DEFAULT_MAX_QUERY_SIZE,
                            DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    auto & create = ast->as<ASTCreateQuery &>();
    if (!create.columns_list || !create.columns_list->columns)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "table definition has no columns");
    if (!create.storage || !create.storage->engine)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "table definition has no ENGINE");
    auto & storage_def = *create.storage;
    if (!functionsRegistered(definitionFunctions(create)))
        registerAllFunctions();

    std::optional<String> key_column;
    auto merging = mergingParams(*storage_def.engine, key_column);

    StorageInMemoryMetadata metadata;
    ColumnsDescription columns = InterpreterCreateQuery::getColumnsDescription(
        *create.columns_list->columns, context, LoadingStrictnessLevel::ATTACH);
    metadata.setColumns(columns);

    /// Keys as registerStorageMergeTree.cpp:693-760 at the tag builds them.
    ASTPtr partition_by = storage_def.partition_by ? storage_def.partition_by->ptr() : nullptr;
    metadata.partition_key = KeyDescription::getKeyFromAST(partition_by, metadata.columns, MergeTreeData::createVirtuals(nullptr), context);
    metadata.virtuals = MergeTreeData::createVirtuals(&metadata.partition_key);

    if (!storage_def.order_by && storage_def.primary_key)
        storage_def.set(storage_def.order_by, storage_def.primary_key->clone());
    if (!storage_def.order_by)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "table definition has no ORDER BY");

    NamesAndTypesList additional_columns;
    if (key_column)
        additional_columns.emplace_back(*key_column, metadata.columns.getPhysical(*key_column).type);
    metadata.sorting_key = KeyDescription::getKeyFromAST(storage_def.order_by->ptr(), metadata.columns, metadata.virtuals,
                                                         context, additional_columns);
    if (storage_def.primary_key)
        metadata.primary_key = KeyDescription::getPrimaryKeyFromAST(storage_def.primary_key->ptr(), metadata.sorting_key,
                                                                    metadata.columns, metadata.virtuals, context);
    else
    {
        metadata.primary_key = KeyDescription::getKeyFromAST(storage_def.order_by->ptr(), metadata.columns, metadata.virtuals, context);
        metadata.primary_key.definition_ast = nullptr;
    }

    /// No minmax_count projection, which registerStorageMergeTree.cpp:755 adds: only ClickHouse's query
    /// plans read it (optimizeUseAggregateProjection.cpp:604), and making it analyzes a query of count(),
    /// min() and max() at every open, the aggregate functions registered for it (VC12).

    if (storage_def.sample_by)
        metadata.sampling_key = KeyDescription::getKeyFromAST(storage_def.sample_by->ptr(), metadata.columns, metadata.virtuals, context);

    if (create.columns_list->indices)
        for (const auto & index : create.columns_list->indices->children)
            metadata.secondary_indices.push_back(IndexDescription::getIndexFromAST(
                index, columns, /* is_implicitly_created= */ false, metadata.escape_index_filenames, context));

    if (create.columns_list->projections)
        for (const auto & projection : create.columns_list->projections->children)
            metadata.projections.add(ProjectionDescription::getProjectionFromAST(
                projection, columns, &metadata.partition_key, context, LoadingStrictnessLevel::ATTACH));

    auto settings = std::make_unique<MergeTreeSettings>(context->getMergeTreeSettings());
    settings->loadFromQuery(storage_def, context, /* is_loading_from_existing_metadata= */ true);
    if (storage_def.settings)
        metadata.settings_changes = storage_def.settings->ptr();

    /// Statistics of every column, as registerStorageMergeTree.cpp:1022-1027 adds them: the
    /// parts' least and greatest values and NULLs, built as a part is written or merged.
    String statistics_types = (*settings)[MergeTreeSetting::auto_statistics_types];
    if (!statistics_types.empty())
        addImplicitStatistics(metadata.columns, statistics_types);
    if (statisticsNeedFunctions(metadata.columns))
        registerAllFunctions();

    StorageID id("chte", def->name && *def->name ? def->name : "table");
    std::shared_ptr<PagesDisk> pages_disk;
    if (def->pages)
        pages_disk = std::make_shared<PagesDisk>("chte_pages", e.parts_disk->getPath(), *def->pages);
    auto storage = std::make_shared<PgMergeTree>(id, def->relative_path, metadata, context, merging, std::move(settings),
                                                 std::move(pages_disk));
    StorageMetadataHandle handle = storage->getInMemoryMetadataPtr(context, /* bypass_metadata_cache= */ true);
    StorageMetadataPtr snapshot = handle;
    return new chte_table{storage, snapshot, context, {}, {}, {}};
}

}

extern "C" int chte_table_open(const chte_table_def * def, chte_table ** table, chte_error * error)
{
    return guarded(error, [&]
    {
        try
        {
            *table = openTable(def);
        }
        catch (const Exception & e)
        {
            /// a function ClickHouse's analysis of the definition looks up that its names did not show: again, with all
            if (allFunctionsRegistered()
                || (e.code() != ErrorCodes::UNKNOWN_FUNCTION && e.code() != ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION))
                throw;
            registerAllFunctions();
            *table = openTable(def);
        }
    });
}

extern "C" void chte_table_close(chte_table * table)
{
    delete table;
}

extern "C" int chte_pages_attach(chte_table * table, const char * part_name, const chte_page_file * files, size_t nfiles,
                                 chte_error * error)
{
    return guarded(error, [&]
    {
        auto & storage = *table->storage;
        if (!storage.pages_disk)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} is not a table of the pages store", storage.getStorageID().getTableName());
        storage.pages_disk->attachPart(fs::path(storage.getRelativeDataPath()) / part_name, files, nfiles);
    });
}

extern "C" void chte_pages_detach(chte_table * table, const char * part_name)
{
    try
    {
        auto & storage = *table->storage;
        if (storage.pages_disk)
            storage.pages_disk->detachPart(fs::path(storage.getRelativeDataPath()) / part_name);
    }
    catch (...)
    {
    }
}
