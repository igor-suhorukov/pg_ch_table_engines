/*
 * Tables of libchte_engine.so (ch_table_engines.md §2.2): a CREATE TABLE statement in
 * ClickHouse's syntax, compiled by ClickHouse's parsers and description builders into the
 * metadata of a PgMergeTree, the MergeTreeData subclass whose parts PostgreSQL tracks.
 */
#include "internal.h"

#include <Common/Exception.h>
#include <Compression/CompressionFactory.h>
#include <DataTypes/DataTypeFactory.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/StoragePolicy.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/IndicesDescription.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/ProjectionsDescription.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <base/scope_guard.h>

#include <atomic>

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
    /// pg_accel's §3.32 F12 (VC38): MergeTreeData::initializeDirectoriesAndFormatVersion()'s work but its
    /// read of format_version.txt -- 0.17 of its 0.25 Mcycles, a process's first open of the table: the shim's
    /// tables are of the format of custom partitions, the least where there is no date column
    /// (MergeTreeData.cpp:528-530), and nothing writes another; the directories made as it makes them
    /// (:540-544), the file neither read nor written
    relative_data_path = relative_path;
    for (const auto & disk : getDisks())
        if (!disk->isBroken() && !disk->isReadOnly())
        {
            disk->createDirectories(relative_data_path);
            disk->createDirectories(std::filesystem::path(relative_data_path) / DETACHED_DIR_NAME);
        }
    format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
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

/// The parts loaded in the process (VC29's tests)
std::atomic<uint64_t> parts_loaded{0};

}

uint64_t partsLoaded()
{
    return parts_loaded.load(std::memory_order_relaxed);
}

namespace
{

/// VC21: a process's types read from their names, once each: a table's hundred columns name a few
std::unordered_map<String, DataTypePtr> known_types;

}

DataTypePtr typeNamed(const String & name)
{
    auto it = known_types.find(name);
    if (it == known_types.end())
        it = known_types.emplace(name, DataTypeFactory::instance().get(name)).first;
    return it->second;
}

bool specCheck()
{
    static const bool check = []
    {
        const char * value = getenv("CHTE_SPEC_CHECK");
        return value && *value && strcmp(value, "0") != 0;
    }();
    return check;
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
    parts_loaded.fetch_add(1, std::memory_order_relaxed);
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

void PgMergeTree::forgetLoadedProjections() const
{
    for (auto & [name, loaded] : parts)
        loaded.projections = false;
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

/// A definition's MergeTree settings, storage_def's, as the storage takes them
std::unique_ptr<MergeTreeSettings> loadSettings(ContextMutablePtr context, ASTStorage & storage_def)
{
    auto settings = std::make_unique<MergeTreeSettings>(context->getMergeTreeSettings());
    settings->loadFromQuery(storage_def, context, /* is_loading_from_existing_metadata= */ true);
    return settings;
}

/// The table of a definition's columns and keys, in either open: its settings -- storage_def's, loaded --
/// its columns' statistics, where the columns do not carry them yet, and the storage; its projections
/// described as first needed (describeProjections)
chte_table * makeTable(const chte_table_def * def, ContextMutablePtr context, StorageInMemoryMetadata & metadata,
                       ASTStorage & storage_def, const MergeTreeData::MergingParams & merging, ASTPtr projections,
                       std::unique_ptr<MergeTreeSettings> settings, bool with_statistics)
{
    auto & e = engine();
    if (storage_def.settings)
        metadata.settings_changes = storage_def.settings->ptr();

    /// Statistics of every column, as registerStorageMergeTree.cpp:1022-1027 adds them: the
    /// parts' least and greatest values and NULLs, built as a part is written or merged.
    String statistics_types = (*settings)[MergeTreeSetting::auto_statistics_types];
    if (!statistics_types.empty() && !with_statistics)
        addImplicitStatistics(metadata.columns, statistics_types);
    if (statisticsNeedFunctions(metadata.columns))
        registerAllFunctions();

    StorageID id("chte", def->name && *def->name ? def->name : "table");
    std::shared_ptr<PagesDisk> pages_disk;
    if (def->pages)
        pages_disk = std::make_shared<PagesDisk>("chte_pages", e.parts_disk->getPath(), *def->pages);
    startBackgroundExecutors();
    auto storage = std::make_shared<PgMergeTree>(id, def->relative_path, metadata, context, merging, std::move(settings),
                                                 std::move(pages_disk));
    StorageMetadataHandle handle = storage->getInMemoryMetadataPtr(context, /* bypass_metadata_cache= */ true);
    StorageMetadataPtr snapshot = handle;
    auto * table = new chte_table{storage, snapshot, context, {}, {}, {}, {}, {}, {}, {}, {}, {}};
    if (projections && !projections->children.empty())
        table->projections = projections;
    return table;
}

chte_table * openTable(const chte_table_def * def)
{
    if (!def || !def->create_query || !def->relative_path)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_table_open: create_query and relative_path are required");

    auto context = Context::createCopy(engine().global);
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

    /// VC21 (ch_table_engines.md §8 item 24): the projections -- _pg_rowid_index, a search by row id's
    /// -- described where a search, an insert, a merge or a rewrite first needs them, not at every open:
    /// ClickHouse analyzes each one's query to describe it, 0.6 to 0.9 ms of a fresh process's 1.6 for
    /// the table's open, which a scan never reads
    ASTPtr projections = create.columns_list->projections ? create.columns_list->projections->clone() : nullptr;
    auto settings = loadSettings(context, storage_def);
    return makeTable(def, context, metadata, storage_def, merging, projections, std::move(settings), /* with_statistics= */ false);
}

/// VC21: a process's codecs read from their definitions' text, once each -- a table's hundred
/// columns name a few -- as InterpreterCreateQuery::getColumnsDescription() makes them at ATTACH:
/// validated for the column's type as already accepted; a generic compression alone -- LZ4, LZ4HC,
/// ZSTD, NONE -- once for every type, which its validation neither changes nor refuses
std::map<std::pair<String, String>, ASTPtr> known_codecs;

/// The statistics addImplicitStatistics() gives every column, of auto_statistics_types' text, read once a
/// process as its parseColumnStatisticsFromString() reads them (Statistics.cpp at the tag)
std::map<String, ColumnStatisticsDescription::StatisticsTypeDescMap> known_statistics;

const ColumnStatisticsDescription::StatisticsTypeDescMap & implicitStatistics(const String & types)
{
    auto it = known_statistics.find(types);
    if (it != known_statistics.end())
        return it->second;
    ParserStatisticsType parser;
    auto ast = parseQuery(parser, "(" + types + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    ColumnStatisticsDescription::StatisticsTypeDescMap result;
    for (const auto & arg : ast->as<ASTFunction &>().arguments->children)
    {
        const auto * function = arg->as<ASTFunction>();
        if (!function)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected a function for statistic type, got: {}", arg->formatForLogging());
        auto type = stringToStatisticsType(function->name);
        result.emplace(type, SingleStatisticsDescription(type, arg, true));
    }
    return known_statistics.emplace(types, std::move(result)).first->second;
}

bool genericCodec(const String & arguments)
{
    if (arguments.find(',') != String::npos)
        return false;
    String family = arguments.substr(0, arguments.find('('));
    return family == "LZ4" || family == "LZ4HC" || family == "ZSTD" || family == "NONE";
}

ASTPtr codecFor(const String & arguments, const DataTypePtr & type)
{
    auto key = std::make_pair(arguments, genericCodec(arguments) ? String() : type->getName());
    auto it = known_codecs.find(key);
    if (it == known_codecs.end())
    {
        ParserCodec parser;
        String text = "(" + arguments + ")";
        ASTPtr ast = parseQuery(parser, text, "codec", DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH,
                                DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        it = known_codecs.emplace(key, CompressionCodecFactory::instance().validateCodecAndGetPreprocessedAST(
            ast, type, CodecValidationSettings::trusted())).first;
    }
    return it->second->clone();
}

/// A key's columns as ClickHouse's parser reads its definition's text: one as itself in parentheses, "(a)";
/// more as the tuple operator of parentheses, "(a, b)"; none as the function call "tuple()"
ASTPtr keyOf(const char * const * names, size_t n)
{
    if (n == 1)
    {
        auto column = make_intrusive<ASTIdentifier>(names[0]);
        column->setParenthesized(true);
        return column;
    }
    auto tuple = n > 1 ? makeASTOperator("tuple") : makeASTFunction("tuple");
    for (size_t i = 0; i < n; ++i)
        tuple->arguments->children.push_back(make_intrusive<ASTIdentifier>(names[i]));
    return tuple;
}

/// A key's description over its own columns, where ClickHouse's analysis of it reads every column of the
/// table: the key's -- columns, the additional ones among them, and the virtual ones -- alone resolve it
KeyDescription keyOver(const ASTPtr & ast, const ColumnsDescription & columns, const char * const * names, size_t n,
                       const VirtualColumnsDescription & virtuals, ContextPtr context,
                       const NamesAndTypesList & additional_columns = {})
{
    /// in the table's order, as the analysis of every column finds them: its actions' inputs the same
    std::set<String> key;
    for (size_t i = 0; i < n; ++i)
        key.insert(names[i]);
    for (const auto & column : additional_columns)
        key.insert(column.name);
    ColumnsDescription own;
    for (const auto & column : columns)
        if (key.contains(column.name))
            own.add(column);
    return KeyDescription::getKeyFromAST(ast, own, virtuals, context, additional_columns);
}

chte_table * openTableSpec(const chte_table_spec * spec)
{
    const chte_table_def * def = spec->def;
    if (!def || !def->relative_path || !spec->engine || spec->ncolumns == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_table_open_spec: columns, an engine and relative_path are required");

    auto context = Context::createCopy(engine().global);
    /// the storage's clauses as ClickHouse's parser makes them of a definition's text: what the settings'
    /// loading and the engine's parameters read
    auto storage_ast = make_intrusive<ASTStorage>();
    auto & storage_def = *storage_ast;
    auto engine_ast = make_intrusive<ASTFunction>();
    engine_ast->name = spec->engine;
    engine_ast->setNoEmptyArgs(true);
    if (spec->nengine_args > 0)
    {
        engine_ast->arguments = make_intrusive<ASTExpressionList>();
        engine_ast->children.push_back(engine_ast->arguments);
        if (spec->engine_tuple)
            engine_ast->arguments->children.push_back(keyOf(spec->engine_args, spec->nengine_args));
        else
            for (size_t i = 0; i < spec->nengine_args; ++i)
                engine_ast->arguments->children.push_back(make_intrusive<ASTIdentifier>(spec->engine_args[i]));
    }
    storage_def.set(storage_def.engine, engine_ast);
    storage_def.set(storage_def.order_by, keyOf(spec->order_by, spec->norder_by));
    if (spec->nprimary_key > 0)
        storage_def.set(storage_def.primary_key, keyOf(spec->primary_key, spec->nprimary_key));
    auto settings_ast = make_intrusive<ASTSetQuery>();
    settings_ast->is_standalone = false;
    for (size_t i = 0; i < spec->nsettings; ++i)
    {
        const auto & s = spec->settings[i];
        settings_ast->changes.emplace_back(s.name, s.string ? Field(String(s.string)) : Field(UInt64(s.number)));
    }
    if (spec->user_settings && *spec->user_settings)
    {
        ParserSetQuery parser(/* parse_only_internals_= */ true);
        ASTPtr user = parseQuery(parser, spec->user_settings, "table settings", DBMS_DEFAULT_MAX_QUERY_SIZE,
                                 DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        for (const auto & change : user->as<ASTSetQuery &>().changes)
            settings_ast->changes.push_back(change);
    }
    storage_def.set(storage_def.settings, settings_ast);

    std::optional<String> key_column;
    auto merging = mergingParams(*storage_def.engine, key_column);

    /// the settings first, so that each column is made with its statistics, as addImplicitStatistics() adds
    /// them to every column after -- whose ColumnsDescription::modify() takes each column's subcolumns out and
    /// puts them back
    auto settings = loadSettings(context, storage_def);
    String statistics_types = (*settings)[MergeTreeSetting::auto_statistics_types];
    const auto * statistics = statistics_types.empty() ? nullptr : &implicitStatistics(statistics_types);

    StorageInMemoryMetadata metadata;
    {
        ColumnsDescription columns;
        for (size_t i = 0; i < spec->ncolumns; ++i)
        {
            const auto & c = spec->columns[i];
            ColumnDescription column;
            column.name = c.name;
            column.type = typeNamed(c.type);
            if (c.codec && *c.codec)
                column.codec = codecFor(c.codec, column.type);
            if (statistics)
            {
                ColumnStatisticsDescription desc;
                desc.data_type = column.type;
                desc.types_to_desc = *statistics;
                desc = MergeTreeStatisticsFactory::instance().cloneWithSupportedStatistics(desc, column.type);
                if (!desc.empty())
                    column.statistics.merge(desc, column.name, column.type, /* if_not_exists= */ true);
            }
            columns.add(std::move(column));
        }
        metadata.setColumns(std::move(columns));
    }

    /// Keys as openTable() builds them, each over its own columns: no partition key; the primary key the
    /// sorting key's where it is the same columns and the engine adds none to the sorting key
    /// pg_accel's §3.32 F12 (VC38): the empty partition key as ClickHouse makes one without its analyzer
    /// (KeyDescription::buildEmptyKey()), where getKeyFromAST(nullptr) analyzed an empty list -- 0.39 Mcycles of a
    /// process's first open; CHTE_SPEC_CHECK compares it with the CREATE TABLE text's
    metadata.partition_key = KeyDescription::buildEmptyKey();
    metadata.virtuals = MergeTreeData::createVirtuals(&metadata.partition_key);
    NamesAndTypesList additional_columns;
    if (key_column)
        additional_columns.emplace_back(*key_column, metadata.columns.getPhysical(*key_column).type);
    metadata.sorting_key = keyOver(storage_def.order_by->ptr(), metadata.columns, spec->order_by, spec->norder_by,
                                   metadata.virtuals, context, additional_columns);
    if (spec->nprimary_key > 0)
    {
        auto primary = keyOver(storage_def.primary_key->ptr(), metadata.columns, spec->primary_key, spec->nprimary_key,
                               metadata.virtuals, context);
        /// as getPrimaryKeyFromAST() takes the sorting key's directions
        if (!metadata.sorting_key.reverse_flags.empty())
            primary.reverse_flags.assign(metadata.sorting_key.reverse_flags.begin(),
                                         metadata.sorting_key.reverse_flags.begin()
                                             + std::min(primary.column_names.size(), metadata.sorting_key.reverse_flags.size()));
        metadata.primary_key = std::move(primary);
    }
    else if (additional_columns.empty())
    {
        metadata.primary_key = metadata.sorting_key;
        metadata.primary_key.definition_ast = nullptr;
    }
    else
    {
        metadata.primary_key = keyOver(storage_def.order_by->ptr(), metadata.columns, spec->order_by, spec->norder_by,
                                       metadata.virtuals, context);
        metadata.primary_key.definition_ast = nullptr;
    }

    auto * table = makeTable(def, context, metadata, storage_def, merging, nullptr, std::move(settings),
                             /* with_statistics= */ statistics != nullptr);
    for (size_t i = 0; i < spec->nprojections; ++i)
        table->projection_texts.emplace_back(spec->projections[i]);
    return table;
}

}

void chte::describeProjections(chte_table & table)
{
    if (!table.projections && !table.projection_texts.empty())
    {
        /// VC21: a definition opened from its parts, its projections' declarations as their text
        auto list = make_intrusive<ASTExpressionList>();
        for (const auto & text : table.projection_texts)
        {
            ParserProjectionDeclaration parser;
            list->children.push_back(parseQuery(parser, text, "projection", DBMS_DEFAULT_MAX_QUERY_SIZE,
                                                DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS));
        }
        table.projections = list;
        table.projection_texts.clear();
    }
    if (!table.projections)
        return;
    Names names;
    collectFunctionNames(table.projections.get(), names);
    if (!functionsRegistered(names))
        registerAllFunctions();
    auto copy = std::make_shared<StorageInMemoryMetadata>(*table.metadata);
    for (const auto & projection : table.projections->children)
        copy->projections.add(ProjectionDescription::getProjectionFromAST(
            projection, copy->columns, &copy->partition_key, table.context, LoadingStrictnessLevel::ATTACH));
    table.storage->setInMemoryMetadata(*copy);
    StorageMetadataHandle handle = table.storage->getInMemoryMetadataPtr(table.context, /* bypass_metadata_cache= */ true);
    table.metadata = handle;
    /// a part loaded before has its projections to load, which its loads found none of
    table.storage->forgetLoadedProjections();
    table.projections = nullptr;
}

namespace
{

/// What a table is, for the tests' comparison of its two opens: its columns, their codecs and statistics,
/// its keys -- their definitions, columns, types, directions, sample blocks and actions -- its settings,
/// as its metadata and as its storage has them, its engine's parameters, and its projections described
String describeTable(chte_table & table)
{
    describeProjections(table);
    const auto & m = *table.metadata;
    WriteBufferFromOwnString out;
    auto ast = [](const ASTPtr & a) { return a ? a->formatWithSecretsOneLine() : String("-"); };
    for (const auto & c : m.getColumns())
    {
        out << c.name << ' ' << c.type->getName() << ' ' << ast(c.codec) << " stats "
            << (c.statistics.data_type ? c.statistics.data_type->getName() : "-");
        for (const auto & [type, desc] : c.statistics.types_to_desc)
            out << ' ' << static_cast<int>(type) << (desc.is_implicit ? " implicit " : " explicit ") << ast(desc.ast);
        out << '\n';
    }
    auto key = [&](const char * what, const KeyDescription & k)
    {
        out << what << ": " << ast(k.definition_ast) << " | " << ast(k.expression_list_ast) << " |";
        for (const auto & name : k.column_names)
            out << ' ' << name;
        for (const auto & type : k.data_types)
            out << ' ' << type->getName();
        for (bool reverse : k.reverse_flags)
            out << (reverse ? " desc" : " asc");
        out << " | " << k.sample_block.dumpStructure() << " | " << (k.expression ? k.expression->dumpActions() : "-") << '\n';
    };
    key("partition", m.partition_key);
    key("sorting", m.sorting_key);
    key("primary", m.primary_key);
    key("sampling", m.sampling_key);
    out << "virtuals:";
    for (const auto & v : m.virtuals)
        out << ' ' << v.name << ' ' << v.type->getName();
    out << "\nsettings: " << ast(m.settings_changes) << '\n';
    for (const auto & change : table.storage->getSettings()->changes())
        out << change.name << '=' << change.value.dump() << ' ';
    const auto & p = table.storage->merging_params;
    out << "\nmerging: " << p.getModeName() << ' ' << p.sign_column << ' ' << p.version_column << ' ' << p.is_deleted_column;
    for (const auto & name : p.columns_to_sum)
        out << ' ' << name;
    out << '\n';
    for (const auto & projection : m.getProjections())
        out << "projection " << projection.name << ' ' << ast(projection.definition_ast) << " | "
            << projection.sample_block.dumpStructure() << '\n';
    out.finalize();
    return out.str();
}

/// CHTE_SPEC_CHECK in the environment, for the tests: each open from a definition's parts compared with an
/// open of its CREATE TABLE, both described afresh, the first difference an error
void checkSpec(const chte_table_spec * spec)
{
    std::unique_ptr<chte_table> from_parts(openTableSpec(spec));
    std::unique_ptr<chte_table> from_query(openTable(spec->def));
    String a = describeTable(*from_parts);
    String b = describeTable(*from_query);
    if (a != b)
    {
        size_t at = 0;
        while (at < a.size() && at < b.size() && a[at] == b[at])
            ++at;
        size_t from = a.rfind('\n', at);
        from = from == String::npos ? 0 : from + 1;
        throw Exception(ErrorCodes::LOGICAL_ERROR, "chte_table_open_spec: {} opened from its parts differs from its CREATE TABLE "
                        "at\n  {}\n  where the query makes\n  {}", spec->def->name ? spec->def->name : "a table",
                        a.substr(from, a.find('\n', at) - from), b.substr(from, b.find('\n', at) - from));
    }
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

extern "C" int chte_table_open_spec(const chte_table_spec * spec, chte_table ** table, chte_error * error)
{
    return guarded(error, [&]
    {
        if (!spec || spec->size < sizeof(chte_table_spec))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_table_open_spec: a spec of {} bytes, {} expected",
                            spec ? spec->size : 0, sizeof(chte_table_spec));
        try
        {
            *table = openTableSpec(spec);
        }
        catch (const Exception & e)
        {
            /// as chte_table_open(): a function the analysis looks up that the first registrations lack
            if (allFunctionsRegistered()
                || (e.code() != ErrorCodes::UNKNOWN_FUNCTION && e.code() != ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION))
                throw;
            registerAllFunctions();
            *table = openTableSpec(spec);
        }
        if (specCheck())
        {
            try
            {
                checkSpec(spec);
            }
            catch (...)
            {
                delete *table;
                *table = nullptr;
                throw;
            }
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
