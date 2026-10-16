/*
 * Tables of libchte_engine.so (ch_table_engines.md §2.2): a CREATE TABLE statement in
 * ClickHouse's syntax, compiled by ClickHouse's parsers and description builders into the
 * metadata of a PgMergeTree, the MergeTreeData subclass whose parts PostgreSQL tracks.
 */
#include "internal.h"

#include <Common/Exception.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/StoragePolicy.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/InterpreterCreateQuery.h>
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
#include <Storages/StorageInMemoryMetadata.h>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
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

MergeTreeData::MutableDataPartPtr PgMergeTree::loadPart(const String & part_name) const
{
    auto info = MergeTreePartInfo::fromPartName(part_name, format_version);
    auto disk = getStoragePolicy()->getDisks().at(0);
    auto volume = std::make_shared<SingleDiskVolume>("volume_" + part_name, disk, 0);
    auto part = getDataPartBuilder(part_name, volume, part_name, getReadSettings(), PartDirIntent::OpenExisting)
        .withPartInfo(info)
        .withPartFormatFromDisk()
        .build();
    part->loadColumnsChecksumsIndexes(/* require_columns_checksums= */ true, /* check_consistency= */ true);
    part->setState(DataPartState::Active);
    return part;
}

}

using namespace chte;

extern "C" int chte_table_open(const chte_table_def * def, chte_table ** table, chte_error * error)
{
    return guarded(error, [&]
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

        auto minmax_columns = metadata.getColumnsRequiredForPartitionKey();
        auto partition_key = metadata.partition_key.expression_list_ast->clone();
        FunctionNameNormalizer::visit(partition_key.get());
        metadata.minmax_count_projection.emplace(ProjectionDescription::getMinMaxCountProjection(
            columns, partition_key, minmax_columns, metadata.primary_key, &metadata.partition_key, context));

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

        StorageID id("chte", def->name && *def->name ? def->name : "table");
        std::shared_ptr<PagesDisk> pages_disk;
        if (def->pages)
            pages_disk = std::make_shared<PagesDisk>("chte_pages", e.parts_disk->getPath(), *def->pages);
        auto storage = std::make_shared<PgMergeTree>(id, def->relative_path, metadata, context, merging, std::move(settings),
                                                     std::move(pages_disk));
        StorageMetadataHandle handle = storage->getInMemoryMetadataPtr(context, /* bypass_metadata_cache= */ true);
        StorageMetadataPtr snapshot = handle;
        *table = new chte_table{storage, snapshot, context};
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
        std::map<String, PagesDisk::File> map;
        for (size_t i = 0; i < nfiles; ++i)
            map[files[i].path] = PagesDisk::File{files[i].size, files[i].first_block, files[i].nblocks};
        storage.pages_disk->attachPart(fs::path(storage.getRelativeDataPath()) / part_name, std::move(map));
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
