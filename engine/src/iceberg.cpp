/*
 * Tables of Iceberg tables on a local disk (ch_table_engines.md §2.17, CH7's Iceberg step), read by ClickHouse's
 * own Iceberg code: IcebergMetadata's snapshot and schemas, the manifests' entries as ManifestFileIterator hands
 * them, a data file's delete files matched as IcebergIterator matches them (IcebergIterator.cpp's
 * defineDeletesSpan() and its bounds of the files a position delete file names), its position and equality
 * deletes applied by IcebergMetadata's transforms, a file of an older schema brought to the snapshot's by its
 * schema transform.  They are driven here on the calling thread: IcebergMetadata::iterate() decodes manifests on
 * ClickHouse's IO pool and a producer thread of its own.
 *
 * A statement's listing is one snapshot -- the newest at its start, or the one the table's settings pin
 * (iceberg_snapshot_id, iceberg_timestamp_ms) -- its files the snapshot's data files, in the manifest list's
 * order, each one unit: a scan's filter keeps it or not by its manifest's statistics and partition, as
 * ClickHouse's ManifestFilesPruner does, and its reader, ParquetV3BlockInputFormat's as a Parquet table's,
 * prunes its row groups.  A row's id is its data file's first row's id and its place there, deleted rows
 * counted.  Every data and delete file is read under the table's location.
 *
 * A parallel scan's workers read the leader's snapshot: its listing's bytes carry each data file as ClickHouse
 * sends one to another node (IcebergObjectSerializableInfo), and the schemas its files were written under.
 *
 * And the tests' writer of Iceberg tables (chte_iceberg_write_begin): IcebergMetadata::createInitial(),
 * IcebergStorageSink for rows, and for deletes ClickHouse's Parquet writer and its generators of manifests,
 * manifest lists and metadata, as its mutations commit a position delete file (Mutations.cpp's
 * writeMetadataFiles()).  ClickHouse writes no equality delete: its manifest's entry is given the field ids of
 * its columns here.  A schema's change is IcebergMetadata::alter()'s (chte_iceberg_alter).
 */
#include "external.h"

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>
#include <Core/ProtocolDefines.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/sortBlock.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/parseQuery.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/Impl/AvroRowOutputFormat.h>
#include <Processors/Formats/Impl/ParquetV3BlockInputFormat.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/AlterCommands.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/DataFileStatistics.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFileIterator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/MetadataGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/StatelessMetadataFileGetter.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

#include <DataFile.hh>
#include <Generic.hh>
#include <Stream.hh>
#include <ValidSchema.hh>

#include <Poco/JSON/Parser.h>
#include <Poco/String.h>

#include <filesystem>
#include <sstream>
#include <sys/stat.h>

namespace DB
{
void registerInputFormatParquet(FormatFactory & factory);
void registerParquetSchemaReader(FormatFactory & factory);
}

namespace DB::DataLakeStorageSetting
{
    extern const DataLakeStorageSettingsBool iceberg_use_version_hint;
}

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_STAT;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int PATH_ACCESS_DENIED;
    extern const int THERE_IS_NO_COLUMN;
}

namespace fs = std::filesystem;

namespace chte
{

/// An Iceberg table's ClickHouse metadata, over its location: kept as long as the table's handle
struct IcebergTable
{
    std::shared_ptr<StorageLocalIcebergConfiguration> configuration;
    ObjectStoragePtr object_storage;
    std::shared_ptr<IDataLakeMetadata> holder;
    IcebergMetadata * metadata = nullptr;
    /// the table's columns resolved against a schema of the Iceberg table's, by the schema's id
    std::map<Int32, std::shared_ptr<const ResolvedFile>> resolved;

    const Iceberg::PersistentTableComponents & components() const { return metadata->getPersistentComponents(); }
    Iceberg::IcebergSchemaProcessor & schemas() const { return *components().schema_processor; }
};

/// A listing's snapshot
struct IcebergSnapshot
{
    Int32 schema_id = 0;
    std::optional<Int64> snapshot_id;
    String metadata_path;
    /// its manifest list's entries, which a scan's filter prunes the data files of
    ManifestFileCacheKeys manifests;
    /// the table's columns in its schema
    std::shared_ptr<const ResolvedFile> resolved;
};

namespace
{

LoggerPtr icebergLog()
{
    static LoggerPtr log = getLogger("chte.iceberg");
    return log;
}

/// What ClickHouse's Iceberg code calls by name: the Parquet input format and its schema reader -- the delete
/// transforms read delete files through the format factory -- and the functions of its filters and partitions
void registerIcebergReading()
{
    static bool done = false;
    if (done)
        return;
    registerAllFunctions();
    registerInputFormatParquet(FormatFactory::instance());
    registerParquetSchemaReader(FormatFactory::instance());
    done = true;
}

/// A local Iceberg table's configuration, as IcebergLocal's: its path the table's root
std::shared_ptr<StorageLocalIcebergConfiguration> localConfiguration(const String & location)
{
    auto configuration = std::make_shared<StorageLocalIcebergConfiguration>(std::make_shared<DataLakeStorageSettings>());
    configuration->setPaths({StorageObjectStorageConfiguration::Path(location)});
    configuration->setPathForRead(configuration->getRawPath());
    configuration->format = "Parquet";
    return configuration;
}

/// The local disk as an object storage, its keys absolute paths, as clickhouse-local has it
ObjectStoragePtr localStorage(bool read_only)
{
    return std::make_shared<LocalObjectStorage>(LocalObjectStorageSettings("", "/", read_only));
}

IcebergTable & icebergOf(ExternalTable & table)
{
    if (!table.iceberg)
    {
        registerIcebergReading();
        auto ice = std::make_shared<IcebergTable>();
        ice->configuration = localConfiguration(table.location);
        ice->object_storage = localStorage(/* read_only= */ true);
        ice->holder = IcebergMetadata::create(ice->object_storage, ice->configuration, table.context);
        ice->metadata = &dynamic_cast<IcebergMetadata &>(*ice->holder);
        table.iceberg = std::move(ice);
    }
    return *table.iceberg;
}

/// A path the table reads, refused where it is not under its location
void checkUnder(const ExternalTable & table, const String & path, const char * what)
{
    String location = fs::path(table.location).lexically_normal();
    while (location.size() > 1 && location.back() == '/')
        location.pop_back();
    String normal = fs::path(path).lexically_normal();
    if (!normal.starts_with(location + "/"))
        throw Exception(ErrorCodes::PATH_ACCESS_DENIED, "{} {} of Iceberg table {} is not under its location", what, path,
                        table.location);
}

/// The table's columns in a schema of the Iceberg table, checked once a schema
std::shared_ptr<const ResolvedFile> resolveSchema(ExternalTable & table, IcebergTable & ice, Int32 schema_id)
{
    auto it = ice.resolved.find(schema_id);
    if (it != ice.resolved.end())
        return it->second;
    auto file = std::make_shared<ResolvedFile>();
    file->schema = *ice.schemas().getClickHouseTableSchemaById(schema_id);
    resolveColumns(table, *file, fmt::format("Iceberg table {}'s schema {}", table.location, schema_id));
    ice.resolved.emplace(schema_id, file);
    return file;
}

bool entryLess(const Iceberg::ProcessedManifestFileEntryPtr & lhs, const Iceberg::ProcessedManifestFileEntryPtr & rhs)
{
    return (lhs <=> rhs) < 0;
}

/// The delete files that may apply to a data file, as IcebergIterator.cpp's defineDeletesSpan() finds them among
/// deletes sorted by their partition specification, partition and sequence number: those of the data file's
/// partition, of a sequence number not below its own (position deletes) or above it (equality deletes)
std::span<const Iceberg::ProcessedManifestFileEntryPtr> deletesSpan(const Iceberg::ProcessedManifestFileEntryPtr & data,
                                                                    const std::vector<Iceberg::ProcessedManifestFileEntryPtr> & deletes,
                                                                    bool equality)
{
    if (deletes.empty())
        return {};
    auto begin = equality ? std::upper_bound(deletes.begin(), deletes.end(), data, entryLess)
                          : std::lower_bound(deletes.begin(), deletes.end(), data, entryLess);
    auto end = std::upper_bound(
        deletes.begin(), deletes.end(), data,
        [](const Iceberg::ProcessedManifestFileEntryPtr & lhs, const Iceberg::ProcessedManifestFileEntryPtr & rhs)
        {
            return std::tie(*lhs->common_partition_specification, lhs->parsed_entry->partition_key_value)
                < std::tie(*rhs->common_partition_specification, rhs->parsed_entry->partition_key_value);
        });
    if (begin > end)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "the Iceberg delete files are not sorted by their partitions");
    return {begin, end};
}

/// A manifest's entries, pruned by a filter where given, as ClickHouse's iterator hands them
std::shared_ptr<Iceberg::ManifestFileIterator> manifestEntries(IcebergTable & ice, const ContextPtr & context,
                                                               const ManifestFileCacheKey & manifest, Int32 schema_id,
                                                               std::shared_ptr<const ActionsDAG> filter)
{
    const auto & components = ice.components();
    auto file = Iceberg::getManifestFile(ice.object_storage, components, context, icebergLog(), manifest.manifest_file_path,
                                         manifest.manifest_file_byte_size);
    return Iceberg::ManifestFileIterator::create(file.deserializer, manifest.manifest_file_path, components.path_resolver,
                                                 ice.schemas(), manifest.added_sequence_number, manifest.added_snapshot_id,
                                                 context, std::move(filter), schema_id);
}

bool isParquet(const String & format)
{
    return Poco::toLower(format) == "parquet";
}

/// The schemas of a metadata file, and its snapshots' schemas, made known to a table's metadata as
/// getRelevantState() makes them (IcebergMetadata.cpp, traverseMetadataAndFindNecessarySnapshotObject()): a
/// manifest's entry takes its schema from its snapshot's, else from the manifest's own
void registerMetadata(IcebergTable & ice, const ContextPtr & context, const String & metadata_path)
{
    const auto & components = ice.components();
    auto metadata = Iceberg::getMetadataJSONObject(metadata_path, ice.object_storage, nullptr, context, icebergLog(),
                                                   components.metadata_compression_method, components.table_uuid);
    if (metadata->has(Iceberg::f_schemas))
    {
        auto schemas = metadata->getArray(Iceberg::f_schemas);
        for (UInt32 i = 0; i < schemas->size(); ++i)
            if (!ice.schemas().hasClickHouseTableSchemaById(schemas->getObject(i)->getValue<Int32>(Iceberg::f_schema_id)))
                ice.schemas().addIcebergTableSchema(schemas->getObject(i));
    }
    if (metadata->has(Iceberg::f_snapshots))
    {
        auto snapshots = metadata->getArray(Iceberg::f_snapshots);
        for (UInt32 i = 0; i < snapshots->size(); ++i)
        {
            auto snapshot = snapshots->getObject(i);
            if (snapshot->has(Iceberg::f_schema_id))
                ice.schemas().registerSnapshotWithSchemaId(snapshot->getValue<Int64>(Iceberg::f_metadata_snapshot_id),
                                                           snapshot->getValue<Int32>(Iceberg::f_schema_id));
        }
    }
}

/// The schemas a listing's files need: its snapshot's, its data files' and their equality deletes'
std::set<Int32> schemasOf(const chte_external_listing & listing)
{
    std::set<Int32> ids{listing.iceberg->schema_id};
    for (const auto & file : listing.files)
    {
        ids.insert(file.iceberg->info.underlying_format_read_schema_id);
        for (const auto & deletes : file.iceberg->info.equality_deletes_objects)
            ids.insert(deletes.schema_id);
    }
    return ids;
}

}

void listIceberg(chte_external_listing & listing)
{
    auto & ext = *listing.table;
    auto & ice = icebergOf(ext);
    const auto & components = ice.components();
    auto [data_snapshot, state] = ice.metadata->getRelevantState(ext.context);

    auto snapshot = std::make_shared<IcebergSnapshot>();
    snapshot->schema_id = state.schema_id;
    snapshot->snapshot_id = state.snapshot_id;
    snapshot->metadata_path = state.metadata_file_path;
    snapshot->resolved = resolveSchema(ext, ice, state.schema_id);
    listing.iceberg = snapshot;
    if (!data_snapshot)
        return;
    snapshot->manifests = data_snapshot->manifest_list_entries;

    std::vector<Iceberg::ProcessedManifestFileEntryPtr> data;
    std::vector<Iceberg::ProcessedManifestFileEntryPtr> positions;
    std::vector<Iceberg::ProcessedManifestFileEntryPtr> equalities;
    for (const auto & manifest : snapshot->manifests)
    {
        auto entries = manifestEntries(ice, ext.context, manifest, state.schema_id, nullptr);
        while (auto entry = entries->next())
        {
            if (entry->parsed_entry->content_type == Iceberg::FileContentType::DATA)
                data.push_back(entry);
            else if (entry->parsed_entry->equality_ids.has_value())
                equalities.push_back(entry);
            else
                positions.push_back(entry);
        }
    }
    std::sort(positions.begin(), positions.end(), entryLess);
    std::sort(equalities.begin(), equalities.end(), entryLess);

    UInt64 first_row = 0;
    for (const auto & entry : data)
    {
        const auto & parsed = *entry->parsed_entry;
        auto object = std::make_shared<IcebergDataObjectInfo>(
            entry, components.path_resolver.resolve(parsed.file_path_key), state.schema_id,
            Iceberg::getIdentityPartitionColumnValues(*entry, ice.schemas()));
        const String path = object->getPath();
        checkUnder(ext, path, "data file");
        if (!isParquet(parsed.file_format))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "data file {} of Iceberg table {} is of format {}: only Parquet's are read",
                            path, ext.location, parsed.file_format);
        for (const auto & deletes : deletesSpan(entry, positions, /* equality= */ false))
        {
            /// a position delete file names the data files it deletes from between bounds, where it keeps them
            const auto & lower = deletes->parsed_entry->lower_reference_data_file_path;
            const auto & upper = deletes->parsed_entry->upper_reference_data_file_path;
            if ((lower && *lower > parsed.file_path_key) || (upper && *upper < parsed.file_path_key))
                continue;
            String deletes_path = components.path_resolver.resolve(deletes->parsed_entry->file_path_key);
            checkUnder(ext, deletes_path, "position delete file");
            object->addPositionDeleteObject(deletes, deletes_path);
        }
        for (const auto & deletes : deletesSpan(entry, equalities, /* equality= */ true))
        {
            String deletes_path = components.path_resolver.resolve(deletes->parsed_entry->file_path_key);
            checkUnder(ext, deletes_path, "equality delete file");
            object->addEqualityDeleteObject(deletes, deletes_path);
        }

        struct stat st;
        if (stat(path.c_str(), &st) != 0)
            throw ErrnoException(ErrorCodes::CANNOT_STAT, "Cannot stat {}", path);
        if (parsed.file_size_in_bytes > 0 && static_cast<UInt64>(st.st_size) != static_cast<UInt64>(parsed.file_size_in_bytes))
            throw Exception(ErrorCodes::INCORRECT_DATA, "data file {} of Iceberg table {} is {} bytes where its manifest has {}", path,
                            ext.location, st.st_size, parsed.file_size_in_bytes);
        ListedFile file;
        file.path = path;
        file.size = static_cast<UInt64>(st.st_size);
        file.mtime = Int64(st.st_mtim.tv_sec) * 1000000000 + st.st_mtim.tv_nsec;
        file.first_row = first_row;
        file.rows = static_cast<UInt64>(std::max<Int64>(parsed.record_count, 0));
        file.units = 1;
        file.resolved = snapshot->resolved;
        file.iceberg = std::move(object);
        first_row += file.rows;
        listing.files.push_back(std::move(file));
    }
}

void writeIcebergListing(const chte_external_listing & listing, WriteBuffer & out)
{
    const auto & snapshot = *listing.iceberg;
    auto & ice = icebergOf(*listing.table);
    writeVarInt(snapshot.schema_id, out);
    writeBinary(snapshot.snapshot_id.has_value(), out);
    writeVarInt(snapshot.snapshot_id.value_or(0), out);
    writeStringBinary(snapshot.metadata_path, out);
    writeVarUInt(snapshot.manifests.size(), out);
    for (const auto & manifest : snapshot.manifests)
    {
        writeStringBinary(manifest.manifest_file_path.serialize(), out);
        writeVarUInt(manifest.manifest_file_byte_size, out);
        writeVarInt(manifest.added_sequence_number, out);
        writeVarInt(manifest.added_snapshot_id, out);
        writeVarInt(static_cast<Int32>(manifest.content_type), out);
        writeVarInt(manifest.partition_spec_id, out);
    }
    /// the schemas, as the metadata has them, that a worker's metadata may not know yet
    auto ids = schemasOf(listing);
    writeVarUInt(ids.size(), out);
    for (Int32 id : ids)
    {
        std::ostringstream json; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        ice.schemas().getIcebergTableSchemaById(id)->stringify(json);
        writeStringBinary(json.str(), out);
    }
    for (const auto & file : listing.files)
    {
        writeVarUInt(file.first_row, out);
        writeVarUInt(file.rows, out);
        file.iceberg->info.serializeForClusterFunctionProtocol(out, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION);
    }
}

void readIcebergListing(chte_external_listing & listing, ReadBuffer & in)
{
    auto & ext = *listing.table;
    auto & ice = icebergOf(ext);
    auto snapshot = std::make_shared<IcebergSnapshot>();
    bool has_snapshot_id = false;
    Int64 snapshot_id = 0;
    readVarInt(snapshot->schema_id, in);
    readBinary(has_snapshot_id, in);
    readVarInt(snapshot_id, in);
    if (has_snapshot_id)
        snapshot->snapshot_id = snapshot_id;
    readStringBinary(snapshot->metadata_path, in);
    UInt64 nmanifests;
    readVarUInt(nmanifests, in);
    for (UInt64 i = 0; i < nmanifests; ++i)
    {
        String path;
        ManifestFileCacheKey manifest;
        Int32 content = 0;
        readStringBinary(path, in);
        manifest.manifest_file_path = Iceberg::IcebergPathFromMetadata::deserialize(std::move(path));
        readVarUInt(manifest.manifest_file_byte_size, in);
        readVarInt(manifest.added_sequence_number, in);
        readVarInt(manifest.added_snapshot_id, in);
        readVarInt(content, in);
        manifest.content_type = static_cast<Iceberg::ManifestFileContentType>(content);
        readVarInt(manifest.partition_spec_id, in);
        snapshot->manifests.push_back(std::move(manifest));
    }
    UInt64 nschemas;
    readVarUInt(nschemas, in);
    for (UInt64 i = 0; i < nschemas; ++i)
    {
        String json;
        readStringBinary(json, in);
        Poco::JSON::Parser parser;
        auto schema = parser.parse(json).extract<Poco::JSON::Object::Ptr>();
        if (!ice.schemas().hasClickHouseTableSchemaById(schema->getValue<Int32>(Iceberg::f_schema_id)))
            ice.schemas().addIcebergTableSchema(schema);
    }
    /// the leader's metadata's snapshots, so that the manifests' entries a filter prunes are read under their own
    /// schemas here too
    if (snapshot->snapshot_id)
        registerMetadata(ice, ext.context, snapshot->metadata_path);
    snapshot->resolved = resolveSchema(ext, ice, snapshot->schema_id);
    listing.iceberg = snapshot;
    for (auto & file : listing.files)
    {
        readVarUInt(file.first_row, in);
        readVarUInt(file.rows, in);
        Iceberg::IcebergObjectSerializableInfo info;
        info.deserializeForClusterFunctionProtocol(in, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION);
        file.units = 1;
        file.resolved = snapshot->resolved;
        file.iceberg = std::make_shared<IcebergDataObjectInfo>(RelativePathWithMetadata(file.path), info);
    }
}

NamesAndTypesList icebergSchema(ExternalTable & table)
{
    auto & ice = icebergOf(table);
    auto [data_snapshot, state] = ice.metadata->getRelevantState(table.context);
    return *ice.schemas().getClickHouseTableSchemaById(state.schema_id);
}

std::shared_ptr<const ResolvedFile> icebergResolvedFor(ExternalTable & table, const chte_external_listing & listing)
{
    auto & ice = icebergOf(table);
    /// another handle's metadata may not know the listing's schema yet
    if (!ice.schemas().hasClickHouseTableSchemaById(listing.iceberg->schema_id))
        ice.schemas().addIcebergTableSchema(icebergOf(*listing.table).schemas().getIcebergTableSchemaById(listing.iceberg->schema_id));
    return resolveSchema(table, ice, listing.iceberg->schema_id);
}

bool icebergDeletes(const ListedFile & file)
{
    return file.iceberg
        && (!file.iceberg->info.position_deletes_objects.empty() || !file.iceberg->info.equality_deletes_objects.empty());
}

namespace
{

/// A filter's columns as the Iceberg table's manifests name them: its snapshot schema's names and types
class SchemaColumns final : public FilterColumns
{
public:
    SchemaColumns(const ExternalTable & table_, const ResolvedFile & schema_) : table(table_), schema(schema_) { }

    NameAndTypePair input(const String & name) const override
    {
        size_t i = table.columnIndex(name);
        return {schema.names[i], schema.file_types[i]};
    }

    ColumnWithTypeAndName constant(const String & name, const chte_vector & values, uint32_t i) const override
    {
        const auto & column = table.column(name);
        auto value = column.type->createColumn();
        appendValue(*value, values, i);
        ColumnWithTypeAndName constant{std::move(value), column.type, ""};
        switch (column.convert)
        {
            case CHTE_CONVERT_DATE:
            {
                /// days from 2000-01-01 as Date32's from 1970-01-01
                auto date = ColumnInt32::create();
                date->insertValue(assert_cast<const ColumnInt32 &>(*constant.column).getData()[0] + PG_EPOCH_DAYS);
                return {std::move(date), column.read_type, ""};
            }
            case CHTE_CONVERT_TIMESTAMP:
            {
                /// microseconds from 2000-01-01 as DateTime64(6)'s from 1970-01-01
                auto timestamp = ColumnDecimal<DateTime64>::create(0, 6);
                timestamp->insertValue(DateTime64(assert_cast<const ColumnInt64 &>(*constant.column).getData()[0] + PG_EPOCH_MICROSECONDS));
                return {std::move(timestamp), column.read_type, ""};
            }
            default:
                return constant;
        }
    }

private:
    const ExternalTable & table;
    const ResolvedFile & schema;
};

}

bool icebergKeeps(chte_filter & filter, ExternalTable & table, const chte_external_listing & listing, const ListedFile & file)
{
    /// the data files a filter keeps of a snapshot, worked out once a filter and a snapshot: its manifests'
    /// entries again, ClickHouse's iterator given the filter
    const auto & snapshot = *listing.iceberg;
    String key = fmt::format("{}\n{}", snapshot.metadata_path, snapshot.snapshot_id.value_or(-1));
    auto it = filter.kept_files.find(key);
    if (it == filter.kept_files.end())
    {
        /// the listing's metadata, which knows its snapshot's schemas; the filter's columns the scan's table's
        auto & ice = icebergOf(*listing.table);
        auto resolved = icebergResolvedFor(table, listing);
        auto dag = std::make_shared<ActionsDAG>();
        const auto & predicate = buildFilter(*dag, filter.nodes.data(), filter.nodes.size(), table.context, SchemaColumns(table, *resolved));
        dag->getOutputs() = {&predicate};
        std::shared_ptr<const ActionsDAG> pruning = std::move(dag);

        /// the kept files' paths, as their manifests name them
        std::set<String> kept;
        for (const auto & manifest : snapshot.manifests)
        {
            if (manifest.content_type != Iceberg::ManifestFileContentType::DATA)
                continue;
            auto entries = manifestEntries(ice, table.context, manifest, snapshot.schema_id, pruning);
            while (auto entry = entries->next())
                if (entry->parsed_entry->content_type == Iceberg::FileContentType::DATA)
                    kept.insert(entry->parsed_entry->file_path_key.serialize());
        }
        it = filter.kept_files.emplace(std::move(key), std::move(kept)).first;
    }
    return it->second.contains(file.iceberg->info.data_object_file_path_key.serialize());
}

namespace
{

/// A data file's columns of its identity partition replaced by the partition's values, which the file need not
/// hold, as ClickHouse's object storage source replaces them (StorageObjectStorageSource.cpp): none where the
/// header has none of them
std::optional<ActionsDAG> identityPartitionColumns(const Block & header, const std::vector<std::pair<String, Field>> & values)
{
    ActionsDAG dag;
    bool replaced = false;
    for (const auto & column : header)
    {
        const auto & input = dag.addInput(column.name, column.type);
        auto it = std::find_if(values.begin(), values.end(), [&](const auto & value) { return value.first == column.name; });
        if (it == values.end())
        {
            dag.getOutputs().push_back(&input);
            continue;
        }
        const auto & constant = dag.addColumn(column.type->createColumnConst(1, it->second), column.type, column.name);
        dag.getOutputs().push_back(&dag.materializeNode(constant));
        replaced = true;
    }
    if (!replaced)
        return {};
    return dag;
}

}

std::unique_ptr<chte_reader> openIcebergReader(chte_table & table, const chte_external_listing & listing, const ListedFile & listed,
                                               const chte_filter * filter, const Names & names,
                                               std::shared_ptr<const PaddedPODArray<UInt64>> rows_to_read)
{
    auto & ext = *table.external;
    auto & ice = icebergOf(ext);
    auto & metadata = *ice.metadata;
    auto resolved = icebergResolvedFor(ext, listing);
    auto object = listed.iceberg;
    Int32 file_schema = object->info.underlying_format_read_schema_id;
    const auto & identity = object->info.identity_partition_columns;
    /// a file of the snapshot's schema, nothing deleting by its values, is read in the columns asked as the table
    /// reads them, pruned by the filter; one of an identity partition in the columns asked in their Iceberg types,
    /// which the partition's values are of; one of another schema or with equality deletes in all its columns, as
    /// ClickHouse reads it, the deletes and the schema transform taking them by their names and types there
    bool evolved = file_schema != object->info.schema_id_relevant_to_iterator;
    bool whole = evolved || !object->info.equality_deletes_objects.empty();
    bool own_types = whole || !identity.empty();

    Names read;
    for (const auto & name : names)
        if (name != ROWID && std::find(read.begin(), read.end(), name) == read.end())
            read.push_back(name);
    SharedHeader header;
    if (whole)
    {
        Block block;
        for (const auto & [name, type] : *ice.schemas().getClickHouseTableSchemaById(file_schema))
            block.insert({type->createColumn(), type, name});
        header = std::make_shared<const Block>(std::move(block));
    }
    else if (own_types)
    {
        Block block;
        for (const auto & name : read)
        {
            size_t i = ext.columnIndex(name);
            if (!block.has(resolved->names[i]))
                block.insert({resolved->file_types[i]->createColumn(), resolved->file_types[i], resolved->names[i]});
        }
        header = std::make_shared<const Block>(std::move(block));
    }
    else
        header = std::make_shared<const Block>(fileHeader(ext, *resolved, read));

    FormatSettings settings = ext.format_settings;
    metadata.modifyFormatSettings(settings, *ext.context);
    /// the file's columns found by their field ids, under its schema's names
    auto mapper = metadata.getColumnMapperForObject(object);
    FormatFilterInfoPtr info;
    if (!own_types && filter && !filter->nodes.empty())
        info = formatFilter(*filter, ext, *resolved, *header, mapper);
    if (!info)
        info = std::make_shared<FormatFilterInfo>(nullptr, ext.context, mapper, nullptr, nullptr);
    if (rows_to_read)
        info->rows_to_read = std::move(rows_to_read);

    std::shared_ptr<ReadBufferFromFilePRead> buf = openListed(listed.path, listed.size, listed.mtime);
    auto resources = singleThreadedResources(ext.context);
    auto format = std::make_shared<ParquetV3BlockInputFormat>(*buf, header, settings, resources, info,
                                                              settings.parquet.local_read_min_bytes_for_seek);
    QueryPipelineBuilder builder;
    builder.init(Pipe(format));
    if (!identity.empty())
        if (auto dag = identityPartitionColumns(builder.getHeader(), identity))
        {
            auto actions = std::make_shared<ExpressionActions>(std::move(*dag));
            builder.addSimpleTransform([&](const SharedHeader & in) { return std::make_shared<ExpressionTransform>(in, actions); });
        }
    metadata.addDeleteTransformers(object, builder, settings, resources, ext.context);
    if (evolved)
        if (auto transform = metadata.getSchemaTransformer(ext.context, object))
        {
            auto actions = std::make_shared<ExpressionActions>(transform->clone());
            builder.addSimpleTransform([&](const SharedHeader & in) { return std::make_shared<ExpressionTransform>(in, actions); });
        }
    return pipelineReader(table, std::move(resolved), listed, names, QueryPipelineBuilder::getPipeline(std::move(builder)),
                          /* cast= */ own_types, std::move(buf));
}

void icebergStats(chte_table & table, ExternalTable & ext, const chte_external_listing & listing, const ListedFile & file,
                  const String & name, chte_column_stats * stats)
{
    *stats = {};
    /// the footer's statistics are the rows' only where none of them is deleted, and name the columns as the
    /// snapshot's schema does only where the file was written under it
    if (icebergDeletes(file) || file.iceberg->info.underlying_format_read_schema_id != file.iceberg->info.schema_id_relevant_to_iterator)
        return;
    auto resolved = std::make_shared<ResolvedFile>(*icebergResolvedFor(ext, listing));
    resolved->footer = footerOf(ext, file.path, file.size, file.mtime);
    if (resolved->footer->rows != file.rows)
        return;
    parquetStats(table, ext, file, *resolved, name, stats);
}

/// The tests' writer of an Iceberg table
struct IcebergWriter
{
    int action = CHTE_ICEBERG_CREATE;
    String location;
    ContextMutablePtr context;
    FormatSettings format_settings;
    std::shared_ptr<StorageLocalIcebergConfiguration> configuration;
    ObjectStoragePtr object_storage;
    std::shared_ptr<IDataLakeMetadata> holder;
    IcebergMetadata * metadata = nullptr;
    /// CREATE's columns, and its partition spec as ClickHouse's PARTITION BY names one
    ColumnsDescription columns;
    ASTPtr partition_by;
    /// APPEND's sink, IcebergStorageSink, pushed to
    QueryPipeline pipeline;
    std::unique_ptr<PushingPipelineExecutor> executor;
    /// a delete file's rows, kept until finish sorts and writes them; its columns' field ids
    Block header;
    MutableColumns rows;
    std::vector<Int32> field_ids;
    /// the bytes of the table's files as it began, which finish's are compared with
    UInt64 bytes = 0;
};

namespace
{

/// The bytes of the files under a directory
UInt64 directoryBytes(const String & dir)
{
    UInt64 bytes = 0;
    std::error_code ec;
    for (fs::recursive_directory_iterator it(dir, ec), end; !ec && it != end; it.increment(ec))
        if (it->is_regular_file(ec))
            bytes += it->file_size(ec);
    return bytes;
}

/// The latest metadata of a table, and a generator of its next files' names
struct LatestMetadata
{
    Poco::JSON::Object::Ptr object;
    Int32 version = 0;
    CompressionMethod compression = CompressionMethod::None;
};

LatestMetadata latestMetadata(const IcebergWriter & writer)
{
    const auto & components = writer.metadata->getPersistentComponents();
    auto [version, path, compression] = Iceberg::getLatestOrExplicitMetadataFileAndVersion(
        writer.object_storage, components.table_path, writer.configuration->getDataLakeSettings(), nullptr, writer.context,
        icebergLog().get(), components.table_uuid, components.metadata_compression_method);
    LatestMetadata latest;
    latest.object = Iceberg::getMetadataJSONObject(path, writer.object_storage, nullptr, writer.context, icebergLog(), compression,
                                                   components.table_uuid);
    latest.version = version;
    latest.compression = compression;
    return latest;
}

Poco::JSON::Object::Ptr partitionSpec(const Poco::JSON::Object::Ptr & metadata, Int64 & spec_id)
{
    spec_id = metadata->getValue<Int64>(Iceberg::f_default_spec_id);
    auto specs = metadata->getArray(Iceberg::f_partition_specs);
    for (size_t i = 0; i < specs->size(); ++i)
    {
        auto spec = specs->getObject(static_cast<UInt32>(i));
        if (spec->getValue<Int64>(Iceberg::f_spec_id) == spec_id)
        {
            if (spec->getArray(Iceberg::f_fields)->size() > 0)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "the tests' writer writes deletes of unpartitioned Iceberg tables only");
            return spec;
        }
    }
    throw Exception(ErrorCodes::INCORRECT_DATA, "no partition spec {} in the Iceberg table's metadata", spec_id);
}

/// An equality delete file's manifest as generateManifestFile() wrote it, its entries given the field ids of the
/// delete file's columns, which ClickHouse leaves NULL
String withEqualityIds(const String & manifest, const std::vector<Int32> & field_ids)
{
    auto base = std::make_unique<avro::DataFileReaderBase>(
        avro::memoryInputStream(reinterpret_cast<const uint8_t *>(manifest.data()), manifest.size()));
    /// the header's metadata -- the Iceberg schema, the partition spec, the id-carrying Avro schema -- as written
    auto header = base->metadata();
    avro::DataFileReader<avro::GenericDatum> reader(std::move(base));
    avro::ValidSchema schema = reader.dataSchema();
    WriteBufferFromOwnString out;
    {
        auto adapter = std::make_unique<OutputStreamWriteBufferAdapter>(out);
        avro::DataFileWriter<avro::GenericDatum> writer(std::move(adapter), schema);
        for (const auto & [key, value] : header)
            if (key != "avro.codec")
                writer.setMetadata(key, String(value.begin(), value.end()));
        avro::GenericDatum datum(schema);
        while (reader.read(datum))
        {
            auto & data_file = datum.value<avro::GenericRecord>().field(Iceberg::f_data_file).value<avro::GenericRecord>();
            auto & ids = data_file.field("equality_ids");
            ids.selectBranch(1);
            auto & array = ids.value<avro::GenericArray>().value();
            array.clear();
            for (Int32 id : field_ids)
                array.emplace_back(id);
            writer.write(datum);
        }
        writer.close();
    }
    out.finalize();
    return std::move(out.str());
}

/// A snapshot of one delete file, committed as ClickHouse's mutations commit theirs (Mutations.cpp's
/// writeMetadataFiles()), an equality delete's manifest given its field ids
void commitDeletes(IcebergWriter & writer, const Iceberg::IcebergPathFromMetadata & delete_path, Int64 rows, Int64 bytes,
                   const DataFileStatistics & statistics, SharedHeader sample_block)
{
    const auto & components = writer.metadata->getPersistentComponents();
    const auto & resolver = components.path_resolver;
    auto latest = latestMetadata(writer);
    auto metadata = latest.object;
    FileNamesGenerator generator(resolver.getTableLocation(), false, latest.compression, "Parquet");
    generator.setVersion(latest.version + 1);
    Int64 spec_id = 0;
    auto spec = partitionSpec(metadata, spec_id);
    bool equality = writer.action == CHTE_ICEBERG_DELETE_EQUAL;

    auto metadata_info = generator.generateMetadataPathWithInfo();
    Int64 parent = metadata->has(Iceberg::f_current_snapshot_id) && !metadata->isNull(Iceberg::f_current_snapshot_id)
        ? metadata->getValue<Int64>(Iceberg::f_current_snapshot_id) : -1;
    auto [snapshot, manifest_list_path] = MetadataGenerator(metadata).generateNextMetadata(
        generator, metadata_info.path, parent, /* added_files= */ 0, /* added_records= */ 0, /* added_files_size= */ bytes,
        /* num_partitions= */ 1, /* added_delete_files= */ 1, /* num_deleted_rows= */ rows);
    if (equality)
    {
        /// the summary counts the file's rows as position deletes: they are equality deletes
        auto summary = snapshot->getObject(Iceberg::f_summary);
        summary->remove(Iceberg::f_added_position_delete_files);
        summary->remove(Iceberg::f_added_position_deletes);
        summary->set(Iceberg::f_added_equality_delete_files, String("1"));
        summary->set(Iceberg::f_added_equality_deletes, std::to_string(rows));
        auto shift = [&](const char * field, Int64 by)
        { summary->set(field, std::to_string(parse<Int64>(summary->getValue<String>(field)) + by)); };
        shift(Iceberg::f_total_position_deletes, -rows);
        shift(Iceberg::f_total_equality_deletes, rows);
    }

    auto manifest_path = generator.generateManifestEntryName();
    WriteBufferFromOwnString manifest;
    generateManifestFile(metadata, {}, {}, {}, {delete_path}, {static_cast<UInt64>(rows)}, {static_cast<UInt64>(bytes)}, statistics,
                         sample_block, snapshot, "Parquet", spec, spec_id, manifest,
                         equality ? Iceberg::FileContentType::EQUALITY_DELETE : Iceberg::FileContentType::POSITION_DELETE);
    manifest.finalize();
    String manifest_bytes = equality ? withEqualityIds(manifest.str(), writer.field_ids) : manifest.str();
    {
        auto out = writer.object_storage->writeObject(StoredObject(resolver.resolve(manifest_path)), WriteMode::Rewrite, std::nullopt,
                                                      DBMS_DEFAULT_BUFFER_SIZE, writer.context->getWriteSettings());
        out->write(manifest_bytes.data(), manifest_bytes.size());
        out->finalize();
    }
    {
        /// a manifest list's content is a manifest's, data or deletes: an equality delete's manifest is of deletes
        auto out = writer.object_storage->writeObject(StoredObject(resolver.resolve(manifest_list_path)), WriteMode::Rewrite,
                                                      std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, writer.context->getWriteSettings());
        generateManifestList(resolver, metadata, writer.object_storage, writer.context, {manifest_path}, snapshot,
                             {static_cast<Int64>(manifest_bytes.size())}, *out, Iceberg::FileContentType::POSITION_DELETE,
                             /* use_previous_snapshots= */ true);
        out->finalize();
    }
    if (!Iceberg::writeMetadataFileAndVersionHint(resolver, metadata_info, stringifyJSON(metadata, 4), generator.generateVersionHint(),
                                                  writer.object_storage, writer.context,
                                                  writer.configuration->getDataLakeSettings()[DataLakeStorageSetting::iceberg_use_version_hint]))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "the Iceberg table's metadata version {} was written meanwhile", metadata_info.version);
}

/// The delete file's rows, sorted -- a position delete file's by its data files' paths and positions, as Iceberg
/// keeps them and ClickHouse's streaming transform reads them -- written by ClickHouse's Parquet writer, its
/// columns given their field ids; and committed
void finishDeletes(IcebergWriter & writer)
{
    size_t nrows = writer.rows.empty() ? 0 : writer.rows.front()->size();
    Block block = writer.header.cloneWithColumns(std::move(writer.rows));
    if (writer.action == CHTE_ICEBERG_DELETE_POSITIONS)
    {
        SortDescription description;
        description.emplace_back(Iceberg::IcebergPositionDeleteTransform::data_file_path_column_name);
        description.emplace_back(Iceberg::IcebergPositionDeleteTransform::positions_column_name);
        sortBlock(block, description);
    }

    const auto & components = writer.metadata->getPersistentComponents();
    auto latest = latestMetadata(writer);
    FileNamesGenerator generator(components.path_resolver.getTableLocation(), false, latest.compression, "Parquet");
    auto delete_path = generator.generatePositionDeleteFile();

    auto mapper = std::make_shared<ColumnMapper>();
    std::unordered_map<String, Int64> encoding;
    std::unordered_set<String> strings;
    for (size_t i = 0; i < block.columns(); ++i)
    {
        encoding[block.getByPosition(i).name] = writer.field_ids[i];
        if (writer.action == CHTE_ICEBERG_DELETE_POSITIONS && i == 0)
            strings.insert(block.getByPosition(i).name);
    }
    mapper->setStorageColumnEncoding(std::move(encoding));
    if (!strings.empty())
        mapper->setIcebergStringPaths(std::move(strings));

    Poco::JSON::Array::Ptr fields = new Poco::JSON::Array;
    for (Int32 id : writer.field_ids)
    {
        Poco::JSON::Object::Ptr field = new Poco::JSON::Object;
        field->set(Iceberg::f_id, id);
        fields->add(field);
    }
    DataFileStatistics statistics(fields);
    Chunk chunk(block.getColumns(), nrows);
    statistics.update(chunk);

    Int64 bytes = 0;
    {
        auto out = writer.object_storage->writeObject(StoredObject(components.path_resolver.resolve(delete_path)), WriteMode::Rewrite,
                                                      std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, writer.context->getWriteSettings());
        auto info = std::make_shared<FormatFilterInfo>(nullptr, writer.context, mapper, nullptr, nullptr);
        auto format = FormatFactory::instance().getOutputFormat("Parquet", *out, writer.header, writer.context, writer.format_settings, info);
        format->write(block);
        format->finalize();
        out->finalize();
        bytes = static_cast<Int64>(out->count());
    }
    commitDeletes(writer, delete_path, static_cast<Int64>(nrows), bytes, statistics, std::make_shared<const Block>(writer.header.cloneEmpty()));
}

}

std::shared_ptr<IcebergWriter> icebergWriteBegin(const String & location, int action, const String & spec,
                                                 const chte_file_column * columns, size_t ncolumns, ContextMutablePtr context,
                                                 const FormatSettings & format_settings, Block & header,
                                                 std::vector<DataTypePtr> & sources, std::vector<int32_t> & converts,
                                                 std::vector<size_t> & vectors)
{
    registerIcebergReading();
    auto w = std::make_shared<IcebergWriter>();
    w->action = action;
    w->location = location;
    w->context = context;
    w->format_settings = format_settings;
    w->configuration = localConfiguration(location);
    w->object_storage = localStorage(/* read_only= */ false);
    w->bytes = directoryBytes(location);
    StorageID table_id("chte", "iceberg");

    for (size_t i = 0; i < ncolumns; ++i)
        if (!columns[i].name || (action == CHTE_ICEBERG_CREATE && !columns[i].type) || (action != CHTE_ICEBERG_CREATE && !columns[i].source))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_iceberg_write_begin: column {} has no name, type or source", i);

    if (action == CHTE_ICEBERG_CREATE)
    {
        NamesAndTypesList list;
        for (size_t i = 0; i < ncolumns; ++i)
            list.emplace_back(columns[i].name, DataTypeFactory::instance().get(columns[i].type));
        w->columns = ColumnsDescription(list);
        if (!spec.empty())
        {
            ParserExpression parser;
            w->partition_by = parseQuery(parser, spec, "an Iceberg table's partition spec", DBMS_DEFAULT_MAX_QUERY_SIZE,
                                         DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        }
        return w;
    }
    if (action != CHTE_ICEBERG_APPEND && action != CHTE_ICEBERG_DELETE_POSITIONS && action != CHTE_ICEBERG_DELETE_EQUAL)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_iceberg_write_begin: no action {}", action);

    w->holder = IcebergMetadata::create(w->object_storage, w->configuration, w->context);
    w->metadata = &dynamic_cast<IcebergMetadata &>(*w->holder);
    auto [data_snapshot, state] = w->metadata->getRelevantState(w->context);
    auto & schemas = *w->metadata->getPersistentComponents().schema_processor;
    auto schema = schemas.getClickHouseTableSchemaById(state.schema_id);
    auto type_of = [&](const String & name) -> DataTypePtr
    {
        for (const auto & column : *schema)
            if (column.name == name)
                return column.type;
        throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "the Iceberg table at {} has no column {}", location, name);
    };
    auto given = [&](const String & name) -> const chte_file_column *
    {
        for (size_t i = 0; i < ncolumns; ++i)
            if (columns[i].name == name)
            {
                vectors.push_back(i);
                return &columns[i];
            }
        return nullptr;
    };

    if (action == CHTE_ICEBERG_APPEND)
    {
        /// every column of the table, in its order, as the sink takes them
        for (const auto & column : *schema)
        {
            const auto * c = given(column.name);
            if (!c)
                throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "the rows appended to the Iceberg table at {} have no column {}", location,
                                column.name);
            header.insert({column.type->createColumn(), column.type, column.name});
            sources.push_back(makeNullable(DataTypeFactory::instance().get(c->source)));
            converts.push_back(c->convert);
        }
        auto sample = std::make_shared<const Block>(header.cloneEmpty());
        auto sink = std::make_shared<IcebergStorageSink>(w->object_storage, w->configuration, w->format_settings, sample, w->context,
                                                         nullptr, w->metadata->getPersistentComponents(), table_id);
        w->pipeline = QueryPipeline(std::static_pointer_cast<SinkToStorage>(sink));
        w->executor = std::make_unique<PushingPipelineExecutor>(w->pipeline);
        w->executor->start();
        return w;
    }

    if (action == CHTE_ICEBERG_DELETE_POSITIONS)
    {
        using Transform = Iceberg::IcebergPositionDeleteTransform;
        const char * names[] = {Transform::data_file_path_column_name, Transform::positions_column_name};
        DataTypePtr types[] = {std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt64>()};
        Int32 ids[] = {static_cast<Int32>(Transform::data_file_path_column_field_id), static_cast<Int32>(Transform::positions_column_field_id)};
        for (size_t k = 0; k < 2; ++k)
        {
            const auto * c = given(names[k]);
            if (!c || ncolumns != 2)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "a position delete file's rows are file_path and pos");
            header.insert({types[k]->createColumn(), types[k], names[k]});
            sources.push_back(makeNullable(DataTypeFactory::instance().get(c->source)));
            converts.push_back(c->convert);
            w->field_ids.push_back(ids[k]);
        }
    }
    else
    {
        /// the columns whose values delete a row, as the table's schema has them
        for (size_t i = 0; i < ncolumns; ++i)
        {
            auto type = type_of(columns[i].name);
            auto id = schemas.tryGetColumnIDByName(state.schema_id, columns[i].name);
            if (!id)
                throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "the Iceberg table at {} has no field id of column {}", location, columns[i].name);
            header.insert({type->createColumn(), type, columns[i].name});
            sources.push_back(makeNullable(DataTypeFactory::instance().get(columns[i].source)));
            converts.push_back(columns[i].convert);
            vectors.push_back(i);
            w->field_ids.push_back(*id);
        }
    }
    w->header = header.cloneEmpty();
    w->rows = w->header.cloneEmptyColumns();
    return w;
}

void icebergWrite(IcebergWriter & writer, Block block)
{
    if (writer.action == CHTE_ICEBERG_CREATE)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "an Iceberg table is created with no rows: append them");
    if (writer.action == CHTE_ICEBERG_APPEND)
    {
        writer.executor->push(std::move(block));
        return;
    }
    for (size_t i = 0; i < writer.rows.size(); ++i)
        writer.rows[i]->insertRangeFrom(*block.getByPosition(i).column, 0, block.rows());
}

UInt64 icebergWriteFinish(IcebergWriter & writer)
{
    switch (writer.action)
    {
        case CHTE_ICEBERG_CREATE:
            fs::create_directories(writer.location);
            IcebergMetadata::createInitial(writer.object_storage, writer.configuration, writer.context, writer.columns,
                                           writer.partition_by, nullptr, /* if_not_exists= */ false, nullptr, StorageID("chte", "iceberg"));
            break;
        case CHTE_ICEBERG_APPEND:
            writer.executor->finish();
            writer.executor.reset();
            writer.pipeline.reset();
            break;
        default:
            finishDeletes(writer);
            break;
    }
    UInt64 bytes = directoryBytes(writer.location);
    return bytes > writer.bytes ? bytes - writer.bytes : 0;
}

void icebergWriteCancel(IcebergWriter & writer)
{
    if (writer.executor)
    {
        writer.executor->cancel();
        writer.executor.reset();
    }
    writer.pipeline.reset();
}

}

namespace chte
{

void icebergAlter(const String & location, const String & command, ContextMutablePtr context)
{
    registerIcebergReading();
    auto configuration = localConfiguration(location);
    auto storage = localStorage(/* read_only= */ false);
    std::shared_ptr<IDataLakeMetadata> holder = IcebergMetadata::create(storage, configuration, context);
    auto & metadata = dynamic_cast<IcebergMetadata &>(*holder);

    String query = "ALTER TABLE iceberg " + command;
    ParserAlterQuery parser;
    ASTPtr ast = parseQuery(parser, query, "an Iceberg table's change", DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH,
                            DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    const auto & alter = ast->as<ASTAlterQuery &>();
    AlterCommands commands;
    if (alter.command_list)
        for (const auto & child : alter.command_list->children)
            if (auto parsed = AlterCommand::parse(child->as<ASTAlterCommand>()))
                commands.push_back(std::move(*parsed));
    if (commands.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "an Iceberg table is changed by one command at a time: {}", command);
    metadata.checkAlterIsPossible(commands);
    /// ClickHouse's Iceberg writes are experimental, and its ALTER says so
    context->setSetting("allow_insert_into_iceberg", Field(true));
    metadata.alter(commands, context, StorageID("chte", "iceberg"), nullptr);
}

}
