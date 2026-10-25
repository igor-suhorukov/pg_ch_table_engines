/*
 * External tables (ch_table_engines.md §2.17, CH7): files PostgreSQL does not own, read by ClickHouse's
 * own readers on the calling thread -- Parquet's v3 reader (ParquetV3BlockInputFormat) here, Arrow IPC's
 * decoder of record batches in arrow.cpp, Iceberg's tables in iceberg.cpp -- and never written but by the
 * tests' writers below.
 *
 * A table is its format, its location and its columns: each the caller's name ("c<attnum>"), the
 * files' name of it, the ClickHouse type the caller takes it as (§2.5's) and how its values become
 * that type.  The reader is asked for each column under the file's own name, in the type it reads
 * the caller's as -- the caller's own, cast by the reader where the file's differs (a widened
 * integer, a decimal of another scale), Date32 for a date, DateTime64(6, 'UTC') for a timestamp --
 * Nullable where the file may hold NULLs; the shim then shifts dates and timestamps to PostgreSQL's
 * epoch, refuses a NULL of a column the caller declared NOT NULL and a string that is not UTF-8, and
 * adds each row's id, its file's first row's id and its place in the file, from the row numbers the
 * reader hands with its chunks (ChunkInfoRowNumbers).
 *
 * A listing is the files a location names, each one's footer read -- kept in the process by path,
 * size and modification time -- and its columns resolved and checked.  A file's units are its row
 * groups, which a filter prunes by their statistics as the reader itself does
 * (Parquet::Reader::prefilterAndInitRowGroups()); a read of units hands the reader the filter too,
 * so that its page index, bloom filters and dictionaries pass over what they rule out.
 */
#include "external.h"

#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/FieldAccurateComparison.h>
#include <Common/isValidUTF8.h>
#include <Common/parseGlobs.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatFilterInfo.h>
#include <Formats/FormatParserSharedResources.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/castColumn.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/Impl/Parquet/Prefetcher.h>
#include <Processors/Formats/Impl/Parquet/Reader.h>
#include <Processors/Formats/Impl/Parquet/SchemaConverter.h>
#include <Processors/Formats/Impl/ParquetV3BlockInputFormat.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <base/scope_guard.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <re2/re2.h>

#include <atomic>
#include <filesystem>
#include <set>
#include <sys/stat.h>
#include <unistd.h>

namespace DB
{
void registerOutputFormatParquet(FormatFactory & factory);
}

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_FSTAT;
    extern const int CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN;
    extern const int CANNOT_STAT;
    extern const int FILE_CHANGED_DURING_READ;
    extern const int FILE_DOESNT_EXIST;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int THERE_IS_NO_COLUMN;
    extern const int TYPE_MISMATCH;
    extern const int UNICODE_ERROR;
}

namespace fs = std::filesystem;

namespace chte
{

namespace
{

/// Footers kept in the process, at most this many: a listing's first read of a file costs its footer
constexpr size_t MAX_FOOTERS = 4096;

}

size_t ExternalTable::columnIndex(const String & caller_name) const
{
    auto it = by_name.find(caller_name);
    if (it == by_name.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} has no column {}", name, caller_name);
    return it->second;
}

}

struct chte_file_writer
{
    std::string path;
    std::string temp_path;
    DB::ContextMutablePtr context;
    std::vector<DB::DataTypePtr> types;         /// the file's
    std::vector<DB::DataTypePtr> sources;       /// the batches', Nullable
    std::vector<int32_t> converts;
    /// per column, the batch's vector of it: none, each its own
    std::vector<size_t> vectors;
    DB::Block header;
    std::unique_ptr<DB::WriteBufferFromFile> out;
    DB::OutputFormatPtr format;
    /// an Iceberg table's writer (iceberg.cpp), which the rows go to in place of a file
    std::shared_ptr<chte::IcebergWriter> iceberg;
    bool finished = false;
    /// chte_own_write_begin's (§2.18): the rows written in slices of exactly these rows, but the last, 0 as they
    /// come; the rows of a slice not yet whole
    size_t slice_rows = 0;
    DB::MutableColumns pending;
};

using namespace chte;

namespace chte
{

namespace
{

/// The format settings of an external table's reads: the caller's, then what keeps the reader on the calling
/// thread and its columns' types the ones the extension takes (JSON and geometry as their strings).
void forceFormatSettings(FormatSettings & settings)
{
    settings.arrow.case_insensitive_column_matching = false;
    settings.parquet.enable_row_group_prefetch = false;
    settings.parquet.enable_json_parsing = false;
    settings.parquet.allow_geoparquet_parser = false;
    settings.parquet.case_insensitive_column_matching = false;
    settings.parquet.parallel_encoding = false;
    settings.max_threads = 1;
}

Parquet::ReadOptions readOptions(const FormatSettings & settings)
{
    /// as ParquetV3BlockInputFormat's convertReadOptions(), nullability as the file has it
    Parquet::ReadOptions options;
    options.format = settings;
    options.schema_inference_force_nullable = false;
    options.schema_inference_force_not_nullable = false;
    options.dictionary_filter_limit_bytes = settings.parquet.dictionary_filter_push_down;
    options.min_bytes_for_seek = settings.parquet.local_read_min_bytes_for_seek;
    options.bytes_per_read_task = settings.parquet.local_read_min_bytes_for_seek * 4;
    return options;
}

/// A file's identity: its size and modification time in nanoseconds
std::pair<UInt64, Int64> statIdentity(const struct stat & st)
{
    return {static_cast<UInt64>(st.st_size), Int64(st.st_mtim.tv_sec) * 1000000000 + st.st_mtim.tv_nsec};
}

}

/// Parsing on the caller (max_parsing_threads 1 is ParquetV3BlockInputFormat's "no pool"), no prefetching pool
/// (max_io_threads 0)
FormatParserSharedResourcesPtr singleThreadedResources(const ContextPtr & context)
{
    auto resources = FormatParserSharedResources::singleThreaded(context->getSettingsRef());
    const_cast<size_t &>(resources->max_io_threads) = 0;
    return resources;
}

std::unique_ptr<ReadBufferFromFilePRead> openListed(const String & path, UInt64 size, Int64 mtime, size_t buffer_size)
{
    auto buf = std::make_unique<ReadBufferFromFilePRead>(path, buffer_size, -1, nullptr, 0, size);
    struct stat st;
    if (fstat(buf->getFD(), &st) != 0)
        throw ErrnoException(ErrorCodes::CANNOT_FSTAT, "Cannot fstat {}", path);
    auto [now_size, now_mtime] = statIdentity(st);
    if (now_size != size || now_mtime != mtime)
        throw Exception(ErrorCodes::FILE_CHANGED_DURING_READ,
                        "file {} changed since the statement listed it: {} bytes where {} were", path, now_size, size);
    return buf;
}

namespace
{

std::shared_ptr<ExternalFooter> readParquetFooter(const ExternalTable & table, const String & path, UInt64 size, Int64 mtime)
{
    auto buf = openListed(path, size, mtime);
    Parquet::Prefetcher prefetcher;
    prefetcher.init(buf.get(), table.read_options, /* parser_shared_resources_= */ nullptr);
    auto footer = std::make_shared<ExternalFooter>();
    footer->size = size;
    footer->mtime = mtime;
    auto metadata = std::make_shared<parquet::format::FileMetaData>(Parquet::Reader::readFileMetaData(prefetcher));
    for (const auto & row_group : metadata->row_groups)
    {
        footer->unit_rows.push_back(static_cast<UInt64>(std::max<Int64>(row_group.num_rows, 0)));
        footer->rows += footer->unit_rows.back();
    }
    footer->parquet = std::move(metadata);
    return footer;
}

/// Footers kept in the process (MAX_FOOTERS), by format and path
std::map<std::pair<int, String>, std::shared_ptr<const ExternalFooter>> footers;

/// VC25: the footers read from their files in the process (chte_footers_read), but those a check of a summary reads
std::atomic<uint64_t> footers_read{0};
thread_local int checking = 0;

/// A summary's answer checked against its file's footer (CHTE_SPEC_CHECK): its reads not counted
struct CheckingReads
{
    CheckingReads() { ++checking; }
    ~CheckingReads() { --checking; }
};

}

namespace
{

/// A file's footer read, or found kept, of a known identity: an Arrow file's units' rows, where given, not read again
std::shared_ptr<const ExternalFooter> footerKept(const ExternalTable & table, const String & path, UInt64 size, Int64 mtime,
                                                 const std::vector<UInt64> * unit_rows)
{
    auto key = std::make_pair(table.format, path);
    auto it = footers.find(key);
    if (it != footers.end() && it->second->size == size && it->second->mtime == mtime)
        return it->second;
    std::shared_ptr<const ExternalFooter> footer = table.format == CHTE_EXTERNAL_ARROW
        ? readArrowFooter(table, path, size, mtime, unit_rows)
        : readParquetFooter(table, path, size, mtime);
    if (!checking)
        footers_read.fetch_add(1, std::memory_order_relaxed);
    if (footers.size() >= MAX_FOOTERS)
        footers.clear();
    footers[key] = footer;
    return footer;
}

}

std::shared_ptr<const ExternalFooter> footerOf(const ExternalTable & table, const String & path, UInt64 size, Int64 mtime)
{
    return footerKept(table, path, size, mtime, nullptr);
}

std::shared_ptr<const ExternalFooter> fullFooter(const ExternalTable & table, const ListedFile & listed)
{
    const auto & listed_footer = listed.resolved->footer;
    if (listed_footer->parquet || listed_footer->arrow)
        return listed_footer;
    auto footer = footerKept(table, listed.path, listed.size, listed.mtime, &listed_footer->unit_rows);
    if (footer->unit_rows != listed_footer->unit_rows)
        throw Exception(ErrorCodes::INCORRECT_DATA, "file {} holds {} rows in {} units, where its part's summary has {} in {}",
                        listed.path, footer->rows, footer->unit_rows.size(), listed_footer->rows, listed_footer->unit_rows.size());
    return footer;
}

namespace
{

/// A file's columns, as ClickHouse's schema inference of its format reads them
NamesAndTypesList fileSchema(const ExternalTable & table, const ExternalFooter & footer)
{
    if (table.format == CHTE_EXTERNAL_ARROW)
        return arrowFileSchema(table, footer);
    Parquet::SchemaConverter schemer(*footer.parquet, table.read_options, /* sample_block= */ nullptr);
    return schemer.inferSchema();
}

/// Whether a value of the file's type, Nullable and LowCardinality left out, is one of the caller's
/// column exactly, its read type cast as ClickHouse casts it
bool takes(const ExternalColumn & column, const DataTypePtr & file_type)
{
    WhichDataType file(file_type);
    WhichDataType caller(column.type);
    switch (column.convert)
    {
        case CHTE_CONVERT_DATE:
            return file.isDate() || file.isDate32();
        case CHTE_CONVERT_TIMESTAMP:
            return file.isDateTime() || file.isDateTime64();
        default:
            break;
    }
    /// a table of its own files' one-byte column, "char" and its kin (§2.18)
    if (caller.isInt8())
        return file.isInt8();
    if (caller.isInt16())
        return file.isInt8() || file.isUInt8() || file.isInt16();
    if (caller.isInt32())
        return file.isInt8() || file.isUInt8() || file.isInt16() || file.isUInt16() || file.isInt32();
    if (caller.isInt64())
        return file.isInt8() || file.isUInt8() || file.isInt16() || file.isUInt16() || file.isInt32() || file.isUInt32()
            || file.isInt64();
    if (caller.isUInt32())
        return file.isUInt8() || file.isUInt16() || file.isUInt32();
    /// a table of its own files' _pg_rowid (§2.18)
    if (caller.isUInt64())
        return file.isUInt64();
    if (caller.isFloat32())
        return file.isFloat32();
    if (caller.isFloat64())
        return file.isFloat32() || file.isFloat64();
    if (isBool(column.type))
        return isBool(file_type);
    if (caller.isDecimal())
    {
        UInt32 precision = getDecimalPrecision(*column.type);
        UInt32 scale = getDecimalScale(*column.type);
        if (file.isDecimal())
        {
            UInt32 file_precision = getDecimalPrecision(*file_type);
            UInt32 file_scale = getDecimalScale(*file_type);
            return file_scale <= scale && file_precision - file_scale <= precision - scale;
        }
        size_t digits = file.isInt8() || file.isUInt8() ? 3 : file.isInt16() || file.isUInt16() ? 5
            : file.isInt32() || file.isUInt32() ? 10 : file.isInt64() ? 19 : 0;
        return digits > 0 && digits <= precision - scale;
    }
    if (caller.isString())
        return file.isString() || file.isFixedString();
    if (caller.isFixedString())
        return file.isFixedString() && file_type->getSizeOfValueInMemory() == column.type->getSizeOfValueInMemory();
    return false;
}

/// The column of a file's schema that is the caller's column: by its name exactly, else regardless of
/// case where one alone matches so (*ambiguous where more do)
const NameAndTypePair * findColumn(const NamesAndTypesList & schema, const String & name, bool & ambiguous)
{
    ambiguous = false;
    for (const auto & column : schema)
        if (column.name == name)
            return &column;
    const NameAndTypePair * found = nullptr;
    String lower = boost::algorithm::to_lower_copy(name);
    for (const auto & column : schema)
        if (boost::algorithm::to_lower_copy(column.name) == lower)
        {
            if (found)
            {
                ambiguous = true;
                return nullptr;
            }
            found = &column;
        }
    return found;
}

}

void resolveColumns(const ExternalTable & table, ResolvedFile & file, const String & where)
{
    for (const auto & column : table.columns)
    {
        bool ambiguous;
        const auto * in_file = findColumn(file.schema, column.file_name, ambiguous);
        if (!in_file && !ambiguous && table.own && column.name != ROWID)
        {
            /// a table of its own files' column added after the file was written (§2.18)
            file.names.emplace_back();
            file.file_types.push_back(nullptr);
            file.nullable.push_back(1);
            file.absent.push_back(1);
            continue;
        }
        if (!in_file && ambiguous)
            throw Exception(ErrorCodes::THERE_IS_NO_COLUMN,
                            "column \"{}\" of \"{}\" names more than one column of {} regardless of case, and none exactly",
                            column.file_name, table.name, where);
        if (!in_file)
            throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "column \"{}\" of \"{}\" is not in {}", column.file_name,
                            table.name, where);
        DataTypePtr bare = removeLowCardinalityAndNullable(in_file->type);
        if (!takes(column, bare))
            throw Exception(ErrorCodes::TYPE_MISMATCH,
                            "column \"{}\" of \"{}\" cannot take the values of {}, of ClickHouse's type {}", column.file_name,
                            table.name, where, in_file->type->getName());
        file.names.push_back(in_file->name);
        file.file_types.push_back(in_file->type);
        file.nullable.push_back(isNullableOrLowCardinalityNullable(in_file->type) ? 1 : 0);
        file.absent.push_back(0);
    }
}

namespace
{

/// A file as the table reads it, its columns checked against the table's
std::shared_ptr<const ResolvedFile> resolve(ExternalTable & table, const String & path, UInt64 size, Int64 mtime)
{
    auto it = table.resolved.find(path);
    if (it != table.resolved.end() && it->second->footer->size == size && it->second->footer->mtime == mtime)
        return it->second;

    auto file = std::make_shared<ResolvedFile>();
    file->footer = footerOf(table, path, size, mtime);
    file->schema = fileSchema(table, *file->footer);
    resolveColumns(table, *file, "file " + path);
    table.resolved[path] = file;
    return file;
}

/// The files a glob names: its path's segments matched one by one, "**" any number of directories; a name
/// beginning with '.' only where the segment matching it does
void globSegments(const String & dir, const std::vector<String> & segments, size_t i, std::set<String> & out, size_t depth)
{
    if (i == segments.size() || depth > 1000)
        return;
    const String & segment = segments[i];
    bool last = i + 1 == segments.size();
    std::error_code ec;
    if (segment == "**")
    {
        globSegments(dir, segments, i + 1, out, depth + 1);
        for (fs::directory_iterator it(dir, ec), end; !ec && it != end; it.increment(ec))
        {
            String name = it->path().filename();
            if (!name.starts_with('.') && it->is_directory(ec))
                globSegments(dir + name + "/", segments, i, out, depth + 1);
        }
        return;
    }
    if (segment.find_first_of("*?{") == String::npos)
    {
        String path = dir + segment;
        if (last ? fs::is_regular_file(path, ec) : fs::is_directory(path, ec))
        {
            if (last)
                out.insert(path);
            else
                globSegments(path + "/", segments, i + 1, out, depth + 1);
        }
        return;
    }
    re2::RE2 matcher(makeRegexpPatternFromGlobs("/" + segment));
    if (!matcher.ok())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "cannot match the glob {}: {}", segment, matcher.error());
    for (fs::directory_iterator it(dir, ec), end; !ec && it != end; it.increment(ec))
    {
        String name = it->path().filename();
        if (name.starts_with('.') && !segment.starts_with('.'))
            continue;
        if (!re2::RE2::FullMatch("/" + name, matcher))
            continue;
        if (last && it->is_regular_file(ec))
            out.insert(dir + name);
        else if (!last && it->is_directory(ec))
            globSegments(dir + name + "/", segments, i + 1, out, depth + 1);
    }
}

/// The files of a directory and those below it, but those whose names begin with '.' or '_', Spark's and
/// Hadoop's markers among them
void walkDirectory(const String & dir, std::set<String> & out, size_t depth)
{
    std::error_code ec;
    if (depth > 1000)
        return;
    for (fs::directory_iterator it(dir, ec), end; !ec && it != end; it.increment(ec))
    {
        String name = it->path().filename();
        if (name.starts_with('.') || name.starts_with('_'))
            continue;
        if (it->is_directory(ec))
            walkDirectory(dir + name + "/", out, depth + 1);
        else if (it->is_regular_file(ec))
            out.insert(dir + name);
    }
}

/// The files a location names, in their paths' order: a glob's, a directory's, or the file itself
std::vector<String> locationFiles(const String & location)
{
    std::set<String> files;
    if (location.find_first_of("*?{") != String::npos)
    {
        std::vector<String> segments;
        size_t start = 1;
        while (start <= location.size())
        {
            size_t slash = location.find('/', start);
            if (slash == String::npos)
                slash = location.size();
            if (slash > start)
                segments.push_back(location.substr(start, slash - start));
            start = slash + 1;
        }
        globSegments("/", segments, 0, files, 0);
        return {files.begin(), files.end()};
    }
    std::error_code ec;
    auto status = fs::status(location, ec);
    if (ec || !fs::exists(status))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "location {} does not exist", location);
    if (fs::is_directory(status))
    {
        walkDirectory(location.ends_with('/') ? location : location + "/", files, 0);
        return {files.begin(), files.end()};
    }
    return {location};
}

/// A listing of the files, their first rows' ids the rows of the files before them
void listFiles(chte_external_listing & listing, const std::vector<std::tuple<String, UInt64, Int64>> & files)
{
    UInt64 first_row = 0;
    for (const auto & [path, size, mtime] : files)
    {
        ListedFile file;
        file.path = path;
        file.size = size;
        file.mtime = mtime;
        file.first_row = first_row;
        file.resolved = resolve(*listing.table, path, size, mtime);
        file.rows = file.resolved->footer->rows;
        file.units = file.resolved->footer->unit_rows.size();
        first_row += file.rows;
        listing.files.push_back(std::move(file));
    }
}

const ListedFile & listedFile(const chte_external_listing & listing, size_t file)
{
    if (file >= listing.files.size())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "a listing of {} files has no file {}", listing.files.size(), file);
    return listing.files[file];
}

/// A listed file as a table reads it: as the listing resolved it, or, for another handle of the table -- a
/// rewrite's, of the descriptor the old rows were written with -- resolved again for its own columns
std::shared_ptr<const ResolvedFile> resolvedFor(ExternalTable & table, const chte_external_listing & listing, const ListedFile & file)
{
    if (listing.table.get() == &table)
        return file.resolved;
    if (table.format == CHTE_EXTERNAL_ICEBERG)
        return icebergResolvedFor(table, listing);
    return resolve(table, file.path, file.size, file.mtime);
}

/// A filter's columns in one file: its names of them, and the types they are read as there
class FileColumns final : public FilterColumns
{
public:
    FileColumns(const ExternalTable & table_, const ResolvedFile & file_) : table(table_), file(file_) { }

    NameAndTypePair input(const String & name) const override
    {
        size_t i = table.columnIndex(name);
        return {file.names[i], fileReadType(table, file, i)};
    }

    ColumnWithTypeAndName constant(const String & name, const chte_vector & values, uint32_t i) const override
    {
        const auto & column = table.column(name);
        auto value = column.type->createColumn();
        appendValue(*value, values, i);
        switch (column.convert)
        {
            case CHTE_CONVERT_DATE:
            {
                /// days from 2000-01-01 as Date32's from 1970-01-01
                auto date = ColumnInt32::create();
                date->insertValue(assert_cast<const ColumnInt32 &>(*value).getData()[0] + PG_EPOCH_DAYS);
                return {std::move(date), column.read_type, ""};
            }
            case CHTE_CONVERT_TIMESTAMP:
            {
                /// microseconds from 2000-01-01 as DateTime64(6)'s from 1970-01-01
                auto timestamp = ColumnDecimal<DateTime64>::create(0, 6);
                timestamp->insertValue(DateTime64(assert_cast<const ColumnInt64 &>(*value).getData()[0] + PG_EPOCH_MICROSECONDS));
                return {std::move(timestamp), column.read_type, ""};
            }
            default:
                return {std::move(value), column.type, ""};
        }
    }

private:
    const ExternalTable & table;
    const ResolvedFile & file;
};

}

/// none where the filter names a column the header lacks: the filter only prunes, the caller evaluating its
/// quals on what is read
FormatFilterInfoPtr formatFilter(const chte_filter & filter, const ExternalTable & table, const ResolvedFile & file,
                                 const Block & keys, ColumnMapperPtr mapper)
{
    auto dag = std::make_shared<ActionsDAG>();
    const auto & predicate = buildFilter(*dag, filter.nodes.data(), filter.nodes.size(), table.context, FileColumns(table, file));
    dag->getOutputs() = {&predicate};
    for (const auto & name : dag->getRequiredColumnsNames())
        if (!keys.has(name))
            return nullptr;
    auto info = std::make_shared<FormatFilterInfo>(dag, table.context, std::move(mapper), nullptr, nullptr);
    info->initKeyConditionOnce(keys);
    return info;
}

Block fileHeader(const ExternalTable & table, const ResolvedFile & file, const Names & names)
{
    Block header;
    for (const auto & name : names)
    {
        size_t i = table.columnIndex(name);
        if (file.isAbsent(i))
            continue;
        DataTypePtr type = fileReadType(table, file, i);
        header.insert({type->createColumn(), type, file.names[i]});
    }
    return header;
}

namespace
{

/// The filter's columns, as a header of the file's
Block filterKeys(const chte_filter & filter, const ExternalTable & table, const ResolvedFile & file)
{
    Names names;
    for (const auto & node : filter.nodes)
        if (node.column && std::find(names.begin(), names.end(), node.column) == names.end())
            names.emplace_back(node.column);
    return fileHeader(table, file, names);
}

/// The row groups the reader's pruning keeps of a file's footer, kept[] of the file's units set
void keptByFooter(const ExternalTable & table, const ListedFile & file, const ExternalFooter & footer, const Block & keys,
                  const FormatFilterInfoPtr & info, std::vector<uint8_t> & kept)
{
    auto buf = openListed(file.path, file.size, file.mtime);
    Parquet::Reader reader;
    reader.prefetcher.init(buf.get(), table.read_options, /* parser_shared_resources_= */ nullptr);
    /// the footer's metadata lent to the reader, and taken back once its pruning is done, where a copy of it each
    /// statement took a fifth of a fresh plan of ClickBench's 105 columns, and 2.8 ms of a warm one (VC25): the
    /// reader's row groups point into its row groups, which the move keeps where they are
    auto & metadata = const_cast<parquet::format::FileMetaData &>(*footer.parquet);
    reader.file_metadata = std::move(metadata);
    SCOPE_EXIT({ metadata = std::move(reader.file_metadata); });
    reader.init(table.read_options, keys, info);
    reader.prefilterAndInitRowGroups(std::nullopt);
    std::fill(kept.begin(), kept.end(), 0);
    for (const auto & row_group : reader.row_groups)
        kept.at(row_group.row_group_idx) = 1;
}

}

namespace
{

/// The row groups of a file the filter keeps by their statistics, as the reader keeps them
/// (Parquet::Reader::prefilterAndInitRowGroups()), worked out once a filter and a file
const std::vector<uint8_t> & keptUnits(chte_filter & filter, const ExternalTable & table, const ListedFile & file,
                                       const ResolvedFile & resolved)
{
    String key = fmt::format("{}\n{}\n{}", file.path, file.size, file.mtime);
    auto it = filter.kept_units.find(key);
    if (it != filter.kept_units.end())
        return it->second;

    const auto & footer = *resolved.footer;
    std::vector<uint8_t> kept(footer.unit_rows.size(), 1);
    Block keys = filterKeys(filter, table, resolved);
    auto info = keys.columns() > 0 ? formatFilter(filter, table, resolved, keys) : nullptr;
    if (info && info->key_condition && footer.summary)
    {
        /// VC25: each unit's ranges of the keys' columns from its part's summary, as the reader's pruning reads its row
        /// group's statistics, the file not read -- a column of a type other than the summary's, or of no statistics,
        /// unbounded
        if (table.read_options.format.parquet.filter_push_down)
        {
            DataTypes types = keys.getDataTypes();
            std::vector<const OwnSummaryColumn *> columns;
            for (size_t c = 0; c < keys.columns(); ++c)
            {
                const auto & k = keys.getByPosition(c);
                auto it_column = footer.summary->columns.find(k.name);
                columns.push_back(it_column != footer.summary->columns.end() && it_column->second.type == k.type->getName()
                                  ? &it_column->second : nullptr);
            }
            for (size_t u = 0; u < kept.size(); ++u)
            {
                Hyperrectangle hyperrectangle(keys.columns(), Range::createWholeUniverse());
                for (size_t c = 0; c < columns.size(); ++c)
                    if (columns[c] && u < columns[c]->ranges.size() && columns[c]->ranges[u])
                        hyperrectangle[c] = *columns[c]->ranges[u];
                kept[u] = info->key_condition->checkInHyperrectangle(hyperrectangle, types).can_be_true ? 1 : 0;
            }
        }
        if (specCheck())
        {
            CheckingReads checking_reads;
            std::vector<uint8_t> again(kept.size(), 1);
            keptByFooter(table, file, *fullFooter(table, file), keys, info, again);
            if (again != kept)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "file {}: the units its summary keeps of a filter are not those its footer keeps",
                                file.path);
        }
    }
    else if (info && info->key_condition)
        keptByFooter(table, file, footer, keys, info, kept);
    return filter.kept_units.emplace(std::move(key), std::move(kept)).first->second;
}

}

DataTypePtr fileReadType(const ExternalTable & table, const ResolvedFile & file, size_t i)
{
    return file.nullable[i] ? makeNullable(table.columns[i].read_type) : table.columns[i].read_type;
}

ColumnPtr convertColumn(const ExternalColumn & column, ColumnPtr col, const String & path)
{
    col = col->convertToFullColumnIfConst()->convertToFullColumnIfSparse();
    if (column.not_null)
        if (const auto * nullable = typeid_cast<const ColumnNullable *>(col.get()))
        {
            const auto & map = nullable->getNullMapData();
            if (memchr(map.data(), 1, map.size()))
                throw Exception(ErrorCodes::CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN,
                                "file {} holds a NULL of column \"{}\", which the table declares NOT NULL", path, column.file_name);
            col = nullable->getNestedColumnPtr();
        }
    if (column.convert != CHTE_CONVERT_NONE)
    {
        auto mutable_col = IColumn::mutate(std::move(col));
        IColumn * values = mutable_col.get();
        if (auto * nullable = typeid_cast<ColumnNullable *>(values))
            values = &nullable->getNestedColumn();
        if (column.convert == CHTE_CONVERT_DATE)
            for (auto & v : assert_cast<ColumnInt32 &>(*values).getData())
                v -= PG_EPOCH_DAYS;
        else
            for (auto & v : assert_cast<ColumnDecimal<DateTime64> &>(*values).getData())
                v.value -= PG_EPOCH_MICROSECONDS;
        col = std::move(mutable_col);
    }
    if (column.check_utf8)
    {
        const IColumn * values = col.get();
        if (const auto * nullable = typeid_cast<const ColumnNullable *>(values))
            values = &nullable->getNestedColumn();
        const auto & strings = assert_cast<const ColumnString &>(*values);
        const auto & chars = strings.getChars();
        const auto & offsets = strings.getOffsets();
        size_t prev = 0;
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            const UInt8 * data = chars.data() + prev;
            size_t len = offsets[i] - prev;
            if (memchr(data, 0, len) || !UTF8::isValidUTF8(data, len))
                throw Exception(ErrorCodes::UNICODE_ERROR, "file {} holds a value of column \"{}\" that is not valid UTF-8 text",
                                path, column.file_name);
            prev = offsets[i];
        }
    }
    return col;
}

namespace
{

/// What a reader of a pipeline reads through, kept with it
struct PipelineRead
{
    std::shared_ptr<ExternalTable> table;
    std::shared_ptr<const ResolvedFile> resolved;
    String path;
    UInt64 first_row = 0;
    /// what the pipeline's source reads through, its file's buffer
    std::shared_ptr<void> keep;
    QueryPipeline pipeline;
    std::unique_ptr<PullingPipelineExecutor> executor;
    /// per column asked for: the table's column, or SIZE_MAX the row id; its place in the pipeline's header,
    /// its type there, and the type it is cast to first where asked
    std::vector<size_t> columns;
    std::vector<size_t> positions;
    std::vector<DataTypePtr> types;
    std::vector<DataTypePtr> casts;

    bool next(Block & block)
    {
        Chunk chunk;
        while (executor->pull(chunk))
        {
            checkInterrupted();
            size_t rows = chunk.getNumRows();
            if (rows == 0)
                continue;
            ColumnsWithTypeAndName out;
            const auto & in = chunk.getColumns();
            for (size_t c = 0; c < columns.size(); ++c)
            {
                if (columns[c] == SIZE_MAX)
                {
                    auto info = chunk.getChunkInfos().get<ChunkInfoRowNumbers>();
                    if (!info)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "the reader of {} handed rows without their numbers", path);
                    auto ids = ColumnUInt64::create(rows);
                    auto & data = ids->getData();
                    UInt64 base = first_row + info->row_num_offset;
                    if (info->applied_filter)
                    {
                        const auto & filter = *info->applied_filter;
                        size_t k = 0;
                        for (size_t i = 0; i < filter.size() && k < rows; ++i)
                            if (filter[i])
                                data[k++] = base + i;
                    }
                    else
                        for (size_t i = 0; i < rows; ++i)
                            data[i] = base + i;
                    out.emplace_back(std::move(ids), std::make_shared<DataTypeUInt64>(), ROWID);
                    continue;
                }
                const auto & column = table->columns[columns[c]];
                if (positions[c] == ABSENT)
                {
                    /// not in the file: defaults, which the caller passes over (chte_read_present)
                    out.emplace_back(column.type->createColumnConstWithDefaultValue(rows)->convertToFullColumnIfConst(),
                                     column.type, column.name);
                    continue;
                }
                ColumnPtr col = in.at(positions[c]);
                if (casts[c])
                {
                    try
                    {
                        col = castColumn({col, types[c], column.file_name}, casts[c]);
                    }
                    catch (Exception & e)
                    {
                        e.addMessage("while reading column {} of file {}", column.file_name, path);
                        throw;
                    }
                }
                out.emplace_back(convertColumn(column, col, path), column.type, column.name);
            }
            block = Block(std::move(out));
            return true;
        }
        return false;
    }
};

/// A reader of a listed file's units (none: all of them), or of its rows at offsets; the filter, where given,
/// for the reader's own pruning
std::unique_ptr<chte_reader> openExternalReader(chte_table & table, const chte_external_listing & listing, size_t file,
                                                const chte_filter * filter, const Names & names,
                                                std::optional<std::vector<size_t>> units,
                                                std::shared_ptr<const PaddedPODArray<UInt64>> rows_to_read)
{
    auto & ext = *table.external;
    const auto & listed = listedFile(listing, file);
    if (ext.format == CHTE_EXTERNAL_ARROW)
        return openArrowReader(table, resolvedFor(ext, listing, listed), listed, names, std::move(units), std::move(rows_to_read));
    if (ext.format == CHTE_EXTERNAL_ICEBERG)
        return openIcebergReader(table, listing, listed, filter, names, std::move(rows_to_read));
    auto resolved = resolvedFor(ext, listing, listed);

    /// the row id a row's place, but a table of its own files' (§2.18), a column of the file
    Names read;
    for (const auto & name : names)
        if ((name != ROWID || ext.own) && std::find(read.begin(), read.end(), name) == read.end() &&
            !(name != ROWID && resolved->isAbsent(ext.columnIndex(name))))
            read.push_back(name);
    auto header = std::make_shared<const Block>(fileHeader(ext, *resolved, read));

    FormatFilterInfoPtr info;
    if (filter && !filter->nodes.empty())
        info = formatFilter(*filter, ext, *resolved, *header);
    if (rows_to_read)
    {
        if (!info)
            info = std::make_shared<FormatFilterInfo>();
        info->rows_to_read = std::move(rows_to_read);
    }

    std::shared_ptr<ReadBufferFromFilePRead> buf = openListed(listed.path, listed.size, listed.mtime);
    auto format = std::make_shared<ParquetV3BlockInputFormat>(
        *buf, header, ext.format_settings, singleThreadedResources(ext.context), info,
        ext.format_settings.parquet.local_read_min_bytes_for_seek);
    if (units)
        format->setBucketsToRead(std::make_shared<ParquetFileBucketInfo>(*units));
    return pipelineReader(table, std::move(resolved), listed, names, QueryPipeline(Pipe(format)), /* cast= */ false, std::move(buf));
}

ExternalTable & externalOf(chte_table * table)
{
    if (!table || !table->external)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "not an external table");
    return *table->external;
}

}

std::unique_ptr<chte_reader> pipelineReader(chte_table & table, std::shared_ptr<const ResolvedFile> resolved, const ListedFile & listed,
                                            const Names & names, QueryPipeline pipeline, bool cast, std::shared_ptr<void> keep)
{
    auto & ext = *table.external;
    auto state = std::make_shared<PipelineRead>();
    state->table = table.external;
    state->resolved = std::move(resolved);
    state->path = listed.path;
    state->first_row = listed.first_row;
    state->keep = std::move(keep);
    state->pipeline = std::move(pipeline);
    const auto & header = state->pipeline.getHeader();
    for (const auto & name : names)
    {
        if (name == ROWID && !ext.own)
        {
            state->columns.push_back(SIZE_MAX);
            state->positions.push_back(SIZE_MAX);
            state->types.push_back(nullptr);
            state->casts.push_back(nullptr);
            continue;
        }
        size_t i = ext.columnIndex(name);
        if (state->resolved->isAbsent(i))
        {
            state->columns.push_back(i);
            state->positions.push_back(ABSENT);
            state->types.push_back(nullptr);
            state->casts.push_back(nullptr);
            continue;
        }
        size_t position = header.getPositionByName(state->resolved->names[i]);
        state->columns.push_back(i);
        state->positions.push_back(position);
        state->types.push_back(header.getByPosition(position).type);
        state->casts.push_back(cast ? fileReadType(ext, *state->resolved, i) : nullptr);
    }
    state->executor = std::make_unique<PullingPipelineExecutor>(state->pipeline);

    auto r = std::make_unique<chte_reader>();
    r->table = &table;
    r->present.assign(names.size(), 1);
    for (size_t c = 0; c < names.size(); ++c)
        if (state->positions[c] == ABSENT)
            r->present[c] = 0;
    r->next_block = [raw = state.get()](Block & block) { return raw->next(block); };
    r->external_state = std::move(state);
    return r;
}

namespace
{

/// A file's column statistics from its Parquet footer: its rows and NULLs where every row group counts them, its least
/// and greatest values where every one has them exact -- the column's chunk in each row group decoded as the reader
/// decodes them (Parquet::Reader::getHyperrectangleForRowGroup()) into the type it reads the column as
struct FileColumnStats
{
    bool counted = false;
    UInt64 rows = 0;
    UInt64 nulls = 0;
    bool bounded = false;
    Field min;
    Field max;
};

FileColumnStats parquetFileStats(const ExternalTable & ext, const ResolvedFile & resolved,
                                 const parquet::format::FileMetaData & metadata, size_t i)
{
    FileColumnStats s;
    Block sample = fileHeader(ext, resolved, {ext.columns[i].name});
    Parquet::SchemaConverter schemer(metadata, ext.read_options, &sample);
    schemer.prepareForReading();
    if (schemer.primitive_columns.size() != 1)
        return s;
    const auto & primitive = schemer.primitive_columns.front();
    const IDataType & output_type = *sample.getByPosition(0).type;
    bool nullable = resolved.nullable[i];

    UInt64 rows = 0;
    UInt64 nulls = 0;
    bool counted = true;
    bool bounded = primitive.decoder.allow_stats;
    Field min;
    Field max;
    for (const auto & row_group : metadata.row_groups)
    {
        if (row_group.num_rows <= 0)
            continue;
        rows += static_cast<UInt64>(row_group.num_rows);
        const auto & meta = row_group.columns.at(primitive.column_idx).meta_data;
        if (!meta.__isset.statistics)
        {
            counted = bounded = false;
            break;
        }
        const auto & statistics = meta.statistics;
        UInt64 group_nulls = 0;
        if (statistics.__isset.null_count)
            group_nulls = static_cast<UInt64>(statistics.null_count);
        else if (nullable)
            counted = false;
        nulls += group_nulls;
        if (group_nulls == static_cast<UInt64>(row_group.num_rows))
            continue;			/* every value NULL: no bound of its */
        if (!bounded || !statistics.__isset.min_value || !statistics.__isset.max_value
            || (statistics.__isset.is_min_value_exact && !statistics.is_min_value_exact)
            || (statistics.__isset.is_max_value_exact && !statistics.is_max_value_exact))
        {
            bounded = false;
            continue;
        }
        Field group_min;
        Field group_max;
        primitive.decoder.decodeField(statistics.min_value, /* is_max= */ false, *primitive.decoded_type, output_type, group_min);
        primitive.decoder.decodeField(statistics.max_value, /* is_max= */ true, *primitive.decoded_type, output_type, group_max);
        if (group_min.isNull() || group_max.isNull() || accurateLess(group_max, group_min))
        {
            bounded = false;
            continue;
        }
        if (min.isNull() || accurateLess(group_min, min))
            min = group_min;
        if (max.isNull() || accurateLess(max, group_max))
            max = group_max;
    }
    s.counted = counted;
    s.rows = rows;
    s.nulls = nulls;
    s.bounded = bounded && !min.isNull() && !max.isNull();
    s.min = std::move(min);
    s.max = std::move(max);
    return s;
}

/// VC25: a table column's own in its file's part's summary, where the summary has it of the type the table reads it as
const OwnSummaryColumn * summaryColumn(const ExternalTable & ext, const ResolvedFile & resolved, size_t i)
{
    const auto & summary = resolved.footer->summary;
    if (!summary || resolved.isAbsent(i))
        return nullptr;
    auto it = summary->columns.find(resolved.names[i]);
    if (it == summary->columns.end() || it->second.type != fileReadType(ext, resolved, i)->getName())
        return nullptr;
    return &it->second;
}

}

/// From its part's summary (VC25), or its footer
void parquetStats(chte_table & table, const ExternalTable & ext, const ListedFile & file, const ResolvedFile & resolved,
                  const String & name, chte_column_stats * stats)
{
    *stats = {};
    table.scratch.clear();
    size_t i = ext.columnIndex(name);
    const auto & column = ext.columns[i];
    if (resolved.isAbsent(i))
        return;
    FileColumnStats s;
    if (const auto * kept = summaryColumn(ext, resolved, i))
    {
        s.counted = kept->counted;
        s.rows = kept->rows;
        s.nulls = kept->nulls;
        s.bounded = kept->bounded;
        s.min = kept->min;
        s.max = kept->max;
        if (specCheck())
        {
            CheckingReads checking_reads;
            auto again = parquetFileStats(ext, resolved, *fullFooter(ext, file)->parquet, i);
            if (again.counted != s.counted || again.rows != s.rows || again.nulls != s.nulls || again.bounded != s.bounded
                || again.min != s.min || again.max != s.max)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "file {}: column {}'s statistics in its summary are not its footer's",
                                file.path, name);
        }
    }
    else
        s = parquetFileStats(ext, resolved, *fullFooter(ext, file)->parquet, i);
    if (!s.counted)
        return;
    stats->has_stats = 1;
    stats->has_null_count = 1;
    stats->rows = s.rows;
    stats->null_count = s.nulls;
    if (!s.bounded)
        return;
    const Field & min = s.min;
    const Field & max = s.max;
    auto bound = [&](const Field & value) -> ColumnPtr
    {
        auto read = column.read_type->createColumn();
        read->insert(value);
        auto as_read = ColumnWithTypeAndName(std::move(read), column.read_type, "");
        ExternalColumn plain = column;
        plain.not_null = false;
        plain.check_utf8 = false;
        return convertColumn(plain, as_read.column, file.path);
    };
    table.scratch.push_back(bound(min));
    table.scratch.push_back(bound(max));
    stats->min = vectorOf(*table.scratch[0]);
    stats->max = vectorOf(*table.scratch[1]);
    stats->has_minmax = 1;
}

namespace
{

DataTypePtr readType(const ExternalColumn & column)
{
    switch (column.convert)
    {
        case CHTE_CONVERT_DATE:
            return std::make_shared<DataTypeDate32>();
        case CHTE_CONVERT_TIMESTAMP:
            return std::make_shared<DataTypeDateTime64>(6, "UTC");
        default:
            return column.type;
    }
}

}

void keepExternalFilter(chte_filter & filter, const chte_filter_node * nodes, size_t nnodes)
{
    const auto & ext = *filter.table->external;
    for (size_t i = 0; i < nnodes; ++i)
    {
        chte_filter_node node = nodes[i];
        if (node.column)
        {
            filter.names.push_back(std::make_shared<std::string>(node.column));
            node.column = filter.names.back()->c_str();
        }
        if (node.nvalues > 0)
        {
            /// the values in a column of the caller's type, which the node then points into
            auto values = ext.column(node.column).type->createColumn();
            appendVector(*values, node.nvalues, node.values);
            node.values = vectorOf(*values);
            filter.values.push_back(std::move(values));
        }
        filter.nodes.push_back(node);
    }
}

}

namespace
{

/// An external table, or one of its own files (§2.18): its location not read, its _pg_rowid a column of the files
void openExternal(const chte_external_def * def, bool own, chte_table ** table)
{
    auto & e = engine();
    if (!def || (!def->location && !own))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_external_open: location is required");
    if (own ? def->format != CHTE_EXTERNAL_PARQUET && def->format != CHTE_EXTERNAL_ARROW
            : def->format != CHTE_EXTERNAL_PARQUET && def->format != CHTE_EXTERNAL_ARROW && def->format != CHTE_EXTERNAL_ICEBERG)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} tables of format {} are not supported", own ? "own files'" : "external",
                        def->format);
    auto ext = std::make_shared<ExternalTable>();
    ext->format = def->format;
    ext->name = def->name && *def->name ? def->name : "table";
    ext->own = own;
    if (!own)
    {
        ext->location = def->location;
        if (ext->location.empty() || ext->location.front() != '/')
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "location {} is not an absolute path", ext->location);
    }
    ext->context = Context::createCopy(e.global);
    applySettings(ext->context, def->settings);
    /// whatever the caller's settings say, nothing starts a thread; and an Iceberg table's metadata is read
    /// again for each statement, which ClickHouse's cache of it would not (its latest version kept a while)
    applySingleThreadedSettings(ext->context);
    ext->context->setSetting("use_iceberg_metadata_files_cache", Field(false));
    ext->format_settings = getFormatSettings(ext->context);
    forceFormatSettings(ext->format_settings);
    ext->read_options = readOptions(ext->format_settings);
    for (size_t i = 0; i < def->ncolumns; ++i)
    {
        const auto & c = def->columns[i];
        if (!c.name || !c.type)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_external_open: column {} has no name or type", i);
        ExternalColumn column;
        column.name = c.name;
        column.file_name = c.file_name && *c.file_name ? c.file_name : c.name;
        column.type = typeNamed(c.type);
        column.convert = c.convert;
        column.not_null = c.not_null != 0;
        column.check_utf8 = c.check_utf8 != 0;
        column.read_type = readType(column);
        if ((column.convert == CHTE_CONVERT_DATE && !WhichDataType(column.type).isInt32())
            || (column.convert == CHTE_CONVERT_TIMESTAMP && !WhichDataType(column.type).isInt64())
            || (column.check_utf8 && !WhichDataType(column.type).isString()))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_external_open: column {} of type {} cannot be converted so",
                            column.name, column.type->getName());
        ext->by_name.emplace(column.name, ext->columns.size());
        ext->columns.push_back(std::move(column));
    }
    if (own)
    {
        /// each row's id, which every file of the table holds, never NULL
        if (ext->by_name.contains(ROWID))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_own_open: a column of the caller's is named {}", ROWID);
        ExternalColumn column;
        column.name = column.file_name = ROWID;
        column.type = std::make_shared<DataTypeUInt64>();
        column.not_null = true;
        column.read_type = readType(column);
        ext->by_name.emplace(column.name, ext->columns.size());
        ext->columns.push_back(std::move(column));
    }
    auto t = std::make_unique<chte_table>();
    t->context = ext->context;
    t->external = std::move(ext);
    *table = t.release();
}

}

extern "C" int chte_external_open(const chte_external_def * def, chte_table ** table, chte_error * error)
{
    return guarded(error, [&] { openExternal(def, /* own= */ false, table); });
}

extern "C" int chte_own_open(const chte_external_def * def, chte_table ** table, chte_error * error)
{
    return guarded(error, [&] { openExternal(def, /* own= */ true, table); });
}

extern "C" int chte_external_list(chte_table * table, chte_external_listing ** listing, chte_error * error)
{
    return guarded(error, [&]
    {
        auto & ext = externalOf(table);
        if (ext.own)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "a table of its own files has no location to list");
        if (ext.format == CHTE_EXTERNAL_ICEBERG)
        {
            auto l = std::make_unique<chte_external_listing>();
            l->table = table->external;
            listIceberg(*l);
            *listing = l.release();
            return;
        }
        std::vector<std::tuple<String, UInt64, Int64>> files;
        for (const auto & path : locationFiles(ext.location))
        {
            struct stat st;
            if (stat(path.c_str(), &st) != 0)
                throw ErrnoException(ErrorCodes::CANNOT_STAT, "Cannot stat {}", path);
            auto [size, mtime] = statIdentity(st);
            files.emplace_back(path, size, mtime);
        }
        auto l = std::make_unique<chte_external_listing>();
        l->table = table->external;
        listFiles(*l, files);
        *listing = l.release();
    });
}

extern "C" int chte_external_listing_bytes(chte_external_listing * listing, const void ** bytes, size_t * size, chte_error * error)
{
    return guarded(error, [&]
    {
        if (listing->bytes.empty())
        {
            WriteBufferFromOwnString out;
            writeVarUInt(listing->files.size(), out);
            for (const auto & file : listing->files)
            {
                writeStringBinary(file.path, out);
                writeVarUInt(file.size, out);
                writeVarInt(file.mtime, out);
            }
            if (listing->table->format == CHTE_EXTERNAL_ICEBERG)
                writeIcebergListing(*listing, out);
            out.finalize();
            listing->bytes = std::move(out.str());
        }
        *bytes = listing->bytes.data();
        *size = listing->bytes.size();
    });
}

extern "C" int chte_external_listing_load(chte_table * table, const void * bytes, size_t size, chte_external_listing ** listing,
                                          chte_error * error)
{
    return guarded(error, [&]
    {
        externalOf(table);
        ReadBufferFromMemory in(static_cast<const char *>(bytes), size);
        UInt64 n;
        readVarUInt(n, in);
        std::vector<std::tuple<String, UInt64, Int64>> files;
        for (UInt64 i = 0; i < n; ++i)
        {
            String path;
            UInt64 file_size;
            Int64 mtime;
            readStringBinary(path, in);
            readVarUInt(file_size, in);
            readVarInt(mtime, in);
            files.emplace_back(std::move(path), file_size, mtime);
        }
        auto l = std::make_unique<chte_external_listing>();
        l->table = table->external;
        if (l->table->format == CHTE_EXTERNAL_ICEBERG)
        {
            /// the leader's snapshot: its data files and their deletes, as it listed them
            for (auto & [path, file_size, mtime] : files)
            {
                ListedFile file;
                file.path = std::move(path);
                file.size = file_size;
                file.mtime = mtime;
                l->files.push_back(std::move(file));
            }
            readIcebergListing(*l, in);
        }
        else
            /// each file's footer read, or found kept, for the identity the listing has: one changed since fails
            listFiles(*l, files);
        *listing = l.release();
    });
}

extern "C" void chte_external_listing_free(chte_external_listing * listing)
{
    delete listing;
}

extern "C" size_t chte_external_files(chte_external_listing * listing)
{
    return listing ? listing->files.size() : 0;
}

extern "C" int chte_external_file_info(chte_external_listing * listing, size_t file, chte_external_file * info, chte_error * error)
{
    return guarded(error, [&]
    {
        const auto & f = listedFile(*listing, file);
        *info = {};
        info->path = f.path.c_str();
        info->size = f.size;
        info->mtime = f.mtime;
        info->rows = f.rows;
        info->units = static_cast<uint32_t>(f.units);
        info->first_row = f.first_row;
        info->deletes = icebergDeletes(f) ? 1 : 0;
    });
}

extern "C" int chte_external_schema(chte_table * table, const chte_external_field ** fields, size_t * nfields, chte_error * error)
{
    return guarded(error, [&]
    {
        auto & ext = externalOf(table);
        if (ext.own)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "a table of its own files has no location to infer its columns from");
        ext.schema.clear();
        ext.schema_types.clear();
        ext.fields.clear();
        auto files = ext.format == CHTE_EXTERNAL_ICEBERG ? std::vector<String>{} : locationFiles(ext.location);
        if (ext.format == CHTE_EXTERNAL_ICEBERG)
            ext.schema = icebergSchema(ext);
        else if (!files.empty())
        {
            struct stat st;
            if (stat(files.front().c_str(), &st) != 0)
                throw ErrnoException(ErrorCodes::CANNOT_STAT, "Cannot stat {}", files.front());
            auto [size, mtime] = statIdentity(st);
            ext.schema = fileSchema(ext, *footerOf(ext, files.front(), size, mtime));
        }
        for (const auto & column : ext.schema)
            ext.schema_types.push_back(column.type->getName());
        size_t k = 0;
        for (const auto & column : ext.schema)
            ext.fields.push_back({column.name.c_str(), ext.schema_types[k++].c_str()});
        *fields = ext.fields.data();
        *nfields = ext.fields.size();
    });
}

extern "C" int chte_external_select(chte_table * table, chte_external_listing * listing, size_t file, chte_filter * filter,
                                    uint32_t begin, uint32_t end, chte_mark_range * ranges, size_t max_ranges, size_t * nranges,
                                    uint64_t * rows, chte_error * error)
{
    return guarded(error, [&]
    {
        auto & ext = externalOf(table);
        const auto & f = listedFile(*listing, file);
        if (ext.format == CHTE_EXTERNAL_ICEBERG)
        {
            /// a data file is one unit, which a filter keeps or not by its manifest's statistics, as ClickHouse's
            /// Iceberg pruning does (ManifestFilesPruner); its reader prunes its row groups by their own
            bool kept = begin == 0 && f.rows > 0 && (!filter || filter->nodes.empty() || icebergKeeps(*filter, ext, *listing, f));
            if (kept && max_ranges > 0)
                ranges[0] = chte_mark_range{0, 1};
            *nranges = kept ? 1 : 0;
            *rows = kept ? f.rows : 0;
            return;
        }
        auto resolved = resolvedFor(ext, *listing, f);
        const auto & unit_rows = resolved->footer->unit_rows;
        size_t last = end == 0 ? unit_rows.size() : std::min<size_t>(end, unit_rows.size());
        size_t first = std::min<size_t>(begin, last);
        /// Parquet's row groups by their statistics; Arrow's record batches have none
        const std::vector<uint8_t> * kept = filter && !filter->nodes.empty() && ext.format == CHTE_EXTERNAL_PARQUET
            ? &keptUnits(*filter, ext, f, *resolved) : nullptr;

        size_t n = 0;
        uint64_t total = 0;
        for (size_t u = first; u < last;)
        {
            if ((kept && !(*kept)[u]) || unit_rows[u] == 0)
            {
                ++u;
                continue;
            }
            size_t run = u;
            while (run < last && (!kept || (*kept)[run]) && unit_rows[run] > 0)
                total += unit_rows[run++];
            if (n < max_ranges)
                ranges[n] = chte_mark_range{static_cast<uint32_t>(u), static_cast<uint32_t>(run)};
            ++n;
            u = run;
        }
        *nranges = n;
        *rows = total;
    });
}

extern "C" int chte_external_column_bytes(chte_table * table, chte_external_listing * listing, size_t file, const char * columns,
                                          uint64_t * compressed, uint64_t * uncompressed, chte_error * error)
{
    return guarded(error, [&]
    {
        auto & ext = externalOf(table);
        const auto & f = listedFile(*listing, file);
        auto resolved = resolvedFor(ext, *listing, f);
        *compressed = 0;
        *uncompressed = 0;
        if (ext.format == CHTE_EXTERNAL_ICEBERG)
        {
            /// a data file's bytes shared out among the snapshot's columns, its footer not read for it
            size_t asked = 0;
            for (const auto & name : columnNames(columns))
                asked += name != ROWID;
            *compressed = *uncompressed = f.size * asked / std::max<size_t>(resolved->schema.size(), 1);
            return;
        }
        std::set<String> paths;
        for (const auto & name : columnNames(columns))
            if (name != ROWID)
                paths.insert(resolved->names[ext.columnIndex(name)]);
        auto of_footer = [&](const ExternalFooter & footer, uint64_t * c, uint64_t * u)
        {
            *c = *u = 0;
            if (ext.format == CHTE_EXTERNAL_ARROW)
            {
                arrowColumnBytes(footer, paths.size(), c, u);
                return;
            }
            for (const auto & row_group : footer.parquet->row_groups)
                for (const auto & chunk : row_group.columns)
                {
                    const auto & path = chunk.meta_data.path_in_schema;
                    if (path.empty() || !paths.contains(path.front()))
                        continue;
                    *c += static_cast<uint64_t>(std::max<Int64>(chunk.meta_data.total_compressed_size, 0));
                    *u += static_cast<uint64_t>(std::max<Int64>(chunk.meta_data.total_uncompressed_size, 0));
                }
        };
        /// VC25: a part's from its summary, as from its footer
        if (const auto & summary = resolved->footer->summary)
        {
            if (ext.format == CHTE_EXTERNAL_ARROW)
                *compressed = *uncompressed = summary->arrow_body * paths.size() / std::max<UInt64>(summary->arrow_fields, 1);
            else
                for (const auto & path : paths)
                    if (auto it = summary->columns.find(path); it != summary->columns.end())
                    {
                        *compressed += it->second.compressed;
                        *uncompressed += it->second.uncompressed;
                    }
            if (specCheck())
            {
                CheckingReads checking_reads;
                uint64_t c;
                uint64_t u;
                of_footer(*fullFooter(ext, f), &c, &u);
                if (c != *compressed || u != *uncompressed)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "file {}: its summary's bytes of columns {} are not its footer's",
                                    f.path, columns);
            }
            return;
        }
        of_footer(*resolved->footer, compressed, uncompressed);
    });
}

extern "C" int chte_external_stats(chte_table * table, chte_external_listing * listing, size_t file, const char * column,
                                   chte_column_stats * stats, chte_error * error)
{
    return guarded(error, [&]
    {
        auto & ext = externalOf(table);
        const auto & f = listedFile(*listing, file);
        *stats = {};
        /// Parquet's from its footer, an Iceberg data file's from its own where nothing deletes its rows; an Arrow IPC
        /// file keeps none
        if (ext.format == CHTE_EXTERNAL_PARQUET)
            parquetStats(*table, ext, f, *resolvedFor(ext, *listing, f), column ? column : "", stats);
        else if (ext.format == CHTE_EXTERNAL_ICEBERG)
            icebergStats(*table, ext, *listing, f, column ? column : "", stats);
    });
}

extern "C" int chte_external_read_begin(chte_table * table, chte_external_listing * listing, size_t file, chte_filter * filter,
                                        const char * columns, const chte_mark_range * ranges, size_t nranges,
                                        chte_reader ** reader, chte_error * error)
{
    return guarded(error, [&]
    {
        externalOf(table);
        const auto & f = listedFile(*listing, file);
        std::optional<std::vector<size_t>> units;
        if (nranges > 0)
        {
            size_t count = f.units;
            units.emplace();
            for (size_t i = 0; i < nranges; ++i)
            {
                if (ranges[i].begin >= ranges[i].end || ranges[i].end > count || (i > 0 && ranges[i].begin < ranges[i - 1].end))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_external_read_begin: units [{}, {}) are not a range of {} in order",
                                    ranges[i].begin, ranges[i].end, f.path);
                for (size_t u = ranges[i].begin; u < ranges[i].end; ++u)
                    units->push_back(u);
            }
        }
        *reader = openExternalReader(*table, *listing, file, filter, columnNames(columns), std::move(units), nullptr).release();
    });
}

extern "C" int chte_external_read_rows(chte_table * table, chte_external_listing * listing, size_t file, const char * columns,
                                       const uint64_t * offsets, size_t n, chte_reader ** reader, chte_error * error)
{
    return guarded(error, [&]
    {
        externalOf(table);
        const auto & f = listedFile(*listing, file);
        auto wanted = std::make_shared<PaddedPODArray<UInt64>>();
        for (size_t i = 0; i < n; ++i)
        {
            if (offsets[i] >= f.rows || (i > 0 && offsets[i] <= offsets[i - 1]))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_external_read_rows: row {} is not one of {} in order", offsets[i], f.path);
            wanted->push_back(offsets[i]);
        }
        *reader = openExternalReader(*table, *listing, file, nullptr, columnNames(columns), std::nullopt, std::move(wanted)).release();
    });
}

namespace
{

/// The output formats the writer may name, registered as it first needs them
void registerOutputFormats()
{
    static bool done = false;
    if (done)
        return;
    registerOutputFormatParquet(FormatFactory::instance());
    done = true;
}

}

namespace
{

/// A writer of a file, its blocks written in slices of slice_rows (0: whole)
std::unique_ptr<chte_file_writer> fileWriteBegin(const char * format, const char * path, const chte_file_column * columns,
                                                 size_t ncolumns, const char * settings, size_t slice_rows)
{
    auto & e = engine();
    if (!format || !path || *path != '/')
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_file_write_begin: a format and an absolute path are required");
    String name = format;
    if (name == "Parquet")
        registerOutputFormats();
    else if (name == "Arrow")
        registerArrowOutput();
    else
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "files of format {} are not written", name);
    auto w = std::make_unique<chte_file_writer>();
    w->path = path;
    w->temp_path = fmt::format("{}.tmp{}", path, getpid());
    w->slice_rows = slice_rows;
    w->context = Context::createCopy(e.global);
    applySettings(w->context, settings);
    applySingleThreadedSettings(w->context);
    auto format_settings = getFormatSettings(w->context);
    forceFormatSettings(format_settings);
    for (size_t i = 0; i < ncolumns; ++i)
    {
        const auto & c = columns[i];
        if (!c.name || !c.type || !c.source)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_file_write_begin: column {} has no name, type or source", i);
        auto type = DataTypeFactory::instance().get(c.type);
        w->types.push_back(type);
        w->sources.push_back(makeNullable(DataTypeFactory::instance().get(c.source)));
        w->converts.push_back(c.convert);
        w->header.insert({type->createColumn(), type, c.name});
    }
    w->out = std::make_unique<WriteBufferFromFile>(w->temp_path);
    w->format = FormatFactory::instance().getOutputFormat(name, *w->out, w->header, w->context, format_settings);
    return w;
}

/// A column of the caller's values -- a batch's vector of its source type, or a reader's of a table's type -- as
/// the file's column c holds them: days and microseconds moved to 1970's epoch, cast to the file's type
ColumnWithTypeAndName fileColumn(const chte_file_writer & writer, size_t c, ColumnWithTypeAndName value)
{
    if (writer.converts[c] == CHTE_CONVERT_DATE)
    {
        auto days = castColumn(value, makeNullable(std::make_shared<DataTypeInt32>()));
        auto mutable_days = IColumn::mutate(std::move(days));
        for (auto & v : assert_cast<ColumnInt32 &>(assert_cast<ColumnNullable &>(*mutable_days).getNestedColumn()).getData())
            v += PG_EPOCH_DAYS;
        value = {std::move(mutable_days), makeNullable(std::make_shared<DataTypeDate32>()), ""};
    }
    else if (writer.converts[c] == CHTE_CONVERT_TIMESTAMP)
    {
        auto micros = castColumn(value, makeNullable(std::make_shared<DataTypeInt64>()));
        const auto & nullable = assert_cast<const ColumnNullable &>(*micros);
        auto timestamps = ColumnDecimal<DateTime64>::create(0, 6);
        for (auto v : assert_cast<const ColumnInt64 &>(nullable.getNestedColumn()).getData())
            timestamps->insertValue(DateTime64(v + PG_EPOCH_MICROSECONDS));
        value = {ColumnNullable::create(std::move(timestamps), nullable.getNullMapColumnPtr()),
                 makeNullable(std::make_shared<DataTypeDateTime64>(6, "UTC")), ""};
    }
    return {castColumn(value, writer.types[c]), writer.types[c], writer.header.getByPosition(c).name};
}

/// The rows of a slice gathered so far written, a block of the file's columns
void writePending(chte_file_writer & writer)
{
    if (writer.pending.empty() || writer.pending[0]->empty())
        return;
    ColumnsWithTypeAndName piece;
    for (size_t c = 0; c < writer.pending.size(); ++c)
    {
        const auto & column = writer.header.getByPosition(c);
        piece.emplace_back(std::move(writer.pending[c]), column.type, column.name);
        writer.pending[c] = column.type->createColumn();
    }
    writer.format->write(Block(std::move(piece)));
}

/// A block of the file's columns written: to an Iceberg table's writer, or the file's format -- where the writer has
/// slices, gathered into slices of exactly slice_rows, so that a format that gathers its rows to a target of a
/// multiple of them cuts its units at that target, whatever blocks come
void writeFileBlock(chte_file_writer & writer, Block block)
{
    if (writer.iceberg)
    {
        icebergWrite(*writer.iceberg, std::move(block));
        return;
    }
    if (writer.slice_rows == 0)
    {
        writer.format->write(block);
        return;
    }
    if (writer.pending.empty())
        for (const auto & column : writer.header)
            writer.pending.push_back(column.type->createColumn());
    size_t rows = block.rows();
    for (size_t at = 0; at < rows;)
    {
        size_t n = std::min(writer.slice_rows - writer.pending[0]->size(), rows - at);
        for (size_t c = 0; c < writer.pending.size(); ++c)
            writer.pending[c]->insertRangeFrom(*block.getByPosition(c).column, at, n);
        at += n;
        if (writer.pending[0]->size() == writer.slice_rows)
            writePending(writer);
    }
}

/// The file finished, synced and renamed to its path: its bytes
UInt64 fileWriteFinish(chte_file_writer & writer)
{
    writePending(writer);
    writer.format->finalize();
    writer.out->finalize();
    writer.out->sync();
    writer.format.reset();
    writer.out.reset();
    fs::rename(writer.temp_path, writer.path);
    writer.finished = true;
    return fs::file_size(writer.path);
}

}

extern "C" int chte_file_write_begin(const char * format, const char * path, const chte_file_column * columns, size_t ncolumns,
                                     const char * settings, chte_file_writer ** writer, chte_error * error)
{
    return guarded(error, [&] { *writer = fileWriteBegin(format, path, columns, ncolumns, settings, 0).release(); });
}

/// A part's file of a table of its own files (§2.18): its blocks sliced, so that the format's writer gathers its rows
/// to its targets -- Parquet's row groups, Arrow's record batches -- and cuts them there
constexpr size_t OWN_SLICE_ROWS = 65536;

extern "C" int chte_own_write_begin(const char * format, const char * path, const chte_file_column * columns, size_t ncolumns,
                                    const char * settings, chte_file_writer ** writer, chte_error * error)
{
    return guarded(error, [&] { *writer = fileWriteBegin(format, path, columns, ncolumns, settings, OWN_SLICE_ROWS).release(); });
}

extern "C" int chte_iceberg_write_begin(const char * location, int action, const char * spec, const chte_file_column * columns,
                                        size_t ncolumns, const char * settings, chte_file_writer ** writer, chte_error * error)
{
    return guarded(error, [&]
    {
        auto & e = engine();
        if (!location || *location != '/')
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_iceberg_write_begin: an absolute location is required");
        registerOutputFormats();
        auto w = std::make_unique<chte_file_writer>();
        w->path = location;
        w->context = Context::createCopy(e.global);
        applySettings(w->context, settings);
        applySingleThreadedSettings(w->context);
        w->context->setSetting("use_iceberg_metadata_files_cache", Field(false));
        auto format_settings = getFormatSettings(w->context);
        forceFormatSettings(format_settings);
        w->iceberg = icebergWriteBegin(location, action, spec ? spec : "", columns, ncolumns, w->context, format_settings, w->header,
                                       w->sources, w->converts, w->vectors);
        for (const auto & column : w->header)
            w->types.push_back(column.type);
        *writer = w.release();
    });
}

extern "C" int chte_iceberg_alter(const char * location, const char * command, const char * settings, chte_error * error)
{
    return guarded(error, [&]
    {
        auto & e = engine();
        if (!location || *location != '/' || !command)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_iceberg_alter: an absolute location and a command are required");
        auto context = Context::createCopy(e.global);
        applySettings(context, settings);
        applySingleThreadedSettings(context);
        context->setSetting("use_iceberg_metadata_files_cache", Field(false));
        icebergAlter(location, command, context);
    });
}

extern "C" int chte_file_write_batch(chte_file_writer * writer, uint32_t nrows, const chte_vector * columns, chte_error * error)
{
    return guarded(error, [&]
    {
        if (writer->finished)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "chte_file_write_batch: the file is finished");
        ColumnsWithTypeAndName out;
        for (size_t c = 0; c < writer->types.size(); ++c)
        {
            auto source = writer->sources[c]->createColumn();
            appendVector(*source, nrows, columns[writer->vectors.empty() ? c : writer->vectors[c]]);
            out.push_back(fileColumn(*writer, c, {std::move(source), writer->sources[c], ""}));
        }
        writeFileBlock(*writer, Block(std::move(out)));
    });
}

extern "C" int chte_file_write_finish(chte_file_writer * writer, uint64_t * bytes, chte_error * error)
{
    return guarded(error, [&]
    {
        if (writer->finished)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "chte_file_write_finish: the file is finished");
        if (writer->iceberg)
        {
            *bytes = icebergWriteFinish(*writer->iceberg);
            writer->finished = true;
            return;
        }
        *bytes = fileWriteFinish(*writer);
    });
}

extern "C" void chte_file_write_close(chte_file_writer * writer)
{
    if (!writer)
        return;
    try
    {
        if (writer->iceberg && !writer->finished)
            icebergWriteCancel(*writer->iceberg);
        writer->iceberg.reset();
        if (writer->format)
            writer->format->cancel();
    }
    catch (...)
    {
    }
    try
    {
        writer->format.reset();
        if (writer->out)
        {
            writer->out->cancel();
            writer->out.reset();
        }
        if (!writer->finished && !writer->temp_path.empty())
        {
            std::error_code ec;
            fs::remove(writer->temp_path, ec);
        }
    }
    catch (...)
    {
    }
    delete writer;
}

/*
 * Tables of their own files (ch_table_engines.md §2.18, CH8): a part's one file, its rows in their ids' order, each
 * id in its _pg_rowid.  The external readers read them (ExternalTable::own); a row is found by its id in the file's
 * ids, read once for its identity; a merge reads its parts' files through the same readers and writes one.
 */

namespace chte
{

namespace
{

/// The files' row ids a table keeps, beyond which it forgets them all
constexpr size_t MAX_ROW_ID_FILES = 256;

ExternalTable & ownOf(chte_table * table)
{
    auto & ext = externalOf(table);
    if (!ext.own)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "not a table of its own files");
    return ext;
}

/// A listed file's row ids, as its _pg_rowid holds them, checked ascending
std::shared_ptr<const OwnRowIds> ownRowIds(chte_table & table, const chte_external_listing & listing, size_t file)
{
    auto & ext = *table.external;
    const auto & listed = listedFile(listing, file);
    auto it = ext.row_ids.find(listed.path);
    if (it != ext.row_ids.end() && it->second->size == listed.size && it->second->mtime == listed.mtime)
        return it->second;

    auto ids = std::make_shared<OwnRowIds>();
    ids->size = listed.size;
    ids->mtime = listed.mtime;
    UInt64 first = 0;
    for (UInt64 rows : listed.resolved->footer->unit_rows)
    {
        ids->unit_first.push_back(first);
        first += rows;
    }
    ids->ids.reserve(first);
    auto reader = openExternalReader(table, listing, file, nullptr, {ROWID}, std::nullopt, nullptr);
    Block block;
    while (reader->next_block(block))
    {
        const auto & data = assert_cast<const ColumnUInt64 &>(*block.getByPosition(0).column).getData();
        ids->ids.insert(data.begin(), data.end());
    }
    if (ids->ids.size() != first)
        throw Exception(ErrorCodes::INCORRECT_DATA, "file {}: {} row ids read of {} rows", listed.path, ids->ids.size(), first);
    for (size_t i = 1; i < ids->ids.size(); ++i)
        if (ids->ids[i] <= ids->ids[i - 1])
            throw Exception(ErrorCodes::INCORRECT_DATA, "file {}: row id {} after {}, not ascending", listed.path, ids->ids[i],
                            ids->ids[i - 1]);
    if (ext.row_ids.size() >= MAX_ROW_ID_FILES)
        ext.row_ids.clear();
    ext.row_ids[listed.path] = ids;
    return ids;
}

/// A Parquet column chunk's codec, by its number in parquet::format::CompressionCodec
const char * parquetCodecName(int codec)
{
    static const char * const names[] = {"NONE", "SNAPPY", "GZIP", "LZO", "BROTLI", "LZ4", "ZSTD", "LZ4_RAW"};
    return codec >= 0 && codec < static_cast<int>(std::size(names)) ? names[codec] : "UNKNOWN";
}

/// A column as a merge holds it: Nullable where the table's column may hold NULLs
DataTypePtr mergeType(const ExternalColumn & column)
{
    return column.not_null ? column.type : makeNullable(column.type);
}

ColumnPtr asMergeColumn(const ExternalColumn & column, ColumnPtr col)
{
    if (column.not_null || col->isNullable())
        return col;
    return makeNullable(col);
}

/*
 * VC25: a part's summary, which the caller keeps in its catalog row (chte_own_summary) and lists the part by
 * (chte_own_list_summaries), its file not opened to plan: a format byte -- the mergetree summaries' is 1 -- the
 * file's format and size, its units' rows, its schema as the readers infer it, an Arrow file's batches' bodies and
 * fields, and per column of the table the file has, by the file's name of it: the type it is read as, its bytes,
 * its file's statistics and each unit's range of its values, a Parquet file's, as parquetStats() and the reader's
 * pruning read them.
 */
constexpr UInt8 OWN_SUMMARY_FORMAT = 2;

/// A range's end: a value, or one of the infinities a Range takes NULL as, which readFieldBinary() loses
void writeRangeEnd(const Field & value, bool included, WriteBuffer & out)
{
    UInt8 kind = value.isNegativeInfinity() ? 1 : value.isPositiveInfinity() ? 2 : value.isNull() ? 3 : 0;
    writeBinary(kind, out);
    if (kind == 0)
        writeFieldBinary(value, out);
    writeBinary(UInt8(included ? 1 : 0), out);
}

void readRangeEnd(ReadBuffer & in, Field & value, bool & included)
{
    UInt8 kind;
    readBinary(kind, in);
    switch (kind)
    {
        case 0: value = readFieldBinary(in); break;
        case 1: value = NEGATIVE_INFINITY; break;
        case 2: value = POSITIVE_INFINITY; break;
        case 3: value = Field(); break;
        default: throw Exception(ErrorCodes::INCORRECT_DATA, "a summary's range end of kind {}", static_cast<int>(kind));
    }
    UInt8 flag;
    readBinary(flag, in);
    included = flag != 0;
}

/// Each unit's range of each column of sample, as Parquet::Reader::getHyperrectangleForRowGroup() and
/// adjustRangeFromIndexIfNeeded() at the tag make it of the row group's statistics for the reader's pruning -- every
/// column as one a filter uses -- none where the reader takes none: no statistics, a type whose statistics it does not
/// decode, a column of several primitive columns, or statistics it would refuse
std::vector<std::vector<std::optional<Range>>> parquetRanges(const ExternalTable & ext, const parquet::format::FileMetaData & metadata,
                                                              const Block & sample)
{
    std::vector<std::vector<std::optional<Range>>> out(sample.columns(),
                                                        std::vector<std::optional<Range>>(metadata.row_groups.size()));
    Parquet::SchemaConverter schemer(metadata, ext.read_options, &sample);
    schemer.prepareForReading();
    std::vector<size_t> primitives(sample.columns(), 0);
    for (const auto & column_info : schemer.primitive_columns)
        if (column_info.idx_in_output_block < sample.columns())
            ++primitives[column_info.idx_in_output_block];
    for (const auto & column_info : schemer.primitive_columns)
    {
        size_t idx = column_info.idx_in_output_block;
        if (idx >= sample.columns() || primitives[idx] != 1 || !column_info.decoder.allow_stats)
            continue;
        const IDataType & output_block_type = *sample.getByPosition(idx).type;
        bool nullable = column_info.levels.back().def > 0;
        bool null_as_default = ext.read_options.format.null_as_default && !column_info.output_nullable;
        for (size_t u = 0; u < metadata.row_groups.size(); ++u)
        {
            const auto & column_meta = metadata.row_groups[u].columns.at(column_info.column_idx).meta_data;
            if (!column_meta.__isset.statistics)
                continue;
            Range range = Range::createWholeUniverse();
            bool always_null = column_meta.statistics.__isset.null_count && column_meta.statistics.null_count == column_meta.num_values;
            bool can_be_null = !column_meta.statistics.__isset.null_count || column_meta.statistics.null_count != 0;
            if (nullable && always_null)
            {
                if (null_as_default)
                    range.right = range.left = output_block_type.getDefault();
                else
                    range.right = range.left;
                out[idx][u] = range;
                continue;
            }
            try
            {
                if (column_meta.statistics.__isset.min_value)
                    column_info.decoder.decodeField(column_meta.statistics.min_value, /* is_max= */ false, *column_info.decoded_type,
                                                    output_block_type, range.left);
                if (column_meta.statistics.__isset.max_value)
                    column_info.decoder.decodeField(column_meta.statistics.max_value, /* is_max= */ true, *column_info.decoded_type,
                                                    output_block_type, range.right);
            }
            catch (const Exception &)
            {
                continue;
            }
            if (accurateLess(range.right, range.left))
                continue;
            if (nullable && can_be_null)
            {
                if (null_as_default)
                {
                    Field default_value = output_block_type.getDefault();
                    if (!range.left.isNull() && accurateLess(default_value, range.left))
                        range.left = default_value;
                    if (!range.right.isNull() && accurateLess(range.right, default_value))
                        range.right = default_value;
                }
                else if (!range.left.isNull() && !range.right.isNull())
                    range.left = NEGATIVE_INFINITY;
            }
            else
            {
                if (range.left.isNull())
                    range.left_included = false;
                if (range.right.isNull())
                    range.right_included = false;
            }
            out[idx][u] = range;
        }
    }
    return out;
}

/// A listed file's summary, of its footer as read from the file
String ownSummaryBytes(const ExternalTable & ext, const ListedFile & listed)
{
    auto footer = fullFooter(ext, listed);
    const auto & resolved = *listed.resolved;
    WriteBufferFromOwnString out;
    writeBinary(OWN_SUMMARY_FORMAT, out);
    writeBinary(static_cast<UInt8>(ext.format), out);
    writeVarUInt(listed.size, out);
    writeVarUInt(footer->unit_rows.size(), out);
    for (UInt64 rows : footer->unit_rows)
        writeVarUInt(rows, out);
    writeVarUInt(resolved.schema.size(), out);
    for (const auto & column : resolved.schema)
    {
        writeStringBinary(column.name, out);
        writeStringBinary(column.type->getName(), out);
    }
    UInt64 body = 0;
    UInt64 fields = 0;
    if (footer->arrow)
    {
        for (const auto & block : footer->arrow->record_batch_blocks)
            body += static_cast<UInt64>(std::max<Int64>(block.body_length, 0));
        fields = footer->arrow->schema.fields.size();
    }
    writeVarUInt(body, out);
    writeVarUInt(fields, out);

    std::vector<size_t> present;
    Names names;
    for (size_t i = 0; i < ext.columns.size(); ++i)
        if (!resolved.isAbsent(i))
        {
            present.push_back(i);
            names.push_back(ext.columns[i].name);
        }
    std::vector<std::vector<std::optional<Range>>> ranges;
    if (footer->parquet)
        ranges = parquetRanges(ext, *footer->parquet, fileHeader(ext, resolved, names));
    writeVarUInt(present.size(), out);
    for (size_t k = 0; k < present.size(); ++k)
    {
        size_t i = present[k];
        const String & file_name = resolved.names[i];
        writeStringBinary(file_name, out);
        writeStringBinary(fileReadType(ext, resolved, i)->getName(), out);
        UInt64 compressed = 0;
        UInt64 uncompressed = 0;
        FileColumnStats s;
        if (footer->parquet)
        {
            for (const auto & row_group : footer->parquet->row_groups)
                for (const auto & chunk : row_group.columns)
                {
                    const auto & path = chunk.meta_data.path_in_schema;
                    if (path.empty() || path.front() != file_name)
                        continue;
                    compressed += static_cast<uint64_t>(std::max<Int64>(chunk.meta_data.total_compressed_size, 0));
                    uncompressed += static_cast<uint64_t>(std::max<Int64>(chunk.meta_data.total_uncompressed_size, 0));
                }
            s = parquetFileStats(ext, resolved, *footer->parquet, i);
        }
        writeVarUInt(compressed, out);
        writeVarUInt(uncompressed, out);
        writeBinary(UInt8((s.counted ? 1 : 0) | (s.bounded ? 2 : 0)), out);
        if (s.counted)
        {
            writeVarUInt(s.rows, out);
            writeVarUInt(s.nulls, out);
        }
        if (s.bounded)
        {
            writeFieldBinary(s.min, out);
            writeFieldBinary(s.max, out);
        }
        size_t n = 0;
        if (!ranges.empty())
            for (const auto & range : ranges[k])
                n += range.has_value();
        writeVarUInt(n, out);
        for (size_t u = 0; n > 0 && u < ranges[k].size(); ++u)
            if (const auto & range = ranges[k][u])
            {
                writeVarUInt(u, out);
                writeRangeEnd(range->left, range->left_included, out);
                writeRangeEnd(range->right, range->right_included, out);
            }
    }
    out.finalize();
    return std::move(out.str());
}

/// A summary's bytes read; none where they are of another format, or do not read
std::shared_ptr<const OwnSummary> parseOwnSummary(const void * data, size_t size)
{
    if (!data || size == 0)
        return nullptr;
    try
    {
        ReadBufferFromMemory in(static_cast<const char *>(data), size);
        UInt8 byte;
        readBinary(byte, in);
        if (byte != OWN_SUMMARY_FORMAT)
            return nullptr;
        auto s = std::make_shared<OwnSummary>();
        readBinary(byte, in);
        s->format = byte;
        readVarUInt(s->file_size, in);
        UInt64 n;
        readVarUInt(n, in);
        s->unit_rows.resize(n);
        for (auto & rows : s->unit_rows)
            readVarUInt(rows, in);
        readVarUInt(n, in);
        for (UInt64 k = 0; k < n; ++k)
        {
            String name;
            String type;
            readStringBinary(name, in);
            readStringBinary(type, in);
            s->schema.emplace_back(name, typeNamed(type));
        }
        readVarUInt(s->arrow_body, in);
        readVarUInt(s->arrow_fields, in);
        readVarUInt(n, in);
        for (UInt64 k = 0; k < n; ++k)
        {
            String name;
            readStringBinary(name, in);
            auto & c = s->columns[name];
            readStringBinary(c.type, in);
            readVarUInt(c.compressed, in);
            readVarUInt(c.uncompressed, in);
            readBinary(byte, in);
            c.counted = (byte & 1) != 0;
            c.bounded = (byte & 2) != 0;
            if (c.counted)
            {
                readVarUInt(c.rows, in);
                readVarUInt(c.nulls, in);
            }
            if (c.bounded)
            {
                c.min = readFieldBinary(in);
                c.max = readFieldBinary(in);
            }
            UInt64 nranges;
            readVarUInt(nranges, in);
            if (nranges > 0)
                c.ranges.resize(s->unit_rows.size());
            for (UInt64 r = 0; r < nranges; ++r)
            {
                UInt64 u;
                readVarUInt(u, in);
                Range range = Range::createWholeUniverse();
                Field left;
                Field right;
                readRangeEnd(in, left, range.left_included);
                readRangeEnd(in, right, range.right_included);
                range.left = left;
                range.right = right;
                if (u < c.ranges.size())
                    c.ranges[u] = std::move(range);
            }
        }
        return s;
    }
    catch (...)
    {
        return nullptr;
    }
}

/// A part's summary read once a table: by its file's path, which its part's name makes unique
std::shared_ptr<const OwnSummary> summaryOf(ExternalTable & table, const String & path, const void * data, size_t size)
{
    auto it = table.summaries.find(path);
    if (it != table.summaries.end() && it->second.first == size)
        return it->second.second;
    auto summary = parseOwnSummary(data, size);
    if (table.summaries.size() >= MAX_FOOTERS)
        table.summaries.clear();
    table.summaries[path] = {size, summary};
    return summary;
}

/// A file as the table reads it, from its part's summary: its schema the summary's, its footer read where a reader needs it
std::shared_ptr<const ResolvedFile> resolveSummary(ExternalTable & table, const String & path, UInt64 size, Int64 mtime,
                                                   std::shared_ptr<const OwnSummary> summary)
{
    auto it = table.resolved.find(path);
    if (it != table.resolved.end() && it->second->footer->size == size && it->second->footer->mtime == mtime)
        return it->second;
    auto footer = std::make_shared<ExternalFooter>();
    footer->size = size;
    footer->mtime = mtime;
    footer->unit_rows = summary->unit_rows;
    for (UInt64 rows : footer->unit_rows)
        footer->rows += rows;
    auto file = std::make_shared<ResolvedFile>();
    file->schema = summary->schema;
    footer->summary = std::move(summary);
    file->footer = std::move(footer);
    resolveColumns(table, *file, "file " + path);
    table.resolved[path] = file;
    return file;
}

}

}

extern "C" int chte_own_list(chte_table * table, const char * const * paths, size_t npaths, chte_external_listing ** listing,
                             chte_error * error)
{
    return guarded(error, [&]
    {
        ownOf(table);
        std::vector<std::tuple<String, UInt64, Int64>> files;
        for (size_t i = 0; i < npaths; ++i)
        {
            struct stat st;
            if (stat(paths[i], &st) != 0)
                throw ErrnoException(ErrorCodes::CANNOT_STAT, "Cannot stat {}", paths[i]);
            auto [size, mtime] = statIdentity(st);
            files.emplace_back(paths[i], size, mtime);
        }
        auto l = std::make_unique<chte_external_listing>();
        l->table = table->external;
        listFiles(*l, files);
        *listing = l.release();
    });
}

extern "C" int chte_own_list_summaries(chte_table * table, const char * const * paths, const void * const * summaries,
                                       const size_t * sizes, size_t npaths, chte_external_listing ** listing, chte_error * error)
{
    return guarded(error, [&]
    {
        auto & ext = ownOf(table);
        auto l = std::make_unique<chte_external_listing>();
        l->table = table->external;
        UInt64 first_row = 0;
        for (size_t i = 0; i < npaths; ++i)
        {
            struct stat st;
            if (stat(paths[i], &st) != 0)
                throw ErrnoException(ErrorCodes::CANNOT_STAT, "Cannot stat {}", paths[i]);
            auto [size, mtime] = statIdentity(st);
            ListedFile file;
            file.path = paths[i];
            file.size = size;
            file.mtime = mtime;
            file.first_row = first_row;
            /// a summary of another file's, or unreadable, as none: the footer read
            auto summary = summaries && summaries[i] ? summaryOf(ext, file.path, summaries[i], sizes[i]) : nullptr;
            if (summary && summary->file_size == size && summary->format == ext.format)
            {
                if (specCheck())
                {
                    CheckingReads checking_reads;
                    auto footer = footerOf(ext, file.path, size, mtime);
                    auto schema = fileSchema(ext, *footer);
                    bool same = footer->unit_rows == summary->unit_rows && schema.size() == summary->schema.size();
                    auto b = summary->schema.begin();
                    for (auto a = schema.begin(); same && a != schema.end(); ++a, ++b)
                        same = a->name == b->name && a->type->getName() == b->type->getName();
                    if (!same)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "file {}: its summary's units or columns are not its footer's",
                                        file.path);
                }
                file.resolved = resolveSummary(ext, file.path, size, mtime, std::move(summary));
            }
            else
                file.resolved = resolve(ext, file.path, size, mtime);
            file.rows = file.resolved->footer->rows;
            file.units = file.resolved->footer->unit_rows.size();
            first_row += file.rows;
            l->files.push_back(std::move(file));
        }
        *listing = l.release();
    });
}

extern "C" uint64_t chte_footers_read(void)
{
    return footers_read.load(std::memory_order_relaxed);
}

extern "C" int chte_own_summary(chte_table * table, chte_external_listing * listing, size_t file, const void ** bytes,
                                size_t * size, chte_error * error)
{
    return guarded(error, [&]
    {
        auto & ext = ownOf(table);
        ext.summary_bytes = ownSummaryBytes(ext, listedFile(*listing, file));
        *bytes = ext.summary_bytes.data();
        *size = ext.summary_bytes.size();
    });
}

extern "C" int chte_own_locate(chte_table * table, chte_external_listing * listing, size_t file, const uint64_t * ids, size_t n,
                               uint64_t * offsets, chte_error * error)
{
    return guarded(error, [&]
    {
        ownOf(table);
        auto row_ids = ownRowIds(*table, *listing, file);
        const auto & data = row_ids->ids;
        for (size_t i = 0; i < n; ++i)
        {
            const auto * at = std::lower_bound(data.begin(), data.end(), ids[i]);
            offsets[i] = at != data.end() && *at == ids[i] ? static_cast<uint64_t>(at - data.begin()) : UINT64_MAX;
        }
    });
}

extern "C" int chte_own_unit_ids(chte_table * table, chte_external_listing * listing, size_t file, const int32_t * units,
                                 size_t nunits, const uint64_t ** ids, size_t * n, chte_error * error)
{
    return guarded(error, [&]
    {
        ownOf(table);
        auto row_ids = ownRowIds(*table, *listing, file);
        const auto & unit_rows = listedFile(*listing, file).resolved->footer->unit_rows;
        auto out = ColumnUInt64::create();
        auto & data = out->getData();
        for (size_t i = 0; i < nunits; ++i)
        {
            if (units[i] < 0 || static_cast<size_t>(units[i]) >= unit_rows.size())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_own_unit_ids: a file of {} units has no unit {}", unit_rows.size(),
                                units[i]);
            const auto * begin = row_ids->ids.begin() + row_ids->unit_first[units[i]];
            data.insert(begin, begin + unit_rows[units[i]]);
        }
        *ids = data.data();
        *n = data.size();
        table->scratch.clear();
        table->scratch.push_back(std::move(out));
    });
}

extern "C" int chte_own_merge(chte_table * table, chte_external_listing * listing, const size_t * files, size_t nfiles,
                              const uint64_t * dead, size_t ndead, uint8_t * dropped, const char * format, const char * path,
                              const chte_file_column * columns, size_t ncolumns, const chte_vector * missing,
                              const char * settings, uint64_t * rows, uint64_t * bytes, chte_error * error)
{
    std::unique_ptr<chte_file_writer> writer;
    int rc = guarded(error, [&]
    {
        auto & ext = ownOf(table);
        writer = fileWriteBegin(format, path, columns, ncolumns, settings, OWN_SLICE_ROWS);
        Names names;
        std::vector<const ExternalColumn *> table_columns;
        size_t rowid_at = SIZE_MAX;
        for (size_t c = 0; c < ncolumns; ++c)
        {
            names.push_back(columns[c].name);
            table_columns.push_back(&ext.column(columns[c].name));
            if (names.back() == ROWID)
                rowid_at = c;
        }
        if (rowid_at == SIZE_MAX)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_own_merge: no column {} among the columns", ROWID);
        if (dropped && ndead > 0)
            memset(dropped, 0, ndead);

        /// each input's block, its rows but the dead ones, and the place of its next row
        struct Input
        {
            std::unique_ptr<chte_reader> reader;
            Columns block;
            const PaddedPODArray<UInt64> * ids = nullptr;
            size_t pos = 0;
            bool done = false;
        };
        std::vector<Input> inputs(nfiles);
        auto pull = [&](Input & in)
        {
            in.pos = 0;
            in.ids = nullptr;
            Block block;
            while (in.reader->next_block(block))
            {
                size_t n = block.rows();
                if (n == 0)
                    continue;
                Columns cols;
                for (size_t c = 0; c < ncolumns; ++c)
                {
                    if (!in.reader->present[c])
                    {
                        /// a column the file lacks: its missing value, every row
                        if (!missing)
                            throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_own_merge: a file lacks column {}, and no missing value is given",
                                            names[c]);
                        auto one = makeNullable(table_columns[c]->type)->createColumn();
                        appendVector(*one, 1, missing[c]);
                        ColumnPtr value = std::move(one);
                        if (table_columns[c]->not_null)
                            value = assert_cast<const ColumnNullable &>(*value).getNestedColumnPtr();
                        cols.push_back(ColumnConst::create(value, n)->convertToFullColumnIfConst());
                        continue;
                    }
                    cols.push_back(asMergeColumn(*table_columns[c], block.getByPosition(c).column));
                }
                if (ndead > 0)
                {
                    const auto & ids = assert_cast<const ColumnUInt64 &>(*cols[rowid_at]).getData();
                    IColumn::Filter keep(n, 1);
                    size_t kept = n;
                    for (size_t i = 0; i < n; ++i)
                    {
                        const auto * d = std::lower_bound(dead, dead + ndead, ids[i]);
                        if (d != dead + ndead && *d == ids[i])
                        {
                            keep[i] = 0;
                            --kept;
                            if (dropped)
                                dropped[d - dead] = 1;
                        }
                    }
                    if (kept == 0)
                        continue;
                    if (kept != n)
                        for (auto & col : cols)
                            col = col->filter(keep, static_cast<ssize_t>(kept));
                }
                in.block = std::move(cols);
                in.ids = &assert_cast<const ColumnUInt64 &>(*in.block[rowid_at]).getData();
                return;
            }
            in.done = true;
        };
        for (size_t i = 0; i < nfiles; ++i)
        {
            inputs[i].reader = openExternalReader(*table, *listing, files[i], nullptr, names, std::nullopt, nullptr);
            pull(inputs[i]);
        }

        MutableColumns out;
        for (size_t c = 0; c < ncolumns; ++c)
            out.push_back(mergeType(*table_columns[c])->createColumn());
        UInt64 written = 0;
        auto flush = [&]
        {
            if (out[0]->empty())
                return;
            ColumnsWithTypeAndName block;
            for (size_t c = 0; c < ncolumns; ++c)
            {
                ColumnPtr col = std::move(out[c]);
                block.push_back(fileColumn(*writer, c, {col, mergeType(*table_columns[c]), names[c]}));
                out[c] = mergeType(*table_columns[c])->createColumn();
            }
            written += block.front().column->size();
            writeFileBlock(*writer, Block(std::move(block)));
        };

        /// the inputs' runs in their ids' order: the input of the least next id, its rows below every other's next
        for (;;)
        {
            checkInterrupted();
            size_t best = SIZE_MAX;
            UInt64 best_id = 0;
            UInt64 next_other = UINT64_MAX;
            for (size_t i = 0; i < nfiles; ++i)
            {
                if (inputs[i].done)
                    continue;
                UInt64 id = (*inputs[i].ids)[inputs[i].pos];
                if (best == SIZE_MAX || id < best_id)
                {
                    if (best != SIZE_MAX)
                        next_other = std::min(next_other, best_id);
                    best = i;
                    best_id = id;
                }
                else
                    next_other = std::min(next_other, id);
            }
            if (best == SIZE_MAX)
                break;
            auto & in = inputs[best];
            const auto & ids = *in.ids;
            size_t end = next_other == UINT64_MAX ? ids.size()
                                                  : static_cast<size_t>(std::lower_bound(ids.begin() + in.pos, ids.end(), next_other) - ids.begin());
            if (end == in.pos)
                throw Exception(ErrorCodes::INCORRECT_DATA, "row id {} is in two of the merged files", best_id);
            for (size_t c = 0; c < ncolumns; ++c)
                out[c]->insertRangeFrom(*in.block[c], in.pos, end - in.pos);
            in.pos = end;
            if (in.pos == ids.size())
                pull(in);
            if (out[0]->size() >= OWN_SLICE_ROWS)
                flush();
        }
        flush();
        *rows = written;
        *bytes = fileWriteFinish(*writer);
    });
    if (writer)
        chte_file_write_close(writer.release());
    return rc;
}

extern "C" int chte_own_file_layout(chte_table * table, chte_external_listing * listing, size_t file, chte_file_layout * layout,
                                    chte_error * error)
{
    return guarded(error, [&]
    {
        auto & ext = ownOf(table);
        const auto & listed = listedFile(*listing, file);
        auto full = fullFooter(ext, listed);
        const auto & footer = *full;
        *layout = {};
        layout->units = static_cast<uint32_t>(footer.unit_rows.size());
        layout->rows = footer.rows;
        for (size_t u = 0; u < footer.unit_rows.size(); ++u)
        {
            layout->min_unit_rows = u == 0 ? footer.unit_rows[u] : std::min(layout->min_unit_rows, footer.unit_rows[u]);
            layout->max_unit_rows = std::max(layout->max_unit_rows, footer.unit_rows[u]);
        }
        ext.layout_codec = "NONE";
        if (ext.format == CHTE_EXTERNAL_ARROW)
            ext.layout_codec = arrowCodec(listed.path, footer);
        else
        {
            bool page_index = !footer.parquet->row_groups.empty();
            bool first = true;
            for (const auto & row_group : footer.parquet->row_groups)
                for (const auto & chunk : row_group.columns)
                {
                    if (first)
                        ext.layout_codec = parquetCodecName(static_cast<int>(chunk.meta_data.codec));
                    first = false;
                    page_index = page_index && chunk.__isset.column_index_offset && chunk.__isset.offset_index_offset;
                    if (chunk.meta_data.__isset.bloom_filter_offset)
                        layout->bloom_filters = 1;
                    if (chunk.meta_data.__isset.dictionary_page_offset)
                        layout->dictionaries = 1;
                }
            layout->page_index = page_index ? 1 : 0;
        }
        layout->codec = ext.layout_codec.c_str();
    });
}
