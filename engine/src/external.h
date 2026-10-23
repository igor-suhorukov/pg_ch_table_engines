/*
 * External tables of libchte_engine.so (ch_table_engines.md §2.17, CH7): what their formats' readers share --
 * a table's columns and settings, its files' footers kept in the process, a listing's files, and how a
 * column the files hand becomes the caller's -- external.cpp's calls and Parquet's reader, arrow.cpp's Arrow
 * IPC reader, iceberg.cpp's Iceberg tables.
 */
#pragma once

#include "internal.h"

#include <Formats/FormatFilterInfo.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromFile.h>
#include <Processors/Formats/Impl/ArrowIPC/SchemaConverter.h>
#include <Processors/Formats/Impl/Parquet/ReadCommon.h>

namespace parquet::format
{
class FileMetaData;
}

namespace DB
{
struct IcebergDataObjectInfo;
}

namespace chte
{

/// PostgreSQL's epoch, 2000-01-01, in days and microseconds from 1970-01-01
constexpr Int32 PG_EPOCH_DAYS = 10957;
constexpr Int64 PG_EPOCH_MICROSECONDS = Int64(PG_EPOCH_DAYS) * 86400 * 1000000;

/// The row id column every reader may be asked for (chte.h's CH_ROWID_COLUMN)
constexpr const char * ROWID = "_pg_rowid";

/// A column of an external table
struct ExternalColumn
{
    String name;            /// the caller's
    String file_name;       /// the files', as the caller gave it
    DataTypePtr type;       /// the caller's, without Nullable
    DataTypePtr read_type;  /// what the reader is asked for, without Nullable
    int convert = CHTE_CONVERT_NONE;
    bool not_null = false;
    bool check_utf8 = false;
};

/// A file's footer, as read once in the process for a format, a path, a size and a modification time
struct ExternalFooter
{
    UInt64 size = 0;
    Int64 mtime = 0;
    std::shared_ptr<const parquet::format::FileMetaData> parquet;
    std::shared_ptr<const ArrowIPC::ArrowFileFooter> arrow;
    /// each unit's rows, and their sum
    std::vector<UInt64> unit_rows;
    UInt64 rows = 0;
};

/// A file as a table reads it: its footer, and each of the table's columns resolved in it
struct ResolvedFile
{
    std::shared_ptr<const ExternalFooter> footer;
    NamesAndTypesList schema;               /// as ClickHouse infers it
    std::vector<String> names;              /// per table column: the file's name of it
    std::vector<DataTypePtr> file_types;    /// and its type there, Nullable where it may hold NULLs
    std::vector<uint8_t> nullable;
};

/// A file of a listing
struct ListedFile
{
    String path;
    UInt64 size = 0;
    Int64 mtime = 0;
    UInt64 first_row = 0;
    /// its rows -- an Iceberg data file's deleted ones among them -- and its units
    UInt64 rows = 0;
    size_t units = 0;
    std::shared_ptr<const ResolvedFile> resolved;
    /// an Iceberg table's data file: ClickHouse's object of it, which carries the delete files that apply
    std::shared_ptr<DB::IcebergDataObjectInfo> iceberg;
};

struct IcebergTable;
struct IcebergSnapshot;
struct IcebergWriter;

class ExternalTable
{
public:
    int format = CHTE_EXTERNAL_PARQUET;
    String name;
    String location;
    ContextMutablePtr context;
    FormatSettings format_settings;
    Parquet::ReadOptions read_options;
    std::vector<ExternalColumn> columns;
    std::unordered_map<String, size_t> by_name;
    /// the files this table has resolved, by path: each one's identity checked at a listing
    std::map<String, std::shared_ptr<const ResolvedFile>> resolved;
    /// chte_external_schema's, until the next call
    NamesAndTypesList schema;
    std::vector<String> schema_types;
    std::vector<chte_external_field> fields;
    /// an Iceberg table's: ClickHouse's metadata of it, made as it is first listed (iceberg.cpp)
    std::shared_ptr<IcebergTable> iceberg;

    const ExternalColumn & column(const String & caller_name) const { return columns[columnIndex(caller_name)]; }
    size_t columnIndex(const String & caller_name) const;
};

/// A listed file opened for reading, its identity checked against the listing's
std::unique_ptr<ReadBufferFromFilePRead> openListed(const String & path, UInt64 size, Int64 mtime);

/// Shared resources of one reader that start no thread
FormatParserSharedResourcesPtr singleThreadedResources(const ContextPtr & context);

/// A file's footer of the table's format -- Parquet's for an Iceberg table's data file -- read once for its identity
std::shared_ptr<const ExternalFooter> footerOf(const ExternalTable & table, const String & path, UInt64 size, Int64 mtime);

/// The table's columns found in file->schema, a file's or an Iceberg table's (where, for the errors), each
/// checked to take its values: file->names, file_types and nullable
void resolveColumns(const ExternalTable & table, ResolvedFile & file, const String & where);

/// A header of a file's columns: per caller name, the file's name and the type it is read as there
Block fileHeader(const ExternalTable & table, const ResolvedFile & file, const Names & names);

/// The filter as a reader of a file takes it, over the columns of keys, its header (none where the filter names a
/// column the header lacks); a column mapper, where the file's columns are found by their field ids
FormatFilterInfoPtr formatFilter(const chte_filter & filter, const ExternalTable & table, const ResolvedFile & file,
                                 const Block & keys, ColumnMapperPtr mapper = nullptr);

/// A reader handing a pipeline's chunks -- each with its rows' numbers -- as the caller's columns: each the
/// pipeline's column of the files' name of it, cast where asked to the type the table reads it as; what the
/// pipeline reads through kept with it
std::unique_ptr<chte_reader> pipelineReader(chte_table & table, std::shared_ptr<const ResolvedFile> resolved, const ListedFile & listed,
                                            const Names & names, QueryPipeline pipeline, bool cast, std::shared_ptr<void> keep);

/// A file's column statistics from its Parquet footer
void parquetStats(chte_table & table, const ExternalTable & ext, const ListedFile & file, const ResolvedFile & resolved,
                  const String & name, chte_column_stats * stats);

/// A column as a reader hands it, in the type the table reads it as there, as the caller takes it: NOT NULL
/// checked, dates and timestamps moved to PostgreSQL's epoch, strings checked
ColumnPtr convertColumn(const ExternalColumn & column, ColumnPtr col, const String & path);

/// The type a table's column i is read as from a file: its read type, Nullable where the file may hold NULLs
DataTypePtr fileReadType(const ExternalTable & table, const ResolvedFile & file, size_t i);

/// arrow.cpp: an Arrow IPC file's footer -- its schema, its record batches and each one's rows -- and its
/// columns as ClickHouse infers them; a reader of its record batches, or of its rows at offsets; the bytes of
/// the asked columns, from the batches' bodies; ClickHouse's Arrow IPC writer, for the tests' files
std::shared_ptr<ExternalFooter> readArrowFooter(const ExternalTable & table, const String & path, UInt64 size, Int64 mtime);
NamesAndTypesList arrowFileSchema(const ExternalTable & table, const ExternalFooter & footer);
std::unique_ptr<chte_reader> openArrowReader(chte_table & table, std::shared_ptr<const ResolvedFile> resolved,
                                             const ListedFile & listed, const Names & names,
                                             std::optional<std::vector<size_t>> units,
                                             std::shared_ptr<const PaddedPODArray<UInt64>> rows_to_read);
void arrowColumnBytes(const ExternalFooter & footer, size_t ncolumns, uint64_t * compressed, uint64_t * uncompressed);
void registerArrowOutput();

/// iceberg.cpp: a listing of an Iceberg table's snapshot, the newest at the call (or the one its settings pin),
/// each data file with the deletes that apply to it; its bytes for a parallel scan's workers, and theirs read;
/// the snapshot's schema; whether a filter keeps a data file by its manifest's statistics; a reader of a data
/// file, its deletes applied; its statistics where it has no deletes; the tests' writer of Iceberg tables
void listIceberg(chte_external_listing & listing);
void writeIcebergListing(const chte_external_listing & listing, WriteBuffer & out);
void readIcebergListing(chte_external_listing & listing, ReadBuffer & in);
NamesAndTypesList icebergSchema(ExternalTable & table);
std::shared_ptr<const ResolvedFile> icebergResolvedFor(ExternalTable & table, const chte_external_listing & listing);
bool icebergKeeps(chte_filter & filter, ExternalTable & table, const chte_external_listing & listing, const ListedFile & file);
std::unique_ptr<chte_reader> openIcebergReader(chte_table & table, const chte_external_listing & listing, const ListedFile & listed,
                                               const chte_filter * filter, const Names & names,
                                               std::shared_ptr<const PaddedPODArray<UInt64>> rows_to_read);
void icebergStats(chte_table & table, ExternalTable & ext, const chte_external_listing & listing, const ListedFile & file,
                  const String & name, chte_column_stats * stats);
bool icebergDeletes(const ListedFile & file);
std::shared_ptr<IcebergWriter> icebergWriteBegin(const String & location, int action, const String & spec,
                                                 const chte_file_column * columns, size_t ncolumns, ContextMutablePtr context,
                                                 const FormatSettings & format_settings, Block & header,
                                                 std::vector<DataTypePtr> & sources, std::vector<int32_t> & converts,
                                                 std::vector<size_t> & vectors);
void icebergWrite(IcebergWriter & writer, Block block);
void icebergAlter(const String & location, const String & command, ContextMutablePtr context);
UInt64 icebergWriteFinish(IcebergWriter & writer);
void icebergWriteCancel(IcebergWriter & writer);

}

struct chte_external_listing
{
    std::shared_ptr<chte::ExternalTable> table;
    std::vector<chte::ListedFile> files;
    std::string bytes;
    /// an Iceberg table's snapshot (iceberg.cpp)
    std::shared_ptr<chte::IcebergSnapshot> iceberg;
};
