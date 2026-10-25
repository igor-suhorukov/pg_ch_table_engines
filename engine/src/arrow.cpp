/*
 * Tables of Arrow IPC files (ch_table_engines.md §2.17, CH7's Arrow step): ClickHouse's own reader of the
 * Arrow file format -- its footer, its messages, and its decoder of record batches, which decompresses a
 * batch's buffers, LZ4 frames or zstd (Processors/Formats/Impl/ArrowIPC/) -- driven here a record batch at a
 * time, a table's unit, so that a parallel scan shares the batches out and each row's place in its file is
 * known: ClickHouse's ArrowIPCBlockInputFormat reads a file's batches in order only, and numbers no row.
 * What it does with a decoded batch -- its columns by name, each cast to the type asked -- is done here
 * alike, for the columns a table of files takes.  An Arrow file keeps no statistics: an external table's batches
 * are never pruned, and a table of its own files' part keeps its batches' ranges of each column in its summary,
 * every batch decoded once as the part is summarized (VC28, forEachArrowBatch()), which prune them as a Parquet
 * file's row groups are pruned.
 */
#include "external.h"

#include <atomic>
#include <cerrno>
#include <unistd.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/castColumn.h>
#include <Processors/Formats/Impl/ArrowIPC/MessageReader.h>
#include <Processors/Formats/Impl/ArrowIPC/RecordBatchDecoder.h>

namespace DB
{
void registerOutputFormatArrow(FormatFactory & factory);
}

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_TYPE;
}

namespace chte
{

/// F6 (VC27): the rows the process read alone of record batches, for the tests (chte_rows_read_alone)
std::atomic<uint64_t> rows_read_alone{0};

namespace
{

/// A record batch's message at its block of the footer, its metadata bounded by the block's
const ArrowIPC::flatbuf::RecordBatch & recordBatchAt(ReadBufferFromFilePRead & buf, ArrowIPC::MessageReader & reader,
                                                      const ArrowIPC::ArrowFileBlock & block, ArrowIPC::MessageReader::Message & msg,
                                                      const String & path)
{
    buf.seek(block.offset, SEEK_SET);
    if (!reader.readNextMessage(msg, block.metadata_length) || msg.header->header_type() != ArrowIPC::flatbuf::MessageHeader_RecordBatch)
        throw Exception(ErrorCodes::INCORRECT_DATA, "file {}: no record batch at offset {}", path, block.offset);
    if (msg.body_length != block.body_length)
        throw Exception(ErrorCodes::INCORRECT_DATA, "file {}: a record batch's body of {} bytes where its block has {}", path,
                        msg.body_length, block.body_length);
    const auto * batch = msg.header->header_as_RecordBatch();
    if (batch->length() < 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "file {}: a record batch of {} rows", path, batch->length());
    return *batch;
}

/// The value fields of a schema's dictionaries, by their ids (ArrowIPCBlockInputFormat::collectDictionaryFields())
void collectDictionaryFields(const ArrowIPC::ArrowFields & fields, std::map<Int64, ArrowIPC::ArrowField> & out)
{
    for (const auto & field : fields)
    {
        if (field.dictionary)
        {
            ArrowIPC::ArrowField value_field = field;
            value_field.dictionary.reset();
            out.emplace(field.dictionary->id, std::move(value_field));
        }
        collectDictionaryFields(field.type.children, out);
    }
}

void collectDictionaryIds(const ArrowIPC::ArrowField & field, std::set<Int64> & out)
{
    if (field.dictionary)
        out.insert(field.dictionary->id);
    for (const auto & child : field.type.children)
        collectDictionaryIds(child, out);
}

/// What a reader of an Arrow file reads through, kept with it
struct ArrowRead
{
    std::shared_ptr<ExternalTable> table;
    std::shared_ptr<const ResolvedFile> resolved;
    String path;
    UInt64 first_row = 0;
    std::unique_ptr<ReadBufferFromFilePRead> buf;
    std::optional<ArrowIPC::MessageReader> messages;
    ArrowIPC::DictionaryRegistry dictionaries;
    std::unique_ptr<ArrowIPC::RecordBatchDecoder> decoder;
    /// the files' names of the columns asked, and the types they are read as
    UnorderedSetWithMemoryTracking<String> fields;
    UnorderedMapWithMemoryTracking<String, DataTypePtr> target_types;
    PODArray<char> body;
    /// the record batches to read, in order, and each batch's first row in the file
    std::vector<size_t> units;
    size_t next_unit = 0;
    std::vector<UInt64> unit_first;
    /// chte_external_read_rows': the rows at these offsets alone, ascending
    std::shared_ptr<const PaddedPODArray<UInt64>> rows_to_read;
    size_t wanted = 0;
    /// per column asked for: the table's column, or SIZE_MAX the row id; and whether the file lacks it (§2.18)
    std::vector<size_t> columns;
    std::vector<uint8_t> absent;

    /// F6 (pg_accel's pg_vector_executor.md §3.28; VC27): a schema's top-level field as rows read alone take it --
    /// none where a field is not flat -- its layout, a fixed width's bytes, and its node and validity buffer
    struct RowField
    {
        enum Layout : UInt8 { NONE, FIXED, BITS, OFFSETS32, OFFSETS64 } layout = NONE;
        size_t width = 0;
        size_t node = 0;
        size_t buffer = 0;
    };
    std::optional<std::vector<RowField>> row_fields;
    bool row_fields_known = false;

    /// The schema's fields as rows read alone take them, worked out once: none where a field is dictionary-encoded or
    /// not of a flat type -- a list, a struct, a view
    const std::vector<RowField> * rowFields()
    {
        if (row_fields_known)
            return row_fields ? &*row_fields : nullptr;
        row_fields_known = true;
        std::vector<RowField> out;
        size_t node = 0;
        size_t buffer = 0;
        for (const auto & field : resolved->footer->arrow->schema.fields)
        {
            RowField f;
            const auto & t = field.type;

            if (field.dictionary)
                return nullptr;
            f.node = node++;
            switch (t.kind)
            {
                case ArrowIPC::TypeKind::Null: f.layout = RowField::NONE; break;
                case ArrowIPC::TypeKind::Int: f.layout = RowField::FIXED; f.width = t.bit_width / 8; break;
                case ArrowIPC::TypeKind::FloatingPoint:
                    f.layout = RowField::FIXED;
                    f.width = t.float_precision == 0 ? 2 : t.float_precision == 1 ? 4 : 8;
                    break;
                case ArrowIPC::TypeKind::Decimal: f.layout = RowField::FIXED; f.width = t.decimal_bit_width / 8; break;
                case ArrowIPC::TypeKind::Date:
                    f.layout = RowField::FIXED;
                    f.width = t.unit == ArrowIPC::flatbuf::DateUnit_DAY ? 4 : 8;
                    break;
                case ArrowIPC::TypeKind::Time: f.layout = RowField::FIXED; f.width = t.time_bit_width / 8; break;
                case ArrowIPC::TypeKind::Timestamp:
                case ArrowIPC::TypeKind::Duration: f.layout = RowField::FIXED; f.width = 8; break;
                case ArrowIPC::TypeKind::FixedSizeBinary: f.layout = RowField::FIXED; f.width = t.byte_width; break;
                case ArrowIPC::TypeKind::Bool: f.layout = RowField::BITS; break;
                case ArrowIPC::TypeKind::Utf8:
                case ArrowIPC::TypeKind::Binary: f.layout = RowField::OFFSETS32; break;
                case ArrowIPC::TypeKind::LargeUtf8:
                case ArrowIPC::TypeKind::LargeBinary: f.layout = RowField::OFFSETS64; break;
                default: return nullptr;
            }
            if (f.layout == RowField::FIXED && f.width == 0)
                return nullptr;
            if (f.layout != RowField::NONE)
            {
                f.buffer = buffer;
                buffer += f.layout == RowField::OFFSETS32 || f.layout == RowField::OFFSETS64 ? 3 : 2;
            }
            out.push_back(f);
        }
        row_fields = std::move(out);
        return &*row_fields;
    }

    /// Bytes [at, at + n) of the file, by pread() of its descriptor alone: a row's few bytes, not a buffer's megabyte
    void readAt(UInt64 at, size_t n, char * to)
    {
        while (n > 0)
        {
            ssize_t got = ::pread(buf->getFD(), to, n, static_cast<off_t>(at));
            if (got < 0 && errno == EINTR)
                continue;
            if (got <= 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "file {}: could not read {} bytes at {}", path, n, at);
            to += got;
            at += static_cast<UInt64>(got);
            n -= static_cast<size_t>(got);
        }
    }

    /*
     * F6 (VC27): rows `wanted` of an uncompressed record batch -- offsets within it, ascending -- decoded alone: each
     * asked field's validity bits, values or offsets and bytes at those rows read from the file, as a record batch of
     * those rows that ClickHouse's decoder decodes as it decodes the file's -- every other field of no bytes, which it
     * passes over -- where the whole batch's asked columns were read and decoded for a fetch of a row of them
     */
    ArrowIPC::RecordBatchDecoder::DecodedColumns decodeRows(const ArrowIPC::flatbuf::RecordBatch & batch,
                                                           const ArrowIPC::ArrowFileBlock & block,
                                                           const std::vector<size_t> & wanted_rows)
    {
        const auto & schema_fields = resolved->footer->arrow->schema.fields;
        const auto & layout = *row_fields;
        const UInt64 body_at = static_cast<UInt64>(block.offset) + static_cast<UInt64>(block.metadata_length);
        const size_t k = wanted_rows.size();
        std::vector<ArrowIPC::flatbuf::FieldNode> nodes;
        std::vector<ArrowIPC::flatbuf::Buffer> buffers;
        PODArray<char> fake;

        auto add = [&](const char * data, size_t n)
        {
            size_t at = (fake.size() + 7) / 8 * 8;
            fake.resize(at + n);
            if (n > 0)
                memcpy(fake.data() + at, data, n);
            buffers.emplace_back(static_cast<int64_t>(at), static_cast<int64_t>(n));
        };
        auto source = [&](size_t i) -> const ArrowIPC::flatbuf::Buffer &
        {
            if (!batch.buffers() || i >= batch.buffers()->size())
                throw Exception(ErrorCodes::INCORRECT_DATA, "file {}: a record batch of fewer buffers than its schema's", path);
            return *batch.buffers()->Get(static_cast<flatbuffers::uoffset_t>(i));
        };
        auto bit = [&](const ArrowIPC::flatbuf::Buffer & b, size_t row) -> bool
        {
            char byte;
            readAt(body_at + static_cast<UInt64>(b.offset()) + row / 8, 1, &byte);
            return (static_cast<UInt8>(byte) >> (row % 8)) & 1;
        };
        auto bits_of = [&](const std::vector<uint8_t> & set)
        {
            std::string bytes((k + 7) / 8, '\0');
            for (size_t j = 0; j < k; ++j)
                if (set[j])
                    bytes[j / 8] = static_cast<char>(static_cast<UInt8>(bytes[j / 8]) | (1u << (j % 8)));
            add(bytes.data(), bytes.size());
        };

        for (size_t fi = 0; fi < schema_fields.size(); ++fi)
        {
            const RowField & f = layout[fi];
            size_t nbuffers = f.layout == RowField::NONE ? 0 : f.layout == RowField::OFFSETS32 || f.layout == RowField::OFFSETS64 ? 3 : 2;
            if (!fields.contains(schema_fields[fi].name) || f.layout == RowField::NONE)
            {
                nodes.emplace_back(static_cast<int64_t>(k), f.layout == RowField::NONE ? static_cast<int64_t>(k) : 0);
                for (size_t b = 0; b < nbuffers; ++b)
                    buffers.emplace_back(0, 0);
                continue;
            }
            /// the validity bits, where the batch's field has any
            const auto & validity = source(f.buffer);
            std::vector<uint8_t> valid(k, 1);
            size_t nulls = 0;
            if (validity.length() > 0)
                for (size_t j = 0; j < k; ++j)
                    if (!bit(validity, wanted_rows[j]))
                    {
                        valid[j] = 0;
                        ++nulls;
                    }
            nodes.emplace_back(static_cast<int64_t>(k), static_cast<int64_t>(nulls));
            if (nulls > 0)
                bits_of(valid);
            else
                add(nullptr, 0);
            const auto & values = source(f.buffer + 1);
            switch (f.layout)
            {
                case RowField::FIXED:
                {
                    std::string bytes(k * f.width, '\0');
                    for (size_t j = 0; j < k; ++j)
                        readAt(body_at + static_cast<UInt64>(values.offset()) + wanted_rows[j] * f.width, f.width,
                               bytes.data() + j * f.width);
                    add(bytes.data(), bytes.size());
                    break;
                }
                case RowField::BITS:
                {
                    std::vector<uint8_t> set(k, 0);
                    for (size_t j = 0; j < k; ++j)
                        set[j] = bit(values, wanted_rows[j]);
                    bits_of(set);
                    break;
                }
                case RowField::OFFSETS32:
                case RowField::OFFSETS64:
                {
                    bool wide = f.layout == RowField::OFFSETS64;
                    size_t ow = wide ? 8 : 4;
                    const auto & data = source(f.buffer + 2);
                    std::string offsets((k + 1) * ow, '\0');
                    std::string bytes;
                    for (size_t j = 0; j < k; ++j)
                    {
                        char pair[16];
                        readAt(body_at + static_cast<UInt64>(values.offset()) + wanted_rows[j] * ow, 2 * ow, pair);
                        Int64 begin;
                        Int64 end;
                        if (wide)
                        {
                            memcpy(&begin, pair, 8);
                            memcpy(&end, pair + 8, 8);
                        }
                        else
                        {
                            Int32 b32;
                            Int32 e32;
                            memcpy(&b32, pair, 4);
                            memcpy(&e32, pair + 4, 4);
                            begin = b32;
                            end = e32;
                        }
                        if (begin < 0 || end < begin || end > data.length())
                            throw Exception(ErrorCodes::INCORRECT_DATA, "file {}: a value's offsets [{}, {}) past its batch's data",
                                            path, begin, end);
                        size_t at = bytes.size();
                        bytes.resize(at + static_cast<size_t>(end - begin));
                        if (end > begin)
                            readAt(body_at + static_cast<UInt64>(data.offset()) + static_cast<UInt64>(begin),
                                   static_cast<size_t>(end - begin), bytes.data() + at);
                        Int64 next = static_cast<Int64>(bytes.size());
                        if (wide)
                            memcpy(offsets.data() + (j + 1) * 8, &next, 8);
                        else
                        {
                            Int32 n32 = static_cast<Int32>(next);
                            memcpy(offsets.data() + (j + 1) * 4, &n32, 4);
                        }
                    }
                    add(offsets.data(), offsets.size());
                    add(bytes.data(), bytes.size());
                    break;
                }
                case RowField::NONE:
                    break;
            }
        }

        flatbuffers::FlatBufferBuilder fbb;
        auto nodes_vec = fbb.CreateVectorOfStructs(nodes.data(), nodes.size());
        auto buffers_vec = fbb.CreateVectorOfStructs(buffers.data(), buffers.size());
        fbb.Finish(ArrowIPC::flatbuf::CreateRecordBatch(fbb, static_cast<int64_t>(k), nodes_vec, buffers_vec));
        const auto * rows_batch = flatbuffers::GetRoot<ArrowIPC::flatbuf::RecordBatch>(fbb.GetBufferPointer());
        auto reachable = decoder->reachableTopLevelBuffers(*rows_batch, &fields);
        auto decoded = decoder->decodeBatch(*rows_batch, fake, &fields, &target_types, &reachable);
        rows_read_alone += k;
        return decoded;
    }

    /// The dictionaries the asked columns use, decoded before any record batch, as the input format does
    void loadDictionaries()
    {
        const auto & footer = *resolved->footer->arrow;
        std::map<Int64, ArrowIPC::ArrowField> value_fields;
        collectDictionaryFields(footer.schema.fields, value_fields);
        std::set<Int64> reachable;
        for (const auto & field : footer.schema.fields)
            if (fields.contains(field.name))
                collectDictionaryIds(field, reachable);
        if (reachable.empty())
            return;
        ArrowIPC::RecordBatchDecoder dictionary_decoder(footer.schema, table->format_settings, dictionaries);
        for (const auto & block : footer.dictionary_blocks)
        {
            buf->seek(block.offset, SEEK_SET);
            ArrowIPC::MessageReader::Message msg;
            if (!messages->readNextMessage(msg, block.metadata_length)
                || msg.header->header_type() != ArrowIPC::flatbuf::MessageHeader_DictionaryBatch)
                throw Exception(ErrorCodes::INCORRECT_DATA, "file {}: no dictionary batch at offset {}", path, block.offset);
            const auto * batch = msg.header->header_as_DictionaryBatch();
            if (!reachable.contains(batch->id()))
                continue;
            if (!batch->data() || msg.body_length != block.body_length)
                throw Exception(ErrorCodes::INCORRECT_DATA, "file {}: a malformed dictionary batch at offset {}", path, block.offset);
            auto field = value_fields.find(batch->id());
            if (field == value_fields.end())
                throw Exception(ErrorCodes::INCORRECT_DATA, "file {}: a dictionary batch of unknown id {}", path, batch->id());
            const ArrowIPC::ArrowFields one{field->second};
            dictionary_decoder.validateBatchLayout(*batch->data(), one);
            messages->readBody(*batch->data(), msg.body_length, body);
            auto decoded = dictionary_decoder.decodeColumns(*batch->data(), body, one);
            dictionaries.set(batch->id(), decoded.at(0).column, batch->isDelta());
        }
    }

    bool next(Block & block)
    {
        const auto & footer = *resolved->footer;
        while (next_unit < units.size())
        {
            checkInterrupted();
            size_t u = units[next_unit++];
            UInt64 first = unit_first[u];
            size_t rows = footer.unit_rows[u];
            if (rows == 0)
                continue;
            /// chte_external_read_rows': a batch none of whose rows is wanted is not read
            if (rows_to_read)
            {
                while (wanted < rows_to_read->size() && (*rows_to_read)[wanted] < first)
                    ++wanted;
                if (wanted == rows_to_read->size() || (*rows_to_read)[wanted] >= first + rows)
                    continue;
            }

            ArrowIPC::MessageReader::Message msg;
            const auto & batch = recordBatchAt(*buf, *messages, footer.arrow->record_batch_blocks[u], msg, path);
            if (static_cast<size_t>(batch.length()) != rows)
                throw Exception(ErrorCodes::INCORRECT_DATA, "file {}: a record batch of {} rows where its listing had {}", path,
                                batch.length(), rows);
            /// F6 (VC27): a fetch's few rows of an uncompressed batch of flat fields read alone; else the asked columns'
            /// buffers alone read, decompressed and decoded
            std::vector<size_t> wanted_rows;
            if (rows_to_read)
                for (size_t w = wanted; w < rows_to_read->size() && (*rows_to_read)[w] < first + rows; ++w)
                    wanted_rows.push_back((*rows_to_read)[w] - first);
            bool alone = rows_to_read && !batch.compression() && wanted_rows.size() * 64 <= rows && rowFields() != nullptr;
            ArrowIPC::RecordBatchDecoder::DecodedColumns decoded;
            if (alone)
            {
                decoded = decodeRows(batch, footer.arrow->record_batch_blocks[u], wanted_rows);
                rows = wanted_rows.size();
                wanted += wanted_rows.size();
            }
            else
            {
                auto reachable = decoder->reachableTopLevelBuffers(batch, &fields);
                messages->readBody(batch, msg.body_length, body, &reachable);
                decoded = decoder->decodeBatch(batch, body, &fields, &target_types, &reachable);
            }

            std::unordered_map<String, size_t> by_name;
            for (size_t i = 0; i < decoded.size(); ++i)
                by_name.emplace(decoded[i].name, i);
            ColumnsWithTypeAndName out;
            for (size_t k = 0; k < columns.size(); ++k)
            {
                size_t column = columns[k];
                if (absent[k])
                {
                    /// not in the file: defaults, which the caller passes over (chte_read_present)
                    const auto & c = table->columns[column];
                    out.emplace_back(c.type->createColumnConstWithDefaultValue(rows)->convertToFullColumnIfConst(), c.type, c.name);
                    continue;
                }
                if (column == SIZE_MAX)
                {
                    auto ids = ColumnUInt64::create(rows);
                    auto & data = ids->getData();
                    for (size_t i = 0; i < rows; ++i)
                        data[i] = first_row + first + (alone ? wanted_rows[i] : i);
                    out.emplace_back(std::move(ids), std::make_shared<DataTypeUInt64>(), ROWID);
                    continue;
                }
                const auto & c = table->columns[column];
                const String & file_name = resolved->names[column];
                auto it = by_name.find(file_name);
                if (it == by_name.end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "the decoder of {} handed no column {}", path, file_name);
                const auto & src = decoded[it->second];
                ColumnPtr col;
                try
                {
                    col = castColumn({src.column, src.type, src.name}, fileReadType(*table, *resolved, column));
                }
                catch (Exception & e)
                {
                    e.addMessage("while reading column {} of file {}", file_name, path);
                    throw;
                }
                out.emplace_back(convertColumn(c, col, path), c.type, c.name);
            }
            block = Block(std::move(out));

            if (rows_to_read && !alone)
            {
                /// the rows at the offsets wanted within the batch, the others left out
                IColumn::Filter keep(rows, 0);
                size_t kept = 0;
                for (; wanted < rows_to_read->size() && (*rows_to_read)[wanted] < first + rows; ++wanted)
                {
                    keep[(*rows_to_read)[wanted] - first] = 1;
                    ++kept;
                }
                for (auto & column : block)
                    column.column = column.column->filter(keep, static_cast<ssize_t>(kept));
            }
            return true;
        }
        return false;
    }
};

}

std::shared_ptr<ExternalFooter> readArrowFooter(const ExternalTable &, const String & path, UInt64 size, Int64 mtime,
                                                const std::vector<UInt64> * unit_rows)
{
    auto buf = openListed(path, size, mtime);
    auto footer = std::make_shared<ExternalFooter>();
    footer->size = size;
    footer->mtime = mtime;
    auto arrow = std::make_shared<ArrowIPC::ArrowFileFooter>(ArrowIPC::readArrowFileFooter(*buf, size));
    if (unit_rows && unit_rows->size() == arrow->record_batch_blocks.size())
    {
        /// VC25: a part's units' rows its summary's, its batches' messages not read for them
        footer->unit_rows = *unit_rows;
        for (UInt64 rows : footer->unit_rows)
            footer->rows += rows;
        footer->arrow = std::move(arrow);
        return footer;
    }
    /// each record batch's rows, from its message's metadata alone, as ClickHouse's schema reader counts them:
    /// read through a buffer of the largest message's metadata (VC25), where the footer's of a megabyte read one
    /// a batch -- 154 MB a fresh backend's planning of ClickBench's two files
    size_t metadata = 4096;
    for (const auto & block : arrow->record_batch_blocks)
        metadata = std::max<size_t>(metadata, static_cast<size_t>(std::max<Int64>(block.metadata_length, 0)));
    auto messages = openListed(path, size, mtime, std::min<size_t>((metadata + 4095) / 4096 * 4096, DBMS_DEFAULT_BUFFER_SIZE));
    ArrowIPC::MessageReader reader(*messages);
    for (const auto & block : arrow->record_batch_blocks)
    {
        ArrowIPC::MessageReader::Message msg;
        const auto & batch = recordBatchAt(*messages, reader, block, msg, path);
        footer->unit_rows.push_back(static_cast<UInt64>(batch.length()));
        footer->rows += footer->unit_rows.back();
    }
    footer->arrow = std::move(arrow);
    return footer;
}

NamesAndTypesList arrowFileSchema(const ExternalTable & table, const ExternalFooter & footer)
{
    /// as ArrowIPCSchemaReader::readSchema() infers them, nullability as the file has it; a column of a type
    /// ClickHouse's reader takes not, Nothing, which no column of the caller's takes
    NamesAndTypesList result;
    for (const auto & field : footer.arrow->schema.fields)
    {
        DataTypePtr type;
        try
        {
            type = ArrowIPC::fieldToCHType(field, table.format_settings, /* make_nullable= */ field.nullable);
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::NOT_IMPLEMENTED && e.code() != ErrorCodes::UNKNOWN_TYPE)
                throw;
            type = std::make_shared<DataTypeNothing>();
        }
        if (field.dictionary && type->canBeInsideLowCardinality())
            type = std::make_shared<DataTypeLowCardinality>(type);
        result.emplace_back(field.name, type);
    }
    return result;
}

std::unique_ptr<chte_reader> openArrowReader(chte_table & table, std::shared_ptr<const ResolvedFile> resolved,
                                             const ListedFile & listed, const Names & names,
                                             std::optional<std::vector<size_t>> units,
                                             std::shared_ptr<const PaddedPODArray<UInt64>> rows_to_read)
{
    auto & ext = *table.external;
    /// VC25: a part listed from its summary read with its footer, which a reader alone needs
    if (!resolved->footer->arrow)
    {
        auto full = std::make_shared<ResolvedFile>(*resolved);
        full->footer = fullFooter(ext, listed);
        resolved = std::move(full);
    }
    auto state = std::make_shared<ArrowRead>();
    state->table = table.external;
    state->resolved = std::move(resolved);
    state->path = listed.path;
    state->first_row = listed.first_row;
    const auto & footer = *state->resolved->footer;
    for (const auto & name : names)
    {
        /// the row id a row's place, but a table of its own files' (§2.18), a column of the file
        if (name == ROWID && !ext.own)
        {
            state->columns.push_back(SIZE_MAX);
            state->absent.push_back(0);
            continue;
        }
        size_t i = ext.columnIndex(name);
        state->columns.push_back(i);
        state->absent.push_back(state->resolved->isAbsent(i) ? 1 : 0);
        if (state->absent.back())
            continue;
        state->fields.insert(state->resolved->names[i]);
        state->target_types[state->resolved->names[i]] = fileReadType(ext, *state->resolved, i);
    }
    if (units)
        state->units = std::move(*units);
    else
        for (size_t u = 0; u < footer.unit_rows.size(); ++u)
            state->units.push_back(u);
    UInt64 first = 0;
    for (UInt64 rows : footer.unit_rows)
    {
        state->unit_first.push_back(first);
        first += rows;
    }
    state->rows_to_read = std::move(rows_to_read);

    state->buf = openListed(listed.path, listed.size, listed.mtime);
    state->messages.emplace(*state->buf);
    state->loadDictionaries();
    state->decoder = std::make_unique<ArrowIPC::RecordBatchDecoder>(footer.arrow->schema, ext.format_settings, state->dictionaries);

    auto r = std::make_unique<chte_reader>();
    r->table = &table;
    r->present.assign(names.size(), 1);
    for (size_t k = 0; k < names.size(); ++k)
        if (state->absent[k])
            r->present[k] = 0;
    r->next_block = [raw = state.get()](Block & block) { return raw->next(block); };
    r->external_state = std::move(state);
    return r;
}

bool forEachArrowBatch(const ExternalTable & ext, const ListedFile & listed, const ResolvedFile & resolved_in,
                       const std::vector<size_t> & columns, const std::function<void(size_t, const Columns &)> & each)
{
    std::shared_ptr<ResolvedFile> full;
    const ResolvedFile * resolved = &resolved_in;
    if (!resolved->footer->arrow)
    {
        full = std::make_shared<ResolvedFile>(resolved_in);
        full->footer = fullFooter(ext, listed);
        resolved = full.get();
    }
    const auto & footer = *resolved->footer;
    UnorderedSetWithMemoryTracking<String> fields;
    UnorderedMapWithMemoryTracking<String, DataTypePtr> target_types;
    std::unordered_map<String, size_t> wanted;
    for (size_t k = 0; k < columns.size(); ++k)
    {
        const String & name = resolved->names[columns[k]];
        fields.insert(name);
        target_types[name] = fileReadType(ext, *resolved, columns[k]);
        wanted.emplace(name, k);
    }
    /// a dictionary-encoded column: none of its batches' ranges (a table of its own files writes none)
    std::set<Int64> reachable_dictionaries;
    for (const auto & field : footer.arrow->schema.fields)
        if (fields.contains(field.name))
            collectDictionaryIds(field, reachable_dictionaries);
    if (!reachable_dictionaries.empty())
        return false;

    auto buf = openListed(listed.path, listed.size, listed.mtime);
    ArrowIPC::MessageReader messages(*buf);
    ArrowIPC::DictionaryRegistry dictionaries;
    ArrowIPC::RecordBatchDecoder decoder(footer.arrow->schema, ext.format_settings, dictionaries);
    PODArray<char> body;
    for (size_t u = 0; u < footer.unit_rows.size(); ++u)
    {
        checkInterrupted();
        ArrowIPC::MessageReader::Message msg;
        const auto & batch = recordBatchAt(*buf, messages, footer.arrow->record_batch_blocks[u], msg, listed.path);
        auto reachable = decoder.reachableTopLevelBuffers(batch, &fields);
        messages.readBody(batch, msg.body_length, body, &reachable);
        auto decoded = decoder.decodeBatch(batch, body, &fields, &target_types, &reachable);
        Columns out(columns.size());
        for (const auto & src : decoded)
        {
            auto it = wanted.find(src.name);
            if (it == wanted.end())
                continue;
            out[it->second] = castColumn({src.column, src.type, src.name}, fileReadType(ext, *resolved, columns[it->second]));
        }
        for (size_t k = 0; k < out.size(); ++k)
            if (!out[k])
                throw Exception(ErrorCodes::LOGICAL_ERROR, "the decoder of {} handed no column {}", listed.path,
                                resolved->names[columns[k]]);
        each(u, out);
    }
    return true;
}

void arrowColumnBytes(const ExternalFooter & footer, size_t ncolumns, uint64_t * compressed, uint64_t * uncompressed)
{
    /// the batches' bodies shared out among the file's columns: an Arrow file says no column's bytes apart
    UInt64 body = 0;
    for (const auto & block : footer.arrow->record_batch_blocks)
        body += static_cast<UInt64>(std::max<Int64>(block.body_length, 0));
    size_t fields = std::max<size_t>(footer.arrow->schema.fields.size(), 1);
    *compressed = *uncompressed = body * ncolumns / fields;
}

String arrowCodec(const String & path, const ExternalFooter & footer)
{
    if (footer.arrow->record_batch_blocks.empty())
        return "NONE";
    auto buf = openListed(path, footer.size, footer.mtime);
    ArrowIPC::MessageReader reader(*buf);
    ArrowIPC::MessageReader::Message msg;
    const auto & batch = recordBatchAt(*buf, reader, footer.arrow->record_batch_blocks.front(), msg, path);
    if (!batch.compression())
        return "NONE";
    return batch.compression()->codec() == ArrowIPC::flatbuf::CompressionType_ZSTD ? "ZSTD" : "LZ4_FRAME";
}

extern "C" uint64_t chte_rows_read_alone(void)
{
    return rows_read_alone.load();
}

void registerArrowOutput()
{
    static bool done = false;
    if (done)
        return;
    registerOutputFormatArrow(FormatFactory::instance());
    done = true;
}

}
