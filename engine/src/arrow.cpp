/*
 * Tables of Arrow IPC files (ch_table_engines.md §2.17, CH7's Arrow step): ClickHouse's own reader of the
 * Arrow file format -- its footer, its messages, and its decoder of record batches, which decompresses a
 * batch's buffers, LZ4 frames or zstd (Processors/Formats/Impl/ArrowIPC/) -- driven here a record batch at a
 * time, a table's unit, so that a parallel scan shares the batches out and each row's place in its file is
 * known: ClickHouse's ArrowIPCBlockInputFormat reads a file's batches in order only, and numbers no row.
 * What it does with a decoded batch -- its columns by name, each cast to the type asked -- is done here
 * alike, for the columns a table of files takes.  An Arrow file keeps no statistics: nothing prunes its
 * batches.
 */
#include "external.h"

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
            /// the asked columns' buffers alone read, decompressed and decoded
            auto reachable = decoder->reachableTopLevelBuffers(batch, &fields);
            messages->readBody(batch, msg.body_length, body, &reachable);
            auto decoded = decoder->decodeBatch(batch, body, &fields, &target_types, &reachable);

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
                        data[i] = first_row + first + i;
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

            if (rows_to_read)
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

void registerArrowOutput()
{
    static bool done = false;
    if (done)
        return;
    registerOutputFormatArrow(FormatFactory::instance());
    done = true;
}

}
