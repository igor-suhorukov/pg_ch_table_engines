/*
 * Writing, reading and merging parts with ClickHouse's own code (ch_table_engines.md §2.2):
 * MergeTreeDataWriter for inserts, MergeTreeSequentialSource for reading one part,
 * MergeTask for merges — every one driven step by step on the calling thread.
 */
#include "internal.h"

#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/SimpleIncrement.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/MergeTree/Compaction/CompactionStatistics.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/MergeTask.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeMarksLoader.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/RangesInDataPart.h>

#include <base/scope_guard.h>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <atomic>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

using namespace chte;

namespace chte
{

void appendVector(IColumn & column, uint32_t nrows, const chte_vector & v)
{
    if (auto * nullable = typeid_cast<ColumnNullable *>(&column))
    {
        auto & map = nullable->getNullMapData();
        size_t old = map.size();
        map.resize(old + nrows);
        if (v.nulls)
            memcpy(map.data() + old, v.nulls, nrows);
        else
            memset(map.data() + old, 0, nrows);
        appendVector(nullable->getNestedColumn(), nrows, v);
        return;
    }
    if (auto * low_cardinality = typeid_cast<ColumnLowCardinality *>(&column))
    {
        auto full = low_cardinality->getDictionary().getNestedColumn()->cloneEmpty();
        appendVector(*full, nrows, v);
        low_cardinality->insertRangeFromFullColumn(*full, 0, nrows);
        return;
    }
    if (auto * strings = typeid_cast<ColumnString *>(&column))
    {
        if (!v.offsets)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "a String column needs offsets");
        auto & chars = strings->getChars();
        auto & offsets = strings->getOffsets();
        const auto * bytes = static_cast<const UInt8 *>(v.values);
        size_t base = chars.size();
        chars.insert(bytes + v.offsets[0], bytes + v.offsets[nrows]);
        offsets.reserve(offsets.size() + nrows);
        for (uint32_t i = 0; i < nrows; ++i)
            offsets.push_back(base + (v.offsets[i + 1] - v.offsets[0]));
        return;
    }
    if (column.isFixedAndContiguous())
    {
        size_t width = column.sizeOfValueIfFixed();
        size_t old = column.size();
        column.insertManyDefaults(nrows);
        /// The defaults just inserted are the column's own buffer; overwrite them in place.
        char * data = const_cast<char *>(column.getRawData().data());
        memcpy(data + old * width, v.values, nrows * width);
        return;
    }
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "columns of type {} are not supported yet", column.getName());
}

void appendValue(IColumn & column, const chte_vector & v, size_t i)
{
    const IColumn * nested = &column;
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(&column))
        nested = &nullable->getNestedColumn();
    chte_vector one{};
    one.nulls = v.nulls ? v.nulls + i : nullptr;
    if (v.offsets)
    {
        one.values = v.values;
        one.offsets = v.offsets + i;
    }
    else
        one.values = static_cast<const char *>(v.values) + i * nested->sizeOfValueIfFixed();
    appendVector(column, 1, one);
}

chte_vector vectorOf(const IColumn & column)
{
    chte_vector vector{};
    const IColumn * c = &column;
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(c))
    {
        vector.nulls = reinterpret_cast<const uint8_t *>(nullable->getNullMapData().data());
        c = &nullable->getNestedColumn();
    }
    if (const auto * strings = typeid_cast<const ColumnString *>(c))
    {
        vector.values = reinterpret_cast<const uint8_t *>(strings->getChars().data());
        /// PODArray keeps zeroed padding before its data, so offsets[-1] is 0: the
        /// nrows + 1 offsets of chte_vector are ClickHouse's own (PODArray.h).
        vector.offsets = strings->getOffsets().data() - 1;
        return vector;
    }
    if (c->isFixedAndContiguous())
    {
        vector.values = c->getRawData().data();
        return vector;
    }
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "columns of type {} are not supported yet", c->getName());
}

}

namespace chte
{

void fillPartInfo(const IMergeTreeDataPart & part, chte_part_info & info)
{
    memset(&info, 0, sizeof(info));
    snprintf(info.name, sizeof(info.name), "%s", part.name.c_str());
    snprintf(info.partition_id, sizeof(info.partition_id), "%s", part.info.getPartitionId().c_str());
    info.min_block = static_cast<uint64_t>(part.info.min_block);
    info.max_block = static_cast<uint64_t>(part.info.max_block);
    info.level = part.info.level;
    info.rows = part.rows_count;
    info.bytes_on_disk = part.getBytesOnDisk();
    info.marks = part.index_granularity->getMarksCountWithoutFinal();
    snprintf(info.temp_dir, sizeof(info.temp_dir), "%s", part.getDataPartStorage().getPartDirectory().c_str());
}

/// Gives a finished part its final directory name.
void commitPart(IMergeTreeDataPart & part)
{
    part.renameTo(part.name, /* remove_new_dir_if_exists= */ false);
    part.is_temp = false;
}

}

extern "C" int chte_write_begin(chte_table * table, uint64_t block_number, chte_writer ** writer, chte_error * error)
{
    return guarded(error, [&]
    {
        /// block 0: numbered by chte_write_block before chte_write_finish (VC18)
        /// a part's writing -- its sort, statistics and projections -- looks up functions of its own
        registerAllFunctions();
        /// VC21: a part's projections written with it
        describeProjections(*table);
        auto w = std::make_unique<chte_writer>();
        w->table = table;
        w->block_number = block_number;
        w->header = table->metadata->getSampleBlockNonMaterialized();
        w->columns = w->header.cloneEmptyColumns();
        *writer = w.release();
    });
}

extern "C" int chte_write_batch(chte_writer * writer, uint32_t nrows, const chte_vector * columns, chte_error * error)
{
    return guarded(error, [&]
    {
        if (writer->finished)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "chte_write_batch after chte_write_finish");
        for (size_t i = 0; i < writer->columns.size(); ++i)
            appendVector(*writer->columns[i], nrows, columns[i]);
    });
}

namespace
{

inline bool validAt(const uint64_t * validity, size_t i)
{
    return validity == nullptr || ((validity[i / 64] >> (i % 64)) & 1);
}

/// nrows values of a column of vexec's layouts (chte_column_in) appended to a ClickHouse column, read
/// once: a NULL row's value the type's default, as appendVector makes it (VC18).
void appendIn(IColumn & column, uint32_t nrows, const chte_column_in & in)
{
    if (auto * nullable = typeid_cast<ColumnNullable *>(&column))
    {
        auto & map = nullable->getNullMapData();
        size_t old = map.size();
        map.resize(old + nrows);
        for (uint32_t i = 0; i < nrows; ++i)
            map[old + i] = validAt(in.validity, i) ? 0 : 1;
        appendIn(nullable->getNestedColumn(), nrows, in);
        return;
    }
    if (auto * low_cardinality = typeid_cast<ColumnLowCardinality *>(&column))
    {
        auto full = low_cardinality->getDictionary().getNestedColumn()->cloneEmpty();
        appendIn(*full, nrows, in);
        low_cardinality->insertRangeFromFullColumn(*full, 0, nrows);
        return;
    }
    if (auto * strings = typeid_cast<ColumnString *>(&column))
    {
        auto & chars = strings->getChars();
        auto & offsets = strings->getOffsets();
        const auto * bytes = static_cast<const UInt8 *>(in.values);
        size_t base = chars.size();
        offsets.reserve(offsets.size() + nrows);
        if (in.offsets32)
        {
            int32_t first = in.offsets32[0];
            chars.insert(bytes + first, bytes + in.offsets32[nrows]);
            for (uint32_t i = 0; i < nrows; ++i)
                offsets.push_back(base + static_cast<size_t>(in.offsets32[i + 1] - first));
        }
        else if (in.offsets64)
        {
            uint64_t first = in.offsets64[0];
            chars.insert(bytes + first, bytes + in.offsets64[nrows]);
            for (uint32_t i = 0; i < nrows; ++i)
                offsets.push_back(base + (in.offsets64[i + 1] - first));
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "a String column needs offsets");
        return;
    }
    if (column.isFixedAndContiguous())
    {
        size_t width = column.sizeOfValueIfFixed();
        size_t old = column.size();
        const auto * src = static_cast<const char *>(in.values);
        column.insertManyDefaults(nrows);
        /// the defaults just inserted are the column's own buffer: overwritten in place
        char * dst = const_cast<char *>(column.getRawData().data()) + old * width;
        if (in.width == width && in.stride == width)
            memcpy(dst, src, nrows * width);
        else if (in.width == width)
            for (uint32_t i = 0; i < nrows; ++i)
                memcpy(dst + i * width, src + static_cast<size_t>(i) * in.stride, width);
        else if (in.width == 8 && width == 4)
        {
            /// a Decimal32's values, scaled as int64
            for (uint32_t i = 0; i < nrows; ++i)
            {
                Int64 v;
                memcpy(&v, src + static_cast<size_t>(i) * in.stride, sizeof(v));
                Int32 narrow = static_cast<Int32>(v);
                memcpy(dst + i * width, &narrow, sizeof(narrow));
            }
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "values of {} bytes for a column of type {}", in.width, column.getName());
        if (in.validity)
            for (uint32_t i = 0; i < nrows; ++i)
                if (!validAt(in.validity, i))
                    memset(dst + i * width, 0, width);
        return;
    }
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "columns of type {} are not supported yet", column.getName());
}

}

extern "C" int chte_write_columns(chte_writer * writer, uint32_t nrows, const chte_column_in * columns, chte_error * error)
{
    return guarded(error, [&]
    {
        if (writer->finished)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "chte_write_columns after chte_write_finish");
        for (size_t i = 0; i < writer->columns.size(); ++i)
            appendIn(*writer->columns[i], nrows, columns[i]);
    });
}

extern "C" int chte_write_block(chte_writer * writer, uint64_t block_number, chte_error * error)
{
    return guarded(error, [&]
    {
        if (block_number == 0 || writer->finished)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_write_block: block {} of a writer {}finished", block_number,
                            writer->finished ? "" : "not ");
        writer->block_number = block_number;
    });
}

namespace
{

/// A column's string chars, below Nullable and LowCardinality, or null
ColumnString::Chars * charsOf(IColumn & column)
{
    IColumn * c = &column;
    if (auto * nullable = typeid_cast<ColumnNullable *>(c))
        c = &nullable->getNestedColumn();
    if (auto * strings = typeid_cast<ColumnString *>(c))
        return &strings->getChars();
    return nullptr;
}

}

extern "C" int chte_write_reserve(chte_writer * writer, uint64_t nrows, chte_error * error)
{
    return guarded(error, [&]
    {
        if (writer->finished)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "chte_write_reserve after chte_write_finish");
        const auto & per_row = writer->table->last_bytes_per_row;
        for (size_t i = 0; i < writer->columns.size(); ++i)
        {
            auto & column = *writer->columns[i];
            if (column.size() != 0 || typeid_cast<ColumnLowCardinality *>(&column))
                continue;
            column.reserve(nrows);
            if (auto * chars = charsOf(column); chars && i < per_row.size())
                chars->reserve(static_cast<size_t>(per_row[i] * static_cast<double>(nrows) * 1.05));
        }
    });
}

extern "C" int chte_write_vector(chte_writer * writer, uint32_t column, chte_vector * vector, chte_error * error)
{
    return guarded(error, [&]
    {
        if (column >= writer->columns.size() || writer->finished)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_write_vector: no column {}", column);
        const IColumn & c = *writer->columns[column];
        if (!typeid_cast<const ColumnLowCardinality *>(&c))
        {
            *vector = vectorOf(c);
            return;
        }
        /// its values made full once for the rows it holds now
        writer->full_columns.resize(writer->columns.size());
        auto & full = writer->full_columns[column];
        if (!full.first || full.second != c.size())
            full = {c.convertToFullColumnIfLowCardinality(), c.size()};
        *vector = vectorOf(*full.first);
    });
}

extern "C" int chte_write_in_memory(chte_writer * writer, chte_error * error)
{
    return guarded(error, [&]
    {
        if (writer->finished || !writer->table->storage->pages_disk)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_write_in_memory: a writer of the pages store, before its parts");
        writer->in_memory = true;
    });
}

extern "C" int chte_write_part_files(chte_writer * writer, size_t i, const chte_part_file ** files, size_t * nfiles,
                                     chte_error * error)
{
    return guarded(error, [&]
    {
        if (i >= writer->parts.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "no part {}", i);
        *files = nullptr;
        *nfiles = 0;
        if (!writer->in_memory || i >= writer->part_files.size())
            return;
        auto & pf = writer->part_files[i];
        pf.listed.clear();
        for (const auto & [path, data] : pf.files)
            pf.listed.push_back(chte_part_file{path.c_str(), data->data(), data->size()});
        *files = pf.listed.data();
        *nfiles = pf.listed.size();
    });
}

namespace chte
{

/// The writer's rows as parts, one a partition, under temporary names.
void writeParts(chte_writer & writer, ContextPtr context)
{
    if (writer.finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "a writer's parts written twice");
    if (writer.block_number == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "block numbers start at 1");
    writer.finished = true;
    auto & table = *writer.table;
    auto & storage = *table.storage;
    /// the strings' bytes a row, for the next writer's room (chte_write_reserve)
    if (!writer.columns.empty() && writer.columns[0]->size() > 0)
    {
        double rows = static_cast<double>(writer.columns[0]->size());
        table.last_bytes_per_row.assign(writer.columns.size(), 0);
        for (size_t i = 0; i < writer.columns.size(); ++i)
            if (auto * chars = charsOf(*writer.columns[i]))
                table.last_bytes_per_row[i] = static_cast<double>(chars->size()) / rows;
    }
    Block block = writer.header.cloneWithColumns(std::move(writer.columns));
    auto blocks = MergeTreeDataWriter::splitBlockIntoParts(std::move(block), /* max_parts= */ 0, table.metadata, context);
    /// VC18: the pages store's parts into memory, for the caller to copy into pages
    auto * pages_disk = writer.in_memory ? storage.pages_disk.get() : nullptr;
    SCOPE_EXIT({ if (pages_disk) pages_disk->setMemoryWrites(false); });
    if (pages_disk)
        pages_disk->setMemoryWrites(true);
    for (auto & part_block : blocks)
    {
        checkInterrupted();
        /// writeTempPart numbers the part with insert_increment.get() (MergeTreeDataWriter.cpp:695):
        /// every part of this write gets the caller's block number.
        storage.insert_increment.set(writer.block_number - 1);
        auto temp = storage.writer.writeTempPart(part_block, table.metadata, context);
        if (!temp->part)
            continue;
        temp->finalize();
        temp->part->getDataPartStorage().commitTransaction();
        if (pages_disk)
        {
            chte_writer::PartFiles pf;
            for (auto & [path, data] : pages_disk->memoryFiles(temp->part->getDataPartStorage().getRelativePath()))
                pf.files.emplace_back(path, data);
            writer.part_files.push_back(std::move(pf));
        }
        writer.parts.push_back(std::move(temp));
    }
}

}

extern "C" int chte_write_finish(chte_writer * writer, size_t * nparts, chte_error * error)
{
    return guarded(error, [&]
    {
        writeParts(*writer, writer->table->context);
        *nparts = writer->parts.size();
    });
}

extern "C" int chte_write_part(chte_writer * writer, size_t i, chte_part_info * part, chte_error * error)
{
    return guarded(error, [&]
    {
        if (i >= writer->parts.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "no part {}", i);
        fillPartInfo(*writer->parts[i]->part, *part);
    });
}

extern "C" int chte_write_part_summary(chte_writer * writer, size_t i, const void ** summary, size_t * size,
                                       chte_error * error)
{
    return guarded(error, [&]
    {
        if (i >= writer->parts.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "no part {}", i);
        auto it = writer->summaries.find(i);
        if (it == writer->summaries.end())
            it = writer->summaries.emplace(i, summarizePart(*writer->parts[i]->part)).first;
        *summary = it->second.data();
        *size = it->second.size();
    });
}

extern "C" int chte_write_commit(chte_writer * writer, chte_error * error)
{
    return guarded(error, [&]
    {
        if (!writer->finished || writer->committed)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "chte_write_commit out of order");
        for (auto & temp : writer->parts)
        {
            commitPart(*temp->part);
            temp->temporary_directory_lock = {};
        }
        writer->committed = true;
    });
}

extern "C" void chte_write_close(chte_writer * writer)
{
    if (!writer)
        return;
    if (!writer->committed)
    {
        for (auto & temp : writer->parts)
        {
            try
            {
                temp->cancel();
            }
            catch (...)
            {
            }
        }
    }
    delete writer;
}

struct chte_batch
{
    Columns columns;
};

namespace chte
{

Names columnNames(const char * columns)
{
    Names names;
    String list = columns ? columns : "";
    boost::split(names, list, [](char c) { return c == ','; });
    for (auto & name : names)
        boost::trim(name);
    std::erase_if(names, [](const String & name) { return name.empty(); });
    if (names.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "no columns to read");
    return names;
}

/// A reader of a part's named columns over mark_ranges (none: every granule); _part_offset
/// is each row's place in the part.
std::vector<std::optional<MarkRanges>> markRuns(const std::optional<MarkRanges> & mark_ranges)
{
    std::vector<std::optional<MarkRanges>> runs;
    if (!mark_ranges)
    {
        runs.emplace_back(std::nullopt);
        return runs;
    }
    for (const auto & range : *mark_ranges)
    {
        if (runs.empty() || runs.back()->back().end != range.begin)
            runs.emplace_back(MarkRanges{});
        runs.back()->push_back(range);
    }
    return runs;
}

std::unique_ptr<chte_reader> openReader(chte_table & table, const MergeTreeData::DataPartPtr & part, const Names & names,
                                        std::optional<MarkRanges> mark_ranges)
{
    auto & storage = *table.storage;
    auto snapshot = storage.getStorageSnapshot(table.metadata, table.context);
    auto mutations = storage.getMutationsSnapshot({});
    auto alter_conversions = MergeTreeData::getAlterConversionsForPart(part, mutations, table.context);

    auto r = std::make_unique<chte_reader>();
    r->table = &table;
    for (const auto & name : names)
        r->present.push_back(!table.metadata->getColumns().hasPhysical(name) || part->getColumnPosition(name).has_value());
    r->open_run = [&storage, snapshot, part, alter_conversions, names](std::optional<MarkRanges> run)
    {
        return createMergeTreeSequentialSource(
            MergeTreeSequentialSourceType::Merge, storage, snapshot, RangesInDataPart(part), alter_conversions,
            /* merged_part_offsets= */ nullptr, names, std::move(run),
            std::make_shared<std::atomic<size_t>>(0), /* apply_deleted_mask= */ false,
            /* read_with_direct_io= */ false, /* prefetch= */ false);
    };
    r->runs = markRuns(mark_ranges);
    r->granularity = part->index_granularity;
    return r;
}

}

/// F7: the granule a block of `rows` rows of the current run is of, as MergeTreeSequentialSource reads a
/// run's granules in turn, a granule's rows in one block or more (MergeTreeSequentialSource.cpp:228-241);
/// a block past its granule's rows -- a compact part's longer last granule -- leaves the run's not known.
void chte_reader::noteGranule(size_t rows)
{
    batch_granule = -1;
    batch_granule_last = false;
    if (!run_mark || rows == 0)
        return;
    if (rows > run_mark_left)
    {
        run_mark.reset();
        return;
    }
    batch_granule = static_cast<int64_t>(*run_mark);
    run_mark_left -= rows;
    batch_granule_last = run_mark_left == 0;
    if (batch_granule_last)
    {
        ++*run_mark;
        run_mark_left = *run_mark < granularity->getMarksCountWithoutFinal() ? granularity->getMarkRows(*run_mark) : 0;
    }
}

bool chte_reader::pull(Block & block)
{
    if (next_block)
    {
        batch_granule = -1;
        batch_granule_last = false;
        return next_block(block);
    }
    for (;;)
    {
        if (executor && executor->pull(block))
        {
            noteGranule(block.rows());
            return true;
        }
        if (next_run >= runs.size())
            return false;
        executor.reset();
        /// F7: the run's first granule, its rows
        run_mark.reset();
        if (granularity)
        {
            size_t first = runs[next_run] ? runs[next_run]->front().begin : 0;

            if (first < granularity->getMarksCountWithoutFinal())
            {
                run_mark = first;
                run_mark_left = granularity->getMarkRows(first);
            }
        }
        pipeline = QueryPipeline(open_run(std::move(runs[next_run++])));
        executor = std::make_unique<PullingPipelineExecutor>(pipeline);
    }
}

extern "C" int chte_read_begin(chte_table * table, const char * part_name, const char * columns, const chte_mark_range * ranges,
                               size_t nranges, chte_reader ** reader, chte_error * error)
{
    return guarded(error, [&]
    {
        auto & storage = *table->storage;
        Names names = columnNames(columns);
        MergeTreeData::DataPartPtr part = storage.getPart(part_name);

        std::optional<MarkRanges> mark_ranges;
        if (nranges > 0)
        {
            size_t marks = part->index_granularity->getMarksCountWithoutFinal();
            mark_ranges.emplace();
            for (size_t i = 0; i < nranges; ++i)
            {
                if (ranges[i].begin >= ranges[i].end || ranges[i].end > marks
                    || (i > 0 && ranges[i].begin < ranges[i - 1].end))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_read_begin: marks [{}, {}) are not a range of part {} in order",
                                    ranges[i].begin, ranges[i].end, part_name);
                mark_ranges->push_back(MarkRange(ranges[i].begin, ranges[i].end));
            }
        }

        *reader = openReader(*table, part, names, std::move(mark_ranges)).release();
    });
}

namespace
{

/// The end of the compressed block at `offset` of a part's file: its checksum's 16 bytes, then its
/// method's byte and its size with the header's 9 bytes, then the rest of it.
uint64_t compressedBlockEnd(const IMergeTreeDataPart & part, const String & file, uint64_t offset)
{
    auto buf = part.getDataPartStorage().readFile(file, ReadSettings{}, 25);
    buf->seek(offset, SEEK_SET);
    char header[25];
    buf->readStrict(header, sizeof(header));
    UInt32 compressed;
    memcpy(&compressed, header + 17, sizeof(compressed));
    return offset + 16 + compressed;
}

}

/// W6 (chte_engine.h): a wide part's columns as its files hold them -- each column's streams as
/// MergeTreeReaderWide::addStreams enumerates them, their marks as it loads them, and their bytes'
/// ranges over the granules asked for.
extern "C" int chte_part_streams_of(chte_table * table, const char * part_name, const char * columns,
                                    uint32_t begin, uint32_t end, chte_part_streams * out, chte_error * error)
{
    return guarded(error, [&]
    {
        auto & storage = *table->storage;
        MergeTreeData::DataPartPtr part = storage.getPart(part_name);
        table->part_streams.clear();
        table->part_types.clear();
        table->part_refusal.clear();
        table->part_granule_rows.clear();
        *out = {};
        auto refuse = [&](const String & why)
        {
            table->part_refusal = why;
            table->part_streams.clear();
            out->refusal = table->part_refusal.c_str();
        };

        size_t marks = part->index_granularity->getMarksCountWithoutFinal();
        if (begin >= end || end > marks)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_part_streams_of: marks [{}, {}) are not a range of part {}'s {}",
                            begin, end, part_name, marks);
        if (!isWidePart(part))
        {
            refuse("a compact part");
            return;
        }
        uint64_t rows = 0;
        for (size_t m = begin; m < end; ++m)
        {
            size_t mark_rows = part->index_granularity->getMarkRows(m);
            rows += mark_rows;
            /// W21: each granule's rows, for the device's condition cache
            table->part_granule_rows.push_back(static_cast<uint32_t>(mark_rows));
        }

        auto mutations = storage.getMutationsSnapshot({});
        auto alter_conversions = MergeTreeData::getAlterConversionsForPart(part, mutations, table->context);
        auto info = std::make_shared<LoadedMergeTreeDataPartInfoForReader>(part, alter_conversions);
        auto settings = storage.getSettings();
        Names names = columnNames(columns);
        for (size_t c = 0; c < names.size(); ++c)
        {
            auto column = part->getColumns().tryGetByName(names[c]);
            if (!column)
            {
                refuse("a column the part lacks, " + names[c]);
                return;
            }
            auto serialization = part->getSerialization(names[c]);
            auto kinds = serialization->getKindStack();
            /// the default's, or the sparse one's over it, whose offsets the device parses
            bool sparse = kinds.size() == 2 && kinds[0] == ISerialization::Kind::DEFAULT &&
                          kinds[1] == ISerialization::Kind::SPARSE;
            if (!sparse && (kinds.size() != 1 || kinds[0] != ISerialization::Kind::DEFAULT))
            {
                refuse("a column of a serialization the device does not decode, " + names[c]);
                return;
            }
            DataTypePtr nested = removeNullable(column->type);
            int width = 0;
            if (!isString(nested))
            {
                if (!nested->isValueRepresentedByNumber() || nested->lowCardinality())
                {
                    refuse("a column of type " + column->type->getName());
                    return;
                }
                width = static_cast<int>(nested->getSizeOfValueInMemory());
            }
            table->part_types += column->type->getName() + ";";

            bool ok = true;
            serialization->enumerateStreams([&](const ISerialization::SubstreamPath & path)
            {
                if (!ok || ISerialization::isEphemeralSubcolumn(path, path.size()))
                    return;
                int kind;
                int stream_width;
                switch (path.back().type)
                {
                    case ISerialization::Substream::SparseOffsets:
                        /// a Nullable's sparse values are those not NULL, its null map none
                        kind = column->type->isNullable() ? CHTE_STREAM_SPARSE_NULLS : CHTE_STREAM_SPARSE;
                        stream_width = 0;
                        break;
                    case ISerialization::Substream::Regular:
                        kind = CHTE_STREAM_VALUES;
                        stream_width = width;
                        break;
                    case ISerialization::Substream::NullMap:
                        kind = CHTE_STREAM_NULLS;
                        stream_width = 1;
                        break;
                    case ISerialization::Substream::StringSizes:
                        kind = CHTE_STREAM_SIZES;
                        stream_width = 8;
                        break;
                    default:
                        ok = false;
                        refuse("a stream of " + names[c] + " of a kind the device does not decode");
                        return;
                }
                auto stream = IMergeTreeDataPart::getStreamNameForColumn(*column, path, ".bin", part->checksums, settings);
                if (!stream)
                {
                    ok = false;
                    refuse("a stream of " + names[c] + " the part lacks");
                    return;
                }
                String file = *stream + ".bin";
                if (file.size() >= sizeof(chte_part_stream::file))
                {
                    ok = false;
                    refuse("a stream's file of a name too long, " + file);
                    return;
                }
                /// W15 (pg_accel's §3.27): the marks kept in the process's mark cache, which a part's later
                /// runs and a scan's readers find -- each run's loaded them again, 0.12 to 0.39 ms a call of
                /// a fresh backend's, five of them a part of ClickBench's 10M rows
                MergeTreeMarksLoader loader(info, table->context->getMarkCache().get(),
                                            part->index_granularity_info.getMarksFilePath(*stream), part->getMarksCount(),
                                            part->index_granularity_info, /* save_marks_in_cache= */ true, ReadSettings{},
                                            nullptr, 1, false);
                auto getter = loader.loadMarks();
                auto first = getter->getMark(begin, 0);
                chte_part_stream s{};
                s.column = static_cast<int32_t>(c);
                s.kind = kind;
                s.width = stream_width;
                memcpy(s.file, file.c_str(), file.size() + 1);
                s.from = first.offset_in_compressed_file;
                s.skip = first.offset_in_decompressed_block;
                if (end < marks)
                {
                    auto last = getter->getMark(end, 0);
                    s.to = last.offset_in_decompressed_block == 0 ? last.offset_in_compressed_file
                                                                  : compressedBlockEnd(*part, file, last.offset_in_compressed_file);
                }
                else
                    s.to = part->getFileSizeOrZero(file);
                table->part_streams.push_back(s);
            });
            if (!ok)
                return;
        }
        out->rows = rows;
        out->nstreams = table->part_streams.size();
        out->streams = table->part_streams.data();
        out->types = table->part_types.c_str();
        out->granule_rows = table->part_granule_rows.data();
    });
}

/// W6: bytes of a part's file, through its storage's reader -- the pages store's buffers among them
extern "C" int chte_part_file_read(chte_table * table, const char * part_name, const char * file, uint64_t offset,
                                   uint64_t length, void * dest, chte_error * error)
{
    return guarded(error, [&]
    {
        MergeTreeData::DataPartPtr part = table->storage->getPart(part_name);
        auto buf = part->getDataPartStorage().readFile(file, ReadSettings{}, length);
        buf->seek(offset, SEEK_SET);
        buf->readStrict(static_cast<char *>(dest), length);
    });
}

namespace chte
{

/// A column of a read as the caller takes it: no wrapper, and LowCardinality kept only where
/// chte_read_keep_dictionaries asked for it (chte_read_next).
static ColumnPtr handedColumn(const chte_reader & reader, ColumnPtr column, size_t c)
{
    column = column->convertToFullIfWrapped();
    if (c >= reader.keep_dictionary.size() || !reader.keep_dictionary[c])
        column = column->convertToFullColumnIfLowCardinality();
    return column;
}

/// The chain that reads a step's columns from granule `mark` on, to the end of its run: the step's
/// reader -- its files open, its buffers -- kept, its stream begun anew, so that the first read seeks.
static void restartStep(chte_reader & reader, ReadStep & step)
{
    auto & main = step.readers.main;
    MergeTreeRangeReader range_reader(main.get(), {}, nullptr, reader.counters, true, main->canReadIncompleteGranules());
    std::vector<MergeTreeRangeReader> range_readers;
    range_readers.push_back(std::move(range_reader));
    step.chain.emplace(std::move(range_readers), MergeTreePatchReaders{});
    step.next_mark.reset();
}

/// Step `s`'s columns of the granule chte_read_next read last into the reader's columns: the
/// step's stream read on where it stands at the granule, else begun there, as ClickHouse's
/// PREWHERE reads a later step's columns of the granules its filter left rows in.
static void readStep(chte_reader & reader, size_t s)
{
    auto & step = reader.steps[s];
    size_t mark = *reader.granule;
    MarkRanges ranges;
    if (!step.chain || step.next_mark != mark)
    {
        restartStep(reader, step);
        ranges.push_back(MarkRange(mark, reader.granule_run_end));
    }
    std::vector<MarkRanges> patch_ranges;
    auto result = step.chain->read(reader.granule_rows, ranges, patch_ranges);
    if (result.num_rows != reader.granule_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "step {} read {} rows of granule {}, its first step {}", s,
                        result.num_rows, mark, reader.granule_rows);
    if (mark + 1 < reader.granule_run_end)
        step.next_mark = mark + 1;
    else
        step.next_mark.reset();
    const auto & header = step.chain->getSampleBlock();
    for (size_t i = 0; i < step.names.size(); ++i)
    {
        size_t c = step.columns[i];
        reader.columns[c] = handedColumn(reader, result.columns[header.getPositionByName(step.names[i])], c);
    }
    reader.step_read[s] = 1;
}

/// The next granule of a reader in steps, its first step's columns read: false at the end.
static bool nextGranule(chte_reader & reader)
{
    while (reader.range_at < reader.step_ranges.size() && reader.mark_at >= reader.step_ranges[reader.range_at].end)
    {
        ++reader.range_at;
        if (reader.range_at < reader.step_ranges.size())
            reader.mark_at = reader.step_ranges[reader.range_at].begin;
    }
    if (reader.range_at >= reader.step_ranges.size())
        return false;
    const auto & part_info = *reader.steps[0].info->data_part_info;
    reader.granule = reader.mark_at;
    reader.granule_rows = part_info.getIndexGranularity().getMarkRows(reader.mark_at);
    reader.granule_run_end = reader.step_ranges[reader.range_at].end;
    ++reader.mark_at;
    reader.columns.assign(reader.present.size(), nullptr);
    std::fill(reader.step_read.begin(), reader.step_read.end(), 0);
    readStep(reader, 0);
    return true;
}

}

extern "C" int chte_read_begin_steps(chte_table * table, const char * part_name, const char * columns, const uint8_t * steps,
                                     const chte_mark_range * ranges, size_t nranges, chte_reader ** reader, chte_error * error)
{
    return guarded(error, [&]
    {
        auto & storage = *table->storage;
        Names names = columnNames(columns);
        MergeTreeData::DataPartPtr part = storage.getPart(part_name);
        size_t marks = part->index_granularity->getMarksCountWithoutFinal();
        size_t nsteps = 0;

        for (size_t c = 0; c < names.size(); ++c)
            nsteps = std::max<size_t>(nsteps, steps[c] + 1);

        auto r = std::make_unique<chte_reader>();
        r->table = table;
        for (const auto & name : names)
            r->present.push_back(!table->metadata->getColumns().hasPhysical(name) || part->getColumnPosition(name).has_value());
        if (nranges > 0)
        {
            for (size_t i = 0; i < nranges; ++i)
            {
                if (ranges[i].begin >= ranges[i].end || ranges[i].end > marks || (i > 0 && ranges[i].begin < ranges[i - 1].end))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_read_begin_steps: marks [{}, {}) are not a range of part {} in order",
                                    ranges[i].begin, ranges[i].end, part_name);
                r->step_ranges.push_back(MarkRange(ranges[i].begin, ranges[i].end));
            }
        }
        else if (marks > 0)
            r->step_ranges.push_back(MarkRange(0, marks));
        r->mark_at = r->step_ranges.empty() ? 0 : r->step_ranges.front().begin;

        /// each step's reader as a merge's sequential source makes its own (MergeTreeSequentialSource.cpp)
        auto snapshot = storage.getStorageSnapshot(table->metadata, table->context);
        auto mutations = storage.getMutationsSnapshot({});
        auto alter_conversions = MergeTreeData::getAlterConversionsForPart(part, mutations, table->context);
        if (alter_conversions->hasPatches() || alter_conversions->hasLightweightDelete() || part->hasLightweightDelete())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "chte_read_begin_steps: part {} has patches or a lightweight delete", part_name);
        ReadSettings read_settings = storage.getContext()->getReadSettings();
        read_settings.local_fs_settings.method = LocalFSReadMethod::pread;
        r->mark_cache = storage.getContext()->getMarkCache();
        r->counters = std::make_shared<ReadStepPerformanceCounters>();
        MergeTreeReadTask::Extras extras =
        {
            .mark_cache = r->mark_cache.get(),
            .patch_join_cache = nullptr,
            .reader_settings = MergeTreeReaderSettings::createForMergeMutation(std::move(read_settings)),
            .storage_snapshot = snapshot,
        };

        r->steps.resize(nsteps);
        for (size_t c = 0; c < names.size(); ++c)
        {
            r->steps[steps[c]].names.push_back(names[c]);
            r->steps[steps[c]].columns.push_back(c);
        }
        for (size_t s = 0; s < nsteps; ++s)
        {
            auto & step = r->steps[s];
            if (step.names.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_read_begin_steps: no column of step {}", s);
            auto info = std::make_shared<MergeTreeReadTaskInfo>();
            info->alter_conversions = alter_conversions;
            info->data_part_info = std::make_shared<LoadedMergeTreeDataPartInfoForReader>(part, info->alter_conversions);
            info->const_virtual_fields.emplace("_part_index", static_cast<UInt64>(0));
            info->const_virtual_fields.emplace("_part_starting_offset", static_cast<UInt64>(0));
            info->const_virtual_fields.emplace("_sample_factor", 1.0);
            info->task_columns = getReadTaskColumnsForMerge(*info->data_part_info, snapshot, step.names, info->mutation_steps);
            info->task_columns.moveAllColumnsFromPrewhere();
            step.info = info;
            step.readers = MergeTreeReadTask::createReaders(info, extras, r->step_ranges, {});
        }
        r->step_read.assign(nsteps, 0);
        *reader = r.release();
    });
}

extern "C" int chte_read_step(chte_reader * reader, uint32_t step, chte_error * error)
{
    return guarded(error, [&]
    {
        if (reader->steps.empty() || step >= reader->steps.size() || !reader->granule)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "chte_read_step: no step {} of a granule to read", step);
        if (!reader->step_read[step])
            readStep(*reader, step);
    });
}

extern "C" int chte_read_next(chte_reader * reader, uint32_t * nrows, chte_error * error)
{
    return guarded(error, [&]
    {
        *nrows = 0;
        if (!reader->steps.empty())
        {
            checkInterrupted();
            if (nextGranule(*reader))
            {
                *nrows = static_cast<uint32_t>(reader->granule_rows);
                /// F7: a granule a call, whole
                reader->batch_granule = static_cast<int64_t>(*reader->granule);
                reader->batch_granule_last = true;
            }
            else
            {
                reader->columns.clear();
                reader->granule.reset();
            }
            return;
        }
        reader->columns.clear();
        Block block;
        while (reader->pull(block))
        {
            checkInterrupted();
            if (block.rows() == 0)
                continue;
            Columns columns;
            /// a column chte_read_keep_dictionaries keeps stays LowCardinality, for chte_read_dictionary
            for (size_t c = 0; c < block.columns(); ++c)
                columns.push_back(handedColumn(*reader, block.getByPosition(c).column, c));
            size_t rows = block.rows();
            if (reader->by_offset)
            {
                /// the rows at the wanted offsets alone: both ascend, so one pass over each
                const auto & offsets = typeid_cast<const ColumnUInt64 &>(*columns.back()).getData();
                IColumn::Filter keep(rows, 0);
                size_t kept = 0;
                for (size_t i = 0; i < rows; ++i)
                {
                    while (reader->wanted_pos < reader->wanted.size() && reader->wanted[reader->wanted_pos] < offsets[i])
                        ++reader->wanted_pos;
                    if (reader->wanted_pos < reader->wanted.size() && reader->wanted[reader->wanted_pos] == offsets[i])
                    {
                        keep[i] = 1;
                        ++kept;
                    }
                }
                columns.pop_back();
                /// F7: some of a granule's rows alone
                reader->batch_granule = -1;
                if (kept == 0)
                    continue;
                if (kept < rows)
                    for (auto & column : columns)
                        column = column->filter(keep, static_cast<ssize_t>(kept));
                rows = kept;
            }
            reader->columns = std::move(columns);
            *nrows = static_cast<uint32_t>(rows);
            return;
        }
    });
}

extern "C" void chte_read_granule(chte_reader * reader, int64_t * granule, int32_t * last)
{
    *granule = reader->batch_granule;
    *last = reader->batch_granule_last ? 1 : 0;
}

extern "C" int chte_read_vector(chte_reader * reader, uint32_t i, chte_vector * vector, chte_error * error)
{
    return guarded(error, [&]
    {
        if (i >= reader->columns.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "no column {}", i);
        if (!reader->columns[i])
            throw Exception(ErrorCodes::LOGICAL_ERROR, "column {} of a step not read", i);
        *vector = vectorOf(*reader->columns[i]);
    });
}

extern "C" int chte_read_keep_dictionaries(chte_reader * reader, const uint8_t * keep, size_t ncolumns, chte_error * error)
{
    return guarded(error, [&]
    {
        reader->keep_dictionary.assign(keep, keep + ncolumns);
        reader->dictionaries.assign(ncolumns, {});
    });
}

namespace
{

/// Dictionaries' ids, never given twice in a process
std::atomic<uint64_t> last_dictionary_id{0};

}

extern "C" int chte_read_dictionary(chte_reader * reader, uint32_t i, chte_dictionary * dictionary, chte_error * error)
{
    return guarded(error, [&]
    {
        if (i >= reader->columns.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "no column {}", i);
        if (!reader->columns[i])
            throw Exception(ErrorCodes::LOGICAL_ERROR, "column {} of a step not read", i);
        *dictionary = {};
        const auto * low_cardinality = typeid_cast<const ColumnLowCardinality *>(reader->columns[i].get());
        if (!low_cardinality)
            return;
        if (i >= reader->dictionaries.size())
            reader->dictionaries.resize(i + 1);
        auto & seen = reader->dictionaries[i];
        if (seen.column.get() != low_cardinality->getDictionaryPtr().get())
        {
            seen.column = low_cardinality->getDictionaryPtr();
            seen.id = ++last_dictionary_id;
        }
        const auto & unique = low_cardinality->getDictionary();
        dictionary->encoded = 1;
        dictionary->values = vectorOf(*unique.getNestedNotNullableColumn());
        dictionary->nvalues = unique.size();
        dictionary->indexes = low_cardinality->getIndexes().getRawData().data();
        dictionary->index_width = static_cast<uint32_t>(low_cardinality->getSizeOfIndexType());
        dictionary->has_null = unique.nestedColumnIsNullable() ? 1 : 0;
        dictionary->null_index = dictionary->has_null ? unique.getNullValueIndex() : 0;
        dictionary->id = seen.id;
    });
}

extern "C" void chte_read_end(chte_reader * reader)
{
    delete reader;
}

extern "C" int chte_read_present(chte_reader * reader, uint32_t column)
{
    return column < reader->present.size() ? reader->present[column] : 0;
}

/// The batch's columns are shared: the reader's next block is new columns, never these.
extern "C" chte_batch * chte_read_retain(chte_reader * reader)
{
    try
    {
        return new chte_batch{reader->columns};
    }
    catch (...)
    {
        return nullptr;
    }
}

extern "C" void chte_batch_release(chte_batch * batch)
{
    delete batch;
}

extern "C" int chte_merge(chte_table * table, const char * const * part_names, size_t nparts, uint64_t version,
                          chte_writer ** writer, chte_error * error)
{
    return guarded(error, [&]
    {
        if (nparts < 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_merge: no parts");
        if (version == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "block numbers start at 1");
        /// a merge's algorithms -- a summing engine's sums, an aggregating one's states -- and its
        /// projections look up functions and aggregate functions by name
        registerAllFunctions();
        /// VC21: the merged part's projections made with it, the inputs' loaded
        describeProjections(*table);
        auto & storage = *table->storage;
        MergeTreeData::DataPartsVector parts;
        for (size_t i = 0; i < nparts; ++i)
            parts.push_back(storage.checkedPart(part_names[i]));
        std::sort(parts.begin(), parts.end(), [](const auto & a, const auto & b) { return a->info < b->info; });

        auto future = std::make_shared<FutureMergedMutatedPart>();
        future->assign(parts, /* patch_parts= */ {}, /* projection= */ nullptr);
        ReservationSharedPtr reservation = MergeTreeData::reserveSpace(
            CompactionStatistics::estimateNeededDiskSpace(parts), parts.front()->getDataPartStorage());

        auto context = Context::createCopy(table->context);
        auto merge_entry = context->getMergeList().insert(storage.getStorageID(), future, context);
        TableLockHolder lock;
        auto task = storage.merger.mergePartsToTemporaryPart(
            future, table->metadata, merge_entry.get(), /* projection_merge_list_element= */ {}, lock, time(nullptr), context,
            reservation, /* deduplicate= */ false, /* deduplicate_by_columns= */ {}, /* cleanup= */ false, storage.merging_params,
            /* txn= */ nullptr);
        while (task->execute())
            checkInterrupted();

        /// the merged part under its temporary name, for the caller to store and commit as an insert's; named
        /// for the inputs' blocks and level as ClickHouse names it, and apart from any other by its version
        auto part = task->getFuture().get();
        part->getDataPartStorage().commitTransaction();
        part->info.mutation = static_cast<Int64>(version);
        part->setName(part->info.getPartNameV1());
        auto w = std::make_unique<chte_writer>();
        w->table = table;
        auto temp = std::make_unique<MergeTreeTemporaryPart>();
        temp->part = std::move(part);
        w->parts.push_back(std::move(temp));
        w->finished = true;
        *writer = w.release();
    });
}

extern "C" int chte_drop_part(chte_table * table, const char * part_name, chte_error * error)
{
    return guarded(error, [&]
    {
        table->storage->forgetPart(part_name);
        auto disk = table->storage->getStoragePolicy()->getDisks().at(0);
        disk->removeRecursive(fs::path(table->storage->getRelativeDataPath()) / part_name);
    });
}
