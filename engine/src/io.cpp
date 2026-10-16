/*
 * Writing, reading and merging parts with ClickHouse's own code (ch_table_engines.md §2.2):
 * MergeTreeDataWriter for inserts, MergeTreeSequentialSource for reading one part,
 * MergeTask for merges — every one driven step by step on the calling thread.
 */
#include "internal.h"

#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <Common/SimpleIncrement.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/MergeTree/Compaction/CompactionStatistics.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/MergeTask.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/RangesInDataPart.h>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

using namespace chte;

namespace
{

/// Appends nrows values of the caller's layout (chte_engine.h) to a ClickHouse column.
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
    info.marks = part.getMarksCount();
}

/// Gives a finished part its final directory name.
void commitPart(IMergeTreeDataPart & part)
{
    part.renameTo(part.name, /* remove_new_dir_if_exists= */ false);
    part.is_temp = false;
}

}

struct chte_writer
{
    chte_table * table = nullptr;
    UInt64 block_number = 0;
    Block header;
    MutableColumns columns;
    std::vector<MergeTreeTemporaryPartPtr> parts;
    bool finished = false;
    bool committed = false;
};

extern "C" int chte_write_begin(chte_table * table, uint64_t block_number, chte_writer ** writer, chte_error * error)
{
    return guarded(error, [&]
    {
        if (block_number == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "block numbers start at 1");
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

extern "C" int chte_write_finish(chte_writer * writer, size_t * nparts, chte_error * error)
{
    return guarded(error, [&]
    {
        if (writer->finished)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "chte_write_finish called twice");
        writer->finished = true;
        auto & table = *writer->table;
        auto & storage = *table.storage;
        Block block = writer->header.cloneWithColumns(std::move(writer->columns));
        auto blocks = MergeTreeDataWriter::splitBlockIntoParts(std::move(block), /* max_parts= */ 0, table.metadata, table.context);
        for (auto & part_block : blocks)
        {
            checkInterrupted();
            /// writeTempPart numbers the part with insert_increment.get() (MergeTreeDataWriter.cpp:695):
            /// every part of this write gets the caller's block number.
            storage.insert_increment.set(writer->block_number - 1);
            auto temp = storage.writer.writeTempPart(part_block, table.metadata, table.context);
            if (!temp->part)
                continue;
            temp->finalize();
            temp->part->getDataPartStorage().commitTransaction();
            writer->parts.push_back(std::move(temp));
        }
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

struct chte_reader
{
    chte_table * table = nullptr;
    QueryPipeline pipeline;
    std::unique_ptr<PullingPipelineExecutor> executor;
    Block block;
    Columns columns;
    std::vector<uint8_t> present;
};

extern "C" int chte_read_begin(chte_table * table, const char * part_name, const char * columns, chte_reader ** reader, chte_error * error)
{
    return guarded(error, [&]
    {
        auto & storage = *table->storage;
        Names names;
        String list = columns ? columns : "";
        boost::split(names, list, [](char c) { return c == ','; });
        for (auto & name : names)
            boost::trim(name);
        std::erase_if(names, [](const String & name) { return name.empty(); });
        if (names.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_read_begin: no columns");

        MergeTreeData::DataPartPtr part = storage.loadPart(part_name);
        auto snapshot = storage.getStorageSnapshot(table->metadata, table->context);
        auto mutations = storage.getMutationsSnapshot({});
        auto alter_conversions = MergeTreeData::getAlterConversionsForPart(part, mutations, table->context);

        Pipe pipe = createMergeTreeSequentialSource(
            MergeTreeSequentialSourceType::Merge, storage, snapshot, RangesInDataPart(part), alter_conversions,
            /* merged_part_offsets= */ nullptr, names, /* mark_ranges= */ std::nullopt,
            std::make_shared<std::atomic<size_t>>(0), /* apply_deleted_mask= */ false,
            /* read_with_direct_io= */ false, /* prefetch= */ false);

        auto r = std::make_unique<chte_reader>();
        r->table = table;
        for (const auto & name : names)
            r->present.push_back(!table->metadata->getColumns().hasPhysical(name) || part->getColumnPosition(name).has_value());
        r->pipeline = QueryPipeline(std::move(pipe));
        r->executor = std::make_unique<PullingPipelineExecutor>(r->pipeline);
        *reader = r.release();
    });
}

extern "C" int chte_read_next(chte_reader * reader, uint32_t * nrows, chte_error * error)
{
    return guarded(error, [&]
    {
        *nrows = 0;
        reader->columns.clear();
        Block block;
        while (reader->executor->pull(block))
        {
            checkInterrupted();
            if (block.rows() == 0)
                continue;
            for (const auto & column : block)
                reader->columns.push_back(column.column->convertToFullColumnIfConst()->convertToFullColumnIfSparse()
                                              ->convertToFullColumnIfLowCardinality());
            *nrows = static_cast<uint32_t>(block.rows());
            return;
        }
    });
}

extern "C" int chte_read_vector(chte_reader * reader, uint32_t i, chte_vector * vector, chte_error * error)
{
    return guarded(error, [&]
    {
        if (i >= reader->columns.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "no column {}", i);
        const IColumn * column = reader->columns[i].get();
        *vector = {};
        if (const auto * nullable = typeid_cast<const ColumnNullable *>(column))
        {
            vector->nulls = reinterpret_cast<const uint8_t *>(nullable->getNullMapData().data());
            column = &nullable->getNestedColumn();
        }
        if (const auto * strings = typeid_cast<const ColumnString *>(column))
        {
            vector->values = reinterpret_cast<const uint8_t *>(strings->getChars().data());
            /// PODArray keeps zeroed padding before its data, so offsets[-1] is 0: the
            /// nrows + 1 offsets of chte_vector are ClickHouse's own (PODArray.h).
            vector->offsets = strings->getOffsets().data() - 1;
            return;
        }
        if (column->isFixedAndContiguous())
        {
            vector->values = column->getRawData().data();
            return;
        }
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "columns of type {} are not supported yet", column->getName());
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

extern "C" int chte_merge(chte_table * table, const char * const * part_names, size_t nparts, chte_part_info * result, chte_error * error)
{
    return guarded(error, [&]
    {
        if (nparts < 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_merge: no parts");
        auto & storage = *table->storage;
        MergeTreeData::DataPartsVector parts;
        for (size_t i = 0; i < nparts; ++i)
            parts.push_back(storage.loadPart(part_names[i]));
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

        auto part = task->getFuture().get();
        part->getDataPartStorage().commitTransaction();
        commitPart(*part);
        fillPartInfo(*part, *result);
    });
}

extern "C" int chte_drop_part(chte_table * table, const char * part_name, chte_error * error)
{
    return guarded(error, [&]
    {
        auto disk = table->storage->getStoragePolicy()->getDisks().at(0);
        disk->removeRecursive(fs::path(table->storage->getRelativeDataPath()) / part_name);
    });
}
