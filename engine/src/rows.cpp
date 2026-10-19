/*
 * Rows by their row ids (ch_table_engines.md §2.6, Appendix E).  Every table has the
 * projection _pg_rowid_index, SELECT _pg_rowid, _part_offset ORDER BY _pg_rowid, whose part
 * keeps each row's offset in its parent part as _parent_part_offset
 * (ProjectionsDescription.cpp:501-509), rebuilt by every merge (MergeTask.cpp:3213-3222).  Its
 * primary index holds the first row id of each of its granules: a row id lies in the last
 * granule whose first id is not greater, and those granules alone are read.  Then the rows
 * at the offsets found: the parent part's granules holding them, the rest of their rows left
 * out by _part_offset (io.cpp).
 */
#include "internal.h"

#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/RangesInDataPart.h>

#include <algorithm>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

using namespace chte;

namespace
{

constexpr const char * ROWID_COLUMN = "_pg_rowid";
constexpr const char * ROWID_PROJECTION = "_pg_rowid_index";

/// Pulls every block of a pipe, handing each to f.
template <typename F>
void pullAll(Pipe pipe, F && f)
{
    QueryPipeline pipeline(std::move(pipe));
    PullingPipelineExecutor executor(pipeline);
    Block block;
    while (executor.pull(block))
    {
        checkInterrupted();
        if (block.rows() > 0)
            f(block);
    }
}

/// Finds the wanted ids (sorted) among a block's ids and offsets: both ids ascending.
void match(const PaddedPODArray<UInt64> & ids, const PaddedPODArray<UInt64> & offsets, const uint64_t * wanted, size_t n,
           uint64_t * found)
{
    const UInt64 * begin = ids.data();
    const UInt64 * end = ids.data() + ids.size();
    for (size_t i = 0; i < n; ++i)
    {
        const UInt64 * it = std::lower_bound(begin, end, wanted[i]);
        if (it != end && *it == wanted[i])
            found[i] = offsets[it - ids.data()];
    }
}

/// A part with no projection to search (it was not written, or is broken): its row ids, whole.
void locateByScan(chte_table & table, const MergeTreeData::DataPartPtr & part, const uint64_t * rowids, size_t n,
                  uint64_t * offsets)
{
    auto reader = openReader(table, part, {ROWID_COLUMN, "_part_offset"}, std::nullopt);
    std::vector<std::pair<UInt64, UInt64>> all;
    Block block;
    while (reader->pull(block))
    {
        checkInterrupted();
        auto ids_column = block.getByPosition(0).column->convertToFullIfWrapped();
        auto offsets_column = block.getByPosition(1).column->convertToFullIfWrapped();
        const auto & ids = typeid_cast<const ColumnUInt64 &>(*ids_column).getData();
        const auto & offs = typeid_cast<const ColumnUInt64 &>(*offsets_column).getData();
        for (size_t i = 0; i < ids.size(); ++i)
            all.emplace_back(ids[i], offs[i]);
    }
    std::sort(all.begin(), all.end());
    for (size_t i = 0; i < n; ++i)
    {
        auto it = std::lower_bound(all.begin(), all.end(), std::make_pair(rowids[i], UInt64(0)));
        if (it != all.end() && it->first == rowids[i])
            offsets[i] = it->second;
    }
}

}

extern "C" int chte_part_locate(chte_table * table, const char * part_name, const uint64_t * rowids, size_t n,
                                uint64_t * offsets, chte_error * error)
{
    return guarded(error, [&]
    {
        std::fill(offsets, offsets + n, UINT64_MAX);
        if (n == 0)
            return;
        for (size_t i = 1; i < n; ++i)
            if (rowids[i] < rowids[i - 1])
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_part_locate: row ids not in order");

        auto & storage = *table->storage;
        auto part = storage.partWithProjections(part_name);
        const auto & projections = part->getProjectionParts();
        auto it = projections.find(ROWID_PROJECTION);
        if (it == projections.end() || it->second->is_broken || !table->metadata->getProjections().has(ROWID_PROJECTION))
        {
            locateByScan(*table, part, rowids, n, offsets);
            return;
        }
        const auto & projection_part = it->second;
        const auto & projection = table->metadata->getProjections().get(ROWID_PROJECTION);

        /// the granules of the projection that may hold the ids: the last one starting at or below each
        size_t marks = projection_part->index_granularity->getMarksCountWithoutFinal();
        if (marks == 0)
            return;
        auto index = projection_part->getIndex();
        if (!index || index->empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "part {} has no index of its {} projection", part_name, ROWID_PROJECTION);
        auto first_ids_column = index->at(0)->convertToFullIfWrapped();
        const auto & first_ids = typeid_cast<const ColumnUInt64 &>(*first_ids_column).getData();
        MarkRanges ranges;
        for (size_t i = 0; i < n; ++i)
        {
            const UInt64 * begin = first_ids.data();
            const UInt64 * end = first_ids.data() + std::min(marks, first_ids.size());
            const UInt64 * up = std::upper_bound(begin, end, rowids[i]);
            if (up == begin)
                continue;
            size_t g = static_cast<size_t>(up - begin) - 1;
            if (!ranges.empty() && ranges.back().end >= g)
                ranges.back().end = std::max(ranges.back().end, g + 1);
            else
                ranges.push_back(MarkRange(g, g + 1));
        }
        if (ranges.empty())
            return;

        /// a source a run of adjacent granules (markRuns)
        auto snapshot = storage.getStorageSnapshot(projection.metadata, table->context);
        for (auto & run : markRuns(std::move(ranges)))
        {
            Pipe pipe = createMergeTreeSequentialSource(
                MergeTreeSequentialSourceType::Merge, storage, snapshot, RangesInDataPart(projection_part),
                std::make_shared<AlterConversions>(), /* merged_part_offsets= */ nullptr,
                {ROWID_COLUMN, "_parent_part_offset"}, std::move(run), std::make_shared<std::atomic<size_t>>(0),
                /* apply_deleted_mask= */ false, /* read_with_direct_io= */ false, /* prefetch= */ false);
            pullAll(std::move(pipe), [&](const Block & block)
            {
                /// a part's few rows may have them sparse: offset 0, the default, is most of a one-row part's
                auto ids_column = block.getByPosition(0).column->convertToFullIfWrapped();
                auto offsets_column = block.getByPosition(1).column->convertToFullIfWrapped();
                match(typeid_cast<const ColumnUInt64 &>(*ids_column).getData(),
                      typeid_cast<const ColumnUInt64 &>(*offsets_column).getData(), rowids, n, offsets);
            });
        }
    });
}

extern "C" int chte_read_rows(chte_table * table, const char * part_name, const char * columns, const uint64_t * offsets,
                              size_t n, chte_reader ** reader, chte_error * error)
{
    return guarded(error, [&]
    {
        auto & storage = *table->storage;
        Names names = columnNames(columns);
        auto part = storage.getPart(part_name);
        const auto & granularity = *part->index_granularity;

        std::vector<UInt64> wanted(offsets, offsets + n);
        for (size_t i = 1; i < wanted.size(); ++i)
            if (wanted[i] <= wanted[i - 1])
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_read_rows: offsets not ascending");
        MarkRanges ranges;
        for (UInt64 offset : wanted)
        {
            if (offset >= part->rows_count)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_read_rows: no row {} in part {}", offset, part_name);
            MarkRange range = granularity.getMarkRangeForRowOffset(offset);
            if (!ranges.empty() && ranges.back().end >= range.begin)
                ranges.back().end = std::max(ranges.back().end, range.end);
            else
                ranges.push_back(range);
        }

        names.push_back("_part_offset");
        std::optional<MarkRanges> mark_ranges;
        if (!ranges.empty())
            mark_ranges = std::move(ranges);
        else
            mark_ranges.emplace();
        auto r = openReader(*table, part, names, std::move(mark_ranges));
        r->present.pop_back();
        r->wanted = std::move(wanted);
        r->by_offset = true;
        *reader = r.release();
    });
}
