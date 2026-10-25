/*
 * Rows by their row ids (ch_table_engines.md §2.6, Appendix E).  Every table has the
 * projection _pg_rowid_index, SELECT _pg_rowid, _part_offset ORDER BY _pg_rowid, whose part
 * keeps each row's offset in its parent part as _parent_part_offset
 * (ProjectionsDescription.cpp:501-509), rebuilt by every merge (MergeTask.cpp:3213-3222).  Its
 * primary index holds the first row id of each of its granules: a row id lies in the last
 * granule whose first id is not greater, and those granules alone are read.  Then the rows
 * at the offsets found: the parent part's granules holding them, the rest of their rows left
 * out by _part_offset (io.cpp).  Their marks are kept in the process's mark cache (VC33), which
 * a merge's sequential source only looks in.  Or, rows of one granule (VC34), each value read
 * where it lies from the granule's marks the caller keeps for every process: a fetch of a row
 * no longer makes a whole granule of every column (chte_read_granule_rows).
 */
#include "internal.h"

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/ReadSettings.h>
#include <IO/VarInt.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/RangesInDataPart.h>

#include <algorithm>
#include <optional>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
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

/// VC34: the sparse serialization's offsets' flag on a granule's last varint, its rows of the default after
/// the granule's last value (SerializationSparse.cpp, END_OF_GRANULE_FLAG)
constexpr UInt64 END_OF_GRANULE = 1ULL << 62;

/// VC34: a column's stream over one granule, read forward from the granule's mark: its compressed blocks
/// from the block the mark names, decompressed as the reads reach them, the first past the mark's offset in it
struct GranuleStream
{
    std::unique_ptr<CompressedReadBufferFromFile> in;
    uint64_t at = 0;    /// the granule's bytes of the stream passed so far

    GranuleStream(const IMergeTreeDataPart & part, const chte_part_stream & s)
    {
        std::optional<size_t> hint;
        if (s.to > s.from)
            hint = static_cast<size_t>(s.to - s.from);
        in = std::make_unique<CompressedReadBufferFromFile>(part.getDataPartStorage().readFile(s.file, ReadSettings{}, hint));
        in->seek(s.from, s.skip);
    }

    void skipTo(uint64_t pos)
    {
        if (pos < at)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "a granule's stream read back from byte {} to {}", at, pos);
        in->ignore(pos - at);
        at = pos;
    }

    void read(char * to, size_t n)
    {
        in->readStrict(to, n);
        at += n;
    }
};

/// VC34: a column of one granule decoded at its rows, ascending, as pg_accel's W6 decodes its streams on the
/// device (units_mergetree.cpp): a fixed width's value at its place; a String's sizes, a uint64 each, walked
/// to it and its bytes there; a Nullable's map, a byte each, 1 a NULL; the sparse serialization's offsets,
/// each the rows of the default before the next value, its values' streams holding the other rows' alone
struct GranuleColumn
{
    String name;
    DataTypePtr type;
    bool nullable = false;
    bool string = false;
    size_t width = 0;
    bool sparse_kind = false;
    /// the streams given for it: their places among the caller's
    std::optional<size_t> values_at, sizes_at, nulls_at, sparse_at;
    std::optional<GranuleStream> values, sizes, nulls, sparse;
    /// the granule's rows; the sparse offsets read so far: the rows they place, the row of the next value,
    /// UINT64_MAX past the last, and its place among the values
    uint64_t granule_rows = 0;
    bool started = false;
    uint64_t placed = 0;
    uint64_t next_row = 0;
    uint64_t next_value = 0;
    uint64_t seen = 0;
    /// a String's values' sizes read, and the bytes of those values
    uint64_t sizes_read = 0;
    uint64_t chars_before = 0;
    std::string scratch;

    /// The next value's row and place.  A granule a writer's block ended in is written in two calls or more,
    /// each with its offsets' flagged last varint (MergeTreeDataPartWriterWide's granule not marked on its
    /// start): the varints read on past each until the granule's rows are placed, as ClickHouse's reader
    /// reads them (deserializeOffsets()) and pg_accel's device does
    void nextSparse()
    {
        started = true;
        while (placed < granule_rows)
        {
            UInt64 v = 0;
            readVarUInt(v, *sparse->in);
            if (v & END_OF_GRANULE)
            {
                placed += v & ~END_OF_GRANULE;
                continue;
            }
            next_row = placed + v;
            next_value = seen++;
            placed = next_row + 1;
            return;
        }
        next_row = UINT64_MAX;
    }

    /// Row i of the granule appended to col
    void append(IColumn & col, uint64_t i)
    {
        uint64_t k = i;
        if (sparse)
        {
            if (!started)
                nextSparse();
            while (next_row != UINT64_MAX && next_row < i)
                nextSparse();
            /// the default: 0, '' or, a Nullable's, NULL
            if (next_row != i)
            {
                col.insertDefault();
                return;
            }
            k = next_value;
        }
        if (nulls)
        {
            char null = 0;
            nulls->skipTo(i);
            nulls->read(&null, 1);
            if (null)
            {
                col.insertDefault();
                return;
            }
        }
        if (string)
        {
            UInt64 size = 0;
            while (sizes_read < k)
            {
                sizes->read(reinterpret_cast<char *>(&size), sizeof(size));
                chars_before += size;
                ++sizes_read;
            }
            sizes->read(reinterpret_cast<char *>(&size), sizeof(size));
            ++sizes_read;
            values->skipTo(chars_before);
            scratch.resize(size);
            values->read(scratch.data(), size);
            chars_before += size;
        }
        else
        {
            values->skipTo(k * width);
            scratch.resize(width);
            values->read(scratch.data(), width);
        }
        if (nullable)
        {
            auto & column = assert_cast<ColumnNullable &>(col);
            column.getNestedColumn().insertData(scratch.data(), scratch.size());
            column.getNullMapData().push_back(static_cast<UInt8>(0));
        }
        else
            col.insertData(scratch.data(), scratch.size());
    }
};

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

        /// VC21: the row id's projection, its parts' loaded
        describeProjections(*table);
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

        /// VC33: the projection's columns' marks kept in the mark cache, which its sources then find
        warmMarks(*table, projection_part, {ROWID_COLUMN, "_parent_part_offset"}, std::make_shared<AlterConversions>());
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

        /// VC33: the columns' marks kept in the mark cache, which the reader then finds
        warmMarks(*table, part, names,
                  MergeTreeData::getAlterConversionsForPart(part, storage.getMutationsSnapshot({}), table->context));
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

extern "C" int chte_read_granule_rows(chte_table * table, const char * part_name, const char * columns, uint32_t granule,
                                      const uint64_t * offsets, size_t n, const chte_part_stream * streams, size_t nstreams,
                                      chte_reader ** reader, chte_error * error)
{
    return guarded(error, [&]
    {
        *reader = nullptr;
        auto & storage = *table->storage;
        Names names = columnNames(columns);
        auto part = storage.getPart(part_name);
        if (n == 0 || !isWidePart(part))
            return;
        const auto & granularity = *part->index_granularity;
        if (granule >= granularity.getMarksCountWithoutFinal())
            return;
        size_t first = granularity.getMarkStartingRow(granule);
        size_t rows = granularity.getMarkRows(granule);
        for (size_t i = 0; i < n; ++i)
            if (offsets[i] < first || offsets[i] >= first + rows || (i > 0 && offsets[i] <= offsets[i - 1]))
                return;

        /// each column one the device decodes too (chte_part_streams_of), of the table's type, its streams
        /// those its serialization has, each given once
        std::vector<GranuleColumn> cols(names.size());
        const auto & table_columns = table->metadata->getColumns();
        for (size_t c = 0; c < names.size(); ++c)
        {
            auto & gc = cols[c];
            auto column = part->getColumns().tryGetByName(names[c]);
            if (!column || !table_columns.hasPhysical(names[c]) || !table_columns.getPhysical(names[c]).type->equals(*column->type))
                return;
            gc.name = names[c];
            gc.type = column->type;
            gc.granule_rows = rows;
            gc.nullable = column->type->isNullable();
            DataTypePtr nested = removeNullable(column->type);
            gc.string = isString(nested);
            if (!gc.string)
            {
                if (!nested->isValueRepresentedByNumber() || nested->lowCardinality())
                    return;
                gc.width = nested->getSizeOfValueInMemory();
            }
            auto kinds = part->getSerialization(names[c])->getKindStack();
            gc.sparse_kind = kinds.size() == 2 && kinds[0] == ISerialization::Kind::DEFAULT && kinds[1] == ISerialization::Kind::SPARSE;
            if (!gc.sparse_kind && (kinds.size() != 1 || kinds[0] != ISerialization::Kind::DEFAULT))
                return;
            for (size_t s = 0; s < nstreams; ++s)
            {
                const chte_part_stream & st = streams[s];
                if (st.column != static_cast<int32_t>(c))
                    continue;
                std::optional<size_t> * slot;
                switch (st.kind)
                {
                    case CHTE_STREAM_VALUES:
                        if (!gc.string && st.width != static_cast<int32_t>(gc.width))
                            return;
                        slot = &gc.values_at;
                        break;
                    case CHTE_STREAM_SIZES:
                        slot = &gc.sizes_at;
                        break;
                    case CHTE_STREAM_NULLS:
                        slot = &gc.nulls_at;
                        break;
                    case CHTE_STREAM_SPARSE:
                    case CHTE_STREAM_SPARSE_NULLS:
                        if ((st.kind == CHTE_STREAM_SPARSE_NULLS) != gc.nullable)
                            return;
                        slot = &gc.sparse_at;
                        break;
                    default:
                        return;
                }
                if (slot->has_value())
                    return;
                *slot = s;
            }
            if (!gc.values_at || gc.string != gc.sizes_at.has_value() || gc.sparse_kind != gc.sparse_at.has_value()
                || gc.nulls_at.has_value() != (gc.nullable && !gc.sparse_kind))
                return;
        }

        /// the values, each column's rows in turn, its streams opened at the granule's marks
        Block block;
        for (auto & gc : cols)
        {
            gc.values.emplace(*part, streams[*gc.values_at]);
            if (gc.sizes_at)
                gc.sizes.emplace(*part, streams[*gc.sizes_at]);
            if (gc.nulls_at)
                gc.nulls.emplace(*part, streams[*gc.nulls_at]);
            if (gc.sparse_at)
                gc.sparse.emplace(*part, streams[*gc.sparse_at]);
            auto col = gc.type->createColumn();
            col->reserve(n);
            for (size_t i = 0; i < n; ++i)
                gc.append(*col, offsets[i] - first);
            checkInterrupted();
            block.insert(ColumnWithTypeAndName(std::move(col), gc.type, gc.name));
            gc.values.reset();
            gc.sizes.reset();
            gc.nulls.reset();
            gc.sparse.reset();
        }
        auto r = std::make_unique<chte_reader>();
        r->table = table;
        r->present.assign(names.size(), 1);
        r->next_block = [rows_block = std::move(block), done = false](Block & out) mutable
        {
            if (done)
                return false;
            out = std::move(rows_block);
            done = true;
            return true;
        };
        *reader = r.release();
    });
}

extern "C" int chte_part_granule_of(chte_table * table, const char * part_name, uint64_t offset, uint32_t * granule,
                                    uint64_t * first, uint64_t * rows, chte_error * error)
{
    return guarded(error, [&]
    {
        auto part = table->storage->getPart(part_name);
        const auto & granularity = *part->index_granularity;
        if (offset >= part->rows_count)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_part_granule_of: no row {} in part {}", offset, part_name);
        MarkRange range = granularity.getMarkRangeForRowOffset(offset);
        *granule = static_cast<uint32_t>(range.begin);
        *first = granularity.getMarkStartingRow(range.begin);
        *rows = granularity.getMarkRows(range.begin);
    });
}
