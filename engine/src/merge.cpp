/*
 * Merges (ch_table_engines.md §2.11): which parts to merge, as ClickHouse's own merge selector
 * chooses them for a StorageMergeTree (StorageMergeTree.cpp, selectPartsToMerge(): a
 * MergeSelectorApplier over the parts, bounded by CompactionStatistics' largest merge), from
 * the parts PostgreSQL's catalog lists; and how long an insert waits when merges fall behind,
 * as MergeTreeData::delayInsertOrThrowIfNeeded() decides for active parts.  The merge itself is
 * io.cpp's chte_merge.
 */
#include "internal.h"

#include <Common/Exception.h>
#include <Common/formatReadable.h>
#include <Storages/MergeTree/Compaction/CompactionStatistics.h>
#include <Storages/MergeTree/Compaction/MergePredicates/IMergePredicate.h>
#include <Storages/MergeTree/Compaction/MergeSelectorApplier.h>
#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <Storages/MergeTree/Compaction/PartitionStatistics.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

#include <algorithm>
#include <ctime>

namespace DB::MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 parts_to_delay_insert;
    extern const MergeTreeSettingsUInt64 parts_to_throw_insert;
    extern const MergeTreeSettingsUInt64 max_parts_in_total;
    extern const MergeTreeSettingsUInt64 max_avg_part_size_for_too_many_parts;
    extern const MergeTreeSettingsUInt64 max_delay_to_insert;
    extern const MergeTreeSettingsUInt64 min_delay_to_insert_ms;
}

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int TOO_MANY_PARTS;
}

using namespace chte;

namespace
{

/// Any parts of the list may merge: the caller lists only those no other merge holds.
class AnyMergePredicate final : public IMergePredicate
{
public:
    std::expected<void, PreformattedMessage> canMergeParts(const PartProperties &, const PartProperties &) const override
    {
        return {};
    }
    PartsRange getPatchesToApplyOnMerge(const PartsRange &) const override { return {}; }
};

}

extern "C" int chte_merge_select(chte_table * table, const chte_merge_candidate * parts, size_t n, int aggressive,
                                 size_t * first, size_t * count, chte_error * error)
{
    return guarded(error, [&]
    {
        *first = 0;
        *count = 0;
        if (n < 2)
            return;
        auto & storage = *table->storage;
        time_t now = time(nullptr);

        PartsRange range;
        range.reserve(n);
        for (size_t i = 0; i < n; ++i)
        {
            auto info = MergeTreePartInfo::fromPartName(parts[i].name, storage.format_version);
            /// in order; not disjoint: a block number an insert took is its part's only once it commits, so a
            /// merge may span one whose part comes later (chte_merge names it apart by its version)
            if (!range.empty() && !(range.back().info < info))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_merge_select: parts {} and {} are not in block order",
                                range.back().name, parts[i].name);
            range.push_back(PartProperties{
                .name = parts[i].name,
                .info = info,
                .all_ttl_calculated_if_any = true,
                .size = parts[i].bytes,
                .age = static_cast<time_t>(parts[i].age_seconds),
                .rows = parts[i].rows,
            });
        }

        /// the partition's statistics, as MergeTreeDataMergerMutator's calculateStatisticsForPartitions()
        PartitionsStatistics stats;
        auto & partition = stats[range.front().info.getPartitionId()];
        for (const auto & p : range)
        {
            partition.part_count += 1;
            partition.min_age = std::min(partition.min_age, p.age);
            partition.total_size += p.size;
        }

        UInt64 max_bytes = CompactionStatistics::getMaxSourcePartsBytesForMerge(storage);
        UInt64 max_rows = CompactionStatistics::getMaxResultPartRowsCount(storage);
        if (max_bytes == 0)
            return;
        MergeSelectorApplier selector(
            /* merge_constraints= */ {{max_bytes, max_rows}}, /* merge_with_ttl_allowed= */ false, aggressive != 0,
            /* range_filter= */ nullptr, storage.getStorageID());
        AnyMergePredicate predicate;
        PartsRanges ranges;
        ranges.push_back(std::move(range));
        auto choices = selector.chooseMergesFrom(ranges, stats, predicate, table->metadata, storage.getSettings(),
                                                 /* next_delete_times= */ {}, /* next_recompress_times= */ {},
                                                 /* can_use_ttl_merges= */ false, now);
        if (choices.empty() || choices.front().range.size() < 2)
            return;
        const auto & chosen = choices.front().range;
        for (size_t i = 0; i < n; ++i)
            if (chosen.front().name == parts[i].name)
            {
                *first = i;
                *count = chosen.size();
                break;
            }
    });
}

extern "C" int chte_insert_delay(chte_table * table, size_t parts, uint64_t bytes, uint64_t * delay_ms, chte_error * error)
{
    return guarded(error, [&]
    {
        *delay_ms = 0;
        auto settings = table->storage->getSettings();
        UInt64 to_delay = (*settings)[MergeTreeSetting::parts_to_delay_insert];
        UInt64 to_throw = (*settings)[MergeTreeSetting::parts_to_throw_insert];
        UInt64 max_avg = (*settings)[MergeTreeSetting::max_avg_part_size_for_too_many_parts];
        UInt64 average = parts ? bytes / parts : 0;
        bool large_enough = max_avg && average > max_avg;

        if (parts >= (*settings)[MergeTreeSetting::max_parts_in_total])
            throw Exception(ErrorCodes::TOO_MANY_PARTS, "Too many parts ({}) in the table: merges are processing significantly "
                            "slower than inserts", parts);
        if (to_throw && parts >= to_throw && !large_enough)
            throw Exception(ErrorCodes::TOO_MANY_PARTS, "Too many parts ({} with average size of {}) in the table: merges are "
                            "processing significantly slower than inserts", parts, ReadableSize(average));
        if (!to_delay || parts < to_delay || large_enough)
            return;

        /// if parts == parts_to_delay_insert, one part over the threshold
        UInt64 over = parts - to_delay + 1;
        UInt64 allowed = to_throw > to_delay ? to_throw - to_delay : 1;
        UInt64 max_delay = (*settings)[MergeTreeSetting::max_delay_to_insert] > 0
            ? (*settings)[MergeTreeSetting::max_delay_to_insert] * 1000 : 1000;
        if (over >= allowed)
            *delay_ms = max_delay;
        else
            *delay_ms = std::max<UInt64>((*settings)[MergeTreeSetting::min_delay_to_insert_ms],
                                         static_cast<UInt64>(static_cast<double>(max_delay) * static_cast<double>(over)
                                                             / static_cast<double>(allowed)));
    });
}
