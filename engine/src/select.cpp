/*
 * What a scan reads (ch_table_engines.md §2.8): a filter of the caller's, compiled into an
 * ActionsDAG of ClickHouse's own functions, prunes a part whole by its statistics and its
 * granules by the primary key (KeyCondition and MergeTreeDataSelectExecutor::
 * markRangesFromPKRange), as ClickHouse's ReadFromPart does with a filter
 * (MergeTreeSequentialSource.cpp:446-470); a part's column sizes and its statistics, for
 * the caller's estimates and aggregates.
 *
 * The statistics are checked as ClickHouse's StatisticsPartPruner checks them -- a
 * KeyCondition over the columns that have them, against a hyperrectangle of each
 * column's range -- but with a column's NULLs apart from its values: the pruner widens
 * a Nullable column's range to +inf, where KeyCondition keeps NULL
 * (StatisticsPartPruner.cpp:45-48), so that no upper bound prunes a part of a Nullable
 * column, and every column here is Nullable.  "basic" statistics count a part's NULLs
 * exactly, so a column is the box of its values, [min, max], and, where the part has
 * NULLs, the point NULL; the filter can be true in the part when it can in one of the
 * boxes' products.
 */
#include "internal.h"

#include <Columns/ColumnConst.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/Statistics/Statistics.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

using namespace chte;

struct chte_filter
{
    chte_table * table = nullptr;
    ActionsDAG dag;
    const ActionsDAG::Node * predicate = nullptr;
    std::optional<KeyCondition> key_condition;
    /// the filter over the columns it names that have statistics, in this order
    NamesAndTypesList stats_columns;
    std::optional<KeyCondition> stats_condition;
    /// each part's granules the filter keeps, worked out once
    std::map<String, MarkRanges> selected;
};

namespace
{

const char * functionName(int op)
{
    switch (op)
    {
        case CHTE_FILTER_AND: return "and";
        case CHTE_FILTER_OR: return "or";
        case CHTE_FILTER_NOT: return "not";
        case CHTE_FILTER_EQ: return "equals";
        case CHTE_FILTER_NE: return "notEquals";
        case CHTE_FILTER_LT: return "less";
        case CHTE_FILTER_LE: return "lessOrEquals";
        case CHTE_FILTER_GT: return "greater";
        case CHTE_FILTER_GE: return "greaterOrEquals";
        case CHTE_FILTER_IS_NULL: return "isNull";
        case CHTE_FILTER_IS_NOT_NULL: return "isNotNull";
        default: break;
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "unknown filter operation {}", op);
}

class FilterBuilder
{
public:
    FilterBuilder(chte_filter & filter_, const chte_filter_node * nodes_, size_t nnodes_)
        : filter(filter_), nodes(nodes_), nnodes(nnodes_), context(filter_.table->context)
    {
    }

    const ActionsDAG::Node & build()
    {
        const auto & node = build(0);
        if (next != nnodes)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "a filter of {} nodes has {} more than its tree", nnodes, nnodes - next);
        return node;
    }

private:
    const ActionsDAG::Node & build(size_t depth)
    {
        if (next >= nnodes)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "a filter's tree ends before its nodes");
        if (depth > 256)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "a filter's tree is too deep");
        const chte_filter_node & node = nodes[next++];
        auto & dag = filter.dag;

        switch (node.op)
        {
            case CHTE_FILTER_AND:
            case CHTE_FILTER_OR:
            {
                ActionsDAG::NodeRawConstPtrs children;
                for (uint32_t i = 0; i < node.nargs; ++i)
                    children.push_back(&build(depth + 1));
                if (children.empty())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "AND and OR need an argument");
                if (children.size() == 1)
                    return *children.front();
                return dag.addFunction(function(functionName(node.op)), std::move(children), "");
            }
            case CHTE_FILTER_NOT:
                return dag.addFunction(function("not"), {&build(depth + 1)}, "");
            case CHTE_FILTER_IS_NULL:
            case CHTE_FILTER_IS_NOT_NULL:
                return dag.addFunction(function(functionName(node.op)), {&input(node.column)}, "");
            case CHTE_FILTER_EQ:
            case CHTE_FILTER_NE:
            case CHTE_FILTER_LT:
            case CHTE_FILTER_LE:
            case CHTE_FILTER_GT:
            case CHTE_FILTER_GE:
                if (node.nvalues != 1)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "a comparison takes one value");
                return dag.addFunction(function(functionName(node.op)), {&input(node.column), &constant(node, 0)}, "");
            case CHTE_FILTER_IN:
            {
                /// equalities ORed, which KeyCondition analyses as a set of points
                if (node.nvalues == 0)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "IN takes a value at least");
                const auto & column = input(node.column);
                ActionsDAG::NodeRawConstPtrs children;
                for (uint32_t i = 0; i < node.nvalues; ++i)
                    children.push_back(&dag.addFunction(function("equals"), {&column, &constant(node, i)}, ""));
                if (children.size() == 1)
                    return *children.front();
                return dag.addFunction(function("or"), std::move(children), "");
            }
            default:
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "unknown filter operation {}", node.op);
        }
    }

    FunctionOverloadResolverPtr function(const char * name) { return FunctionFactory::instance().get(name, context); }

    const ActionsDAG::Node & input(const char * name)
    {
        if (!name)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "a filter's leaf names no column");
        auto it = inputs.find(name);
        if (it != inputs.end())
            return *it->second;
        const auto & columns = filter.table->metadata->getColumns();
        if (!columns.hasPhysical(name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "a filter names {}, which is not a column", name);
        const auto & node = filter.dag.addInput(name, columns.getPhysical(name).type);
        inputs.emplace(name, &node);
        return node;
    }

    /// value i of a leaf, a constant of its column's type without Nullable and LowCardinality
    const ActionsDAG::Node & constant(const chte_filter_node & node, uint32_t i)
    {
        const auto & columns = filter.table->metadata->getColumns();
        DataTypePtr type = removeLowCardinalityAndNullable(columns.getPhysical(node.column).type);
        auto column = type->createColumn();
        appendValue(*column, node.values, i);
        auto value = ColumnConst::create(std::move(column), 1);
        return filter.dag.addColumn(std::move(value), type, fmt::format("__chte_value_{}", nconstants++));
    }

    chte_filter & filter;
    const chte_filter_node * nodes;
    size_t nnodes;
    ContextPtr context;
    size_t next = 0;
    size_t nconstants = 0;
    std::map<String, const ActionsDAG::Node *> inputs;
};

/// The boxes of a column's rows in a part, by its statistics: its values, its NULLs.
std::vector<Range> columnBoxes(const Estimates & estimates, const String & name, const DataTypePtr & type)
{
    bool nullable = isNullableOrLowCardinalityNullable(type);
    auto it = estimates.find(name);
    if (it == estimates.end() || !it->second.types.contains(StatisticsType::Basic))
        return {Range::createWholeUniverseTypeAware(type)};
    const auto & e = it->second;
    UInt64 nulls = nullable ? e.estimated_null_count.value_or(e.rows_count) : 0;
    bool maybe_nulls = nullable && (!e.estimated_null_count || nulls > 0);
    std::vector<Range> boxes;
    if (e.rows_count > nulls)
    {
        if (e.estimated_min && e.estimated_max && !(*e.estimated_max < *e.estimated_min))
            boxes.emplace_back(*e.estimated_min, true, *e.estimated_max, true);
        else
            boxes.push_back(Range::createWholeUniverseWithoutNull());
    }
    if (maybe_nulls)
        /// NULL sorts last: KeyCondition's +inf, included
        boxes.emplace_back(POSITIVE_INFINITY, true, POSITIVE_INFINITY, true);
    return boxes;
}

/// Whether a part's statistics leave a row the filter may keep.
bool statisticsCanMatch(chte_filter & filter, const MergeTreeData::DataPartPtr & part)
{
    if (!filter.stats_condition)
        return true;
    auto estimates = part->getEstimates();
    std::vector<std::vector<Range>> boxes;
    DataTypes types;
    size_t combinations = 1;
    for (const auto & [name, type] : filter.stats_columns)
    {
        boxes.push_back(columnBoxes(estimates, name, type));
        types.push_back(type);
        if (boxes.back().empty())
            return false;			/* no rows at all */
        combinations *= boxes.back().size();
    }
    if (combinations > 64)
    {
        /// too many products: each column's hull, as the pruner has it
        for (auto & column : boxes)
            if (column.size() > 1)
                column = {Range(column.front().left, column.front().left_included, POSITIVE_INFINITY, true)};
        combinations = 1;
    }
    Hyperrectangle rectangle;
    for (size_t k = 0; k < combinations; ++k)
    {
        size_t rest = k;
        rectangle.clear();
        for (const auto & column : boxes)
        {
            rectangle.push_back(column[rest % column.size()]);
            rest /= column.size();
        }
        if (filter.stats_condition->checkInHyperrectangle(rectangle, types).can_be_true)
            return true;
    }
    return false;
}

/// The granules of a part the filter keeps, worked out once per part.
const MarkRanges & selectedRanges(chte_filter & filter, const MergeTreeData::DataPartPtr & part)
{
    auto it = filter.selected.find(part->name);
    if (it != filter.selected.end())
        return it->second;

    MarkRanges ranges{MarkRange(0, part->index_granularity->getMarksCountWithoutFinal())};
    bool pruned = !statisticsCanMatch(filter, part);
    if (!pruned && filter.key_condition)
    {
        if (filter.key_condition->alwaysFalse())
            pruned = true;
        else
            ranges = MergeTreeDataSelectExecutor::markRangesFromPKRange(
                RangesInDataPart(part), filter.table->metadata, *filter.key_condition, /* part_offset_condition= */ nullptr,
                /* total_offset_condition= */ nullptr, /* exact_ranges= */ nullptr, /* pk_to_minmax_slot= */ nullptr,
                filter.table->context->getSettingsRef(), getLogger("chte"));
    }
    if (pruned)
        ranges.clear();
    return filter.selected.emplace(part->name, std::move(ranges)).first->second;
}

Names splitNames(const char * list)
{
    Names names;
    String text = list ? list : "";
    boost::split(names, text, [](char c) { return c == ','; });
    for (auto & name : names)
        boost::trim(name);
    std::erase_if(names, [](const String & name) { return name.empty(); });
    return names;
}

}

extern "C" int chte_filter_create(chte_table * table, const chte_filter_node * nodes, size_t nnodes, chte_filter ** filter,
                                  chte_error * error)
{
    return guarded(error, [&]
    {
        if (nnodes == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "a filter needs a node");
        auto f = std::make_unique<chte_filter>();
        f->table = table;
        f->predicate = &FilterBuilder(*f, nodes, nnodes).build();
        f->dag.getOutputs() = {f->predicate};
        auto context = table->context;
        if (table->metadata->hasPrimaryKey())
        {
            ActionsDAGWithInversionPushDown filter_dag(f->predicate, context, /* boolean_context= */ true);
            f->key_condition.emplace(filter_dag, context, table->metadata->getPrimaryKey());
        }
        /// the columns the filter names that have statistics in the table's definition
        const auto & columns = table->metadata->getColumns();
        for (const auto & name : f->dag.getRequiredColumnsNames())
            if (const auto * column = columns.tryGet(name); column && !column->statistics.empty())
                f->stats_columns.emplace_back(name, column->type);
        if (!f->stats_columns.empty())
        {
            ActionsDAGWithInversionPushDown filter_dag(f->predicate, context, /* boolean_context= */ true);
            auto expression = std::make_shared<ExpressionActions>(ActionsDAG(f->stats_columns));
            f->stats_condition.emplace(filter_dag, context, f->stats_columns.getNames(), expression,
                                       /* single_point_= */ false, /* skip_analysis_= */ false, /* require_ready_sets_= */ true);
            if (f->stats_condition->alwaysUnknownOrTrue())
                f->stats_condition.reset();
        }
        *filter = f.release();
    });
}

extern "C" void chte_filter_free(chte_filter * filter)
{
    delete filter;
}

extern "C" int chte_part_select(chte_table * table, const char * part_name, chte_filter * filter, uint32_t begin, uint32_t end,
                                chte_mark_range * ranges, size_t max_ranges, size_t * nranges, uint64_t * rows,
                                chte_error * error)
{
    return guarded(error, [&]
    {
        auto part = table->storage->getPart(part_name);
        size_t marks = part->index_granularity->getMarksCountWithoutFinal();
        size_t last = end == 0 ? marks : std::min<size_t>(end, marks);
        MarkRanges whole{MarkRange(std::min<size_t>(begin, last), last)};
        const MarkRanges & selected = filter ? selectedRanges(*filter, part) : whole;

        size_t n = 0;
        uint64_t total = 0;
        for (const auto & range : selected)
        {
            size_t b = std::max<size_t>(range.begin, begin);
            size_t e = std::min<size_t>(range.end, last);
            if (b >= e)
                continue;
            if (n < max_ranges)
                ranges[n] = chte_mark_range{static_cast<uint32_t>(b), static_cast<uint32_t>(e)};
            ++n;
            total += part->index_granularity->getRowsCountInRange(b, e);
        }
        *nranges = n;
        *rows = total;
    });
}

extern "C" int chte_part_column_bytes(chte_table * table, const char * part_name, const char * columns, uint64_t * compressed,
                                      uint64_t * uncompressed, chte_error * error)
{
    return guarded(error, [&]
    {
        auto part = table->storage->getPart(part_name);
        *compressed = 0;
        *uncompressed = 0;
        for (const auto & name : splitNames(columns))
        {
            auto size = part->getColumnSize(name);
            *compressed += size.data_compressed;
            *uncompressed += size.data_uncompressed;
        }
    });
}

extern "C" int chte_part_stats(chte_table * table, const char * part_name, const char * column, chte_column_stats * stats,
                               chte_error * error)
{
    return guarded(error, [&]
    {
        auto part = table->storage->getPart(part_name);
        *stats = {};
        table->scratch.clear();
        auto estimates = part->getEstimates();
        auto it = estimates.find(column ? column : "");
        /// only "basic" statistics count, whose least and greatest values are exact
        if (it == estimates.end() || !it->second.types.contains(StatisticsType::Basic))
            return;
        const auto & estimate = it->second;
        stats->has_stats = 1;
        stats->rows = estimate.rows_count;
        if (estimate.estimated_null_count)
        {
            stats->has_null_count = 1;
            stats->null_count = *estimate.estimated_null_count;
        }
        else if (!isNullableOrLowCardinalityNullable(table->metadata->getColumns().getPhysical(column).type))
            stats->has_null_count = 1;	/* none can be NULL */
        if (estimate.estimated_min && estimate.estimated_max)
        {
            DataTypePtr type = removeLowCardinalityAndNullable(table->metadata->getColumns().getPhysical(column).type);
            auto min = type->createColumn();
            auto max = type->createColumn();
            min->insert(*estimate.estimated_min);
            max->insert(*estimate.estimated_max);
            table->scratch.push_back(std::move(min));
            table->scratch.push_back(std::move(max));
            stats->min = vectorOf(*table->scratch[0]);
            stats->max = vectorOf(*table->scratch[1]);
            stats->has_minmax = 1;
        }
    });
}
