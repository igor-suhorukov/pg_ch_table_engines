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
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

using namespace chte;


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
    FilterBuilder(ActionsDAG & dag_, const chte_filter_node * nodes_, size_t nnodes_, ContextPtr context_,
                  const FilterColumns & columns_)
        : dag(dag_), nodes(nodes_), nnodes(nnodes_), context(std::move(context_)), columns(columns_)
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
        auto column = columns.input(name);
        const auto & node = dag.addInput(column.name, column.type);
        inputs.emplace(name, &node);
        return node;
    }

    /// value i of a leaf, a constant of its input's type without Nullable and LowCardinality
    const ActionsDAG::Node & constant(const chte_filter_node & node, uint32_t i)
    {
        auto value = columns.constant(node.column, node.values, i);
        return dag.addColumn(ColumnConst::create(std::move(value.column), 1), value.type,
                             fmt::format("__chte_value_{}", nconstants++));
    }

    ActionsDAG & dag;
    const chte_filter_node * nodes;
    size_t nnodes;
    ContextPtr context;
    const FilterColumns & columns;
    size_t next = 0;
    size_t nconstants = 0;
    std::map<String, const ActionsDAG::Node *> inputs;
};

/// An engine's table's columns, by their names and types in its metadata
class TableColumns final : public FilterColumns
{
public:
    explicit TableColumns(const chte_table & table_) : table(table_) { }

    NameAndTypePair input(const String & name) const override
    {
        const auto & columns = table.metadata->getColumns();
        if (!columns.hasPhysical(name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "a filter names {}, which is not a column", name);
        return columns.getPhysical(name);
    }

    ColumnWithTypeAndName constant(const String & name, const chte_vector & values, uint32_t i) const override
    {
        DataTypePtr type = removeLowCardinalityAndNullable(table.metadata->getColumns().getPhysical(name).type);
        auto column = type->createColumn();
        appendValue(*column, values, i);
        return {std::move(column), type, ""};
    }

private:
    const chte_table & table;
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

/// Whether a part's statistics, their estimates, leave a row the filter may keep.
bool statisticsCanMatch(chte_filter & filter, const Estimates & estimates)
{
    if (!filter.stats_condition)
        return true;
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
    bool pruned = filter.stats_condition
        && !statisticsCanMatch(filter, filter.table->storage->partEstimates(part, filter.stats_columns.getNames()));
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

/// A column's statistics for the caller, from their estimate, where it has one.
void fillStats(chte_table & table, const Estimate * estimate, const char * column, chte_column_stats * stats)
{
    *stats = {};
    table.scratch.clear();
    /// only "basic" statistics count, whose least and greatest values are exact
    if (!estimate || !estimate->types.contains(StatisticsType::Basic))
        return;
    stats->has_stats = 1;
    stats->rows = estimate->rows_count;
    if (estimate->estimated_null_count)
    {
        stats->has_null_count = 1;
        stats->null_count = *estimate->estimated_null_count;
    }
    else if (!isNullableOrLowCardinalityNullable(table.metadata->getColumns().getPhysical(column).type))
        stats->has_null_count = 1;	/* none can be NULL */
    if (estimate->estimated_min && estimate->estimated_max)
    {
        DataTypePtr type = removeLowCardinalityAndNullable(table.metadata->getColumns().getPhysical(column).type);
        auto min = type->createColumn();
        auto max = type->createColumn();
        min->insert(*estimate->estimated_min);
        max->insert(*estimate->estimated_max);
        table.scratch.push_back(std::move(min));
        table.scratch.push_back(std::move(max));
        stats->min = vectorOf(*table.scratch[0]);
        stats->max = vectorOf(*table.scratch[1]);
        stats->has_minmax = 1;
    }
}

/*
 * A summary's bytes: a format byte; the number of columns; for each, its name, its compressed
 * and uncompressed bytes, and whether it has statistics -- if so, their estimate: its types as
 * bits, its rows, which of its optional members it has, and those, each count a varint and each
 * least and greatest value ClickHouse's binary Field.
 */
constexpr UInt8 SUMMARY_FORMAT = 1;
enum : UInt8
{
    HAS_CARDINALITY = 1,
    HAS_MIN = 2,
    HAS_MAX = 4,
    HAS_NULL_COUNT = 8,
    HAS_DEFAULT_COUNT = 16,
};

}

namespace chte
{

const ActionsDAG::Node & buildFilter(ActionsDAG & dag, const chte_filter_node * nodes, size_t nnodes, ContextPtr context,
                                     const FilterColumns & columns)
{
    return FilterBuilder(dag, nodes, nnodes, std::move(context), columns).build();
}

String summarizePart(const IMergeTreeDataPart & part)
{
    Names names = part.getColumns().getNames();
    auto statistics = part.loadStatistics(names);
    WriteBufferFromOwnString out;
    writeBinary(SUMMARY_FORMAT, out);
    writeVarUInt(names.size(), out);
    for (const auto & name : names)
    {
        auto size = columnSize(part, name);
        writeStringBinary(name, out);
        writeVarUInt(size.data_compressed, out);
        writeVarUInt(size.data_uncompressed, out);
        auto it = statistics.find(name);
        writeBinary(UInt8(it != statistics.end()), out);
        if (it == statistics.end())
            continue;
        Estimate e = it->second->getEstimate();
        UInt64 types = 0;
        for (auto type : e.types)
            types |= UInt64(1) << static_cast<UInt8>(type);
        UInt8 has = (e.estimated_cardinality ? HAS_CARDINALITY : 0) | (e.estimated_min ? HAS_MIN : 0)
            | (e.estimated_max ? HAS_MAX : 0) | (e.estimated_null_count ? HAS_NULL_COUNT : 0)
            | (e.estimated_default_count ? HAS_DEFAULT_COUNT : 0);
        writeVarUInt(types, out);
        writeVarUInt(e.rows_count, out);
        writeBinary(has, out);
        if (e.estimated_cardinality)
            writeVarUInt(*e.estimated_cardinality, out);
        if (e.estimated_min)
            writeFieldBinary(*e.estimated_min, out);
        if (e.estimated_max)
            writeFieldBinary(*e.estimated_max, out);
        if (e.estimated_null_count)
            writeVarUInt(*e.estimated_null_count, out);
        if (e.estimated_default_count)
            writeVarUInt(*e.estimated_default_count, out);
    }
    out.finalize();
    return std::move(out.str());
}

std::shared_ptr<const PartSummary> parseSummary(const void * data, size_t size)
{
    if (!data || size == 0)
        return nullptr;
    ReadBufferFromMemory in(static_cast<const char *>(data), size);
    UInt8 format;
    readBinary(format, in);
    if (format != SUMMARY_FORMAT)
        return nullptr;
    auto summary = std::make_shared<PartSummary>();
    UInt64 ncolumns;
    readVarUInt(ncolumns, in);
    for (UInt64 i = 0; i < ncolumns; ++i)
    {
        String name;
        ColumnSize size_of;
        UInt8 has_estimate;
        readStringBinary(name, in);
        readVarUInt(size_of.data_compressed, in);
        readVarUInt(size_of.data_uncompressed, in);
        summary->sizes.emplace(name, size_of);
        readBinary(has_estimate, in);
        if (!has_estimate)
            continue;
        Estimate e;
        UInt64 types;
        UInt8 has;
        readVarUInt(types, in);
        for (UInt8 t = 0; t < 64; ++t)
            if (types & (UInt64(1) << t))
                e.types.insert(static_cast<StatisticsType>(t));
        readVarUInt(e.rows_count, in);
        readBinary(has, in);
        UInt64 count;
        if (has & HAS_CARDINALITY)
        {
            readVarUInt(count, in);
            e.estimated_cardinality = count;
        }
        if (has & HAS_MIN)
            e.estimated_min = readFieldBinary(in);
        if (has & HAS_MAX)
            e.estimated_max = readFieldBinary(in);
        if (has & HAS_NULL_COUNT)
        {
            readVarUInt(count, in);
            e.estimated_null_count = count;
        }
        if (has & HAS_DEFAULT_COUNT)
        {
            readVarUInt(count, in);
            e.estimated_default_count = count;
        }
        summary->estimates.emplace(std::move(name), std::move(e));
    }
    return summary;
}

}

namespace
{

/// The summary a caller hands with a part's name; nullptr where there is none of this format.
std::shared_ptr<const PartSummary> summaryOf(chte_table & table, const char * part_name, const void * summary, size_t size)
{
    if (!part_name || !summary || size == 0)
        return nullptr;
    return table.storage->partSummary(part_name, summary, size);
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
        if (table->external)
        {
            /// an external table's: each file's DAG is built over its names and types, as it is read (external.cpp)
            keepExternalFilter(*f, nodes, nnodes);
            *filter = f.release();
            return;
        }
        f->predicate = &FilterBuilder(f->dag, nodes, nnodes, table->context, TableColumns(*table)).build();
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
        auto size = table->storage->columnsSize(part_name, splitNames(columns));
        *compressed = size.data_compressed;
        *uncompressed = size.data_uncompressed;
    });
}

extern "C" int chte_part_stats(chte_table * table, const char * part_name, const char * column, chte_column_stats * stats,
                               chte_error * error)
{
    return guarded(error, [&]
    {
        auto part = table->storage->getPart(part_name);
        auto estimates = table->storage->partEstimates(part, {column ? column : ""});
        auto it = estimates.find(column ? column : "");
        fillStats(*table, it == estimates.end() ? nullptr : &it->second, column, stats);
    });
}

extern "C" int chte_summary_column_bytes(chte_table * table, const char * part_name, const void * summary, size_t size,
                                         const char * columns, uint64_t * compressed, uint64_t * uncompressed, int * found,
                                         chte_error * error)
{
    return guarded(error, [&]
    {
        *found = 0;
        *compressed = 0;
        *uncompressed = 0;
        auto s = summaryOf(*table, part_name, summary, size);
        if (!s)
            return;
        uint64_t c = 0;
        uint64_t u = 0;
        for (const auto & name : splitNames(columns))
        {
            auto it = s->sizes.find(name);
            if (it == s->sizes.end())
                return;			/* a column the part was written without: the part's to tell */
            c += it->second.data_compressed;
            u += it->second.data_uncompressed;
        }
        *compressed = c;
        *uncompressed = u;
        *found = 1;
    });
}

extern "C" int chte_summary_stats(chte_table * table, const char * part_name, const void * summary, size_t size,
                                  const char * column, chte_column_stats * stats, int * found, chte_error * error)
{
    return guarded(error, [&]
    {
        *found = 0;
        *stats = {};
        auto s = summaryOf(*table, part_name, summary, size);
        if (!s || !column || !s->sizes.contains(column))
            return;
        auto it = s->estimates.find(column);
        fillStats(*table, it == s->estimates.end() ? nullptr : &it->second, column, stats);
        *found = 1;
    });
}

extern "C" int chte_filter_summary_select(chte_filter * filter, const char * part_name, const void * summary, size_t size,
                                          int * verdict, chte_error * error)
{
    return guarded(error, [&]
    {
        /// as selectedRanges() decides, but for the granules the key keeps, which only the part's index tells
        *verdict = CHTE_SELECT_RANGES;
        if (filter->key_condition && filter->key_condition->alwaysFalse())
        {
            *verdict = CHTE_SELECT_NONE;
            return;
        }
        if (filter->stats_condition)
        {
            auto s = summaryOf(*filter->table, part_name, summary, size);
            if (!s)
                return;
            for (const auto & name : filter->stats_columns.getNames())
                if (!s->sizes.contains(name))
                    return;		/* a column the part was written without */
            if (!statisticsCanMatch(*filter, s->estimates))
            {
                *verdict = CHTE_SELECT_NONE;
                return;
            }
        }
        if (!filter->key_condition || filter->key_condition->alwaysUnknownOrTrue())
            *verdict = CHTE_SELECT_ALL;
    });
}
