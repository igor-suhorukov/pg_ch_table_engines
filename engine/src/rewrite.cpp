/*
 * Folding deletions into a part (ch_table_engines.md §2.10): the part read and written again
 * without the rows every snapshot sees deleted, as a new part, by the reader and the writer
 * every scan and insert use, on the calling thread.  ClickHouse's own lightweight delete,
 * MutateTask with `_row_exists = 0`, first asks whether the mutation touches the part by a
 * query it runs on a thread of the global pool (MutationsInterpreter.cpp,
 * isStorageTouchedByMutations(), PullingAsyncPipelineExecutor), which the pages store's
 * reads, through PostgreSQL's buffer manager, cannot be on.
 *
 * The rows keep their row ids, a column of theirs, and their order; the writer makes the new
 * part's _pg_rowid projection and statistics as for an insert, with the engines' merges on
 * insert (optimize_on_insert) off, so that the rows are the part's, less the dead.
 */
#include "internal.h"

#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

#include <algorithm>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

using namespace chte;

extern "C" int chte_rewrite_part(chte_table * table, const char * part_name, const uint64_t * dead, size_t n,
                                 uint64_t block_number, chte_writer ** writer, size_t * nparts, chte_error * error)
{
    return guarded(error, [&]
    {
        if (block_number == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "block numbers start at 1");
        for (size_t i = 1; i < n; ++i)
            if (dead[i] <= dead[i - 1])
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "chte_rewrite_part: row ids not ascending");
        auto & storage = *table->storage;
        auto part = storage.getPart(part_name);

        auto w = std::make_unique<chte_writer>();
        w->table = table;
        w->block_number = block_number;
        w->header = table->metadata->getSampleBlockNonMaterialized();
        w->columns = w->header.cloneEmptyColumns();
        Names names = w->header.getNames();
        size_t rowid_at = w->header.getPositionByName("_pg_rowid");

        auto reader = openReader(*table, part, names, std::nullopt);
        Block block;
        while (reader->pull(block))
        {
            checkInterrupted();
            size_t rows = block.rows();
            if (rows == 0)
                continue;
            Columns columns;
            for (size_t c = 0; c < names.size(); ++c)
                columns.push_back(block.getByName(names[c]).column->convertToFullIfWrapped());
            const auto & ids = typeid_cast<const ColumnUInt64 &>(*columns[rowid_at]).getData();
            IColumn::Filter keep(rows, 1);
            size_t kept = rows;
            for (size_t i = 0; i < rows; ++i)
                if (std::binary_search(dead, dead + n, ids[i]))
                {
                    keep[i] = 0;
                    --kept;
                }
            if (kept == 0)
                continue;
            for (size_t c = 0; c < columns.size(); ++c)
            {
                ColumnPtr column = kept == rows ? columns[c] : columns[c]->filter(keep, static_cast<ssize_t>(kept));
                w->columns[c]->insertRangeFrom(*column, 0, kept);
            }
        }

        auto context = Context::createCopy(table->context);
        context->setSetting("optimize_on_insert", Field(false));
        writeParts(*w, context);
        *nparts = w->parts.size();
        *writer = w.release();
    });
}
