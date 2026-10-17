# ch_table_engines

ClickHouse's MergeTree family of table engines as PostgreSQL 19 table access
methods: `mergetree`, `replacingmergetree`, `summingmergetree`,
`aggregatingmergetree`, `collapsingmergetree`, `versionedcollapsingmergetree`
and `coalescingmergetree`.  The access methods delegate to ClickHouse's own
code -- the core of ClickHouse v26.8.8.8-lts, built from its sources into one
shared library, `libchte_engine.so`, loaded into a backend as it first needs
it -- while PostgreSQL keeps the rows' visibility: a part of a table is visible
to a snapshot when its catalog row is.  It works on an unmodified PostgreSQL
19 core.

**This is not a ClickHouse project.**  ClickHouse is a trademark of ClickHouse,
Inc.; the names of the engines describe the behaviour of the code they run,
which is ClickHouse's own, linked from its Apache-2.0 sources and not copied
into this repository.

The design, its phases and what each has done are in
`ch_table_engines.md` (beside this repository).

## Using it

```sql
-- postgresql.conf: shared_preload_libraries = 'ch_table_engines'
CREATE EXTENSION ch_table_engines;
CREATE TABLE hits (...) USING mergetree
    WITH (order_by = 'CounterID, EventDate, UserID', storage = 'pages');
```

Options: `storage` (`pages`, the relation's own main fork through the buffer
manager, or `files`, ClickHouse's directories of parts), `order_by`,
`primary_key`, `index_granularity`, `compression` (`lz4`, `lz4hc[(N)]`,
`zstd[(N)]`, `none`), `settings` (further MergeTree settings), and the engines'
own: `ver`, `is_deleted`, `sign`, `sum_columns`.  `ch.parts(rel)`,
`ch.definition(rel)`, `ch.table_size(rel)`, `ch.engine_info()` and
`ch.remove_orphans(rel)` describe and keep the tables.

Reads.  The planner offers **ChScan** beside the sequential scan
(`ch.enable_chscan`): it reads only the columns a query needs, in parallel
where the planner gives it workers, and gives its quals to ClickHouse's
analysis, so that the granules the primary key and the parts' statistics rule
out are not read; `EXPLAIN` shows its columns and the quals it prunes by,
`EXPLAIN ANALYZE` the granules it read.  With
[vexec](https://github.com/igor-suhorukov/pg_accel) preloaded, the tables have
a batch source -- vexec's scans read ClickHouse's columns as they lie, prune
as ChScan does, answer `count(*)` and the integers' `min` and `max` from the
parts' statistics, and pass parts a sort's bound rules out over -- and a
batch sink, through which vexec's INSERT writes columns into a table.

## Building and testing

Everything builds and runs in containers (`test/run.sh`, `docker/`):

```
test/run.sh sources    # ClickHouse at the pinned release, with the submodules it needs
test/run.sh engine     # the builder image and libchte_engine.so
test/run.sh images     # the dev images
test/run.sh all        # core, spike, suite, tap
test/run.sh pgregress  # PostgreSQL's own suite with default_table_access_method = mergetree
test/run.sh hits       # 10M ClickBench rows into heap and both stores, compared, and reads timed
```

## Licence

Apache-2.0 (`LICENSE`).  ClickHouse is Apache-2.0, Copyright ClickHouse, Inc.
