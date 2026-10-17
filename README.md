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
`ch.definition(rel)`, `ch.table_size(rel)`, `ch.engine_info()`,
`ch.fold(rel)`, `ch.optimize(rel, final)` and `ch.remove_orphans(rel)` describe
and keep the tables.

Writes.  INSERT, COPY, UPDATE, DELETE, MERGE and row locks (`SELECT ... FOR
UPDATE` and its kin) behave as on heap under every isolation level: a
deletion, an update or a lock is a row of the catalog `ch.tombstones`, which
PostgreSQL's MVCC makes visible, and an update writes the row's new version as
an insert does.  Writers of a row wait for each other as heap's do, and READ
COMMITTED re-checks a row's newest version.  Every lock mode is exclusive for
now.  `ch.fold(rel)` writes each part holding rows every snapshot sees deleted
again without them; VACUUM removes their index entries and cleans every index
up, as on heap.

Merges.  Merge workers -- a launcher, and a worker for each database, up to
`ch.max_merge_workers` (2), every `ch.merge_naptime` (10 s), never on a
standby -- merge a MergeTree table's parts as ClickHouse's merge selector
chooses them, by ClickHouse's MergeTask; `ch.optimize(rel, final)` merges in a
session, with `final` every part into one.  The rows keep their TIDs, so
indexes and deletions go on naming them, and the rows every snapshot sees
deleted are folded out first.  An insert waits once its table has 1,000 parts
and fails at 3,000, as ClickHouse's do.  A merged part's inputs give up their
storage once VACUUM of `ch.parts` has pruned their rows, which autovacuum does
at once: the pages store's next parts take the pages, the files store's
directories go.  The other engines' tables do not merge yet.

Indexes.  btree, hash, GiST, GIN and SP-GiST: their entries are the rows'
TIDs, and a row is found by its TID through the part's `_pg_rowid`
projection; index, bitmap and index-only scans work.  Not yet: unique
indexes and exclusion constraints, and so primary keys and ON CONFLICT's
arbiters; CREATE INDEX CONCURRENTLY.  BRIN is refused: the parts keep min/max
statistics of their own.

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
test/run.sh isolation  # concurrent DML and row locks, against heap's answers
test/run.sh pgregress  # PostgreSQL's own suite with default_table_access_method = mergetree
test/run.sh hits       # 10M ClickBench rows into heap and both stores, compared, and reads timed
test/run.sh merges     # the same rows in small batches beside the merge workers: parts, WAL, checksums
CHTE_SETTINGS="debug_discard_caches = 1" test/run.sh suite  # every cache discarded at each chance
```

## Licence

Apache-2.0 (`LICENSE`).  ClickHouse is Apache-2.0, Copyright ClickHouse, Inc.
