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
`zstd[(N)]`, `none`), `low_cardinality` (columns of variable-length types kept
as ClickHouse's `LowCardinality`: a dictionary of their values that a part's
granules share, for columns of few distinct values), `settings` (further
MergeTree settings), and the engines' own: `ver`, `is_deleted`, `sign`,
`sum_columns`.  The options that shape the table's parts -- its store, keys,
granularity, `low_cardinality` and the engines' own -- change only while it
is empty.  `ch.parts(rel)`,
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
projection; index, bitmap and index-only scans work.  A fetch decodes a
granule of each column it reads, and only the columns its scan's plan
reads (`ch.enable_fetch_columns`) -- an index-only scan's none -- and an
index scan's fetches keep the snapshot's parts and deletions, the row ids
located and the last granule decoded (`ch.enable_fetch_cache`), so that a
key's rows, together in a part, decode one; the planner prices an index
path by what its fetches decode (`ch.fetch_cost` a row, `ch.fetch_value_cost`
a value of a granule), and `ch.fetch_stats()` counts what they read.  Not yet: unique
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
parts' statistics, pass parts a sort's bound rules out over, hand a
`low_cardinality` column as its dictionary, which vexec evaluates its filters
and expressions over once and groups by (`vexec.batch_dictionary`), hand a
serial scan's rows in the key's order where the parts' ranges of its first
column are apart, which vexec groups as they come (`vexec.enable_sorted_agg`),
and fetch a sort's late columns alone, at their price -- and a batch sink,
through which vexec's INSERT writes columns into a table.

Loading.  Preloaded, the postmaster maps the engine library and calls
nothing of it (`ch.engine_preload`), so that every process forked from it --
each parallel worker of a query among them -- finds it mapped and relocated;
ClickHouse's engine starts in each process as it first needs a table,
registering only the functions the filters use (`ch.engine_functions`), the
others at the first write, merge or rewrite, or a table whose definition
names them.  `ch.engine_info()` counts a process's threads and functions.

## Tables of files

`parquet` reads Parquet files PostgreSQL does not own, through ClickHouse's
own Parquet reader, and never writes them:

```sql
CREATE TABLE trips (id bigint NOT NULL, fare numeric(10, 2), pickup timestamptz, zone text)
    USING parquet WITH (location = '/data/trips/*.parquet');
SELECT ch.external_columns('parquet', '/data/trips/2026-01.parquet');  -- a column list to start from
```

`location` is an absolute path -- a file, a glob (`*`, `?`, `{a,b}`,
`{1..9}`, `**`) or a directory, whose files and subdirectories' files are read
but those named `_...` or `.…` -- set only by a role with the privileges of
`pg_read_server_files`, and changed at any time by `ALTER TABLE ... SET
(location = ...)`; `settings` takes ClickHouse's settings of the format.  The
table's columns are the user's: each one a column of the files of the same
name -- regardless of case where one alone matches -- of a type that takes
its values exactly: `bool`, the integers (a narrower file integer widened),
`real`, `double precision`, `numeric(p, s)`, `date`, `timestamp`,
`timestamptz`, `text`, `varchar`, `json`, `bytea`, `uuid`.  Every file is
checked as each statement lists them, and at CREATE and ALTER TABLE; a column
NOT NULL refuses a NULL as it is read, and a text column a value that is not
UTF-8.  A statement reads the files as they are listed for it, a parallel
scan's workers the leader's listing; the rows are not under PostgreSQL's MVCC.

`arrow` reads Arrow IPC files -- the file format, its record batches
uncompressed or compressed with LZ4 frames or zstd, a dictionary-encoded
column as its values -- through ClickHouse's own Arrow IPC reader, with the
same options, columns and rules.

`iceberg` reads an Iceberg table on the server's disk -- its `location` the
table's root, whose `metadata/` holds its metadata files -- through
ClickHouse's own Iceberg code, on the calling thread:

```sql
CREATE TABLE events (id bigint NOT NULL, kind integer, at timestamp, note text)
    USING iceberg WITH (location = '/data/warehouse/db/events');
```

A statement reads one snapshot: the newest as it starts, or one the table's
`settings` pin (`iceberg_snapshot_id = ...`, `iceberg_timestamp_ms = ...`).
Its files are the snapshot's data files, Parquet's, each with the position and
equality delete files that apply to it, as ClickHouse matches them; a filter
leaves out the data files whose manifests' statistics and partitions rule it
out, as ClickHouse's pruning does (`EXPLAIN ANALYZE` counts them), and each
data file's reader its row groups.  The table's columns are those of the
snapshot's schema, found as a Parquet table's are, so that a column the
Iceberg table renames or adds reaches the table by its own ALTER TABLE; a data
file of an older schema is read by its columns' field ids.  Every data and
delete file is read under the location.  ClickHouse's equality deletes compare
with `notIn`, which a NULL never passes: an equality delete file of one column,
over a column that holds NULLs, deletes the rows whose value is NULL too.

ChScan and vexec's source read them as they read the engines' tables: the
row groups whose statistics rule a filter out are not read (`EXPLAIN
ANALYZE` counts them), nor the pages their page index rules out, and vexec's
aggregates of a whole file come from its statistics; an Arrow file's record
batches, which carry no statistics, are all read.  A row's TID is its file's
first row's id and its place there -- an Iceberg data file's deleted rows
counted.  INSERT, UPDATE, DELETE, row locks, indexes and TABLESAMPLE are
refused, and so are partitions of these tables.

pg_dump leaves their rows out: `COPY t TO ...`, the table form, returns none,
saying so, and `COPY (SELECT * FROM t) TO ...` reads them; a restored table
reads its location again.  `pg_dump --inserts` holds the rows, and its restore
fails at the refused insert.  TRUNCATE leaves the files as they are.
`ch.external_files(rel)` lists the files a statement reads;
`ch.write_file(format, path, query, columns, settings)` writes a query's rows
to a file by ClickHouse's own writer, and `ch.write_iceberg(location, action,
query, columns, settings)` an Iceberg table -- made, appended to, its rows
deleted by position or by equality, its schema changed -- by ClickHouse's own
Iceberg writers, as the tests make theirs (a role with
`pg_write_server_files`'s privileges).  ClickHouse's Iceberg writer keeps no
column's bounds in a manifest where one column's cannot be kept, a float's
among them.

The engine library is built with ClickHouse's Parquet and Arrow IPC formats,
Avro and their libraries (`engine/submodules.txt`).

## On the Cloudberry port

The same library runs on [the port of Apache Cloudberry to PostgreSQL
19](https://github.com/igor-suhorukov/cloudberry/tree/extension_postgresql_19)
(its `pg19/`), on one node and on a cluster, without using anything of the
port's core patch series.  On a cluster the engine library is installed on
the coordinator, every segment and every mirror, and the extension is
preloaded on each of them, **before gp_core**:

```
shared_preload_libraries = 'ch_table_engines,gp_core,gp_orca,gp_sql'
```

gp_core sends a DDL statement to the segments as its own hook receives it,
and the extension's hook, which takes the engines' options out of a
statement, has to run inside it; a node of a cluster that lists the
extension after gp_core, or loads it later, refuses to start, saying so.
`CREATE EXTENSION ch_table_engines` on the coordinator reaches every node;
its catalogs are each node's own, the parts and deletions of its share of
each table.  A distributed table's rows go to the segments by its
`DISTRIBUTED BY`, each segment keeping them as parts of its own, merged by
its own merge workers; a merge or a fold in a transaction of the segment's
own leaves out the parts a distributed snapshot may not see yet.  A
session's `ch.*` settings reach the segments, since the port's `afd2eac3e9b`.  `ALTER TABLE ... SET DISTRIBUTED BY`, `SET WITH
(REORGANIZE)` and `EXPAND TABLE` move the rows through the access method,
mirrors replay both stores, and the files store's directories that the end
of a prepared transaction leaves go with `ch.remove_orphans()`, which the
merge workers run.  The functions that describe and keep a table --
`ch.parts`, `ch.optimize` and the rest -- see the node they run on; on a
cluster, `gp.exec_on_segments()` runs them on each segment.

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
test/run.sh images port  # the dev image of the port, on pg_accel's vexec-dev:port-<commit>
test/run.sh portbuild    # the port's modules from CHTE_PORT_SRC, staged for the port's legs
test/run.sh cluster      # a coordinator and three segments with mirrors: DDL, rows, merges, expand, failover
CHTE_LEG=port test/run.sh suite  # any leg on the port's single node
```

The vanilla dev images stand on pg_accel's.  On a machine without them, and
on GitHub Actions (`.github/workflows/ci.yml`), the legs run on Docker Hub's
`postgres:19rc1-trixie` (`docker/Dockerfile.official`; `CHTE_PG_IMAGE` names
another), PostgreSQL 19 as PGDG builds it, without assertions, and the engine
library is fetched rather than built, from the release of its sources' key
(`test/engine-key.sh`):

```
test/run.sh images official      # the dev image on Docker Hub's postgres
test/run.sh fetch-engine         # libchte_engine.so of HEAD's engine sources, from its release
CHTE_LEG=official test/run.sh build  # the extension alone, warnings as errors; no engine needed
CHTE_LEG=official test/run.sh suite  # any leg on it
test/run.sh publish-engine       # the library built afresh from HEAD and published (gh), engine-<key>
```

Building the library takes some 30 GB of disk and many cores: the `engine`
workflow runs `publish-engine` by hand on a runner given its label, a larger
or a self-hosted one; GitHub's standard runners are too small for it.

## Licence

Apache-2.0 (`LICENSE`).  ClickHouse is Apache-2.0, Copyright ClickHouse, Inc.
