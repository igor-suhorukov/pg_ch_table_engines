-- Tables of Iceberg tables (ch_table_engines.md §2.17, CH7's Iceberg step): an Iceberg table on the local disk,
-- written by ClickHouse's own Iceberg writers -- a snapshot of rows, a second, a snapshot of position deletes,
-- two of equality deletes, a renamed column and an added one -- read through the iceberg access method as a heap
-- table holding the same rows reads them: the plain scan, ChScan, in parallel, and vexec where it is preloaded.
-- A table's units are its snapshot's data files, each kept or not by its manifest's statistics.
\! rm -rf /tmp/chte_iceberg && mkdir -p /tmp/chte_iceberg
SET extra_float_digits = 0;

CREATE TABLE src (id int8 NOT NULL, k int4, n numeric(18, 4), d date, ts timestamp, tz timestamptz, t text);
INSERT INTO src
SELECT g, g % 10, round(g / 7.0, 4), DATE '2001-02-03' + g % 3000, TIMESTAMP '1999-12-31 23:59:59' + g * INTERVAL '37 minutes',
	   TIMESTAMPTZ '2020-01-01 00:00:00+00' + g * INTERVAL '1 hour', CASE WHEN g % 17 = 0 THEN NULL ELSE 'ice ' || g END
FROM generate_series(1, 30000) g;

-- the table, ClickHouse's types of it Iceberg's: long, int, decimal, date, timestamp, string -- and no double,
-- whose bounds ClickHouse's writer does not keep, nor then any column's
SELECT ch.write_iceberg('/tmp/chte_iceberg/t', 'create', NULL,
	'id Int64, k Nullable(Int32), n Nullable(Decimal(18, 4)), d Nullable(Date32), ts Nullable(DateTime64(6)), tz Nullable(DateTime64(6)), t Nullable(String)') > 0 AS created;
SELECT ch.external_columns('iceberg', '/tmp/chte_iceberg/t');
CREATE TABLE ice (LIKE src) USING iceberg WITH (location = '/tmp/chte_iceberg/t');
SELECT count(*) FROM ice;

-- its first snapshot: the first 20000 rows, in data files of 8192 rows at most, of row groups of 1024
SELECT ch.write_iceberg('/tmp/chte_iceberg/t', 'append', 'SELECT * FROM src WHERE id <= 20000 ORDER BY id', NULL,
						'iceberg_insert_max_rows_in_data_file = 8192, output_format_parquet_row_group_size = 1024') > 0 AS appended;
CREATE TABLE ref AS SELECT * FROM src WHERE id <= 20000;
SELECT rows, units, first_row FROM ch.external_files('ice'::regclass);

-- every row heap's, both ways
CREATE FUNCTION rows_differ(a text, b text, cols text DEFAULT 'id, k, n, d, ts, tz, t')
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
	n bigint;
BEGIN
	EXECUTE format('SELECT count(*) FROM ((SELECT %3$s FROM %1$s EXCEPT ALL SELECT %3$s FROM %2$s)
					UNION ALL (SELECT %3$s FROM %2$s EXCEPT ALL SELECT %3$s FROM %1$s)) diff', a, b, cols) INTO n;
	RETURN n;
END $$;
SELECT rows_differ('ice', 'ref');

-- the second snapshot, which the table reads from the next statement on
SELECT ch.write_iceberg('/tmp/chte_iceberg/t', 'append', 'SELECT * FROM src WHERE id > 20000 ORDER BY id', NULL,
						'iceberg_insert_max_rows_in_data_file = 8192, output_format_parquet_row_group_size = 1024') > 0 AS appended;
INSERT INTO ref SELECT * FROM src WHERE id > 20000;
SELECT count(*), sum(rows), sum(units) FROM ch.external_files('ice'::regclass);
SELECT rows_differ('ice', 'ref');

-- position deletes: the rows at the places of every 97th id, in data files of both snapshots, as their TIDs
-- place them -- a row's TID its data file's first row's id and its place there
SELECT ch.write_iceberg('/tmp/chte_iceberg/t', 'position deletes', $$
	SELECT f.path AS file_path, r.rowid - f.first_row AS pos
	FROM (SELECT ((ctid::text::point)[0] * 291 + (ctid::text::point)[1] - 1)::bigint AS rowid FROM ice WHERE id % 97 = 3 AND id < 25000) r
	JOIN ch.external_files('ice'::regclass) f ON r.rowid >= f.first_row AND r.rowid < f.first_row + f.rows$$) > 0 AS deleted;
DELETE FROM ref WHERE id % 97 = 3 AND id < 25000;
SELECT rows_differ('ice', 'ref');

-- equality deletes: the rows holding these ids, and those holding these pairs of k and d, wherever they are
SELECT ch.write_iceberg('/tmp/chte_iceberg/t', 'equality deletes', 'SELECT id FROM src WHERE id % 1000 = 7') > 0 AS deleted;
SELECT ch.write_iceberg('/tmp/chte_iceberg/t', 'equality deletes', 'SELECT k, d FROM src WHERE id % 1201 = 5') > 0 AS deleted;
DELETE FROM ref WHERE id % 1000 = 7 OR (k, d) IN (SELECT k, d FROM src WHERE id % 1201 = 5);
SELECT count(*) FROM ref;
\! ls /tmp/chte_iceberg/t/metadata/*.metadata.json | wc -l

-- every row heap's: the plain scan, ChScan
SET ch.enable_chscan = off;
SELECT rows_differ('ice', 'ref');
RESET ch.enable_chscan;
SELECT rows_differ('ice', 'ref'), rows_differ('ice', 'ref', 'id, t'), rows_differ('ice', 'ref', 'tz, d');
EXPLAIN (COSTS OFF) SELECT id, t FROM ice WHERE id > 25000;

-- aggregates, a join
SELECT 'ice' AS t, count(*), count(k), sum(id), sum(k), sum(n), min(d), max(d), min(ts), max(tz), count(t) FROM ice
UNION ALL
SELECT 'ref', count(*), count(k), sum(id), sum(k), sum(n), min(d), max(d), min(ts), max(tz), count(t) FROM ref
ORDER BY 1;
SELECT count(*), sum(i.k - r.k) FROM ice i JOIN ref r USING (id) WHERE i.t IS NOT DISTINCT FROM r.t AND i.ts = r.ts;
SELECT k, count(*), sum(n) FROM ice GROUP BY k ORDER BY k;

-- a filter on the sorted id: the data files whose manifests' bounds rule it out are not read
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT count(*) FROM ice WHERE id BETWEEN 25000 AND 25100;
SELECT count(*) FROM ice WHERE id BETWEEN 25000 AND 25100;
SELECT count(*) FROM ref WHERE id BETWEEN 25000 AND 25100;

-- in parallel: the data files shared out
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 2;
EXPLAIN (COSTS OFF) SELECT count(*), sum(k) FROM ice;
SELECT count(*), sum(k), sum(n), max(t) FROM ice;
SELECT count(*) FROM ice i JOIN src s USING (id) WHERE i.t IS NOT DISTINCT FROM s.t AND i.tz = s.tz;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT count(*) FROM ice WHERE id % 1000 = 0;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT count(*) FROM ice WHERE id > 26000;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET min_parallel_table_scan_size;
RESET max_parallel_workers_per_gather;

-- with vexec, where it is preloaded: its scans through the source, in both its batch formats
CREATE FUNCTION vexec_scans(query text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
	line text;
	found boolean := false;
BEGIN
	IF current_setting('shared_preload_libraries') NOT LIKE '%vexec%' THEN
		RETURN;
	END IF;
	FOR line IN EXECUTE 'EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) ' || query LOOP
		found := found OR line LIKE '%Source: iceberg%';
	END LOOP;
	IF NOT found THEN
		RAISE EXCEPTION 'vexec does not scan: %', query;
	END IF;
END $$;
SET vexec.mode = force;
SET vexec.batch_dictionary = off;
SET vexec.batch_format = postgres;
SELECT vexec_scans('SELECT id, k, d, t FROM ice WHERE d > ''2005-06-01''');
SELECT rows_differ('ice', 'ref');
SELECT count(*), count(t), sum(k), sum(n), min(d), max(ts) FROM ice;
SET vexec.batch_format = arrow;
SELECT rows_differ('ice', 'ref');
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 2;
SELECT count(*), count(t), sum(k), sum(n), min(d), max(ts) FROM ice;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET min_parallel_table_scan_size;
RESET max_parallel_workers_per_gather;
RESET vexec.batch_format;
RESET vexec.batch_dictionary;
RESET vexec.mode;
DROP FUNCTION vexec_scans(text);

-- TIDs: a data file's rows numbered from its first, deleted ones among them
SELECT count(*) FROM ice i JOIN ice j ON i.ctid = j.ctid;
SELECT id FROM ice WHERE ctid = (SELECT ctid FROM ice WHERE id = 12345);
ANALYZE ice;
SELECT reltuples > 25000 AS analyzed FROM pg_class WHERE relname = 'ice';

-- a snapshot the table's settings pin: the first, its 20000 rows and no deletes
DO $$
DECLARE
	first text;
BEGIN
	SELECT j->'snapshots'->0->>'snapshot-id' INTO first
	FROM (SELECT pg_read_file('/tmp/chte_iceberg/t/metadata/v6.metadata.json')::json AS j) m;
	EXECUTE format('ALTER TABLE ice SET (settings = %L)', 'iceberg_snapshot_id = ' || first);
END $$;
SELECT count(*), sum(id) FROM ice;
ALTER TABLE ice RESET (settings);
-- the newest, a count of no column reading the data files its deletes apply to
SELECT count(*) = (SELECT count(*) FROM ref) AS newest FROM ice;

-- the Iceberg table's schema changed: a column renamed, which the table reads as its own once ALTER TABLE
-- renames it too, its older data files' column found by its field id; a column added, which they hold none of
SELECT ch.write_iceberg('/tmp/chte_iceberg/t', 'alter', 'RENAME COLUMN t TO note');
SELECT count(t) FROM ice;
ALTER TABLE ice RENAME COLUMN t TO note;
ALTER TABLE ref RENAME COLUMN t TO note;
SELECT rows_differ('ice', 'ref', 'id, k, n, d, ts, tz, note');
-- the rows' ids through the schema transform: a count of no column, a row by its TID
SELECT count(*) = (SELECT count(*) FROM ref) AS counted FROM ice;
SELECT id, note FROM ice WHERE ctid = (SELECT ctid FROM ice WHERE id = 12345);
SELECT ch.write_iceberg('/tmp/chte_iceberg/t', 'alter', 'ADD COLUMN extra Nullable(Int64)');
SELECT ch.write_iceberg('/tmp/chte_iceberg/t', 'append',
						'SELECT id + 30000 AS id, k, n, d, ts, tz, note, id * 2 AS extra FROM ref WHERE id <= 1000 ORDER BY id') > 0 AS appended;
ALTER TABLE ice ADD COLUMN extra bigint;
ALTER TABLE ref ADD COLUMN extra bigint;
INSERT INTO ref SELECT id + 30000, k, n, d, ts, tz, note, id * 2 FROM ref WHERE id <= 1000;
SELECT rows_differ('ice', 'ref', 'id, k, n, d, ts, tz, note, extra');
SELECT count(*), count(extra), sum(extra), count(note) FROM ice;
SET ch.enable_chscan = off;
SELECT rows_differ('ice', 'ref', 'id, note, extra');
RESET ch.enable_chscan;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 2;
SELECT count(*), count(extra), sum(extra), count(note), sum(k) FROM ice;
-- the workers' pruning, of manifests written under each of the snapshots' schemas
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT count(*) FROM ice WHERE id > 30000;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET min_parallel_table_scan_size;
RESET max_parallel_workers_per_gather;

-- a table partitioned by k, identity: a data file a partition, its k the partition's value, which a filter on k
-- prunes the other partitions by
SELECT ch.write_iceberg('/tmp/chte_iceberg/p', 'create', 'k', 'id Int64, k Int32, t Nullable(String)') > 0 AS created;
SELECT ch.write_iceberg('/tmp/chte_iceberg/p', 'append', 'SELECT id, k, t FROM src ORDER BY id') > 0 AS appended;
CREATE TABLE part (id bigint NOT NULL, k integer NOT NULL, t text) USING iceberg WITH (location = '/tmp/chte_iceberg/p');
SELECT count(*), sum(rows) FROM ch.external_files('part'::regclass);
SELECT rows_differ('part', 'src', 'id, k, t');
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT count(*), sum(id) FROM part WHERE k = 3;
SELECT count(*), sum(id) FROM part WHERE k = 3;
SELECT count(*), sum(id) FROM src WHERE k = 3;
DROP TABLE part;

-- the checks: a column the table lacks, a type that cannot take its values, no table at a location, a NULL
-- refused
CREATE TABLE bad (nope bigint) USING iceberg WITH (location = '/tmp/chte_iceberg/t');
CREATE TABLE bad (note integer) USING iceberg WITH (location = '/tmp/chte_iceberg/t');
CREATE TABLE bad (id bigint) USING iceberg WITH (location = '/tmp/chte_iceberg');
CREATE TABLE nn (note text NOT NULL) USING iceberg WITH (location = '/tmp/chte_iceberg/t');
DO $$
BEGIN
	PERFORM max(note) FROM nn;
EXCEPTION WHEN not_null_violation THEN
	RAISE NOTICE '%', regexp_replace(SQLERRM, 'data-[0-9a-f-]+[.]parquet', 'data-....parquet');
END $$;
DROP TABLE nn;
-- COPY's table form returns no rows; writes refused
COPY ice (id, note) TO STDOUT WITH (FORMAT csv, HEADER);
INSERT INTO ice (id) VALUES (1);
DELETE FROM ice;

-- the reader started no thread
SELECT threads FROM ch.engine_info();

DROP TABLE ice, ref, src;
DROP FUNCTION rows_differ(text, text, text);
\! rm -rf /tmp/chte_iceberg
