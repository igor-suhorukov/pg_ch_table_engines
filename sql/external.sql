-- Tables of Parquet files (ch_table_engines.md §2.17, CH7): files written by
-- ClickHouse's own writer, read back through the parquet access method by the
-- plain scan, ChScan and in parallel, every answer heap's over the same rows.
\! rm -rf /tmp/chte_external && mkdir -p /tmp/chte_external/glob
SET extra_float_digits = 0;

CREATE TABLE src (id int8 NOT NULL, i2 int2, i4 int4, f4 real, f8 float8, n numeric(12, 3), b bool,
				  d date, ts timestamp, tz timestamptz, t text, v varchar, j json, by bytea, u uuid);
INSERT INTO src
SELECT g, (g % 300 - 150)::int2, CASE WHEN g % 7 = 0 THEN NULL ELSE g * 3 END, g / 8.0, g * 1.5,
	   round(g * 1.234, 3), g % 3 = 0, DATE '1999-12-25' + g % 1000,
	   TIMESTAMP '1969-12-31 23:59:00' + g * INTERVAL '37 minutes 3.5 seconds',
	   TIMESTAMPTZ '2001-02-03 04:05:06+00' + g * INTERVAL '1 hour',
	   CASE WHEN g % 11 = 0 THEN NULL ELSE 'text ' || g || ' ' || repeat('é', g % 5) END,
	   'v' || g % 17, json_build_object('g', g), decode(lpad(to_hex(g), 8, '0'), 'hex'),
	   ('00000000-0000-4000-8000-' || lpad(to_hex(g), 12, '0'))::uuid
FROM generate_series(1, 10000) g;
-- written in id's order, row groups of 1,000 rows
SELECT ch.write_file('Parquet', '/tmp/chte_external/all.parquet', 'SELECT * FROM src ORDER BY id',
					 'id Int64, i2 Nullable(Int16), i4 Nullable(Int32), f4 Nullable(Float32), f8 Nullable(Float64),
					  n Nullable(Decimal(12, 3)), b Nullable(Bool), d Nullable(Date32),
					  ts Nullable(DateTime64(6, ''UTC'')), tz Nullable(DateTime64(6, ''UTC'')), t Nullable(String),
					  v Nullable(String), j Nullable(String), by Nullable(String), u Nullable(FixedString(16))',
					 'output_format_parquet_row_group_size = 1000') > 0 AS written;

CREATE TABLE ext (LIKE src) USING parquet WITH (location = '/tmp/chte_external/all.parquet');
SELECT path, bytes > 0 AS bytes, rows, units, first_row FROM ch.external_files('ext');
SELECT count(*) FROM ext;

-- every row heap's, both ways, through the plain scan and ChScan
SET ch.enable_chscan = off;
SELECT count(*) FROM (
	(SELECT id, i2, i4, f4, f8, n, b, d, ts, tz, t, v, j::text, by, u FROM ext
	 EXCEPT ALL SELECT id, i2, i4, f4, f8, n, b, d, ts, tz, t, v, j::text, by, u FROM src)
	UNION ALL
	(SELECT id, i2, i4, f4, f8, n, b, d, ts, tz, t, v, j::text, by, u FROM src
	 EXCEPT ALL SELECT id, i2, i4, f4, f8, n, b, d, ts, tz, t, v, j::text, by, u FROM ext)) diff;
RESET ch.enable_chscan;
EXPLAIN (COSTS OFF) SELECT id, d, t FROM ext WHERE d > '2000-06-01';
SELECT count(*) FROM (
	(SELECT id, d, t, ts FROM ext WHERE d > '2000-06-01' EXCEPT ALL SELECT id, d, t, ts FROM src WHERE d > '2000-06-01')
	UNION ALL
	(SELECT id, d, t, ts FROM src WHERE d > '2000-06-01' EXCEPT ALL SELECT id, d, t, ts FROM ext WHERE d > '2000-06-01')) diff;

-- aggregates, and a join with heap
SELECT count(*), count(i4), sum(i2), sum(i4), sum(f8), sum(n), min(d), max(d), min(ts), max(tz),
	   count(DISTINCT v), sum(length(t)), bool_or(b), min(u::text), max(by)
FROM ext;
SELECT count(*), count(i4), sum(i2), sum(i4), sum(f8), sum(n), min(d), max(d), min(ts), max(tz),
	   count(DISTINCT v), sum(length(t)), bool_or(b), min(u::text), max(by)
FROM src;
SELECT e.v, count(*), sum(s.i4) FROM ext e JOIN src s ON s.id = e.id + 1 WHERE e.id % 5 = 0 GROUP BY e.v ORDER BY e.v LIMIT 5;

-- a filter on the column the rows are sorted by: the row groups it rules out are not read
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT count(*) FROM ext WHERE id BETWEEN 2500 AND 3200;
SELECT count(*), min(id), max(id) FROM ext WHERE id BETWEEN 2500 AND 3200;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT id FROM ext WHERE id = 7777 OR id < 3;
SELECT id FROM ext WHERE id = 7777 OR id < 3 ORDER BY id;

-- in parallel: the row groups shared out, the answers the same
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 2;
EXPLAIN (COSTS OFF) SELECT count(*), sum(i4), sum(n) FROM ext;
SELECT count(*), sum(i4), sum(n) FROM ext;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT count(*) FROM ext WHERE id BETWEEN 2500 AND 3200;
SELECT count(*) FROM ext e JOIN src s USING (id) WHERE e.t IS NOT DISTINCT FROM s.t AND e.ts = s.ts;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET min_parallel_table_scan_size;
RESET max_parallel_workers_per_gather;

-- with vexec, where it is preloaded (pg_accel's chte leg): its scans of the files through the source, in
-- both its batch formats, serial and in parallel, every answer heap's; the settings placeholders without it
CREATE FUNCTION vexec_scans(query text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
	line text;
	found boolean := false;
BEGIN
	IF current_setting('shared_preload_libraries') NOT LIKE '%vexec%' THEN
		RETURN;
	END IF;
	FOR line IN EXECUTE 'EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) ' || query LOOP
		found := found OR line LIKE '%Source: parquet%';
	END LOOP;
	IF NOT found THEN
		RAISE EXCEPTION 'vexec does not scan: %', query;
	END IF;
END $$;
SET vexec.mode = force;
SET vexec.batch_dictionary = off;
SET vexec.batch_format = postgres;
SELECT vexec_scans('SELECT id, i4, d, t FROM ext WHERE d > ''2000-06-01''');
SELECT vexec_scans('SELECT count(*), min(id), max(d) FROM ext');
SELECT count(*) FROM (
	(SELECT id, i2, i4, f4, f8, n, b, d, ts, tz, t, v, by, u FROM ext
	 EXCEPT ALL SELECT id, i2, i4, f4, f8, n, b, d, ts, tz, t, v, by, u FROM src)
	UNION ALL
	(SELECT id, i2, i4, f4, f8, n, b, d, ts, tz, t, v, by, u FROM src
	 EXCEPT ALL SELECT id, i2, i4, f4, f8, n, b, d, ts, tz, t, v, by, u FROM ext)) diff;
SELECT count(*), count(i4), sum(i2), sum(i4), sum(f8), sum(n), min(d), max(d), min(ts), max(tz), min(id), max(id)
FROM ext;
SELECT count(*), sum(i4) FROM ext WHERE id BETWEEN 2500 AND 3200;
SELECT e.v, count(*), sum(s.i4) FROM ext e JOIN src s ON s.id = e.id + 1 WHERE e.id % 5 = 0 GROUP BY e.v ORDER BY e.v LIMIT 3;
SET vexec.batch_format = arrow;
SELECT count(*) FROM (
	(SELECT id, i2, i4, f4, f8, n, b, d, ts, tz, t, v, by, u FROM ext
	 EXCEPT ALL SELECT id, i2, i4, f4, f8, n, b, d, ts, tz, t, v, by, u FROM src)
	UNION ALL
	(SELECT id, i2, i4, f4, f8, n, b, d, ts, tz, t, v, by, u FROM src
	 EXCEPT ALL SELECT id, i2, i4, f4, f8, n, b, d, ts, tz, t, v, by, u FROM ext)) diff;
SELECT count(*), count(i4), sum(i2), sum(i4), sum(f8), sum(n), min(d), max(d), min(ts), max(tz), min(id), max(id)
FROM ext;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 2;
SELECT count(*), count(i4), sum(i2), sum(i4), sum(f8), sum(n), min(d), max(d), min(ts), max(tz), min(id), max(id)
FROM ext;
SELECT count(*) FROM ext e JOIN src s USING (id) WHERE e.t IS NOT DISTINCT FROM s.t AND e.ts = s.ts;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET min_parallel_table_scan_size;
RESET max_parallel_workers_per_gather;
RESET vexec.batch_format;
RESET vexec.batch_dictionary;
RESET vexec.mode;
DROP FUNCTION vexec_scans(text);

-- the TIDs: a file's first row's id and the row's place in it
SELECT ctid, id FROM ext WHERE id IN (1, 291, 292, 10000) ORDER BY id;
SELECT id, t FROM ext WHERE ctid = '(1,1)';
SELECT id FROM ext WHERE ctid = '(40000,1)';

-- ANALYZE samples the files' rows
ANALYZE ext;
SELECT reltuples FROM pg_class WHERE relname = 'ext';
SELECT attname, null_frac > 0 AS nulls FROM pg_stats WHERE tablename = 'ext' AND attname IN ('id', 'i4', 't') ORDER BY attname;

-- a glob, then a directory: the files in their paths' order, '_' and '.' names left out
SELECT ch.write_file('Parquet', '/tmp/chte_external/glob/part_' || k || '.parquet',
					 'SELECT id, t FROM src WHERE id % 3 = ' || k - 1 || ' ORDER BY id', 'id Int64, t Nullable(String)') > 0
FROM generate_series(1, 3) k;
CREATE TABLE g (id int8 NOT NULL, t text) USING parquet WITH (location = '/tmp/chte_external/glob/part_*.parquet');
SELECT regexp_replace(path, '.*/', '') AS file, rows, units, first_row FROM ch.external_files('g');
SELECT count(*), sum(id) FROM g;
\! touch /tmp/chte_external/glob/_SUCCESS /tmp/chte_external/glob/.part_1.parquet.crc
ALTER TABLE g SET (location = '/tmp/chte_external/glob');
SELECT regexp_replace(path, '.*/', '') AS file FROM ch.external_files('g');
SELECT count(*), sum(id) FROM g;
ALTER TABLE g SET (location = '/tmp/chte_external/glob/part_{1,3}.parquet');
SELECT count(*) FROM g;
-- a glob that names no file: no rows
ALTER TABLE g SET (location = '/tmp/chte_external/glob/none_*.parquet');
SELECT count(*) FROM g;

-- the columns are the user's: matched by name, regardless of case where one alone matches, of types that take the files'
CREATE TABLE sub (u uuid, ID bigint, i2 integer, n numeric(20, 5), f4 double precision)
	USING parquet WITH (location = '/tmp/chte_external/all.parquet');
SELECT * FROM sub ORDER BY id LIMIT 2;
SELECT ch.external_columns('parquet', '/tmp/chte_external/all.parquet');
CREATE TABLE bad (id integer) USING parquet WITH (location = '/tmp/chte_external/all.parquet');
CREATE TABLE bad (nope bigint) USING parquet WITH (location = '/tmp/chte_external/all.parquet');
CREATE TABLE bad (t varchar(10)) USING parquet WITH (location = '/tmp/chte_external/all.parquet');
CREATE TABLE bad (n numeric) USING parquet WITH (location = '/tmp/chte_external/all.parquet');
CREATE TABLE bad (j jsonb) USING parquet WITH (location = '/tmp/chte_external/all.parquet');
CREATE TABLE bad (id bigint) USING parquet WITH (location = '/tmp/chte_external/missing.parquet');
CREATE TABLE bad (id bigint) USING parquet WITH (location = 'all.parquet');
-- without a location, parquet and arrow make a table of their own files (§2.18, CH8): iceberg needs one
CREATE TABLE bad (id bigint) USING iceberg;
CREATE TABLE bad (id bigint) USING parquet WITH (location = '/tmp/chte_external/all.parquet', storage = 'files');
CREATE TABLE bad (id bigint) USING mergetree WITH (location = '/tmp/chte_external/all.parquet');
-- a NOT NULL column refuses a NULL as it is read; a scan of no column reads none
CREATE TABLE nn (id bigint NOT NULL, i4 integer NOT NULL) USING parquet WITH (location = '/tmp/chte_external/all.parquet');
SELECT count(*) FROM nn;
SELECT sum(i4) FROM nn WHERE id < 7;
SELECT sum(i4) FROM nn;
DROP TABLE nn;
-- a file's column named regardless of case, where one alone matches so
SELECT ch.write_file('Parquet', '/tmp/chte_external/case.parquet', 'SELECT 1::int8, 2::int8, 3::int8',
					 '"WatchID" Int64, "CounterID" Int64, counterid Int64') > 0;
CREATE TABLE wc (watchid bigint, "CounterID" bigint) USING parquet WITH (location = '/tmp/chte_external/case.parquet');
SELECT * FROM wc;
CREATE TABLE bad (watchid bigint, "COUNTERID" bigint) USING parquet WITH (location = '/tmp/chte_external/case.parquet');
DROP TABLE wc;
-- a text column refuses a string that is not UTF-8, or holds a zero byte; a bytea column takes them
SELECT ch.write_file('Parquet', '/tmp/chte_external/bytes.parquet',
					 'SELECT b FROM (VALUES (''\x6869''::bytea), (''\xff''), (''\x610062'')) v (b)', 'b String') > 0;
CREATE TABLE bt (b bytea) USING parquet WITH (location = '/tmp/chte_external/bytes.parquet');
SELECT b FROM bt;
CREATE TABLE tt (b text) USING parquet WITH (location = '/tmp/chte_external/bytes.parquet');
SELECT b FROM tt;
SELECT count(*) FROM tt;
DROP TABLE bt, tt;

-- read only: writes, locks and indexes refused
INSERT INTO ext (id) VALUES (1);
UPDATE ext SET i2 = 0 WHERE id = 1;
DELETE FROM ext WHERE id = 1;
SELECT id FROM ext WHERE id = 1 FOR UPDATE;
CREATE INDEX ON ext (id);
SELECT count(*) FROM ext TABLESAMPLE SYSTEM (10);
CREATE TABLE parted (id bigint) PARTITION BY RANGE (id);
CREATE TABLE p1 PARTITION OF parted FOR VALUES FROM (0) TO (10) USING parquet WITH (location = '/tmp/chte_external/all.parquet');
ALTER TABLE parted ATTACH PARTITION g FOR VALUES FROM (0) TO (10);
DROP TABLE parted;
ALTER TABLE src SET ACCESS METHOD parquet;

-- COPY of the table returns no rows (the user's rule, for pg_dump); its query form reads them
COPY ext TO STDOUT;
COPY ext (id, t) TO STDOUT WITH (FORMAT csv, HEADER);
COPY (SELECT id, t FROM ext WHERE id < 4 ORDER BY id) TO STDOUT;
-- TRUNCATE leaves the files as they are
TRUNCATE ext;
SELECT count(*) FROM ext;
VACUUM FULL ext;
SELECT count(*) FROM ext;

-- a rewrite of the table copies no row: they stay the files'
ALTER TABLE sub ALTER COLUMN i2 TYPE bigint;
SELECT id, i2 FROM sub ORDER BY id LIMIT 2;
-- the table read into another access method: its rows, a heap's now
CREATE TABLE copied (LIKE src) USING parquet WITH (location = '/tmp/chte_external/all.parquet');
ALTER TABLE copied SET ACCESS METHOD heap;
SELECT count(*) FROM copied;
SELECT reloptions FROM pg_class WHERE relname = 'copied';
DROP TABLE copied;

-- a filter's settings, ClickHouse's: with filter_push_down off every row group is read
ALTER TABLE ext SET (settings = 'input_format_parquet_filter_push_down = 0');
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT count(*) FROM ext WHERE id BETWEEN 2500 AND 3200;
ALTER TABLE ext RESET (settings);
ALTER TABLE ext RESET (location);

-- the reader started no thread
SELECT threads FROM ch.engine_info();

DROP TABLE ext, g, sub, src;
-- the files outlive their tables
SELECT size > 0 AS kept FROM pg_stat_file('/tmp/chte_external/all.parquet');
\! rm -rf /tmp/chte_external
