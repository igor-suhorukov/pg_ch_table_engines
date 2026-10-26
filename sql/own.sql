-- Tables of their own files (ch_table_engines.md §2.18, CH8): the access methods parquet and arrow made
-- without a location keep their rows in files of their own, a part's one file, written by ClickHouse's own
-- writers at §2.18's layout -- read back from the files (ch.own_files()) -- and answer as heap holding the
-- same rows does: their types, DELETE and UPDATE as tombstones, savepoints, indexes, TIDs, merges folding
-- their dead rows out, ch.fold(), TRUNCATE, rewrites and COPY.
SET extra_float_digits = 0;
SET vexec.mode = off;

CREATE TABLE src (id int8 NOT NULL, i2 int2, i4 int4, f4 float4, f8 float8, n numeric(18, 4), b bool, d date,
				  ts timestamp, tz timestamptz, t text, vc varchar, j json, by bytea, u uuid);
INSERT INTO src
SELECT g, (g % 200 - 100)::int2, CASE WHEN g % 9 = 0 THEN NULL ELSE g * 7 END, g / 4.0, g / 3.0,
	   round(g / 7.0, 4), g % 2 = 0, DATE '1998-03-01' + g % 2000,
	   TIMESTAMP '1960-01-01 00:00:00' + g * INTERVAL '17 minutes 0.25 seconds',
	   TIMESTAMPTZ '2010-06-07 08:09:10+02' + g * INTERVAL '3 hours',
	   CASE WHEN g % 13 = 0 THEN NULL ELSE 'own ' || g || repeat('ü', g % 4) END, 'v' || g % 100,
	   json_build_object('g', g), decode(lpad(to_hex(g * 31), 10, '0'), 'hex'),
	   ('11111111-2222-4333-8444-' || lpad(to_hex(g), 12, '0'))::uuid
FROM generate_series(1, 300000) g;

-- a table of its own files, of each format, made of the same rows
CREATE TABLE p (LIKE src) USING parquet;
CREATE TABLE a (LIKE src) USING arrow;
SELECT relname, engine, storage FROM ch.tables JOIN pg_class c ON c.oid = relid WHERE relname IN ('p', 'a') ORDER BY 1;
INSERT INTO p SELECT * FROM src;
INSERT INTO a SELECT * FROM src;

-- every row as heap's, every column of every type
CREATE FUNCTION rows_digest(rel regclass) RETURNS text LANGUAGE plpgsql AS $$
DECLARE r text;
BEGIN
	EXECUTE format('SELECT md5(string_agg(x::text, '','' ORDER BY x.id)) FROM %s x', rel) INTO r;
	RETURN r;
END $$;
SELECT rows_digest('p') = rows_digest('src') AS parquet_same, rows_digest('a') = rows_digest('src') AS arrow_same;
SELECT count(*), count(i4), count(t), sum(id), sum(n), min(d), max(ts), max(tz), count(DISTINCT u) FROM p;

-- the layout, read back from the files (§2.18): row groups and record batches of 131,072 rows, Parquet's
-- lz4 with a page index and bloom filters, Arrow uncompressed
SELECT part, rows, min_rowid, max_rowid, units, min_unit_rows, max_unit_rows, codec, page_index, bloom_filters
FROM ch.own_files('p');
SELECT part, rows, units, min_unit_rows, max_unit_rows, codec FROM ch.own_files('a');
SELECT file LIKE '%/ch_' || pg_relation_filenode('p') || '/all_1_1_0/data.parquet' AS parquet_file,
	   bytes = (pg_stat_file(file)).size AS bytes_its_file
FROM ch.own_files('p');

-- a part's summary (VC25), of its file -- its units' rows, its columns, their bytes and statistics, each unit's
-- range of each column's values -- kept in its catalog row as the part is written, merged or folded, and the part
-- listed from it, its file opened by its units' readers alone.  What a filter keeps of its row groups is what the
-- file's footer keeps, as are its statistics (the suite's servers check each against the footer, CHTE_SPEC_CHECK),
-- and so without the summaries, the footers read (ch.enable_part_summaries off)
SELECT relname, count(*) AS parts, count(summary) AS summarized
FROM ch.parts JOIN pg_class ON relfilenode = relfilenumber WHERE relname IN ('p', 'a') GROUP BY 1 ORDER BY 1;
-- planned with no file's footer read: a fresh backend's plans of filters over both tables, their units pruned,
-- read none (ch.footers_read()); without the summaries, another fresh backend's read the files' footers
\c
SET extra_float_digits = 0;
SET vexec.mode = off;
DO $$ BEGIN
	EXECUTE 'EXPLAIN SELECT count(*), sum(i4) FROM p WHERE id BETWEEN 140000 AND 150000';
	EXECUTE 'EXPLAIN SELECT count(*), sum(id) FROM a WHERE id BETWEEN 140000 AND 150000';
	EXECUTE 'EXPLAIN SELECT min(d), max(t) FROM p';
END $$;
SELECT ch.footers_read() AS footers_read;
-- pg_accel's §3.32 F11 (VC37): of a summary's columns, the units' ranges of those a filter or min() and max() ask
-- for alone decoded, each once -- the plans above: id's of each table's part, and a column min() and max() answer
-- of p's; a plan with no filter or min() decodes none, nor the same filter's again
SELECT ch.ranges_decoded() AS ranges_decoded;
DO $$ BEGIN
	EXECUTE 'EXPLAIN SELECT count(*), sum(i4) FROM p';
	EXECUTE 'EXPLAIN SELECT count(*), sum(id) FROM p WHERE id BETWEEN 20000 AND 30000';
END $$;
SELECT ch.ranges_decoded() AS ranges_decoded;
-- F13 (VC39): the unit's reader of the row groups a filter keeps given their footer alone, the others' column
-- chunks left out -- the suite's servers compare it with the whole footer (CHTE_SPEC_CHECK) -- its rows as ever
SET max_parallel_workers_per_gather = 0;
SELECT count(*), sum(i4), min(d), max(t) FROM p WHERE id BETWEEN 140000 AND 150000;
SELECT ch.footers_pruned() AS footers_pruned;
RESET max_parallel_workers_per_gather;
\c
SET extra_float_digits = 0;
SET vexec.mode = off;
SET ch.enable_part_summaries = off;
DO $$ BEGIN
	EXECUTE 'EXPLAIN SELECT count(*), sum(i4) FROM p WHERE id BETWEEN 140000 AND 150000';
	EXECUTE 'EXPLAIN SELECT count(*), sum(id) FROM a WHERE id BETWEEN 140000 AND 150000';
END $$;
SELECT ch.footers_read() > 0 AS footers_read;
RESET ch.enable_part_summaries;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT count(*), sum(i4) FROM p WHERE id BETWEEN 140000 AND 150000;
SELECT count(*), sum(i4), min(d), max(t), count(u) FROM p WHERE id BETWEEN 140000 AND 150000;
SELECT count(*), sum(id) FROM p WHERE t = 'own 7' OR ts < TIMESTAMP '1960-01-02';
SELECT count(*), sum(id) FROM a WHERE id BETWEEN 140000 AND 150000;
SET ch.enable_part_summaries = off;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT count(*), sum(i4) FROM p WHERE id BETWEEN 140000 AND 150000;
SELECT count(*), sum(i4), min(d), max(t), count(u) FROM p WHERE id BETWEEN 140000 AND 150000;
SELECT count(*), sum(id) FROM p WHERE t = 'own 7' OR ts < TIMESTAMP '1960-01-02';
SELECT count(*), sum(id) FROM a WHERE id BETWEEN 140000 AND 150000;
RESET ch.enable_part_summaries;

-- an Arrow part's record batches pruned by their ranges, which its summary keeps as Parquet's row groups keep their
-- statistics (VC28): a filter reads the batches it may keep alone, and the answers are heap's; the columns' least and
-- greatest values from the summary, as a Parquet part's from its footer's.  A fresh backend: this one listed the
-- part from its footer above, which an Arrow file's holds no range in
\c
SET extra_float_digits = 0;
SET vexec.mode = off;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT count(*), sum(id) FROM a WHERE id BETWEEN 140000 AND 150000;
SELECT count(*), sum(id), min(d), max(t), count(u) FROM a WHERE id BETWEEN 140000 AND 150000;
SELECT count(*), sum(id) FROM a WHERE t = 'own 72' OR ts < TIMESTAMP '1960-01-02';
SELECT count(*), sum(id) FROM src WHERE t = 'own 72' OR ts < TIMESTAMP '1960-01-02';
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT count(*) FROM a WHERE id > 1000000;

-- Parquet's dictionary filter (VC26): a table of its own Parquet files is read with ClickHouse's dictionary
-- filter limited to ch.own_parquet_dictionary_filter bytes a dictionary page, 64 kB by default, where
-- ClickHouse's reader takes 1 MB; not for Arrow, nor where ch.engine_settings or the table's settings name
-- it; -1 leaves ClickHouse's own limit.  Read as a process opens the table.
\c
SET extra_float_digits = 0;
SET vexec.mode = off;
SELECT ch.read_settings('p') AS parquet, ch.read_settings('a') AS arrow;
SELECT count(*), sum(id) FROM p WHERE t = 'own 72' OR t = 'own 73ü';
\c
SET extra_float_digits = 0;
SET vexec.mode = off;
SET ch.own_parquet_dictionary_filter = '1MB';
SELECT ch.read_settings('p') AS parquet;
\c
SET extra_float_digits = 0;
SET vexec.mode = off;
SET ch.own_parquet_dictionary_filter = -1;
SELECT ch.read_settings('p') AS parquet;
\c
SET extra_float_digits = 0;
SET vexec.mode = off;
SET ch.engine_settings = 'input_format_parquet_dictionary_filter_push_down = 0';
SELECT ch.read_settings('p') AS parquet;
SELECT count(*), sum(id) FROM p WHERE t = 'own 72' OR t = 'own 73ü';
\c
SET extra_float_digits = 0;
SET vexec.mode = off;
CREATE TABLE pd (id int, t text) USING parquet WITH (settings = 'input_format_parquet_dictionary_filter_push_down = 0');
SELECT ch.read_settings('pd') AS parquet;
DROP TABLE pd;

-- the codecs: zstd and none, and lz4 frames for Arrow; a codec changed for the next parts
CREATE TABLE pz (id int, t text) USING parquet WITH (codec = 'zstd');
CREATE TABLE pn (id int, t text) USING parquet WITH (codec = 'none');
CREATE TABLE al (id int, t text) USING arrow WITH (codec = 'lz4');
CREATE TABLE az (id int, t text) USING arrow WITH (codec = 'zstd');
INSERT INTO pz SELECT g, 't' || g FROM generate_series(1, 1000) g;
INSERT INTO pn SELECT g, 't' || g FROM generate_series(1, 1000) g;
INSERT INTO al SELECT g, 't' || g FROM generate_series(1, 1000) g;
INSERT INTO az SELECT g, 't' || g FROM generate_series(1, 1000) g;
SELECT 'pz' AS t, codec FROM ch.own_files('pz') UNION ALL SELECT 'pn', codec FROM ch.own_files('pn')
UNION ALL SELECT 'al', codec FROM ch.own_files('al') UNION ALL SELECT 'az', codec FROM ch.own_files('az');
ALTER TABLE pn SET (codec = 'zstd');
INSERT INTO pn SELECT g, 't' || g FROM generate_series(1001, 1010) g;
SELECT part, rows, codec FROM ch.own_files('pn') ORDER BY part;
SELECT count(*), sum(id) FROM pn;

-- every other type as the library keeps it (§2.5): its bytes where no file type holds it exactly -- a
-- numeric of no precision with its NaN, varchar(n), interval, "char", time, an array, a domain, name
CREATE DOMAIN posint AS int CHECK (VALUE > 0);
CREATE TABLE ot (id int NOT NULL, n numeric, vc varchar(5), iv interval, c "char", tm time, ar int[], dm posint,
				 nm name) USING parquet;
CREATE TABLE ota (LIKE ot) USING arrow;
INSERT INTO ot SELECT g, g / 7.0 * 1e20, 'v' || g % 100, g * INTERVAL '1 minute 1.5 seconds', chr(65 + g % 26)::"char",
	   TIME '00:00' + g * INTERVAL '1.25 seconds', ARRAY[g, NULL, g + 1], g, 'n' || g
FROM generate_series(1, 1000) g;
INSERT INTO ot VALUES (0, 'NaN', NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO ota SELECT * FROM ot;
CREATE TABLE ot_heap AS SELECT * FROM ot;
SELECT rows_digest('ot') = rows_digest('ot_heap') AS parquet_same, rows_digest('ota') = rows_digest('ot_heap') AS arrow_same;
SELECT * FROM ot WHERE id IN (0, 1, 500) ORDER BY id;
\set VERBOSITY terse
INSERT INTO ot (id, vc) VALUES (2000, 'too long');
INSERT INTO ot (id, dm) VALUES (2001, -1);
\set VERBOSITY default
DROP TABLE ot, ota, ot_heap;
DROP DOMAIN posint;

-- what does not apply
\set VERBOSITY terse
CREATE TABLE bad (id int) USING parquet WITH (codec = 'snappy');
CREATE TABLE bad (id int) USING parquet WITH (location = '/tmp/x.parquet', codec = 'lz4');
CREATE TABLE bad (id int) USING iceberg;
ALTER TABLE p SET (location = '/tmp/x.parquet');
\set VERBOSITY default

-- ADD COLUMN: a part written before it lacks the column, its rows the column's missing value -- NULL, or the
-- DEFAULT it was added with -- as scans, fetches and vexec read them, and as a merge writes them into its part
CREATE TABLE ac (id int NOT NULL, t text) USING parquet;
CREATE TABLE aca (id int NOT NULL, t text) USING arrow;
INSERT INTO ac SELECT g, 't' || g FROM generate_series(1, 1000) g;
INSERT INTO aca SELECT g, 't' || g FROM generate_series(1, 1000) g;
ALTER TABLE ac ADD COLUMN x int, ADD COLUMN y int NOT NULL DEFAULT 42, ADD COLUMN z text DEFAULT 'zed';
ALTER TABLE aca ADD COLUMN x int, ADD COLUMN y int NOT NULL DEFAULT 42, ADD COLUMN z text DEFAULT 'zed';
INSERT INTO ac SELECT g, 't' || g, g, g * 2, 'new' FROM generate_series(1001, 1010) g;
INSERT INTO aca SELECT g, 't' || g, g, g * 2, 'new' FROM generate_series(1001, 1010) g;
SELECT count(*), count(x), sum(y), count(z), min(z), max(z) FROM ac;
SELECT count(*), count(x), sum(y), count(z), min(z), max(z) FROM aca;
SELECT * FROM ac WHERE id IN (1, 1001) ORDER BY id;
SET vexec.mode = force;
SELECT y, z, count(*) FROM ac GROUP BY y, z ORDER BY y, z LIMIT 3;
SELECT y, z, count(*) FROM aca GROUP BY y, z ORDER BY y, z LIMIT 3;
-- a qual and other columns, which vexec's scan asks in steps (VC2): read in one step, as an external table's,
-- the steps being a MergeTree part's granules'
SELECT count(*), sum(x), sum(y), min(z), max(t) FROM ac WHERE id > 500;
SELECT count(*), sum(x), sum(y), min(z), max(t) FROM aca WHERE id > 500;
SELECT t, count(*) FROM ac WHERE t LIKE 't100%' GROUP BY t ORDER BY t LIMIT 3;
SET vexec.mode = off;
CREATE INDEX ac_id ON ac (id);
SET enable_seqscan = off;
SELECT * FROM ac WHERE id IN (5, 1005) ORDER BY id;
RESET enable_seqscan;
SELECT ch.optimize('ac', true) AS merged, ch.optimize('aca', true) AS merged_arrow;
SELECT count(*), count(x), sum(y), count(z), min(z), max(z) FROM ac;
SELECT count(*), count(x), sum(y), count(z), min(z), max(z) FROM aca;
UPDATE ac SET z = NULL WHERE id = 2;
ALTER TABLE ac DROP COLUMN t;
SELECT * FROM ac WHERE id IN (1, 2, 1001) ORDER BY id;
DROP TABLE ac, aca;

-- an Arrow part's rows fetched by their TIDs (F6, VC27): each row's values alone read from its uncompressed record
-- batch -- validity bits, values, offsets and bytes -- and decoded as the whole batch's would be: every type, NULLs
-- among them, at the batches' edges, as heap's; a compressed batch's columns read whole
\c
SET extra_float_digits = 0;
SET vexec.mode = off;
CREATE INDEX a_id ON a (id);
CREATE INDEX al_id ON al (id);
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT ch.rows_read_alone() AS before;
SELECT (SELECT md5(string_agg(x::text, ',' ORDER BY x.id)) FROM a x WHERE id IN (1, 9, 13, 131072, 131073, 299999, 300000)) =
	   (SELECT md5(string_agg(y::text, ',' ORDER BY y.id)) FROM src y WHERE id IN (1, 9, 13, 131072, 131073, 299999, 300000))
	   AS same;
SELECT * FROM a WHERE ctid = (SELECT ctid FROM a WHERE id = 117);
SELECT ch.rows_read_alone() >= 8 AS read_alone;
SELECT id, t FROM al WHERE id IN (5, 999) ORDER BY id;
SELECT ch.rows_read_alone() >= 8 AS read_alone;
RESET enable_bitmapscan;
RESET enable_seqscan;
DROP INDEX a_id, al_id;

-- DELETE and UPDATE: tombstones, the new versions a part of their own
DELETE FROM p WHERE id % 3 = 0;
DELETE FROM src WHERE id % 3 = 0;
UPDATE p SET i4 = -i4, t = t || ' updated' WHERE id % 1000 = 1;
UPDATE src SET i4 = -i4, t = t || ' updated' WHERE id % 1000 = 1;
SELECT rows_digest('p') = rows_digest('src') AS same;
SELECT part, rows, min_rowid, max_rowid, units FROM ch.own_files('p') ORDER BY part;
SELECT count(*) AS tombstones FROM ch.tombstones WHERE relfilenumber = pg_relation_filenode('p');

-- a transaction's own rows, savepoints, a rollback
BEGIN;
INSERT INTO p SELECT * FROM src WHERE id <= 10;
SELECT count(*) FROM p WHERE id <= 10;
SAVEPOINT s;
DELETE FROM p WHERE id <= 5;
SELECT count(*) FROM p WHERE id <= 10;
ROLLBACK TO s;
SELECT count(*) FROM p WHERE id <= 10;
ROLLBACK;
SELECT count(*) FROM p WHERE id <= 10;

-- TIDs: a row found again by its ctid
SELECT id, i4 FROM p WHERE ctid = (SELECT ctid FROM p WHERE id = 1001);
SELECT count(*) FROM p WHERE ctid IN (SELECT ctid FROM p WHERE id BETWEEN 2000 AND 2100);

-- indexes: index and bitmap scans after the updates and deletes
CREATE INDEX p_id ON p (id);
CREATE INDEX p_t ON p (t);
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF) SELECT id, i4, t FROM p WHERE id = 1001;
SELECT id, i4, t FROM p WHERE id IN (1001, 1002, 1003) ORDER BY id;
SELECT id FROM p WHERE t = 'own 4' || repeat('ü', 0);
RESET enable_bitmapscan;
SET enable_indexscan = off;
EXPLAIN (COSTS OFF) SELECT count(*) FROM p WHERE id BETWEEN 100 AND 200;
SELECT count(*), sum(id) FROM p WHERE id BETWEEN 100 AND 200;
RESET enable_indexscan;
RESET enable_seqscan;

-- merges: the parts below 8M rows into one, its rows in their ids' order, the dead ones folded out with
-- their tombstones and index entries
SELECT ch.optimize('p', false) AS merged;
SELECT part, rows, units, min_unit_rows, max_unit_rows, codec, page_index FROM ch.own_files('p');
SELECT count(*) AS tombstones FROM ch.tombstones WHERE relfilenumber = pg_relation_filenode('p');
SELECT rows_digest('p') = rows_digest('src') AS same;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT id, i4 FROM p WHERE id IN (3, 1001, 299999, 300000) ORDER BY id;
RESET enable_bitmapscan;
RESET enable_seqscan;

-- ch.fold(): one part, its dead rows written out of it
DELETE FROM p WHERE id % 5 = 0;
DELETE FROM src WHERE id % 5 = 0;
SELECT ch.fold('p') AS folded;
SELECT part, rows, units FROM ch.own_files('p');
SELECT count(*) AS tombstones FROM ch.tombstones WHERE relfilenumber = pg_relation_filenode('p');
SELECT rows_digest('p') = rows_digest('src') AS same;
-- a merged and a folded part summarized too (VC25)
SELECT count(*) AS parts, count(summary) AS summarized FROM ch.parts WHERE relfilenumber = pg_relation_filenode('p');

-- Arrow: the same DML and merges
DELETE FROM a WHERE id % 3 = 0;
UPDATE a SET i4 = -i4, t = t || ' updated' WHERE id % 1000 = 1;
DELETE FROM a WHERE id % 5 = 0;
SELECT rows_digest('a') = rows_digest('src') AS same;
SELECT ch.optimize('a', true) AS merged;
SELECT part, rows, units, min_unit_rows, max_unit_rows, codec FROM ch.own_files('a');
SELECT rows_digest('a') = rows_digest('src') AS same;
-- the writers and the merges on the caller's thread (§1.2 rule 4): the library's threads as they were
SELECT threads FROM ch.engine_info();

-- COPY: its rows, in and out, as any table's
COPY (SELECT * FROM p WHERE id < 20 ORDER BY id) TO '/tmp/chte_own_copy.csv' WITH (FORMAT csv);
CREATE TABLE pc (LIKE src) USING parquet;
COPY pc FROM '/tmp/chte_own_copy.csv' WITH (FORMAT csv);
SELECT count(*), sum(id) FROM pc;
COPY pc TO STDOUT WITH (FORMAT csv);

-- CREATE TABLE AS, a rewrite (VACUUM FULL, ALTER TABLE ... TYPE), TRUNCATE, VACUUM
CREATE TABLE pt USING parquet AS SELECT id, t FROM src WHERE id < 1000;
SELECT count(*), sum(id) FROM pt;
DELETE FROM pt WHERE id < 100;
VACUUM FULL pt;
SELECT count(*), sum(id) FROM pt;
SELECT count(*) AS tombstones FROM ch.tombstones WHERE relfilenumber = pg_relation_filenode('pt');
ALTER TABLE pt ALTER COLUMN id TYPE int4;
SELECT count(*), sum(id) FROM pt;
SELECT pg_typeof(id) FROM pt LIMIT 1;
TRUNCATE pt;
SELECT count(*) FROM pt;
SELECT count(*) FROM ch.own_files('pt');
INSERT INTO pt SELECT g, 'again' FROM generate_series(1, 10) g;
VACUUM pt;
SELECT count(*), max(t) FROM pt;

-- temporary and unlogged tables
CREATE TEMP TABLE tmp (id int, t text) USING parquet;
INSERT INTO tmp SELECT g, 't' || g FROM generate_series(1, 1000) g;
DELETE FROM tmp WHERE id > 10;
SELECT count(*), sum(id) FROM tmp;
CREATE UNLOGGED TABLE ul (id int, t text) USING arrow;
INSERT INTO ul SELECT g, 't' || g FROM generate_series(1, 1000) g;
UPDATE ul SET t = 'u' WHERE id = 7;
SELECT count(*), max(t) FROM ul;

-- a parallel scan's participants read the parts' files
SET max_parallel_workers_per_gather = 2;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SELECT count(*), sum(id), count(t) FROM p;
RESET max_parallel_workers_per_gather;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET min_parallel_table_scan_size;

DROP TABLE p, a, pz, pn, al, az, pc, pt, tmp, ul;
SELECT count(*) FROM ch.tables t LEFT JOIN pg_class c ON c.oid = t.relid WHERE c.oid IS NULL;
DROP FUNCTION rows_digest(regclass);
DROP TABLE src;
\! rm -f /tmp/chte_own_copy.csv
