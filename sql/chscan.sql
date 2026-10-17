-- ChScan (ch_table_engines.md §2.8): the columns a scan needs, and the
-- granules its quals keep by ClickHouse's analysis of the key and of the
-- parts' statistics; every answer the same as the plain scan's.
SET client_min_messages = warning;
SET ch.insert_buffer_rows = 20000;
CREATE TABLE cs (a int, b text, c numeric(10, 2), d date, e bool, f int8, g float8)
  USING mergetree WITH (order_by = 'a', settings = 'index_granularity = 1024');
-- five parts of 20,000 rows, a's ranges apart: 1-20000, 20001-40000, ...
INSERT INTO cs SELECT i, 'k' || (i % 100), i / 4.0, date '2020-01-01' + i / 100, i % 3 = 0,
    CASE WHEN i % 10 = 0 THEN NULL ELSE i * 1000 END, i / 8.0
  FROM generate_series(1, 100000) i;
RESET ch.insert_buffer_rows;
SELECT count(*) AS parts, sum(rows) AS rows FROM ch.parts('cs');

-- every query answered by ChScan and by the plain scan, the same
CREATE FUNCTION same_answer(q text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  with_chscan text;
  without text;
BEGIN
  SET LOCAL ch.enable_chscan = on;
  EXECUTE 'SELECT md5(coalesce(string_agg(r::text, '','' ORDER BY r::text), '''')) FROM (' || q || ') r'
    INTO with_chscan;
  SET LOCAL ch.enable_chscan = off;
  EXECUTE 'SELECT md5(coalesce(string_agg(r::text, '','' ORDER BY r::text), '''')) FROM (' || q || ') r'
    INTO without;
  RETURN CASE WHEN with_chscan = without THEN 'same' ELSE 'DIFFERENT' END;
END $$;
-- what ChScan read, from EXPLAIN ANALYZE, whatever the workers launched
CREATE FUNCTION granules(q text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  plan jsonb;
  node jsonb;
BEGIN
  EXECUTE 'EXPLAIN (ANALYZE, FORMAT JSON) ' || q INTO plan;
  node := jsonb_path_query_first(plan, 'strict $.**? (@."Custom Plan Provider" == "ChScan")');
  RETURN format('%s read of %s, parts pruned: %s', node->>'Granules Read', node->>'Granules', node->>'Parts Pruned');
END $$;

-- the key: a range of granules of one part; the other parts ruled out by their statistics
EXPLAIN (COSTS OFF) SELECT count(*), sum(c) FROM cs WHERE a BETWEEN 1000 AND 2000;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
  SELECT count(*), sum(c) FROM cs WHERE a BETWEEN 1000 AND 2000;
SELECT count(*), sum(c) FROM cs WHERE a BETWEEN 1000 AND 2000;
SELECT same_answer('SELECT a, b, c FROM cs WHERE a BETWEEN 1000 AND 2000');

-- comparisons of every kind, constant first or last, and the rows they keep
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) SELECT a FROM cs WHERE a = 54321;
SELECT same_answer('SELECT a FROM cs WHERE a = 54321');
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) SELECT a FROM cs WHERE 99990 < a;
SELECT same_answer('SELECT a FROM cs WHERE 99990 < a');
SELECT same_answer('SELECT a FROM cs WHERE a <= 3');
SELECT same_answer('SELECT a FROM cs WHERE a >= 99998');
SELECT same_answer('SELECT count(*) FROM cs WHERE a <> 7');
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
  SELECT a FROM cs WHERE a IN (5, 50000, 99999);
SELECT same_answer('SELECT a, b FROM cs WHERE a IN (5, 50000, 99999)');
SELECT same_answer('SELECT a FROM cs WHERE a = ANY (ARRAY[1, NULL, 77777])');
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
  SELECT a FROM cs WHERE a < 100 OR a > 99900;
SELECT same_answer('SELECT a FROM cs WHERE a < 100 OR a > 99900');
-- an OR with a branch that cannot be translated prunes nothing
EXPLAIN (COSTS OFF) SELECT a FROM cs WHERE a < 100 OR g > 5;
SELECT same_answer('SELECT a FROM cs WHERE a < 100 OR g > 5');
-- nor one whose branch is a column with no least and greatest values kept
SELECT granules('SELECT a FROM cs WHERE a < 100 OR b = ''k5''');
SELECT same_answer('SELECT a FROM cs WHERE a < 100 OR b = ''k5''');

-- across integer widths; constants a column cannot hold
SELECT same_answer('SELECT a FROM cs WHERE a = 54321::int8');
SELECT same_answer('SELECT a FROM cs WHERE a < 10::int2');
EXPLAIN (COSTS OFF) SELECT a FROM cs WHERE a > 10000000000;
SELECT same_answer('SELECT a FROM cs WHERE a > 10000000000');
SELECT same_answer('SELECT a FROM cs WHERE a = ANY (ARRAY[3, 10000000000])');

-- columns that are not the key: their parts' statistics
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) SELECT count(*) FROM cs WHERE f > 99000000;
SELECT same_answer('SELECT a, f FROM cs WHERE f > 99000000');
SELECT same_answer('SELECT count(*) FROM cs WHERE f IS NULL');
SELECT same_answer('SELECT count(*) FROM cs WHERE f IS NOT NULL AND a < 50');
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) SELECT count(*) FROM cs WHERE c < 100;
SELECT same_answer('SELECT a, c FROM cs WHERE c < 100');
-- numeric constants: exact at the column's scale, or one no value can equal
SELECT same_answer('SELECT a FROM cs WHERE c = 2.25');
SELECT same_answer('SELECT a FROM cs WHERE c = 2.251');
SELECT same_answer('SELECT a FROM cs WHERE c > 24999.75');
SELECT same_answer('SELECT a FROM cs WHERE c > 1e30');
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
  SELECT count(*) FROM cs WHERE d >= date '2022-09-01';
SELECT same_answer('SELECT a, d FROM cs WHERE d >= date ''2022-09-01''');
SELECT same_answer('SELECT a FROM cs WHERE e AND a < 30');
SELECT same_answer('SELECT a FROM cs WHERE NOT e AND a < 30');
-- text: equality under the database's collation; floats are left to the executor
SELECT same_answer('SELECT count(*) FROM cs WHERE b = ''k42''');
SELECT same_answer('SELECT count(*) FROM cs WHERE b < ''k2'' COLLATE "C"');
EXPLAIN (COSTS OFF) SELECT a FROM cs WHERE g > 12000;
SELECT same_answer('SELECT a FROM cs WHERE g > 12000');
SELECT same_answer('SELECT a FROM cs WHERE g = ''NaN''::float8');

-- the columns read: none for count(*), the row id for ctid
EXPLAIN (COSTS OFF) SELECT count(*) FROM cs;
SELECT count(*) FROM cs;
EXPLAIN (COSTS OFF) SELECT ctid, a FROM cs WHERE a = 77;
SELECT ctid AS tid FROM cs WHERE a = 77 \gset
SELECT a FROM cs WHERE ctid = :'tid';
EXPLAIN (COSTS OFF) SELECT cs FROM cs WHERE a = 1;
SELECT cs FROM cs WHERE a = 1;

-- a cursor's scan gives its rows' TIDs, whatever it names: WHERE CURRENT OF takes them
BEGIN;
DECLARE c CURSOR FOR SELECT a FROM cs WHERE a < 3;
FETCH 1 FROM c;
DELETE FROM cs WHERE CURRENT OF c;
ROLLBACK;

-- NULL constants, stable functions and parameters prune nothing
SELECT same_answer('SELECT a FROM cs WHERE a = NULL::int');
EXPLAIN (COSTS OFF) SELECT a FROM cs WHERE d > now()::date - 10000;
PREPARE p(int) AS SELECT count(*) FROM cs WHERE a < $1;
EXECUTE p(10);
SET plan_cache_mode = force_generic_plan;
EXPLAIN (COSTS OFF) EXECUTE p(10);
EXECUTE p(10);
RESET plan_cache_mode;
DEALLOCATE p;

-- the inner side of a nested loop, rescanned for each outer row
CREATE TABLE outer_keys (k int);
INSERT INTO outer_keys VALUES (10), (20000), (99999);
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;
EXPLAIN (COSTS OFF) SELECT k, a FROM outer_keys JOIN cs ON a = k;
SELECT k, a FROM outer_keys JOIN cs ON a = k ORDER BY k;
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_material;

-- a column added after the parts were written: their rows have its default, and no statistics
ALTER TABLE cs ADD COLUMN h int DEFAULT 7;
INSERT INTO cs (a, h) VALUES (100001, 8);
SELECT same_answer('SELECT count(*), sum(h) FROM cs WHERE h = 7');
SELECT same_answer('SELECT a, h FROM cs WHERE h > 7');

-- parallel: the participants share the units, and EXPLAIN ANALYZE adds up what they read
ALTER TABLE cs SET (parallel_workers = 4);
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 4;
EXPLAIN (COSTS OFF) SELECT count(*), sum(c) FROM cs WHERE a > 50000;
SELECT granules('SELECT count(*), sum(c) FROM cs WHERE a > 50000');
SELECT count(*), sum(c) FROM cs WHERE a > 50000;
SELECT same_answer('SELECT a, b, c, d, e, f, g, h FROM cs WHERE a > 50000');
SELECT same_answer('SELECT count(*), sum(a) FROM cs');
-- the workers, without the option: as a SeqScan's over a heap table of the
-- rows read, the columns read in them -- a count(*) reads no column, but
-- 100,001 rows, 2.7 MB in heap's tuples
ALTER TABLE cs RESET (parallel_workers);
SET min_parallel_table_scan_size = '1MB';
EXPLAIN (COSTS OFF) SELECT count(*) FROM cs;
SET min_parallel_table_scan_size = '4MB';
EXPLAIN (COSTS OFF) SELECT count(*) FROM cs;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET min_parallel_table_scan_size;
RESET max_parallel_workers_per_gather;

-- the files store answers alike
CREATE TABLE cf (a int, b text) USING mergetree WITH (storage = 'files', order_by = 'a',
  settings = 'index_granularity = 1024');
INSERT INTO cf SELECT i, 'v' || i FROM generate_series(1, 50000) i;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) SELECT b FROM cf WHERE a = 4096;
SELECT b FROM cf WHERE a = 4096;
SELECT same_answer('SELECT a, b FROM cf WHERE a BETWEEN 300 AND 5000');

-- off, the plain scan
SET ch.enable_chscan = off;
EXPLAIN (COSTS OFF) SELECT a FROM cs WHERE a = 5;
RESET ch.enable_chscan;

DROP TABLE cs, cf, outer_keys;
DROP FUNCTION same_answer(text), granules(text);
