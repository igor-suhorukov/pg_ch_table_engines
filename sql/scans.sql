-- Scans: sequential, parallel, by TID; ANALYZE (ch_table_engines.md §2.8, §2.13).
SET client_min_messages = warning;
CREATE TABLE sc (a int, b text, c float8) USING mergetree WITH (order_by = 'a');
SET ch.insert_buffer_rows = 20000;
INSERT INTO sc SELECT g, md5(g::text), g / 7.0 FROM generate_series(1, 100000) g;
RESET ch.insert_buffer_rows;
SELECT count(*) AS parts FROM ch.parts('sc');

-- a parallel scan shares out the parts and reads every row once
SET ch.enable_chscan = off;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 4;
EXPLAIN (COSTS OFF) SELECT count(*), sum(a) FROM sc;
SELECT count(*), sum(a), sum(length(b)) FROM sc;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET min_parallel_table_scan_size;
RESET max_parallel_workers_per_gather;
RESET ch.enable_chscan;

-- a TID scan fetches a row by its row id
SELECT ctid AS tid FROM sc WHERE a = 54321 \gset
SELECT a, b FROM sc WHERE ctid = :'tid';

-- ANALYZE counts the parts' rows, whole or sampled
ANALYZE sc;
SELECT reltuples FROM pg_class WHERE relname = 'sc';
SET default_statistics_target = 10;
ANALYZE sc;
SELECT reltuples FROM pg_class WHERE relname = 'sc';
-- a sample of 3,000 rows of 100,000 spread over the table, not its first rows
SELECT (histogram_bounds::text::int[])[1] < 5000 AS low, (histogram_bounds::text::int[])[11] > 95000 AS high
  FROM pg_stats WHERE tablename = 'sc' AND attname = 'a';
RESET default_statistics_target;
ANALYZE sc;
SELECT attname, null_frac, n_distinct FROM pg_stats WHERE tablename = 'sc' ORDER BY attname;
SET ch.enable_chscan = off;
EXPLAIN (COSTS ON) SELECT * FROM sc WHERE a < 0;
RESET ch.enable_chscan;

-- joins, sorts and aggregates over it, as over heap
CREATE TABLE sh AS SELECT * FROM sc;
SELECT count(*) FROM sc JOIN sh USING (a, b, c);
SELECT (SELECT md5(string_agg(b, ',' ORDER BY a)) FROM sc) = (SELECT md5(string_agg(b, ',' ORDER BY a)) FROM sh) AS same;

-- a table never merged reads its rows in the order they entered, as heap's
-- does: its parts in their rows' order, not their names' (all_10_10_0 sorts
-- before all_2_2_0); and pgstat counts its rows as heap's
CREATE TABLE ord (a int) USING mergetree;
DO $$ BEGIN FOR i IN 1..12 LOOP INSERT INTO ord VALUES (i); END LOOP; END $$;
SELECT count(*) AS parts FROM ch.parts('ord');
SELECT string_agg(a::text, ',') FROM ord;
SET ch.enable_chscan = off;
SELECT string_agg(a::text, ',') FROM ord;
RESET ch.enable_chscan;
SELECT pg_stat_force_next_flush();
SELECT n_tup_ins, seq_scan > 0 AS scanned FROM pg_stat_user_tables WHERE relname = 'ord';
DROP TABLE ord;

DROP TABLE sc, sh;
