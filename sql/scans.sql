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
-- VC42 (pg_accel's §3.38): on, each participant claims the units of a part of
-- its own first, then of the part with the most left; off, the default, every
-- participant the next unit of all: every row read once either way
SELECT count(*), sum(a), sum(length(b)) FROM sc WHERE a % 3 <> 1;
SET ch.parallel_part_affinity = on;
SELECT count(*), sum(a), sum(length(b)) FROM sc;
SELECT count(*), sum(a), sum(length(b)) FROM sc WHERE a % 3 <> 1;
RESET ch.parallel_part_affinity;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET min_parallel_table_scan_size;
RESET max_parallel_workers_per_gather;
RESET ch.enable_chscan;
-- its units (VC36): each the granules left to claim over 34 -- smaller toward the
-- scan's end -- within a least of 64K rows, 8 of a mergetree part's granules, and
-- a most of a million; an Arrow file's least one record batch, as a table of its
-- own Parquet files' one row group (VC39), a Parquet file's reader's of its whole
-- footer, a table of files not its own, eight row groups; every granule in one
-- unit, a part's last cut short
SELECT string_agg(granules::text, ' ' ORDER BY part, first_granule) AS units
  FROM ch.parallel_units(ARRAY[1164, 40, 18]);
SELECT part, count(*) AS units, min(granules), max(granules), sum(granules)
  FROM ch.parallel_units(ARRAY[1164, 40, 18]) GROUP BY part ORDER BY part;
SELECT count(*) AS units, min(granules), max(granules), sum(granules)
  FROM ch.parallel_units(ARRAY[63, 15], 131072);
SELECT count(*) AS units, min(granules), max(granules), sum(granules)
  FROM ch.parallel_units(ARRAY[63, 15], 131072, true);
SELECT * FROM ch.parallel_units(ARRAY[0, 3, 200000], 8192) WHERE part < 3 OR first_granule = 0;
SELECT * FROM ch.parallel_units(ARRAY[-1]);

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

-- VC41 (pg_accel's §3.38): a scan's part readers over its mark ranges --
-- two runs of the part's granules here, a reader each -- take a wide part's
-- marks from the process's mark cache, where the scan warmed them as it
-- began: a second scan of the same columns decompresses none of them.  Off,
-- each reader decompresses its own, a second scan's as the first's did.
-- Each in the session's own process, no worker beside it
CREATE TABLE sm (k int, a int8, n int) USING mergetree
  WITH (order_by = 'k', index_granularity = 1024, settings = 'min_bytes_for_wide_part = 0');
INSERT INTO sm SELECT i, i * 7, i % 13 FROM generate_series(1, 10000) i;
INSERT INTO sm SELECT i, i * 7, i % 13 FROM generate_series(10001, 20000) i;
SELECT ch.optimize('sm', true) > 0 AS merged;
\c
SET max_parallel_workers_per_gather = 0;
SET ch.enable_scan_marks = off;
SELECT sum(a) FROM sm WHERE k BETWEEN 1000 AND 3000 OR k BETWEEN 15000 AND 17000;
SELECT ch.marks_loaded() AS m0 \gset
SELECT sum(a) FROM sm WHERE k BETWEEN 1000 AND 3000 OR k BETWEEN 15000 AND 17000;
SELECT ch.marks_loaded() - :m0 > 0 AS loaded_again;
\c
SET max_parallel_workers_per_gather = 0;
SELECT ch.marks_loaded() AS m0 \gset
SELECT sum(a) FROM sm WHERE k BETWEEN 1000 AND 3000 OR k BETWEEN 15000 AND 17000;
SELECT ch.marks_loaded() - :m0 > 0 AS loaded_first;
SELECT ch.marks_loaded() AS m1 \gset
SELECT sum(a) FROM sm WHERE k BETWEEN 1000 AND 3000 OR k BETWEEN 15000 AND 17000;
SELECT ch.marks_loaded() - :m1 AS loaded_again;
DROP TABLE sm;

DROP TABLE sc, sh;
