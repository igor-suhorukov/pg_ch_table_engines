-- A part's decompressed marks kept in shared memory (ch_table_engines.md VC42's
-- marks, pg_accel's §3.38): the first process to read a stream of a part with
-- marks to seek by decompresses them and keeps them, and the processes after it
-- make their mark cache's entry of that and read no marks file; every answer
-- the same with the setting off, as it is by default.
SET client_min_messages = warning;
ALTER ROLE CURRENT_USER SET ch.enable_shared_marks = on;
SET max_parallel_workers_per_gather = 0;
SET ch.insert_buffer_rows = 20000;
CREATE TABLE pmarks (a int NOT NULL, b int NOT NULL, s text NOT NULL, n int, lc text NOT NULL)
  USING mergetree WITH (order_by = 'a', storage = 'pages', low_cardinality = 'lc',
                        settings = 'index_granularity = 512, min_bytes_for_wide_part = 0');
INSERT INTO pmarks SELECT i, i % 1000, 'v' || (i % 97), CASE WHEN i % 7 = 0 THEN NULL ELSE i END, 'c' || (i % 5)
  FROM generate_series(1, 80000) i;
RESET ch.insert_buffer_rows;
SELECT count(*) AS parts FROM ch.parts('pmarks');
CREATE TABLE pmarks_want AS
  SELECT b, count(*) AS c, sum(a) AS sa, sum(n) AS sn, max(s) AS ms, max(lc) AS mlc
  FROM pmarks WHERE a BETWEEN 1000 AND 70000 AND a % 3 = 0 GROUP BY b;
SELECT count(*) > 0 AS some FROM pmarks_want;
-- a new process's mark cache is empty: the first's read of a range keeps its streams' marks
\c
SET max_parallel_workers_per_gather = 0;
SELECT ch.marks_loaded() AS m0 \gset
SELECT count(*) = (SELECT count(*) FROM pmarks_want) AS same
  FROM (SELECT b, count(*) AS c, sum(a) AS sa, sum(n) AS sn, max(s) AS ms, max(lc) AS mlc
        FROM pmarks WHERE a BETWEEN 1000 AND 70000 AND a % 3 = 0 GROUP BY b) r
  JOIN pmarks_want w ON r.b = w.b AND r.c = w.c AND r.sa = w.sa AND r.sn IS NOT DISTINCT FROM w.sn AND r.ms = w.ms AND r.mlc = w.mlc;
SELECT entries > 0 AS kept_or_there, bytes > 0 AS bytes, found + kept > 0 AS found_or_kept FROM ch.part_marks();
-- the next process's the same marks, found, none of its files read
\c
SET max_parallel_workers_per_gather = 0;
SELECT ch.marks_loaded() AS m0 \gset
SELECT count(*) = (SELECT count(*) FROM pmarks_want) AS same
  FROM (SELECT b, count(*) AS c, sum(a) AS sa, sum(n) AS sn, max(s) AS ms, max(lc) AS mlc
        FROM pmarks WHERE a BETWEEN 1000 AND 70000 AND a % 3 = 0 GROUP BY b) r
  JOIN pmarks_want w ON r.b = w.b AND r.c = w.c AND r.sa = w.sa AND r.sn IS NOT DISTINCT FROM w.sn AND r.ms = w.ms AND r.mlc = w.mlc;
SELECT ch.marks_loaded() - :m0 AS files_read, found > 0 AS found, kept AS kept FROM ch.part_marks();
-- off, its files'
\c
SET max_parallel_workers_per_gather = 0;
SET ch.enable_shared_marks = off;
SELECT ch.marks_loaded() AS m0 \gset
SELECT count(*) = (SELECT count(*) FROM pmarks_want) AS same
  FROM (SELECT b, count(*) AS c, sum(a) AS sa, sum(n) AS sn, max(s) AS ms, max(lc) AS mlc
        FROM pmarks WHERE a BETWEEN 1000 AND 70000 AND a % 3 = 0 GROUP BY b) r
  JOIN pmarks_want w ON r.b = w.b AND r.c = w.c AND r.sa = w.sa AND r.sn IS NOT DISTINCT FROM w.sn AND r.ms = w.ms AND r.mlc = w.mlc;
SELECT ch.marks_loaded() - :m0 > 0 AS files_read, found, kept FROM ch.part_marks();
RESET ch.enable_shared_marks;
-- a part merged, a new name: its own marks, and no one's old
SELECT ch.optimize('pmarks', true);
\c
SET max_parallel_workers_per_gather = 0;
SELECT count(*) = (SELECT count(*) FROM pmarks_want) AS same
  FROM (SELECT b, count(*) AS c, sum(a) AS sa, sum(n) AS sn, max(s) AS ms, max(lc) AS mlc
        FROM pmarks WHERE a BETWEEN 1000 AND 70000 AND a % 3 = 0 GROUP BY b) r
  JOIN pmarks_want w ON r.b = w.b AND r.c = w.c AND r.sa = w.sa AND r.sn IS NOT DISTINCT FROM w.sn AND r.ms = w.ms AND r.mlc = w.mlc;
\c
SET max_parallel_workers_per_gather = 0;
SELECT count(*) = (SELECT count(*) FROM pmarks_want) AS same
  FROM (SELECT b, count(*) AS c, sum(a) AS sa, sum(n) AS sn, max(s) AS ms, max(lc) AS mlc
        FROM pmarks WHERE a BETWEEN 1000 AND 70000 AND a % 3 = 0 GROUP BY b) r
  JOIN pmarks_want w ON r.b = w.b AND r.c = w.c AND r.sa = w.sa AND r.sn IS NOT DISTINCT FROM w.sn AND r.ms = w.ms AND r.mlc = w.mlc;
SELECT found > 0 AS found FROM ch.part_marks();
ALTER ROLE CURRENT_USER RESET ch.enable_shared_marks;
DROP TABLE pmarks, pmarks_want;
