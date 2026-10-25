-- The granules a filter keeps of a part, kept in shared memory (ch_table_engines.md
-- §3, VC31): a process that plans or scans a part under a filter its summary
-- cannot decide -- the key keeps some of the part's granules -- asks the engine
-- library once, and the processes after it find the answer kept and load no
-- part for it; every answer the same with the selections off.  With
-- CHTE_SPEC_CHECK, the suite's, each answer found is compared with the
-- library's, which loads the part.
SET client_min_messages = warning;
SET max_parallel_workers_per_gather = 0;
SET ch.insert_buffer_rows = 20000;
CREATE TABLE sel (a int NOT NULL, b int, t text)
  USING mergetree WITH (order_by = 'a', settings = 'index_granularity = 1024');
-- five parts of 20,000 rows, a's ranges apart
INSERT INTO sel SELECT i, i % 97, 'v' || (i % 13) FROM generate_series(1, 100000) i;
RESET ch.insert_buffer_rows;
SELECT count(*) AS parts FROM ch.parts('sel');
-- the answers, the selections off, to compare with
SET ch.enable_part_selections = off;
CREATE TABLE sel_want AS
  SELECT 1 AS q, count(*) AS n, sum(b) AS s FROM sel WHERE a BETWEEN 30000 AND 30500
  UNION ALL SELECT 2, count(*), sum(b) FROM sel WHERE a BETWEEN 70000 AND 70200
  UNION ALL SELECT 3, count(*), sum(b) FROM sel WHERE a BETWEEN 30000 AND 30500 AND t = 'v3';
SELECT * FROM ch.part_selections() WHERE found > 0 OR stored > 0;
RESET ch.enable_part_selections;
CREATE FUNCTION sel_got(which int) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  got record;
  want record;
BEGIN
  SELECT * INTO want FROM sel_want WHERE q = which;
  IF which = 1 THEN
    SELECT count(*) AS n, sum(b) AS s INTO got FROM sel WHERE a BETWEEN 30000 AND 30500;
  ELSIF which = 2 THEN
    SELECT count(*) AS n, sum(b) AS s INTO got FROM sel WHERE a BETWEEN 70000 AND 70200;
  ELSE
    SELECT count(*) AS n, sum(b) AS s INTO got FROM sel WHERE a BETWEEN 30000 AND 30500 AND t = 'v3';
  END IF;
  RETURN CASE WHEN got.n = want.n AND got.s = want.s THEN format('same: %s rows', got.n)
              ELSE format('DIFFER: %s, %s against %s, %s', got.n, got.s, want.n, want.s) END;
END $$;

-- a fresh backend: the library asked as it plans, its answer kept, and found
-- again as the scan claims the part
\c
SET max_parallel_workers_per_gather = 0;
SELECT sel_got(1);
SELECT stored > 0 AS kept, checked IN (0, found) AS checked FROM ch.part_selections();
-- another: the same filter's answer found, planned without loading the part
\c
EXPLAIN (COSTS OFF) SELECT count(*) FROM sel WHERE a BETWEEN 30000 AND 30500;
SELECT s.found > 0 AS found, s.stored = 0 AS none_kept, s.checked > 0 OR d.parts_loaded = 0 AS no_part_loaded
  FROM ch.part_selections() s, ch.device_streams() d;
SET max_parallel_workers_per_gather = 0;
SELECT sel_got(1);
-- a filter of other values or other quals: a key of its own, asked and kept
\c
SET max_parallel_workers_per_gather = 0;
SELECT sel_got(2);
SELECT sel_got(3);
SELECT stored >= 2 AS kept FROM ch.part_selections();
\c
SET max_parallel_workers_per_gather = 0;
SELECT sel_got(2);
SELECT sel_got(3);
SELECT found > 0 AS found, stored = 0 AS none_kept FROM ch.part_selections();
-- off: nothing found or kept, the same answers
SET ch.enable_part_selections = off;
SELECT sel_got(1);
SELECT sel_got(2);
SELECT found, stored FROM ch.part_selections() WHERE stored > 0;
RESET ch.enable_part_selections;
-- a parallel scan's units of a part: each its own marks, its own entry
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 2;
SELECT sel_got(1);
SELECT sel_got(3);
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET min_parallel_table_scan_size;
SET max_parallel_workers_per_gather = 0;
-- the parts merged into one, whose name the entries do not have: asked again
SELECT ch.optimize('sel', true);
SELECT count(*) AS parts FROM ch.parts('sel');
\c
SET max_parallel_workers_per_gather = 0;
SELECT sel_got(1);
SELECT stored > 0 AS kept FROM ch.part_selections();
-- rows deleted: the part's granules as they were, its deletions the scan's
DELETE FROM sel WHERE a BETWEEN 30100 AND 30199;
SELECT count(*), sum(b) FROM sel WHERE a BETWEEN 30000 AND 30500;
SET ch.enable_part_selections = off;
SELECT count(*), sum(b) FROM sel WHERE a BETWEEN 30000 AND 30500;
RESET ch.enable_part_selections;

DROP TABLE sel, sel_want;
DROP FUNCTION sel_got(int);
