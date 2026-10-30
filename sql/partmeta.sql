-- A part's loaded metadata kept in shared memory (ch_table_engines.md VC42,
-- pg_accel's §3.38): the first process to attach a part of the pages store
-- loads it and keeps its metadata -- columns, serialization, checksums, index
-- granularity, counts, default codec -- and the processes after it make
-- their part of that, reading and parsing none of its metadata files; every
-- answer the same with the setting off.  With CHTE_SPEC_CHECK, the suite's,
-- each part made so is compared with one loaded from its files.
SET client_min_messages = warning;
SET max_parallel_workers_per_gather = 0;
SET ch.insert_buffer_rows = 20000;
CREATE TABLE pmeta (a int NOT NULL, d date NOT NULL, n numeric(10,2) NOT NULL, s text NOT NULL, z int,
                    lc text NOT NULL, sp smallint NOT NULL)
  USING mergetree WITH (order_by = 'a', storage = 'pages', low_cardinality = 'lc',
                        settings = 'index_granularity = 1024');
-- four parts; sp mostly zeros, its parts' serialization sparse
INSERT INTO pmeta SELECT i, DATE '2020-01-01' + i % 300, i / 4.0, 'v' || (i % 97), CASE WHEN i % 7 = 0 THEN NULL ELSE i END,
  'c' || (i % 5), CASE WHEN i % 50 = 0 THEN i % 9 + 1 ELSE 0 END FROM generate_series(1, 80000) i;
RESET ch.insert_buffer_rows;
SELECT count(*) AS parts FROM ch.parts('pmeta');
CREATE TABLE pmeta_want AS
  SELECT count(*) AS n, sum(a) AS sa, max(d) AS md, sum(n) AS sn, max(s) AS ms, count(z) AS cz, max(lc) AS mlc,
         sum(sp) AS ssp, count(*) FILTER (WHERE sp <> 0) AS nsp
  FROM pmeta;
CREATE FUNCTION pmeta_same() RETURNS text LANGUAGE sql AS $$
  SELECT CASE WHEN w = g THEN 'same' ELSE format('DIFFER: %s against %s', g, w) END
  FROM (SELECT row(count(*), sum(a), max(d), sum(n), max(s), count(z), max(lc), sum(sp),
                   count(*) FILTER (WHERE sp <> 0))::text AS g FROM pmeta) x,
       (SELECT row(n, sa, md, sn, ms, cz, mlc, ssp, nsp)::text AS w FROM pmeta_want) y
$$;

-- a fresh backend: its parts loaded, their metadata kept or found
\c
SET max_parallel_workers_per_gather = 0;
SELECT pmeta_same();
SELECT kept + found >= 4 AS kept_or_found FROM ch.part_metadata();
-- the next: every part made of what was kept
\c
SET max_parallel_workers_per_gather = 0;
SELECT pmeta_same();
SELECT found >= 4 AS found, made >= 4 AS made FROM ch.part_metadata();
-- off: each part loaded from its files, none made
\c
SET ch.enable_shared_part_metadata = off;
SET max_parallel_workers_per_gather = 0;
SELECT pmeta_same();
SELECT found, made FROM ch.part_metadata();
RESET ch.enable_shared_part_metadata;
-- a part merged: its own entry, the merged-away parts' left to go with a full table
\c
SELECT ch.optimize('pmeta', true) > 0 AS merged;
SELECT count(*) AS parts FROM ch.parts('pmeta');
\c
SET max_parallel_workers_per_gather = 0;
SELECT pmeta_same();
\c
SET max_parallel_workers_per_gather = 0;
SELECT pmeta_same();
SELECT made >= 1 AS made FROM ch.part_metadata();
DROP FUNCTION pmeta_same();
DROP TABLE pmeta, pmeta_want;
