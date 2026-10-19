-- The engine's start in each process (ch_table_engines.md §3, VC12).
SET client_min_messages = warning;
-- the postmaster mapped the engine library as it preloaded the extension: a
-- new backend has it before its first call into it
SELECT current_setting('ch.engine_preload') AS preload,
	   count(*) > 0 AS mapped
  FROM regexp_split_to_table(pg_read_file('/proc/self/maps'), E'\n') AS line
 WHERE line LIKE '%/libchte_engine.so%';
-- and called nothing of it: the postmaster has its one thread
SELECT count(*) AS postmaster_threads
  FROM pg_ls_dir('/proc/' || split_part(pg_read_file('postmaster.pid'), E'\n', 1) || '/task');

CREATE TABLE ef (a int, b text) USING mergetree WITH (order_by = 'a');
INSERT INTO ef SELECT g, 'v' || g FROM generate_series(1, 100) g;
-- a write registers every function the library links, and its aggregate
-- functions: those the engine and ClickHouse's own code name, where ClickHouse
-- has 1,770 (engine.cmake)
SELECT functions AS all_functions FROM ch.engine_info() \gset
SELECT threads, :all_functions < 100 AS linked_functions FROM ch.engine_info();

\c
SET client_min_messages = warning;
-- a filtered scan registers only the functions its filter uses
SELECT count(*), sum(a) FROM ef WHERE a > 10 AND b IS NOT NULL AND a <> 50;
SELECT threads, functions BETWEEN 1 AND :all_functions - 1 AS filters_functions FROM ch.engine_info();
-- an index-like fetch, statistics, a sort by the key: no others
SELECT min(a), max(a) FROM ef WHERE a IN (3, 30, 300);
SELECT a FROM ef WHERE a < 4 ORDER BY a;
SELECT functions BETWEEN 1 AND :all_functions - 1 AS filters_functions FROM ch.engine_info();
-- the first write registers the rest
INSERT INTO ef VALUES (101, 'v101');
SELECT functions = :all_functions AS all_functions FROM ch.engine_info();
SELECT count(*), sum(a) FROM ef WHERE a > 10;

-- a definition that names more: statistics that aggregate
CREATE TABLE eu (a int) USING mergetree
	WITH (order_by = 'a', settings = 'auto_statistics_types = ''basic, uniq''');
INSERT INTO eu SELECT generate_series(1, 50);
\c
SET client_min_messages = warning;
SELECT count(*) FROM eu WHERE a > 25;
SELECT functions = :all_functions AS all_functions FROM ch.engine_info();

-- every function at once
\c
SET ch.engine_functions = 'all';
SELECT count(*) FROM ef WHERE a > 90;
SELECT functions = :all_functions AS all_functions FROM ch.engine_info();
RESET ch.engine_functions;

-- a scan reads the manifests of the parts it attaches alone, from TOAST: a
-- table of the pages store whose parts, wide -- a file or two a column --
-- have manifests stored out of line
SELECT format('CREATE TABLE ew (a int, %s) USING mergetree WITH (storage = ''pages'', order_by = ''a'', '
			  'settings = ''min_bytes_for_wide_part = 0'')', string_agg(format('c%s int', i), ', ')) AS ddl
  FROM generate_series(1, 60) i \gset
:ddl;
INSERT INTO ew (a, c1, c40, c60) SELECT g, g, g * 2, g * 3 FROM generate_series(1, 3000) g;
INSERT INTO ew (a, c1, c40, c60) SELECT g, g, g * 2, g * 3 FROM generate_series(3001, 6000) g;
CREATE INDEX ew_a ON ew (a);
SELECT count(*) AS parts, bool_and(pg_column_toast_chunk_id(p.manifest) IS NOT NULL) AS out_of_line
  FROM ch.parts p JOIN pg_class c ON c.relfilenode = p.relfilenumber
 WHERE c.oid = 'ew'::regclass;
\c
SET client_min_messages = warning;
SELECT count(*), sum(a), sum(c40) FROM ew WHERE a > 100;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT a, c1, c40, c60 FROM ew WHERE a IN (7, 3007);
RESET enable_seqscan;
RESET enable_bitmapscan;
\c
SET client_min_messages = warning;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 2;
EXPLAIN (COSTS OFF) SELECT count(*), sum(c60) FROM ew WHERE c1 % 2 = 0;
SELECT count(*), sum(c60) FROM ew WHERE c1 % 2 = 0;

DROP TABLE ef, eu, ew;
