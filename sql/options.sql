-- Options of the engines' tables (ch_table_engines.md §2.4).
SET client_min_messages = warning;

-- defaults: storage pages (ch.default_storage), no key
CREATE TABLE o1 (a int, b text) USING mergetree;
SELECT reloptions FROM pg_class WHERE relname = 'o1';
SELECT ch.definition('o1');

-- the pages store
CREATE TABLE op (a int) USING mergetree WITH (storage = 'pages');
SELECT reloptions FROM pg_class WHERE relname = 'op';
DROP TABLE op;

-- the engines' options, and heap's beside them
CREATE TABLE o2 (a int, b text, c date) USING mergetree
	WITH (order_by = 'a, c', primary_key = 'a', index_granularity = 1024,
		  compression = 'zstd(3)', settings = 'min_bytes_for_wide_part = 0', parallel_workers = 2);
SELECT reloptions FROM pg_class WHERE relname = 'o2';
SELECT ch.definition('o2');
SELECT relname, (SELECT option_value FROM pg_options_to_table(reloptions) WHERE option_name = 'parallel_workers')
  FROM pg_class WHERE relname = 'o2';

-- refused
CREATE TABLE bad (a int) USING mergetree WITH (order_by = 'nosuch');
CREATE TABLE bad (a int) USING mergetree WITH (storage = 'tape');
CREATE TABLE bad (a int) USING mergetree WITH (storage = 'pages', order_by = 'nosuch');
CREATE TABLE bad (a int) USING mergetree WITH (index_granularity = 0);
CREATE TABLE bad (a int) USING mergetree WITH (compression = 'gzip');
CREATE TABLE bad (a int) USING mergetree WITH (ver = 'a');
CREATE TABLE bad (a int) USING mergetree WITH (settings = 'storage_policy = ''x''');
CREATE TABLE bad (a int) USING mergetree WITH (nosuch = 1);
CREATE TABLE bad (a int) USING heap WITH (order_by = 'a');
SELECT count(*) FROM pg_class WHERE relname = 'bad';

-- an engine's own options
CREATE TABLE r1 (k int, ver int, x text) USING replacingmergetree WITH (order_by = 'k', ver = 'ver');
CREATE TABLE r1 (k int, ver int NOT NULL, x text) USING replacingmergetree WITH (order_by = 'k', ver = 'ver');
SELECT ch.definition('r1');
CREATE TABLE c1 (k int, s int8) USING collapsingmergetree WITH (order_by = 'k');
CREATE TABLE c1 (k int, s int8 NOT NULL) USING collapsingmergetree WITH (order_by = 'k', sign = 's');
ALTER TABLE c1 ALTER COLUMN s DROP NOT NULL;
SELECT ch.definition('c1');

-- ALTER TABLE SET and RESET: heap's options and the engines' alike
ALTER TABLE o1 SET (parallel_workers = 3, compression = 'lz4hc');
SELECT reloptions FROM pg_class WHERE relname = 'o1';
ALTER TABLE o1 RESET (parallel_workers);
SELECT reloptions FROM pg_class WHERE relname = 'o1';
ALTER TABLE o1 SET (order_by = 'a');
SELECT reloptions FROM pg_class WHERE relname = 'o1';
SELECT ch.definition('o1');
ALTER TABLE o1 SET (nosuch = 1);
ALTER TABLE o1 SET (fillfactor = 5);
-- a table's keys stay while it has data
INSERT INTO o1 VALUES (1, 'x');
ALTER TABLE o1 SET (order_by = 'b');
ALTER TABLE o1 SET (compression = 'none');
SELECT reloptions FROM pg_class WHERE relname = 'o1';
TRUNCATE o1;
ALTER TABLE o1 SET (order_by = 'b');
SELECT reloptions FROM pg_class WHERE relname = 'o1';

-- CREATE TABLE AS and SELECT INTO
CREATE TABLE ctas USING mergetree WITH (order_by = 'g') AS SELECT g, g * 2 AS h FROM generate_series(1, 10) g;
SELECT reloptions FROM pg_class WHERE relname = 'ctas';
SELECT count(*), sum(h) FROM ctas;
SET default_table_access_method = mergetree;
SELECT g INTO si FROM generate_series(1, 5) g;
SELECT amname FROM pg_class c JOIN pg_am a ON a.oid = c.relam WHERE relname = 'si';
SELECT reloptions FROM pg_class WHERE relname = 'si';
CREATE TABLE dflt (a int) WITH (order_by = 'a');
SELECT reloptions FROM pg_class WHERE relname = 'dflt';
RESET default_table_access_method;

-- SET ACCESS METHOD, both ways
CREATE TABLE h1 (a int, b text) WITH (fillfactor = 70);
INSERT INTO h1 SELECT g, g::text FROM generate_series(1, 100) g;
ALTER TABLE h1 SET ACCESS METHOD mergetree;
SELECT amname, reloptions FROM pg_class c JOIN pg_am a ON a.oid = c.relam WHERE relname = 'h1';
SELECT count(*), sum(a) FROM h1;
ALTER TABLE h1 SET ACCESS METHOD heap;
SELECT amname, reloptions FROM pg_class c JOIN pg_am a ON a.oid = c.relam WHERE relname = 'h1';
SELECT count(*), sum(a) FROM h1;
ALTER TABLE h1 SET (fillfactor = 80);
SELECT reloptions FROM pg_class WHERE relname = 'h1';

DROP TABLE o1, o2, r1, c1, ctas, si, dflt, h1;
