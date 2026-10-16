-- DDL over a table of an engine (ch_table_engines.md §2.13).
SET client_min_messages = warning;
CREATE TABLE dd (a int, b text) USING mergetree WITH (order_by = 'a');
INSERT INTO dd SELECT g, 'v' || g FROM generate_series(1, 1000) g;

-- TRUNCATE, rolled back and committed
BEGIN;
TRUNCATE dd;
SELECT count(*) FROM dd;
ROLLBACK;
SELECT count(*), sum(a) FROM dd;
TRUNCATE dd;
SELECT count(*) FROM dd;
SELECT count(*) FROM ch.parts('dd');
INSERT INTO dd SELECT g, 'v' || g FROM generate_series(1, 1000) g;

-- ADD COLUMN: older parts read the column's missing value
ALTER TABLE dd ADD COLUMN c int DEFAULT 7;
ALTER TABLE dd ADD COLUMN d text;
INSERT INTO dd VALUES (2000, 'new', 8, 'dee');
SELECT c, d, count(*) FROM dd GROUP BY c, d ORDER BY c;
-- a volatile default rewrites the table
ALTER TABLE dd ADD COLUMN e float8 DEFAULT random();
SELECT count(*), count(e), bool_and(e >= 0 AND e < 1) FROM dd;

-- DROP COLUMN
ALTER TABLE dd DROP COLUMN b;
SELECT count(*), sum(a), sum(c) FROM dd;
INSERT INTO dd (a, c, d) VALUES (3000, 9, 'after drop');
SELECT a, c, d FROM dd WHERE a >= 2000 ORDER BY a;
SELECT ch.definition('dd');

-- a type change rewrites
ALTER TABLE dd ALTER COLUMN a TYPE int8;
SELECT pg_typeof(a), count(*), sum(a) FROM dd GROUP BY 1;

-- VACUUM FULL rewrites into new storage
SELECT relfilenode AS before FROM pg_class WHERE relname = 'dd' \gset
VACUUM FULL dd;
SELECT relfilenode <> :before AS new_storage FROM pg_class WHERE relname = 'dd';
SELECT count(*), sum(a), sum(c) FROM dd;
VACUUM dd;
-- SET NOT NULL scans; the parts keep their columns as they are
ALTER TABLE dd ALTER COLUMN a SET NOT NULL;
INSERT INTO dd (a, e) VALUES (NULL, 0.5);
ALTER TABLE dd ALTER COLUMN a DROP NOT NULL;

-- SET TABLESPACE copies the parts
CREATE TABLESPACE chts LOCATION '';
ALTER TABLE dd SET TABLESPACE chts;
SELECT count(*), sum(a), sum(c) FROM dd;
INSERT INTO dd (a, c) VALUES (4000, 1);
SELECT count(*) FROM dd;
ALTER TABLE dd SET TABLESPACE pg_default;
SELECT count(*), sum(a) FROM dd;

-- UNLOGGED and TEMPORARY tables, and SET LOGGED
CREATE UNLOGGED TABLE du (a int) USING mergetree;
INSERT INTO du SELECT generate_series(1, 100);
SELECT count(*) FROM du;
SELECT count(*) FROM ch.tables_unlogged t JOIN pg_class c ON c.relfilenode = t.relfilenumber WHERE c.relname = 'du';
ALTER TABLE du SET LOGGED;
SELECT relpersistence, (SELECT count(*) FROM du) FROM pg_class WHERE relname = 'du';
CREATE TEMP TABLE dt (a int) USING mergetree;
INSERT INTO dt SELECT generate_series(1, 50);
SELECT count(*) FROM dt;
SELECT backend >= 0 AS has_backend FROM ch.tables_unlogged t JOIN pg_class c ON c.relfilenode = t.relfilenumber WHERE c.relname = 'dt';

-- DROP, rolled back and committed
BEGIN;
DROP TABLE du;
ROLLBACK;
SELECT count(*) FROM du;
DROP TABLE du, dt;

-- the catalogs keep nothing of dropped tables
DROP TABLE dd;
DROP TABLESPACE chts;
SELECT count(*) FROM ch.tables t WHERE NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relfilenode = t.relfilenumber);
SELECT count(*) FROM ch.parts p WHERE NOT EXISTS (SELECT 1 FROM ch.tables t WHERE t.relfilenumber = p.relfilenumber);
