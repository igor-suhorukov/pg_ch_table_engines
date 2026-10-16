-- The files store's sweep and its limits (ch_table_engines.md §2.9).
SET client_min_messages = warning;
SET ch.default_storage = files;
SELECT current_database() AS regress_db \gset
CREATE TABLE so (a int) USING mergetree;
INSERT INTO so VALUES (1);

-- a prepared transaction ended elsewhere leaves its part to the sweep
BEGIN;
INSERT INTO so VALUES (2);
PREPARE TRANSACTION 'po';
ROLLBACK PREPARED 'po';
SELECT count(*) FROM so;
SELECT ch.remove_orphans('so');
SELECT ch.remove_orphans('so');
SELECT count(*) FROM so;
SELECT count(*) FROM ch.parts('so');

-- VACUUM sweeps too
BEGIN;
INSERT INTO so VALUES (3);
PREPARE TRANSACTION 'po2';
ROLLBACK PREPARED 'po2';
VACUUM so;
SELECT ch.remove_orphans('so');
SELECT array_agg(a) FROM so;

-- while a prepared transaction holds the table, its parts stay
BEGIN;
INSERT INTO so VALUES (4);
PREPARE TRANSACTION 'po3';
VACUUM so;
COMMIT PREPARED 'po3';
SELECT array_agg(a ORDER BY a) FROM so;

-- CREATE DATABASE does not copy a template's tables of the engines
CREATE DATABASE chte_tmpl;
\c chte_tmpl
SET client_min_messages = warning;
CREATE EXTENSION ch_table_engines;
CREATE TABLE tt (a int) USING mergetree WITH (storage = 'files');
INSERT INTO tt VALUES (1);
\c :regress_db
SET client_min_messages = warning;
CREATE DATABASE chte_copy TEMPLATE chte_tmpl;
DROP DATABASE chte_tmpl;
CREATE DATABASE chte_copy TEMPLATE template0;
DROP DATABASE chte_copy;
SELECT ch.remove_orphans('pg_class');
DROP TABLE so;
