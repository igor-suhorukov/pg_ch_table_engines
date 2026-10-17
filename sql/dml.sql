-- DELETE, UPDATE and row locks on the tables of the engines
-- (ch_table_engines.md §2.10, CH2): a deletion is a row of ch.tombstones the
-- snapshot sees, an update a deletion naming the row id of the new version,
-- which is written as an inserted row is.  Every statement runs on a heap
-- table and on a mergetree table alike, and both its RETURNING rows and the
-- two tables' rows are compared.
SET client_min_messages = warning;
CREATE TABLE d_heap (a int, b text, c numeric(10, 2), d int[]) USING heap;
CREATE TABLE d_mt (a int, b text, c numeric(10, 2), d int[]) USING mergetree WITH (order_by = 'a');
INSERT INTO d_heap SELECT i, 'r' || i, i / 4.0, ARRAY[i % 7, i % 11] FROM generate_series(1, 2000) i;
INSERT INTO d_mt SELECT * FROM d_heap;

-- a statement over %t on both tables: its RETURNING rows, then the tables' rows, the same
CREATE FUNCTION apply(stmt text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  r1 text;
  r2 text;
  t1 text;
  t2 text;
BEGIN
  EXECUTE format('WITH x AS (%s) SELECT coalesce(md5(string_agg(x::text, %L ORDER BY x::text)), %L) FROM x',
                 replace(stmt, '%t', 'd_heap'), '|', 'none') INTO r1;
  EXECUTE format('WITH x AS (%s) SELECT coalesce(md5(string_agg(x::text, %L ORDER BY x::text)), %L) FROM x',
                 replace(stmt, '%t', 'd_mt'), '|', 'none') INTO r2;
  SELECT md5(string_agg(x::text, '|' ORDER BY x::text)) INTO t1 FROM d_heap x;
  SELECT md5(string_agg(x::text, '|' ORDER BY x::text)) INTO t2 FROM d_mt x;
  IF r1 IS DISTINCT FROM r2 THEN
    RETURN 'RETURNING DIFFERS';
  END IF;
  IF t1 IS DISTINCT FROM t2 THEN
    RETURN 'ROWS DIFFER';
  END IF;
  RETURN 'same';
END
$$;
-- the two tables' rows, compared
CREATE FUNCTION rows_same() RETURNS text LANGUAGE sql AS $$
  SELECT CASE WHEN (SELECT count(*) FROM (SELECT * FROM d_heap EXCEPT ALL SELECT * FROM d_mt) x) = 0 AND
                   (SELECT count(*) FROM (SELECT * FROM d_mt EXCEPT ALL SELECT * FROM d_heap) x) = 0
              THEN 'same' ELSE 'ROWS DIFFER' END
$$;

-- deletions
SELECT apply('DELETE FROM %t WHERE a % 10 = 3 RETURNING a, b');
SELECT apply('DELETE FROM %t WHERE a BETWEEN 100 AND 150 RETURNING *');
SELECT apply('DELETE FROM %t WHERE b IN (''r7'', ''r8'', ''r9999'') RETURNING a');
SELECT apply('DELETE FROM %t t USING generate_series(400, 410) g WHERE t.a = g RETURNING t.a, g');
SELECT apply('DELETE FROM %t WHERE a IN (SELECT a FROM %t WHERE c > 450) RETURNING a');
SELECT apply('DELETE FROM %t WHERE a = -1 RETURNING a');
-- updates: a column, the key (the row moves among the parts), NULLs, an array
SELECT apply('UPDATE %t SET b = b || ''!'' WHERE a % 10 = 4 RETURNING a, b');
SELECT apply('UPDATE %t SET a = a + 10000 WHERE a BETWEEN 200 AND 220 RETURNING a, b, c');
SELECT apply('UPDATE %t SET c = NULL, d = NULL WHERE a % 17 = 0 RETURNING *');
SELECT apply('UPDATE %t SET d = d || a WHERE a < 50 RETURNING a, d');
SELECT apply('UPDATE %t t SET c = t.c + v.x FROM (VALUES (30, 1.5), (31, 2.5), (31, 9.9)) v(k, x) WHERE t.a = v.k RETURNING t.a');
-- a row a statement updates twice (the join's duplicate): updated once
SELECT a, c FROM d_mt WHERE a = 31;
-- every row: the new versions are not updated again (Halloween)
SELECT apply('UPDATE %t SET c = coalesce(c, 0) + 1 RETURNING a');
SELECT count(*) FROM d_mt;
-- deleting and updating the new versions
SELECT apply('DELETE FROM %t WHERE a > 10000 RETURNING a');
SELECT apply('UPDATE %t SET b = upper(b) WHERE a % 10 = 4 RETURNING b');

-- the new version's TID is the one RETURNING and a later scan give
CREATE TABLE tid_mt (a int, b text) USING mergetree;
INSERT INTO tid_mt VALUES (1, 'one'), (2, 'two');
CREATE TEMP TABLE tids AS WITH u AS (UPDATE tid_mt SET b = 'uno' WHERE a = 1 RETURNING ctid) SELECT ctid AS t FROM u;
SELECT ctid = (SELECT t FROM tids) AS same_tid, a, b FROM tid_mt WHERE a = 1;
SELECT a, b FROM tid_mt WHERE ctid = (SELECT t FROM tids);
-- the old version's tombstone names the new one
SELECT kind, successor IS NOT NULL AS names_successor FROM ch.tombstones
  WHERE relfilenumber = pg_relation_filenode('tid_mt') ORDER BY rowid;
DROP TABLE tid_mt, tids;

-- in a transaction: rolled back, and with savepoints
BEGIN;
SELECT apply('DELETE FROM %t WHERE a < 20 RETURNING a');
SELECT count(*) FROM d_mt WHERE a < 20;
SAVEPOINT s;
SELECT apply('UPDATE %t SET b = ''sp'' WHERE a BETWEEN 20 AND 25 RETURNING a');
ROLLBACK TO s;
SELECT a, b FROM d_mt WHERE a BETWEEN 20 AND 25 ORDER BY a;
SELECT apply('UPDATE %t SET b = ''kept'' WHERE a = 26 RETURNING a');
RELEASE s;
ROLLBACK;
SELECT count(*) FROM d_mt WHERE a < 20;
SELECT a, b FROM d_mt WHERE a = 26;
SELECT rows_same();

-- a row deleted twice in one transaction: the second finds it gone
BEGIN;
DELETE FROM d_mt WHERE a = 1;
DELETE FROM d_mt WHERE a = 1;
UPDATE d_mt SET b = 'x' WHERE a = 1;
ROLLBACK;

-- cursors: WHERE CURRENT OF deletes and updates the row the cursor is on
BEGIN;
DECLARE c CURSOR FOR SELECT a FROM d_mt WHERE a BETWEEN 40 AND 45;
FETCH 1 FROM c;
DELETE FROM d_mt WHERE CURRENT OF c;
FETCH 1 FROM c;
UPDATE d_mt SET b = 'cursor' WHERE CURRENT OF c;
CLOSE c;
SELECT a, b FROM d_mt WHERE a BETWEEN 40 AND 45 ORDER BY a;
ROLLBACK;

-- triggers see the old row and the new, fetched by TID; transition tables too
CREATE TABLE trig_log (op text, old_a int, old_b text, new_a int, new_b text);
CREATE FUNCTION log_row() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO trig_log VALUES (TG_OP, OLD.a, OLD.b, NEW.a, NEW.b);
  RETURN coalesce(NEW, OLD);
END
$$;
CREATE FUNCTION log_stmt() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO trig_log SELECT TG_OP || ' old table', count(*), NULL, NULL, NULL FROM old_rows;
  RETURN NULL;
END
$$;
CREATE TABLE tr_mt (a int, b text) USING mergetree;
INSERT INTO tr_mt SELECT i, 'v' || i FROM generate_series(1, 5) i;
CREATE TRIGGER tr_before BEFORE UPDATE OR DELETE ON tr_mt FOR EACH ROW EXECUTE FUNCTION log_row();
CREATE TRIGGER tr_after AFTER UPDATE ON tr_mt FOR EACH ROW EXECUTE FUNCTION log_row();
CREATE TRIGGER tr_stmt AFTER DELETE ON tr_mt REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT
  EXECUTE FUNCTION log_stmt();
UPDATE tr_mt SET b = b || '+' WHERE a IN (2, 3);
DELETE FROM tr_mt WHERE a > 3;
SELECT * FROM trig_log ORDER BY op, old_a;
SELECT * FROM tr_mt ORDER BY a;
DROP TABLE tr_mt, trig_log;
DROP FUNCTION log_row(), log_stmt();

-- a statement that fails in the middle of its scan: its executor's memory goes
-- before its resource owners are released (AtAbort_Portals()); the scan's
-- place among the running ones goes with the latter, and the statements
-- after it, which fetch rows from running scans, go on
SELECT a * 2000000000 FROM d_mt;
BEGIN;
SAVEPOINT s;
UPDATE d_mt SET c = c / 0 WHERE a = 2;
ROLLBACK TO s;
SELECT apply('UPDATE %t SET c = c + 1 WHERE a BETWEEN 2 AND 9 RETURNING a, c');
COMMIT;
SELECT apply('DELETE FROM %t WHERE a = 2 RETURNING *');

-- MERGE: its updates, deletions and inserts
SELECT apply('MERGE INTO %t t USING (SELECT g FROM generate_series(1990, 2010) g) s ON t.a = s.g
  WHEN MATCHED AND t.a % 2 = 0 THEN DELETE
  WHEN MATCHED THEN UPDATE SET b = ''merged''
  WHEN NOT MATCHED THEN INSERT VALUES (s.g, ''new'', 0, NULL) RETURNING t.a, t.b');

-- row locks: rows of ch.tombstones while their transaction runs, gone at commit
BEGIN;
SELECT a FROM d_mt WHERE a IN (60, 61) ORDER BY a FOR UPDATE;
SELECT a FROM d_mt WHERE a = 62 FOR SHARE;
SELECT kind, count(*) FROM ch.tombstones
  WHERE relfilenumber = pg_relation_filenode('d_mt') AND kind = 'l' GROUP BY kind;
-- a row locked, then deleted and updated by its locker
DELETE FROM d_mt WHERE a = 60;
UPDATE d_mt SET b = 'locked' WHERE a = 61;
COMMIT;
SELECT count(*) AS locks_left FROM ch.tombstones WHERE relfilenumber = pg_relation_filenode('d_mt') AND kind = 'l';
SELECT a, b FROM d_mt WHERE a BETWEEN 60 AND 62 ORDER BY a;
DELETE FROM d_heap WHERE a = 60;
UPDATE d_heap SET b = 'locked' WHERE a = 61;
SELECT rows_same();
-- a lock in a savepoint rolled back with it
BEGIN;
SAVEPOINT s;
SELECT a FROM d_mt WHERE a = 65 FOR UPDATE;
ROLLBACK TO s;
COMMIT;
SELECT count(*) AS locks_left FROM ch.tombstones WHERE relfilenumber = pg_relation_filenode('d_mt') AND kind = 'l';

-- folding: each part holding rows every snapshot sees deleted written again
-- without them, their tombstones gone; the rows as before, and deletions and
-- updates go on over the new parts
SELECT ch.fold('d_mt') > 0 AS folded;
SELECT count(*) AS tombstones FROM ch.tombstones
  WHERE relfilenumber = pg_relation_filenode('d_mt') AND kind <> 'l';
SELECT sum(rows) = (SELECT count(*) FROM d_mt) AS parts_hold_live_rows_only FROM ch.parts
  WHERE relfilenumber = pg_relation_filenode('d_mt');
SELECT rows_same();
SELECT apply('DELETE FROM %t WHERE a % 10 = 5 RETURNING a');
SELECT apply('UPDATE %t SET c = c * 2 WHERE a % 10 = 6 RETURNING a, c');
SELECT ch.fold('d_mt') > 0 AS folded_again;
SELECT ch.fold('d_mt') AS nothing_left;
SELECT rows_same();
-- the fold ran on the calling thread: the process's threads as after the library loaded
SELECT threads FROM ch.engine_info();

-- scans of no column, ANALYZE and the planner's counts see the deletions
SELECT count(*) = (SELECT count(*) FROM d_heap) AS same_count FROM d_mt;
SET ch.enable_chscan = off;
SELECT count(*) = (SELECT count(*) FROM d_heap) AS same_count FROM d_mt;
RESET ch.enable_chscan;
ANALYZE d_mt;
SELECT reltuples = (SELECT count(*) FROM d_heap) AS reltuples_live FROM pg_class WHERE relname = 'd_mt';

-- UNLOGGED and temporary tables: their deletions in ch.tombstones_unlogged
CREATE UNLOGGED TABLE u_mt (a int) USING mergetree;
CREATE TEMP TABLE t_mt (a int) USING mergetree;
INSERT INTO u_mt SELECT generate_series(1, 10);
INSERT INTO t_mt SELECT generate_series(1, 10);
DELETE FROM u_mt WHERE a > 5;
UPDATE t_mt SET a = a * 10 WHERE a > 8;
SELECT array_agg(a ORDER BY a) FROM u_mt;
SELECT array_agg(a ORDER BY a) FROM t_mt;
SELECT count(*) AS logged_rows FROM ch.tombstones WHERE relfilenumber IN (pg_relation_filenode('u_mt'), pg_relation_filenode('t_mt'));
SELECT count(*) > 0 AS unlogged_rows FROM ch.tombstones_unlogged;
DROP TABLE u_mt, t_mt;

-- the deletions go with the storage: TRUNCATE, a rewrite, DROP; SET TABLESPACE keeps them
CREATE TABLE s_mt (a int, b text) USING mergetree;
INSERT INTO s_mt SELECT i, 'x' || i FROM generate_series(1, 100) i;
DELETE FROM s_mt WHERE a % 2 = 0;
CREATE TABLESPACE chte_dml_ts LOCATION '';
ALTER TABLE s_mt SET TABLESPACE chte_dml_ts;
SELECT count(*), sum(a) FROM s_mt;
SELECT count(*) AS tombstones FROM ch.tombstones WHERE relfilenumber = pg_relation_filenode('s_mt');
ALTER TABLE s_mt ALTER COLUMN b TYPE varchar(10);
SELECT count(*), sum(a) FROM s_mt;
SELECT count(*) AS tombstones FROM ch.tombstones WHERE relfilenumber = pg_relation_filenode('s_mt');
DELETE FROM s_mt WHERE a < 10;
TRUNCATE s_mt;
SELECT count(*) AS tombstones FROM ch.tombstones WHERE relfilenumber = pg_relation_filenode('s_mt');
INSERT INTO s_mt VALUES (1, 'a');
DELETE FROM s_mt;
VACUUM FULL s_mt;
SELECT count(*) FROM s_mt;
DROP TABLE s_mt;
DROP TABLESPACE chte_dml_ts;

-- a compact part whose last granule is the longer one a compact part may end
-- with: 26,576 rows in granules of 8,192, 8,192 and 10,192.  Reads that pass a
-- granule over -- ChScan's pruning, the projection's lookup of a fold, a fetch
-- by TID -- read granule by granule all the same
CREATE TABLE g_mt (a int, b text) USING mergetree WITH (order_by = 'a');
INSERT INTO g_mt SELECT g, 'r' || g FROM generate_series(1, 26576) g;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
  SELECT count(*) FROM g_mt WHERE a < 100 OR a > 26000;
SELECT count(*), sum(a) FROM g_mt WHERE a < 100 OR a > 26000;
DELETE FROM g_mt WHERE a IN (50, 26500) RETURNING a, b;
SELECT ch.fold('g_mt') AS folded;
SELECT count(*), sum(a) FROM g_mt WHERE a < 100 OR a > 26000;
SELECT a, b FROM g_mt WHERE ctid = ANY (ARRAY(SELECT ctid FROM g_mt WHERE a IN (49, 26499))) ORDER BY a;
DROP TABLE g_mt;

SELECT rows_same();
DROP TABLE d_heap, d_mt;
DROP FUNCTION apply(text), rows_same();
