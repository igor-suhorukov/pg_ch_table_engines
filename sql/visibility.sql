-- PostgreSQL's MVCC over the parts' catalog rows (ch_table_engines.md §2.6).
SET client_min_messages = warning;
CREATE TABLE vis (a int) USING mergetree;

-- a rolled-back transaction leaves no rows and no part
BEGIN;
INSERT INTO vis SELECT generate_series(1, 10);
SELECT count(*) FROM vis;
ROLLBACK;
SELECT count(*) FROM vis;
SELECT count(*) FROM ch.parts('vis');

-- savepoints: a rolled-back subtransaction's rows go; a released one's stay
BEGIN;
INSERT INTO vis VALUES (1);
SAVEPOINT s1;
INSERT INTO vis VALUES (2);
SAVEPOINT s2;
INSERT INTO vis VALUES (3);
ROLLBACK TO s2;
INSERT INTO vis VALUES (4);
RELEASE s1;
SAVEPOINT s3;
INSERT INTO vis VALUES (5);
ROLLBACK TO s3;
SELECT array_agg(a ORDER BY a) FROM vis;
COMMIT;
SELECT array_agg(a ORDER BY a) FROM vis;

-- an exception block is a subtransaction
DO $$
BEGIN
	INSERT INTO vis VALUES (6);
	BEGIN
		INSERT INTO vis VALUES (7);
		RAISE EXCEPTION 'undo 7';
	EXCEPTION WHEN raise_exception THEN
		NULL;
	END;
	INSERT INTO vis VALUES (8);
END $$;
SELECT array_agg(a ORDER BY a) FROM vis;

-- a cursor's snapshot does not see later rows
BEGIN;
DECLARE c CURSOR FOR SELECT a FROM vis ORDER BY a;
INSERT INTO vis VALUES (100);
FETCH ALL FROM c;
CLOSE c;
SELECT count(*) FROM vis WHERE a = 100;
COMMIT;

-- REPEATABLE READ keeps its snapshot
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT count(*) FROM vis;
INSERT INTO vis VALUES (101);
SELECT count(*) FROM vis;
COMMIT;

-- two-phase commit
BEGIN;
INSERT INTO vis VALUES (200);
PREPARE TRANSACTION 'p1';
SELECT count(*) FROM vis WHERE a = 200;
COMMIT PREPARED 'p1';
SELECT count(*) FROM vis WHERE a = 200;
BEGIN;
INSERT INTO vis VALUES (201);
PREPARE TRANSACTION 'p2';
ROLLBACK PREPARED 'p2';
SELECT count(*) FROM vis WHERE a = 201;

-- a table made, filled and read in one transaction, then rolled back
BEGIN;
CREATE TABLE vis2 (a int) USING mergetree;
INSERT INTO vis2 SELECT generate_series(1, 5);
SELECT count(*) FROM vis2;
TRUNCATE vis2;
INSERT INTO vis2 VALUES (42);
SELECT * FROM vis2;
ROLLBACK;
SELECT count(*) FROM pg_class WHERE relname = 'vis2';

-- and committed: its row ids continue from what the transaction took
BEGIN;
CREATE TABLE vis3 (a int) USING mergetree;
INSERT INTO vis3 SELECT generate_series(1, 3);
COMMIT;
INSERT INTO vis3 VALUES (4);
SELECT a, ctid FROM vis3 ORDER BY a;

DROP TABLE vis, vis3;
