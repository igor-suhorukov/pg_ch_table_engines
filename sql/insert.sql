-- The ways rows come in, and when they become parts (ch_table_engines.md §2.7).
SET client_min_messages = warning;
CREATE TABLE ins (a int, b text) USING mergetree WITH (order_by = 'a');

-- INSERT: one part per statement
INSERT INTO ins VALUES (3, 'c'), (1, 'a'), (2, 'b');
INSERT INTO ins VALUES (4, 'd');
SELECT name, rows, level FROM ch.parts('ins');
SELECT * FROM ins ORDER BY a;

-- RETURNING, with the row's TID: its row id
INSERT INTO ins VALUES (5, 'e') RETURNING a, b, ctid IS NOT NULL AS has_tid;

-- INSERT ... SELECT from itself sees the table as it was (Halloween)
INSERT INTO ins SELECT a + 10, b FROM ins;
SELECT count(*), sum(a) FROM ins;

-- COPY
COPY ins FROM stdin;
100	copy one
101	copy two
\.
SELECT count(*), sum(a) FROM ins WHERE a >= 100;

-- a statement's rows past ch.insert_buffer_rows become several parts
SET ch.insert_buffer_rows = 1000;
INSERT INTO ins SELECT g, g::text FROM generate_series(1000, 4499) g;
RESET ch.insert_buffer_rows;
SELECT count(*), sum(rows) FROM ch.parts('ins') WHERE rows >= 500;
SELECT count(*), sum(a) FROM ins WHERE a >= 1000;

-- row triggers: AFTER ROW fetches the new row by its TID, from the write buffer
CREATE TABLE ins_log (op text, a int, b text);
CREATE FUNCTION ins_trig() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	INSERT INTO ins_log VALUES (TG_WHEN || ' ' || TG_OP, NEW.a, NEW.b);
	RETURN NEW;
END $$;
CREATE TRIGGER ins_before BEFORE INSERT ON ins FOR EACH ROW EXECUTE FUNCTION ins_trig();
CREATE TRIGGER ins_after AFTER INSERT ON ins FOR EACH ROW EXECUTE FUNCTION ins_trig();
INSERT INTO ins VALUES (7, 'trig'), (8, 'trig2');
SELECT * FROM ins_log ORDER BY op, a;
DROP TRIGGER ins_before ON ins;
DROP TRIGGER ins_after ON ins;

-- statement triggers with transition tables
CREATE FUNCTION ins_stmt() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	INSERT INTO ins_log SELECT 'statement', count(*), string_agg(b, ',' ORDER BY a) FROM new_rows;
	RETURN NULL;
END $$;
CREATE TRIGGER ins_stmt AFTER INSERT ON ins REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION ins_stmt();
INSERT INTO ins VALUES (9, 'x'), (10, 'y');
SELECT * FROM ins_log WHERE op = 'statement';
DROP TRIGGER ins_stmt ON ins;

-- a writable CTE does not see its own rows; the next statement does
WITH w AS (INSERT INTO ins VALUES (20, 'cte') RETURNING a)
SELECT (SELECT count(*) FROM ins WHERE a = 20) AS seen, (SELECT count(*) FROM w) AS written;
SELECT count(*) FROM ins WHERE a = 20;

-- a function that inserts, then reads, in one statement
CREATE FUNCTION ins_and_count(v int) RETURNS bigint LANGUAGE plpgsql AS $$
BEGIN
	INSERT INTO ins VALUES (v, 'fn');
	RETURN (SELECT count(*) FROM ins WHERE b = 'fn');
END $$;
SELECT ins_and_count(30), ins_and_count(31);
SELECT count(*) FROM ins WHERE b = 'fn';

-- what is not yet supported
UPDATE ins SET b = 'u' WHERE a = 1;
DELETE FROM ins WHERE a = 1;
INSERT INTO ins VALUES (1, 'x') ON CONFLICT DO NOTHING;
CREATE INDEX ON ins (a);
SELECT * FROM ins TABLESAMPLE system (10);

DROP TABLE ins, ins_log;
DROP FUNCTION ins_trig(), ins_stmt(), ins_and_count(int);
