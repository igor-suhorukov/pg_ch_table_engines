-- NOT NULL columns as ClickHouse's plain types (ch_table_engines.md §2.5,
-- VC11): plain where the storage is made with them NOT NULL, unless a key
-- or a low_cardinality column; Nullable once a column loses NOT NULL, and
-- not plain again in that storage, which loses the column from its set
-- before the next ALTER TABLE; plain again in new storage (TRUNCATE, a
-- rewrite); a column an ALTER TABLE adds NOT NULL plain.
SET client_min_messages = warning;
CREATE FUNCTION plain(r regclass) RETURNS int2[] LANGUAGE sql AS $$
	SELECT t.plain FROM ch.tables t JOIN pg_class c ON c.relfilenode = t.relfilenumber WHERE c.oid = r
$$;

CREATE TABLE nn (k int NOT NULL, a int NOT NULL, b text NOT NULL, c int, l text NOT NULL)
  USING mergetree WITH (order_by = 'k', low_cardinality = 'l');
SELECT plain('nn');
-- k, the key, and l, the low_cardinality column, stay Nullable
SELECT ch.definition('nn');
INSERT INTO nn SELECT i, i, 'b' || i, nullif(i % 3, 0), 'l' || (i % 5) FROM generate_series(1, 20000) i;
SELECT count(*), sum(a), count(b), count(c), count(DISTINCT l) FROM nn;

-- DROP NOT NULL: Nullable, the plain parts read through ClickHouse's conversion
ALTER TABLE nn ALTER COLUMN a DROP NOT NULL;
SELECT ch.definition('nn') LIKE '%c2 Nullable(Int32)%' AS a_nullable;
INSERT INTO nn VALUES (30000, NULL, 'x', 1, 'l1');
SELECT count(*), count(a), sum(a) FROM nn;
-- and NOT NULL again: the set loses a first, so a stays Nullable
DELETE FROM nn WHERE a IS NULL;
ALTER TABLE nn ALTER COLUMN a SET NOT NULL;
SELECT plain('nn');
SELECT ch.definition('nn') LIKE '%c2 Nullable(Int32)%' AS a_nullable;
SELECT count(*), count(a), sum(a) FROM nn;
-- a merge of the plain parts and the Nullable one
SELECT ch.optimize('nn', true) > 0 AS merged;
SELECT count(*), count(a), sum(a), count(b) FROM nn;

-- ADD COLUMN NOT NULL: plain, the older parts its missing value
ALTER TABLE nn ADD COLUMN d int NOT NULL DEFAULT 7;
SELECT plain('nn');
INSERT INTO nn VALUES (40000, 1, 'y', 2, 'l2', 9);
SELECT d, count(*) FROM nn GROUP BY d ORDER BY d;

-- a rewrite: new storage, every NOT NULL column plain again
VACUUM FULL nn;
SELECT plain('nn');
SELECT ch.definition('nn') LIKE '%c2 Int32%' AS a_plain;
SELECT count(*), count(a), sum(a), sum(d) FROM nn;
-- TRUNCATE: new storage
ALTER TABLE nn ALTER COLUMN b DROP NOT NULL;
TRUNCATE nn;
SELECT plain('nn');
SELECT ch.definition('nn') LIKE '%c3 Nullable(String)%' AS b_nullable;

-- a NOT NULL that is not valid: Nullable in new storage too
CREATE TABLE nv (k int NOT NULL, a int) USING mergetree WITH (order_by = 'k');
INSERT INTO nv VALUES (1, NULL), (2, 2);
ALTER TABLE nv ADD CONSTRAINT nv_a NOT NULL a NOT VALID;
TRUNCATE nv;
SELECT plain('nv');

-- partitions: an ALTER TABLE of the parent revokes in each
CREATE TABLE np (k int NOT NULL, a int NOT NULL) PARTITION BY RANGE (k);
CREATE TABLE np1 PARTITION OF np FOR VALUES FROM (0) TO (1000) USING mergetree;
CREATE TABLE np2 PARTITION OF np FOR VALUES FROM (1000) TO (2000) USING mergetree;
SELECT plain('np1'), plain('np2');
INSERT INTO np SELECT i, i FROM generate_series(0, 1999) i;
ALTER TABLE np ALTER COLUMN a DROP NOT NULL;
INSERT INTO np VALUES (5, NULL), (1005, NULL);
DELETE FROM np WHERE a IS NULL;
ALTER TABLE np ALTER COLUMN a SET NOT NULL;
SELECT plain('np1'), plain('np2');
SELECT count(*), count(a), sum(a) FROM np;
-- ATTACH PARTITION: the parent's other partitions untouched, the new one's set kept
CREATE TABLE np3 (k int NOT NULL, a int NOT NULL) USING mergetree;
INSERT INTO np3 VALUES (2000, 1), (2500, 2);
ALTER TABLE np ATTACH PARTITION np3 FOR VALUES FROM (2000) TO (3000);
SELECT plain('np1'), plain('np3');
-- ADD COLUMN NOT NULL through the parent: plain in every partition
ALTER TABLE np ADD COLUMN e int NOT NULL DEFAULT 0;
SELECT plain('np1'), plain('np2'), plain('np3');
SELECT count(*), sum(a), sum(e) FROM np;

DROP TABLE nn, nv, np;
DROP FUNCTION plain(regclass);
