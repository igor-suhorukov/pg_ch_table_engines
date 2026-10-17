-- Indexes on the tables of the engines (ch_table_engines.md §2.10, CH2):
-- btree, hash, GIN, GiST and SP-GiST, their entries keyed by the rows' TIDs;
-- a fetch finds a row through the parts' _pg_rowid projections.  Index,
-- bitmap and index-only scans answer as the plain scan does, the deletions
-- the snapshot sees left out.
SET client_min_messages = warning;
CREATE TABLE ix (a int, b text, c int[], p point, r int4range) USING mergetree WITH (order_by = 'b');
INSERT INTO ix SELECT i, 'k' || (i % 500), ARRAY[i % 13, i % 17], point(i % 100, i / 100),
    int4range(i, i + 10) FROM generate_series(1, 5000) i;
DELETE FROM ix WHERE a % 9 = 0;
UPDATE ix SET a = a + 100000 WHERE a % 11 = 0;
CREATE INDEX ix_a ON ix (a);
CREATE INDEX ix_b ON ix USING hash (b);
CREATE INDEX ix_c ON ix USING gin (c);
CREATE INDEX ix_p ON ix USING gist (p);
CREATE INDEX ix_r ON ix USING spgist (r);
CREATE INDEX ix_expr ON ix ((a % 100)) WHERE a > 1000;
ANALYZE ix;

-- every query through the index and through a plain scan: the same rows
CREATE FUNCTION via_index(q text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  with_index text;
  without text;
  plan text := '';
  first text;
  line text;
BEGIN
  PERFORM set_config('enable_seqscan', 'off', true), set_config('ch.enable_chscan', 'off', true);
  FOR line IN EXECUTE 'EXPLAIN (COSTS OFF) ' || q LOOP
    plan := plan || line || E'\n';
    IF first IS NULL AND line ~ 'Scan' THEN
      first := btrim(regexp_replace(line, '^.*->', ''));
    END IF;
  END LOOP;
  IF plan NOT LIKE '%Index%' THEN
    RETURN 'NO INDEX IN THE PLAN: ' || plan;
  END IF;
  EXECUTE format('SELECT md5(string_agg(x::text, %L ORDER BY x::text)) FROM (%s) x', '|', q) INTO with_index;
  PERFORM set_config('enable_seqscan', 'on', true), set_config('ch.enable_chscan', 'on', true),
    set_config('enable_indexscan', 'off', true), set_config('enable_bitmapscan', 'off', true),
    set_config('enable_indexonlyscan', 'off', true);
  EXECUTE format('SELECT md5(string_agg(x::text, %L ORDER BY x::text)) FROM (%s) x', '|', q) INTO without;
  PERFORM set_config('enable_indexscan', 'on', true), set_config('enable_bitmapscan', 'on', true),
    set_config('enable_indexonlyscan', 'on', true);
  RETURN CASE WHEN with_index IS NOT DISTINCT FROM without THEN first ELSE 'DIFFERS' END;
END
$$;

SELECT via_index('SELECT * FROM ix WHERE a = 42');
SELECT via_index('SELECT a, b FROM ix WHERE a BETWEEN 100 AND 300');
SELECT via_index('SELECT a FROM ix WHERE a > 100000');
SELECT via_index('SELECT a FROM ix WHERE a = 99');
SELECT via_index('SELECT a, b FROM ix WHERE b = ''k77''');
SELECT via_index('SELECT a, c FROM ix WHERE c @> ARRAY[5, 7]');
SELECT via_index('SELECT a, p FROM ix WHERE p <@ box(point(10, 10), point(20, 20))');
SELECT via_index('SELECT a, r FROM ix WHERE r && int4range(2000, 2005)');
SELECT via_index('SELECT a FROM ix WHERE a % 100 = 7 AND a > 1000');
SELECT via_index('SELECT count(*) FROM ix WHERE a < 2500');

-- a bitmap scan over many TID blocks, lossy where work_mem is small
SET enable_seqscan = off;
SET enable_indexscan = off;
SET ch.enable_chscan = off;
EXPLAIN (COSTS OFF) SELECT count(*), sum(a) FROM ix WHERE a < 4000;
SELECT count(*), sum(a) FROM ix WHERE a < 4000;
SET work_mem = '64kB';
SELECT count(*), sum(a) FROM ix WHERE a < 4000 OR c @> ARRAY[1];
RESET work_mem;
RESET enable_seqscan;
RESET enable_indexscan;
RESET ch.enable_chscan;
SELECT count(*), sum(a) FROM ix WHERE a < 4000;
SELECT count(*), sum(a) FROM ix WHERE a < 4000 OR c @> ARRAY[1];

-- the index follows the rows: deleted ones found no more, updated ones under their new values
DELETE FROM ix WHERE a = 42;
UPDATE ix SET a = -1 WHERE a = 43;
SELECT via_index('SELECT a FROM ix WHERE a IN (42, 43, -1)');
-- in a transaction that rolls back, the old versions again
BEGIN;
DELETE FROM ix WHERE a BETWEEN 500 AND 600;
SELECT via_index('SELECT count(*) FROM ix WHERE a BETWEEN 500 AND 600');
ROLLBACK;
SELECT via_index('SELECT count(*) FROM ix WHERE a BETWEEN 500 AND 600');

-- REINDEX, and VACUUM over the dead rows' entries
REINDEX TABLE ix;
VACUUM ix;
SELECT via_index('SELECT a, b FROM ix WHERE a BETWEEN 100 AND 300');
-- the deletions folded into the parts: the rows found through the new versions' projections
SELECT ch.fold('ix') > 0 AS folded;
SELECT via_index('SELECT a, b FROM ix WHERE a BETWEEN 100 AND 300');
SELECT via_index('SELECT a, c FROM ix WHERE c @> ARRAY[5, 7]');

-- a build reads the columns its keys, INCLUDE columns, expressions and
-- predicate take, or every column for an expression of the whole row
CREATE INDEX ix_inc ON ix (a) INCLUDE (b);
CREATE FUNCTION ix_key(ix) RETURNS int IMMUTABLE LANGUAGE sql AS 'SELECT $1.a * 2 + length($1.b)';
CREATE INDEX ix_row ON ix (ix_key(ix));
-- an index-only scan: b from the index's entries
SET enable_bitmapscan = off;
SELECT via_index('SELECT a, b FROM ix WHERE a BETWEEN 10 AND 20');
RESET enable_bitmapscan;
SELECT via_index('SELECT a FROM ix WHERE ix_key(ix) = 2002');
SELECT a, b FROM ix WHERE ix_key(ix) = 2002;
DROP INDEX ix_inc, ix_row;
DROP FUNCTION ix_key(ix);

-- a parallel build: its participants' scans, which the build hands in, ended by the build
SET max_parallel_maintenance_workers = 2;
SET min_parallel_table_scan_size = 0;
SET client_min_messages = debug1;
CREATE INDEX ix_par ON ix (a);
CREATE INDEX ix_par_c ON ix USING gin (c);
SET client_min_messages = warning;
RESET max_parallel_maintenance_workers;
RESET min_parallel_table_scan_size;
SELECT via_index('SELECT a, b FROM ix WHERE a BETWEEN 100 AND 300');
DROP INDEX ix_par, ix_par_c;

-- VACUUM cleans every index up, as heap's does, where no row is dead too: GIN's pending list is flushed
CREATE TABLE ixg (c int[]) USING mergetree;
CREATE INDEX ixg_c ON ixg USING gin (c) WITH (fastupdate = on);
INSERT INTO ixg SELECT ARRAY[i % 7, i] FROM generate_series(1, 2000) i;
VACUUM ixg;
SELECT gin_clean_pending_list('ixg_c') AS left_to_flush;
DROP TABLE ixg;

-- what is refused: BRIN, unique indexes and constraints, CREATE INDEX CONCURRENTLY
CREATE INDEX ix_brin ON ix USING brin (a);
CREATE UNIQUE INDEX ix_u ON ix (a);
ALTER TABLE ix ADD PRIMARY KEY (a);
CREATE INDEX CONCURRENTLY ix_cc ON ix (a);

-- the files store alike
CREATE TABLE ixf (a int, b text) USING mergetree WITH (storage = 'files');
INSERT INTO ixf SELECT i, 'f' || i FROM generate_series(1, 3000) i;
DELETE FROM ixf WHERE a % 4 = 0;
CREATE INDEX ixf_a ON ixf (a);
UPDATE ixf SET b = 'updated' WHERE a BETWEEN 10 AND 20;
SELECT via_index('SELECT a, b FROM ixf WHERE a BETWEEN 5 AND 25');

DROP TABLE ix, ixf;
DROP FUNCTION via_index(text);
