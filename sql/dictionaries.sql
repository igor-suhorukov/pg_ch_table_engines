-- LowCardinality columns (ch_table_engines.md §3, VC5): option
-- low_cardinality names the columns a table keeps as ClickHouse's
-- LowCardinality(Nullable(String)), a dictionary its parts' granules share.
-- Every read answers as a heap copy's: the plain scan, ChScan, a fetch by
-- TID, the rows of updates and deletions, after merges and a fold.
SET client_min_messages = warning;

-- columns kept as strings alone, and the relation's own
CREATE TABLE lc_bad (a int, b text) USING mergetree WITH (low_cardinality = 'a');
CREATE TABLE lc_bad (a int, b text) USING mergetree WITH (low_cardinality = 'b, nope');
CREATE TABLE lc_bad (a int, b text) USING mergetree WITH (low_cardinality = 'b,');
CREATE TABLE lc_bad (a int, b text, n numeric(10, 2)) USING mergetree WITH (low_cardinality = 'n');

CREATE TABLE lc (a int, b text, c varchar(10), d bytea, e jsonb, f int8, g numeric)
  USING mergetree WITH (order_by = 'a', low_cardinality = 'b, c, d, e, g',
                        settings = 'index_granularity = 1024');
SELECT ch.definition('lc');
SET ch.insert_buffer_rows = 10000;
INSERT INTO lc
SELECT i, CASE WHEN i % 11 = 0 THEN NULL ELSE 'b' || (i % 9) END, ('c' || (i % 3))::varchar(10),
       decode(lpad(to_hex(i % 5), 2, '0'), 'hex'), jsonb_build_object('k', i % 4), i * 10,
       CASE WHEN i % 7 = 0 THEN NULL ELSE (i % 6) / 4.0 END
FROM generate_series(1, 30000) i;
RESET ch.insert_buffer_rows;
SELECT count(*) AS parts, sum(rows) AS rows FROM ch.parts('lc');
CREATE TABLE lc_heap (LIKE lc) USING heap;
INSERT INTO lc_heap SELECT * FROM lc;

-- a query's rows through ChScan and through the plain scan, against heap's
CREATE FUNCTION as_heap(q text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  ref text;
  got text;
  s text;
BEGIN
  EXECUTE 'SELECT md5(coalesce(string_agg(r::text, '','' ORDER BY r::text), '''')) FROM ('
    || replace(q, 'lc', 'lc_heap') || ') r' INTO ref;
  FOREACH s IN ARRAY ARRAY['on', 'off'] LOOP
    PERFORM set_config('ch.enable_chscan', s, true);
    EXECUTE 'SELECT md5(coalesce(string_agg(r::text, '','' ORDER BY r::text), '''')) FROM ('
      || q || ') r' INTO got;
    IF got IS DISTINCT FROM ref THEN
      RETURN format('DIFFERENT with ch.enable_chscan = %s', s);
    END IF;
  END LOOP;
  RETURN 'as heap';
END $$;

SELECT as_heap('SELECT * FROM lc');
SELECT as_heap('SELECT a, b FROM lc WHERE b = ''b3''');
SELECT as_heap('SELECT count(*) FROM lc WHERE b IS NULL');
SELECT as_heap('SELECT a, c, d FROM lc WHERE b IN (''b1'', ''b8'') AND a < 5000');
SELECT as_heap('SELECT b, count(*), min(c), max(e::text), sum(g) FROM lc GROUP BY b');
SELECT as_heap('SELECT a, b FROM lc ORDER BY b NULLS FIRST, a LIMIT 20');
SELECT b, count(*) FROM lc GROUP BY b ORDER BY b;
-- a row by its TID, as a TID scan and an index scan fetch it
SELECT ctid AS tid FROM lc WHERE a = 777 \gset
SELECT a, b, c, d, e, g FROM lc WHERE ctid = :'tid';
CREATE INDEX lc_b ON lc (b);
SET enable_seqscan = off;
SET ch.enable_chscan = off;
EXPLAIN (COSTS OFF) SELECT a, b FROM lc WHERE b = 'b5' AND a < 100;
SELECT a, b, c FROM lc WHERE b = 'b5' AND a < 100 ORDER BY a;
RESET enable_seqscan;
RESET ch.enable_chscan;

-- updates and deletions, as on heap
UPDATE lc SET b = b || 'x', c = 'u' WHERE a % 7 = 0;
UPDATE lc_heap SET b = b || 'x', c = 'u' WHERE a % 7 = 0;
DELETE FROM lc WHERE b = 'b2' OR a % 13 = 0;
DELETE FROM lc_heap WHERE b = 'b2' OR a % 13 = 0;
SELECT as_heap('SELECT * FROM lc');
SELECT as_heap('SELECT b, count(*) FROM lc GROUP BY b');
-- merged into one part, and the deleted rows folded out
SELECT ch.fold('lc') > 0 AS folded;
SELECT ch.optimize('lc', true) > 0 AS merged;
SELECT count(*) AS parts FROM ch.parts('lc');
SELECT as_heap('SELECT * FROM lc');
SELECT as_heap('SELECT a, b, d FROM lc WHERE b LIKE ''b1%'' OR d = ''\x03''');

-- a LowCardinality key prunes granules as a String's does
CREATE TABLE lck (k text, v int) USING mergetree
  WITH (order_by = 'k', low_cardinality = 'k', settings = 'index_granularity = 512');
INSERT INTO lck SELECT 'k' || lpad((i % 50)::text, 2, '0'), i FROM generate_series(1, 50000) i;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
  SELECT count(*), sum(v) FROM lck WHERE k = 'k07';
SELECT count(*), sum(v) FROM lck WHERE k = 'k07';
SELECT count(*) FROM lck WHERE k BETWEEN 'k10' AND 'k12';

-- the option is fixed while the table has parts; an empty one may take it
ALTER TABLE lck SET (low_cardinality = '');
TRUNCATE lck;
ALTER TABLE lck RESET (low_cardinality);
SELECT ch.definition('lck');
ALTER TABLE lck SET (low_cardinality = 'k');
INSERT INTO lck VALUES ('a', 1), (NULL, 2), ('', 3);
SELECT k, v FROM lck ORDER BY v;
SELECT k IS NULL AS null_k, k = '' AS empty_k FROM lck ORDER BY v;

-- the names are checked as the option is given; a column dropped since is
-- passed over, and one of the option's may not become another type
CREATE TABLE lcd (a int, b text, c text) USING mergetree WITH (low_cardinality = 'b, c');
ALTER TABLE lcd SET (low_cardinality = 'b, nope');
ALTER TABLE lcd SET (low_cardinality = 'a');
INSERT INTO lcd VALUES (1, 'x', 'y'), (2, NULL, 'z');
ALTER TABLE lcd DROP COLUMN b;
INSERT INTO lcd VALUES (3, 'w');
SELECT * FROM lcd ORDER BY a;
ALTER TABLE lcd ALTER COLUMN c TYPE int USING length(c);
SELECT * FROM lcd ORDER BY a;

DROP TABLE lc, lc_heap, lck, lcd;
DROP FUNCTION as_heap(text);
