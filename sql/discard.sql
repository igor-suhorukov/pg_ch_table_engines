-- Writes, reads, folds and merges with every cache discarded wherever an
-- invalidation may be taken (debug_discard_caches, on a server with assertions;
-- elsewhere the test runs without).  A table of the engine library's cache
-- (table.c) goes at each discard, so code that holds one across a lock or a
-- catalog read without pinning it reads freed memory, which such a server
-- wipes (CLOBBER_FREED_MEMORY): a deletion's tombstone, keyed by the table's
-- storage, went to no table at all.  The answers are heap's.
SET client_min_messages = warning;
SELECT max_val::int > 0 AS can_discard FROM pg_settings WHERE name = 'debug_discard_caches' \gset

CREATE TABLE d_heap (k int, v text, w int);
CREATE TABLE d_mt (k int, v text, w int) USING mergetree WITH (order_by = 'k');
INSERT INTO d_heap SELECT g, md5(g::text), g % 5 FROM generate_series(1, 40) g;
INSERT INTO d_mt SELECT g, md5(g::text), g % 5 FROM generate_series(1, 40) g;
CREATE FUNCTION d_same() RETURNS boolean LANGUAGE sql AS $$
  SELECT NOT EXISTS (SELECT * FROM d_heap EXCEPT ALL SELECT * FROM d_mt)
     AND NOT EXISTS (SELECT * FROM d_mt EXCEPT ALL SELECT * FROM d_heap)
$$;

\if :can_discard
SET debug_discard_caches = 1;
\endif
-- a second part; an index built over both
INSERT INTO d_heap SELECT g, md5(g::text), g % 5 FROM generate_series(41, 60) g;
INSERT INTO d_mt SELECT g, md5(g::text), g % 5 FROM generate_series(41, 60) g;
CREATE INDEX d_mt_w ON d_mt (w);
-- deletions and an update found by the plain scan
DELETE FROM d_heap WHERE k % 7 = 0;
DELETE FROM d_mt WHERE k % 7 = 0;
UPDATE d_heap SET v = 'u' WHERE k % 9 = 0;
UPDATE d_mt SET v = 'u' WHERE k % 9 = 0;
-- through the index, and by TID: no scan of the table pins its cached table
SET enable_seqscan = off;
SET ch.enable_chscan = off;
SET enable_bitmapscan = off;
DELETE FROM d_heap WHERE w = 3;
DELETE FROM d_mt WHERE w = 3;
UPDATE d_heap SET v = 'w' WHERE w = 1;
UPDATE d_mt SET v = 'w' WHERE w = 1;
DELETE FROM d_heap WHERE k = 2;
DELETE FROM d_mt WHERE ctid = (SELECT ctid FROM d_mt WHERE k = 2);
BEGIN;
SELECT k, v FROM d_mt WHERE w = 4 ORDER BY k FOR UPDATE;
COMMIT;
SELECT count(*), sum(k), string_agg(v, ',' ORDER BY k) FROM d_mt WHERE w = 1;
RESET enable_seqscan;
RESET ch.enable_chscan;
RESET enable_bitmapscan;
SELECT d_same();
-- the dead rows folded out, every part merged, VACUUM and ANALYZE
SELECT ch.fold('d_mt') AS folded;
SELECT ch.optimize('d_mt', true) > 0 AS merged;
VACUUM d_mt;
ANALYZE d_mt;
SELECT count(*) AS parts, sum(rows) AS rows FROM ch.parts('d_mt');
SELECT d_same();
SET enable_seqscan = off;
SET ch.enable_chscan = off;
SELECT count(*), sum(k) FROM d_mt WHERE w = 4;
RESET enable_seqscan;
RESET ch.enable_chscan;
SELECT count(*), sum(k) FROM d_heap WHERE w = 4;
RESET debug_discard_caches;

DROP TABLE d_heap, d_mt;
DROP FUNCTION d_same();
