-- A part's summary (ch_table_engines.md §3, VC20's B6 and B7): each column's
-- bytes and statistics in its ch.parts row, the engine library's, written by
-- every writer of a part; the planner's estimate and a scan's choice of parts
-- taken from it without loading the part.  Every plan, count and answer the
-- same as from the parts themselves (ch.enable_part_summaries and
-- ch.enable_summary_pruning off).
SET client_min_messages = warning;
SET max_parallel_workers_per_gather = 0;
SET ch.insert_buffer_rows = 20000;
CREATE TABLE su (a int NOT NULL, b int, c numeric(10, 2), d date, t text)
  USING mergetree WITH (order_by = 'a', settings = 'index_granularity = 1024');
-- five parts of 20,000 rows, a's ranges apart; b NULL in a seventh of the rows
INSERT INTO su SELECT i, CASE WHEN i % 7 = 0 THEN NULL ELSE i * 3 END, i / 4.0,
    date '2020-01-01' + i / 100, 'k' || (i % 100)
  FROM generate_series(1, 100000) i;
RESET ch.insert_buffer_rows;

-- how many of the table's parts have a summary in their rows
CREATE FUNCTION summarized(rel regclass) RETURNS text LANGUAGE sql AS $$
  SELECT format('%s parts, %s summarized', count(*), count(p.summary))
    FROM ch.parts p WHERE p.relfilenumber = pg_relation_filenode(rel) $$;
-- a query's plan, costs and all, and its answer, with the summaries and without
CREATE FUNCTION same(q text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  plans text[] := '{}';
  answers text[] := '{}';
  plan text;
  answer text;
  line text;
BEGIN
  FOREACH line IN ARRAY ARRAY['on', 'off'] LOOP
    PERFORM set_config('ch.enable_part_summaries', line, true);
    PERFORM set_config('ch.enable_summary_pruning', line, true);
    plan := '';
    FOR answer IN EXECUTE 'EXPLAIN (VERBOSE) ' || q LOOP
      plan := plan || answer || E'\n';
    END LOOP;
    EXECUTE 'SELECT md5(coalesce(string_agg(r::text, '','' ORDER BY r::text), '''')) FROM (' || q || ') r'
      INTO answer;
    plans := plans || plan;
    answers := answers || answer;
  END LOOP;
  RETURN CASE WHEN answers[1] <> answers[2] THEN 'ANSWERS DIFFER'
              WHEN plans[1] <> plans[2] THEN 'PLANS DIFFER: ' || plans[1] || plans[2]
              ELSE 'same' END;
END $$;
-- what ChScan read, from EXPLAIN ANALYZE, with the summaries and without
CREATE FUNCTION read_by(q text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  got text[] := '{}';
  plan jsonb;
  node jsonb;
  s text;
BEGIN
  FOREACH s IN ARRAY ARRAY['on', 'off'] LOOP
    PERFORM set_config('ch.enable_part_summaries', s, true);
    PERFORM set_config('ch.enable_summary_pruning', s, true);
    EXECUTE 'EXPLAIN (ANALYZE, FORMAT JSON) ' || q INTO plan;
    node := jsonb_path_query_first(plan, 'strict $.**? (@."Custom Plan Provider" == "ChScan")');
    got := got || format('%s read of %s, parts pruned: %s', node->>'Granules Read', node->>'Granules',
                         node->>'Parts Pruned');
  END LOOP;
  RETURN CASE WHEN got[1] = got[2] THEN got[1] ELSE 'DIFFER: ' || got[1] || ' / ' || got[2] END;
END $$;
CREATE FUNCTION all_same() RETURNS SETOF text LANGUAGE sql AS $$
  SELECT same(q) FROM unnest(ARRAY[
    'SELECT count(*), sum(c) FROM su WHERE a BETWEEN 1000 AND 2000',
    'SELECT count(*) FROM su WHERE b <> 9',
    'SELECT count(*), max(a) FROM su WHERE b > 200000',
    'SELECT count(*) FROM su WHERE d < date ''2020-03-01''',
    'SELECT a, t FROM su WHERE t = ''k5'' AND a > 99000',
    'SELECT count(*) FROM su WHERE b IS NULL',
    'SELECT count(*) FROM su WHERE a < 0',
    'SELECT min(a), max(a), count(c) FROM su',
    'SELECT * FROM su WHERE a = 54321']) q $$;

SELECT summarized('su');
SELECT * FROM all_same();
-- the key's range: one part read, the others ruled out by their statistics
SELECT read_by('SELECT count(*) FROM su WHERE a BETWEEN 1000 AND 2000');
-- a qual outside the key that statistics prune: the parts whose b is all below
SELECT read_by('SELECT count(*) FROM su WHERE b > 200000');
-- nothing pruned: every granule of every part
SELECT read_by('SELECT count(*) FROM su WHERE b <> 9');

-- a part whose row has no summary, and one whose summary is of another
-- format: each loaded where it is asked of, as before
UPDATE ch.parts SET summary = NULL
  WHERE relfilenumber = pg_relation_filenode('su') AND name = (SELECT min(name) FROM ch.parts('su'));
UPDATE ch.parts SET summary = '\x00'::bytea
  WHERE relfilenumber = pg_relation_filenode('su') AND name = (SELECT max(name) FROM ch.parts('su'));
SELECT summarized('su');
SELECT * FROM all_same();
SELECT read_by('SELECT count(*) FROM su WHERE b > 200000');

-- a fold of deletions, a merge, a rewrite: each new part with its summary
DELETE FROM su WHERE a % 1000 = 3;
SELECT ch.fold('su');
SELECT summarized('su');
SELECT * FROM all_same();
DELETE FROM su WHERE a % 1000 = 5;
SELECT ch.optimize('su', true);
SELECT summarized('su');
SELECT * FROM all_same();
VACUUM FULL su;
SELECT summarized('su');
SELECT * FROM all_same();
SELECT count(*), count(b), min(b), max(b), sum(c) FROM su;

DROP TABLE su;
DROP FUNCTION all_same(), same(text), read_by(text), summarized(regclass);
