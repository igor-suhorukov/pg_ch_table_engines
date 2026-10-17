# Merges beside concurrent DML (ch_table_engines.md §2.11, CH3): a merge of
# every part (ch.optimize(rel, true), through merge_all(), which does nothing
# over heap) between another transaction's steps -- its update, deletion, lock
# or insert in progress, its REPEATABLE READ snapshot open -- changes no answer.
# Run over heap and over mergetree, whose answers must be heap's
# (test/isolation.sh).

setup
{
  CREATE TABLE acct (k int, v int);
  INSERT INTO acct SELECT g, 0 FROM generate_series(1, 20) g;
  INSERT INTO acct SELECT g, 0 FROM generate_series(21, 40) g;
  INSERT INTO acct SELECT g, 0 FROM generate_series(41, 60) g;
  CREATE FUNCTION merge_all(r regclass) RETURNS void LANGUAGE plpgsql AS $$
  BEGIN
    IF (SELECT a.amname FROM pg_class c JOIN pg_am a ON a.oid = c.relam WHERE c.oid = r) = 'mergetree' THEN
      PERFORM ch.optimize(r, true);
    END IF;
  END
  $$;
}

teardown
{
  DROP TABLE acct;
  DROP FUNCTION merge_all(regclass);
}

session s1
step s1b	{ BEGIN; }
step s1rr	{ BEGIN ISOLATION LEVEL REPEATABLE READ; }
step s1upd	{ UPDATE acct SET v = v + 1 WHERE k = 1; }
step s1del	{ DELETE FROM acct WHERE k BETWEEN 2 AND 5; }
step s1lock	{ SELECT k, v FROM acct WHERE k = 30 FOR UPDATE; }
step s1ins	{ INSERT INTO acct VALUES (100, 1); }
step s1sum	{ SELECT count(*), sum(k), sum(v) FROM acct; }
step s1c	{ COMMIT; }
step s1a	{ ROLLBACK; }

session s2
step s2merge	{ SELECT merge_all('acct'); }
step s2b	{ BEGIN; }
step s2mergetx	{ SELECT merge_all('acct'); }
step s2upd30	{ UPDATE acct SET v = v + 10 WHERE k = 30; }
step s2upd1	{ UPDATE acct SET v = v + 100 WHERE k = 1; }
step s2del	{ DELETE FROM acct WHERE k = 50; }
step s2sum	{ SELECT count(*), sum(k), sum(v) FROM acct; }
step s2rows	{ SELECT k, v FROM acct WHERE k IN (1, 2, 30, 50, 100) ORDER BY k; }
step s2c	{ COMMIT; }

# an update in progress: the merge carries the row and its tombstone, and the
# update, once committed, is what the next statements see
permutation s1b s1upd s2merge s1c s2rows s2sum
permutation s1b s1upd s2merge s1a s2rows s2sum
# a deletion in progress, then the merge again after it committed: the rows go
permutation s1b s1del s2merge s1c s2merge s2rows s2sum
# a lock in progress: the merge passes it over, the update waits for the lock
permutation s1b s1lock s2merge s2upd30 s1c s2rows
# an insert in progress: its part not the merge's, and seen once committed
permutation s1b s1ins s2merge s1c s2rows s2sum
# a REPEATABLE READ snapshot: the merge's new part not seen, the answers as before
permutation s1rr s1sum s2del s2merge s1sum s1c s1sum
# a merge in a transaction beside writers: they go on, and wait only for each other
permutation s2b s2mergetx s1b s1upd s1c s2upd1 s2c s2rows s2sum
