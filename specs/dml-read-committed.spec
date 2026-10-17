# Concurrent UPDATE and DELETE of a row under READ COMMITTED (ch_table_engines.md
# §2.10): the second writer waits for the first; after a commit it re-checks the
# row's newest version (EvalPlanQual), after an abort it takes the row as it was.
# Run over heap and over mergetree, whose answers must be heap's (test/isolation.sh).

setup
{
  CREATE TABLE acct (k int, v int);
  INSERT INTO acct VALUES (1, 0), (2, 0), (3, 0);
}

teardown
{
  DROP TABLE acct;
}

session s1
setup		{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step s1inc	{ UPDATE acct SET v = v + 1 WHERE k = 1; }
step s1set	{ UPDATE acct SET v = 10 WHERE k = 1; }
step s1del	{ DELETE FROM acct WHERE k = 1; }
step s1c	{ COMMIT; }
step s1a	{ ROLLBACK; }

session s2
setup		{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step s2inc	{ UPDATE acct SET v = v + 1 WHERE k = 1 RETURNING k, v; }
step s2zero	{ UPDATE acct SET v = v + 100 WHERE k = 1 AND v = 0 RETURNING k, v; }
step s2del	{ DELETE FROM acct WHERE k = 1 RETURNING k, v; }
step s2sel	{ SELECT k, v FROM acct ORDER BY k; }
step s2c	{ COMMIT; }

# two increments: the second applied to the first's new version
permutation s1inc s2inc s1c s2sel s2c
# the first aborts: the second increments the row as it was
permutation s1inc s2inc s1a s2sel s2c
# a re-check that fails: the new version no longer has v = 0
permutation s1set s2zero s1c s2sel s2c
# a re-check that passes after an abort
permutation s1set s2zero s1a s2sel s2c
# update then delete: the delete takes the new version
permutation s1inc s2del s1c s2sel s2c
# delete then update: nothing left to update
permutation s1del s2inc s1c s2sel s2c
# two deletes: the second finds the row gone
permutation s1del s2del s1c s2sel s2c
# the first delete aborts: the second deletes
permutation s1del s2del s1a s2sel s2c
