# Concurrent UPDATE and DELETE of a row under REPEATABLE READ: a writer that
# finds the row changed by a transaction its snapshot does not see fails with
# a serialization failure, once that transaction commits; after an abort it
# goes on.  Answers compared with heap's (test/isolation.sh).

setup
{
  CREATE TABLE acct (k int, v int);
  INSERT INTO acct VALUES (1, 0), (2, 0);
}

teardown
{
  DROP TABLE acct;
}

session s1
setup		{ BEGIN ISOLATION LEVEL REPEATABLE READ; }
step s1inc	{ UPDATE acct SET v = v + 1 WHERE k = 1; }
step s1del	{ DELETE FROM acct WHERE k = 1; }
step s1c	{ COMMIT; }
step s1a	{ ROLLBACK; }

session s2
setup		{ BEGIN ISOLATION LEVEL REPEATABLE READ; }
step s2snap	{ SELECT k, v FROM acct ORDER BY k; }
step s2inc	{ UPDATE acct SET v = v + 1 WHERE k = 1; }
step s2del	{ DELETE FROM acct WHERE k = 1; }
step s2lock	{ SELECT k, v FROM acct WHERE k = 1 FOR UPDATE; }
step s2other { UPDATE acct SET v = v + 1 WHERE k = 2; }
step s2c	{ COMMIT; }

session s3
step s3sel	{ SELECT k, v FROM acct ORDER BY k; }

permutation s2snap s1inc s2inc s1c s2c s3sel
permutation s2snap s1inc s2inc s1a s2c s3sel
permutation s2snap s1del s2inc s1c s2c s3sel
permutation s2snap s1inc s2del s1c s2c s3sel
permutation s2snap s1inc s2lock s1c s2c s3sel
# a row the other did not touch: no conflict
permutation s2snap s1inc s2other s1c s2c s3sel
# the change committed before the snapshot: the writer sees it, and goes on
permutation s1inc s1c s2snap s2inc s2c s3sel
