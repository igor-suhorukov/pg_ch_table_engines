# Row locks over mergetree tables (ch_table_engines.md §2.10): SELECT ... FOR
# UPDATE blocks a writer and another locker until it commits; SKIP LOCKED passes
# the locked row over, NOWAIT fails at once; a lock taken in a savepoint goes
# with it.  Answers compared with heap's (test/isolation.sh).

setup
{
  CREATE TABLE queue (id int, payload text);
  INSERT INTO queue SELECT i, 'job ' || i FROM generate_series(1, 5) i;
}

teardown
{
  DROP TABLE queue;
}

session s1
setup		{ BEGIN; }
step s1lock	{ SELECT id FROM queue WHERE id = 1 FOR UPDATE; }
step s1sp	{ SAVEPOINT a; }
step s1lock2 { SELECT id FROM queue WHERE id = 2 FOR UPDATE; }
step s1rb	{ ROLLBACK TO a; }
step s1c	{ COMMIT; }

session s2
setup		{ BEGIN; }
step s2lock	{ SELECT id FROM queue WHERE id = 1 FOR UPDATE; }
step s2lock2 { SELECT id FROM queue WHERE id = 2 FOR UPDATE; }
step s2skip	{ SELECT id FROM queue ORDER BY id LIMIT 2 FOR UPDATE SKIP LOCKED; }
step s2nowait { SELECT id FROM queue WHERE id = 1 FOR UPDATE NOWAIT; }
step s2upd	{ UPDATE queue SET payload = 'taken' WHERE id = 1; }
step s2del	{ DELETE FROM queue WHERE id = 1; }
step s2c	{ COMMIT; }

session s3
step s3sel	{ SELECT id, payload FROM queue ORDER BY id; }

permutation s1lock s2lock s1c s2c s3sel
permutation s1lock s2upd s1c s2c s3sel
permutation s1lock s2del s1c s2c s3sel
permutation s1lock s2skip s1c s2c s3sel
permutation s1lock s2nowait s1c s2c s3sel
# a lock rolled back with its savepoint: no longer in the way
permutation s1lock s1sp s1lock2 s1rb s2lock2 s1c s2c s3sel
