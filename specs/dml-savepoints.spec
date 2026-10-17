# Deletions and updates made in a subtransaction: rolled back with it, so that
# a writer waiting on the row goes on; a deletion in a released savepoint holds
# until the transaction ends.  Answers compared with heap's (test/isolation.sh).

setup
{
  CREATE TABLE items (id int, qty int);
  INSERT INTO items VALUES (1, 10), (2, 20);
}

teardown
{
  DROP TABLE items;
}

session s1
setup		{ BEGIN; }
step s1sp	{ SAVEPOINT a; }
step s1del	{ DELETE FROM items WHERE id = 1; }
step s1upd	{ UPDATE items SET qty = qty - 1 WHERE id = 1; }
step s1rb	{ ROLLBACK TO a; }
step s1rel	{ RELEASE a; }
step s1sel	{ SELECT id, qty FROM items ORDER BY id; }
step s1c	{ COMMIT; }

session s2
setup		{ BEGIN; }
step s2upd	{ UPDATE items SET qty = qty + 5 WHERE id = 1; }
step s2c	{ COMMIT; }

session s3
step s3sel	{ SELECT id, qty FROM items ORDER BY id; }

permutation s1sp s1del s2upd s1rb s1sel s1c s2c s3sel
permutation s1sp s1upd s2upd s1rb s1c s2c s3sel
permutation s1sp s1del s1rel s2upd s1c s2c s3sel
permutation s1sp s1upd s1upd s1rb s1sel s1c s3sel
