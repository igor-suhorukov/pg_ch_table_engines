# Writers that find their rows through an index (ch_table_engines.md §2.10):
# an index scan's and a bitmap scan's TIDs, the rows fetched through the parts'
# _pg_rowid projections, the new versions of updated rows entered into the
# index; concurrent writers as through a sequential scan.  Answers compared
# with heap's (test/isolation.sh).

setup
{
  CREATE TABLE stock (sku int, qty int);
  INSERT INTO stock SELECT i, 100 FROM generate_series(1, 1000) i;
  CREATE INDEX stock_sku ON stock (sku);
  ANALYZE stock;
}

teardown
{
  DROP TABLE stock;
}

session s1
setup		{ BEGIN; SET LOCAL enable_seqscan = off; SET LOCAL ch.enable_chscan = off; SET LOCAL enable_bitmapscan = off; }
step s1take	{ UPDATE stock SET qty = qty - 1 WHERE sku = 7; }
step s1gone	{ DELETE FROM stock WHERE sku = 8; }
step s1c	{ COMMIT; }

session s2
setup		{ BEGIN; SET LOCAL enable_seqscan = off; SET LOCAL ch.enable_chscan = off; SET LOCAL enable_indexscan = off; }
step s2take	{ UPDATE stock SET qty = qty - 2 WHERE sku BETWEEN 6 AND 8 RETURNING sku, qty; }
step s2c	{ COMMIT; }

session s3
setup		{ SET enable_seqscan = off; SET ch.enable_chscan = off; }
step s3sel	{ SELECT sku, qty FROM stock WHERE sku BETWEEN 5 AND 9 ORDER BY sku; }

permutation s1take s2take s1c s2c s3sel
permutation s1gone s2take s1c s2c s3sel
permutation s1take s1gone s1c s2take s2c s3sel
