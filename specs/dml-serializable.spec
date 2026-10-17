# SERIALIZABLE over mergetree tables (ch_table_engines.md §2.6): predicate locks
# on the relation, and a conflict check at the relation's grain on writes.  A
# write skew between two transactions fails one of them, as on heap; each
# answer compared with heap's (test/isolation.sh).

setup
{
  CREATE TABLE oncall (doctor text, shift int, on_duty bool);
  INSERT INTO oncall VALUES ('alice', 1, true), ('bob', 1, true);
}

teardown
{
  DROP TABLE oncall;
}

session s1
setup		{ BEGIN ISOLATION LEVEL SERIALIZABLE; }
step s1count { SELECT count(*) FROM oncall WHERE shift = 1 AND on_duty; }
step s1off	{ UPDATE oncall SET on_duty = false WHERE doctor = 'alice'; }
step s1c	{ COMMIT; }

session s2
setup		{ BEGIN ISOLATION LEVEL SERIALIZABLE; }
step s2count { SELECT count(*) FROM oncall WHERE shift = 1 AND on_duty; }
step s2off	{ UPDATE oncall SET on_duty = false WHERE doctor = 'bob'; }
step s2c	{ COMMIT; }

session s3
step s3sel	{ SELECT doctor, on_duty FROM oncall ORDER BY doctor; }

# each reads both, then takes one off: one of the two must fail
permutation s1count s2count s1off s2off s1c s2c s3sel
permutation s1count s1off s1c s2count s2off s2c s3sel
