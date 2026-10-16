# Two sessions at once over one table of an engine (ch_table_engines.md §2.6,
# §6 "isolation"): writers take disjoint row ids, snapshots keep their view,
# SERIALIZABLE sees a write skew at the relation's grain, TRUNCATE waits for
# an open writer.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('concurrency');
$node->init;
$node->append_conf('postgresql.conf', qq{shared_preload_libraries = 'ch_table_engines'});
$node->start;
$node->safe_psql('postgres', q{
	CREATE EXTENSION ch_table_engines;
	CREATE TABLE c (k int, v text) USING mergetree WITH (order_by = 'k');
});

# writers at once: each its row id ranges, both visible after commit
my $s1 = $node->background_psql('postgres', on_error_stop => 0);
my $s2 = $node->background_psql('postgres', on_error_stop => 0);
$s1->query_safe(q{BEGIN});
$s2->query_safe(q{BEGIN});
$s1->query_safe(q{INSERT INTO c SELECT g, 's1' FROM generate_series(1, 5000) g});
$s2->query_safe(q{INSERT INTO c SELECT g, 's2' FROM generate_series(1, 5000) g});
$s1->query_safe(q{INSERT INTO c SELECT g, 's1' FROM generate_series(5001, 6000) g});
is($s2->query_safe(q{SELECT count(*) FROM c}), '5000', 'a writer sees only its own uncommitted rows');
$s1->query_safe(q{COMMIT});
$s2->query_safe(q{COMMIT});
is($node->safe_psql('postgres', q{SELECT count(*), count(DISTINCT ctid) FROM c}), '11000|11000',
   'both writers\' rows, every TID distinct');

# REPEATABLE READ keeps its snapshot while another session commits
$s1->query_safe(q{BEGIN ISOLATION LEVEL REPEATABLE READ});
is($s1->query_safe(q{SELECT count(*) FROM c}), '11000', 'the snapshot');
$s2->query_safe(q{INSERT INTO c VALUES (0, 'later')});
is($s1->query_safe(q{SELECT count(*) FROM c}), '11000', 'a later commit is not seen');
$s1->query_safe(q{COMMIT});
is($s1->query_safe(q{SELECT count(*) FROM c}), '11001', 'and is, in the next transaction');

# SERIALIZABLE: each reads what the other writes; one of them must fail
$node->safe_psql('postgres', q{CREATE TABLE doctors (name text, on_call bool) USING mergetree;
	INSERT INTO doctors VALUES ('alice', true), ('bob', true)});
$s1->query_safe(q{BEGIN ISOLATION LEVEL SERIALIZABLE});
$s2->query_safe(q{BEGIN ISOLATION LEVEL SERIALIZABLE});
$s1->query_safe(q{SELECT count(*) FROM doctors WHERE on_call});
$s2->query_safe(q{SELECT count(*) FROM doctors WHERE on_call});
$s1->query_safe(q{INSERT INTO doctors VALUES ('carol', false)});
my ($out2, $err2) = ('', '');
$out2 = $s2->query(q{INSERT INTO doctors VALUES ('dave', false)});
$err2 = $s2->{stderr};
my $c1 = $s1->query(q{COMMIT});
my $e1 = $s1->{stderr};
my $c2 = $s2->query(q{COMMIT});
my $e2 = $s2->{stderr};
my $failures = grep { /could not serialize access/ } ($err2, $e1, $e2);
ok($failures >= 1, 'a write skew under SERIALIZABLE fails one transaction');

# TRUNCATE waits for an open writer
$s1->query_safe(q{BEGIN});
$s1->query_safe(q{INSERT INTO c VALUES (-1, 'open')});
$s2->query_until(qr/started/, q{\echo started
SET lock_timeout = '2s';
TRUNCATE c;
});
my $res = $s2->query(q{SELECT 1});
like($s2->{stderr}, qr/lock timeout/, 'TRUNCATE waits for the writer');
$s1->query_safe(q{COMMIT});
is($node->safe_psql('postgres', q{SELECT count(*) FROM c WHERE k = -1}), '1', "the writer's row");

$s1->quit;
$s2->quit;
$node->stop;
done_testing();
