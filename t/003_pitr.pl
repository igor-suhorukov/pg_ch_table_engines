# A base backup and point-in-time recovery (ch_table_engines.md §2.9): the
# parts of a load made during the backup, and a recovery target between loads.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('pitr');
$node->init(has_archiving => 1, allows_streaming => 1);
$node->append_conf('postgresql.conf', qq{shared_preload_libraries = 'ch_table_engines'
ch.max_merge_workers = 0});
$node->start;
$node->safe_psql('postgres', q{
	CREATE EXTENSION ch_table_engines;
	CREATE TABLE p (a int, b text) USING mergetree WITH (order_by = 'a', storage = 'files');
	INSERT INTO p SELECT g, 'before backup' FROM generate_series(1, 20000) g;
	CREATE TABLE pp (a int, b text) USING mergetree WITH (order_by = 'a', storage = 'pages');
	INSERT INTO pp SELECT g, 'before backup' FROM generate_series(1, 20000) g;
});
$node->backup('base');
$node->safe_psql('postgres', q{INSERT INTO p SELECT g, 'first' FROM generate_series(20001, 30000) g});
$node->safe_psql('postgres', q{INSERT INTO pp SELECT g, 'first' FROM generate_series(20001, 30000) g});
$node->safe_psql('postgres', q{SELECT pg_create_restore_point('rp')});
$node->safe_psql('postgres', q{INSERT INTO p SELECT g, 'second' FROM generate_series(30001, 40000) g});
$node->safe_psql('postgres', q{INSERT INTO pp SELECT g, 'second' FROM generate_series(30001, 40000) g});
$node->safe_psql('postgres', q{SELECT pg_switch_wal()});
$node->stop;

my $restored = PostgreSQL::Test::Cluster->new('restored');
$restored->init_from_backup($node, 'base', has_restoring => 1);
$restored->append_conf('postgresql.conf', qq{
recovery_target_name = 'rp'
recovery_target_action = 'promote'
});
$restored->start;
$restored->poll_query_until('postgres', 'SELECT NOT pg_is_in_recovery()');
is($restored->safe_psql('postgres', q{SELECT b, count(*) FROM p GROUP BY b ORDER BY b}),
   "before backup|20000\nfirst|10000", 'the rows up to the restore point, and none after');
is($restored->safe_psql('postgres', q{SELECT b, count(*) FROM pp GROUP BY b ORDER BY b}),
   "before backup|20000\nfirst|10000", 'the pages store, up to the restore point');
$restored->safe_psql('postgres', q{INSERT INTO p VALUES (0, 'after recovery')});
is($restored->safe_psql('postgres', q{SELECT count(*) FROM p}), '30001', 'the recovered table writes');

$restored->stop;
done_testing();
