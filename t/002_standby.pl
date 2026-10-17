# A streaming standby replays the files store's WAL (ch_table_engines.md §2.9):
# a load, then TRUNCATE and DROP, read on the standby as on the primary.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 1);
$primary->append_conf('postgresql.conf', qq{shared_preload_libraries = 'ch_table_engines'
ch.max_merge_workers = 0});
$primary->start;
$primary->safe_psql('postgres', 'CREATE EXTENSION ch_table_engines');
$primary->backup('base');

my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($primary, 'base', has_streaming => 1);
$standby->start;

$primary->safe_psql('postgres', q{
	CREATE TABLE s (a int, b text, c numeric(12, 2)) USING mergetree WITH (order_by = 'a', storage = 'files');
	INSERT INTO s SELECT g, md5(g::text), g / 3.0 FROM generate_series(1, 50000) g;
	INSERT INTO s SELECT g, 'second', 0 FROM generate_series(50001, 60000) g;
	CREATE TABLE s2 (a int) USING mergetree WITH (storage = 'files');
	INSERT INTO s2 SELECT generate_series(1, 1000);
	CREATE TABLE sp (a int, b text) USING mergetree WITH (order_by = 'a', storage = 'pages');
	INSERT INTO sp SELECT g, md5(g::text) FROM generate_series(1, 40000) g;
});
$primary->wait_for_catchup($standby);
my $qp = q{SELECT count(*), sum(a), md5(string_agg(b, ',' ORDER BY a)) FROM sp};
is($standby->safe_psql('postgres', $qp), $primary->safe_psql('postgres', $qp), 'the standby reads the pages store');
$primary->safe_psql('postgres', q{TRUNCATE sp; INSERT INTO sp VALUES (1, 'after truncate')});
$primary->wait_for_catchup($standby);
is($standby->safe_psql('postgres', q{SELECT count(*), max(b) FROM sp}), '1|after truncate', 'TRUNCATE of the pages store followed');

my $q = q{SELECT count(*), sum(a), sum(c), md5(string_agg(b, ',' ORDER BY a)) FROM s};
is($standby->safe_psql('postgres', $q), $primary->safe_psql('postgres', $q), 'the standby reads the load');
is($standby->safe_psql('postgres', q{SELECT count(*) FROM ch.parts('s')}), '2', 'its parts');

# TRUNCATE: the standby follows to the new storage
my $old = $primary->safe_psql('postgres', q{SELECT relfilenode FROM pg_class WHERE relname = 's'});
$primary->safe_psql('postgres', q{TRUNCATE s; INSERT INTO s VALUES (1, 'after truncate', 1)});
$primary->wait_for_catchup($standby);
is($standby->safe_psql('postgres', q{SELECT count(*), max(b) FROM s}), '1|after truncate', 'TRUNCATE followed');
my $db = $primary->safe_psql('postgres', q{SELECT oid FROM pg_database WHERE datname = 'postgres'});
ok(!-d $standby->data_dir . "/base/$db/ch_$old", "the old storage's directory is gone on the standby");

# DROP: its directory goes on the standby too
my $s2 = $primary->safe_psql('postgres', q{SELECT relfilenode FROM pg_class WHERE relname = 's2'});
ok(-d $standby->data_dir . "/base/$db/ch_$s2", "a table's directory on the standby");
$primary->safe_psql('postgres', q{DROP TABLE s2});
$primary->wait_for_catchup($standby);
ok(!-d $standby->data_dir . "/base/$db/ch_$s2", "the dropped table's directory is gone on the standby");

# promoted, the standby writes
$standby->promote;
$standby->poll_query_until('postgres', 'SELECT NOT pg_is_in_recovery()');
$standby->safe_psql('postgres', q{INSERT INTO s VALUES (2, 'on the new primary', 2)});
is($standby->safe_psql('postgres', q{SELECT count(*) FROM s}), '2', 'the promoted standby writes');

$standby->stop;
$primary->stop;
done_testing();
