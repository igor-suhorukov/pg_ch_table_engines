# pg_dump and restore (ch_table_engines.md §2.13): a table's access method and
# options travel in the dump, its rows by COPY through the access method.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('dump');
$node->init;
$node->append_conf('postgresql.conf', qq{shared_preload_libraries = 'ch_table_engines'});
$node->start;
$node->safe_psql('postgres', q{CREATE DATABASE src; CREATE DATABASE dst});
$node->safe_psql('src', q{
	CREATE EXTENSION ch_table_engines;
	CREATE TABLE d1 (a int, b text, c date) USING mergetree
		WITH (order_by = 'a, c', compression = 'zstd(3)', parallel_workers = 2, storage = 'files');
	INSERT INTO d1 SELECT g, md5(g::text), '2026-01-01'::date + g % 365 FROM generate_series(1, 30000) g;
	CREATE TABLE d2 (k int, ver int NOT NULL, x text) USING replacingmergetree WITH (order_by = 'k', ver = 'ver');
	INSERT INTO d2 SELECT g % 100, g, g::text FROM generate_series(1, 1000) g;
	CREATE TABLE h (a int);
	INSERT INTO h VALUES (1);
	CREATE TABLE dp (a int, b text) USING mergetree WITH (order_by = 'a', storage = 'pages');
	INSERT INTO dp SELECT g, md5(g::text) FROM generate_series(1, 20000) g;
});
my $dump = $node->basedir . '/src.dump';
$node->command_ok([ 'pg_dump', '-Fc', '-f', $dump, '-d', $node->connstr('src') ], 'pg_dump');
$node->command_ok([ 'pg_restore', '-d', $node->connstr('dst'), $dump ], 'pg_restore');

for my $q (
	q{SELECT c.relname, a.amname, c.reloptions FROM pg_class c JOIN pg_am a ON a.oid = c.relam WHERE c.relname IN ('d1', 'd2', 'h', 'dp') ORDER BY 1},
	q{SELECT count(*), sum(a), md5(string_agg(b || c, ',' ORDER BY a)) FROM d1},
	q{SELECT count(*), sum(k), sum(ver), md5(string_agg(x, ',' ORDER BY ver)) FROM d2},
	q{SELECT * FROM h},
	q{SELECT c.reloptions, count(*), sum(a), md5(string_agg(b, ',' ORDER BY a)) FROM dp, pg_class c WHERE c.relname = 'dp' GROUP BY 1})
{
	is($node->safe_psql('dst', $q), $node->safe_psql('src', $q), "restored: $q");
}
is($node->safe_psql('dst', q{SELECT count(*) FROM ch.parts('d1')}) > 0 ? 1 : 0, 1, 'the restored table has parts');

$node->stop;
done_testing();
