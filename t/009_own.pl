# Tables of their own files (ch_table_engines.md §2.18, CH8) under WAL, as the
# files store's parts are (§2.9): a part's file is logged under its part's
# name before its directory gets it, so that crash recovery writes again the
# parts lost after the last checkpoint and leaves an uncommitted writer's part
# out; a streaming standby replays a load, DELETE and UPDATE, a merge, TRUNCATE
# and DROP, its rows the primary's; pg_dump dumps the rows and restores them.
use strict;
use warnings FATAL => 'all';
use File::Path;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $digest = q{SELECT md5(string_agg(id || ':' || coalesce(t, '-') || ':' || coalesce(d::text, '-'), ',' ORDER BY id)) FROM %s};

# ---- crash recovery ----
my $node = PostgreSQL::Test::Cluster->new('own_crash');
$node->init(allows_streaming => 1);
$node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'ch_table_engines'
ch.max_merge_workers = 0
fsync = on
});
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION ch_table_engines');
for my $am ('parquet', 'arrow')
{
	$node->safe_psql('postgres', qq{
		CREATE TABLE c_$am (id int NOT NULL, t text, d date) USING $am;
		INSERT INTO c_$am SELECT g, md5(g::text), DATE '2020-01-01' + g % 1000 FROM generate_series(1, 200000) g;
		DELETE FROM c_$am WHERE id % 7 = 0;
	});
	my $sum = $node->safe_psql('postgres', sprintf($digest, "c_$am"));
	my $dir = $node->data_dir . '/' . $node->safe_psql('postgres',
		qq{SELECT 'base/' || (SELECT oid FROM pg_database WHERE datname = 'postgres') || '/ch_' || relfilenode FROM pg_class WHERE relname = 'c_$am'});

	# a writer whose part is written, its catalog row not committed, at the crash
	my $bg = $node->background_psql('postgres');
	$bg->query_safe(q{BEGIN});
	$bg->query_safe(qq{INSERT INTO c_$am SELECT g, 'uncommitted', NULL FROM generate_series(300001, 310000) g});
	$bg->query_safe(qq{SELECT count(*) FROM c_$am});
	$node->stop('immediate');
	$bg->{run}->kill_kill;
	$node->start;
	is($node->safe_psql('postgres', qq{SELECT count(*) FROM c_$am}), '171429', "$am: committed rows after a crash");
	is($node->safe_psql('postgres', qq{SELECT count(*) FROM c_$am WHERE t = 'uncommitted'}), '0',
		"$am: the uncommitted writer's rows stay invisible");
	is($node->safe_psql('postgres', sprintf($digest, "c_$am")), $sum, "$am: the same rows");

	# parts committed after the last checkpoint, lost, written again by recovery from WAL
	$node->safe_psql('postgres', q{CHECKPOINT});
	$node->safe_psql('postgres', qq{INSERT INTO c_$am SELECT g, md5(g::text), NULL FROM generate_series(200001, 250000) g});
	$node->safe_psql('postgres', qq{UPDATE c_$am SET t = 'updated' WHERE id % 1000 = 1});
	my $sum2 = $node->safe_psql('postgres', sprintf($digest, "c_$am"));
	my @new_parts = split /\n/, $node->safe_psql('postgres', qq{SELECT part FROM ch.own_files('c_$am') WHERE min_rowid > 200000 ORDER BY part});
	is(scalar(@new_parts), 2, "$am: two parts after the checkpoint");
	$node->stop('immediate');
	for my $p (@new_parts)
	{
		File::Path::rmtree("$dir/$p");
		ok(!-d "$dir/$p", "$am: part $p removed before recovery");
	}
	$node->start;
	for my $p (@new_parts)
	{
		ok(-f "$dir/$p/data.$am", "$am: part ${p}'s file is there after recovery");
	}
	is($node->safe_psql('postgres', sprintf($digest, "c_$am")), $sum2, "$am: every committed row after recovery");
	# the dead writer's directory, which no part names, goes with VACUUM's sweep
	$node->safe_psql('postgres', qq{VACUUM c_$am});
	my @dirs = grep { -d "$dir/$_" } do { opendir(my $dh, $dir) or die; grep { !/^\./ } readdir($dh) };
	my $names = $node->safe_psql('postgres', qq{SELECT string_agg(part, ' ' ORDER BY part) FROM ch.own_files('c_$am')});
	is(join(' ', sort @dirs), $names, "${am}: the parts' directories alone after VACUUM");
	$node->safe_psql('postgres', qq{INSERT INTO c_$am VALUES (0, 'after', NULL)});
	is($node->safe_psql('postgres', qq{SELECT count(*) FROM c_$am WHERE id = 0}), '1', "$am: inserts after recovery");
}
$node->stop;

# ---- a streaming standby ----
my $primary = PostgreSQL::Test::Cluster->new('own_primary');
$primary->init(allows_streaming => 1);
$primary->append_conf('postgresql.conf', qq{shared_preload_libraries = 'ch_table_engines'
ch.max_merge_workers = 0});
$primary->start;
$primary->safe_psql('postgres', 'CREATE EXTENSION ch_table_engines');
$primary->backup('base');
my $standby = PostgreSQL::Test::Cluster->new('own_standby');
$standby->init_from_backup($primary, 'base', has_streaming => 1);
$standby->start;

$primary->safe_psql('postgres', q{
	CREATE TABLE s (id int NOT NULL, t text, d date) USING parquet WITH (codec = 'zstd');
	INSERT INTO s SELECT g, md5(g::text), DATE '2020-01-01' + g % 1000 FROM generate_series(1, 150000) g;
	INSERT INTO s SELECT g, 'second', NULL FROM generate_series(150001, 160000) g;
	CREATE TABLE sa (id int NOT NULL, t text, d date) USING arrow;
	INSERT INTO sa SELECT g, md5(g::text), NULL FROM generate_series(1, 50000) g;
});
$primary->wait_for_catchup($standby);
for my $t ('s', 'sa')
{
	is($standby->safe_psql('postgres', sprintf($digest, $t)), $primary->safe_psql('postgres', sprintf($digest, $t)),
		"standby: ${t}'s rows as the primary's");
}
$primary->safe_psql('postgres', q{
	DELETE FROM s WHERE id % 3 = 0;
	UPDATE s SET t = 'updated' WHERE id % 1000 = 1;
	DELETE FROM sa WHERE id > 40000;
});
$primary->wait_for_catchup($standby);
is($standby->safe_psql('postgres', sprintf($digest, 's')), $primary->safe_psql('postgres', sprintf($digest, 's')),
	'standby: the rows after DELETE and UPDATE');
is($primary->safe_psql('postgres', q{SELECT ch.optimize('s', true)}), '3', 'primary: three parts merged');
$primary->wait_for_catchup($standby);
is($standby->safe_psql('postgres', sprintf($digest, 's')), $primary->safe_psql('postgres', sprintf($digest, 's')),
	'standby: the rows after a merge');
is($standby->safe_psql('postgres', q{SELECT string_agg(part || ' ' || rows || ' ' || units || ' ' || codec, ', ') FROM ch.own_files('s')}),
	$primary->safe_psql('postgres', q{SELECT string_agg(part || ' ' || rows || ' ' || units || ' ' || codec, ', ') FROM ch.own_files('s')}),
	"standby: the merged part's file as the primary's");
my $sdir = $standby->data_dir . '/' . $standby->safe_psql('postgres',
	q{SELECT 'base/' || (SELECT oid FROM pg_database WHERE datname = 'postgres') || '/ch_' || relfilenode FROM pg_class WHERE relname = 'sa'});
ok(-d $sdir, "standby: sa's directory");
$primary->safe_psql('postgres', q{TRUNCATE s; DROP TABLE sa});
$primary->wait_for_catchup($standby);
is($standby->safe_psql('postgres', q{SELECT count(*) FROM s}), '0', 'standby: TRUNCATE');
ok(!-d $sdir, 'standby: the dropped table\'s directory removed');
$standby->stop;
$primary->stop;

# ---- pg_dump and restore ----
my $dnode = PostgreSQL::Test::Cluster->new('own_dump');
$dnode->init;
$dnode->append_conf('postgresql.conf', qq{shared_preload_libraries = 'ch_table_engines'
ch.max_merge_workers = 0});
$dnode->start;
$dnode->safe_psql('postgres', 'CREATE DATABASE src');
$dnode->safe_psql('postgres', 'CREATE DATABASE dst');
$dnode->safe_psql('src', q{
	CREATE EXTENSION ch_table_engines;
	CREATE TABLE dp (id int NOT NULL, t text, d date) USING parquet WITH (codec = 'zstd');
	INSERT INTO dp SELECT g, md5(g::text), DATE '2020-01-01' + g % 1000 FROM generate_series(1, 30000) g;
	DELETE FROM dp WHERE id % 4 = 0;
	CREATE TABLE da (id int NOT NULL, t text, d date) USING arrow;
	INSERT INTO da SELECT g, md5(g::text), NULL FROM generate_series(1, 20000) g;
	CREATE INDEX dp_id ON dp (id);
});
my $dump = $dnode->basedir . '/own.dump';
$dnode->command_ok([ 'pg_dump', '-Fc', '-f', $dump, '-d', $dnode->connstr('src') ], 'pg_dump of tables of their own files');
$dnode->command_ok([ 'pg_restore', '-d', $dnode->connstr('dst'), $dump ], 'pg_restore');
for my $t ('dp', 'da')
{
	is($dnode->safe_psql('dst', sprintf($digest, $t)), $dnode->safe_psql('src', sprintf($digest, $t)), "restore: ${t}'s rows");
}
is($dnode->safe_psql('dst', q{SELECT amname FROM pg_class c JOIN pg_am a ON a.oid = c.relam WHERE relname = 'dp'}), 'parquet',
	'restore: the table of its own files is one again');
is($dnode->safe_psql('dst', q{SELECT codec FROM ch.own_files('dp') LIMIT 1}), 'ZSTD', 'restore: its codec');
$dnode->stop;

done_testing();
