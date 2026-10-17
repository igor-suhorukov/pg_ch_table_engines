# Crash recovery of both stores (ch_table_engines.md §2.9, §5 CH0): what
# committed comes back, what did not commit stays invisible.  In the files
# store, with wal_level = minimal a part's files are synced before its row
# commits and carry no WAL; above it, parts lost after the last checkpoint are
# written again from WAL.  The pages store's data are pages, WAL-logged as
# full-page images.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

for my $case (['files', 'minimal'], ['files', 'replica'], ['pages', 'minimal'], ['pages', 'replica'])
{
	my ($store, $wal) = @$case;
	my $level = "$store/$wal";
	(my $name = "crash_${store}_$wal") =~ s/\W/_/g;
	my $node = PostgreSQL::Test::Cluster->new($name);
	$node->init(allows_streaming => $wal eq 'replica' ? 1 : 0);
	$node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'ch_table_engines'
ch.max_merge_workers = 0
fsync = on
});
	$node->start;
	$node->safe_psql('postgres', 'CREATE EXTENSION ch_table_engines');
	$node->safe_psql('postgres', q{
		CREATE TABLE t (a int, b text) USING mergetree WITH (order_by = 'a', storage = '} . $store . q{');
		INSERT INTO t SELECT g, md5(g::text) FROM generate_series(1, 100000) g;
	});
	my $sum = $node->safe_psql('postgres', q{SELECT md5(string_agg(b, ',' ORDER BY a)) FROM t});
	my $relpath = $node->safe_psql('postgres', q{SELECT 'base/' || (SELECT oid FROM pg_database WHERE datname = 'postgres') || '/ch_' || relfilenode FROM pg_class WHERE relname = 't'});
	my $dir = $node->data_dir . "/$relpath";
	ok(-d $dir, "$level: the table's directory exists") if $store eq 'files';

	# a writer whose part is on disk, its catalog row not committed, at the crash
	my $bg = $node->background_psql('postgres');
	$bg->query_safe(q{BEGIN});
	$bg->query_safe(q{INSERT INTO t SELECT g, 'uncommitted' FROM generate_series(200001, 210000) g});

	$node->stop('immediate');
	$bg->{run}->kill_kill;
	$node->start;
	is($node->safe_psql('postgres', q{SELECT count(*) FROM t}), '100000', "$level: committed rows after a crash");
	is($node->safe_psql('postgres', q{SELECT count(*) FROM t WHERE b = 'uncommitted'}), '0', "$level: uncommitted rows stay invisible");
	is($node->safe_psql('postgres', q{SELECT md5(string_agg(b, ',' ORDER BY a)) FROM t}), $sum, "$level: the same rows");

	# parts committed after the last checkpoint
	$node->safe_psql('postgres', q{CHECKPOINT});
	$node->safe_psql('postgres', q{INSERT INTO t SELECT g, md5(g::text) FROM generate_series(100001, 150000) g});
	$node->safe_psql('postgres', q{INSERT INTO t SELECT g, md5(g::text) FROM generate_series(150001, 160000) g});
	my $sum2 = $node->safe_psql('postgres', q{SELECT md5(string_agg(b, ',' ORDER BY a)) FROM t});
	my @new_parts = split /\n/, $node->safe_psql('postgres', q{SELECT name FROM ch.parts('t') WHERE min_rowid > 100000 ORDER BY name});
	is(scalar(@new_parts), 2, "$level: two parts after the checkpoint");
	$node->stop('immediate');
	if ($store eq 'files' && $wal eq 'replica')
	{
		# lost, and written again by recovery from WAL
		for my $p (@new_parts)
		{
			File::Path::rmtree("$dir/$p");
			ok(!-d "$dir/$p", "$level: part $p removed before recovery");
		}
	}
	$node->start;
	if ($store eq 'files')
	{
		for my $p (@new_parts)
		{
			ok(-f "$dir/$p/checksums.txt", "$level: part $p is there after recovery");
		}
	}
	is($node->safe_psql('postgres', q{SELECT count(*) FROM t}), '160000', "$level: every committed row after recovery");
	is($node->safe_psql('postgres', q{SELECT md5(string_agg(b, ',' ORDER BY a)) FROM t}), $sum2, "$level: the same rows after recovery");

	# and the table keeps working; the dead writer's part does not get in the way
	$node->safe_psql('postgres', q{INSERT INTO t VALUES (0, 'after')});
	is($node->safe_psql('postgres', q{SELECT count(*) FROM t}), '160001', "$level: inserts after recovery");
	is($node->safe_psql('postgres', q{SELECT threads FROM ch.engine_info()}), '5', "$level: the engine library after recovery");
	$node->stop;
}
done_testing();
