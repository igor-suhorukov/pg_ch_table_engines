# DELETE, UPDATE, row locks, indexes and folding (ch_table_engines.md §2.10)
# under WAL: a streaming standby answers as the primary, through the plain
# scan and through an index, in both stores; after a crash the primary
# answers as before it; the files store's old part version goes with VACUUM's
# sweep on both servers.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('dml_primary');
$primary->init(allows_streaming => 1);
$primary->append_conf('postgresql.conf', qq{shared_preload_libraries = 'ch_table_engines'
ch.max_merge_workers = 0});
$primary->start;
$primary->safe_psql('postgres', 'CREATE EXTENSION ch_table_engines');
$primary->backup('base');

my $standby = PostgreSQL::Test::Cluster->new('dml_standby');
$standby->init_from_backup($primary, 'base', has_streaming => 1);
$standby->start;

my %answer;
for my $store ('files', 'pages')
{
	my $t = "d_$store";
	$primary->safe_psql('postgres', qq{
		CREATE TABLE $t (a int, b text, c int) USING mergetree WITH (order_by = 'a', storage = '$store');
		INSERT INTO $t SELECT g, md5(g::text), g % 100 FROM generate_series(1, 30000) g;
		CREATE INDEX ${t}_c ON $t (c);
		DELETE FROM $t WHERE a % 7 = 0;
		UPDATE $t SET b = 'updated', c = c + 1000 WHERE a % 11 = 0;
		BEGIN; SELECT a FROM $t WHERE a < 10 FOR UPDATE; COMMIT;
	});
	$answer{$t} = qq{SELECT count(*), sum(a), sum(c), md5(string_agg(b, ',' ORDER BY a)) FROM $t};
	$answer{"${t}_index"} = qq{SET enable_seqscan = off; SET ch.enable_chscan = off;
		SELECT count(*), sum(a), md5(string_agg(b, ',' ORDER BY a)) FROM $t WHERE c BETWEEN 1000 AND 1050};
}
$primary->wait_for_catchup($standby);
for my $q (sort keys %answer)
{
	is($standby->safe_psql('postgres', $answer{$q}), $primary->safe_psql('postgres', $answer{$q}),
		"$q: the standby answers as the primary after deletions and updates");
}

# the deletions folded into the parts: the answers the same, on both servers
my %before = map { $_ => $primary->safe_psql('postgres', $answer{$_}) } keys %answer;
for my $store ('files', 'pages')
{
	my $folded = $primary->safe_psql('postgres', qq{SELECT ch.fold('d_$store')});
	ok($folded > 0, "d_$store: deletions folded ($folded rows)");
}
$primary->wait_for_catchup($standby);
for my $q (sort keys %answer)
{
	is($primary->safe_psql('postgres', $answer{$q}), $before{$q}, "$q: the primary answers as before the fold");
	is($standby->safe_psql('postgres', $answer{$q}), $before{$q}, "$q: the standby answers as before the fold");
}
is($standby->safe_psql('postgres', q{SELECT count(*) FROM ch.tombstones}), '0', 'the folded tombstones are gone on the standby');

# the files store's old part version, which no snapshot sees: once VACUUM of
# ch.parts has pruned its row, VACUUM's sweep removes it on both servers
my $db = $primary->safe_psql('postgres', q{SELECT oid FROM pg_database WHERE datname = 'postgres'});
my $node = $primary->safe_psql('postgres', q{SELECT relfilenode FROM pg_class WHERE relname = 'd_files'});
$primary->safe_psql('postgres', q{VACUUM ch.parts});
$primary->safe_psql('postgres', q{VACUUM d_files});
$primary->wait_for_catchup($standby);
for my $server ($primary, $standby)
{
	opendir(my $dh, $server->data_dir . "/base/$db/ch_$node") or die "no directory of d_files";
	my @parts = sort grep { /^all_/ } readdir($dh);
	closedir($dh);
	is(scalar(@parts), 2, $server->name . ': the folded part and the update\'s new rows, the old version gone (' . join(' ', @parts) . ')');
}

# a crash after deletions, updates and a fold that did not commit: the answers as before
$primary->safe_psql('postgres', q{
	DELETE FROM d_files WHERE a BETWEEN 100 AND 200;
	UPDATE d_pages SET b = 'again' WHERE a BETWEEN 300 AND 400;
});
my %committed = map { $_ => $primary->safe_psql('postgres', $answer{$_}) } keys %answer;
my $bg = $primary->background_psql('postgres');
$bg->query_safe(q{BEGIN});
$bg->query_safe(q{DELETE FROM d_files WHERE a < 50});
$bg->query_safe(q{SELECT ch.fold('d_pages')});
$primary->stop('immediate');
eval { $bg->quit };
$primary->start;
for my $q (sort keys %answer)
{
	is($primary->safe_psql('postgres', $answer{$q}), $committed{$q}, "$q: after a crash, the committed answers");
}

$standby->stop;
$primary->stop;
done_testing();
