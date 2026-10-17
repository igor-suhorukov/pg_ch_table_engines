# Merges by the merge workers (ch_table_engines.md §2.11, CH3): a stream of
# inserts, deletions and updates beside them answers as heap does throughout,
# in both stores, through the plain scan and an index; the workers run on the
# primary, never on a standby, which follows the merges.  A standby's query
# whose snapshot predates a merge, the VACUUM of ch.parts that frees the
# inputs' storage, and the reuse of their pages answers as before or is
# cancelled by the conflict with recovery, never wrongly; with
# hot_standby_feedback it answers as before.  After promotion the new primary
# merges; DROP DATABASE goes through with a merge worker connected.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('merge_primary');
$primary->init(allows_streaming => 1);
$primary->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'ch_table_engines'
ch.max_merge_workers = 4
ch.merge_naptime = 1
autovacuum_naptime = 1
});
$primary->start;
$primary->safe_psql('postgres', 'CREATE EXTENSION ch_table_engines');
$primary->backup('base');

my $standby = PostgreSQL::Test::Cluster->new('merge_standby');
$standby->init_from_backup($primary, 'base', has_streaming => 1);
$standby->append_conf('postgresql.conf', qq{
max_standby_streaming_delay = 2s
hot_standby_feedback = off
});
$standby->start;

my $workers = q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'ch_table_engines merge worker'
	AND datname = current_database()};
ok($primary->poll_query_until('postgres', "$workers", '1'), 'a merge worker for the database on the primary');
is($standby->safe_psql('postgres', q{SELECT count(*) FROM pg_stat_activity WHERE backend_type LIKE 'ch_table_engines merge%'}),
   '0', 'and none on the standby');

# the stream, the workers merging beside it
my @stores = ('pages', 'files');
for my $store (@stores)
{
	$primary->safe_psql('postgres', qq{
		CREATE TABLE h_$store (a int, b text, c int);
		CREATE TABLE m_$store (a int, b text, c int) USING mergetree WITH (order_by = 'a', storage = '$store');
		CREATE INDEX m_${store}_c ON m_$store (c);
	});
}
my $answer = q{SELECT count(*), sum(a), sum(c), md5(string_agg(b, ',' ORDER BY a, b)) FROM %s};
my $through_index = q{SET enable_seqscan = off; SET ch.enable_chscan = off; SET enable_bitmapscan = off;
	SELECT count(*), sum(a), md5(string_agg(b, ',' ORDER BY a, b)) FROM %s WHERE c = 7};
my %differ = map { $_ => 0 } @stores;
my %differ_index = map { $_ => 0 } @stores;
for my $round (1 .. 40)
{
	for my $store (@stores)
	{
		my $lo = $round * 1000;
		my ($d, $u) = ($round % 17, $round % 23);
		$primary->safe_psql('postgres', qq{
			INSERT INTO h_$store SELECT g, md5(g::text), g % 10 FROM generate_series($lo, $lo + 999) g;
			INSERT INTO m_$store SELECT g, md5(g::text), g % 10 FROM generate_series($lo, $lo + 999) g;
			DELETE FROM h_$store WHERE a % 17 = $d;
			DELETE FROM m_$store WHERE a % 17 = $d;
			UPDATE h_$store SET b = 'r$round', c = c % 10 WHERE a % 23 = $u;
			UPDATE m_$store SET b = 'r$round', c = c % 10 WHERE a % 23 = $u;
		});
		$differ{$store}++ if $primary->safe_psql('postgres', sprintf($answer, "h_$store"))
			ne $primary->safe_psql('postgres', sprintf($answer, "m_$store"));
		$differ_index{$store}++ if $primary->safe_psql('postgres', sprintf($through_index, "h_$store"))
			ne $primary->safe_psql('postgres', sprintf($through_index, "m_$store"));
	}
}
for my $store (@stores)
{
	is($differ{$store}, 0, "m_$store: every answer beside the merges heap's");
	is($differ_index{$store}, 0, "m_$store: every answer through the index heap's");
	# 80 statements wrote parts, inserts and updates: the workers merged most of them
	ok($primary->poll_query_until('postgres', qq{SELECT count(*) <= 12 FROM ch.parts('m_$store')}),
	   "m_$store: the workers merged its parts");
	is($primary->safe_psql('postgres', sprintf($answer, "m_$store")), $primary->safe_psql('postgres', sprintf($answer, "h_$store")),
	   "m_$store: heap's answer after the merges");
}
is($primary->safe_psql('postgres', q{SELECT max(level) > 0 FROM ch.parts('m_pages')}), 't', 'merged parts');

# the standby follows the merges
$primary->wait_for_catchup($standby);
for my $store (@stores)
{
	is($standby->safe_psql('postgres', sprintf($answer, "m_$store")), $primary->safe_psql('postgres', sprintf($answer, "m_$store")),
	   "m_$store: the standby answers as the primary");
}

# a standby's snapshot older than a merge, the pruning of its inputs' rows,
# and the reuse of their pages: its query answers as before, or is cancelled
sub old_snapshot
{
	my ($feedback) = @_;
	my $reader = $standby->background_psql('postgres', on_error_stop => 0);
	$reader->query_safe(q{BEGIN ISOLATION LEVEL REPEATABLE READ});
	my $before = $reader->query_safe(sprintf($answer, 'm_pages'));
	$primary->safe_psql('postgres', q{SELECT ch.optimize('m_pages', true)});
	my $merged = $primary->safe_psql('postgres', q{SELECT pg_relation_size('m_pages')});
	$primary->safe_psql('postgres', q{VACUUM ch.parts});
	$primary->safe_psql('postgres', q{INSERT INTO m_pages SELECT g, md5(g::text), 0 FROM generate_series(1000000, 1050000) g;
		INSERT INTO h_pages SELECT g, md5(g::text), 0 FROM generate_series(1000000, 1050000) g});
	my $inserted = $primary->safe_psql('postgres', q{SELECT pg_relation_size('m_pages')});
	$primary->wait_for_catchup($standby);
	my $after = eval { $reader->query(sprintf($answer, 'm_pages')) };
	my $stderr = $reader->{stderr} // '';
	eval { $reader->quit };
	note("feedback $feedback: m_pages $merged bytes after the merge, $inserted after the inserts; the standby's snapshot conflicts: "
		 . $standby->safe_psql('postgres', q{SELECT confl_snapshot FROM pg_stat_database_conflicts WHERE datname = 'postgres'}));
	return ($before, $after, $stderr, $merged, $inserted);
}
my ($before, $after, $stderr) = old_snapshot(0);
ok((defined $after && $after eq $before) || $stderr =~ /conflict with recovery/,
   'without feedback: the old snapshot answers as before, or is cancelled by the conflict (' . ($stderr =~ /conflict/ ? 'cancelled' : 'answered') . ')');
my $conflicts = q{SELECT confl_snapshot FROM pg_stat_database_conflicts WHERE datname = 'postgres'};
my $conflicted = $standby->safe_psql('postgres', $conflicts);
$standby->append_conf('postgresql.conf', q{hot_standby_feedback = on});
$standby->reload;
$standby->poll_query_until('postgres', q{SELECT setting = 'on' FROM pg_settings WHERE name = 'hot_standby_feedback'});
ok($primary->poll_query_until('postgres', q{SELECT backend_xmin IS NOT NULL FROM pg_stat_replication}), 'the standby\'s feedback reaches the primary');
my ($merged, $inserted);
($before, $after, $stderr, $merged, $inserted) = old_snapshot(1);
is($after, $before, 'with feedback: the old snapshot answers as before');
is($standby->safe_psql('postgres', $conflicts), $conflicted, 'with no conflict with recovery');
is($standby->safe_psql('postgres', sprintf($answer, 'm_pages')), $primary->safe_psql('postgres', sprintf($answer, 'h_pages')),
   'and a new one as the primary');

# after promotion the new primary's workers merge
$standby->promote;
$standby->safe_psql('postgres', q{SELECT 1});
ok($standby->poll_query_until('postgres', "$workers", '1'), 'a merge worker on the promoted standby');
my $parts = $standby->safe_psql('postgres', q{SELECT count(*) FROM ch.parts('m_files')});
for my $i (1 .. 8)
{
	$standby->safe_psql('postgres', qq{INSERT INTO m_files SELECT g, md5(g::text), 1 FROM generate_series(2000000 + $i * 100, 2000000 + $i * 100 + 99) g});
}
ok($standby->poll_query_until('postgres', qq{SELECT count(*) < $parts + 8 FROM ch.parts('m_files')}), 'whose workers merge');

# DROP DATABASE with a merge worker in the database
$primary->safe_psql('postgres', 'CREATE DATABASE chte_drop');
$primary->safe_psql('chte_drop', q{CREATE EXTENSION ch_table_engines; CREATE TABLE d (a int) USING mergetree; INSERT INTO d VALUES (1)});
ok($primary->poll_query_until('chte_drop', "$workers", '1'), 'a merge worker in the database to drop');
$primary->safe_psql('postgres', 'DROP DATABASE chte_drop');
is($primary->safe_psql('postgres', q{SELECT count(*) FROM pg_database WHERE datname = 'chte_drop'}), '0', 'DROP DATABASE went through');

# the threads: a merge worker's process as any backend's (ch.engine_info() in a session)
is($primary->safe_psql('postgres', q{SELECT ch.optimize('m_files', true) >= 0; SELECT threads FROM ch.engine_info()}), "t\n5",
   'five threads after a merge in a session');

$standby->stop;
$primary->stop;
done_testing();
