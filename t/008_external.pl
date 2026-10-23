# pg_dump and restore of tables of files (ch_table_engines.md §2.17): the table
# form of COPY ... TO returns none of their rows, so that no dump holds them
# but --inserts's, whose restore fails at the access method's refused insert;
# a restored table reads its location again.  Beside 004_dump.pl.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('external');
$node->init;
$node->append_conf('postgresql.conf', qq{shared_preload_libraries = 'ch_table_engines'
ch.max_merge_workers = 0});
$node->start;
my $files = $node->basedir . '/files';
mkdir $files or die "mkdir $files: $!";
mkdir "$files/glob" or die "mkdir $files/glob: $!";

$node->safe_psql('postgres', q{CREATE DATABASE src});
$node->safe_psql('src', qq{
	CREATE EXTENSION ch_table_engines;
	CREATE TABLE h (id int8 NOT NULL, t text, d date, n numeric(10, 2));
	INSERT INTO h SELECT g, 'row ' || g, DATE '2026-01-01' + g % 400, g / 7.0 FROM generate_series(1, 5000) g;
	SELECT ch.write_file('Parquet', '$files/one.parquet', 'SELECT * FROM h ORDER BY id', NULL,
						 'output_format_parquet_row_group_size = 1000');
	SELECT ch.write_file('Parquet', '$files/glob/part_' || k || '.parquet',
						 'SELECT id, t FROM h WHERE id % 2 = ' || k - 1 || ' ORDER BY id')
	FROM generate_series(1, 2) k;
	CREATE TABLE p1 (id int8, t text, d date, n numeric(10, 2)) USING parquet
		WITH (location = '$files/one.parquet', settings = 'input_format_parquet_bloom_filter_push_down = 0');
	CREATE TABLE p2 (id int8, t text) USING parquet WITH (location = '$files/glob/part_*.parquet');
	CREATE TABLE p3 (t text, id int8) USING parquet WITH (location = '$files/glob');
});

my %q = (
	h => q{SELECT count(*), sum(id), md5(string_agg(t || d || n, ',' ORDER BY id)) FROM h},
	p1 => q{SELECT count(*), sum(id), md5(string_agg(t || d || n, ',' ORDER BY id)) FROM p1},
	p2 => q{SELECT count(*), sum(id), md5(string_agg(t, ',' ORDER BY id)) FROM p2},
	p3 => q{SELECT count(*), sum(id), md5(string_agg(t, ',' ORDER BY id)) FROM p3},
	am => q{SELECT c.relname, a.amname, c.reloptions FROM pg_class c JOIN pg_am a ON a.oid = c.relam
			WHERE c.relname IN ('h', 'p1', 'p2', 'p3') ORDER BY 1});
my %expect = map { $_ => $node->safe_psql('src', $q{$_}) } keys %q;
is((split /\|/, $expect{p1})[0], 5000, 'the table reads its file');
is((split /\|/, $expect{p2})[0], 5000, 'the table reads its glob');

# The COPY blocks of a dump's text: each table's rows
sub copy_rows
{
	my ($text, $table) = @_;
	my ($block) = $text =~ /^COPY public\.\Q$table\E [^\n]*FROM stdin;\n(.*?)^\\\.$/ms;
	return undef unless defined $block;
	my @rows = grep { length } split /\n/, $block;
	return scalar @rows;
}

sub restored_ok
{
	my ($db, $what) = @_;
	for my $k (sort keys %q)
	{
		is($node->safe_psql($db, $q{$k}), $expect{$k}, "$what: $k as before");
	}
}

# a plain dump: the tables' definitions and options, the heap's rows, none of theirs
my $plain = $node->basedir . '/plain.sql';
$node->command_ok([ 'pg_dump', '-f', $plain, '-d', $node->connstr('src') ], 'pg_dump, plain');
my $text = slurp_file($plain);
is(copy_rows($text, 'h'), 5000, 'the plain dump holds the heap table\'s rows');
is(copy_rows($text, $_), 0, "the plain dump holds no row of $_") for qw(p1 p2 p3);
like($text, qr/default_table_access_method = parquet/, 'the plain dump has the access method');
like($text, qr/location='\Q$files\E\/one\.parquet'/, 'the plain dump has the location');
$node->safe_psql('postgres', q{CREATE DATABASE dst1});
$node->command_ok([ 'psql', '-X', '-q', '-v', 'ON_ERROR_STOP=1', '-f', $plain, '-d', $node->connstr('dst1') ],
	'restore of the plain dump');
restored_ok('dst1', 'plain');

# -Fc, restored by pg_restore -j 4: TRUNCATE of a table made in the same run leaves its files
my $custom = $node->basedir . '/custom.dump';
$node->command_ok([ 'pg_dump', '-Fc', '-f', $custom, '-d', $node->connstr('src') ], 'pg_dump -Fc');
my ($out, $err) = run_command([ 'pg_restore', '-f', '-', $custom ]);
is(copy_rows($out, $_), 0, "the custom dump holds no row of $_") for qw(p1 p2 p3);
is(copy_rows($out, 'h'), 5000, 'the custom dump holds the heap table\'s rows');
$node->safe_psql('postgres', q{CREATE DATABASE dst2});
$node->command_ok([ 'pg_restore', '-j', '4', '-d', $node->connstr('dst2'), $custom ], 'pg_restore -j 4');
restored_ok('dst2', 'pg_restore -j 4');

# --data-only: no row of theirs, and its restore beside the schema reads the files
my $data = $node->basedir . '/data.sql';
$node->command_ok([ 'pg_dump', '--data-only', '-f', $data, '-d', $node->connstr('src') ], 'pg_dump --data-only');
$text = slurp_file($data);
is(copy_rows($text, $_), 0, "the data-only dump holds no row of $_") for qw(p1 p2 p3);
$node->safe_psql('postgres', q{CREATE DATABASE dst3});
$node->command_ok([ 'pg_restore', '--schema-only', '-d', $node->connstr('dst3'), $custom ], 'the schema alone');
$node->command_ok([ 'psql', '-X', '-q', '-v', 'ON_ERROR_STOP=1', '-f', $data, '-d', $node->connstr('dst3') ],
	'restore of the data-only dump');
restored_ok('dst3', 'data-only');

# --inserts reads the rows through a cursor over a SELECT: the dump holds them, its restore fails loudly
my $inserts = $node->basedir . '/inserts.sql';
$node->command_ok([ 'pg_dump', '--inserts', '-f', $inserts, '-d', $node->connstr('src') ], 'pg_dump --inserts');
$text = slurp_file($inserts);
my @p1_rows = $text =~ /^INSERT INTO public\.p1 /mg;
is(scalar @p1_rows, 5000, 'the --inserts dump holds the rows');
$node->safe_psql('postgres', q{CREATE DATABASE dst4});
ok(!IPC::Run::run([ 'psql', '-X', '-q', '-v', 'ON_ERROR_STOP=1', '-f', $inserts, '-d', $node->connstr('dst4') ],
		'>', \$out, '2>', \$err), 'restore of the --inserts dump fails');
like($err, qr/cannot insert into "p\d"/, 'at the access method\'s refused insert');

# a user's COPY t TO: its header alone, or binary's header and trailer; the query form reads the rows
my $rc;
($rc, $out, $err) = $node->psql('src', q{COPY p1 TO STDOUT WITH (FORMAT csv, HEADER)});
is($out, 'id,t,d,n', 'COPY TO with HEADER: the header alone');
like($err, qr/COPY of p1 returns no rows/, 'COPY TO says why');
my $bin = $node->basedir . '/p1.bin';
$node->psql('src', qq{COPY p1 TO '$bin' WITH (FORMAT binary)});
is(-s $bin, 21, 'COPY TO in binary: its header and trailer alone');
$node->safe_psql('src', qq{COPY (SELECT * FROM p1) TO '$bin' WITH (FORMAT binary)});
cmp_ok(-s $bin, '>', 21, 'COPY of the query form in binary: the rows');
$node->safe_psql('src', q{CREATE TABLE back (LIKE h)});
$node->safe_psql('src', qq{COPY back FROM '$bin' WITH (FORMAT binary)});
is($node->safe_psql('src', q{SELECT count(*), sum(id), md5(string_agg(t || d || n, ',' ORDER BY id)) FROM back}),
	$expect{p1}, 'the query form\'s rows, copied back into a heap table');

# a statement reads the files as listed at its start: a file it has open it reads as it was, one changed
# before it opens it fails its read; the next statement reads the files as they are
mkdir "$files/changing" or die "mkdir $files/changing: $!";
$node->safe_psql('src', qq{
	SELECT ch.write_file('Parquet', '$files/changing/' || k || '.parquet', 'SELECT id FROM h ORDER BY id')
	FROM generate_series(1, 2) k;
	CREATE TABLE ch1 (id int8) USING parquet WITH (location = '$files/changing');
});
($rc, $out, $err) = $node->psql('src', qq{
	BEGIN;
	DECLARE c CURSOR FOR SELECT id FROM ch1;
	FETCH 1 FROM c;
	SELECT ch.write_file('Parquet', '$files/changing/' || k || '.parquet', 'SELECT id FROM h WHERE id < 10 ORDER BY id') > 0
	FROM generate_series(1, 2) k;
	FETCH 4999 FROM c;
	FETCH ALL FROM c;
	COMMIT;
});
like($out, qr/^1\nt\nt\n(\d+\n){4998}5000$/s, 'a file the statement has open: read as it was');
like($err, qr/file \S+\/2\.parquet changed since the statement listed it/, 'a file changed before it opens it: an error');
is($node->safe_psql('src', q{SELECT count(*) FROM ch1}), 18, 'the next statement reads the files as they are');

$node->stop;
done_testing();
