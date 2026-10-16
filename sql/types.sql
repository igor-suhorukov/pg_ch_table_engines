-- Every type round-trips (ch_table_engines.md §2.5): the same values come
-- back, checked against a heap table of the same rows.
SET client_min_messages = warning;
SET extra_float_digits = 3;
CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
CREATE TYPE pair AS (x int, y text);
CREATE DOMAIN posint AS int CHECK (VALUE > 0);

CREATE TABLE th (
	i2 int2, i4 int4, i8 int8, o oid, f4 float4, f8 float8, b bool, ch "char",
	n numeric, n92 numeric(9, 2), n182 numeric(18, 2), n380 numeric(38, 0), n3810 numeric(38, 10), n500 numeric(50, 5),
	t text, vc varchar(10), bp char(5), by bytea, nm name,
	d date, tim time, ttz timetz, ts timestamp, tstz timestamptz, iv interval,
	u uuid, js json, jb jsonb, x xml, ia int[], ta text[], ir int4range, tsr tstzrange,
	mn money, bt bit(4), vb varbit, inet inet, cidr cidr, mac macaddr, mac8 macaddr8,
	pt point, ln line, ls lseg, bx box, pth path, pg polygon, ci circle,
	tv tsvector, tq tsquery, md mood, pr pair, dm posint, rc regclass, pl pg_lsn, ar int[][]
);
INSERT INTO th VALUES
	(1, 2, 3, 4, 1.5, 2.25, true, 'a',
	 12345.678901234567890, 1234567.89, -1234567890123456.78, 12345678901234567890123456789012345678, 1234567890123456789012345678.0123456789, 123456789012345678901234567890123456789012345.12345,
	 'hello', 'varchar', 'ab', '\x00ff10', 'nm',
	 '2026-10-16', '12:34:56.789', '12:34:56+03', '2026-10-16 12:34:56.123456', '2026-10-16 12:34:56.123456+05', '1 year 2 mons 3 days 04:05:06.789',
	 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{"a": [1, 2]}', '{"b": {"c": null}}', '<a>x</a>', '{1,2,3}', '{"x","y z",NULL}', '[1,10)', '[2026-01-01,2026-02-01)',
	 '12.34', B'1010', B'110011', '192.168.0.1/24', '10.0.0.0/8', '08:00:2b:01:02:03', '08:00:2b:01:02:03:04:05',
	 '(1,2)', '{1,2,3}', '[(1,2),(3,4)]', '((1,2),(3,4))', '[(1,2),(3,4),(5,6)]', '((0,0),(1,1),(1,0))', '<(1,2),3>',
	 'a fat cat', 'fat & cat', 'happy', ROW(1, 'one'), 7, 'th', '16/B374D848', '{{1,2},{3,4}}'),
	(-32768, -2147483648, -9223372036854775808, 0, '-Infinity', 'NaN', false, '',
	 'NaN', 'NaN', -0.01, -99999999999999999999999999999999999999, 'NaN', -0.00001,
	 '', '', '', '\x', '',
	 '-infinity', '00:00', '00:00+00', '-infinity', 'infinity', '-178000000 years',
	 '00000000-0000-0000-0000-000000000000', '[]', '[]', '', '{}', '{}', 'empty', 'empty',
	 '-92233720368547758.08', B'0000', B'', '::1', '::/0', '00:00:00:00:00:00', '00:00:00:00:00:00:00:00',
	 '(-0,0)', '{1,-1,0}', '[(0,0),(0,0)]', '((0,0),(0,0))', '((0,0))', '((0,0))', '<(0,0),0>',
	 '', 'a', 'sad', ROW(NULL, NULL), 1, 'pg_class', '0/0', '{}'),
	(32767, 2147483647, 9223372036854775807, 4294967295, 'Infinity', '-0', NULL, NULL,
	 '-Infinity', 9999999.99, 9999999999999999.99, 99999999999999999999999999999999999999, 9999999999999999999999999999.9999999999, NULL,
	 repeat('x', 100000), NULL, NULL, decode(repeat('ab', 50000), 'hex'), NULL,
	 'infinity', '24:00', NULL, 'infinity', '-infinity', NULL,
	 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	 NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	 NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
	(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	 NULL, NULL, NULL, NULL, NULL, NULL,
	 'unicode ✓ ünïcödé 日本語', 'ü', 'ü', '\x00', 'ü',
	 NULL, NULL, NULL, NULL, NULL, NULL,
	 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	 NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	 NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

CREATE TABLE tm (LIKE th) USING mergetree WITH (order_by = 'i4');
INSERT INTO tm SELECT * FROM th;

-- how the columns are kept
SELECT ch.definition('tm');

-- every value comes back: no row in one that is not in the other, by text
SELECT count(*) AS only_heap FROM (SELECT th::text FROM th EXCEPT ALL SELECT tm::text FROM tm) x;
SELECT count(*) AS only_mergetree FROM (SELECT tm::text FROM tm EXCEPT ALL SELECT th::text FROM th) x;
-- and column by column, by each type's own equality where it has one
SELECT a.attname,
	   (SELECT count(*) FROM th) AS rows,
	   (SELECT count(*) FROM th, tm WHERE th.i8 IS NOT DISTINCT FROM tm.i8 AND th.t IS NOT DISTINCT FROM tm.t) AS matched
  FROM pg_attribute a WHERE a.attrelid = 'th'::regclass AND a.attnum = 1;
SELECT bool_and(th.n IS NOT DISTINCT FROM tm.n AND th.n92 IS NOT DISTINCT FROM tm.n92 AND
				th.n182 IS NOT DISTINCT FROM tm.n182 AND th.n380 IS NOT DISTINCT FROM tm.n380 AND
				th.n3810 IS NOT DISTINCT FROM tm.n3810 AND th.n500 IS NOT DISTINCT FROM tm.n500) AS numerics,
	   bool_and(th.f4::text IS NOT DISTINCT FROM tm.f4::text AND th.f8::text IS NOT DISTINCT FROM tm.f8::text) AS floats,
	   bool_and(th.by IS NOT DISTINCT FROM tm.by AND th.t IS NOT DISTINCT FROM tm.t) AS bytes,
	   bool_and(th.d IS NOT DISTINCT FROM tm.d AND th.ts IS NOT DISTINCT FROM tm.ts AND th.tstz IS NOT DISTINCT FROM tm.tstz) AS times
  FROM th JOIN tm ON th.t IS NOT DISTINCT FROM tm.t;
-- numeric's NaN in a Decimal column, and its extremes
SELECT n92, n182, n380 FROM tm ORDER BY i4 NULLS LAST;
SELECT length(t), length(by) FROM tm WHERE i4 = 2147483647;
-- infinity has no place in a numeric(p, s)
INSERT INTO tm (n92) VALUES ('Infinity');

DROP TABLE th, tm;
DROP TYPE pair;
DROP TYPE mood;
DROP DOMAIN posint;
