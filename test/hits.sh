#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# 10M ClickBench rows by COPY into heap and into mergetree in both stores
# (ch_table_engines.md §5 CH0, §6 "hits"), in a dev container: count(*),
# per-column checksums and pg_stats compared, the loads timed.  The data is mounted at /data
# (hits_10m.tsv); ClickBench's table, from its PostgreSQL entry, with its
# primary key as the mergetree's order_by (§4).
#
#   CHTE_HITS_ROWS   the file: hits_10m.tsv (hits_1m.tsv for a quick run)
#   CHTE_WORK        the run's directory
set -u
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/lib.sh"
WORK="${CHTE_WORK:-/work}"
TSV="/data/${CHTE_HITS_ROWS:-hits_10m.tsv}"
[ -f "$TSV" ] || { echo "no $TSV"; exit 1; }
"$here/build.sh" > "$WORK/build.log" 2>&1 || { cat "$WORK/build.log"; exit 1; }

D=/tmp/hits
server_init "$D" "shared_preload_libraries = 'ch_table_engines'" "shared_buffers = 4GB" \
	"max_wal_size = 64GB" "maintenance_work_mem = 1GB" "work_mem = 256MB" || exit 1
server_start "$D" || { cat "$D/log"; exit 1; }
PSQL=("$BINDIR/psql" -X -q -At -v ON_ERROR_STOP=1 -h "$D/sock" -U postgres)
now_ms() { date +%s%3N; }
fail() { echo "hits: $*"; server_stop "$D"; exit 1; }

"${PSQL[@]}" -d postgres -c "CREATE DATABASE hits" || fail "CREATE DATABASE"
"${PSQL[@]}" -d hits -c "CREATE EXTENSION ch_table_engines" || fail "CREATE EXTENSION"

PK="CounterID, EventDate, UserID, EventTime, WatchID"
# ClickBench's columns, without its PRIMARY KEY (an index, which comes in CH2)
cols=$(sed -n '/^(/,/^)/p' /src/test/hits-columns.sql)
"${PSQL[@]}" -d hits -c "CREATE TABLE hits_heap $cols" || fail "heap table"
"${PSQL[@]}" -d hits -c "CREATE TABLE hits_mt $cols USING mergetree WITH (order_by = '$PK', storage = 'files')" || fail "mergetree table"
"${PSQL[@]}" -d hits -c "CREATE TABLE hits_mtp $cols USING mergetree WITH (order_by = '$PK', storage = 'pages')" || fail "mergetree pages table"

: > "$WORK/load.tsv"
for t in hits_heap hits_mt hits_mtp; do
	s=$(now_ms)
	"${PSQL[@]}" -d hits -c "BEGIN" -c "TRUNCATE $t" -c "\\copy $t FROM '$TSV'" -c "COMMIT" || fail "COPY into $t"
	e=$(now_ms)
	printf '%s\tcopy\t%d\n' "$t" $((e - s)) | tee -a "$WORK/load.tsv"
	s=$(now_ms)
	"${PSQL[@]}" -d hits -c "ANALYZE $t" || fail "ANALYZE $t"
	e=$(now_ms)
	printf '%s\tanalyze\t%d\n' "$t" $((e - s)) | tee -a "$WORK/load.tsv"
done
"${PSQL[@]}" -d hits -c "SELECT 'heap', pg_table_size('hits_heap') UNION ALL SELECT 'mergetree files', ch.table_size('hits_mt') UNION ALL SELECT 'mergetree pages', pg_relation_size('hits_mtp')" | tee "$WORK/sizes.txt"
"${PSQL[@]}" -d hits -c "SELECT count(*), sum(rows), min(rows), max(rows) FROM ch.parts('hits_mt')" | tee "$WORK/parts.txt"

# per-column checksums, order-independent: sums of the numbers, hashes of the rest
agg=$("${PSQL[@]}" -d hits -c "
	SELECT string_agg(
		CASE WHEN data_type IN ('smallint', 'integer', 'bigint')
			 THEN format('sum(%I::numeric)', column_name)
			 WHEN data_type = 'date'
			 THEN format('sum(%I - date ''2000-01-01'')', column_name)
			 WHEN data_type LIKE 'timestamp%'
			 THEN format('sum(extract(epoch FROM %I)::numeric)', column_name)
			 ELSE format('sum(hashtextextended(%I::text, 0)::numeric)', column_name)
		END, ', ' ORDER BY ordinal_position)
	  FROM information_schema.columns WHERE table_name = 'hits_heap'")
status=0
for t in hits_heap hits_mt hits_mtp; do
	s=$(now_ms)
	"${PSQL[@]}" -d hits -c "SELECT count(*), $agg FROM $t" > "$WORK/checksums.$t" || fail "checksums of $t"
	e=$(now_ms)
	printf '%s\tchecksums\t%d\n' "$t" $((e - s)) | tee -a "$WORK/load.tsv"
done
for t in hits_mt hits_mtp; do
	if cmp -s "$WORK/checksums.hits_heap" "$WORK/checksums.$t"; then
		echo "hits: $t: count(*) and every column's checksum agree with heap's: $(cut -d'|' -f1 "$WORK/checksums.hits_heap") rows"
	else
		echo "hits: $t: the checksums differ from heap's"
		diff <(tr '|' '\n' < "$WORK/checksums.hits_heap") <(tr '|' '\n' < "$WORK/checksums.$t") | head -20
		status=1
	fi
done

# pg_stats of the two, side by side: the difference is recorded, not failed on (§5 CH0)
"${PSQL[@]}" -d hits -c "
	SELECT h.attname, h.null_frac, m.null_frac, h.n_distinct, m.n_distinct, round(h.correlation::numeric, 3), round(m.correlation::numeric, 3)
	  FROM pg_stats h JOIN pg_stats m ON m.attname = h.attname AND m.tablename = 'hits_mt'
	 WHERE h.tablename = 'hits_heap' ORDER BY h.attname" > "$WORK/pg_stats.txt"
"${PSQL[@]}" -d hits -c "
	SELECT count(*) AS columns,
		   count(*) FILTER (WHERE abs(h.null_frac - m.null_frac) > 0.01) AS null_frac_differs,
		   count(*) FILTER (WHERE abs(h.n_distinct - m.n_distinct) > greatest(0.2 * abs(h.n_distinct), 10)) AS n_distinct_differs_20pct
	  FROM pg_stats h JOIN pg_stats m ON m.attname = h.attname AND m.tablename = 'hits_mt'
	 WHERE h.tablename = 'hits_heap'" | tee "$WORK/pg_stats.summary"

server_stop "$D"
if log_failures "$D"; then echo "hits: the server log has failures"; status=1; fi
cp "$D/log" "$WORK/server.log"
exit $status
