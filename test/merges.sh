#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# Merges beside a load in small batches (ch_table_engines.md §5 CH3, §6
# "merges"), in a dev container: ClickBench's 10M rows into heap by COPY,
# then into a mergetree table of each store in batches -- each an INSERT ...
# SELECT of a range of heap's TIDs, about 11,000 rows -- while the merge
# workers merge beside the load (ch.max_merge_workers 2, ch.merge_naptime
# 1 s).  Recorded for each store: the parts every 50 batches; the load's time,
# and the time until the merges settle (no change of the parts for 10 s);
# the WAL written meanwhile -- every page and file the inserts and the merges
# wrote -- against the table's bytes at the end, as the write amplification;
# and every column's checksum, which must be heap's.  The data is mounted at
# /data (hits_10m.tsv).
#
#   CHTE_HITS_ROWS     the file: hits_10m.tsv (hits_1m.tsv for a quick run)
#   CHTE_BATCH_PAGES   heap's pages a batch: 1000
#   CHTE_WORK          the run's directory
set -u
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/lib.sh"
WORK="${CHTE_WORK:-/work}"
TSV="/data/${CHTE_HITS_ROWS:-hits_10m.tsv}"
STEP="${CHTE_BATCH_PAGES:-1000}"
[ -f "$TSV" ] || { echo "no $TSV"; exit 1; }
"$here/build.sh" > "$WORK/build.log" 2>&1 || { cat "$WORK/build.log"; exit 1; }

D=/tmp/merges
server_init "$D" "shared_preload_libraries = 'ch_table_engines'" "shared_buffers = 4GB" \
	"max_wal_size = 4GB" "maintenance_work_mem = 1GB" "work_mem = 256MB" \
	"ch.max_merge_workers = 2" "ch.merge_naptime = 1" || exit 1
server_start "$D" || { cat "$D/log"; exit 1; }
# the server's log kept however the leg ends
trap 'cp "$D/log" "$WORK/server.log" 2> /dev/null' EXIT
PSQL=("$BINDIR/psql" -X -q -At -v ON_ERROR_STOP=1 -h "$D/sock" -U postgres)
now_ms() { date +%s%3N; }
fail() { echo "merges: $*"; server_stop "$D"; exit 1; }
q() { "${PSQL[@]}" -d hits -c "$1"; }

"${PSQL[@]}" -d postgres -c "CREATE DATABASE hits" || fail "CREATE DATABASE"
q "CREATE EXTENSION ch_table_engines" || fail "CREATE EXTENSION"
PK="CounterID, EventDate, UserID, EventTime, WatchID"
cols=$(sed -n '/^(/,/^)/p' /src/test/hits-columns.sql)
q "CREATE TABLE hits_heap $cols" || fail "heap table"
s=$(now_ms)
"${PSQL[@]}" -d hits -c "\\copy hits_heap FROM '$TSV'" || fail "COPY into hits_heap"
echo "merges: heap loaded by COPY in $(( $(now_ms) - s )) ms"
pages=$(q "SELECT pg_relation_size('hits_heap') / current_setting('block_size')::int")

agg=$(q "SELECT string_agg(
		CASE WHEN data_type IN ('smallint', 'integer', 'bigint') THEN format('sum(%I::numeric)', column_name)
			 WHEN data_type = 'date' THEN format('sum(%I - date ''2000-01-01'')', column_name)
			 WHEN data_type LIKE 'timestamp%' THEN format('sum(extract(epoch FROM %I)::numeric)', column_name)
			 ELSE format('sum(hashtextextended(%I::text, 0)::numeric)', column_name)
		END, ', ' ORDER BY ordinal_position)
	  FROM information_schema.columns WHERE table_name = 'hits_heap'")
q "SELECT count(*), $agg FROM hits_heap" > "$WORK/checksums.hits_heap" || fail "heap's checksums"

status=0
: > "$WORK/parts.tsv"
: > "$WORK/summary.tsv"
for store in pages files; do
	t="hits_$store"
	q "CREATE TABLE $t $cols USING mergetree WITH (order_by = '$PK', storage = '$store')" || fail "$t"
	lsn0=$(q "SELECT pg_current_wal_insert_lsn()")
	s=$(now_ms)
	b=0
	batch=0
	most=0
	while [ "$b" -lt "$pages" ]; do
		q "INSERT INTO $t SELECT * FROM hits_heap WHERE ctid >= '($b,0)' AND ctid < '($((b + STEP)),0)'" ||
			fail "batch $batch into $t"
		b=$((b + STEP))
		batch=$((batch + 1))
		if [ $((batch % 50)) = 0 ]; then
			n=$(q "SELECT count(*) FROM ch.parts('$t')")
			[ "$n" -gt "$most" ] && most=$n
			printf '%s\t%d\t%d\t%s\n' "$t" "$batch" $(( $(now_ms) - s )) "$n" | tee -a "$WORK/parts.tsv"
		fi
	done
	loaded=$(( $(now_ms) - s ))
	# the merges settle: the parts unchanged for 10 s, 10 minutes at most
	prev=-1
	stable=0
	for _ in $(seq 1 600); do
		n=$(q "SELECT count(*) || ',' || max(level) FROM ch.parts('$t')")
		if [ "$n" = "$prev" ]; then stable=$((stable + 1)); else stable=0; fi
		prev=$n
		[ "$stable" -ge 10 ] && break
		sleep 1
	done
	settled=$(( $(now_ms) - s ))
	lsn1=$(q "SELECT pg_current_wal_insert_lsn()")
	wal=$(q "SELECT pg_wal_lsn_diff('$lsn1', '$lsn0')::bigint")
	# the parts' bytes in both stores; the pages store's relation besides, its freed pages in it
	size=$(q "SELECT ch.table_size('$t')")
	relation=$(q "SELECT pg_relation_size('$t')")
	final=$(q "SELECT count(*) || ' parts, level ' || max(level) || ', ' || sum(rows) || ' rows' FROM ch.parts('$t')")
	printf '%s\tbatches %d\tloaded %d ms\tsettled %d ms\tmost parts %d\tat the end %s\tWAL %s\tparts %s bytes\trelation %s bytes\twrite amplification %s\n' \
		"$t" "$batch" "$loaded" "$settled" "$most" "$final" "$wal" "$size" "$relation" \
		"$(awk -v w="$wal" -v z="$size" 'BEGIN { printf "%.2f", w / z }')" | tee -a "$WORK/summary.tsv"
	q "SELECT count(*), $agg FROM $t" > "$WORK/checksums.$t" || fail "checksums of $t"
	if cmp -s "$WORK/checksums.hits_heap" "$WORK/checksums.$t"; then
		echo "merges: $t: count(*) and every column's checksum agree with heap's"
	else
		echo "merges: $t: the checksums differ from heap's"
		status=1
	fi
	q "DROP TABLE $t" || fail "DROP $t"
done
q "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'ch_table_engines merge worker'" > "$WORK/workers.txt"

server_stop "$D"
if log_failures "$D"; then echo "merges: the server log has failures"; status=1; fi
exit $status
