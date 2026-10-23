#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# ClickBench's Parquet files as tables of the parquet access method (ch_table_engines.md
# §5 CH7), beside the hits leg's MergeTree loads, in a dev container: hits.parquet, and its
# 100 files of a million rows each (ClickBench's lib/download-hits-parquet-single and
# -partitioned, into parquet/ and parquet/partitioned/), their columns as
# ch.external_columns() gives them; each table's rows counted, its columns' checksums
# compared with the other's, and ClickBench's queries over the two compared -- in parallel
# and not -- nothing timed (rule 7).  The data is mounted at /data.
#
#   CHTE_WORK        the run's directory
set -u
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/lib.sh"
WORK="${CHTE_WORK:-/work}"
ONE=/data/parquet/hits.parquet
MANY='/data/parquet/partitioned/hits_*.parquet'
[ -f "$ONE" ] || { echo "no $ONE"; exit 1; }
"$here/build.sh" > "$WORK/build.log" 2>&1 || { cat "$WORK/build.log"; exit 1; }

D=/tmp/hits-parquet
server_init "$D" "shared_preload_libraries = 'ch_table_engines'" "shared_buffers = 1GB" "work_mem = 256MB" \
	"max_parallel_workers_per_gather = 4" || exit 1
server_start "$D" || { cat "$D/log"; exit 1; }
PSQL=("$BINDIR/psql" -X -q -At -v ON_ERROR_STOP=1 -h "$D/sock" -U postgres)
fail() { echo "hits-parquet: $*"; server_stop "$D"; exit 1; }

"${PSQL[@]}" -d postgres -c "CREATE DATABASE hits" || fail "CREATE DATABASE"
"${PSQL[@]}" -d hits -c "CREATE EXTENSION ch_table_engines" || fail "CREATE EXTENSION"

# the files' columns, named as ClickBench's queries name them, lower-case: each matches the files' own
# regardless of case
cols=$("${PSQL[@]}" -d hits -c "SELECT lower(ch.external_columns('parquet', '$ONE'))") || fail "the file's columns"
printf '%s\n' "$cols" > "$WORK/columns.sql"
echo "hits-parquet: $(($(grep -c ',' "$WORK/columns.sql") + 1)) columns"
"${PSQL[@]}" -d hits -c "CREATE TABLE hits_one ($cols) USING parquet WITH (location = '$ONE')" || fail "the table of hits.parquet"
"${PSQL[@]}" -d hits -c "CREATE TABLE hits_many ($cols) USING parquet WITH (location = '$MANY')" || fail "the table of the 100 files"
for t in hits_one hits_many; do
	"${PSQL[@]}" -d hits -c "SELECT count(*), sum(rows), sum(units) FROM ch.external_files('$t')" |
		sed "s/^/hits-parquet: $t: files, rows, row groups: /"
done

# per-column checksums, order-independent, as the hits leg's: every row of every column read and checked
# (a NOT NULL column's NULL, a text column's bytes not UTF-8)
agg=$("${PSQL[@]}" -d hits -c "
	SELECT string_agg(
		CASE WHEN data_type IN ('smallint', 'integer', 'bigint')
			 THEN format('sum(%I::numeric)', column_name)
			 ELSE format('sum(hashtextextended(%I::text, 0)::numeric)', column_name)
		END, ', ' ORDER BY ordinal_position)
	  FROM information_schema.columns WHERE table_name = 'hits_one'")
status=0
for t in hits_one hits_many; do
	"${PSQL[@]}" -d hits -c "SELECT count(*), $agg FROM $t" > "$WORK/checksums.$t" || fail "checksums of $t"
done
if cmp -s "$WORK/checksums.hits_one" "$WORK/checksums.hits_many"; then
	echo "hits-parquet: count(*) and every column's checksum agree: $(cut -d'|' -f1 "$WORK/checksums.hits_one") rows"
else
	echo "hits-parquet: the checksums differ"
	diff <(tr '|' '\n' < "$WORK/checksums.hits_one") <(tr '|' '\n' < "$WORK/checksums.hits_many") | head -20
	status=1
fi

# queries of ClickBench's kinds over the two, in parallel (the server's max_parallel_workers_per_gather),
# their answers compared; the row groups a filter on the sorted CounterID prunes, from its plan
READS=(
	"count|SELECT count(*) FROM %t"
	"one column|SELECT sum(advengineid), count(*), avg(resolutionwidth) FROM %t"
	"key prefix|SELECT count(*), sum(resolutionwidth) FROM %t WHERE counterid = 62"
	"grouping|SELECT regionid, count(*) AS c, sum(isrefresh) FROM %t GROUP BY regionid ORDER BY c DESC, regionid LIMIT 10"
	"min and max|SELECT min(eventdate), max(eventdate), min(eventtime), max(eventtime) FROM %t"
	"a wide column|SELECT count(*) FROM %t WHERE url LIKE '%google%'"
	"search phrases|SELECT searchphrase, count(*) AS c FROM %t WHERE searchphrase <> '' GROUP BY searchphrase ORDER BY c DESC, searchphrase LIMIT 10"
)
: > "$WORK/reads.plans"
for r in "${READS[@]}"; do
	name="${r%%|*}"
	q="${r#*|}"
	ref=""
	for t in hits_one hits_many; do
		sql="${q//%t/$t}"
		got=$("${PSQL[@]}" -d hits -c "$sql") || fail "$name on $t"
		[ "$name" = "key prefix" ] &&
			{ echo "== $name, $t"; "${PSQL[@]}" -d hits -c "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) $sql"; } >> "$WORK/reads.plans"
		if [ "$t" = hits_one ]; then
			ref="$got"
		elif [ "$got" = "$ref" ]; then
			echo "hits-parquet: $name: the same answer"
		else
			echo "hits-parquet: $name on $t answers $got, hits_one $ref"
			status=1
		fi
	done
done
grep -A12 '== key prefix' "$WORK/reads.plans" | grep -E '^==|Row Groups' | sed 's/^/hits-parquet: /'

server_stop "$D"
exit "$status"
