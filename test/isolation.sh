#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# The isolation leg (ch_table_engines.md §5 CH2, §6 "isolation"), in a dev
# container: concurrent DML and row locks, each spec run by PostgreSQL's
# isolationtester twice -- its tables heap's in one database, mergetree's in
# another -- and the two outputs compared: mergetree's must be heap's.
#
#   specs/                  the extension's own specs
#   /cb/isolation/.../specs PostgreSQL's own at the server's commit, the list
#                           below, run with their PRIMARY KEY and UNIQUE
#                           constraints taken out -- unique indexes come in CH5
#                           -- in both databases alike
#
# A difference test/isolation/kept/<spec>.diff names is one reviewed and kept
# (ch_table_engines.md, "CH2's results"); any other fails the leg.
#
#   CHTE_SPECS  the specs to run, by name: all
#   CHTE_WORK   the run's directory: isolation/<spec>.{heap,mergetree,diff}
set -u
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/lib.sh"
WORK="${CHTE_WORK:-/work}"
"$here/build.sh" > "$WORK/build.log" 2>&1 || { cat "$WORK/build.log"; exit 1; }

PG_SPECS="/cb/isolation/src/test/isolation/specs"
PG_LIST="delete-abort-savept delete-abort-savept-2 lock-update-delete lock-update-traversal
	lock-committed-update lock-committed-keyupdate skip-locked skip-locked-2 skip-locked-3 skip-locked-4
	nowait nowait-2 nowait-3 nowait-4 nowait-5 tuplelock-update tuplelock-conflict
	tuplelock-upgrade-no-deadlock multiple-row-versions update-conflict-out two-ids simple-write-skew
	classroom-scheduling total-cash receipt-report project-manager read-only-anomaly
	read-only-anomaly-2 read-only-anomaly-3 predicate-hash predicate-gist predicate-gin
	prepared-transactions deadlock-simple deadlock-hard deadlock-soft deadlock-soft-2 eval-plan-qual"

out="$WORK/isolation"
mkdir -p "$out/specs"
for f in "$CHTE_SRC/specs/"*.spec; do
	cp "$f" "$out/specs/"
done
# PostgreSQL's: their keys and unique constraints out, in a table's own clause or a column's
for name in $PG_LIST; do
	[ -f "$PG_SPECS/$name.spec" ] || { echo "isolation: no spec $name"; exit 1; }
	python3 - "$PG_SPECS/$name.spec" "$out/specs/pg-$name.spec" <<'PY'
import re, sys
s = open(sys.argv[1]).read()
s = re.sub(r',\s*(CONSTRAINT\s+\w+\s+)?(PRIMARY\s+KEY|UNIQUE)\s*\([^)]*\)', '', s, flags=re.I)
s = re.sub(r'\s+(PRIMARY\s+KEY|UNIQUE)\b', '', s, flags=re.I)
open(sys.argv[2], 'w').write(s)
PY
done

D=/tmp/srv-isolation
rm -rf "$D"
server_init "$D" "shared_preload_libraries = 'ch_table_engines'" "max_connections = 60" || exit 1
# a backend that faults writes its stack to the log (module.c)
CHTE_BACKTRACE_ON_CRASH=1 server_start "$D" || { cat "$D/log"; exit 1; }
# the server's log kept however the leg ends
trap 'cp "$D/log" "$out/server.log" 2> /dev/null' EXIT
PSQL=("$BINDIR/psql" -X -q -At -v ON_ERROR_STOP=1 -h "$D/sock" -U postgres)

status=0
passed=0
kept=0
n=0
for spec in "$out/specs/"*.spec; do
	name="$(basename "$spec" .spec)"
	if [ -n "${CHTE_SPECS:-}" ] && ! [[ " $CHTE_SPECS " == *" $name "* ]]; then
		continue
	fi
	n=$((n + 1))
	# lock_timeout ends the waits that only exclusive row locks make -- a FOR
	# SHARE queued behind another's -- each the same way in every run, where
	# they would hold their spec until the leg's timeout
	for am in heap mergetree; do
		db="iso_${am}_$n"
		"${PSQL[@]}" -d postgres -c "CREATE DATABASE $db" || exit 1
		"${PSQL[@]}" -d "$db" -c "CREATE EXTENSION ch_table_engines;
			ALTER DATABASE $db SET default_table_access_method = $am;
			ALTER DATABASE $db SET lock_timeout = '15s'" || exit 1
		timeout 600 isolationtester "host=$D/sock dbname=$db user=postgres" < "$spec" > "$out/$name.$am" 2>&1
		echo "exit $?" >> "$out/$name.$am"
	done
	if diff -u "$out/$name.heap" "$out/$name.mergetree" > "$out/$name.diff"; then
		rm -f "$out/$name.diff"
		passed=$((passed + 1))
		echo "  $name: as heap's"
	elif [ -f "$here/isolation/kept/$name.diff" ] && \
		diff -q <(grep -E '^[-+][^-+]' "$here/isolation/kept/$name.diff") <(grep -E '^[-+][^-+]' "$out/$name.diff") > /dev/null; then
		kept=$((kept + 1))
		echo "  $name: the kept difference"
	else
		status=1
		echo "  $name: DIFFERS from heap's ($out/$name.diff)"
	fi
done
server_stop "$D"
if log_failures "$D"; then
	echo "isolation: the server log has failures"
	log_failures "$D" | head -20
	status=1
fi
echo "isolation: $n specs: $passed as heap's, $kept with their kept differences, $((n - passed - kept)) differ"
exit $status
