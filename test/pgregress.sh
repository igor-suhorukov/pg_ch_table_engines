#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# PostgreSQL's own regression suite with default_table_access_method =
# mergetree (ch_table_engines.md §5 CH0, §6 "pgregress"), in a dev container:
# the suite and its libraries at the server's commit (/cb/pgregress), against
# a server that preloads the library, in a database with the extension.  Its
# differences are kept for review; a crash, an assertion or a PANIC fails the
# leg.  A lock timeout keeps what an unsupported statement leaves behind -- a
# prepared transaction its test expected to fail -- from hanging the leg.
#
#   CHTE_WORK   the run's directory: pgregress/ (results, regression.diffs),
#               summary.txt, server.log
set -u
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/lib.sh"
WORK="${CHTE_WORK:-/work}"
"$here/build.sh" > "$WORK/build.log" 2>&1 || { cat "$WORK/build.log"; exit 1; }
R=/cb/pgregress/src/test/regress
PG_REGRESS="$(dirname "$("$BINDIR/pg_config" --pgxs)")/../test/regress/pg_regress"

D=/tmp/pgr
server_init "$D" "shared_preload_libraries = 'ch_table_engines'" "max_connections = 120" || exit 1
server_start "$D" || { cat "$D/log"; exit 1; }
PSQL=("$BINDIR/psql" -X -q -At -v ON_ERROR_STOP=1 -h "$D/sock" -U postgres)

# the database pg_regress would make, with the extension in it (pg_regress.c, create_database())
"${PSQL[@]}" -d postgres -c "CREATE DATABASE regression TEMPLATE template0" || exit 1
"${PSQL[@]}" -d regression -c "
	ALTER DATABASE regression SET lc_messages TO 'C';
	ALTER DATABASE regression SET lc_monetary TO 'C';
	ALTER DATABASE regression SET lc_numeric TO 'C';
	ALTER DATABASE regression SET lc_time TO 'C';
	ALTER DATABASE regression SET bytea_output TO 'hex';
	ALTER DATABASE regression SET timezone_abbreviations TO 'Default';
	CREATE EXTENSION ch_table_engines;" || exit 1

out="$WORK/pgregress"
mkdir -p "$out"
cd "$out" || exit 1
PGOPTIONS="-c default_table_access_method=mergetree -c lock_timeout=60s" "$PG_REGRESS" --use-existing \
	--inputdir="$R" --outputdir="$out" --bindir="$BINDIR" --dlpath="$R" \
	--host="$D/sock" --port=5432 --user=postgres --dbname=regression \
	--schedule="$R/parallel_schedule" --max-connections=20 > "$out/pg_regress.log" 2>&1
rc=$?

server_stop "$D"
cp "$D/log" "$WORK/server.log"
{
	echo "pg_regress exit $rc"
	grep -cE "^ok " "$out/pg_regress.log" | sed 's/^/tests passed: /'
	grep -cE "^not ok " "$out/pg_regress.log" | sed 's/^/tests failed: /'
	echo "errors the extension raised, by message, in the differences:"
	grep -E "^\+ERROR:" "$out/regression.diffs" 2>/dev/null | sed -E 's/"[^"]*"/"..."/g' | sort | uniq -c | sort -rn | head -30
} | tee "$WORK/summary.txt"
if log_failures "$D"; then
	echo "pgregress: the server log has failures"
	log_failures "$D" | head -20
	exit 1
fi
echo "pgregress: no crash, assertion or PANIC; the differences are in $out/regression.diffs"
