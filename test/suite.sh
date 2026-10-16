#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# The extension's regression suite (ch_table_engines.md §6, "suite"), in a
# dev container: built, then pg_regress over sql/'s tests against a server
# that preloads the library; then the tests that must pass without
# preloading, against one that loads it lazily (§2.1, "Loading").
#
#   CHTE_TESTS   the tests, all of the schedule's by default
#   CHTE_WORK    the run's directory: results/, the servers' logs
set -u
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/lib.sh"
WORK="${CHTE_WORK:-/work}"
BUILD=/tmp/chte-build

"$here/build.sh" > "$WORK/build.log" 2>&1 || { cat "$WORK/build.log"; exit 1; }
PG_REGRESS="$(dirname "$("$BINDIR/pg_config" --pgxs)")/../test/regress/pg_regress"
status=0

run_suite() {
	local name="$1" preload="$2"; shift 2
	local dir="/tmp/srv-$name" out="$WORK/$name"
	rm -rf "$dir" "$out"
	mkdir -p "$out"
	server_init "$dir" ${preload:+"shared_preload_libraries = '$preload'"} ${CHTE_EXTRA_SETTING:+"$CHTE_EXTRA_SETTING"} || return 1
	server_start "$dir" || { cat "$dir/log"; return 1; }
	"$PG_REGRESS" --inputdir="$BUILD" --outputdir="$out" --bindir="$BINDIR" \
		--host="$dir/sock" --port=5432 --user=postgres --dbname=chte_regression \
		"$@" > "$out/pg_regress.log" 2>&1
	local rc=$?
	cat "$out/pg_regress.log" | grep -E "^(ok|not ok|#|\s+[0-9]+ )" | tail -40
	[ -f "$out/regression.diffs" ] && cp "$out/regression.diffs" "$WORK/$name.diffs"
	server_stop "$dir"
	cp "$dir/log" "$out/server.log"
	if log_failures "$dir"; then
		echo "  $name: the server log has failures"
		rc=1
	fi
	return $rc
}

tests=${CHTE_TESTS:-$(sed -n 's/^test: //p' "$BUILD/test/schedule" | tr '\n' ' ')}
echo "== suite, preloaded: $tests"
run_suite preloaded ch_table_engines $tests || status=1
nopreload=$(sed -n 's/^nopreload: //p' "$BUILD/test/schedule" | tr '\n' ' ')
if [ -z "${CHTE_TESTS:-}" ] && [ -n "$nopreload" ]; then
	echo "== suite, not preloaded: $nopreload"
	run_suite lazy "" $nopreload || status=1
fi
files=$(sed -n 's/^files: //p' "$BUILD/test/schedule" | tr '\n' ' ')
if [ -z "${CHTE_TESTS:-}" ] && [ -n "$files" ]; then
	echo "== suite, the files store by default: $files"
	CHTE_EXTRA_SETTING="ch.default_storage = 'files'" run_suite files ch_table_engines $files || status=1
fi
exit $status
