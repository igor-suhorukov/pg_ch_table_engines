#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# The TAP tests (ch_table_engines.md §6, "tap"), in a dev container: prove
# over t/ with PostgreSQL's TAP modules at the server's own commit, which the
# dev image keeps in /cb/pgperl, and IPC::Run.
#
#   CHTE_TAP_TESTS   the tests, t/*.pl by default
#   CHTE_WORK        the run's directory: the nodes' data and logs
set -u
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/lib.sh"
WORK="${CHTE_WORK:-/work}"
BUILD=/tmp/chte-build

"$here/build.sh" > "$WORK/build.log" 2>&1 || { cat "$WORK/build.log"; exit 1; }
export PERL5LIB=/cb/pgperl
export TESTDATADIR="$WORK/tap/data" TESTLOGDIR="$WORK/tap/log"
export PATH="$BINDIR:$PATH"
# Cluster.pm runs pg_regress --config-auth as it makes a node
export PG_REGRESS="$(dirname "$("$BINDIR/pg_config" --pgxs)")/../test/regress/pg_regress"
mkdir -p "$TESTDATADIR" "$TESTLOGDIR"
cd "$BUILD" || exit 1
prove -v ${CHTE_TAP_TESTS:-t/*.pl} 2>&1 | tee "$WORK/prove.log"
exit "${PIPESTATUS[0]}"
