#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# ch_table_engines' legs on the host (ch_table_engines.md §6): the images,
# and each leg in a container of its own (docker/compose.yml), its results in
# a run of the cache.  Nothing is built, installed or run against a server on
# the host (§1.2, rule 8); it needs docker with Compose v2 and BuildKit.
#
#   run.sh sources       ClickHouse's sources at the pinned release, with the
#                        submodules engine/submodules.txt lists (on the host:
#                        git only)
#   run.sh engine        the builder image, then the engine library built in it
#                        (~/.cache/pg_accel/chte/engine)
#   run.sh spike         CH0's spike: engine/spike/spike.c against the library
#   run.sh images        the dev images, on pg_accel's vexec-dev images
#   run.sh core          checks/core.sh: no identifier of the port's core
#                        series named in src/ (§1.2)
#   run.sh suite         the regression suite (sql/, expected/), preloaded,
#                        and its lazy-loading tests without preloading
#   run.sh tap           the TAP tests (t/): crash, standby, backups and
#                        PITR, dump and restore, two sessions at once
#   run.sh isolation     concurrent DML and row locks: each spec, the
#                        extension's and PostgreSQL's, by isolationtester over
#                        heap and over mergetree, the answers compared
#   run.sh pgregress     PostgreSQL's own regression suite with
#                        default_table_access_method = mergetree, its
#                        differences kept for review
#   run.sh hits          10M ClickBench rows by COPY into heap and mergetree
#                        in both stores, their answers compared, and six
#                        reads timed
#   run.sh merges        the 10M rows into mergetree in small batches, the
#                        merge workers merging beside: parts over time, write
#                        amplification, the answers compared
#   run.sh all           core, spike, suite, tap
#
# Settings, from the environment:
#   CHTE_CACHE   the runs: ~/.cache/pg_accel/chte
#   CHTE_CPUS    each container's CPUs: 0, as many as there are; 1 beside a
#                timed run
#   CHTE_TESTS   the suite's tests, all by default
#   CHTE_SETTINGS a setting for the suite's servers: debug_discard_caches = 1
#                discards every cache wherever an invalidation may be taken
#   PG_SRC      the checkout of PostgreSQL the images take PostgreSQL's TAP
#                modules from: ~/.cache/pg_accel/postgres
set -u -o pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$here/.." && pwd)"
COMPOSE="$ROOT/docker/compose.yml"
export CHTE_CACHE="${CHTE_CACHE:-$HOME/.cache/pg_accel/chte}"
export PG_SRC="${PG_SRC:-$HOME/.cache/pg_accel/postgres}"
export CHTE_CPUS="${CHTE_CPUS:-0}"

die() { echo "run.sh: $*" >&2; exit 1; }
compose() { docker compose -f "$COMPOSE" "$@"; }

# leg <name> <command...>: in a vanilla dev container, its run's directory
# under the cache, its output kept there too.
leg() {
	local name="$1"; shift
	local run="$CHTE_CACHE/runs/$(date +%Y%m%dT%H%M%S)-$name"
	mkdir -p "$run"
	echo "== $name: $run"
	CHTE_WORK="$run" compose --profile run run --rm -T vanilla "$@" 2>&1 \
		| grep -v -E '^ (Container|Network) ' | tee "$run/output.log"
	local rc=${PIPESTATUS[0]}
	echo "== $name: $( [ "$rc" = 0 ] && echo passed || echo "FAILED ($rc)")"
	return "$rc"
}

case "${1:-}" in
	sources)
		"$here/engine-sources.sh" ;;
	engine)
		compose --profile build build chte-engine-builder || exit 1
		run="$CHTE_CACHE/runs/$(date +%Y%m%dT%H%M%S)-engine"
		mkdir -p "$run"
		echo "== engine: $run"
		compose --profile run run --rm -T engine /src/test/engine.sh 2>&1 | tee "$run/output.log" | tail -5
		rc=${PIPESTATUS[0]}
		echo "== engine: $( [ "$rc" = 0 ] && echo passed || echo "FAILED ($rc)")"
		exit "$rc" ;;
	spike)
		leg spike /src/test/spike.sh ;;
	images)
		compose --profile build build chte-dev-vanilla ;;
	core)
		leg core /src/checks/core.sh ;;
	suite)
		leg suite /src/test/suite.sh ;;
	isolation)
		leg isolation /src/test/isolation.sh ;;
	tap)
		leg tap /src/test/tap.sh ;;
	pgregress)
		leg pgregress /src/test/pgregress.sh ;;
	hits)
		[ -f "$HOME/.cache/pg_accel/clickbench/hits_10m.tsv" ] ||
			die "no hits_10m.tsv: pg_accel's test/clickbench/run.sh fetch, then its subsets"
		CHTE_DATA="$HOME/.cache/pg_accel/clickbench" leg hits /src/test/hits.sh ;;
	merges)
		[ -f "$HOME/.cache/pg_accel/clickbench/hits_10m.tsv" ] ||
			die "no hits_10m.tsv: pg_accel's test/clickbench/run.sh fetch, then its subsets"
		CHTE_DATA="$HOME/.cache/pg_accel/clickbench" leg merges /src/test/merges.sh ;;
	all)
		"$0" core && "$0" spike && "$0" suite && "$0" tap ;;
	*)
		sed -n '3,/^set /p' "$0" | sed 's/^# \{0,1\}//' | head -n -1
		exit 2 ;;
esac
