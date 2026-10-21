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
#   run.sh images [port] the dev images, on pg_accel's vexec-dev images: the
#                        vanilla server's, or the port's at CHTE_PORT_COMMIT
#                        (pg_accel/vexec-dev:port-<commit>, which pg_accel's
#                        docker/compose.yml and vexec.yml build)
#   run.sh portbuild     the port's modules built from a tree of the port,
#                        CHTE_PORT_SRC, into CHTE_PORT_BUILD and staged there:
#                        the port leg's containers install the stage over
#                        their image's modules (CH6)
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
#   run.sh cluster       the port's cluster (CH6, Appendix D): a coordinator
#                        and CHTE_SEGMENTS segments, the extension on every
#                        node, mirrors, an added segment (test/cluster.sh)
#   run.sh all           core, spike, suite, tap
#
# Settings, from the environment:
#   CHTE_CACHE   the runs: ~/.cache/pg_accel/chte
#   CHTE_CPUS    each container's CPUs: 0, as many as there are; 1 beside a
#                timed run
#   CHTE_LEG     the server a leg runs on: vanilla, or port -- the port's
#                patched server and modules, the port's single node for the
#                suite, tap, isolation and pgregress
#   CHTE_TESTS   the suite's tests, all by default
#   CHTE_SETTINGS a setting for the suite's servers: debug_discard_caches = 1
#                discards every cache wherever an invalidation may be taken
#   CHTE_PRELOAD the suite's preloaded libraries, on the port's single node
#                with the port's modules: ch_table_engines,gp_core,gp_orca
#   PG_SRC      the checkout of PostgreSQL the images take PostgreSQL's TAP
#                modules from: ~/.cache/pg_accel/postgres
#   CHTE_PORT_COMMIT  the port's commit its images were built from:
#                c06bdd36239, extension_postgresql_19's head when CH6 was built;
#                the session's ch.* settings reach the segments since the
#                port's afd2eac3e9b, whose modules portbuild stages over it
#   CHTE_PORT_SRC  the port's tree portbuild builds: the port's checkout beside
#                this one
#   CHTE_PORT_BUILD  its build and stage: $CHTE_CACHE/portbuild
#   CHTE_PORT_STAGE  the stage the port leg installs: CHTE_PORT_BUILD's, when it
#                has one; "none" for the image's own modules
#   CHTE_PAX_CONTRIB  PAX's two submodules, which a worktree of the port has
#                empty: pg_accel's checkout of the port has them
set -u -o pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$here/.." && pwd)"
COMPOSE="$ROOT/docker/compose.yml"
export CHTE_CACHE="${CHTE_CACHE:-$HOME/.cache/pg_accel/chte}"
export PG_SRC="${PG_SRC:-$HOME/.cache/pg_accel/postgres}"
export CHTE_CPUS="${CHTE_CPUS:-0}"
export CHTE_PORT_COMMIT="${CHTE_PORT_COMMIT:-c06bdd36239175cb204bc2d319b1ab8e8380fb1d}"
export CHTE_PORT_SRC="${CHTE_PORT_SRC:-$(cd "$ROOT/.." && pwd)/cloudberry}"
export CHTE_PORT_BUILD="${CHTE_PORT_BUILD:-$CHTE_CACHE/portbuild}"
export CHTE_PAX_CONTRIB="${CHTE_PAX_CONTRIB:-$HOME/.cache/pg_accel/cloudberry/contrib/pax_storage/src/cpp/contrib}"
if [ -z "${CHTE_PORT_STAGE:-}" ]; then
	[ -d "$CHTE_PORT_BUILD/stage/usr/local/pgsql" ] && CHTE_PORT_STAGE="$CHTE_PORT_BUILD/stage" || CHTE_PORT_STAGE=none
fi
[ "$CHTE_PORT_STAGE" = none ] && CHTE_PORT_STAGE=/nonexistent
export CHTE_PORT_STAGE
LEG="${CHTE_LEG:-vanilla}"
case "$LEG" in
	vanilla|port) ;;
	*) echo "run.sh: CHTE_LEG is vanilla or port, not $LEG" >&2; exit 2 ;;
esac

die() { echo "run.sh: $*" >&2; exit 1; }
compose() { docker compose -f "$COMPOSE" "$@"; }

# leg <name> <command...>: in a dev container of the leg's server, its run's
# directory under the cache, its output kept there too.
leg() {
	local name="$1"; shift
	local tag=""
	[ "$LEG" = port ] && tag="-port"
	local run="$CHTE_CACHE/runs/$(date +%Y%m%dT%H%M%S)-$name$tag"
	mkdir -p "$run"
	echo "== $name${tag:+ (the port)}: $run"
	[ "$LEG" = port ] && echo "   the port at ${CHTE_PORT_COMMIT:0:11}, its modules $(
		[ "$CHTE_PORT_STAGE" = /nonexistent ] && echo "the image's" ||
			echo "from the stage: $(cat "$CHTE_PORT_STAGE/COMMIT" 2> /dev/null || echo '?')")"
	CHTE_WORK="$run" compose --profile run run --rm -T "$LEG" "$@" 2>&1 \
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
		case "${2:-vanilla}" in
			vanilla) compose --profile build build chte-dev-vanilla ;;
			port)
				docker image inspect "pg_accel/vexec-dev:port-$CHTE_PORT_COMMIT" > /dev/null 2>&1 ||
					die "no pg_accel/vexec-dev:port-$CHTE_PORT_COMMIT: pg_accel's docker/compose.yml builds pg19-patched and cb-ext at the commit, its docker/vexec.yml vexec-dev-port and vexec-dev-portdev"
				compose --profile build build chte-dev-port ;;
			*) die "images: vanilla or port" ;;
		esac ;;
	portbuild)
		[ -f "$CHTE_PORT_SRC/pg19/meson.build" ] || die "no tree of the port at $CHTE_PORT_SRC (CHTE_PORT_SRC)"
		[ -f "$CHTE_PAX_CONTRIB/tabulate/CMakeLists.txt" ] || die "no PAX submodules at $CHTE_PAX_CONTRIB (CHTE_PAX_CONTRIB)"
		mkdir -p "$CHTE_PORT_BUILD"
		commit="$(git -C "$CHTE_PORT_SRC" rev-parse --short=11 HEAD 2> /dev/null || echo '?')"
		[ -n "$(git -C "$CHTE_PORT_SRC" status --porcelain --untracked-files=no 2> /dev/null)" ] && commit="$commit with changes"
		echo "== portbuild: $CHTE_PORT_SRC ($commit) into $CHTE_PORT_BUILD"
		PORTBUILD_COMMIT="$(git -C "$CHTE_PORT_SRC" rev-parse --abbrev-ref HEAD 2> /dev/null) $commit" \
			compose --profile run run --rm -T portbuild 2>&1 | grep -v -E '^ (Container|Network) '
		exit "${PIPESTATUS[0]}" ;;
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
	cluster)
		LEG=port leg cluster /src/test/cluster.sh ;;
	all)
		"$0" core && "$0" spike && "$0" suite && "$0" tap ;;
	*)
		sed -n '3,/^set /p' "$0" | sed 's/^# \{0,1\}//' | head -n -1
		exit 2 ;;
esac
