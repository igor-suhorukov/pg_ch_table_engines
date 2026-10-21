#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# The port's modules built from a tree of the port (ch_table_engines.md CH6),
# in the portbuild service's container (docker/compose.yml): what CH6 changes
# under the port's pg19/ -- gp_core's settings a session sends the segments
# -- is the port's code, on a branch of its own, and the port leg runs it
# before an image of that branch exists, as pg_accel's legs run theirs
# (its test/vexec/portbuild.sh).
#
#   /cbsrc       the port's tree, read-only, with PAX's two submodules
#                mounted over their directories (a worktree has them empty)
#   /cbbuild     the build directory and the stage, kept between runs, so
#                that ninja builds only what changed
#
# It configures once, builds, and stages an install of every module into
# /cbbuild/stage, which the port leg copies over its image's /usr/local/pgsql
# (build.sh).  /cbbuild/stage/COMMIT names the tree's commit and whether it
# had changes.
#
#   PORTBUILD_JOBS   ninja's jobs: 8
#   PORTBUILD_COMMIT the tree's commit, as the host read it
set -eu

SRC=/cbsrc
BUILD=/cbbuild/build
STAGE=/cbbuild/stage
PG_CONFIG=/usr/local/pgsql/bin/pg_config

[ -f "$SRC/pg19/meson.build" ] || { echo "portbuild: no tree of the port at $SRC"; exit 1; }
[ -f "$SRC/contrib/pax_storage/src/cpp/contrib/tabulate/CMakeLists.txt" ] \
	|| { echo "portbuild: PAX's submodules are not mounted"; exit 1; }

if [ ! -f "$BUILD/build.ninja" ]; then
	echo "portbuild: configuring $BUILD from $SRC/pg19"
	meson setup "$BUILD" "$SRC/pg19" -Dpg_config="$PG_CONFIG" --prefix=/usr/local/pgsql > /cbbuild/setup.log 2>&1 \
		|| { tail -40 /cbbuild/setup.log; exit 1; }
fi
start=$(date +%s)
ninja -C "$BUILD" -j"${PORTBUILD_JOBS:-8}" > /cbbuild/ninja.log 2>&1 \
	|| { grep -E -B2 -A12 'error|FAILED' /cbbuild/ninja.log | head -120; exit 1; }
echo "portbuild: built in $(( $(date +%s) - start )) s ($(grep -c '^\[' /cbbuild/ninja.log) steps)"
rm -rf "$STAGE"
DESTDIR="$STAGE" ninja -C "$BUILD" install > /cbbuild/install.log 2>&1 \
	|| { tail -40 /cbbuild/install.log; exit 1; }
echo "${PORTBUILD_COMMIT:-unknown}" > "$STAGE/COMMIT"
echo "portbuild: staged in $STAGE ($(cat "$STAGE/COMMIT"))"
