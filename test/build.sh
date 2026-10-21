#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# In a dev container (ch_table_engines.md §1.2 rule 8): the extension built
# by PGXS against the container's server from a copy of the tree, which is
# mounted read-only, with warnings as errors, and installed into the
# server's tree.
#
#   CHTE_SRC    the tree: /src
#   CHTE_BUILD  where it is built: /tmp/chte-build
#   PG_CONFIG   the server's: /usr/local/pgsql/bin/pg_config
#   MAKE_JOBS   8
#
# On the port's image (CH6), the port's modules built from a tree of the port
# (portbuild.sh), when the leg is given their stage at /cbstage: installed
# over the image's own first, once a container.
set -eu

SRC="${CHTE_SRC:-/src}"
BUILD="${CHTE_BUILD:-/tmp/chte-build}"
PG_CONFIG="${PG_CONFIG:-/usr/local/pgsql/bin/pg_config}"

if [ -d /cbstage/usr/local/pgsql ] && [ ! -f /tmp/.chte-port-staged ]; then
	cp -a /cbstage/usr/local/pgsql/. "$(dirname "$("$PG_CONFIG" --bindir)")/"
	touch /tmp/.chte-port-staged
	echo "the port's modules from the stage: $(cat /cbstage/COMMIT 2> /dev/null || echo '?')"
fi

rm -rf "$BUILD"
mkdir -p "$BUILD"
for f in Makefile ch_table_engines.control sql expected src include specs t test; do
	[ -e "$SRC/$f" ] && cp -a "$SRC/$f" "$BUILD/"
done
echo "building against $("$PG_CONFIG" --version) at $(cat "$("$PG_CONFIG" --bindir)/../.pg_ref_commit" 2> /dev/null || echo '?')"
make -s -C "$BUILD" PG_CONFIG="$PG_CONFIG" COPT="-Werror" -j"${MAKE_JOBS:-8}" ${CHTE_MAKE_FLAGS:-}
make -s -C "$BUILD" PG_CONFIG="$PG_CONFIG" install > /dev/null
# the engine library, built in the builder image (test/engine.sh), mounted at $CHTE_ENGINE
if [ -f "${CHTE_ENGINE:-/engine/libchte_engine.so}" ]; then
	ln -sf "${CHTE_ENGINE:-/engine/libchte_engine.so}" "$("$PG_CONFIG" --pkglibdir)/libchte_engine.so"
fi
echo "  ch_table_engines built and installed"
