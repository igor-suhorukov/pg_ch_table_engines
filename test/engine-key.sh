#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# The engine library's key (ch_table_engines.md §2.2): a hash of what
# libchte_engine.so is built from at HEAD -- the engine's sources, its C API,
# the builder image, the build, and ClickHouse's release and submodules -- so
# that a library published under it (test/run.sh publish-engine, the engine
# workflow) is fetched by any checkout of a commit it was built for
# (test/run.sh fetch-engine, the ci workflow).  When those files differ from
# HEAD's in the work tree, the key ends in -dirty: no library is published or
# fetched for it.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
paths=(engine/src engine/chte_engine.map engine/engine.cmake engine/inject.cmake engine/submodules.txt
	include/chte_engine.h docker/Dockerfile.engine test/engine.sh test/engine-sources.sh)
key="$(git -C "$root" ls-tree -r HEAD -- "${paths[@]}" | sha256sum | cut -c1-16)"
[ -n "$(git -C "$root" status --porcelain -- "${paths[@]}")" ] && key="$key-dirty"
echo "$key"
