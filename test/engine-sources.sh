#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# ClickHouse's sources for the engine library (ch_table_engines.md §2.2), on the
# host: a clone of the pinned release, independent of the checkout it is cloned
# from, with only the submodules engine/submodules.txt lists, shallow.  Run again
# after adding a line there.
#
#   CHTE_CH_ORIGIN  where to clone from: a local checkout of ClickHouse, if
#                   the user has one, else GitHub
#   CHTE_CH_SRC     the clone: ~/.cache/pg_accel/chte/clickhouse-src
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tag=v26.8.8.8-lts
commit=0de36d42e8bbc78fdaf2bbf90a2853b5e9301bdc
origin="${CHTE_CH_ORIGIN:-https://github.com/ClickHouse/ClickHouse}"
[ -d "$origin" ] && [ ! -d "$origin/.git" ] && origin=https://github.com/ClickHouse/ClickHouse
dst="${CHTE_CH_SRC:-$HOME/.cache/pg_accel/chte/clickhouse-src}"

if [ ! -d "$dst/.git" ]; then
	if [ -d "$origin" ]; then
		git clone --quiet --local --no-checkout "$origin" "$dst"
	else
		git clone --quiet --no-checkout --filter=blob:none "$origin" "$dst"
	fi
	git -C "$dst" checkout --quiet "$tag"
fi
[ "$(git -C "$dst" rev-parse HEAD)" = "$commit" ] || { echo "$dst is not at $tag ($commit)"; exit 1; }

mapfile -t subs < <(grep -v '^#' "$here/engine/submodules.txt" | sed '/^$/d')
git -C "$dst" submodule init -- "${subs[@]}" > /dev/null
printf '%s\n' "${subs[@]}" | xargs -P 12 -I@ git -C "$dst" submodule update --depth 1 --single-branch -- @
echo "ClickHouse $tag with ${#subs[@]} submodules at $dst"
