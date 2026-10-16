#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# CH0's spike (ch_table_engines.md §5, CH0): engine/spike/spike.c built by the
# server image's compiler, as the extension will be, dlopen()ing the engine
# library at $CHTE_ENGINE, its parts under $CHTE_WORK/spike-data.
set -eu
src="${CHTE_SRC:-/src}"
work="${CHTE_WORK:-/work}"
gcc -O1 -g -Wall -Werror -I"$src/include" -o /tmp/spike "$src/engine/spike/spike.c" -ldl
rm -rf "$work/spike-data"
mkdir -p "$work/spike-data"
/tmp/spike "${CHTE_ENGINE:-/engine/libchte_engine.so}" "$work/spike-data"
