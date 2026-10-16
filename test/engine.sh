#!/bin/bash
# Builds libchte_engine.so in the pg_chte/engine-builder container (ch_table_engines.md §2.2;
# containers only, §1.2 rule 8).  Mounts: /ch = ClickHouse v26.8.8.8-lts with only the
# submodules listed in /ch-submodules.txt (read-only), /src = this repository (read-only),
# /build = build tree, ccache and the installed library.
set -euo pipefail
jobs=${CHTE_ENGINE_JOBS:-16}
targets=${CHTE_ENGINE_TARGETS:-chte_engine}
export CCACHE_DIR=/build/ccache CCACHE_MAXSIZE=80G CCACHE_BASEDIR=/ch CCACHE_NOHASHDIR=1 CCACHE_SLOPPINESS=include_file_mtime,include_file_ctime,time_macros

if [ ! -f /build/tree/build.ninja ] || [ "${CHTE_ENGINE_RECONFIGURE:-0}" = 1 ]; then
    # ClickHouse's own hermetic toolchain (PreLoad.cmake: contrib/sysroot, glibc-compatibility,
    # llvm-libc) as in its CI.  PIC for every target: ClickHouse adds -fPIC only after
    # libc++/libc++abi/libunwind are configured (CMakeLists.txt:614-618 at the tag) and refuses
    # custom CMAKE_*_FLAGS (PreLoad.cmake:21-42), so the targets' property carries it.
    cmake -S /ch -B /build/tree -G Ninja -Wno-dev \
        -DCMAKE_BUILD_TYPE="${CHTE_ENGINE_BUILD_TYPE:-RelWithDebInfo}" \
        -DCMAKE_C_COMPILER=clang-21 -DCMAKE_CXX_COMPILER=clang++-21 \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DCMAKE_PROJECT_ClickHouse_INCLUDE=/src/engine/inject.cmake \
        -DCOMPILER_CACHE=ccache \
        -DENABLE_LIBRARIES=OFF -DENABLE_TESTS=OFF -DENABLE_UTILS=OFF -DENABLE_EXAMPLES=OFF \
        -DENABLE_CLICKHOUSE_ALL=OFF -DENABLE_RUST=OFF -DENABLE_JEMALLOC=OFF -DENABLE_NURAFT=OFF \
        -DENABLE_THINLTO=OFF \
        -DENABLE_LIBFIU=ON -DENABLE_CLICKHOUSE_SELF_EXTRACTING=OFF -DENABLE_XRAY=OFF \
        -DPARALLEL_COMPILE_JOBS="$jobs" -DPARALLEL_LINK_JOBS=2
fi
ninja -C /build/tree -j "$jobs" -k "${CHTE_ENGINE_KEEP_GOING:-1}" $targets
if [[ " $targets " == *" chte_engine "* ]]; then
    mkdir -p /build/lib
    cp /build/tree/chte_engine/libchte_engine.so /build/lib/
fi
