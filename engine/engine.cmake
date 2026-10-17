# libchte_engine.so: ClickHouse's MergeTree family behind the C API of include/chte_engine.h
# (ch_table_engines.md §2.2).  inject.cmake includes this file at the end of ClickHouse's
# top-level CMakeLists.txt, so paths are spelled from CHTE_ENGINE_DIR.

add_library(chte_engine SHARED
    "${CHTE_ENGINE_DIR}/src/engine.cpp"
    "${CHTE_ENGINE_DIR}/src/table.cpp"
    "${CHTE_ENGINE_DIR}/src/io.cpp"
    "${CHTE_ENGINE_DIR}/src/pages.cpp"
    "${CHTE_ENGINE_DIR}/src/select.cpp")
target_include_directories(chte_engine PRIVATE "${CHTE_ENGINE_DIR}/../include")
target_link_libraries(chte_engine PRIVATE
    dbms
    clickhouse_functions
    clickhouse_aggregate_functions
    clickhouse_parsers
    clickhouse_common_io)
set_target_properties(chte_engine PROPERTIES
    CXX_VISIBILITY_PRESET hidden
    VISIBILITY_INLINES_HIDDEN ON
    LINK_DEPENDS "${CHTE_ENGINE_DIR}/chte_engine.map"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/chte_engine")
# CMAKE_EXE_LINKER_FLAGS carries compiler-rt's builtins (cmake/compiler_rt_link.cmake) and the
# toolchain's --gcc-toolchain (cmake/linux/toolchain-x86_64.cmake) for executables only, so a
# shared library names them itself.
if (TOOLCHAIN_PATH)
    target_link_options(chte_engine PRIVATE "--gcc-toolchain=${TOOLCHAIN_PATH}" "--no-default-config")
endif ()
# LLD as for ClickHouse's executables (cmake/tools.cmake): it resolves symbols across the static
# archives in any order, which ClickHouse's link lines rely on.
target_link_options(chte_engine PRIVATE "--ld-path=${LLD_PATH}")
target_link_options(chte_engine PRIVATE
    "LINKER:--version-script=${CHTE_ENGINE_DIR}/chte_engine.map"
    "LINKER:--exclude-libs,ALL"
    "LINKER:-Bsymbolic"
    "LINKER:--gc-sections"
    "LINKER:-z,now"
    "${BUILTINS_LIBRARY}")

