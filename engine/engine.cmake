# libchte_engine.so: ClickHouse's MergeTree family behind the C API of include/chte_engine.h
# (ch_table_engines.md §2.2).  inject.cmake includes this file at the end of ClickHouse's
# top-level CMakeLists.txt, so paths are spelled from CHTE_ENGINE_DIR.

add_library(chte_engine SHARED
    "${CHTE_ENGINE_DIR}/src/engine.cpp"
    "${CHTE_ENGINE_DIR}/src/table.cpp"
    "${CHTE_ENGINE_DIR}/src/io.cpp"
    "${CHTE_ENGINE_DIR}/src/pages.cpp"
    "${CHTE_ENGINE_DIR}/src/select.cpp"
    "${CHTE_ENGINE_DIR}/src/rows.cpp"
    "${CHTE_ENGINE_DIR}/src/rewrite.cpp"
    "${CHTE_ENGINE_DIR}/src/merge.cpp")
target_include_directories(chte_engine PRIVATE "${CHTE_ENGINE_DIR}/../include")
# ClickHouse's functions as an archive, not as objects: clickhouse_functions hands a link every
# function's object ($<TARGET_OBJECTS:...>, src/Functions/CMakeLists.txt:77-234), and each object
# registers its functions from a static initializer (src/Common/register_objects.h:28-33), so that
# --gc-sections kept all 792 of them -- 111 of the library's 252 MB of code.  From an archive the
# linker takes only the members something references: the registrations engine.cpp calls, and what
# dbms names (ch_table_engines.md §2.2).
get_target_property(chte_functions_libraries clickhouse_functions INTERFACE_LINK_LIBRARIES)
set(chte_functions_objects ${chte_functions_libraries})
list(FILTER chte_functions_objects INCLUDE REGEX "^\\$<TARGET_OBJECTS:")
list(FILTER chte_functions_libraries EXCLUDE REGEX "^\\$<TARGET_OBJECTS:")
add_library(chte_functions STATIC ${chte_functions_objects})
set_target_properties(chte_functions PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(chte_functions INTERFACE ${chte_functions_libraries})
target_link_libraries(chte_engine PRIVATE
    dbms
    chte_functions
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
# --icf=safe folds the functions whose code is the same and whose address nothing takes, as
# ClickHouse's -faddrsig records it: 7.4 of 135.8 MB of code.  ClickHouse links its executables
# with --icf=all (CMakeLists.txt:176).
target_link_options(chte_engine PRIVATE
    "LINKER:--version-script=${CHTE_ENGINE_DIR}/chte_engine.map"
    "LINKER:--exclude-libs,ALL"
    "LINKER:-Bsymbolic"
    "LINKER:--gc-sections"
    "LINKER:--icf=safe"
    "LINKER:-z,now"
    "${BUILTINS_LIBRARY}")

