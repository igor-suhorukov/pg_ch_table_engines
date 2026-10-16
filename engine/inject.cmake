# Passed to ClickHouse's configure as CMAKE_PROJECT_ClickHouse_INCLUDE, so it runs right after
# ClickHouse's project() call: it schedules engine.cmake to be included at the end of ClickHouse's
# top-level CMakeLists.txt, once every ClickHouse target exists (ch_table_engines.md §2.2).
# ClickHouse's sources stay untouched; the engine is built with ClickHouse's own flags.
# (A deferred call may not add_subdirectory, so engine.cmake runs in the top-level scope.)
set(CHTE_ENGINE_DIR "${CMAKE_CURRENT_LIST_DIR}" CACHE INTERNAL "")
cmake_language(DEFER DIRECTORY "${CMAKE_SOURCE_DIR}" CALL include "${CHTE_ENGINE_DIR}/engine.cmake")
