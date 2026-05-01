cmake_minimum_required(VERSION 3.20)
project(hedgeDB CXX)

enable_testing()

# download googletest
include(FetchContent)
FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)

set(INSTALL_GTEST OFF)
FetchContent_MakeAvailable(googletest)

# download outcome.hpp
file(
    DOWNLOAD
    "https://raw.githubusercontent.com/ned14/outcome/8b25c1713ebb2590981d8c4bc176d028026bdf8a/single-header/outcome.hpp"
    ${CMAKE_SOURCE_DIR}/include/outcome.hpp
    EXPECTED_HASH SHA256=2b9d92acca21cc9294e138c848630c8992edc3540f4fb4819a3b396e9d8a2ccc
    SHOW_PROGRESS
)

# download tsl:robin_map
FetchContent_Declare(
    tsl_robin_map
    GIT_REPOSITORY https://github.com/Tessil/robin-map.git
    GIT_TAG        v1.4.1
)
FetchContent_MakeAvailable(tsl_robin_map)

# download tsl:sparse_map
FetchContent_Declare(
    tsl_sparse_map
    GIT_REPOSITORY https://github.com/Tessil/sparse-map.git
    GIT_TAG        v0.7.0
)
FetchContent_MakeAvailable(tsl_sparse_map)

# download xxhashct
FetchContent_Declare(
    xxhashct
    GIT_REPOSITORY https://github.com/ekpyron/xxhashct.git
)
FetchContent_MakeAvailable(xxhashct)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

if (CMAKE_BUILD_TYPE MATCHES "Debug")
    message (STATUS "Building in Debug mode")
    set(CMAKE_CXX_FLAGS "-O0 -g -fno-omit-frame-pointer -foptimize-sibling-calls -Wall -Wextra -Wpedantic -Werror -fsanitize=address,undefined")
elseif (CMAKE_BUILD_TYPE MATCHES "Release")
    message (STATUS "Building in Release mode")
    set(CMAKE_CXX_FLAGS "-O2 -g -fno-omit-frame-pointer")
endif()

# add_compile_definitions(-DTMC_PRIORITY_COUNT=5)

find_library(HWLOC_LIB hwloc)
if(HWLOC_LIB)
    message(STATUS "hwloc found")
    add_compile_definitions(TMC_USE_HWLOC)
else()
    message(STATUS "hwloc not found — TMC_USE_HWLOC disabled")
endif()

set(PUBLIC_INCLUDES
    "include"
    "include/api"
    "include/async"
)

include_directories(${PUBLIC_INCLUDES})
include_directories("src")

include_directories(${tsl_robin_map_SOURCE_DIR}/include)
include_directories(${tsl_sparse_map_SOURCE_DIR}/include)
include_directories(${xxhashct_SOURCE_DIR})
include_directories("/home/federico/TooManyCooks/include")


set(IO_EXECUTOR_SRCS
    src/io/static_pool.cc
    src/io/io_executor.cc
    src/async/tmc.cc
    src/perf_counter.cc
)

set(DB_SOURCES
    src/types.h
    src/utils.cc
    src/utils.h
    src/db/block.h
    src/db/block.cc
    src/db/sst.h
    src/db/sst.cc
    src/db/index_ops.cc
    src/db/qf.cc
    src/db/quotient_filter.cc
    src/db/range_iterator.cc
    src/db/merge/merge.cc
    src/db/index_ops.h
    src/db/memtable.h
    src/db/memtable.cc
    src/db/wal.h
    src/db/wal.cc
    src/fs/fs.hpp
    src/fs/fs.cc
    src/fs/file_reader2.cc
    src/db/database.cc
    src/db/database.h
    src/db/sst_manager.h
    src/db/sst_manager.cc
    src/db/sst_manager_loader.cc
    src/cache.h
    src/cache.cc
)

# hedgedb: async I/O executor (io_uring + TooManyCooks)
add_library(hedgedb_io STATIC ${IO_EXECUTOR_SRCS})
target_link_libraries(hedgedb_io PUBLIC uring atomic)
if(HWLOC_LIB)
    target_link_libraries(hedgedb_io PUBLIC hwloc)
endif()

# hedgedb: full DB static library
add_library(hedgedb STATIC ${DB_SOURCES})
target_link_libraries(hedgedb PUBLIC hedgedb_io xxhash)


if(USE_TCMALLOC)
    set(MALLOC "tcmalloc")
    message(STATUS "Using tcmalloc")
elseif(USE_JEMALLOC)
    set(MALLOC "jemalloc")
    message(STATUS "Using jemalloc")
else()
    message(STATUS "No custom allocator found")
endif()

# executor_read_test - temporarily disabled (io_executor.h renamed to .deprecated)
# add_executable(executor_read_test src/executor_read_test.cc)
# target_link_libraries(executor_read_test hedgedb_io tcmalloc)

# skiplist_executor_test — benchmarks executor + skiplist insert without full DB machinery
# Temporarily disabled due to missing wait_group.h and incorrect include paths
# add_executable(skiplist_executor_test src/skiplist_executor_test.cc)
# target_link_libraries(skiplist_executor_test hedgedb_io tcmalloc)

# memtable_perf_test — benchmarks executor + memtable::put() (adds rw_sync + value arenas)
add_executable(memtable_perf_test src/memtable_perf_test.cc)
target_link_libraries(memtable_perf_test hedgedb)

# memtable_perf_test_threads — same benchmark but using N plain std::threads instead of executors
# Temporarily disabled due to pre-existing API changes (flush_io_workers removed, put_sync→put_async)
# add_executable(memtable_perf_test_threads src/memtable_perf_test_threads.cc)
# target_link_libraries(memtable_perf_test_threads hedgedb tcmalloc)

# database_perf_test — full database write+read benchmark (no gtest)
add_executable(database_perf_test src/database_perf_test.cc)
target_link_libraries(database_perf_test hedgedb ${MALLOC})

# database_perf_test_threads — same benchmark but using N plain std::threads + sync_submit instead of executor task loop
# Temporarily disabled due to pre-existing API changes (wait_group.h missing)
# add_executable(database_perf_test_threads src/database_perf_test_threads.cc)
# target_link_libraries(database_perf_test_threads hedgedb ${MALLOC})

# database_perf_test_mixed — 50/50 mixed read+write benchmark
# Temporarily disabled due to pre-existing API changes
# add_executable(database_perf_test_mixed src/database_perf_test_mixed.cc)
# target_link_libraries(database_perf_test_mixed hedgedb ${MALLOC})

# database_perf_test_mixed_batch — 50/50 mixed read+write benchmark
# Temporarily disabled due to pre-existing API changes
# add_executable(database_perf_test_mixed_batch src/database_perf_test_mixed_batch.cc)
# target_link_libraries(database_perf_test_mixed_batch hedgedb ${MALLOC})

# benchtool — unified benchmark CLI (write / read / rw / range modes)
add_executable(benchtool
    src/benchtool/main.cc
    src/benchtool/utils.cc
    src/benchtool/load.cc
    src/benchtool/read.cc
    src/benchtool/rw.cc
    src/benchtool/range.cc)
target_link_libraries(benchtool hedgedb ${MALLOC})

# executor tests
# executor_test - temporarily disabled due to pre-existing API changes (missing io request types)
# add_executable(executor_test test/io_executor.spec.cc)
# target_link_libraries(executor_test hedgedb_io gtest gtest_main)

# index (merge) tests
# Temporarily disabled: wait_group.h missing
# add_executable(index_test test/index.spec.cc)
# target_link_libraries(index_test hedgedb gtest gtest_main)

# index2 tests
add_executable(index2_test test/index2.spec.cc)
target_link_libraries(index2_test hedgedb gtest gtest_main)

# parallel flush tests
add_executable(index_test_parallel_flush test/index_test.cc)
target_link_libraries(index_test_parallel_flush hedgedb gtest gtest_main)

# index (delete and merge) tests
add_executable(index_delete_test test/index_delete.spec.cc)
target_link_libraries(index_delete_test hedgedb gtest gtest_main)

# file_reader2 tests
add_executable(file_reader2_test test/file_reader2.spec.cc)
target_link_libraries(file_reader2_test hedgedb gtest gtest_main)

# worker tests
add_executable(worker_test test/worker.spec.cc)
target_link_libraries(worker_test hedgedb gtest gtest_main)

# mpsc tests
add_executable(mpsc_test test/mpsc.spec.cc)
target_link_libraries(mpsc_test gtest gtest_main)

# cache tests
add_executable(cache_test test/cache.spec.cc)
target_link_libraries(cache_test hedgedb gtest gtest_main)

# database tests
add_executable(database_test test/database.spec.cc)
target_link_libraries(database_test hedgedb gtest gtest_main tcmalloc tbb)

# version history test
add_executable(version_history_test test/version_history.spec.cc)
target_link_libraries(version_history_test hedgedb gtest gtest_main tbb)

# WAL replay tests
add_executable(wal_replay_test test/wal_replay.spec.cc)
target_link_libraries(wal_replay_test hedgedb gtest gtest_main tcmalloc)

# varint tests
add_executable(varint_test test/varint.spec.cc)
target_link_libraries(varint_test gtest gtest_main)

# key tests
add_executable(key_test test/key.spec.cc)
target_link_libraries(key_test gtest gtest_main)

# block tests
add_executable(block_test src/db/block.cc test/block.spec.cc)
target_link_libraries(block_test gtest gtest_main)

# page_aligned_buffer tests
add_executable(page_aligned_buffer_test test/page_aligned_buffer.spec.cc src/utils.cc)
target_link_libraries(page_aligned_buffer_test gtest gtest_main)

# rolling_buffer tests
add_executable(rolling_buffer_test test/rolling_buffer.spec.cc)
target_link_libraries(rolling_buffer_test hedgedb gtest gtest_main)

# debug test
add_executable(index_merge test/index_merge.spec.cc)
target_link_libraries(index_merge hedgedb gtest gtest_main tcmalloc)

# concurrent_skiplist tests
add_executable(concurrent_skiplist_test test/concurrent_skiplist.spec.cc)
target_link_libraries(concurrent_skiplist_test gtest gtest_main atomic tcmalloc)

# range scan tests
add_executable(range_scan_test test/range_scan.spec.cc)
target_link_libraries(range_scan_test hedgedb gtest gtest_main tcmalloc)
