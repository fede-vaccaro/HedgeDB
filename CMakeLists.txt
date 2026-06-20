cmake_minimum_required(VERSION 3.20)
project(hedgeDB CXX)

include(FetchContent)

if(BUILD_TESTS)
    message(STATUS "Tests build enabled")
    enable_testing()

    # download googletest
    FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
    )

    set(INSTALL_GTEST OFF)
    FetchContent_MakeAvailable(googletest)
endif()

# download outcome.hpp
file(
    DOWNLOAD
    "https://raw.githubusercontent.com/ned14/outcome/8b25c1713ebb2590981d8c4bc176d028026bdf8a/single-header/outcome.hpp"
    ${CMAKE_SOURCE_DIR}/include/outcome.hpp
    EXPECTED_HASH SHA256=2b9d92acca21cc9294e138c848630c8992edc3540f4fb4819a3b396e9d8a2ccc
    SHOW_PROGRESS
)

# download xxhashct
FetchContent_Declare(
    xxhashct
    GIT_REPOSITORY https://github.com/ekpyron/xxhashct.git
    GIT_TAG 87a9dbcd06a36f31bbada4c0064db7e545b293df
)
FetchContent_MakeAvailable(xxhashct)


# download TooManyCooks
FetchContent_Declare(
    tmc
    GIT_REPOSITORY https://github.com/tzcnt/TooManyCooks.git
    GIT_TAG v1.6.1
)
FetchContent_MakeAvailable(tmc)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

if(CMAKE_BUILD_TYPE MATCHES "Debug")
    message(STATUS "Building in Debug mode")
    set(CMAKE_CXX_FLAGS "-O0 -g -fno-omit-frame-pointer -foptimize-sibling-calls -Wall -Wextra -Wpedantic -Werror -Wno-interference-size")
elseif(CMAKE_BUILD_TYPE MATCHES "Release")
    message(STATUS "Building in Release mode")
    set(CMAKE_CXX_FLAGS "-O2 -g -fno-omit-frame-pointer -Wno-interference-size -fno-rtti")
endif()

if(SANITIZERS)
    message(STATUS "Enabling sanitizers: ${SANITIZERS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=${SANITIZERS}")
    set(CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} -fsanitize=${SANITIZERS}")
endif()

find_library(HWLOC_LIB hwloc)
if(HWLOC_LIB)
    message(STATUS "hwloc found")
    set(TMC_USE_HWLOC ON CACHE BOOL "" FORCE)
else()
    set(TMC_USE_HWLOC OFF CACHE BOOL "" FORCE)
    message(STATUS "hwloc not found — TMC_USE_HWLOC disabled")
endif()

set(PUBLIC_INCLUDES
    "include"
)

include_directories(${PUBLIC_INCLUDES})
include_directories("src")

include_directories(${xxhashct_SOURCE_DIR})

set(IO_EXECUTOR_SRCS
    src/io/static_pool.cc
    src/io/io_executor.cc
    src/async/tmc.cc
    src/perf_counter.cc
)

set(DB_SOURCES
    src/utils.cc
    src/db/block.cc
    src/db/sst.h
    src/db/sst.cc
    src/db/index_ops.cc
    src/db/qf.cc
    src/db/quotient_filter.cc
    src/db/range_iterator.cc
    src/db/merge/merge.cc
    src/db/memtable.cc
    src/db/wal.cc
    src/fs/fs.hpp
    src/fs/fs.cc
    src/fs/file_reader2.cc
    src/db/database.cc
    src/db/sst_manager.cc
    src/db/sst_manager_loader.cc
    src/cache.cc
)

# hedgedb: async I/O executor (io_uring + TooManyCooks)
add_library(hedgedb_io STATIC ${IO_EXECUTOR_SRCS})
target_link_libraries(hedgedb_io PUBLIC uring atomic TooManyCooks::TooManyCooks)
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

# benchtool (unified benchmark CLI)
add_executable(benchtool
    benchtool/main.cc
    benchtool/utils.cc
    benchtool/registry.cc
    benchtool/load.cc
    benchtool/load_timeseries.cc
    benchtool/read.cc
    benchtool/rw.cc
    benchtool/range.cc
    benchtool/range_timeseries.cc
    benchtool/compaction.cc
    benchtool/delete.cc)
target_link_libraries(benchtool hedgedb ${MALLOC})

# Hello world example
add_executable(hello_world examples/hello_world.cc)
target_link_libraries(hello_world hedgedb ${MALLOC})

# memtable_perf_test - benchmarks executor + memtable::put() (adds rw_sync + value arenas)
add_executable(memtable_perf_test src/memtable_perf_test.cc)
target_link_libraries(memtable_perf_test hedgedb)

if (BUILD_TESTS)
    # sst tests (flush memtable -> on-disk SST -> lookup, with disk reload coverage)
    add_executable(sst_test test/sst_test.cc)
    target_link_libraries(sst_test hedgedb gtest gtest_main)
    add_test(NAME sst_test COMMAND sst_test)

    # file_reader2 tests
    add_executable(file_reader2_test test/file_reader2_test.cc)
    target_link_libraries(file_reader2_test hedgedb gtest gtest_main)
    add_test(NAME file_reader2_test COMMAND file_reader2_test)

    # database tests
    add_executable(database_test test/database_test.cc)
    target_link_libraries(database_test hedgedb gtest gtest_main ${MALLOC})
    add_test(NAME database_test COMMAND database_test)

    # version history test
    add_executable(version_history_test test/version_history_test.cc)
    target_link_libraries(version_history_test hedgedb gtest gtest_main)
    add_test(NAME version_history_test COMMAND version_history_test)

    # WAL replay tests
    add_executable(wal_replay_test test/wal_replay_test.cc)
    target_link_libraries(wal_replay_test hedgedb gtest gtest_main)
    add_test(NAME wal_replay_test COMMAND wal_replay_test)

    # varint tests
    add_executable(varint_test test/varint_test.cc)
    target_link_libraries(varint_test gtest gtest_main)
    add_test(NAME varint_test COMMAND varint_test)

    # key tests
    add_executable(key_test test/key_test.cc)
    target_link_libraries(key_test gtest gtest_main)
    add_test(NAME key_test COMMAND key_test)

    # block tests
    add_executable(block_test src/db/block.cc test/block_test.cc)
    target_link_libraries(block_test gtest gtest_main)
    add_test(NAME block_test COMMAND block_test)

    # page_aligned_buffer tests
    add_executable(page_aligned_buffer_test test/page_aligned_buffer_test.cc src/utils.cc)
    target_link_libraries(page_aligned_buffer_test gtest gtest_main)
    add_test(NAME page_aligned_buffer_test COMMAND page_aligned_buffer_test)

    # sst_stream tests
    add_executable(sst_stream_test test/sst_stream_test.cc)
    target_link_libraries(sst_stream_test hedgedb gtest gtest_main)
    add_test(NAME sst_stream_test COMMAND sst_stream_test)

    # index merge tests
    add_executable(index_merge test/index_merge_test.cc)
    target_link_libraries(index_merge hedgedb gtest gtest_main ${MALLOC})
    add_test(NAME index_merge COMMAND index_merge)

    # rw_sync tests
    add_executable(rw_sync_test test/rw_sync_test.cc)
    target_link_libraries(rw_sync_test gtest gtest_main atomic)
    add_test(NAME rw_sync_test COMMAND rw_sync_test)

    # concurrent_skiplist tests
    add_executable(concurrent_skiplist_test test/concurrent_skiplist_test.cc)
    target_link_libraries(concurrent_skiplist_test gtest gtest_main atomic ${MALLOC})

    # range scan tests
    add_executable(range_scan_test test/range_scan_test.cc)
    target_link_libraries(range_scan_test hedgedb gtest gtest_main ${MALLOC})
    add_test(NAME range_scan_test COMMAND range_scan_test)
endif()