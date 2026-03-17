cmake_minimum_required(VERSION 3.10) 
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
    set(CMAKE_CXX_FLAGS "-O0 -g -fno-omit-frame-pointer -foptimize-sibling-calls -Wall -Wextra -Wpedantic -Werror")
elseif (CMAKE_BUILD_TYPE MATCHES "Release")
    message (STATUS "Building in Release mode")
    set(CMAKE_CXX_FLAGS "-O2 -g -fno-omit-frame-pointer")
endif()

set(PUBLIC_INCLUDES 
    "include"
    "include/api"
    "include/async"
)

include_directories(PUBLIC ${PUBLIC_INCLUDES})
include_directories(PRIVATE "src")

include_directories(PUBLIC ${tsl_robin_map_SOURCE_DIR}/include)
include_directories(PUBLIC ${tsl_sparse_map_SOURCE_DIR}/include)
include_directories(PUBLIC ${xxhashct_SOURCE_DIR})

set(DB_SOURCES
    src/types.h
    src/perf_counter.cc
    src/utils.cc
    src/utils.h
    src/db/block.h
    src/db/block.cc
    src/db/sorted_index.cc
    src/db/sorted_index.h
    src/db/sst.h
    src/db/sst.cc
    src/db/index_ops.cc
    src/db/index_ops2.cc
    src/db/qf.cc
    src/db/quotient_filter.cc
    src/db/merge/merge.cc
    src/db/index_ops.h
    src/db/memtable.h
    src/db/memtable.cc
    src/fs/fs.hpp
    src/fs/fs.cc
    src/fs/file_reader.cc
    src/fs/file_reader2.cc
    src/fs/file_reader.h
    src/db/value_table.cc
    src/db/value_table.h
    src/db/database.cc
    src/db/database.h
    src/db/sst_manager.h
    src/db/sst_manager.cc
    src/cache.h
    src/cache.cc
)

set(IO_EXECUTOR_SRCS 
    src/perf_counter.cc
    src/async/io_executor.cc
    src/async/mailbox_impl.cc
    src/async/worker.cc
)

# executor_read_test.cc
add_executable(executor_read_test ${IO_EXECUTOR_SRCS} src/executor_read_test.cc)
target_link_libraries(executor_read_test uring tcmalloc)

# skiplist_executor_test — benchmarks executor + skiplist insert without full DB machinery
add_executable(skiplist_executor_test ${IO_EXECUTOR_SRCS} src/skiplist_executor_test.cc)
target_link_libraries(skiplist_executor_test uring tcmalloc atomic)

# memtable_perf_test — benchmarks executor + memtable::put() (adds rw_sync + value arenas)
add_executable(memtable_perf_test ${DB_SOURCES} ${IO_EXECUTOR_SRCS} src/memtable_perf_test.cc)
target_link_libraries(memtable_perf_test uring tcmalloc atomic)

# database_perf_test — full database write+read benchmark (no gtest)
add_executable(database_perf_test ${DB_SOURCES} ${IO_EXECUTOR_SRCS} src/database_perf_test.cc)
target_link_libraries(database_perf_test uring tcmalloc atomic)

# executor tests
add_executable(executor_test ${IO_EXECUTOR_SRCS} test/io_executor.spec.cc)
target_link_libraries(executor_test uring gtest gtest_main)

# index (merge) tests
add_executable(index_test ${DB_SOURCES} ${IO_EXECUTOR_SRCS} test/index.spec.cc)
target_link_libraries(index_test gtest gtest_main uring atomic)

# index2 tests
add_executable(index2_test ${DB_SOURCES} ${IO_EXECUTOR_SRCS} test/index2.spec.cc)
target_link_libraries(index2_test gtest gtest_main uring atomic)

# parallel flush tests
add_executable(index_test_parallel_flush ${DB_SOURCES} ${IO_EXECUTOR_SRCS} test/index_test.cc)
target_link_libraries(index_test_parallel_flush gtest gtest_main uring atomic)

# index (delete and merge) tests
add_executable(index_delete_test ${DB_SOURCES} ${IO_EXECUTOR_SRCS} test/index_delete.spec.cc)
target_link_libraries(index_delete_test gtest gtest_main uring atomic)

# paginated_view tests
add_executable(file_reader_test ${IO_EXECUTOR_SRCS} ${DB_SOURCES} test/file_reader.spec.cc)
target_link_libraries(file_reader_test uring gtest gtest_main atomic)

# paginated_view tests
add_executable(file_reader2_test ${IO_EXECUTOR_SRCS} ${DB_SOURCES} test/file_reader2.spec.cc)
target_link_libraries(file_reader2_test uring gtest gtest_main atomic)

# value_table tests
set(VALUE_TABLE_TEST_SRCS 
    src/types.h
    src/perf_counter.cc
    src/utils.cc
    src/utils.h
    src/fs/fs.hpp
    src/fs/fs.cc
    src/fs/file_reader.cc
    src/fs/file_reader2.cc
    src/fs/file_reader.h
    src/db/value_table.cc
    src/db/value_table.h
    src/cache.h
    src/cache.cc
)
add_executable(value_table_test ${IO_EXECUTOR_SRCS} ${VALUE_TABLE_TEST_SRCS} test/value_table.spec.cc)
target_link_libraries(value_table_test uring gtest gtest_main)

# worker tests
add_executable(worker_test ${IO_EXECUTOR_SRCS} ${DB_SOURCES} test/worker.spec.cc)
target_link_libraries(worker_test uring gtest gtest_main atomic)

# mpsc tests
add_executable(mpsc_test test/mpsc.spec.cc)
target_link_libraries(mpsc_test gtest gtest_main)

# cache tests
add_executable(cache_test ${IO_EXECUTOR_SRCS} ${DB_SOURCES} test/cache.spec.cc)
target_link_libraries(cache_test uring gtest gtest_main atomic)

# database tests
add_executable(database_test ${IO_EXECUTOR_SRCS} ${DB_SOURCES} test/database.spec.cc)
target_link_libraries(database_test uring gtest gtest_main atomic tcmalloc tbb)

# skiplist tests
add_executable(skiplist_test test/skiplist.spec.cc)
target_link_libraries(skiplist_test gtest gtest_main)

# # library TODO re-enable
# add_library(hedgedb STATIC ${IO_EXECUTOR_SRCS} ${DB_SOURCES} src/api/api.cc)
# target_link_libraries(hedgedb PRIVATE uring)
# target_include_directories(hedgedb PUBLIC $<INSTALL_INTERFACE:${PUBLIC_INCLUDES}> $<BUILD_INTERFACE:${PUBLIC_INCLUDES}>)

# # db api tests TODO re-enable
# add_executable(db_api_test test/api.spec.cc)
# target_link_libraries(db_api_test PUBLIC hedgedb gtest gtest_main)

# # installation rules TODO re-enable
# install(TARGETS hedgedb
#     ARCHIVE 
#     DESTINATION lib
# )

# btree tests
add_executable(btree_test test/btree.spec.cc)
target_link_libraries(btree_test gtest gtest_main tcmalloc atomic)

# varint tests
add_executable(varint_test test/varint.spec.cc)
target_link_libraries(varint_test gtest gtest_main)

# key tests
add_executable(key_test test/key.spec.cc)
target_link_libraries(key_test gtest gtest_main)

# block tests
add_executable(block_test src/db/block.cc test/block.spec.cc)
target_link_libraries(block_test gtest gtest_main)


# btree variant tests
# add_executable(btree_variant_test test/btree_variant.spec.cc)
# target_link_libraries(btree_variant_test gtest gtest_main tcmalloc atomic)

# page_aligned_buffer tests
add_executable(page_aligned_buffer_test test/page_aligned_buffer.spec.cc src/utils.cc)
target_link_libraries(page_aligned_buffer_test gtest gtest_main)

# error tests
add_executable(error_test test/error.spec.cc)
target_link_libraries(error_test gtest gtest_main)

# rolling_buffer tests
add_executable(rolling_buffer_test ${IO_EXECUTOR_SRCS} ${DB_SOURCES} test/rolling_buffer.spec.cc)
target_link_libraries(rolling_buffer_test uring gtest gtest_main atomic)

# single_buffer_arena_allocator tests
add_executable(single_buffer_arena_allocator_test test/single_buffer_arena_allocator.spec.cc)
target_link_libraries(single_buffer_arena_allocator_test gtest gtest_main)


# debug test
add_executable(index_merge ${DB_SOURCES} ${IO_EXECUTOR_SRCS} test/index_merge.spec.cc)
target_link_libraries(index_merge gtest gtest_main uring atomic)

# concurrent_skiplist tests
add_executable(concurrent_skiplist_test test/concurrent_skiplist.spec.cc)
target_link_libraries(concurrent_skiplist_test gtest gtest_main atomic tcmalloc)
