cmake_minimum_required(VERSION 3.20)
project(rocksdb_bench CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

if(CMAKE_BUILD_TYPE MATCHES "Release")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2")
endif()

include(FetchContent)

FetchContent_Declare(
    xxhashct
    GIT_REPOSITORY https://github.com/ekpyron/xxhashct.git
)
FetchContent_MakeAvailable(xxhashct)

# RocksDB: static library only, no tools/tests/benchmarks
set(WITH_TESTS           OFF CACHE BOOL "" FORCE)
set(WITH_BENCHMARK_TOOLS OFF CACHE BOOL "" FORCE)
set(WITH_TOOLS           OFF CACHE BOOL "" FORCE)
set(WITH_CORE_TOOLS      OFF CACHE BOOL "" FORCE)
set(WITH_TRACE_TOOLS     OFF CACHE BOOL "" FORCE)
set(WITH_GFLAGS          OFF CACHE BOOL "" FORCE)
set(ROCKSDB_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(FAIL_ON_WARNINGS     OFF CACHE BOOL "" FORCE)

if(NOT ROCKSDB_VERSION)
    message(FATAL_ERROR "Missing ROCKSDB_VERSION param")
else()
    message("Found RocksDB v" ${ROCKSDB_VERSION})
endif()

set(ROCKSDB_DIR rocksdb-${ROCKSDB_VERSION})

add_subdirectory(${ROCKSDB_DIR})

add_executable(benchtool benchtool.cc)
target_include_directories(benchtool PRIVATE
    ${xxhashct_SOURCE_DIR}
    ${ROCKSDB_DIR}/include
)
target_link_libraries(benchtool PRIVATE rocksdb jemalloc)
