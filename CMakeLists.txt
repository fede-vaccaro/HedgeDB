cmake_minimum_required(VERSION 3.10) 
project(hedgeDB CXX)

enable_testing()

include(FetchContent)
FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)

set(INSTALL_GTEST OFF)
FetchContent_MakeAvailable(googletest)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

if (CMAKE_BUILD_TYPE MATCHES "Debug")
    message (STATUS "Building in Debug mode")
    set(CMAKE_CXX_FLAGS "-O1 -g -fno-omit-frame-pointer -Wall -Wextra -Wpedantic -Werror")
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


set(DB_SOURCES
    src/types.h
    src/utils.cc
    src/utils.h
    src/db/sorted_index.cc
    src/db/sorted_index.h
    src/db/index_ops.cc
    src/db/index_ops.h
    src/db/mem_index.h
    src/db/mem_index.cc
    src/fs/fs.hpp
    src/fs/file_reader.cc
    src/fs/file_reader.h
    src/db/value_table.cc
    src/db/value_table.h
    src/db/database.cc
    src/db/database.h
)

set(IO_EXECUTOR_SRCS 
    src/async/io_executor.cc
    src/async/mailbox_impl.cc
    src/async/worker.cc
)

# executor_read_test.cc
add_executable(executor_read_test ${IO_EXECUTOR_SRCS} src/executor_read_test.cc)
target_link_libraries(executor_read_test uring tcmalloc)

# executor tests
add_executable(executor_test ${IO_EXECUTOR_SRCS} test/io_executor.spec.cc)
target_link_libraries(executor_test uring gtest gtest_main)

# index (merge) tests
add_executable(index_test ${DB_SOURCES} ${IO_EXECUTOR_SRCS} test/index.spec.cc)
target_link_libraries(index_test gtest gtest_main uring)

# index (delete and merge) tests
add_executable(index_delete_test ${DB_SOURCES} ${IO_EXECUTOR_SRCS} test/index_delete.spec.cc)
target_link_libraries(index_delete_test gtest gtest_main uring)


# paginated_view tests
add_executable(file_reader_test ${IO_EXECUTOR_SRCS} ${DB_SOURCES} test/file_reader.spec.cc)
target_link_libraries(file_reader_test uring gtest gtest_main)

# value_table tests
add_executable(value_table_test ${IO_EXECUTOR_SRCS} ${DB_SOURCES} test/value_table.spec.cc)
target_link_libraries(value_table_test uring gtest gtest_main)

# worker tests
add_executable(worker_test ${IO_EXECUTOR_SRCS} ${DB_SOURCES} test/worker.spec.cc)
target_link_libraries(worker_test uring gtest gtest_main)

# database tests
add_executable(database_test ${IO_EXECUTOR_SRCS} ${DB_SOURCES} test/database.spec.cc)
target_link_libraries(database_test uring gtest gtest_main tcmalloc)

# # library
# add_library(hedgedb STATIC ${IO_EXECUTOR_SRCS} ${DB_SOURCES} src/api/api.cc)
# target_link_libraries(hedgedb PRIVATE uring)
# target_include_directories(hedgedb PUBLIC $<INSTALL_INTERFACE:${PUBLIC_INCLUDES}> $<BUILD_INTERFACE:${PUBLIC_INCLUDES}>)

# # db api tests
# add_executable(db_api_test test/api.spec.cc)
# target_link_libraries(db_api_test PUBLIC hedgedb gtest gtest_main)

# # installation rules
# install(TARGETS hedgedb
#     ARCHIVE 
#     DESTINATION lib
# )
# install(DIRECTORY include/ DESTINATION include)
