cmake_minimum_required(VERSION 3.10) 

project(hedgeDB CXX)

enable_testing()

include(FetchContent)
FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)

FetchContent_MakeAvailable(googletest)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

if (CMAKE_BUILD_TYPE MATCHES "Debug")
    message (STATUS "Building in Debug mode")
    set(CMAKE_CXX_FLAGS "-g -fno-omit-frame-pointer")
elseif (CMAKE_BUILD_TYPE MATCHES "Release")
    message (STATUS "Building in Release mode")
    set(CMAKE_CXX_FLAGS "-O2")
endif()

include_directories(include ${asio-standalone_SOURCE_DIR}/asio/include)

set(DB_SOURCES
    common.cc
    common.h
    index.cc
    index.h
    fs.hpp
    file_reader.cc
    file_reader.h
    value_table.cc
    value_table.h
    worker.cc
    worker.h
    database.cc
    database.h
)

## io_executor files
set(IO_EXECUTOR_SRCS 
    io_executor.cc
    io_executor.h 
    mailbox.h
    mailbox_impl.cc
    mailbox_impl.h
    task.h
    generator.h
)

# executor_test.cc
add_executable(executor_read_test ${IO_EXECUTOR_SRCS} executor_read_test.cc)
target_link_libraries(executor_read_test uring)

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
target_link_libraries(database_test uring gtest gtest_main)
