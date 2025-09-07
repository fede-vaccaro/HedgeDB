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
    set(CMAKE_CXX_FLAGS "-g -fno-omit-frame-pointer -Wall -Wextra -Wpedantic -Werror -fsanitize=address,undefined -Wno-redundant-move")
elseif (CMAKE_BUILD_TYPE MATCHES "Release")
    message (STATUS "Building in Release mode")
    set(CMAKE_CXX_FLAGS "-O3 -DNDEBUG")
endif()

include_directories(include src ${asio-standalone_SOURCE_DIR}/asio/include)

set(DB_SOURCES
    src/common.cc
    src/common.h
    src/index.cc
    src/index.h
    src/fs.hpp
    src/file_reader.cc
    src/file_reader.h
    src/value_table.cc
    src/value_table.h
    src/worker.cc
    src/worker.h
    src/database.cc
    src/database.h
)

## io_executor files
set(IO_EXECUTOR_SRCS
    src/io_executor.cc
    src/io_executor.h
    src/mailbox.h
    src/mailbox_impl.cc
    src/mailbox_impl.h
    src/task.h
)


# executor_read_test.cc
add_executable(executor_read_test test/executor_read_test.cc ${IO_EXECUTOR_SRCS})
target_link_libraries(executor_read_test uring)

# executor tests
add_executable(executor_test test/io_executor.spec.cc ${IO_EXECUTOR_SRCS})
target_link_libraries(executor_test uring gtest gtest_main)

# index (merge) tests
add_executable(index_test test/index.spec.cc ${DB_SOURCES} ${IO_EXECUTOR_SRCS})
target_link_libraries(index_test gtest gtest_main uring)

# index (delete and merge) tests
add_executable(index_delete_test test/index_delete.spec.cc ${DB_SOURCES} ${IO_EXECUTOR_SRCS})
target_link_libraries(index_delete_test gtest gtest_main uring)


# paginated_view tests
add_executable(file_reader_test test/file_reader.spec.cc ${IO_EXECUTOR_SRCS} ${DB_SOURCES})
target_link_libraries(file_reader_test uring gtest gtest_main)

# value_table tests
add_executable(value_table_test test/value_table.spec.cc ${IO_EXECUTOR_SRCS} ${DB_SOURCES})
target_link_libraries(value_table_test uring gtest gtest_main)

# worker tests
add_executable(worker_test test/worker.spec.cc ${IO_EXECUTOR_SRCS} ${DB_SOURCES})
target_link_libraries(worker_test uring gtest gtest_main)

# database tests
add_executable(database_test test/database.spec.cc ${IO_EXECUTOR_SRCS} ${DB_SOURCES})
target_link_libraries(database_test uring gtest gtest_main)
