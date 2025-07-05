cmake_minimum_required(VERSION 3.10) 

project(HedgehogDB CXX)

include(FetchContent)
FetchContent_Declare(
        asio-standalone
        URL https://github.com/chriskohlhoff/asio/archive/refs/heads/master.zip
)

FetchContent_MakeAvailable(asio-standalone)

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

set(SOURCES
    index.cc
    index.h
    fs.hpp
)

add_executable(index_test_w test_index_w.cc ${SOURCES})
add_executable(index_test_r from_disk_test.cc ${SOURCES})
add_executable(test_odirect test_odirect.cc)

add_executable(asio_test asio_test_2.cc)
target_compile_definitions(asio_test PRIVATE ASIO_HAS_FILE ASIO_HAS_IO_URING ASIO_HAS_IO_URING_AS_DEFAULT ASIO_DISABLE_EPOLL)

add_executable(uring_test uring_reader.cc)
target_link_libraries(uring_test uring)

target_link_libraries(
    asio_test
    uring
)

## io_executor files
set(IO_EXECUTOR_SRCS io_executor.cc mailbox_impl.cc)

# executor_test.cc
add_executable(executor_read_test ${IO_EXECUTOR_SRCS} executor_read_test.cc)
target_link_libraries(executor_read_test uring)

# executor tests

add_executable(executor_test ${IO_EXECUTOR_SRCS} test/io_executor.spec.cc)
target_link_libraries(executor_test uring gtest gtest_main)