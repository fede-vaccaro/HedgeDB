project(hedgedb_example CXX)
cmake_minimum_required(VERSION 3.10)

set(CMAKE_CXX_STANDARD 20)

set(CMAKE_CXX_FLAGS "-O3")

# Adjust path if example/ is not at top-level
set(HEDGEDB_DIR "${CMAKE_CURRENT_LIST_DIR}/../hedgedb")

# build main.cc
add_executable(example main.cc)
target_include_directories(example PRIVATE ${HEDGEDB_DIR}/include)
target_link_libraries(example PRIVATE ${HEDGEDB_DIR}/lib/libhedgedb.a uring)
