cmake_minimum_required (VERSION 3.10)
project (SSG_TEST)

set (CMAKE_MODULE_PATH
     "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

find_package (PkgConfig REQUIRED)
find_package (MPI)

pkg_check_modules (SSG REQUIRED IMPORTED_TARGET ssg)
find_package (thallium REQUIRED)
find_package (DRC REQUIRED)
include_directories (${DRC_INCLUDE_DIRS})

message(STATUS "DRC include dirs: ${DRC_INCLUDE_DIRS}")
message(STATUS "DRC libraries: ${DRC_LIBRARIES}")

add_executable(test-server test-server.cpp)
target_link_libraries(test-server thallium MPI::MPI_C PkgConfig::SSG ${DRC_LIBRARIES})

add_executable(test-client test-client.cpp)
target_link_libraries(test-client thallium MPI::MPI_C PkgConfig::SSG ${DRC_LIBRARIES})
