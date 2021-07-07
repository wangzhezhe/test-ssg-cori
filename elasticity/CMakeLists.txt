cmake_minimum_required (VERSION 3.10)
project (SSG_TEST_ELASTICITY)

set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH}
     "${CMAKE_CURRENT_SOURCE_DIR}/cmake")


add_definitions(-DUSE_GNI)
find_package(DRC REQUIRED)
include_directories(${DRC_INCLUDE_DIRS})
message ("use cray DRC")
message ("include drc" ${DRC_INCLUDE_DIRS})
message ("link drc lib" ${DRC_LIBRARIES})


find_package (thallium REQUIRED)
find_package (PkgConfig REQUIRED)

# search for mona by pkg
pkg_check_modules (MONA REQUIRED IMPORTED_TARGET mona)
# search for ssg by pkg
pkg_check_modules (SSG REQUIRED IMPORTED_TARGET ssg)
# search for TCLAP by pkg
pkg_check_modules (TCLAP REQUIRED IMPORTED_TARGET tclap)

# search for spdlog
find_package(spdlog REQUIRED)

find_package(MPI REQUIRED)
include_directories(${MPI_C_INCLUDE_PATH})


# set source files
add_executable (dynamic-sim ${CMAKE_CURRENT_SOURCE_DIR}/sim.cpp)
target_link_libraries (dynamic-sim MPI::MPI_C ${DRC_LIBRARIES} PkgConfig::MONA thallium PkgConfig::SSG)

add_executable (dynamic-sim-leave ${CMAKE_CURRENT_SOURCE_DIR}/sim_leave.cpp)
target_link_libraries (dynamic-sim-leave MPI::MPI_C ${DRC_LIBRARIES} PkgConfig::MONA thallium PkgConfig::SSG)