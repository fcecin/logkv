cmake_minimum_required(VERSION 3.14)
project(logkv)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# -------------------------------------
# This block should be replaced with:
#
# find_package(Boost CONFIG REQUIRED)
#
# ...at some point.
if(POLICY CMP0167)
  cmake_policy(SET CMP0167 OLD)
endif()
find_package(Boost REQUIRED)
# -------------------------------------

include(FetchContent)

set(CRC32C_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(CRC32C_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(CRC32C_USE_GLOG OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
  crc32c
  GIT_REPOSITORY https://github.com/google/crc32c.git
  GIT_TAG        main
)
FetchContent_MakeAvailable(crc32c)

add_library(logkv INTERFACE)

target_include_directories(logkv INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
)

target_link_libraries(logkv INTERFACE crc32c Boost::headers)
