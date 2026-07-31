cmake_minimum_required(VERSION 3.20)

project(
  PulseLog
  VERSION 0.1.0
  DESCRIPTION "A low-latency distributed event-streaming engine"
  LANGUAGES CXX)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE
      RelWithDebInfo
      CACHE STRING "Build type" FORCE)
endif()

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------
option(PULSELOG_BUILD_TESTS "Build unit/integration/stress tests" ON)
option(PULSELOG_BUILD_BENCHMARKS "Build the Google Benchmark micro-benchmarks" ON)
option(PULSELOG_BUILD_APPS "Build broker/cli/benchmark executables" ON)
option(PULSELOG_WERROR "Treat compiler warnings as errors" OFF)
option(PULSELOG_ENABLE_LTO "Enable interprocedural optimisation for release builds" OFF)
option(PULSELOG_USE_IO_URING "Use the io_uring event-loop backend when available" OFF)
set(PULSELOG_SANITIZER
    ""
    CACHE STRING "Sanitizer to enable: address, thread, undefined, address+undefined")

include(CompilerWarnings)
include(Sanitizers)
include(Dependencies)

# ---------------------------------------------------------------------------
# Platform detection
# ---------------------------------------------------------------------------
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  set(PULSELOG_PLATFORM_LINUX ON)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  set(PULSELOG_PLATFORM_DARWIN ON)
else()
  message(FATAL_ERROR "PulseLog supports Linux and macOS only (got ${CMAKE_SYSTEM_NAME}).")
endif()

if(PULSELOG_USE_IO_URING AND NOT PULSELOG_PLATFORM_LINUX)
  message(WARNING "io_uring requested on a non-Linux platform; falling back to kqueue.")
  set(PULSELOG_USE_IO_URING OFF)
endif()

# ---------------------------------------------------------------------------
# Core library
# ---------------------------------------------------------------------------
add_subdirectory(src)
add_subdirectory(clients/cpp)

if(PULSELOG_BUILD_APPS)
  add_subdirectory(apps)
endif()

if(PULSELOG_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

if(PULSELOG_BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
message(STATUS "")
message(STATUS "PulseLog ${PROJECT_VERSION} configuration")
message(STATUS "  build type      : ${CMAKE_BUILD_TYPE}")
message(STATUS "  compiler        : ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
message(STATUS "  platform        : ${CMAKE_SYSTEM_NAME} ${CMAKE_SYSTEM_PROCESSOR}")
message(STATUS "  event loop      : ${PULSELOG_POLLER_BACKEND}")
message(STATUS "  sanitizer       : ${PULSELOG_SANITIZER}")
message(STATUS "  warnings->errors: ${PULSELOG_WERROR}")
message(STATUS "  tests           : ${PULSELOG_BUILD_TESTS}")
message(STATUS "  benchmarks      : ${PULSELOG_BUILD_BENCHMARKS}")
message(STATUS "")
