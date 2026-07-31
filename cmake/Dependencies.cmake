# Third-party dependencies.
#
# Only test/benchmark scaffolding is fetched. The engine itself (protocol,
# storage, networking, replication, histograms) has no third-party runtime
# dependencies -- everything on the data path is implemented in this repo.
#
# Set -DPULSELOG_OFFLINE=ON to skip all network fetches; tests and benchmarks
# are then disabled instead of failing to configure.

include(FetchContent)

option(PULSELOG_OFFLINE "Do not fetch third-party dependencies" OFF)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

if(PULSELOG_OFFLINE)
  if(PULSELOG_BUILD_TESTS OR PULSELOG_BUILD_BENCHMARKS)
    message(STATUS "PULSELOG_OFFLINE=ON: disabling tests and benchmarks")
    set(PULSELOG_BUILD_TESTS OFF)
    set(PULSELOG_BUILD_BENCHMARKS OFF)
  endif()
  return()
endif()

set(FETCHCONTENT_QUIET OFF)

if(PULSELOG_BUILD_TESTS)
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.15.2
    GIT_SHALLOW TRUE
    SYSTEM)
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
  set(BUILD_GMOCK ON CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)
endif()

if(PULSELOG_BUILD_BENCHMARKS)
  FetchContent_Declare(
    benchmark
    GIT_REPOSITORY https://github.com/google/benchmark.git
    GIT_TAG v1.9.0
    GIT_SHALLOW TRUE
    SYSTEM)
  set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
  set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
  set(BENCHMARK_ENABLE_WERROR OFF CACHE BOOL "" FORCE)
  set(BENCHMARK_INSTALL_DOCS OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(benchmark)
endif()
