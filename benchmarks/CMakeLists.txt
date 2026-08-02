# Google Benchmark micro-benchmarks. Each one is paired with a claim made in
# the source or the docs; see docs/PERFORMANCE_RESULTS.md for the numbers.
function(pulselog_add_benchmark NAME)
  cmake_parse_arguments(ARG "" "" "SOURCES;DEPS" ${ARGN})
  add_executable(${NAME} ${ARG_SOURCES})
  target_link_libraries(${NAME} PRIVATE ${ARG_DEPS} benchmark::benchmark
                                        pulselog::warnings pulselog::optimise
                                        pulselog::sanitizers)
  set_target_properties(${NAME} PROPERTIES CXX_STANDARD 20 CXX_STANDARD_REQUIRED ON)
endfunction()

pulselog_add_benchmark(bench_core SOURCES bench_core.cc
                       DEPS pulselog::protocol pulselog::concurrency)
pulselog_add_benchmark(bench_storage SOURCES bench_storage.cc DEPS pulselog::storage)
