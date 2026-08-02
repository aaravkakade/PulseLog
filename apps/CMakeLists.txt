function(pulselog_add_app NAME)
  cmake_parse_arguments(ARG "" "" "SOURCES;DEPS" ${ARGN})
  add_executable(${NAME} ${ARG_SOURCES})
  target_link_libraries(${NAME} PRIVATE ${ARG_DEPS} pulselog::warnings pulselog::optimise
                                        pulselog::sanitizers)
  set_target_properties(${NAME} PROPERTIES CXX_STANDARD 20 CXX_STANDARD_REQUIRED ON
                                           CXX_EXTENSIONS OFF)
endfunction()

pulselog_add_app(pulselog-broker SOURCES broker/main.cc DEPS pulselog::broker)
pulselog_add_app(pulselog-cli SOURCES cli/main.cc DEPS pulselog::client)
pulselog_add_app(pulselog-bench SOURCES benchmark/main.cc DEPS pulselog::client pulselog::metrics)
