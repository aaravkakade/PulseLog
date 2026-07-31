# Helper for declaring one PulseLog module as a static library.
#
# Modules are separate CMake targets rather than one blob so the dependency
# graph is enforced by the build system: `storage` cannot accidentally start
# using `net`, and `protocol` stays free of both. The layering is
#
#   base <- concurrency <- protocol <- storage <- metadata
#                                  \      \          \
#                                   +----- net       replication
#                                            \          /
#                                             broker <-+
#
# Usage:
#   pulselog_add_module(storage
#     SOURCES segment.cc partition_log.cc
#     DEPS pulselog::base pulselog::protocol)

function(pulselog_add_module NAME)
  cmake_parse_arguments(ARG "" "" "SOURCES;DEPS;PRIVATE_DEPS" ${ARGN})

  set(target "pulselog_${NAME}")

  if(ARG_SOURCES)
    add_library(${target} STATIC ${ARG_SOURCES})
  else()
    add_library(${target} INTERFACE)
  endif()

  add_library(pulselog::${NAME} ALIAS ${target})

  if(ARG_SOURCES)
    target_include_directories(${target} PUBLIC "${PROJECT_SOURCE_DIR}/include")
    target_link_libraries(
      ${target}
      PUBLIC ${ARG_DEPS}
      PRIVATE ${ARG_PRIVATE_DEPS} pulselog::warnings pulselog::optimise pulselog::sanitizers)
    # Sanitizer flags must also reach consumers so the runtime is linked in.
    target_link_libraries(${target} PUBLIC pulselog::sanitizers)
    set_target_properties(${target} PROPERTIES CXX_STANDARD 20 CXX_STANDARD_REQUIRED ON
                                               CXX_EXTENSIONS OFF)
  else()
    target_include_directories(${target} INTERFACE "${PROJECT_SOURCE_DIR}/include")
    target_link_libraries(${target} INTERFACE ${ARG_DEPS} pulselog::sanitizers)
  endif()
endfunction()
