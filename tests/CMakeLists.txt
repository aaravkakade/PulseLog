include(GoogleTest)

# pulselog_add_test(<name> SOURCES ... DEPS ...)
function(pulselog_add_test NAME)
  cmake_parse_arguments(ARG "" "" "SOURCES;DEPS" ${ARGN})
  add_executable(${NAME} ${ARG_SOURCES})
  target_link_libraries(${NAME} PRIVATE ${ARG_DEPS} GTest::gtest GTest::gtest_main
                                        pulselog::warnings pulselog::sanitizers)
  target_include_directories(${NAME} PRIVATE "${PROJECT_SOURCE_DIR}/tests")
  set_target_properties(${NAME} PROPERTIES CXX_STANDARD 20 CXX_STANDARD_REQUIRED ON)
  gtest_discover_tests(${NAME} DISCOVERY_MODE PRE_TEST PROPERTIES TIMEOUT 120)
endfunction()

add_subdirectory(unit)
add_subdirectory(integration)
