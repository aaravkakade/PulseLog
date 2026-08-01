add_library(pulselog_client STATIC src/client_context.cc src/producer.cc src/consumer.cc)
add_library(pulselog::client ALIAS pulselog_client)

target_include_directories(pulselog_client PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
target_link_libraries(
  pulselog_client
  PUBLIC pulselog::base pulselog::protocol pulselog::net pulselog::metadata pulselog::sanitizers
  PRIVATE pulselog::warnings pulselog::optimise)
set_target_properties(pulselog_client PROPERTIES CXX_STANDARD 20 CXX_STANDARD_REQUIRED ON
                                                 CXX_EXTENSIONS OFF)
