include(PulseLogModule)

add_subdirectory(base)
add_subdirectory(concurrency)
add_subdirectory(protocol)
add_subdirectory(storage)
add_subdirectory(metrics)
add_subdirectory(net)
add_subdirectory(metadata)
add_subdirectory(consumer)
add_subdirectory(broker)
add_subdirectory(replication)

# Umbrella target so applications can link one name.
add_library(pulselog_all INTERFACE)
add_library(pulselog::all ALIAS pulselog_all)
target_link_libraries(pulselog_all INTERFACE pulselog::base pulselog::concurrency pulselog::protocol pulselog::storage pulselog::metrics pulselog::net pulselog::metadata pulselog::consumer
                                  pulselog::replication pulselog::broker)
