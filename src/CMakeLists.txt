include(PulseLogModule)

add_subdirectory(base)
add_subdirectory(concurrency)
add_subdirectory(protocol)

# Umbrella target so applications can link one name.
add_library(pulselog_all INTERFACE)
add_library(pulselog::all ALIAS pulselog_all)
target_link_libraries(pulselog_all INTERFACE pulselog::base pulselog::concurrency pulselog::protocol)
