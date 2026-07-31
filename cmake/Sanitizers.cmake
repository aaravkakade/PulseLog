# Sanitizer wiring. Selected with -DPULSELOG_SANITIZER=<address|thread|undefined|address+undefined>.
#
# ThreadSanitizer is mutually exclusive with AddressSanitizer, so the CI matrix
# runs them as separate build directories rather than trying to combine them.

add_library(pulselog_sanitizers INTERFACE)
add_library(pulselog::sanitizers ALIAS pulselog_sanitizers)

if(PULSELOG_SANITIZER STREQUAL "")
  return()
endif()

set(_san_flags "")

if(PULSELOG_SANITIZER MATCHES "address")
  list(APPEND _san_flags address)
endif()
if(PULSELOG_SANITIZER MATCHES "undefined")
  list(APPEND _san_flags undefined)
endif()
if(PULSELOG_SANITIZER MATCHES "thread")
  if(_san_flags)
    message(FATAL_ERROR "ThreadSanitizer cannot be combined with ${_san_flags}")
  endif()
  list(APPEND _san_flags thread)
endif()

if(NOT _san_flags)
  message(FATAL_ERROR "Unknown PULSELOG_SANITIZER value: ${PULSELOG_SANITIZER}")
endif()

list(JOIN _san_flags "," _san_list)
message(STATUS "Sanitizers enabled: ${_san_list}")

target_compile_options(
  pulselog_sanitizers INTERFACE -fsanitize=${_san_list} -fno-omit-frame-pointer
                                -fno-optimize-sibling-calls -g)
target_link_options(pulselog_sanitizers INTERFACE -fsanitize=${_san_list})

if(PULSELOG_SANITIZER MATCHES "undefined")
  target_compile_options(pulselog_sanitizers INTERFACE -fno-sanitize-recover=undefined)
endif()
