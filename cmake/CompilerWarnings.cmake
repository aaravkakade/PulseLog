# Strict warning configuration shared by every PulseLog target.
#
# Warnings are opt-in-to-error via -DPULSELOG_WERROR=ON so that a local build on
# a newer compiler does not break while CI still enforces a clean build.

add_library(pulselog_warnings INTERFACE)
add_library(pulselog::warnings ALIAS pulselog_warnings)

set(_pulselog_common_warnings
    -Wall
    -Wextra
    -Wpedantic
    -Wshadow
    -Wnon-virtual-dtor
    -Wold-style-cast
    -Wcast-align
    -Wunused
    -Woverloaded-virtual
    -Wconversion
    -Wsign-conversion
    -Wdouble-promotion
    -Wformat=2
    -Wimplicit-fallthrough
    -Wextra-semi
    -Wnull-dereference)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU")
  list(APPEND _pulselog_common_warnings -Wduplicated-cond -Wduplicated-branches -Wlogical-op
       -Wuseless-cast)
endif()

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  list(APPEND _pulselog_common_warnings -Wno-gnu-zero-variadic-macro-arguments)
endif()

target_compile_options(pulselog_warnings INTERFACE ${_pulselog_common_warnings})

if(PULSELOG_WERROR)
  target_compile_options(pulselog_warnings INTERFACE -Werror)
endif()

# Optimisation flags used by the performance-sensitive targets. Kept separate
# from CMAKE_CXX_FLAGS_* so benchmark builds can report exactly what was used.
add_library(pulselog_optimise INTERFACE)
add_library(pulselog::optimise ALIAS pulselog_optimise)

target_compile_options(
  pulselog_optimise INTERFACE $<$<CONFIG:Release,RelWithDebInfo>:-fno-omit-frame-pointer>)

if(PULSELOG_ENABLE_LTO)
  include(CheckIPOSupported)
  check_ipo_supported(RESULT _ipo_ok OUTPUT _ipo_msg)
  if(_ipo_ok)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELEASE ON)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELWITHDEBINFO ON)
  else()
    message(WARNING "LTO requested but unsupported: ${_ipo_msg}")
  endif()
endif()
