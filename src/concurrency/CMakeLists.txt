pulselog_add_module(
  concurrency
  SOURCES
  thread_util.cc
  DEPS
  pulselog::base
  Threads::Threads)
