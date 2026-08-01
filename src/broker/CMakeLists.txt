# The broker is split into two targets to break a dependency cycle: the
# replication module needs PartitionManager, and the full broker needs the
# replicator. `broker_core` holds the partition-side pieces that replication
# depends on; `broker` holds the lifecycle and request handling.
pulselog_add_module(
  broker_core
  SOURCES
  broker_config.cc
  partition_replica.cc
  partition_manager.cc
  worker.cc
  DEPS
  pulselog::base
  pulselog::concurrency
  pulselog::protocol
  pulselog::storage
  pulselog::metadata
  pulselog::net)

pulselog_add_module(
  broker
  SOURCES
  broker.cc
  request_handler.cc
  DEPS
  pulselog::broker_core
  pulselog::consumer
  pulselog::replication
  pulselog::metrics
  pulselog::net)
