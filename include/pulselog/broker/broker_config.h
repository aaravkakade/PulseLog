// Broker configuration, assembled from a properties file, the environment and
// command-line flags.
//
// Every key has a default that produces a working single-broker instance, so
// `pulselog-broker` with no arguments starts and serves. Anything that would
// silently change a durability or ordering guarantee has no silent default:
// it is either explicit or the broker refuses to start.
#ifndef PULSELOG_BROKER_BROKER_CONFIG_H_
#define PULSELOG_BROKER_BROKER_CONFIG_H_

#include <cstdint>
#include <string>
#include <vector>

#include "pulselog/base/config.h"
#include "pulselog/base/logging.h"
#include "pulselog/base/status.h"
#include "pulselog/base/types.h"
#include "pulselog/net/socket.h"
#include "pulselog/storage/partition_log.h"

namespace pulselog::broker {

struct BrokerConfig {
  // --- identity -------------------------------------------------------------
  BrokerId broker_id{0};
  net::Endpoint listen{"0.0.0.0", 9092};
  // Address other brokers and clients should use to reach this broker. Differs
  // from `listen` inside Docker, where the bind address is 0.0.0.0 but peers
  // must use the service name.
  std::string advertised_host = "127.0.0.1";
  std::uint16_t advertised_port = 9092;
  std::vector<std::string> cluster_brokers;  // "1@host:port" entries.
  std::string data_dir = "./pulselog-data";

  // --- networking -----------------------------------------------------------
  std::size_t io_threads = 2;
  std::size_t max_connections = 4096;
  std::uint32_t max_frame_bytes = 64U * 1024 * 1024;
  std::int64_t connection_idle_timeout_ms = 0;  // 0 disables.
  std::size_t output_high_water_bytes = 4U * 1024 * 1024;
  std::size_t output_max_bytes = 64U * 1024 * 1024;

  // --- workers --------------------------------------------------------------
  std::size_t worker_threads = 2;
  std::size_t worker_queue_capacity = 4096;
  bool pin_workers = false;

  // --- storage --------------------------------------------------------------
  std::int64_t segment_bytes = 128LL * 1024 * 1024;
  std::int64_t segment_ms = -1;
  std::int64_t index_interval_bytes = 4096;
  std::int64_t retention_bytes = -1;
  std::int64_t retention_ms = -1;
  std::int64_t min_free_disk_bytes = 64LL * 1024 * 1024;
  bool preallocate_segments = true;
  storage::WriteMode write_mode = storage::WriteMode::kWrite;
  storage::FlushPolicy flush;
  std::int64_t flusher_interval_ms = 20;

  // --- topics ---------------------------------------------------------------
  bool auto_create_topics = true;
  std::int32_t default_partitions = 1;
  std::int16_t default_replication_factor = 1;

  // --- replication ----------------------------------------------------------
  std::int64_t replication_interval_ms = 2;
  std::int64_t replication_timeout_ms = 5000;
  std::uint32_t replication_max_bytes = 1U << 20U;
  // A follower more than this far behind leaves the in-sync replica set.
  std::int64_t replica_lag_max_ms = 10'000;

  // --- consumer groups ------------------------------------------------------
  std::int64_t group_session_timeout_ms = 10'000;
  std::int64_t group_rebalance_delay_ms = 200;

  // --- observability --------------------------------------------------------
  bool metrics_enabled = true;
  std::string metrics_host = "0.0.0.0";
  std::uint16_t metrics_port = 9644;
  std::int64_t metrics_sample_interval_ms = 1000;
  LogLevel log_level = LogLevel::kInfo;
  std::string log_file;

  // --- maintenance ----------------------------------------------------------
  std::int64_t retention_check_interval_ms = 30'000;

  // Builds a config from a store, validating every value. Returns
  // INVALID_ARGUMENT naming the offending key rather than falling back to a
  // default, so a typo cannot silently change behaviour.
  [[nodiscard]] static Result<BrokerConfig> FromStore(const ConfigStore& store);

  // Rejects internally inconsistent combinations.
  [[nodiscard]] Status Validate() const;

  // Multi-line `key=value` rendering for the start-up log and for benchmark
  // metadata.
  [[nodiscard]] std::string Describe() const;

  [[nodiscard]] storage::LogOptions LogOptionsFor(const std::string& topic,
                                                  PartitionIndex partition,
                                                  std::int64_t retention_override,
                                                  std::int64_t segment_override) const;
};

}  // namespace pulselog::broker

#endif  // PULSELOG_BROKER_BROKER_CONFIG_H_
