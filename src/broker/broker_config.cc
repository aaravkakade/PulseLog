#include "pulselog/broker/broker_config.h"

#include <sstream>

#include "pulselog/concurrency/thread_util.h"

namespace pulselog::broker {
namespace {

// Reads an integer key, propagating a parse failure instead of silently
// defaulting. Every getter below follows this pattern.
#define PL_CONFIG_INT(target, key, fallback)                 \
  do {                                                       \
    auto value = store.GetInt(key, fallback);                \
    if (!value.ok()) return value.status();                  \
    (target) = static_cast<decltype(target)>(value.value()); \
  } while (false)

#define PL_CONFIG_BYTES(target, key, fallback)               \
  do {                                                       \
    auto value = store.GetBytes(key, fallback);              \
    if (!value.ok()) return value.status();                  \
    (target) = static_cast<decltype(target)>(value.value()); \
  } while (false)

#define PL_CONFIG_MS(target, key, fallback)                  \
  do {                                                       \
    auto value = store.GetDurationMs(key, fallback);         \
    if (!value.ok()) return value.status();                  \
    (target) = static_cast<decltype(target)>(value.value()); \
  } while (false)

#define PL_CONFIG_BOOL(target, key, fallback)  \
  do {                                         \
    auto value = store.GetBool(key, fallback); \
    if (!value.ok()) return value.status();    \
    (target) = value.value();                  \
  } while (false)

}  // namespace

Result<BrokerConfig> BrokerConfig::FromStore(const ConfigStore& store) {
  BrokerConfig config;

  std::int64_t broker_id = 0;
  PL_CONFIG_INT(broker_id, "broker.id", 0);
  config.broker_id = BrokerId{static_cast<std::int32_t>(broker_id)};

  const std::string listen_text = store.GetString("net.listen", "0.0.0.0:9092");
  PL_ASSIGN_OR_RETURN(config.listen, net::Endpoint::Parse(listen_text));

  config.advertised_host = store.GetString("net.advertised.host", "127.0.0.1");
  std::int64_t advertised_port = config.listen.port;
  PL_CONFIG_INT(advertised_port, "net.advertised.port", config.listen.port);
  config.advertised_port = static_cast<std::uint16_t>(advertised_port);

  config.cluster_brokers = store.GetList("cluster.brokers");
  config.data_dir = store.GetString("broker.data.dir", "./pulselog-data");

  PL_CONFIG_INT(config.io_threads, "net.io.threads", 2);
  PL_CONFIG_INT(config.max_connections, "net.max.connections", 4096);
  PL_CONFIG_BYTES(config.max_frame_bytes, "net.max.frame.bytes", 64LL * 1024 * 1024);
  PL_CONFIG_MS(config.connection_idle_timeout_ms, "net.idle.timeout", 0);
  PL_CONFIG_BYTES(config.output_high_water_bytes, "net.output.high.water.bytes", 4LL * 1024 * 1024);
  PL_CONFIG_BYTES(config.output_max_bytes, "net.output.max.bytes", 64LL * 1024 * 1024);

  PL_CONFIG_INT(config.worker_threads, "broker.worker.threads", 2);
  PL_CONFIG_INT(config.worker_queue_capacity, "broker.worker.queue.capacity", 4096);
  PL_CONFIG_BOOL(config.pin_workers, "broker.pin.workers", false);

  PL_CONFIG_BYTES(config.segment_bytes, "storage.segment.bytes", 128LL * 1024 * 1024);
  PL_CONFIG_MS(config.segment_ms, "storage.segment.ms", -1);
  PL_CONFIG_BYTES(config.index_interval_bytes, "storage.index.interval.bytes", 4096);
  PL_CONFIG_BYTES(config.retention_bytes, "storage.retention.bytes", -1);
  PL_CONFIG_MS(config.retention_ms, "storage.retention.ms", -1);
  PL_CONFIG_BYTES(config.min_free_disk_bytes, "storage.min.free.disk.bytes", 64LL * 1024 * 1024);
  PL_CONFIG_BOOL(config.preallocate_segments, "storage.preallocate", true);

  const std::string write_mode = store.GetString("storage.write.mode", "write");
  if (!storage::ParseWriteMode(write_mode, config.write_mode)) {
    return InvalidArgument("storage.write.mode must be write, writev or mmap; got '" + write_mode +
                           "'");
  }

  const std::string sync_mode = store.GetString("storage.fsync.mode", "full");
  if (!storage::ParseSyncMode(sync_mode, config.sync_mode)) {
    return InvalidArgument("storage.fsync.mode must be full or data; got '" + sync_mode + "'");
  }

  PL_CONFIG_BOOL(config.flush.sync_on_append, "storage.flush.sync.on.append", false);
  PL_CONFIG_MS(config.flush.interval_ms, "storage.flush.interval", 200);
  PL_CONFIG_BYTES(config.flush.max_unflushed_bytes, "storage.flush.max.bytes", 4LL * 1024 * 1024);
  PL_CONFIG_INT(config.flush.max_unflushed_records, "storage.flush.max.records", 10'000);
  PL_CONFIG_MS(config.flusher_interval_ms, "storage.flusher.interval", 20);

  PL_CONFIG_BOOL(config.auto_create_topics, "topics.auto.create", true);
  PL_CONFIG_INT(config.default_partitions, "topics.default.partitions", 1);
  PL_CONFIG_INT(config.default_replication_factor, "topics.default.replication.factor", 1);

  PL_CONFIG_MS(config.replication_interval_ms, "replication.interval", 2);
  PL_CONFIG_MS(config.replication_timeout_ms, "replication.timeout", 5000);
  PL_CONFIG_BYTES(config.replication_max_bytes, "replication.max.bytes", 1LL << 20);
  PL_CONFIG_MS(config.replica_lag_max_ms, "replication.lag.max", 10'000);

  PL_CONFIG_MS(config.group_session_timeout_ms, "group.session.timeout", 10'000);
  PL_CONFIG_MS(config.group_rebalance_delay_ms, "group.rebalance.delay", 200);

  PL_CONFIG_BOOL(config.metrics_enabled, "metrics.enabled", true);
  config.metrics_host = store.GetString("metrics.host", "0.0.0.0");
  std::int64_t metrics_port = 9644;
  PL_CONFIG_INT(metrics_port, "metrics.port", 9644);
  config.metrics_port = static_cast<std::uint16_t>(metrics_port);
  PL_CONFIG_MS(config.metrics_sample_interval_ms, "metrics.sample.interval", 1000);

  const std::string log_level = store.GetString("log.level", "info");
  if (!ParseLogLevel(log_level, config.log_level)) {
    return InvalidArgument("log.level must be trace|debug|info|warn|error|off; got '" + log_level +
                           "'");
  }
  config.log_file = store.GetString("log.file", "");

  PL_CONFIG_MS(config.retention_check_interval_ms, "storage.retention.check.interval", 30'000);

  PL_RETURN_IF_ERROR(config.Validate());
  return config;
}

Status BrokerConfig::Validate() const {
  if (!broker_id.valid()) {
    return InvalidArgument("broker.id must be non-negative");
  }
  if (io_threads == 0) return InvalidArgument("net.io.threads must be at least 1");
  if (worker_threads == 0) return InvalidArgument("broker.worker.threads must be at least 1");
  if (worker_queue_capacity < 8) {
    return InvalidArgument("broker.worker.queue.capacity must be at least 8");
  }
  if (data_dir.empty()) return InvalidArgument("broker.data.dir must not be empty");
  if (segment_bytes < 4096) {
    return InvalidArgument("storage.segment.bytes must be at least 4096");
  }
  if (segment_bytes > 0xFFFFFFFFLL) {
    // The offset index stores positions as 32-bit values.
    return InvalidArgument("storage.segment.bytes must be below 4 GiB");
  }
  if (index_interval_bytes <= 0) {
    return InvalidArgument("storage.index.interval.bytes must be positive");
  }
  if (default_partitions <= 0) {
    return InvalidArgument("topics.default.partitions must be positive");
  }
  if (default_replication_factor <= 0) {
    return InvalidArgument("topics.default.replication.factor must be positive");
  }
  if (!cluster_brokers.empty() &&
      default_replication_factor > static_cast<std::int16_t>(cluster_brokers.size())) {
    return InvalidArgument("topics.default.replication.factor (" +
                           std::to_string(default_replication_factor) +
                           ") exceeds the number of configured brokers (" +
                           std::to_string(cluster_brokers.size()) + ")");
  }
  if (output_high_water_bytes >= output_max_bytes) {
    return InvalidArgument(
        "net.output.high.water.bytes must be below net.output.max.bytes, otherwise "
        "backpressure never engages before the connection is killed");
  }
  if (max_frame_bytes == 0) return InvalidArgument("net.max.frame.bytes must be positive");
  return OkStatus();
}

std::string BrokerConfig::Describe() const {
  std::ostringstream out;
  out << "broker.id=" << broker_id.value() << '\n'
      << "net.listen=" << listen.ToString() << '\n'
      << "net.advertised=" << advertised_host << ':' << advertised_port << '\n'
      << "net.io.threads=" << io_threads << '\n'
      << "net.max.connections=" << max_connections << '\n'
      << "broker.data.dir=" << data_dir << '\n'
      << "broker.worker.threads=" << worker_threads << '\n'
      << "broker.worker.queue.capacity=" << worker_queue_capacity << '\n'
      << "storage.segment.bytes=" << segment_bytes << '\n'
      << "storage.index.interval.bytes=" << index_interval_bytes << '\n'
      << "storage.write.mode=" << storage::WriteModeName(write_mode) << '\n'
      << "storage.fsync.mode=" << storage::SyncModeName(sync_mode) << '\n'
      << "storage.preallocate=" << (preallocate_segments ? "true" : "false") << '\n'
      << "storage.flush.sync.on.append=" << (flush.sync_on_append ? "true" : "false") << '\n'
      << "storage.flush.interval=" << flush.interval_ms << "ms\n"
      << "storage.flush.max.bytes=" << flush.max_unflushed_bytes << '\n'
      << "storage.flush.max.records=" << flush.max_unflushed_records << '\n'
      << "storage.retention.bytes=" << retention_bytes << '\n'
      << "storage.retention.ms=" << retention_ms << '\n'
      << "topics.auto.create=" << (auto_create_topics ? "true" : "false") << '\n'
      << "topics.default.partitions=" << default_partitions << '\n'
      << "topics.default.replication.factor=" << default_replication_factor << '\n'
      << "replication.interval=" << replication_interval_ms << "ms\n"
      << "cluster.brokers=" << cluster_brokers.size() << " configured\n"
      << "metrics.enabled=" << (metrics_enabled ? "true" : "false") << '\n'
      << "metrics.port=" << metrics_port << '\n';
  return out.str();
}

storage::LogOptions BrokerConfig::LogOptionsFor(const std::string& topic,
                                                PartitionIndex partition,
                                                std::int64_t retention_override,
                                                std::int64_t segment_override) const {
  storage::LogOptions options;
  options.directory =
      std::filesystem::path(data_dir) / (topic + "-" + std::to_string(partition.value()));
  options.segment_bytes = segment_override > 0 ? segment_override : segment_bytes;
  options.segment_ms = segment_ms;
  options.index_interval_bytes = index_interval_bytes;
  options.retention_bytes = retention_bytes;
  options.retention_ms = retention_override > 0 ? retention_override : retention_ms;
  options.min_free_disk_bytes = min_free_disk_bytes;
  options.preallocate = preallocate_segments;
  options.write_mode = write_mode;
  options.sync_mode = sync_mode;
  options.flush = flush;
  return options;
}

}  // namespace pulselog::broker
