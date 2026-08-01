// Broker lifecycle: start-up, background threads, shutdown.
// Request dispatch lives in request_handler.cc.
#include "pulselog/broker/broker.h"

#include <algorithm>
#include <chrono>
#include <sstream>
#include <thread>

#include "pulselog/base/clock.h"
#include "pulselog/base/crc32c.h"
#include "pulselog/base/logging.h"
#include "pulselog/storage/file_util.h"

namespace pulselog::broker {
namespace {

constexpr std::string_view kComponent = "broker";

constexpr std::string_view kVersion = "0.1.0";

std::string JsonEscape(std::string_view value) {
  std::string out;
  out.reserve(value.size());
  for (const char c : value) {
    if (c == '"' || c == '\\') {
      out.push_back('\\');
      out.push_back(c);
    } else if (c == '\n') {
      out += "\\n";
    } else {
      out.push_back(c);
    }
  }
  return out;
}

}  // namespace

Broker::Broker(BrokerConfig config)
    : config_(std::move(config)), partitions_(config_, cluster_) {}

Broker::~Broker() { Stop(); }

Status Broker::LoadOrInitialiseMetadata() {
  // Cluster membership. A broker with no configured peers is a single-node
  // cluster consisting of itself, which is what makes `pulselog-broker` with
  // no arguments useful.
  if (config_.cluster_brokers.empty()) {
    protocol::BrokerEndpoint self;
    self.id = config_.broker_id;
    self.host = config_.advertised_host;
    self.port = config_.advertised_port;
    cluster_.SetBrokers({self});
  } else {
    PL_RETURN_IF_ERROR(cluster_.SetBrokersFromSpec(config_.cluster_brokers));
    if (!cluster_.FindBroker(config_.broker_id).has_value()) {
      return InvalidArgument("broker.id " + std::to_string(config_.broker_id.value()) +
                             " does not appear in cluster.brokers");
    }
  }

  const std::filesystem::path metadata_path =
      std::filesystem::path(config_.data_dir) / "metadata.tsv";
  const Status loaded = cluster_.LoadFrom(metadata_path);
  if (!loaded.ok() && loaded.code() != ErrorCode::kNotFound) {
    // A corrupt metadata file is fatal: guessing at topic layout would
    // silently reroute keys and break ordering guarantees.
    return loaded.WithContext("loading " + metadata_path.string());
  }
  return OkStatus();
}

Status Broker::PersistMetadata() {
  const std::filesystem::path path = std::filesystem::path(config_.data_dir) / "metadata.tsv";
  return cluster_.SaveTo(path);
}

Status Broker::Start() {
  if (running_.load(std::memory_order_acquire)) return OkStatus();
  start_nanos_ = MonotonicNanos();

  SetLogLevel(config_.log_level);
  SetLogBrokerId(config_.broker_id.value());
  if (!config_.log_file.empty() && !SetLogFile(config_.log_file)) {
    return IoError("cannot open log file " + config_.log_file);
  }

  PL_RETURN_IF_ERROR(storage::EnsureDirectory(config_.data_dir));
  metrics_ = std::make_unique<metrics::BrokerMetrics>(registry_);

  PL_RETURN_IF_ERROR(LoadOrInitialiseMetadata());

  PL_ASSIGN_OR_RETURN(const PartitionManager::OpenReport report,
                      partitions_.OpenHostedPartitions());

  // Consumer-group coordination, with offsets in their own internal log.
  PL_ASSIGN_OR_RETURN(
      auto offset_store,
      consumer::OffsetStore::Open(std::filesystem::path(config_.data_dir) / "__offsets",
                                  config_.flush.sync_on_append));
  groups_ = std::make_unique<consumer::GroupCoordinator>(
      std::move(offset_store),
      [this](const std::vector<std::string>& topics) {
        std::vector<TopicPartition> partitions;
        for (const auto& topic : topics) {
          auto descriptor = cluster_.GetTopic(topic);
          if (!descriptor.ok()) continue;
          for (const auto& assignment : descriptor->partitions) {
            partitions.push_back(TopicPartition{topic, assignment.index});
          }
        }
        return partitions;
      },
      config_.group_session_timeout_ms);

  // Workers, each with its own buffer pool so they never contend on it.
  worker_pools_.reserve(config_.worker_threads);
  workers_.reserve(config_.worker_threads);
  for (std::size_t i = 0; i < config_.worker_threads; ++i) {
    BufferPoolOptions pool_options;
    pool_options.default_capacity = 256 * 1024;
    pool_options.max_pooled = 128;
    worker_pools_.push_back(std::make_unique<BufferPool>(pool_options));

    const int pin = config_.pin_workers ? static_cast<int>(i) : -1;
    workers_.push_back(std::make_unique<PartitionWorker>(i, config_.worker_queue_capacity, *this,
                                                         pin));
  }
  for (auto& worker : workers_) worker->Start();

  // Network server.
  net::ServerOptions server_options;
  server_options.bind = config_.listen;
  server_options.io_threads = config_.io_threads;
  server_options.max_connections = config_.max_connections;
  server_options.connection.max_frame_bytes = config_.max_frame_bytes;
  server_options.connection.idle_timeout_ms = config_.connection_idle_timeout_ms;
  server_options.connection.output_high_water_bytes = config_.output_high_water_bytes;
  server_options.connection.output_low_water_bytes = config_.output_high_water_bytes / 4;
  server_options.connection.output_max_bytes = config_.output_max_bytes;

  loop_connections_.resize(config_.io_threads);
  loop_pools_.reserve(config_.io_threads);
  for (std::size_t i = 0; i < config_.io_threads; ++i) {
    loop_pools_.push_back(std::make_unique<BufferPool>());
  }

  server_ = std::make_unique<net::TcpServer>(
      server_options,
      [this](net::Connection& connection, const protocol::FrameDecoder::Frame& frame) {
        OnFrame(connection, frame);
      },
      [this](net::Connection& connection, const Status& reason) {
        OnConnectionClosed(connection, reason);
      });
  PL_RETURN_IF_ERROR(server_->Start());

  // Replication, only when there is somewhere to replicate to.
  if (cluster_.Brokers().size() > 1) {
    replication::ReplicatorOptions replicator_options;
    replicator_options.self = config_.broker_id;
    replicator_options.interval_ms = config_.replication_interval_ms;
    replicator_options.timeout_ms = config_.replication_timeout_ms;
    replicator_options.max_bytes = config_.replication_max_bytes;
    replicator_options.lag_max_ms = config_.replica_lag_max_ms;
    replicator_ = std::make_unique<replication::Replicator>(replicator_options, partitions_,
                                                            cluster_);
    PL_RETURN_IF_ERROR(replicator_->Start());
  }

  // Metrics endpoint.
  if (config_.metrics_enabled) {
    exporter_ = std::make_unique<metrics::MetricsExporter>(registry_, config_.metrics_host,
                                                           config_.metrics_port);
    exporter_->AddHandler("/topology", [this] {
      return metrics::HttpResponse{200, "application/json", BuildTopologyJson()};
    });
    const Status status = exporter_->Start();
    if (!status.ok()) {
      // A failed metrics endpoint must not prevent the broker from serving.
      PL_ERROR(kComponent) << "metrics endpoint failed to start: " << status.ToString();
      exporter_.reset();
    }
  }

  running_.store(true, std::memory_order_release);
  stopping_.store(false, std::memory_order_release);

  flusher_thread_ = NamedThread("pl-flusher", [this] { FlusherLoop(); });
  maintenance_thread_ = NamedThread("pl-maint", [this] { MaintenanceLoop(); });
  metrics_thread_ = NamedThread("pl-metrics-sample", [this] { MetricsLoop(); });

  PL_INFO(kComponent) << "broker started"
                      << " id=" << config_.broker_id.value()
                      << " endpoint=" << server_->bound_endpoint().ToString()
                      << " partitions=" << report.partitions_opened
                      << " recovery_ms=" << report.duration_ms
                      << " io_threads=" << config_.io_threads
                      << " workers=" << config_.worker_threads
                      << " poller=" << net::PollerBackendName()
                      << " checksum=" << Crc32cImplementationName();
  return OkStatus();
}

void Broker::Stop() {
  if (!running_.exchange(false, std::memory_order_acq_rel)) return;
  stopping_.store(true, std::memory_order_release);
  PL_INFO(kComponent) << "broker stopping id=" << config_.broker_id.value();

  // Order matters. Stop accepting work first, then drain it, then close the
  // things the work depends on.
  if (server_) server_->Stop();
  if (replicator_) replicator_->Stop();
  for (auto& worker : workers_) worker->Stop();

  flusher_thread_.Join();
  maintenance_thread_.Join();
  metrics_thread_.Join();

  if (exporter_) exporter_->Stop();

  const Status persisted = PersistMetadata();
  if (!persisted.ok()) {
    PL_ERROR(kComponent) << "could not persist metadata: " << persisted.ToString();
  }

  if (groups_) {
    const Status flushed = groups_->Flush();
    if (!flushed.ok()) {
      PL_ERROR(kComponent) << "could not flush consumer offsets: " << flushed.ToString();
    }
    groups_->Close();
  }

  partitions_.Close();
  workers_.clear();
  server_.reset();
  replicator_.reset();
  groups_.reset();
  exporter_.reset();

  PL_INFO(kComponent) << "broker stopped id=" << config_.broker_id.value();
  FlushLogs();
}

net::Endpoint Broker::endpoint() const {
  return server_ ? server_->bound_endpoint() : config_.listen;
}

std::uint16_t Broker::metrics_port() const { return exporter_ ? exporter_->port() : 0; }

std::int64_t Broker::uptime_ms() const {
  if (start_nanos_ == 0) return 0;
  return (MonotonicNanos() - start_nanos_) / 1'000'000;
}

void Broker::FlusherLoop() {
  const auto interval = std::chrono::milliseconds(std::max<std::int64_t>(
      1, config_.flusher_interval_ms));

  while (!stopping_.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(interval);
    const std::int64_t now_ms = MonotonicNanos() / 1'000'000;

    auto flushed = partitions_.FlushDuePartitions(now_ms);
    if (!flushed.ok()) {
      PL_ERROR(kComponent) << "flusher error: " << flushed.status().ToString();
      continue;
    }
    if (flushed.value() > 0 && metrics_) {
      const auto stats = partitions_.GetStats();
      metrics_->flush_latency.Record(static_cast<std::int64_t>(stats.flush_nanos_max));
    }
  }

  // Final flush so a clean shutdown loses nothing that was acknowledged.
  const Status status = partitions_.FlushAll();
  if (!status.ok()) {
    PL_ERROR(kComponent) << "final flush failed: " << status.ToString();
  }
}

void Broker::MaintenanceLoop() {
  std::int64_t next_retention_ms = 0;

  while (!stopping_.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    const std::int64_t now_ms = WallClockMillis();

    // Produce requests whose acknowledgement condition never arrived.
    const std::size_t expired = partitions_.ExpireWaiters(now_ms);
    if (expired > 0 && metrics_) metrics_->failed_requests.Increment(expired);

    // Consumer sessions that stopped heartbeating.
    if (groups_) {
      const std::size_t evicted = groups_->ExpireSessions(now_ms);
      if (evicted > 0) {
        PL_INFO(kComponent) << "expired consumer sessions count=" << evicted;
      }
    }

    if (now_ms >= next_retention_ms) {
      next_retention_ms = now_ms + config_.retention_check_interval_ms;
      auto deleted = partitions_.EnforceRetention();
      if (deleted.ok() && deleted.value() > 0) {
        PL_INFO(kComponent) << "retention deleted segments count=" << deleted.value();
      }
    }
  }
}

void Broker::MetricsLoop() {
  metrics::ProcessStatsSampler sampler;
  const auto interval =
      std::chrono::milliseconds(std::max<std::int64_t>(100, config_.metrics_sample_interval_ms));

  while (!stopping_.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(interval);
    if (metrics_ == nullptr) continue;

    const auto sample = sampler.Sample();
    metrics_->resident_memory_bytes.Set(static_cast<std::int64_t>(sample.resident_bytes));
    metrics_->cpu_percent.Set(static_cast<std::int64_t>(sample.cpu_percent));

    const auto stats = partitions_.GetStats();
    metrics_->hosted_partitions.Set(static_cast<std::int64_t>(stats.partitions));
    metrics_->leader_partitions.Set(static_cast<std::int64_t>(stats.leader_partitions));
    metrics_->total_log_bytes.Set(static_cast<std::int64_t>(stats.total_bytes));
    metrics_->replication_lag_max.Set(stats.max_replication_lag);

    if (server_) {
      metrics_->active_connections.Set(static_cast<std::int64_t>(server_->ConnectionCount()));
    }
    if (groups_) {
      metrics_->consumer_group_count.Set(static_cast<std::int64_t>(groups_->GroupCount()));
    }

    std::int64_t queue_depth = 0;
    for (const auto& worker : workers_) {
      queue_depth += static_cast<std::int64_t>(worker->QueueDepth());
    }
    metrics_->request_queue_depth.Set(queue_depth);
  }
}

std::string Broker::BuildTopologyJson() const {
  std::ostringstream out;
  out << "{\"broker_id\":" << config_.broker_id.value() << ",\"uptime_ms\":" << uptime_ms()
      << ",\"endpoint\":\"" << JsonEscape(endpoint().ToString()) << "\",\"brokers\":[";

  bool first = true;
  for (const auto& broker : cluster_.Brokers()) {
    if (!first) out << ',';
    first = false;
    out << "{\"id\":" << broker.id.value() << ",\"host\":\"" << JsonEscape(broker.host)
        << "\",\"port\":" << broker.port << ",\"self\":"
        << (broker.id == config_.broker_id ? "true" : "false") << '}';
  }
  out << "],\"topics\":[";

  first = true;
  for (const auto& descriptor : cluster_.ListTopics()) {
    if (!first) out << ',';
    first = false;
    out << "{\"name\":\"" << JsonEscape(descriptor.config.name)
        << "\",\"partitions\":[";
    bool first_partition = true;
    for (const auto& assignment : descriptor.partitions) {
      if (!first_partition) out << ',';
      first_partition = false;
      out << "{\"index\":" << assignment.index.value()
          << ",\"leader\":" << assignment.leader.value()
          << ",\"epoch\":" << assignment.leader_epoch << ",\"replicas\":[";
      for (std::size_t i = 0; i < assignment.replicas.size(); ++i) {
        if (i > 0) out << ',';
        out << assignment.replicas[i].value();
      }
      out << ']';

      const TopicPartition topic_partition{descriptor.config.name, assignment.index};
      if (PartitionReplica* replica = partitions_.Find(topic_partition)) {
        const auto stats = replica->GetStats();
        out << ",\"local\":true,\"log_start\":" << stats.log_start_offset
            << ",\"log_end\":" << stats.log_end_offset
            << ",\"high_water_mark\":" << stats.high_water_mark
            << ",\"flushed\":" << stats.flushed_offset << ",\"bytes\":" << stats.total_bytes
            << ",\"segments\":" << stats.segment_count
            << ",\"in_sync_replicas\":" << stats.in_sync_replicas
            << ",\"max_follower_lag\":" << stats.max_follower_lag;
      } else {
        out << ",\"local\":false";
      }
      out << '}';
    }
    out << "]}";
  }
  out << "],\"followers\":[";

  if (replicator_) {
    first = true;
    for (const auto& follower : replicator_->DescribeFollowers()) {
      if (!first) out << ',';
      first = false;
      out << "{\"broker\":" << follower.broker.value() << ",\"connected\":"
          << (follower.connected ? "true" : "false")
          << ",\"batches_sent\":" << follower.batches_sent
          << ",\"max_lag_records\":" << follower.max_lag_records << '}';
    }
  }
  out << "],\"groups\":[";

  if (groups_) {
    first = true;
    for (const auto& group : groups_->Describe()) {
      if (!first) out << ',';
      first = false;
      out << "{\"group\":\"" << JsonEscape(group.group_id) << "\",\"state\":\""
          << GroupStateName(group.state) << "\",\"generation\":" << group.generation.value()
          << ",\"members\":" << group.members.size() << '}';
    }
  }
  out << "],\"version\":\"" << kVersion << "\"}";
  return out.str();
}

}  // namespace pulselog::broker
