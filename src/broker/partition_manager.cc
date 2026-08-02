#include "pulselog/broker/partition_manager.h"

#include <algorithm>

#include "pulselog/base/clock.h"
#include "pulselog/base/logging.h"

namespace pulselog::broker {
namespace {

constexpr std::string_view kComponent = "broker.partitions";

}  // namespace

PartitionManager::PartitionManager(const BrokerConfig& config, metadata::ClusterMetadata& cluster)
    : config_(config), cluster_(cluster) {}

PartitionManager::~PartitionManager() {
  Close();
}

std::size_t PartitionManager::WorkerFor(const TopicPartition& topic_partition) const {
  if (config_.worker_threads <= 1) return 0;
  const std::size_t hash = std::hash<TopicPartition>{}(topic_partition);
  return hash % config_.worker_threads;
}

Result<PartitionReplica*> PartitionManager::OpenPartition(
    const TopicPartition& topic_partition,
    const metadata::TopicConfig& topic_config,
    const metadata::PartitionAssignment& assignment) {
  {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    const auto it = partitions_.find(topic_partition);
    if (it != partitions_.end()) return it->second.get();
  }

  storage::LogOptions options = config_.LogOptionsFor(topic_partition.topic,
                                                      topic_partition.partition,
                                                      topic_config.retention_ms,
                                                      topic_config.segment_bytes);

  storage::RecoveryReport recovery;
  PL_ASSIGN_OR_RETURN(auto log, storage::PartitionLog::Open(topic_partition, options, &recovery));

  auto replica = std::make_unique<PartitionReplica>(
      topic_partition, assignment, config_.broker_id, std::move(log));
  PartitionReplica* raw = replica.get();

  std::unique_lock<std::shared_mutex> lock(mutex_);
  if (closed_) return Status{ErrorCode::kClosed, "partition manager is closed"};
  // Another thread may have opened it while we were recovering.
  const auto [it, inserted] = partitions_.emplace(topic_partition, std::move(replica));
  return inserted ? raw : it->second.get();
}

Result<PartitionManager::OpenReport> PartitionManager::OpenHostedPartitions() {
  const Stopwatch stopwatch;
  OpenReport report;

  const auto hosted = cluster_.PartitionsHostedBy(config_.broker_id);
  for (const auto& topic_partition : hosted) {
    auto topic = cluster_.GetTopic(topic_partition.topic);
    if (!topic.ok()) continue;
    auto assignment = cluster_.GetPartition(topic_partition.topic, topic_partition.partition);
    if (!assignment.ok()) continue;

    storage::LogOptions options = config_.LogOptionsFor(topic_partition.topic,
                                                        topic_partition.partition,
                                                        topic->config.retention_ms,
                                                        topic->config.segment_bytes);

    storage::RecoveryReport recovery;
    auto log = storage::PartitionLog::Open(topic_partition, options, &recovery);
    if (!log.ok()) {
      return log.status().WithContext("opening " + topic_partition.ToString());
    }

    report.records_recovered += recovery.records_scanned;
    report.bytes_recovered += recovery.valid_bytes;
    if (recovery.truncated) ++report.partitions_truncated;

    auto replica = std::make_unique<PartitionReplica>(
        topic_partition, assignment.value(), config_.broker_id, std::move(log).value());
    std::unique_lock<std::shared_mutex> lock(mutex_);
    partitions_.emplace(topic_partition, std::move(replica));
    ++report.partitions_opened;
  }

  report.duration_ms = static_cast<std::int64_t>(stopwatch.ElapsedMillis());
  PL_INFO(kComponent) << "opened hosted partitions"
                      << " partitions=" << report.partitions_opened
                      << " truncated=" << report.partitions_truncated
                      << " records=" << report.records_recovered
                      << " bytes=" << report.bytes_recovered
                      << " duration_ms=" << report.duration_ms;
  return report;
}

Result<metadata::TopicDescriptor> PartitionManager::CreateTopic(const metadata::TopicConfig& config,
                                                                bool* created) {
  PL_ASSIGN_OR_RETURN(const metadata::TopicDescriptor descriptor,
                      cluster_.CreateTopic(config, created));
  PL_RETURN_IF_ERROR(OpenPartitionsForTopic(descriptor));
  return descriptor;
}

Status PartitionManager::OpenPartitionsForTopic(const metadata::TopicDescriptor& descriptor) {
  for (const auto& assignment : descriptor.partitions) {
    if (!assignment.HasReplica(config_.broker_id)) continue;
    const TopicPartition topic_partition{descriptor.config.name, assignment.index};
    auto opened = OpenPartition(topic_partition, descriptor.config, assignment);
    if (!opened.ok()) {
      return opened.status().WithContext("opening " + topic_partition.ToString());
    }
  }
  return OkStatus();
}

Status PartitionManager::DeleteTopic(const std::string& topic, bool delete_data) {
  PL_ASSIGN_OR_RETURN(const metadata::TopicDescriptor descriptor, cluster_.GetTopic(topic));

  std::vector<std::unique_ptr<PartitionReplica>> removed;
  {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    for (const auto& assignment : descriptor.partitions) {
      const TopicPartition topic_partition{topic, assignment.index};
      const auto it = partitions_.find(topic_partition);
      if (it == partitions_.end()) continue;
      removed.push_back(std::move(it->second));
      partitions_.erase(it);
    }
  }

  // Fail any in-flight acknowledgements before the logs disappear, so no
  // producer is left waiting on a partition that no longer exists.
  for (auto& replica : removed) {
    replica->FailAllWaiters(Status{ErrorCode::kNotFound, "topic deleted"});
    const Status status = replica->log().Close();
    if (!status.ok()) {
      PL_WARN(kComponent) << "error closing deleted partition: " << status.ToString();
    }
  }

  if (delete_data) {
    for (const auto& assignment : descriptor.partitions) {
      const std::filesystem::path dir = std::filesystem::path(config_.data_dir) /
                                        (topic + "-" + std::to_string(assignment.index.value()));
      std::error_code ec;
      std::filesystem::remove_all(dir, ec);
      if (ec) {
        PL_WARN(kComponent) << "could not remove " << dir.string() << ": " << ec.message();
      }
    }
  }

  removed.clear();
  return cluster_.DeleteTopic(topic);
}

PartitionReplica* PartitionManager::Find(const TopicPartition& topic_partition) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  const auto it = partitions_.find(topic_partition);
  return it == partitions_.end() ? nullptr : it->second.get();
}

std::vector<PartitionReplica*> PartitionManager::All() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  std::vector<PartitionReplica*> result;
  result.reserve(partitions_.size());
  for (const auto& [key, replica] : partitions_) result.push_back(replica.get());
  return result;
}

std::size_t PartitionManager::Size() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return partitions_.size();
}

Result<std::size_t> PartitionManager::FlushDuePartitions(std::int64_t now_ms) {
  std::size_t flushed = 0;
  Status first_error = OkStatus();

  for (PartitionReplica* replica : All()) {
    if (!replica->log().NeedsFlush(now_ms)) continue;
    const Status status = replica->log().Flush();
    if (!status.ok()) {
      // One bad disk must not stop the others from being flushed.
      PL_ERROR(kComponent) << "flush failed"
                           << " partition=" << replica->topic_partition().ToString()
                           << " error=" << status.ToString();
      if (first_error.ok()) first_error = status;
      continue;
    }
    ++flushed;
    // A flush can satisfy quorum acknowledgements waiting on durability.
    (void)replica->CompleteSatisfiedWaiters(now_ms);
  }

  if (!first_error.ok()) return first_error;
  return flushed;
}

Status PartitionManager::FlushAll() {
  Status first_error = OkStatus();
  for (PartitionReplica* replica : All()) {
    const Status status = replica->log().Flush();
    if (!status.ok() && first_error.ok()) first_error = status;
  }
  return first_error;
}

Result<std::size_t> PartitionManager::EnforceRetention() {
  std::size_t deleted = 0;
  for (PartitionReplica* replica : All()) {
    auto removed = replica->log().EnforceRetention();
    if (!removed.ok()) {
      PL_WARN(kComponent) << "retention failed"
                          << " partition=" << replica->topic_partition().ToString()
                          << " error=" << removed.status().ToString();
      continue;
    }
    deleted += removed.value();
  }
  return deleted;
}

std::size_t PartitionManager::ExpireWaiters(std::int64_t now_ms) {
  std::size_t expired = 0;
  for (PartitionReplica* replica : All()) {
    expired += replica->ExpireWaiters(now_ms);
  }
  return expired;
}

PartitionManager::AggregateStats PartitionManager::GetStats() const {
  AggregateStats aggregate;
  for (PartitionReplica* replica : All()) {
    const auto stats = replica->GetStats();
    ++aggregate.partitions;
    if (stats.leader) ++aggregate.leader_partitions;
    aggregate.total_bytes += stats.total_bytes;
    aggregate.total_records +=
        static_cast<std::uint64_t>(stats.log_end_offset - stats.log_start_offset);
    aggregate.pending_waiters += stats.pending_waiters;
    aggregate.max_replication_lag = std::max(aggregate.max_replication_lag, stats.max_follower_lag);

    const auto log_stats = replica->log().GetStats();
    aggregate.flush_count += log_stats.flush_count;
    aggregate.flush_nanos_max = std::max(aggregate.flush_nanos_max, log_stats.flush_nanos_max);
  }
  return aggregate;
}

void PartitionManager::Close() {
  std::unordered_map<TopicPartition, std::unique_ptr<PartitionReplica>> partitions;
  {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (closed_) return;
    closed_ = true;
    partitions.swap(partitions_);
  }

  // Fail outstanding acknowledgements before closing the logs: a producer
  // waiting on quorum must get an error, not a silent drop.
  for (auto& [key, replica] : partitions) {
    replica->FailAllWaiters(Status{ErrorCode::kUnavailable, "broker is shutting down"});
  }
  for (auto& [key, replica] : partitions) {
    const Status status = replica->log().Flush();
    if (!status.ok()) {
      PL_ERROR(kComponent) << "final flush failed"
                           << " partition=" << key.ToString() << " error=" << status.ToString();
    }
    const Status close_status = replica->log().Close();
    if (!close_status.ok()) {
      PL_ERROR(kComponent) << "close failed"
                           << " partition=" << key.ToString()
                           << " error=" << close_status.ToString();
    }
  }
  PL_INFO(kComponent) << "closed partitions count=" << partitions.size();
}

}  // namespace pulselog::broker
