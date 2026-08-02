// Owns every partition replica hosted by this broker.
//
// Partitions are assigned to worker threads by hashing the topic/partition
// pair, so all mutation of one partition's log happens on one thread and the
// log itself needs no lock. The map from TopicPartition to replica is guarded
// by a shared_mutex, taken exclusively only when a partition is created or
// removed.
#ifndef PULSELOG_BROKER_PARTITION_MANAGER_H_
#define PULSELOG_BROKER_PARTITION_MANAGER_H_

#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "pulselog/base/status.h"
#include "pulselog/broker/broker_config.h"
#include "pulselog/broker/partition_replica.h"
#include "pulselog/metadata/cluster_metadata.h"

namespace pulselog::broker {

class PartitionManager {
 public:
  PartitionManager(const BrokerConfig& config, metadata::ClusterMetadata& cluster);

  PartitionManager(const PartitionManager&) = delete;
  PartitionManager& operator=(const PartitionManager&) = delete;

  ~PartitionManager();

  // Opens every partition this broker hosts according to current metadata, and
  // recovers each log. Reports total recovery time and record counts.
  struct OpenReport {
    std::size_t partitions_opened = 0;
    std::size_t partitions_truncated = 0;
    std::uint64_t records_recovered = 0;
    std::uint64_t bytes_recovered = 0;
    std::int64_t duration_ms = 0;
  };

  [[nodiscard]] Result<OpenReport> OpenHostedPartitions();

  // Creates a topic (registering it in metadata) and opens the partitions this
  // broker hosts. Idempotent for an identical configuration.
  [[nodiscard]] Result<metadata::TopicDescriptor> CreateTopic(const metadata::TopicConfig& config,
                                                              bool* created = nullptr);

  [[nodiscard]] Status DeleteTopic(const std::string& topic, bool delete_data);

  // Returns the replica, or nullptr when this broker does not host it.
  [[nodiscard]] PartitionReplica* Find(const TopicPartition& topic_partition) const;

  // Opens a partition this broker hosts but has not opened yet (for example
  // after a metadata refresh).
  [[nodiscard]] Result<PartitionReplica*> OpenPartition(const TopicPartition& topic_partition,
                                                        const metadata::TopicConfig& topic_config,
                                                        const metadata::PartitionAssignment& assignment);

  // Opens every partition this broker hosts for `topic` according to current
  // metadata. Used after learning about a topic from another broker.
  [[nodiscard]] Status OpenPartitionsForTopic(const metadata::TopicDescriptor& descriptor);

  [[nodiscard]] std::vector<PartitionReplica*> All() const;

  [[nodiscard]] std::size_t Size() const;

  // Which worker owns a partition. Hashing rather than round-robin so the
  // mapping is stable across restarts and identical on every broker, which
  // makes a partition's behaviour reproducible between runs.
  [[nodiscard]] std::size_t WorkerFor(const TopicPartition& topic_partition) const;

  // Flushes every partition whose flush policy has tripped. Returns how many
  // were flushed and completes any produce requests waiting on durability.
  [[nodiscard]] Result<std::size_t> FlushDuePartitions(std::int64_t now_ms);

  [[nodiscard]] Status FlushAll();

  // Applies retention to every partition. Returns segments deleted.
  [[nodiscard]] Result<std::size_t> EnforceRetention();

  // Expires produce acknowledgements past their deadline across all
  // partitions. Returns how many were failed.
  std::size_t ExpireWaiters(std::int64_t now_ms);

  struct AggregateStats {
    std::size_t partitions = 0;
    std::size_t leader_partitions = 0;
    std::uint64_t total_bytes = 0;
    std::uint64_t total_records = 0;
    std::size_t pending_waiters = 0;
    std::int64_t max_replication_lag = 0;
    std::uint64_t flush_count = 0;
    std::uint64_t flush_nanos_max = 0;
  };

  [[nodiscard]] AggregateStats GetStats() const;

  void Close();

 private:
  const BrokerConfig& config_;
  metadata::ClusterMetadata& cluster_;

  mutable std::shared_mutex mutex_;
  std::unordered_map<TopicPartition, std::unique_ptr<PartitionReplica>> partitions_;
  bool closed_ = false;
};

}  // namespace pulselog::broker

#endif  // PULSELOG_BROKER_PARTITION_MANAGER_H_
