// Cluster and topic metadata: which broker leads which partition.
//
// Leadership is **statically assigned** in this version. Assignments are
// computed deterministically when a topic is created and persisted; there is
// no election and no automatic failover. Losing a leader makes its partitions
// unavailable for writes until that broker returns. This is stated here, in
// REPLICATION.md, and in the README, because it is the single biggest
// difference between PulseLog and a production broker.
#ifndef PULSELOG_METADATA_CLUSTER_METADATA_H_
#define PULSELOG_METADATA_CLUSTER_METADATA_H_

#include <cstdint>
#include <filesystem>
#include <map>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <vector>

#include "pulselog/base/status.h"
#include "pulselog/base/types.h"
#include "pulselog/protocol/messages.h"

namespace pulselog::metadata {

using protocol::BrokerEndpoint;

struct TopicConfig {
  std::string name;
  std::int32_t partition_count = 1;
  std::int16_t replication_factor = 1;
  std::int64_t retention_ms = -1;  // -1 inherits the broker default.
  std::int64_t segment_bytes = -1;
  Compression compression = Compression::kNone;
};

struct PartitionAssignment {
  PartitionIndex index{0};
  BrokerId leader{-1};
  LeaderEpoch leader_epoch = 0;
  // Replica set, leader first. Followers are the remainder.
  std::vector<BrokerId> replicas;
  // Replicas currently considered caught up. Maintained by the leader.
  std::vector<BrokerId> in_sync_replicas;

  [[nodiscard]] bool HasReplica(BrokerId broker) const;

  // Acknowledgements needed for a quorum write: a strict majority of the
  // replica set, leader included.
  [[nodiscard]] std::size_t QuorumSize() const { return replicas.size() / 2 + 1; }
};

struct TopicDescriptor {
  TopicConfig config;
  std::vector<PartitionAssignment> partitions;
};

// Routes a keyed record to a partition.
//
// The hash is CRC-32C of the key, which the engine already computes in
// hardware. It must stay identical on client and broker: a client routes with
// it, and the broker validates. Changing it would silently reorder existing
// keys across partitions, so it is part of the compatibility contract.
[[nodiscard]] PartitionIndex PartitionForKey(ByteSpan key, std::int32_t partition_count) noexcept;

// Broker that coordinates a consumer group.
//
// Group membership and committed offsets live on exactly one broker, chosen by
// hashing the group ID over the id-sorted broker list. Client and broker must
// compute this identically, which is why it is one function used by both:
// without a single owner, a JoinGroup and a CommitOffset from the same
// consumer can land on different brokers, and the second one fails with
// "unknown group" while the group's real position never advances.
//
// The hash is CRC-32C, as it is for key routing.
[[nodiscard]] BrokerId CoordinatorForGroup(std::string_view group_id,
                                           const std::vector<BrokerEndpoint>& brokers) noexcept;

// Partition for a record with no key: round-robin via the caller's counter.
[[nodiscard]] PartitionIndex PartitionRoundRobin(std::uint64_t counter,
                                                 std::int32_t partition_count) noexcept;

class ClusterMetadata {
 public:
  ClusterMetadata() = default;

  ClusterMetadata(const ClusterMetadata&) = delete;
  ClusterMetadata& operator=(const ClusterMetadata&) = delete;

  // Declares the static cluster membership. Entries look like
  // "1@broker-1:9092". The order determines partition assignment, so every
  // broker must be configured with the same list.
  [[nodiscard]] Status SetBrokersFromSpec(const std::vector<std::string>& specs);

  void SetBrokers(std::vector<BrokerEndpoint> brokers);

  [[nodiscard]] std::vector<BrokerEndpoint> Brokers() const;

  [[nodiscard]] std::optional<BrokerEndpoint> FindBroker(BrokerId id) const;

  [[nodiscard]] BrokerId ControllerId() const;

  // Broker that coordinates `group_id`. See CoordinatorForGroup.
  [[nodiscard]] BrokerId CoordinatorFor(std::string_view group_id) const;

  // Creates a topic and computes its partition assignments. Returns
  // ALREADY_EXISTS if the name is taken with a different configuration, and OK
  // (idempotently) if the configuration matches. `created` distinguishes the
  // two OK cases, which callers use to avoid re-broadcasting a topic that was
  // already known.
  [[nodiscard]] Result<TopicDescriptor> CreateTopic(const TopicConfig& config,
                                                    bool* created = nullptr);

  // Registers a topic with assignments computed elsewhere (from the persisted
  // file, or from another broker's metadata response).
  [[nodiscard]] Status UpsertTopic(TopicDescriptor descriptor);

  [[nodiscard]] Status DeleteTopic(const std::string& name);

  [[nodiscard]] bool HasTopic(const std::string& name) const;

  [[nodiscard]] Result<TopicDescriptor> GetTopic(const std::string& name) const;

  [[nodiscard]] std::vector<TopicDescriptor> ListTopics() const;

  [[nodiscard]] Result<PartitionAssignment> GetPartition(const std::string& topic,
                                                         PartitionIndex partition) const;

  // Partitions this broker leads / hosts a replica of.
  [[nodiscard]] std::vector<TopicPartition> PartitionsLedBy(BrokerId broker) const;

  [[nodiscard]] std::vector<TopicPartition> PartitionsHostedBy(BrokerId broker) const;

  // Replaces the in-sync replica set for one partition. Called by the leader
  // as followers catch up or fall behind.
  [[nodiscard]] Status UpdateInSyncReplicas(const std::string& topic,
                                            PartitionIndex partition,
                                            std::vector<BrokerId> in_sync);

  // Bumps a partition's leader epoch and sets a new leader. Used only by the
  // admin path today; the hook exists so an election implementation has one
  // place to change.
  [[nodiscard]] Status SetLeader(const std::string& topic,
                                 PartitionIndex partition,
                                 BrokerId leader);

  // --- persistence ----------------------------------------------------------
  // Topic definitions survive a restart. The format is one line per topic
  // followed by one line per partition; it is intentionally text so an
  // operator can read and repair it.
  [[nodiscard]] Status SaveTo(const std::filesystem::path& path) const;

  [[nodiscard]] Status LoadFrom(const std::filesystem::path& path);

  // Builds a METADATA response for the given topics (empty = all).
  [[nodiscard]] protocol::MetadataResponse BuildMetadataResponse(
      const std::vector<std::string>& topics) const;

 private:
  // Deterministic replica placement: replica `r` of partition `p` goes to
  // broker `(p + r + topic_offset) % broker_count`, with `topic_offset`
  // derived from the topic name so different topics do not stack every
  // partition-0 leader onto the same broker.
  [[nodiscard]] std::vector<PartitionAssignment> ComputeAssignments(
      const TopicConfig& config) const;

  mutable std::shared_mutex mutex_;
  std::vector<BrokerEndpoint> brokers_;
  std::map<std::string, TopicDescriptor, std::less<>> topics_;
};

}  // namespace pulselog::metadata

#endif  // PULSELOG_METADATA_CLUSTER_METADATA_H_
