// Runtime state for one partition on this broker.
//
// Wraps the partition's log with the replication bookkeeping the acknowledgement
// policy needs: the high-water mark, per-follower progress, and the set of
// produce requests waiting for a durability condition to be met.
//
// Acknowledgement ladder (see docs/FAILURE_SEMANTICS.md for the full table):
//
//   kNone   -- reply once the record is appended to the leader's log. No
//              durability guarantee: an immediate crash loses it.
//   kLeader -- reply once the record is in the leader's log, subject to the
//              flush policy. Survives a process crash; survives a machine
//              crash only if the flusher had run (or sync_on_append is set).
//   kQuorum -- reply once a majority of the replica set, leader included, has
//              *persisted* the record (their flushed offset covers it).
//              Survives the loss of any minority of replicas.
//
// Threading: appends happen on the owning worker thread. Follower
// acknowledgements arrive on replication threads and durability progress on
// the flusher thread, so the waiter list and follower map are mutex-guarded.
// The mutex is never held while a completion callback runs.
#ifndef PULSELOG_BROKER_PARTITION_REPLICA_H_
#define PULSELOG_BROKER_PARTITION_REPLICA_H_

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "pulselog/base/status.h"
#include "pulselog/base/types.h"
#include "pulselog/metadata/cluster_metadata.h"
#include "pulselog/storage/partition_log.h"

namespace pulselog::broker {

// What the leader knows about one follower.
struct FollowerState {
  BrokerId broker{-1};
  Offset log_end_offset = 0;     // Next offset the follower expects.
  Offset flushed_offset = 0;     // Durable up to (exclusive) on the follower.
  std::int64_t last_ack_ms = 0;  // For lag detection.
  bool in_sync = true;
  std::uint64_t acks_received = 0;
};

// A produce request that has been appended but not yet acknowledged.
struct DurabilityWaiter {
  Offset required_offset = kInvalidOffset;  // Last offset of the batch.
  AckMode mode = AckMode::kLeader;
  std::int64_t deadline_ms = 0;
  std::function<void(Status, Offset high_water_mark)> on_complete;
};

class PartitionReplica {
 public:
  PartitionReplica(TopicPartition topic_partition, metadata::PartitionAssignment assignment,
                   BrokerId self, std::unique_ptr<storage::PartitionLog> log);

  PartitionReplica(const PartitionReplica&) = delete;
  PartitionReplica& operator=(const PartitionReplica&) = delete;

  ~PartitionReplica();

  [[nodiscard]] const TopicPartition& topic_partition() const noexcept { return topic_partition_; }

  [[nodiscard]] storage::PartitionLog& log() noexcept { return *log_; }

  [[nodiscard]] const storage::PartitionLog& log() const noexcept { return *log_; }

  [[nodiscard]] bool is_leader() const noexcept {
    return assignment_.leader == self_;
  }

  [[nodiscard]] BrokerId leader() const noexcept { return assignment_.leader; }

  [[nodiscard]] LeaderEpoch leader_epoch() const noexcept { return assignment_.leader_epoch; }

  [[nodiscard]] metadata::PartitionAssignment assignment() const;

  void SetAssignment(metadata::PartitionAssignment assignment);

  // Highest offset (exclusive) that consumers may read. Equals the smallest
  // log end offset across the in-sync replica set; with a single replica it
  // tracks the leader's log end offset.
  [[nodiscard]] Offset HighWaterMark() const noexcept {
    return high_water_mark_.load(std::memory_order_acquire);
  }

  // Records a follower's reported progress and recomputes the high-water mark.
  // Returns the new high-water mark.
  Offset OnFollowerProgress(BrokerId follower, Offset log_end_offset, Offset flushed_offset,
                            std::int64_t now_ms);

  // Drops a follower from the in-sync set (disconnect, or lag beyond the
  // configured bound). The high-water mark is recomputed without it.
  void MarkFollowerOutOfSync(BrokerId follower, std::int64_t now_ms);

  [[nodiscard]] std::vector<FollowerState> Followers() const;

  // Largest follower lag in records, for the replication-lag metric.
  [[nodiscard]] std::int64_t MaxFollowerLag() const;

  // Called after a leader append so the single-replica high-water mark and any
  // satisfied waiters advance without waiting for a follower.
  void OnLeaderAppend(Offset new_log_end_offset, std::int64_t now_ms);

  // Registers a produce request awaiting its acknowledgement condition. If the
  // condition already holds, `waiter.on_complete` runs immediately (on the
  // calling thread) and nothing is stored.
  void AddWaiter(DurabilityWaiter waiter, std::int64_t now_ms);

  // Completes every waiter whose condition now holds. Called after a flush,
  // after a follower acknowledgement, and from the maintenance sweep.
  // Returns how many completed.
  std::size_t CompleteSatisfiedWaiters(std::int64_t now_ms);

  // Fails every waiter past its deadline with TIMEOUT. Returns how many.
  std::size_t ExpireWaiters(std::int64_t now_ms);

  // Fails every outstanding waiter, used on shutdown and on leadership loss.
  std::size_t FailAllWaiters(const Status& reason);

  [[nodiscard]] std::size_t PendingWaiterCount() const;

  // Number of replicas that have persisted through `offset`, leader included.
  [[nodiscard]] std::size_t PersistedReplicaCount(Offset offset) const;

  struct Stats {
    Offset log_start_offset = 0;
    Offset log_end_offset = 0;
    Offset high_water_mark = 0;
    Offset flushed_offset = 0;
    std::uint64_t total_bytes = 0;
    std::size_t segment_count = 0;
    std::size_t pending_waiters = 0;
    std::int64_t max_follower_lag = 0;
    std::size_t in_sync_replicas = 1;
    bool leader = false;
  };

  [[nodiscard]] Stats GetStats() const;

 private:
  // Caller must hold `mutex_`. Returns the recomputed high-water mark.
  [[nodiscard]] Offset RecomputeHighWaterMarkLocked();

  // Caller must hold `mutex_`. Moves satisfied waiters into `out`.
  void ExtractSatisfiedLocked(std::vector<DurabilityWaiter>& out, std::int64_t now_ms);

  TopicPartition topic_partition_;
  BrokerId self_;
  std::unique_ptr<storage::PartitionLog> log_;

  mutable std::mutex mutex_;
  metadata::PartitionAssignment assignment_;
  std::map<std::int32_t, FollowerState> followers_;
  std::vector<DurabilityWaiter> waiters_;

  std::atomic<Offset> high_water_mark_{0};
};

}  // namespace pulselog::broker

#endif  // PULSELOG_BROKER_PARTITION_REPLICA_H_
