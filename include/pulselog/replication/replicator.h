// Leader-side replication.
//
// Model: the leader **pushes**. One thread per follower broker maintains a
// single connection and, for every partition this broker leads with that
// broker as a replica, streams records from the follower's reported log end
// offset. The follower's response carries its new log end offset and flushed
// offset, which is what advances the high-water mark and satisfies quorum
// acknowledgements.
//
// Why push rather than Kafka-style follower pull: with a small, statically
// assigned cluster, push removes a round trip from the acknowledgement path --
// the leader already knows what it wants to send. The catch-up case still
// needs a pull (REPLICA_FETCH), and the follower asks for one when it detects
// a gap.
//
// What this handles:
//   * follower lag -> the follower leaves the in-sync set, the high-water mark
//     advances without it, and quorum requirements are recomputed
//   * network interruption -> the sender reconnects with backoff and resumes
//     from the follower's reported offset
//   * duplicate batches -> the follower rejects any batch whose base offset
//     does not equal its log end offset, so a resend is a no-op rather than a
//     duplicate append
//   * stale leader -> every batch carries the leader epoch; a follower that
//     has seen a higher epoch rejects it
//
// What this does NOT do: elect a leader. Leadership is static. If a leader
// dies its partitions are unavailable for writes until it returns.
#ifndef PULSELOG_REPLICATION_REPLICATOR_H_
#define PULSELOG_REPLICATION_REPLICATOR_H_

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "pulselog/base/status.h"
#include "pulselog/base/types.h"
#include "pulselog/broker/partition_manager.h"
#include "pulselog/concurrency/thread_util.h"
#include "pulselog/metadata/cluster_metadata.h"
#include "pulselog/net/sync_client.h"

namespace pulselog::replication {

struct ReplicatorOptions {
  BrokerId self{0};
  std::int64_t interval_ms = 2;
  std::int64_t timeout_ms = 5000;
  std::uint32_t max_bytes = 1U << 20U;
  // A follower silent for longer than this leaves the in-sync replica set.
  std::int64_t lag_max_ms = 10'000;
  std::int64_t reconnect_backoff_ms = 200;
  std::int64_t reconnect_backoff_max_ms = 5000;
};

struct ReplicationStats {
  std::uint64_t batches_sent = 0;
  std::uint64_t records_sent = 0;
  std::uint64_t bytes_sent = 0;
  std::uint64_t send_failures = 0;
  std::uint64_t reconnects = 0;
  std::uint64_t rejected_batches = 0;
  std::int64_t max_lag_records = 0;
  std::size_t connected_followers = 0;
};

class Replicator {
 public:
  Replicator(ReplicatorOptions options, broker::PartitionManager& partitions,
             metadata::ClusterMetadata& cluster);

  Replicator(const Replicator&) = delete;
  Replicator& operator=(const Replicator&) = delete;

  ~Replicator();

  // Starts one sender thread per peer broker.
  [[nodiscard]] Status Start();

  void Stop();

  // Wakes the sender for every follower of `topic_partition`. Called after a
  // leader append so a quorum acknowledgement does not have to wait out the
  // polling interval.
  void NotifyAppend(const TopicPartition& topic_partition);

  [[nodiscard]] ReplicationStats GetStats() const;

  // Per-follower view for the dashboard and the topology endpoint.
  struct FollowerView {
    BrokerId broker{-1};
    bool connected = false;
    std::uint64_t batches_sent = 0;
    std::int64_t max_lag_records = 0;
    std::int64_t last_success_ms = 0;
  };

  [[nodiscard]] std::vector<FollowerView> DescribeFollowers() const;

 private:
  // One per peer broker: owns the connection and the send loop.
  class Sender {
   public:
    Sender(BrokerId peer, Replicator& parent);

    Sender(const Sender&) = delete;
    Sender& operator=(const Sender&) = delete;

    ~Sender();

    void Start();

    void Stop();

    void Notify();

    [[nodiscard]] FollowerView Describe() const;

    [[nodiscard]] BrokerId peer() const noexcept { return peer_; }

   private:
    void Run();

    // Sends one round of pending records for every partition this peer
    // replicates from us. Returns how many batches went out.
    [[nodiscard]] Result<std::size_t> SendPending();

    [[nodiscard]] Status EnsureConnected();

    BrokerId peer_;
    Replicator& parent_;
    net::SyncClient client_;
    bool connected_ = false;
    std::int64_t backoff_ms_ = 0;

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    bool notified_ = false;
    std::atomic<bool> stopping_{false};

    std::atomic<std::uint64_t> batches_sent_{0};
    std::atomic<std::int64_t> last_success_ms_{0};
    std::atomic<std::int64_t> max_lag_records_{0};
    RequestId next_request_id_ = 1;
    ByteBuffer scratch_;
    ByteBuffer records_;
    NamedThread thread_;
  };

  ReplicatorOptions options_;
  broker::PartitionManager& partitions_;
  metadata::ClusterMetadata& cluster_;

  std::vector<std::unique_ptr<Sender>> senders_;
  std::atomic<bool> running_{false};

  std::atomic<std::uint64_t> batches_sent_{0};
  std::atomic<std::uint64_t> records_sent_{0};
  std::atomic<std::uint64_t> bytes_sent_{0};
  std::atomic<std::uint64_t> send_failures_{0};
  std::atomic<std::uint64_t> reconnects_{0};
  std::atomic<std::uint64_t> rejected_batches_{0};
};

}  // namespace pulselog::replication

#endif  // PULSELOG_REPLICATION_REPLICATOR_H_
