#include "pulselog/broker/partition_replica.h"

#include <algorithm>
#include <string>

#include "pulselog/base/clock.h"
#include "pulselog/base/logging.h"

namespace pulselog::broker {
namespace {

constexpr std::string_view kComponent = "broker.partition";

}  // namespace

PartitionReplica::PartitionReplica(TopicPartition topic_partition,
                                   metadata::PartitionAssignment assignment,
                                   BrokerId self,
                                   std::unique_ptr<storage::PartitionLog> log)
    : topic_partition_(std::move(topic_partition)),
      self_(self),
      log_(std::move(log)),
      assignment_(std::move(assignment)) {
  for (const BrokerId replica : assignment_.replicas) {
    if (replica == self_) continue;
    FollowerState state;
    state.broker = replica;
    // A follower starts assumed-in-sync but at offset 0. Until it reports
    // progress the high-water mark cannot advance past 0, which is the correct
    // conservative position: nothing has been confirmed replicated yet.
    state.log_end_offset = 0;
    state.flushed_offset = 0;
    followers_.emplace(replica.value(), state);
  }
  high_water_mark_.store(is_leader() && followers_.empty() ? log_->LogEndOffset() : 0,
                         std::memory_order_release);
}

PartitionReplica::~PartitionReplica() {
  FailAllWaiters(Status{ErrorCode::kUnavailable, "partition is shutting down"});
}

metadata::PartitionAssignment PartitionReplica::assignment() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return assignment_;
}

void PartitionReplica::SetAssignment(metadata::PartitionAssignment assignment) {
  std::lock_guard<std::mutex> lock(mutex_);
  assignment_ = std::move(assignment);
  // Drop followers no longer in the replica set; add new ones at offset 0.
  for (auto it = followers_.begin(); it != followers_.end();) {
    const BrokerId broker{it->first};
    if (!assignment_.HasReplica(broker) || broker == self_) {
      it = followers_.erase(it);
    } else {
      ++it;
    }
  }
  for (const BrokerId replica : assignment_.replicas) {
    if (replica == self_) continue;
    if (followers_.find(replica.value()) == followers_.end()) {
      FollowerState state;
      state.broker = replica;
      followers_.emplace(replica.value(), state);
    }
  }
}

Offset PartitionReplica::RecomputeHighWaterMarkLocked() {
  const Offset leader_end = log_->LogEndOffset();
  if (assignment_.leader != self_) {
    // A follower's high-water mark is whatever the leader told it; it is set
    // directly by the replication apply path, not computed here.
    return high_water_mark_.load(std::memory_order_acquire);
  }

  // The high-water mark is the highest offset present on every in-sync
  // replica, so it is the minimum of their log end offsets (the leader's
  // included). Consumers never see a record that could still be lost if the
  // leader died and an in-sync follower took over.
  Offset watermark = leader_end;
  std::size_t in_sync_count = 1;
  for (const auto& [id, follower] : followers_) {
    if (!follower.in_sync) continue;
    ++in_sync_count;
    watermark = std::min(watermark, follower.log_end_offset);
  }
  // With no in-sync followers, the leader alone defines the mark.
  if (in_sync_count == 1) watermark = leader_end;

  const Offset previous = high_water_mark_.load(std::memory_order_acquire);
  // The high-water mark must never move backwards: a consumer that read up to
  // it would otherwise see records disappear.
  if (watermark > previous) {
    high_water_mark_.store(watermark, std::memory_order_release);
    return watermark;
  }
  return previous;
}

void PartitionReplica::OnLeaderAppend(Offset new_log_end_offset, std::int64_t now_ms) {
  std::vector<DurabilityWaiter> satisfied;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    (void)new_log_end_offset;
    (void)RecomputeHighWaterMarkLocked();
    ExtractSatisfiedLocked(satisfied, now_ms);
  }
  const Offset watermark = high_water_mark_.load(std::memory_order_acquire);
  for (auto& waiter : satisfied)
    waiter.on_complete(OkStatus(), watermark, waiter.local_flush_nanos);
}

Offset PartitionReplica::OnFollowerProgress(BrokerId follower,
                                            Offset log_end_offset,
                                            Offset flushed_offset,
                                            std::int64_t now_ms) {
  std::vector<DurabilityWaiter> satisfied;
  Offset watermark = 0;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = followers_.find(follower.value());
    if (it == followers_.end()) {
      // A broker that is not in this partition's replica set. Ignore it rather
      // than let it influence the high-water mark.
      return high_water_mark_.load(std::memory_order_acquire);
    }
    FollowerState& state = it->second;
    // Progress reports can arrive out of order after a reconnect; never move a
    // follower's recorded position backwards on the basis of a stale message.
    state.log_end_offset = std::max(state.log_end_offset, log_end_offset);
    state.flushed_offset = std::max(state.flushed_offset, flushed_offset);
    state.last_ack_ms = now_ms;
    ++state.acks_received;
    if (!state.in_sync) {
      state.in_sync = true;
      PL_INFO(kComponent) << "follower rejoined the in-sync set"
                          << " partition=" << topic_partition_.ToString()
                          << " follower=" << follower.value()
                          << " log_end_offset=" << state.log_end_offset;
    }

    watermark = RecomputeHighWaterMarkLocked();
    ExtractSatisfiedLocked(satisfied, now_ms);
  }
  for (auto& waiter : satisfied)
    waiter.on_complete(OkStatus(), watermark, waiter.local_flush_nanos);
  return watermark;
}

void PartitionReplica::MarkFollowerOutOfSync(BrokerId follower, std::int64_t now_ms) {
  std::vector<DurabilityWaiter> satisfied;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = followers_.find(follower.value());
    if (it == followers_.end() || !it->second.in_sync) return;
    it->second.in_sync = false;
    PL_WARN(kComponent) << "follower left the in-sync set"
                        << " partition=" << topic_partition_.ToString()
                        << " follower=" << follower.value()
                        << " follower_offset=" << it->second.log_end_offset
                        << " leader_offset=" << log_->LogEndOffset();

    // Removing a lagging follower lets the high-water mark advance again --
    // the remaining in-sync replicas are the ones that actually have the data.
    (void)RecomputeHighWaterMarkLocked();
    ExtractSatisfiedLocked(satisfied, now_ms);
  }
  const Offset watermark = high_water_mark_.load(std::memory_order_acquire);
  for (auto& waiter : satisfied)
    waiter.on_complete(OkStatus(), watermark, waiter.local_flush_nanos);
}

std::vector<FollowerState> PartitionReplica::Followers() const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<FollowerState> result;
  result.reserve(followers_.size());
  for (const auto& [id, state] : followers_) result.push_back(state);
  return result;
}

std::int64_t PartitionReplica::MaxFollowerLag() const {
  const Offset leader_end = log_->LogEndOffset();
  std::lock_guard<std::mutex> lock(mutex_);
  std::int64_t worst = 0;
  for (const auto& [id, state] : followers_) {
    worst = std::max(worst, leader_end - state.log_end_offset);
  }
  return worst;
}

std::size_t PartitionReplica::PersistedReplicaCount(Offset offset) const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::size_t count = log_->FlushedOffset() >= offset ? 1 : 0;
  for (const auto& [id, state] : followers_) {
    if (state.in_sync && state.flushed_offset >= offset) ++count;
  }
  return count;
}

void PartitionReplica::PublishWaiterCountLocked() {
  pending_waiters_.store(waiters_.size(), std::memory_order_release);
}

void PartitionReplica::ExtractSatisfiedLocked(std::vector<DurabilityWaiter>& out,
                                              std::int64_t now_ms) {
  if (waiters_.empty()) {
    PublishWaiterCountLocked();
    return;
  }

  const Offset flushed = log_->FlushedOffset();
  const Offset log_end = log_->LogEndOffset();
  const std::size_t quorum = assignment_.QuorumSize();

  // Non-const: stamps the moment the leader's own flush covered the record,
  // which is what separates local fsync cost from replication cost in the
  // latency breakdown. Called under mutex_, so the write needs no atomics.
  auto satisfied = [&](DurabilityWaiter& waiter) {
    switch (waiter.mode) {
      case AckMode::kNone:
        return true;
      case AckMode::kLeader:
        // The record is in the leader's log. Whether that is on media depends
        // on the flush policy, which is exactly what this mode promises.
        return log_end > waiter.required_offset;
      case AckMode::kQuorum: {
        // Count replicas that have *persisted* through the record.
        const bool locally_flushed = flushed > waiter.required_offset;
        if (locally_flushed && waiter.local_flush_nanos == 0) {
          waiter.local_flush_nanos = MonotonicNanos();
        }
        std::size_t persisted = locally_flushed ? 1 : 0;
        for (const auto& [id, follower] : followers_) {
          if (follower.in_sync && follower.flushed_offset > waiter.required_offset) ++persisted;
        }
        return persisted >= quorum;
      }
    }
    return false;
  };

  (void)now_ms;
  auto partition_point =
      std::stable_partition(waiters_.begin(), waiters_.end(), [&](DurabilityWaiter& waiter) {
        return !satisfied(waiter);
      });

  out.reserve(out.size() + static_cast<std::size_t>(waiters_.end() - partition_point));
  for (auto it = partition_point; it != waiters_.end(); ++it) {
    out.push_back(std::move(*it));
  }
  waiters_.erase(partition_point, waiters_.end());
  PublishWaiterCountLocked();
}

void PartitionReplica::AddWaiter(DurabilityWaiter waiter, std::int64_t now_ms) {
  std::vector<DurabilityWaiter> satisfied;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    waiters_.push_back(std::move(waiter));
    ExtractSatisfiedLocked(satisfied, now_ms);
  }
  // Callbacks run outside the lock: they post onto io loops and must never be
  // able to re-enter this partition's mutex.
  const Offset watermark = high_water_mark_.load(std::memory_order_acquire);
  for (auto& completed : satisfied)
    completed.on_complete(OkStatus(), watermark, completed.local_flush_nanos);
}

std::size_t PartitionReplica::CompleteSatisfiedWaiters(std::int64_t now_ms) {
  std::vector<DurabilityWaiter> satisfied;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    (void)RecomputeHighWaterMarkLocked();
    ExtractSatisfiedLocked(satisfied, now_ms);
  }
  const Offset watermark = high_water_mark_.load(std::memory_order_acquire);
  for (auto& waiter : satisfied)
    waiter.on_complete(OkStatus(), watermark, waiter.local_flush_nanos);
  return satisfied.size();
}

std::size_t PartitionReplica::ExpireWaiters(std::int64_t now_ms) {
  std::vector<DurabilityWaiter> expired;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto partition_point = std::stable_partition(
        waiters_.begin(), waiters_.end(), [now_ms](const DurabilityWaiter& waiter) {
          return waiter.deadline_ms > now_ms;
        });
    for (auto it = partition_point; it != waiters_.end(); ++it) expired.push_back(std::move(*it));
    waiters_.erase(partition_point, waiters_.end());
    PublishWaiterCountLocked();
  }

  if (!expired.empty()) {
    // Say which side was behind. A bare "timed out" gives nothing to act on:
    // the leader's own fsync, a follower's fsync, and a follower that stopped
    // reporting all look identical from the outside and have different fixes.
    std::string followers;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      for (const auto& [id, follower] : followers_) {
        followers += " peer=" + std::to_string(id) +
                     "(leo=" + std::to_string(follower.log_end_offset) +
                     ",flushed=" + std::to_string(follower.flushed_offset) +
                     (follower.in_sync ? ",in_sync" : ",OUT_OF_SYNC") + ")";
      }
    }
    PL_WARN(kComponent) << "produce acknowledgements timed out"
                        << " partition=" << topic_partition_.ToString()
                        << " count=" << expired.size()
                        << " required_offset=" << expired.front().required_offset
                        << " leader_log_end=" << log_->LogEndOffset()
                        << " leader_flushed=" << log_->FlushedOffset()
                        << " quorum=" << assignment_.QuorumSize() << followers;
  }
  const Offset watermark = high_water_mark_.load(std::memory_order_acquire);
  for (auto& waiter : expired) {
    waiter.on_complete(
        TimedOut("acknowledgement deadline exceeded before the durability condition was met"),
        watermark,
        waiter.local_flush_nanos);
  }
  return expired.size();
}

std::size_t PartitionReplica::FailAllWaiters(const Status& reason) {
  std::vector<DurabilityWaiter> pending;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    pending.swap(waiters_);
    PublishWaiterCountLocked();
  }
  const Offset watermark = high_water_mark_.load(std::memory_order_acquire);
  for (auto& waiter : pending) waiter.on_complete(reason, watermark, waiter.local_flush_nanos);
  return pending.size();
}

std::size_t PartitionReplica::PendingWaiterCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return waiters_.size();
}

PartitionReplica::Stats PartitionReplica::GetStats() const {
  const auto log_stats = log_->GetStats();
  Stats stats;
  stats.log_start_offset = log_stats.log_start_offset;
  stats.log_end_offset = log_stats.log_end_offset;
  stats.flushed_offset = log_stats.flushed_offset;
  stats.total_bytes = log_stats.total_bytes;
  stats.segment_count = log_stats.segment_count;
  stats.high_water_mark = high_water_mark_.load(std::memory_order_acquire);
  stats.max_follower_lag = MaxFollowerLag();

  std::lock_guard<std::mutex> lock(mutex_);
  stats.pending_waiters = waiters_.size();
  stats.leader = assignment_.leader == self_;
  stats.in_sync_replicas = 1;
  for (const auto& [id, follower] : followers_) {
    if (follower.in_sync) ++stats.in_sync_replicas;
  }
  return stats;
}

}  // namespace pulselog::broker
