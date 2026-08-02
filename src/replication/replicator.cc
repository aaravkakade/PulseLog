#include "pulselog/replication/replicator.h"

#include <algorithm>
#include <chrono>

#include "pulselog/base/clock.h"
#include "pulselog/base/logging.h"
#include "pulselog/protocol/codec.h"
#include "pulselog/protocol/frame.h"
#include "pulselog/protocol/messages.h"

namespace pulselog::replication {
namespace {

constexpr std::string_view kComponent = "replication";

// How often an otherwise idle replication connection is probed for liveness.
// This bounds how long a dead follower can stay in the in-sync replica set,
// and therefore how long a quorum write can block on a replica that is never
// coming back.
constexpr std::int64_t kLivenessProbeIntervalMs = 200;

}  // namespace

Replicator::Replicator(ReplicatorOptions options,
                       broker::PartitionManager& partitions,
                       metadata::ClusterMetadata& cluster)
    : options_(options), partitions_(partitions), cluster_(cluster) {}

Replicator::~Replicator() {
  Stop();
}

Status Replicator::Start() {
  if (running_.exchange(true, std::memory_order_acq_rel)) return OkStatus();

  for (const auto& broker : cluster_.Brokers()) {
    if (broker.id == options_.self) continue;
    senders_.push_back(std::make_unique<Sender>(broker.id, *this));
  }
  for (auto& sender : senders_) sender->Start();

  PL_INFO(kComponent) << "replication started peers=" << senders_.size();
  return OkStatus();
}

void Replicator::Stop() {
  if (!running_.exchange(false, std::memory_order_acq_rel)) return;
  for (auto& sender : senders_) sender->Stop();
  senders_.clear();
  PL_INFO(kComponent) << "replication stopped"
                      << " batches_sent=" << batches_sent_.load(std::memory_order_relaxed)
                      << " records_sent=" << records_sent_.load(std::memory_order_relaxed);
}

void Replicator::NotifyAppend(const TopicPartition& topic_partition) {
  // Wake every peer: the cost of a spurious wakeup is one loop iteration that
  // finds nothing to send, which is far cheaper than looking up the replica
  // set on the produce hot path.
  (void)topic_partition;
  for (auto& sender : senders_) sender->Notify();
}

ReplicationStats Replicator::GetStats() const {
  ReplicationStats stats;
  stats.batches_sent = batches_sent_.load(std::memory_order_relaxed);
  stats.records_sent = records_sent_.load(std::memory_order_relaxed);
  stats.bytes_sent = bytes_sent_.load(std::memory_order_relaxed);
  stats.send_failures = send_failures_.load(std::memory_order_relaxed);
  stats.reconnects = reconnects_.load(std::memory_order_relaxed);
  stats.rejected_batches = rejected_batches_.load(std::memory_order_relaxed);
  for (const auto& sender : senders_) {
    const auto view = sender->Describe();
    if (view.connected) ++stats.connected_followers;
    stats.max_lag_records = std::max(stats.max_lag_records, view.max_lag_records);
  }
  return stats;
}

std::vector<Replicator::FollowerView> Replicator::DescribeFollowers() const {
  std::vector<FollowerView> views;
  views.reserve(senders_.size());
  for (const auto& sender : senders_) views.push_back(sender->Describe());
  return views;
}

// --- Sender ----------------------------------------------------------------

Replicator::Sender::Sender(BrokerId peer, Replicator& parent) : peer_(peer), parent_(parent) {
  net::SyncClientOptions options;
  options.connect_timeout_ms = parent.options_.timeout_ms;
  options.request_timeout_ms = parent.options_.timeout_ms;
  client_ = net::SyncClient(options);
  backoff_ms_ = parent.options_.reconnect_backoff_ms;
}

Replicator::Sender::~Sender() {
  Stop();
}

void Replicator::Sender::Start() {
  thread_ = NamedThread("pl-repl-" + std::to_string(peer_.value()), [this] { Run(); });
}

void Replicator::Sender::Stop() {
  if (stopping_.exchange(true, std::memory_order_acq_rel)) return;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    notified_ = true;
    cv_.notify_all();
  }
  thread_.Join();
}

void Replicator::Sender::Notify() {
  std::lock_guard<std::mutex> lock(mutex_);
  notified_ = true;
  cv_.notify_one();
}

Status Replicator::Sender::EnsureConnected() {
  if (connected_ && client_.connected()) return OkStatus();

  const auto endpoint = parent_.cluster_.FindBroker(peer_);
  if (!endpoint.has_value()) {
    return NotFound("broker " + std::to_string(peer_.value()) + " is not in the cluster config");
  }

  const Status status = client_.Connect(net::Endpoint{endpoint->host, endpoint->port});
  if (!status.ok()) {
    connected_ = false;
    return status;
  }

  connected_ = true;
  backoff_ms_ = parent_.options_.reconnect_backoff_ms;
  parent_.reconnects_.fetch_add(1, std::memory_order_relaxed);
  PL_INFO(kComponent) << "connected to follower"
                      << " peer=" << peer_.value() << " endpoint=" << endpoint->host << ':'
                      << endpoint->port;
  return OkStatus();
}

Result<std::size_t> Replicator::Sender::SendPending() {
  std::size_t batches = 0;
  const std::int64_t now_ms = WallClockMillis();
  std::int64_t worst_lag = 0;

  for (broker::PartitionReplica* replica : parent_.partitions_.All()) {
    if (!replica->is_leader()) continue;
    const auto assignment = replica->assignment();
    if (!assignment.HasReplica(peer_)) continue;

    // Where this follower says it is. On a fresh connection that is 0, so the
    // first round streams from the log start -- which is exactly the
    // catch-up-after-reconnect path.
    Offset follower_offset = 0;
    Offset follower_flushed = 0;
    for (const auto& follower : replica->Followers()) {
      if (follower.broker == peer_) {
        follower_offset = follower.log_end_offset;
        follower_flushed = follower.flushed_offset;
        break;
      }
    }

    const Offset log_start = replica->log().LogStartOffset();
    const Offset log_end = replica->log().LogEndOffset();
    if (follower_offset < log_start) {
      // The follower is behind retention: everything it is missing has been
      // deleted. Restart it from the current log start; the gap is real data
      // loss for that replica and is logged as such.
      PL_WARN(kComponent) << "follower is behind the log start; skipping deleted records"
                          << " peer=" << peer_.value()
                          << " partition=" << replica->topic_partition().ToString()
                          << " follower_offset=" << follower_offset << " log_start=" << log_start;
      follower_offset = log_start;
    }
    worst_lag = std::max(worst_lag, log_end - follower_offset);

    // Caught up *and* durable: nothing to say.
    //
    // The `follower_flushed` half matters more than it looks. A follower
    // reports its flushed offset in the response to a batch, but its own
    // flusher runs afterwards -- so the flushed offset in that response is
    // always stale. Without a probe, the leader would never learn that the
    // last batch became durable on the follower, and every quorum
    // acknowledgement would wait for its deadline instead of completing.
    const bool caught_up = follower_offset >= log_end;
    const bool follower_durable = follower_flushed >= log_end;
    if (caught_up && follower_durable) continue;

    records_.Clear();
    std::uint32_t record_count = 0;
    Offset base_offset = follower_offset;
    if (!caught_up) {
      auto read = replica->log().Read(follower_offset, parent_.options_.max_bytes, records_);
      if (!read.ok()) {
        PL_WARN(kComponent) << "cannot read for replication"
                            << " partition=" << replica->topic_partition().ToString()
                            << " offset=" << follower_offset
                            << " error=" << read.status().ToString();
        continue;
      }
      if (read->record_count == 0) continue;
      record_count = read->record_count;
      base_offset = read->base_offset;
    }
    // When `caught_up` is true this is a zero-record progress probe: it asks
    // the follower to report its current durability without shipping data.

    protocol::ReplicateRequest request;
    request.topic = replica->topic_partition().topic;
    request.partition = replica->topic_partition().partition;
    request.leader_id = parent_.options_.self;
    request.leader_epoch = assignment.leader_epoch;
    request.base_offset = base_offset;
    request.prev_offset = base_offset - 1;
    request.leader_high_water_mark = replica->HighWaterMark();
    request.leader_log_start_offset = log_start;
    request.record_count = record_count;
    request.records = records_.Readable();

    scratch_.Clear();
    protocol::PayloadWriter writer(scratch_);
    request.Encode(writer);

    auto response_frame =
        client_.Call(protocol::OpCode::kReplicate, next_request_id_++, scratch_.Readable());
    if (!response_frame.ok()) {
      connected_ = false;
      parent_.send_failures_.fetch_add(1, std::memory_order_relaxed);
      return response_frame.status();
    }

    protocol::ReplicateResponse response;
    protocol::PayloadReader reader(response_frame->payload);
    if (!response.Decode(reader)) {
      connected_ = false;
      return ProtocolError("malformed replicate response from broker " +
                           std::to_string(peer_.value()));
    }

    if (!response.header.ok()) {
      parent_.rejected_batches_.fetch_add(1, std::memory_order_relaxed);
      if (response.header.error == ErrorCode::kOutOfRange) {
        // The follower's log does not continue from where we sent. Its
        // reported log end offset tells us where to resume; the next round
        // picks that up through OnFollowerProgress below.
        PL_WARN(kComponent) << "follower rejected a batch as discontinuous"
                            << " peer=" << peer_.value()
                            << " partition=" << replica->topic_partition().ToString()
                            << " sent_base=" << request.base_offset
                            << " follower_end=" << response.log_end_offset;
        (void)replica->OnFollowerProgress(
            peer_, response.log_end_offset, response.flushed_offset, now_ms);
        continue;
      }
      if (response.header.error == ErrorCode::kNotLeader) {
        // The follower has seen a newer epoch. We are a stale leader for this
        // partition and must stop trying to write to it.
        PL_ERROR(kComponent) << "follower reports a newer leader epoch; stopping replication"
                             << " peer=" << peer_.value()
                             << " partition=" << replica->topic_partition().ToString()
                             << " our_epoch=" << assignment.leader_epoch;
        continue;
      }
      PL_WARN(kComponent) << "follower rejected a batch"
                          << " peer=" << peer_.value()
                          << " partition=" << replica->topic_partition().ToString()
                          << " error=" << ErrorCodeName(response.header.error) << " message=\""
                          << response.header.error_message << "\"";
      continue;
    }

    // Progress advances the high-water mark and can satisfy quorum
    // acknowledgements immediately.
    (void)replica->OnFollowerProgress(
        peer_, response.log_end_offset, response.flushed_offset, now_ms);

    last_success_ms_.store(now_ms, std::memory_order_relaxed);
    if (record_count == 0) continue;  // A probe is not a batch.

    ++batches;
    batches_sent_.fetch_add(1, std::memory_order_relaxed);
    parent_.batches_sent_.fetch_add(1, std::memory_order_relaxed);
    parent_.records_sent_.fetch_add(record_count, std::memory_order_relaxed);
    parent_.bytes_sent_.fetch_add(records_.ReadableBytes(), std::memory_order_relaxed);
  }

  max_lag_records_.store(worst_lag, std::memory_order_relaxed);
  return batches;
}

void Replicator::Sender::Run() {
  PL_DEBUG(kComponent) << "sender started peer=" << peer_.value();

  while (!stopping_.load(std::memory_order_acquire)) {
    const Status connect = EnsureConnected();
    if (!connect.ok()) {
      // A follower that cannot be reached must eventually leave the in-sync
      // set, or quorum writes would block forever waiting for a dead replica.
      const std::int64_t now_ms = WallClockMillis();
      for (broker::PartitionReplica* replica : parent_.partitions_.All()) {
        if (!replica->is_leader()) continue;
        if (!replica->assignment().HasReplica(peer_)) continue;
        replica->MarkFollowerOutOfSync(peer_, now_ms);
      }

      PL_DEBUG(kComponent) << "cannot reach follower peer=" << peer_.value()
                           << " retry_ms=" << backoff_ms_ << " error=" << connect.ToString();
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait_for(lock, std::chrono::milliseconds(backoff_ms_), [this] {
        return stopping_.load(std::memory_order_acquire);
      });
      backoff_ms_ = std::min(backoff_ms_ * 2, parent_.options_.reconnect_backoff_max_ms);
      continue;
    }

    auto sent = SendPending();
    if (!sent.ok()) {
      PL_WARN(kComponent) << "replication round failed"
                          << " peer=" << peer_.value() << " error=" << sent.status().ToString();
      client_.Close();
      connected_ = false;
      continue;
    }

    if (sent.value() > 0) continue;  // More may already be waiting.

    // Liveness probe.
    //
    // Without this, a follower is only discovered to be dead when the leader
    // happens to have data for it. An idle partition whose followers died
    // would keep them in the in-sync set forever, and every quorum write
    // would then block until its deadline waiting for replicas that are never
    // coming back. A cheap HEALTH round trip on an otherwise idle connection
    // turns that into a bounded eviction.
    const std::int64_t idle_now_ms = WallClockMillis();
    if (idle_now_ms - last_probe_ms_ >= kLivenessProbeIntervalMs) {
      last_probe_ms_ = idle_now_ms;
      auto probe = client_.Call(protocol::OpCode::kHealth, next_request_id_++, ByteSpan{});
      if (!probe.ok()) {
        PL_DEBUG(kComponent) << "liveness probe failed peer=" << peer_.value() << ": "
                             << probe.status().ToString();
        client_.Close();
        connected_ = false;
        continue;
      }
      last_success_ms_.store(idle_now_ms, std::memory_order_relaxed);
    }

    // Nothing to send: wait for a notification from the produce path, or poll
    // at the configured interval so a missed notification costs one interval.
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait_for(lock, std::chrono::milliseconds(parent_.options_.interval_ms), [this] {
      return notified_ || stopping_.load(std::memory_order_acquire);
    });
    notified_ = false;
  }

  client_.Close();
  PL_DEBUG(kComponent) << "sender stopped peer=" << peer_.value();
}

Replicator::FollowerView Replicator::Sender::Describe() const {
  FollowerView view;
  view.broker = peer_;
  view.connected = connected_;
  view.batches_sent = batches_sent_.load(std::memory_order_relaxed);
  view.max_lag_records = max_lag_records_.load(std::memory_order_relaxed);
  view.last_success_ms = last_success_ms_.load(std::memory_order_relaxed);
  return view;
}

}  // namespace pulselog::replication
