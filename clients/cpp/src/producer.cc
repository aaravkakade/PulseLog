#include <algorithm>
#include <thread>

#include "pulselog/base/clock.h"
#include "pulselog/base/logging.h"
#include "pulselog/client/client.h"
#include "pulselog/protocol/codec.h"

namespace pulselog::client {
namespace {

constexpr std::string_view kComponent = "client.producer";

}  // namespace

Producer::Producer(ClientContext& context, ProducerConfig config)
    : context_(context), config_(config) {}

Producer::~Producer() {
  if (pending_count_ > 0) {
    // Buffered records that were never flushed were never acknowledged, so
    // dropping them silently would be a lie about delivery. Flush, and say so
    // if that fails.
    auto result = Flush();
    if (!result.ok()) {
      PL_ERROR(kComponent) << "discarding " << pending_count_
                           << " buffered records: final flush failed: "
                           << result.status().ToString();
    }
  }
}

Result<PartitionIndex> Producer::RouteFor(const std::string& topic,
                                          const OutboundRecord& record) {
  if (config_.forced_partition >= 0) return PartitionIndex{config_.forced_partition};

  auto partition_count = context_.PartitionCount(topic);
  if (!partition_count.ok()) {
    if (partition_count.status().code() != ErrorCode::kNotFound) return partition_count.status();
    // The topic does not exist yet. Send to partition 0: if the broker has
    // auto-create enabled it will create the topic and answer, and the next
    // send routes properly against refreshed metadata. If auto-create is off
    // the broker answers NOT_FOUND, which is what the caller should see.
    return PartitionIndex{0};
  }
  if (partition_count.value() <= 0) return Unavailable("topic '" + topic + "' has no partitions");

  if (record.key_is_null) {
    // No key means no ordering requirement, so spread the load.
    return metadata::PartitionRoundRobin(round_robin_++, partition_count.value());
  }
  // Same key always lands on the same partition, which is what makes
  // per-key ordering a usable guarantee.
  return metadata::PartitionForKey(AsBytes(record.key), partition_count.value());
}

Result<DeliveryResult> Producer::SendEncoded(const std::string& topic, PartitionIndex partition,
                                             ByteSpan records, std::uint32_t record_count) {
  protocol::ProduceRequest request;
  request.topic = topic;
  request.partition = partition;
  request.acks = config_.acks;
  request.timeout_ms = config_.request_timeout_ms;
  request.record_count = record_count;
  request.records = records;

  scratch_.Clear();
  protocol::PayloadWriter writer(scratch_);
  request.Encode(writer);

  const RetryPolicy& retry = context_.config().retry;
  std::int64_t backoff_ms = retry.initial_backoff_ms;
  Status last_error = Unavailable("no attempt made");

  for (int attempt = 0; attempt < retry.max_attempts; ++attempt) {
    if (attempt > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
      backoff_ms = std::min(static_cast<std::int64_t>(
                                static_cast<double>(backoff_ms) * retry.backoff_multiplier),
                            retry.max_backoff_ms);
      ++stats_.retries;
    }

    auto connection = context_.LeaderOrAny(topic, partition);
    if (!connection.ok()) {
      last_error = connection.status();
      if (!IsRetryable(last_error.code())) break;
      continue;
    }

    const std::int64_t started = MonotonicNanos();
    auto frame = connection.value()->Call(protocol::OpCode::kProduce, context_.NextRequestId(),
                                          scratch_.Readable());
    if (!frame.ok()) {
      last_error = frame.status();
      // A dead connection invalidates our idea of where the leader is.
      context_.InvalidateTopic(topic);
      if (!IsRetryable(last_error.code())) break;
      continue;
    }

    protocol::ProduceResponse response;
    const Status decoded = ClientContext::DecodeResponse(frame->payload, response);
    if (!decoded.ok()) {
      last_error = decoded;
      if (decoded.code() == ErrorCode::kNotLeader) {
        // Leadership moved (or we had stale metadata). Refresh and retry.
        context_.InvalidateTopic(topic);
      }
      if (!IsRetryable(decoded.code())) break;
      continue;
    }

    DeliveryResult result;
    result.topic = topic;
    result.partition = partition;
    result.base_offset = response.base_offset;
    result.last_offset = response.last_offset;
    result.high_water_mark = response.high_water_mark;
    result.record_count = record_count;
    result.bytes = records.size();
    result.latency_nanos = MonotonicNanos() - started;

    ++stats_.batches_sent;
    stats_.records_sent += record_count;
    stats_.bytes_sent += records.size();
    return result;
  }

  ++stats_.failures;
  return last_error.WithContext("producing to " + topic + "-" +
                                std::to_string(partition.value()));
}

Result<DeliveryResult> Producer::Send(const std::string& topic, const OutboundRecord& record) {
  PL_ASSIGN_OR_RETURN(const PartitionIndex partition, RouteFor(topic, record));

  // Batching only ever combines records for the same topic and partition; a
  // batch is a single append to a single log.
  const bool different_target = pending_count_ > 0 &&
                                (pending_topic_ != topic || pending_partition_ != partition);
  if (different_target) {
    PL_ASSIGN_OR_RETURN(const DeliveryResult flushed, Flush());
    (void)flushed;
  }

  if (pending_count_ == 0) {
    pending_.Clear();
    pending_topic_ = topic;
    pending_partition_ = partition;
    first_buffered_nanos_ = MonotonicNanos();
  }

  protocol::AppendRecord(pending_, /*offset=*/0, record.timestamp, /*attributes=*/0,
                         record.key_is_null, AsBytes(record.key), AsBytes(record.value));
  ++pending_count_;

  const bool by_count = pending_count_ >= config_.batch_records;
  const bool by_bytes = pending_.ReadableBytes() >= config_.batch_bytes;
  const bool by_time = config_.linger_ms > 0 &&
                       (MonotonicNanos() - first_buffered_nanos_) >= config_.linger_ms * 1'000'000;
  if (by_count || by_bytes || by_time) return Flush();

  // Buffered but not sent: report zero records delivered so a caller that
  // checks cannot mistake this for an acknowledgement.
  DeliveryResult buffered;
  buffered.topic = topic;
  buffered.partition = partition;
  return buffered;
}

Result<DeliveryResult> Producer::SendBatch(const std::string& topic, PartitionIndex partition,
                                           const std::vector<OutboundRecord>& records) {
  if (records.empty()) return InvalidArgument("cannot send an empty batch");
  if (pending_count_ > 0) {
    PL_ASSIGN_OR_RETURN(const DeliveryResult flushed, Flush());
    (void)flushed;
  }

  pending_.Clear();
  for (const auto& record : records) {
    protocol::AppendRecord(pending_, 0, record.timestamp, 0, record.key_is_null,
                           AsBytes(record.key), AsBytes(record.value));
  }
  return SendEncoded(topic, partition, pending_.Readable(),
                     static_cast<std::uint32_t>(records.size()));
}

Result<DeliveryResult> Producer::Flush() {
  if (pending_count_ == 0) return DeliveryResult{};

  const std::uint32_t count = static_cast<std::uint32_t>(pending_count_);
  const std::string topic = pending_topic_;
  const PartitionIndex partition = pending_partition_;

  // Clear the buffer state before sending: if the send fails and the caller
  // retries, the records must not be sent twice.
  pending_count_ = 0;
  auto result = SendEncoded(topic, partition, pending_.Readable(), count);
  pending_.Clear();
  return result;
}

}  // namespace pulselog::client
