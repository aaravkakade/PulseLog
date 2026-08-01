#include <algorithm>

#include "pulselog/base/clock.h"
#include "pulselog/base/logging.h"
#include "pulselog/client/client.h"
#include "pulselog/protocol/codec.h"

namespace pulselog::client {
namespace {

constexpr std::string_view kComponent = "client.consumer";

}  // namespace

Consumer::Consumer(ClientContext& context, ConsumerConfig config)
    : context_(context), config_(std::move(config)) {}

Consumer::~Consumer() {
  if (!member_id_.empty()) {
    const Status status = Leave();
    if (!status.ok()) {
      // Not fatal: the coordinator will expire the session. Leaving cleanly
      // just makes the rebalance immediate instead of session-timeout later.
      PL_DEBUG(kComponent) << "leave-group on shutdown failed: " << status.ToString();
    }
  }
}

Result<std::vector<InboundRecord>> Consumer::FetchInto(const std::string& topic,
                                                       PartitionIndex partition, Offset offset) {
  protocol::FetchRequest request;
  request.topic = topic;
  request.partition = partition;
  request.fetch_offset = offset;
  request.max_bytes = config_.max_bytes;
  request.min_bytes = config_.min_bytes;
  request.max_wait_ms = config_.max_wait_ms;
  request.isolation = config_.isolation;

  scratch_.Clear();
  protocol::PayloadWriter writer(scratch_);
  request.Encode(writer);

  PL_ASSIGN_OR_RETURN(net::SyncClient * connection, context_.LeaderFor(topic, partition));
  auto frame = connection->Call(protocol::OpCode::kFetch, context_.NextRequestId(),
                                scratch_.Readable());
  if (!frame.ok()) {
    context_.InvalidateTopic(topic);
    return frame.status();
  }

  protocol::FetchResponse response;
  const Status decoded = ClientContext::DecodeResponse(frame->payload, response);
  if (!decoded.ok()) {
    if (decoded.code() == ErrorCode::kNotLeader) context_.InvalidateTopic(topic);
    return decoded;
  }

  // The frame's payload view dies with the next call on this connection, so
  // the records are copied into a buffer this consumer owns. The returned
  // string_views point into that buffer and stay valid until the next Poll().
  response_buffer_.Clear();
  response_buffer_.Append(response.records);

  std::vector<InboundRecord> records;
  records.reserve(response.record_count);
  protocol::RecordIterator it(response_buffer_.Readable(), /*verify_crc=*/true);
  protocol::RecordView view;
  while (it.Next(view)) {
    InboundRecord record;
    record.offset = view.offset;
    record.timestamp = view.timestamp;
    record.key_is_null = view.key_is_null;
    record.key = view.key_str();
    record.value = view.value_str();
    records.push_back(record);
  }
  if (!it.status().ok()) {
    // A checksum failure here means corruption between the broker's disk and
    // this process. Surfacing it beats handing the caller bad records.
    return it.status().WithContext("decoding fetched records");
  }

  ++stats_.fetches;
  if (records.empty()) ++stats_.empty_fetches;
  stats_.records_received += records.size();
  stats_.bytes_received += response.records.size();

  const TopicPartition topic_partition{topic, partition};
  last_fetched_ = topic_partition;
  last_fetch_next_offset_ =
      records.empty() ? offset : records.back().offset + 1;
  positions_[topic_partition] = last_fetch_next_offset_;
  return records;
}

Result<std::vector<InboundRecord>> Consumer::Fetch(const std::string& topic,
                                                   PartitionIndex partition, Offset offset) {
  return FetchInto(topic, partition, offset);
}

Result<Offset> Consumer::ListOffset(const std::string& topic, PartitionIndex partition,
                                    TimestampMs timestamp) {
  protocol::ListOffsetsRequest request;
  request.topic = topic;
  request.partition = partition;
  request.timestamp = timestamp;

  scratch_.Clear();
  protocol::PayloadWriter writer(scratch_);
  request.Encode(writer);

  PL_ASSIGN_OR_RETURN(net::SyncClient * connection, context_.LeaderFor(topic, partition));
  PL_ASSIGN_OR_RETURN(auto frame, connection->Call(protocol::OpCode::kListOffsets,
                                                   context_.NextRequestId(), scratch_.Readable()));
  protocol::ListOffsetsResponse response;
  PL_RETURN_IF_ERROR(ClientContext::DecodeResponse(frame.payload, response));
  return response.offset;
}

Status Consumer::Join() {
  if (config_.group_id.empty()) {
    return InvalidArgument("group_id must be set to join a consumer group");
  }
  if (config_.topics.empty()) {
    return InvalidArgument("a group member must subscribe to at least one topic");
  }

  protocol::JoinGroupRequest request;
  request.group_id = config_.group_id;
  request.member_id = member_id_;
  request.topics = config_.topics;
  request.session_timeout_ms = config_.session_timeout_ms;
  request.strategy = config_.strategy;

  scratch_.Clear();
  protocol::PayloadWriter writer(scratch_);
  request.Encode(writer);

  PL_ASSIGN_OR_RETURN(net::SyncClient * connection, context_.AnyBroker());
  PL_ASSIGN_OR_RETURN(auto frame, connection->Call(protocol::OpCode::kJoinGroup,
                                                   context_.NextRequestId(), scratch_.Readable()));
  protocol::JoinGroupResponse response;
  PL_RETURN_IF_ERROR(ClientContext::DecodeResponse(frame.payload, response));

  member_id_ = response.member_id;
  generation_ = response.generation;
  assignment_ = response.assignment;
  next_partition_ = 0;
  last_heartbeat_ms_ = MonotonicNanos() / 1'000'000;
  ++stats_.rebalances;

  // Resolve a starting position for every newly assigned partition: the
  // group's committed offset if there is one, otherwise the configured end.
  std::map<TopicPartition, Offset> positions;
  for (const auto& topic_partition : assignment_) {
    const auto existing = positions_.find(topic_partition);
    if (existing != positions_.end()) {
      positions[topic_partition] = existing->second;
      continue;
    }
    auto committed = CommittedOffset(topic_partition.topic, topic_partition.partition);
    if (committed.ok() && committed.value() >= 0) {
      positions[topic_partition] = committed.value();
      continue;
    }
    auto start = ListOffset(topic_partition.topic, topic_partition.partition,
                            config_.start_from_earliest ? kEarliestOffset : kLatestOffset);
    positions[topic_partition] = start.ok() ? start.value() : 0;
  }
  positions_ = std::move(positions);

  PL_DEBUG(kComponent) << "joined group"
                       << " group=" << config_.group_id << " member=" << member_id_
                       << " generation=" << generation_.value()
                       << " partitions=" << assignment_.size();
  return OkStatus();
}

Status Consumer::Leave() {
  if (member_id_.empty()) return OkStatus();

  protocol::LeaveGroupRequest request;
  request.group_id = config_.group_id;
  request.member_id = member_id_;

  scratch_.Clear();
  protocol::PayloadWriter writer(scratch_);
  request.Encode(writer);

  PL_ASSIGN_OR_RETURN(net::SyncClient * connection, context_.AnyBroker());
  PL_ASSIGN_OR_RETURN(auto frame, connection->Call(protocol::OpCode::kLeaveGroup,
                                                   context_.NextRequestId(), scratch_.Readable()));
  protocol::LeaveGroupResponse response;
  const Status status = ClientContext::DecodeResponse(frame.payload, response);

  member_id_.clear();
  generation_ = Generation{-1};
  assignment_.clear();
  return status;
}

Status Consumer::Heartbeat() {
  if (member_id_.empty()) return OkStatus();

  protocol::HeartbeatRequest request;
  request.group_id = config_.group_id;
  request.member_id = member_id_;
  request.generation = generation_;

  scratch_.Clear();
  protocol::PayloadWriter writer(scratch_);
  request.Encode(writer);

  PL_ASSIGN_OR_RETURN(net::SyncClient * connection, context_.AnyBroker());
  PL_ASSIGN_OR_RETURN(auto frame, connection->Call(protocol::OpCode::kHeartbeat,
                                                   context_.NextRequestId(), scratch_.Readable()));
  protocol::HeartbeatResponse response;
  protocol::PayloadReader reader(frame.payload);
  if (!response.Decode(reader)) return ProtocolError("malformed heartbeat response");

  last_heartbeat_ms_ = MonotonicNanos() / 1'000'000;
  if (response.rejoin_required) {
    // The assignment we hold is stale. Re-joining is mandatory before doing
    // anything else, or two members could work the same partition.
    PL_DEBUG(kComponent) << "coordinator asked us to re-join"
                         << " group=" << config_.group_id
                         << " our_generation=" << generation_.value()
                         << " current=" << response.generation.value();
    return Join();
  }
  return response.header.ToStatus();
}

Result<std::vector<InboundRecord>> Consumer::Poll() {
  if (member_id_.empty()) {
    PL_RETURN_IF_ERROR(Join());
  }

  const std::int64_t now_ms = MonotonicNanos() / 1'000'000;
  if (now_ms - last_heartbeat_ms_ >= config_.heartbeat_interval_ms) {
    const Status status = Heartbeat();
    if (!status.ok() && status.code() != ErrorCode::kRebalanceInProgress) return status;
  }

  if (assignment_.empty()) return std::vector<InboundRecord>{};

  // Round-robin across assigned partitions so one busy partition cannot
  // starve the others.
  for (std::size_t attempt = 0; attempt < assignment_.size(); ++attempt) {
    const TopicPartition& topic_partition =
        assignment_[(next_partition_ + attempt) % assignment_.size()];
    const Offset position = positions_.count(topic_partition) > 0
                                ? positions_[topic_partition]
                                : 0;

    auto records = FetchInto(topic_partition.topic, topic_partition.partition, position);
    if (!records.ok()) {
      if (records.status().code() == ErrorCode::kOutOfRange) {
        // We fell off the log (retention deleted our position). Restart from
        // the earliest surviving offset and report the gap.
        auto earliest =
            ListOffset(topic_partition.topic, topic_partition.partition, kEarliestOffset);
        if (earliest.ok()) {
          PL_WARN(kComponent) << "position was deleted by retention; skipping ahead"
                              << " partition=" << topic_partition.ToString()
                              << " was=" << position << " now=" << earliest.value();
          positions_[topic_partition] = earliest.value();
        }
        continue;
      }
      return records.status();
    }
    if (!records->empty()) {
      next_partition_ = (next_partition_ + attempt + 1) % assignment_.size();
      return records;
    }
  }

  next_partition_ = (next_partition_ + 1) % assignment_.size();
  return std::vector<InboundRecord>{};
}

Status Consumer::CommitOffset(const std::string& topic, PartitionIndex partition, Offset offset) {
  protocol::CommitOffsetRequest request;
  request.group_id = config_.group_id;
  request.member_id = member_id_;
  request.generation = generation_;
  request.topic = topic;
  request.partition = partition;
  request.offset = offset;

  scratch_.Clear();
  protocol::PayloadWriter writer(scratch_);
  request.Encode(writer);

  PL_ASSIGN_OR_RETURN(net::SyncClient * connection, context_.AnyBroker());
  PL_ASSIGN_OR_RETURN(auto frame, connection->Call(protocol::OpCode::kCommitOffset,
                                                   context_.NextRequestId(), scratch_.Readable()));
  protocol::CommitOffsetResponse response;
  PL_RETURN_IF_ERROR(ClientContext::DecodeResponse(frame.payload, response));
  ++stats_.commits;
  return OkStatus();
}

Status Consumer::Commit() {
  if (last_fetch_next_offset_ == kInvalidOffset || last_fetched_.topic.empty()) {
    return OkStatus();  // Nothing consumed yet.
  }
  return CommitOffset(last_fetched_.topic, last_fetched_.partition, last_fetch_next_offset_);
}

Result<Offset> Consumer::CommittedOffset(const std::string& topic, PartitionIndex partition) {
  protocol::FetchOffsetRequest request;
  request.group_id = config_.group_id;
  request.topic = topic;
  request.partition = partition;

  scratch_.Clear();
  protocol::PayloadWriter writer(scratch_);
  request.Encode(writer);

  PL_ASSIGN_OR_RETURN(net::SyncClient * connection, context_.AnyBroker());
  PL_ASSIGN_OR_RETURN(auto frame, connection->Call(protocol::OpCode::kFetchOffset,
                                                   context_.NextRequestId(), scratch_.Readable()));
  protocol::FetchOffsetResponse response;
  PL_RETURN_IF_ERROR(ClientContext::DecodeResponse(frame.payload, response));
  return response.offset;
}

}  // namespace pulselog::client
