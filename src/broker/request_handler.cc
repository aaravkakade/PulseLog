// Request dispatch: from a decoded frame on an io thread to a partition
// worker, and back again.
#include <algorithm>

#include "pulselog/base/clock.h"
#include "pulselog/base/logging.h"
#include "pulselog/broker/broker.h"
#include "pulselog/protocol/codec.h"
#include "pulselog/protocol/messages.h"
#include "pulselog/protocol/record.h"

namespace pulselog::broker {
namespace {

constexpr std::string_view kComponent = "broker.request";

constexpr std::uint16_t kResponseFlag = static_cast<std::uint16_t>(protocol::FrameFlags::kResponse);

// Peeks the topic and partition out of a request payload without fully
// decoding it, so the io thread can pick the owning worker cheaply. Every
// partition-scoped request begins with `topic string, partition i32`.
[[nodiscard]] bool PeekTopicPartition(ByteSpan payload,
                                      std::string& topic,
                                      PartitionIndex& partition) {
  protocol::PayloadReader reader(payload);
  std::int32_t raw = 0;
  if (!reader.GetString(topic)) return false;
  if (!reader.GetI32(raw)) return false;
  if (raw < 0) return false;
  partition = PartitionIndex{raw};
  return true;
}

template<typename Response>
void EncodeInto(ByteBuffer& out, const Response& response) {
  protocol::PayloadWriter writer(out);
  response.Encode(writer);
}

}  // namespace

// --- io-thread entry points -------------------------------------------------

void Broker::OnFrame(net::Connection& connection, const protocol::FrameDecoder::Frame& frame) {
  const std::size_t loop_index = static_cast<std::size_t>(connection.loop().index());
  if (loop_index < loop_connections_.size()) {
    // Registered lazily on first use; the map is touched only by this loop's
    // own thread, so it needs no lock.
    loop_connections_[loop_index].emplace(connection.id(), &connection);
  }

  if (HandleInline(connection, frame)) return;

  // Topic administration goes to a worker rather than running inline: it may
  // have to forward to the controller over the network, and an io loop must
  // never block on another broker.
  if (frame.header.opcode == protocol::OpCode::kCreateTopic ||
      frame.header.opcode == protocol::OpCode::kDeleteTopic) {
    RouteToWorker(connection, frame, 0);
    return;
  }

  const auto worker_index = WorkerForRequest(frame);
  if (!worker_index.has_value()) {
    if (metrics_) metrics_->protocol_errors.Increment();
    ByteBuffer payload;
    protocol::EncodeErrorResponse(
        payload, ErrorCode::kProtocolError, "request payload could not be parsed");
    (void)connection.SendFrame(
        frame.header.opcode, frame.header.request_id, kResponseFlag, payload.Readable());
    return;
  }
  RouteToWorker(connection, frame, *worker_index);
}

void Broker::OnConnectionClosed(net::Connection& connection, const Status& reason) {
  const std::size_t loop_index = static_cast<std::size_t>(connection.loop().index());
  if (loop_index < loop_connections_.size()) {
    loop_connections_[loop_index].erase(connection.id());
  }
  if (metrics_) metrics_->connections_closed.Increment();
  if (!reason.ok() && reason.code() != ErrorCode::kClosed) {
    PL_DEBUG(kComponent) << "connection closed"
                         << " conn=" << connection.id() << " peer=" << connection.peer().ToString()
                         << " reason=" << reason.ToString();
  }
}

std::optional<std::size_t> Broker::WorkerForRequest(const protocol::FrameDecoder::Frame& frame) {
  std::string topic;
  PartitionIndex partition{0};
  if (!PeekTopicPartition(frame.payload, topic, partition)) return std::nullopt;
  return partitions_.WorkerFor(TopicPartition{std::move(topic), partition});
}

void Broker::RouteToWorker(net::Connection& connection,
                           const protocol::FrameDecoder::Frame& frame,
                           std::size_t worker_index) {
  const std::size_t loop_index = static_cast<std::size_t>(connection.loop().index());

  WorkerRequest request;
  request.opcode = frame.header.opcode;
  request.request_id = frame.header.request_id;
  request.connection_id = connection.id();
  request.loop_index = loop_index;
  request.enqueued_nanos = MonotonicNanos();

  // The one copy on the request path. The connection's read buffer is reused
  // the moment this callback returns, so the payload has to be owned before it
  // can cross to another thread. The buffer comes from the worker's pool, so
  // this allocates only when the pool is empty.
  BufferPool& pool = *worker_pools_[worker_index];
  auto buffer = pool.Acquire(frame.payload.size());
  buffer->Append(frame.payload);
  request.payload = PooledBuffer(pool, std::move(buffer));

  if (!workers_[worker_index]->Submit(std::move(request))) {
    // The worker is saturated. Answering immediately with a retryable error is
    // the whole point of a bounded queue: the alternative is unbounded memory
    // growth and unbounded latency.
    if (metrics_) {
      metrics_->backpressure_rejections.Increment();
      metrics_->failed_requests.Increment();
    }
    ByteBuffer payload;
    protocol::EncodeErrorResponse(
        payload, ErrorCode::kBackpressure, "partition worker queue is full; retry shortly");
    (void)connection.SendFrame(
        frame.header.opcode, frame.header.request_id, kResponseFlag, payload.Readable());
  }
}

// Rejects a group request that arrived at the wrong broker.
//
// Group membership and committed offsets live on exactly one broker. Serving a
// request here anyway would create a second, independent copy of the group:
// the consumer would join on one broker and commit on another, and the
// group's real position would never advance. Failing names the coordinator so
// the client can re-route.
bool Broker::RejectIfNotCoordinator(net::Connection& connection,
                                    const protocol::FrameDecoder::Frame& frame,
                                    const std::string& group_id) {
  const BrokerId coordinator = cluster_.CoordinatorFor(group_id);
  if (!coordinator.valid() || coordinator == config_.broker_id) return false;

  ByteBuffer payload;
  protocol::EncodeErrorResponse(
      payload,
      ErrorCode::kNotLeader,
      "broker " + std::to_string(coordinator.value()) + " coordinates group '" + group_id + "'");
  (void)connection.SendFrame(
      frame.header.opcode, frame.header.request_id, kResponseFlag, payload.Readable());
  if (metrics_ != nullptr) metrics_->failed_requests.Increment();
  return true;
}

bool Broker::HandleInline(net::Connection& connection, const protocol::FrameDecoder::Frame& frame) {
  ByteBuffer payload;
  const auto send = [&](auto& response) {
    EncodeInto(payload, response);
    (void)connection.SendFrame(
        frame.header.opcode, frame.header.request_id, kResponseFlag, payload.Readable());
  };
  const auto send_error = [&](ErrorCode code, std::string_view message) {
    protocol::EncodeErrorResponse(payload, code, message);
    (void)connection.SendFrame(
        frame.header.opcode, frame.header.request_id, kResponseFlag, payload.Readable());
    if (metrics_) metrics_->failed_requests.Increment();
  };

  switch (frame.header.opcode) {
    case protocol::OpCode::kHealth: {
      protocol::HealthResponse response;
      response.broker_id = config_.broker_id;
      response.uptime_ms = uptime_ms();
      const auto stats = partitions_.GetStats();
      response.hosted_partitions = static_cast<std::int32_t>(stats.partitions);
      response.leader_partitions = static_cast<std::int32_t>(stats.leader_partitions);
      response.ready = running_.load(std::memory_order_acquire);
      response.version = "0.1.0";
      send(response);
      return true;
    }

    case protocol::OpCode::kMetadata: {
      protocol::MetadataRequest request;
      protocol::PayloadReader reader(frame.payload);
      if (!request.Decode(reader) || !reader.Complete()) {
        send_error(ErrorCode::kProtocolError, "malformed metadata request");
        return true;
      }
      // No auto-create here. Creating a topic is the controller's job, and a
      // metadata lookup arriving at a non-controller must not invent a
      // second, differently-shaped definition of the same topic. Produce is
      // where auto-create happens, on a worker thread that can forward.
      auto response = cluster_.BuildMetadataResponse(request.topics);
      send(response);
      return true;
    }

    case protocol::OpCode::kListTopics: {
      protocol::ListTopicsResponse response;
      for (const auto& descriptor : cluster_.ListTopics()) {
        protocol::TopicSummary summary;
        summary.name = descriptor.config.name;
        summary.partitions = descriptor.config.partition_count;
        for (const auto& assignment : descriptor.partitions) {
          if (PartitionReplica* replica =
                  partitions_.Find(TopicPartition{descriptor.config.name, assignment.index})) {
            const auto stats = replica->GetStats();
            summary.total_bytes += static_cast<std::int64_t>(stats.total_bytes);
            summary.total_records += stats.log_end_offset - stats.log_start_offset;
          }
        }
        response.topics.push_back(std::move(summary));
      }
      send(response);
      return true;
    }

    case protocol::OpCode::kDescribeCluster: {
      protocol::DescribeClusterResponse response;
      response.controller_id = cluster_.ControllerId();
      response.brokers = cluster_.Brokers();
      response.live_brokers.push_back(config_.broker_id);
      if (replicator_) {
        for (const auto& follower : replicator_->DescribeFollowers()) {
          if (follower.connected) response.live_brokers.push_back(follower.broker);
        }
      }
      send(response);
      return true;
    }

    case protocol::OpCode::kJoinGroup: {
      protocol::JoinGroupRequest request;
      protocol::PayloadReader reader(frame.payload);
      if (!request.Decode(reader) || !reader.Complete()) {
        send_error(ErrorCode::kProtocolError, "malformed join-group request");
        return true;
      }
      // Subscribing to a topic that does not exist yet yields an empty
      // assignment rather than creating it; the consumer picks up partitions
      // once a producer (or an explicit create) brings the topic into being.
      if (RejectIfNotCoordinator(connection, frame, request.group_id)) return true;
      auto response = groups_->Join(request, WallClockMillis());
      send(response);
      return true;
    }

    case protocol::OpCode::kHeartbeat: {
      protocol::HeartbeatRequest request;
      protocol::PayloadReader reader(frame.payload);
      if (!request.Decode(reader) || !reader.Complete()) {
        send_error(ErrorCode::kProtocolError, "malformed heartbeat request");
        return true;
      }
      if (RejectIfNotCoordinator(connection, frame, request.group_id)) return true;
      auto response = groups_->Heartbeat(request, WallClockMillis());
      send(response);
      return true;
    }

    case protocol::OpCode::kLeaveGroup: {
      protocol::LeaveGroupRequest request;
      protocol::PayloadReader reader(frame.payload);
      if (!request.Decode(reader) || !reader.Complete()) {
        send_error(ErrorCode::kProtocolError, "malformed leave-group request");
        return true;
      }
      if (RejectIfNotCoordinator(connection, frame, request.group_id)) return true;
      auto response = groups_->Leave(request, WallClockMillis());
      send(response);
      return true;
    }

    case protocol::OpCode::kCommitOffset: {
      protocol::CommitOffsetRequest request;
      protocol::PayloadReader reader(frame.payload);
      if (!request.Decode(reader) || !reader.Complete()) {
        send_error(ErrorCode::kProtocolError, "malformed commit-offset request");
        return true;
      }
      if (RejectIfNotCoordinator(connection, frame, request.group_id)) return true;
      auto response = groups_->Commit(request, WallClockMillis());
      send(response);
      return true;
    }

    case protocol::OpCode::kFetchOffset: {
      protocol::FetchOffsetRequest request;
      protocol::PayloadReader reader(frame.payload);
      if (!request.Decode(reader) || !reader.Complete()) {
        send_error(ErrorCode::kProtocolError, "malformed fetch-offset request");
        return true;
      }
      if (RejectIfNotCoordinator(connection, frame, request.group_id)) return true;
      auto response = groups_->FetchOffset(request);
      send(response);
      return true;
    }

    default:
      return false;  // Partition-scoped: route to a worker.
  }
}

// --- responses --------------------------------------------------------------

void Broker::Respond(std::size_t loop_index,
                     std::uint64_t connection_id,
                     protocol::OpCode opcode,
                     RequestId request_id,
                     ByteBuffer&& payload) {
  if (server_ == nullptr || loop_index >= loop_connections_.size() ||
      loop_index >= server_->LoopCount()) {
    return;
  }

  net::EventLoop& loop = server_->loop(loop_index);
  // The payload moves onto the io loop; it is destroyed there whether or not
  // the connection still exists.
  auto owned = std::make_shared<ByteBuffer>(std::move(payload));
  const bool posted = loop.PostTask([this, loop_index, connection_id, opcode, request_id, owned] {
    auto& registry = loop_connections_[loop_index];
    const auto it = registry.find(connection_id);
    if (it == registry.end()) return;  // Client disconnected; drop the response.
    (void)it->second->SendFrame(opcode, request_id, kResponseFlag, owned->Readable());
  });

  if (!posted) {
    PL_WARN(kComponent) << "dropping response: io loop task queue is full"
                        << " loop=" << loop_index << " conn=" << connection_id;
    if (metrics_) metrics_->failed_requests.Increment();
  }
}

void Broker::RespondError(std::size_t loop_index,
                          std::uint64_t connection_id,
                          protocol::OpCode opcode,
                          RequestId request_id,
                          ErrorCode code,
                          std::string_view message) {
  ByteBuffer payload;
  protocol::EncodeErrorResponse(payload, code, message);
  if (metrics_) metrics_->failed_requests.Increment();
  Respond(loop_index, connection_id, opcode, request_id, std::move(payload));
}

Result<PartitionReplica*> Broker::ResolvePartition(const std::string& topic,
                                                   PartitionIndex partition,
                                                   bool auto_create) {
  const TopicPartition topic_partition{topic, partition};
  if (PartitionReplica* replica = partitions_.Find(topic_partition)) return replica;

  if (!cluster_.HasTopic(topic)) {
    if (!auto_create || !config_.auto_create_topics) {
      return NotFound("unknown topic '" + topic + "'");
    }
    if (!IsValidTopicName(topic)) {
      return InvalidArgument("invalid topic name '" + topic + "'");
    }
    PL_RETURN_IF_ERROR(EnsureTopic(topic,
                                   std::max(config_.default_partitions, partition.value() + 1),
                                   config_.default_replication_factor)
                           .status());
  }

  if (PartitionReplica* replica = partitions_.Find(topic_partition)) return replica;

  // The topic exists but this broker does not host the partition. Telling the
  // client which broker does is what lets it re-route without a full metadata
  // refresh.
  auto assignment = cluster_.GetPartition(topic, partition);
  if (!assignment.ok()) return assignment.status();
  return Status{ErrorCode::kNotLeader,
                "broker " + std::to_string(assignment->leader.value()) + " leads " +
                    topic_partition.ToString()};
}

// --- worker-thread execution ------------------------------------------------

void Broker::Execute(WorkerRequest& request) {
  if (metrics_) {
    metrics_->queue_wait.Record(MonotonicNanos() - request.enqueued_nanos);
  }

  switch (request.opcode) {
    case protocol::OpCode::kProduce:
      ExecuteProduce(request);
      break;
    case protocol::OpCode::kFetch:
      ExecuteFetch(request);
      break;
    case protocol::OpCode::kListOffsets:
      ExecuteListOffsets(request);
      break;
    case protocol::OpCode::kReplicate:
      ExecuteReplicate(request);
      break;
    case protocol::OpCode::kReplicaFetch:
      ExecuteReplicaFetch(request);
      break;
    case protocol::OpCode::kReplicaAck:
      ExecuteReplicaAck(request);
      break;
    case protocol::OpCode::kCreateTopic:
      ExecuteCreateTopic(request);
      break;
    case protocol::OpCode::kDeleteTopic:
      ExecuteDeleteTopic(request);
      break;
    default:
      RespondError(request.loop_index,
                   request.connection_id,
                   request.opcode,
                   request.request_id,
                   ErrorCode::kProtocolError,
                   "operation is not valid on a partition worker");
      break;
  }
}

void Broker::ExecuteCreateTopic(WorkerRequest& request) {
  protocol::CreateTopicRequest create;
  protocol::PayloadReader reader(request.payload->Readable());
  if (!create.Decode(reader) || !reader.Complete()) {
    RespondError(request.loop_index,
                 request.connection_id,
                 request.opcode,
                 request.request_id,
                 ErrorCode::kProtocolError,
                 "malformed create-topic request");
    return;
  }

  protocol::CreateTopicResponse response;
  auto descriptor =
      EnsureTopic(create.topic,
                  create.partitions > 0 ? create.partitions : config_.default_partitions,
                  create.replication_factor > 0 ? create.replication_factor
                                                : config_.default_replication_factor,
                  create.from_controller);
  if (!descriptor.ok()) {
    response.header.error = descriptor.status().code();
    response.header.error_message = descriptor.status().message();
    if (metrics_) metrics_->failed_requests.Increment();
  } else {
    response.partitions = descriptor->config.partition_count;
  }

  ByteBuffer payload;
  protocol::PayloadWriter writer(payload);
  response.Encode(writer);
  Respond(request.loop_index,
          request.connection_id,
          protocol::OpCode::kCreateTopic,
          request.request_id,
          std::move(payload));
}

void Broker::ExecuteDeleteTopic(WorkerRequest& request) {
  protocol::DeleteTopicRequest remove;
  protocol::PayloadReader reader(request.payload->Readable());
  if (!remove.Decode(reader) || !reader.Complete()) {
    RespondError(request.loop_index,
                 request.connection_id,
                 request.opcode,
                 request.request_id,
                 ErrorCode::kProtocolError,
                 "malformed delete-topic request");
    return;
  }

  protocol::DeleteTopicResponse response;
  const Status status = partitions_.DeleteTopic(remove.topic, /*delete_data=*/true);
  if (!status.ok()) {
    response.header.error = status.code();
    response.header.error_message = status.message();
  } else {
    registry_.RemoveByLabel("topic", remove.topic);
    const Status persisted = PersistMetadata();
    if (!persisted.ok()) {
      PL_ERROR(kComponent) << "metadata persist failed: " << persisted.ToString();
    }
  }

  ByteBuffer payload;
  protocol::PayloadWriter writer(payload);
  response.Encode(writer);
  Respond(request.loop_index,
          request.connection_id,
          protocol::OpCode::kDeleteTopic,
          request.request_id,
          std::move(payload));
}

void Broker::ExecuteProduce(WorkerRequest& request) {
  const std::int64_t started = MonotonicNanos();

  protocol::ProduceRequest produce;
  protocol::PayloadReader reader(request.payload->Readable());
  if (!produce.Decode(reader)) {
    if (metrics_) metrics_->protocol_errors.Increment();
    RespondError(request.loop_index,
                 request.connection_id,
                 request.opcode,
                 request.request_id,
                 ErrorCode::kProtocolError,
                 "malformed produce request");
    return;
  }

  auto replica = ResolvePartition(produce.topic, produce.partition, /*auto_create=*/true);
  if (!replica.ok()) {
    RespondError(request.loop_index,
                 request.connection_id,
                 request.opcode,
                 request.request_id,
                 replica.status().code(),
                 replica.status().message());
    return;
  }
  if (!replica.value()->is_leader()) {
    RespondError(request.loop_index,
                 request.connection_id,
                 request.opcode,
                 request.request_id,
                 ErrorCode::kNotLeader,
                 "broker " + std::to_string(replica.value()->leader().value()) + " leads " +
                     produce.topic + "-" + std::to_string(produce.partition.value()));
    return;
  }

  if (produce.acks == AckMode::kQuorum) {
    // Refuse rather than silently degrade: a producer that asked for quorum
    // must not be told "acknowledged" on the strength of one replica.
    const auto assignment = replica.value()->assignment();
    const std::size_t in_sync = replica.value()->GetStats().in_sync_replicas;
    if (in_sync < assignment.QuorumSize()) {
      RespondError(request.loop_index,
                   request.connection_id,
                   request.opcode,
                   request.request_id,
                   ErrorCode::kNotEnoughReplicas,
                   "quorum requires " + std::to_string(assignment.QuorumSize()) +
                       " in-sync replicas; " + std::to_string(in_sync) + " available");
      return;
    }
  }

  // The records live in this request's own pooled buffer, so rewriting offsets
  // in place is safe and needs no further copy.
  const MutableByteSpan records(const_cast<std::uint8_t*>(produce.records.data()),
                                produce.records.size());
  auto appended = replica.value()->log().AppendAssigningOffsets(records, produce.record_count);
  if (!appended.ok()) {
    RespondError(request.loop_index,
                 request.connection_id,
                 request.opcode,
                 request.request_id,
                 appended.status().code(),
                 appended.status().message());
    return;
  }

  const std::int64_t now_ms = WallClockMillis();
  replica.value()->OnLeaderAppend(appended->last_offset + 1, now_ms);
  if (replicator_) replicator_->NotifyAppend(replica.value()->topic_partition());

  if (metrics_) {
    metrics_->produce_requests.Increment();
    metrics_->messages_produced.Increment(appended->record_count);
    metrics_->bytes_produced.Increment(appended->bytes);
    metrics_->produce_batch_size.Record(appended->record_count);
  }

  // Capture only what the completion needs; `request` does not outlive this
  // call, and the callback may fire on a different thread.
  const std::size_t loop_index = request.loop_index;
  const std::uint64_t connection_id = request.connection_id;
  const RequestId request_id = request.request_id;
  const Offset base_offset = appended->base_offset;
  const Offset last_offset = appended->last_offset;
  const TimestampMs append_time = appended->append_time;

  DurabilityWaiter waiter;
  waiter.required_offset = last_offset;
  waiter.mode = produce.acks;
  waiter.deadline_ms =
      now_ms + (produce.timeout_ms > 0 ? produce.timeout_ms : config_.replication_timeout_ms);
  waiter.on_complete =
      [this, loop_index, connection_id, request_id, base_offset, last_offset, append_time, started](
          Status status, Offset high_water_mark) {
        protocol::ProduceResponse response;
        if (!status.ok()) {
          response.header.error = status.code();
          response.header.error_message = status.message();
          if (metrics_) metrics_->failed_requests.Increment();
        }
        response.base_offset = base_offset;
        response.last_offset = last_offset;
        response.append_time = append_time;
        response.high_water_mark = high_water_mark;

        ByteBuffer payload;
        protocol::PayloadWriter writer(payload);
        response.Encode(writer);
        if (metrics_) metrics_->produce_latency.Record(MonotonicNanos() - started);
        Respond(
            loop_index, connection_id, protocol::OpCode::kProduce, request_id, std::move(payload));
      };

  replica.value()->AddWaiter(std::move(waiter), now_ms);
}

void Broker::ExecuteFetch(WorkerRequest& request) {
  const std::int64_t started = MonotonicNanos();

  protocol::FetchRequest fetch;
  protocol::PayloadReader reader(request.payload->Readable());
  if (!fetch.Decode(reader) || !reader.Complete()) {
    if (metrics_) metrics_->protocol_errors.Increment();
    RespondError(request.loop_index,
                 request.connection_id,
                 request.opcode,
                 request.request_id,
                 ErrorCode::kProtocolError,
                 "malformed fetch request");
    return;
  }

  auto replica = ResolvePartition(fetch.topic, fetch.partition, /*auto_create=*/false);
  if (!replica.ok()) {
    RespondError(request.loop_index,
                 request.connection_id,
                 request.opcode,
                 request.request_id,
                 replica.status().code(),
                 replica.status().message());
    return;
  }

  const Offset high_water_mark = replica.value()->HighWaterMark();
  const Offset log_end = replica.value()->log().LogEndOffset();
  const Offset log_start = replica.value()->log().LogStartOffset();

  // Resolve the special positions a client can ask for.
  Offset start = fetch.fetch_offset;
  if (start == kEarliestOffset) {
    start = log_start;
  } else if (start == kLatestOffset) {
    start =
        fetch.isolation == protocol::IsolationLevel::kReadReplicated ? high_water_mark : log_end;
  }

  // A position outside the log is an error, not an empty result. Returning
  // "nothing available" would leave a consumer polling an offset that can
  // never yield anything -- it needs to know its position is invalid.
  if (start > log_end) {
    RespondError(request.loop_index,
                 request.connection_id,
                 request.opcode,
                 request.request_id,
                 ErrorCode::kOutOfRange,
                 "offset " + std::to_string(start) + " is beyond the log end " +
                     std::to_string(log_end) + " for " + fetch.topic + "-" +
                     std::to_string(fetch.partition.value()));
    return;
  }
  if (start < log_start) {
    RespondError(request.loop_index,
                 request.connection_id,
                 request.opcode,
                 request.request_id,
                 ErrorCode::kOutOfRange,
                 "offset " + std::to_string(start) + " was deleted by retention; " + fetch.topic +
                     "-" + std::to_string(fetch.partition.value()) + " now starts at " +
                     std::to_string(log_start));
    return;
  }

  // A consumer must not see records that are not yet on a majority of
  // replicas: if the leader died, those records could vanish.
  const Offset visible_end =
      fetch.isolation == protocol::IsolationLevel::kReadReplicated ? high_water_mark : log_end;

  protocol::FetchResponse response;
  response.high_water_mark = high_water_mark;
  response.log_start_offset = log_start;
  response.base_offset = start;

  ByteBuffer records;
  if (start < visible_end) {
    auto read = replica.value()->log().Read(start, fetch.max_bytes, records);
    if (!read.ok()) {
      RespondError(request.loop_index,
                   request.connection_id,
                   request.opcode,
                   request.request_id,
                   read.status().code(),
                   read.status().message());
      return;
    }
    // Trim anything beyond the visible end. The log may hold records the
    // high-water mark does not cover yet.
    std::uint32_t visible_records = read->record_count;
    std::size_t visible_bytes = read->bytes;
    if (start + static_cast<Offset>(read->record_count) > visible_end) {
      visible_records = static_cast<std::uint32_t>(visible_end - start);
      visible_bytes = 0;
      protocol::RecordIterator it(records.Readable(), /*verify_crc=*/false);
      protocol::RecordView view;
      std::uint32_t seen = 0;
      while (seen < visible_records && it.Next(view)) {
        visible_bytes = it.Position();
        ++seen;
      }
    }
    response.record_count = visible_records;
    response.records = records.Readable().subspan(0, visible_bytes);
  }

  if (metrics_) {
    metrics_->fetch_requests.Increment();
    metrics_->messages_fetched.Increment(response.record_count);
    metrics_->bytes_fetched.Increment(response.records.size());
  }

  ByteBuffer payload;
  protocol::PayloadWriter writer(payload);
  response.Encode(writer);
  if (metrics_) metrics_->fetch_latency.Record(MonotonicNanos() - started);
  Respond(request.loop_index,
          request.connection_id,
          protocol::OpCode::kFetch,
          request.request_id,
          std::move(payload));
}

void Broker::ExecuteListOffsets(WorkerRequest& request) {
  protocol::ListOffsetsRequest list;
  protocol::PayloadReader reader(request.payload->Readable());
  if (!list.Decode(reader) || !reader.Complete()) {
    RespondError(request.loop_index,
                 request.connection_id,
                 request.opcode,
                 request.request_id,
                 ErrorCode::kProtocolError,
                 "malformed list-offsets request");
    return;
  }

  auto replica = ResolvePartition(list.topic, list.partition, /*auto_create=*/false);
  if (!replica.ok()) {
    RespondError(request.loop_index,
                 request.connection_id,
                 request.opcode,
                 request.request_id,
                 replica.status().code(),
                 replica.status().message());
    return;
  }

  protocol::ListOffsetsResponse response;
  auto offset = replica.value()->log().OffsetForTimestamp(list.timestamp);
  if (!offset.ok()) {
    response.header.error = offset.status().code();
    response.header.error_message = offset.status().message();
  } else {
    response.offset = offset.value();
    response.timestamp = list.timestamp;
  }

  ByteBuffer payload;
  protocol::PayloadWriter writer(payload);
  response.Encode(writer);
  Respond(request.loop_index,
          request.connection_id,
          protocol::OpCode::kListOffsets,
          request.request_id,
          std::move(payload));
}

void Broker::ExecuteReplicate(WorkerRequest& request) {
  protocol::ReplicateRequest replicate;
  protocol::PayloadReader reader(request.payload->Readable());
  if (!replicate.Decode(reader)) {
    RespondError(request.loop_index,
                 request.connection_id,
                 request.opcode,
                 request.request_id,
                 ErrorCode::kProtocolError,
                 "malformed replicate request");
    return;
  }

  protocol::ReplicateResponse response;
  response.follower_id = config_.broker_id;

  const TopicPartition topic_partition{replicate.topic, replicate.partition};
  PartitionReplica* replica = partitions_.Find(topic_partition);
  if (replica == nullptr) {
    // The topic may exist in metadata but not have been opened here yet.
    auto assignment = cluster_.GetPartition(replicate.topic, replicate.partition);
    auto topic = cluster_.GetTopic(replicate.topic);
    if (assignment.ok() && topic.ok() && assignment->HasReplica(config_.broker_id)) {
      auto opened = partitions_.OpenPartition(topic_partition, topic->config, assignment.value());
      if (opened.ok()) replica = opened.value();
    }
  }
  if (replica == nullptr) {
    response.header.error = ErrorCode::kNotFound;
    response.header.error_message = "this broker does not host " + topic_partition.ToString();
    response.log_end_offset = 0;
    ByteBuffer payload;
    protocol::PayloadWriter writer(payload);
    response.Encode(writer);
    Respond(request.loop_index,
            request.connection_id,
            protocol::OpCode::kReplicate,
            request.request_id,
            std::move(payload));
    return;
  }

  const Offset log_end = replica->log().LogEndOffset();
  response.leader_epoch = replica->leader_epoch();

  // Reject a leader whose epoch is older than one we have already seen. This
  // is what stops a partitioned-then-returned leader from overwriting a log.
  if (replicate.leader_epoch < replica->leader_epoch()) {
    response.header.error = ErrorCode::kNotLeader;
    response.header.error_message = "stale leader epoch " + std::to_string(replicate.leader_epoch) +
                                    "; current is " + std::to_string(replica->leader_epoch());
    response.log_end_offset = log_end;
    response.flushed_offset = replica->log().FlushedOffset();
  } else if (replicate.record_count == 0 && replicate.base_offset == log_end) {
    // A progress probe: the leader is asking where we are and how much of it
    // is durable. Nothing to append.
    response.log_end_offset = log_end;
    response.flushed_offset = replica->log().FlushedOffset();
    const Offset watermark = std::min(replicate.leader_high_water_mark, log_end);
    replica->OnLeaderAppend(watermark, WallClockMillis());
  } else if (replicate.base_offset != log_end) {
    // A gap or an overlap. Reporting our log end offset lets the leader resume
    // from the right place; a duplicate resend lands here and is a no-op.
    response.header.error = ErrorCode::kOutOfRange;
    response.header.error_message = "expected base offset " + std::to_string(log_end) + ", got " +
                                    std::to_string(replicate.base_offset);
    response.log_end_offset = log_end;
    response.flushed_offset = replica->log().FlushedOffset();
  } else {
    auto appended = replica->log().AppendWithOffsets(replicate.records, replicate.record_count);
    if (!appended.ok()) {
      response.header.error = appended.status().code();
      response.header.error_message = appended.status().message();
      response.log_end_offset = replica->log().LogEndOffset();
    } else {
      // A follower's high-water mark is whatever the leader reports, capped by
      // what the follower actually has.
      const Offset watermark =
          std::min(replicate.leader_high_water_mark, replica->log().LogEndOffset());
      replica->OnLeaderAppend(watermark, WallClockMillis());
      response.log_end_offset = replica->log().LogEndOffset();
    }
    response.flushed_offset = replica->log().FlushedOffset();
  }

  ByteBuffer payload;
  protocol::PayloadWriter writer(payload);
  response.Encode(writer);
  Respond(request.loop_index,
          request.connection_id,
          protocol::OpCode::kReplicate,
          request.request_id,
          std::move(payload));
}

void Broker::ExecuteReplicaFetch(WorkerRequest& request) {
  protocol::ReplicaFetchRequest fetch;
  protocol::PayloadReader reader(request.payload->Readable());
  if (!fetch.Decode(reader) || !reader.Complete()) {
    RespondError(request.loop_index,
                 request.connection_id,
                 request.opcode,
                 request.request_id,
                 ErrorCode::kProtocolError,
                 "malformed replica-fetch request");
    return;
  }

  auto replica = ResolvePartition(fetch.topic, fetch.partition, /*auto_create=*/false);
  if (!replica.ok()) {
    RespondError(request.loop_index,
                 request.connection_id,
                 request.opcode,
                 request.request_id,
                 replica.status().code(),
                 replica.status().message());
    return;
  }

  protocol::ReplicaFetchResponse response;
  response.leader_epoch = replica.value()->leader_epoch();
  response.high_water_mark = replica.value()->HighWaterMark();
  response.log_start_offset = replica.value()->log().LogStartOffset();
  response.base_offset = fetch.fetch_offset;

  // A follower reads the leader's whole log, not just up to the high-water
  // mark: replicating a record is precisely how it becomes committed.
  ByteBuffer records;
  if (fetch.fetch_offset < replica.value()->log().LogEndOffset()) {
    auto read = replica.value()->log().Read(fetch.fetch_offset, fetch.max_bytes, records);
    if (!read.ok()) {
      RespondError(request.loop_index,
                   request.connection_id,
                   request.opcode,
                   request.request_id,
                   read.status().code(),
                   read.status().message());
      return;
    }
    response.record_count = read->record_count;
    response.records = records.Readable();
  }

  ByteBuffer payload;
  protocol::PayloadWriter writer(payload);
  response.Encode(writer);
  Respond(request.loop_index,
          request.connection_id,
          protocol::OpCode::kReplicaFetch,
          request.request_id,
          std::move(payload));
}

void Broker::ExecuteReplicaAck(WorkerRequest& request) {
  protocol::ReplicaAckRequest ack;
  protocol::PayloadReader reader(request.payload->Readable());
  if (!ack.Decode(reader) || !reader.Complete()) {
    RespondError(request.loop_index,
                 request.connection_id,
                 request.opcode,
                 request.request_id,
                 ErrorCode::kProtocolError,
                 "malformed replica-ack request");
    return;
  }

  protocol::ReplicaAckResponse response;
  PartitionReplica* replica = partitions_.Find(TopicPartition{ack.topic, ack.partition});
  if (replica == nullptr) {
    response.header.error = ErrorCode::kNotFound;
    response.header.error_message = "this broker does not host that partition";
  } else {
    response.high_water_mark = replica->OnFollowerProgress(
        ack.follower_id, ack.log_end_offset, ack.flushed_offset, WallClockMillis());
  }

  ByteBuffer payload;
  protocol::PayloadWriter writer(payload);
  response.Encode(writer);
  Respond(request.loop_index,
          request.connection_id,
          protocol::OpCode::kReplicaAck,
          request.request_id,
          std::move(payload));
}

}  // namespace pulselog::broker
