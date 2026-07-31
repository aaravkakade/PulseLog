// Request and response payloads.
//
// Every message is a plain struct with `Encode`/`Decode`. Decode returns false
// on any bounds violation, unknown enum value, or trailing garbage; the caller
// turns that into a PROTOCOL_ERROR response and closes nothing -- a malformed
// payload is a client bug, not a stream desynchronisation (the frame layer
// already re-synchronised on the length field).
//
// Every response payload begins with a `ResponseHeader` so that a client can
// surface an error for an operation it does not otherwise understand.
//
// Compatibility strategy: fields are appended, never reordered or removed. A
// decoder for version N reading a version N+1 payload stops at the fields it
// knows and tolerates the remainder ONLY for responses (see
// `ResponseTrailerPolicy` in docs/PROTOCOL.md); requests must be fully
// consumed so a broker never silently ignores a directive it does not
// implement.
#ifndef PULSELOG_PROTOCOL_MESSAGES_H_
#define PULSELOG_PROTOCOL_MESSAGES_H_

#include <cstdint>
#include <string>
#include <vector>

#include "pulselog/base/types.h"
#include "pulselog/protocol/codec.h"
#include "pulselog/protocol/record.h"

namespace pulselog::protocol {

// Upper bound applied to every array length on decode, so a hostile count
// cannot force a huge reserve(). Real requests are far below this.
inline constexpr std::uint32_t kMaxArrayElements = 1U << 20U;

struct ResponseHeader {
  ErrorCode error = ErrorCode::kOk;
  std::string error_message;

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);

  [[nodiscard]] bool ok() const noexcept { return error == ErrorCode::kOk; }

  [[nodiscard]] Status ToStatus() const {
    return error == ErrorCode::kOk ? OkStatus() : Status{error, error_message};
  }
};

// ---------------------------------------------------------------------------
// Data plane
// ---------------------------------------------------------------------------

struct ProduceRequest {
  std::string topic;
  PartitionIndex partition{0};
  AckMode acks = AckMode::kLeader;
  std::int32_t timeout_ms = 5000;
  std::uint32_t record_count = 0;
  // Concatenated records in the format of record.h, with the offset field set
  // to 0 -- the leader assigns real offsets. This is a view into the request
  // frame's payload; no copy is made on either side.
  ByteSpan records;

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

struct ProduceResponse {
  ResponseHeader header;
  Offset base_offset = kInvalidOffset;   // Offset of the first appended record.
  Offset last_offset = kInvalidOffset;   // Offset of the last appended record.
  TimestampMs append_time = 0;           // Broker-side append timestamp.
  Offset high_water_mark = kInvalidOffset;

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

// Controls whether a consumer may read past the high-water mark.
enum class IsolationLevel : std::uint8_t {
  kReadUncommitted = 0,  // Up to the leader's log end offset.
  kReadReplicated = 1,   // Up to the high-water mark only (default).
};

struct FetchRequest {
  std::string topic;
  PartitionIndex partition{0};
  Offset fetch_offset = 0;
  std::uint32_t max_bytes = 1U << 20U;
  std::uint32_t min_bytes = 1;      // Broker waits until this much is available.
  std::int32_t max_wait_ms = 0;     // ...but no longer than this (long poll).
  IsolationLevel isolation = IsolationLevel::kReadReplicated;

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

struct FetchResponse {
  ResponseHeader header;
  Offset high_water_mark = kInvalidOffset;
  Offset log_start_offset = 0;
  Offset base_offset = kInvalidOffset;  // Offset of the first returned record.
  std::uint32_t record_count = 0;
  // Raw stored record bytes, copied verbatim out of the segment.
  ByteSpan records;

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

// Special timestamps understood by ListOffsets.
inline constexpr TimestampMs kListOffsetsEarliest = -2;
inline constexpr TimestampMs kListOffsetsLatest = -1;

struct ListOffsetsRequest {
  std::string topic;
  PartitionIndex partition{0};
  TimestampMs timestamp = kListOffsetsLatest;

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

struct ListOffsetsResponse {
  ResponseHeader header;
  Offset offset = kInvalidOffset;
  TimestampMs timestamp = -1;

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

// ---------------------------------------------------------------------------
// Control plane: topics and metadata
// ---------------------------------------------------------------------------

struct CreateTopicRequest {
  std::string topic;
  std::int32_t partitions = 1;
  std::int16_t replication_factor = 1;
  std::int64_t retention_ms = -1;      // -1 = inherit broker default.
  std::int64_t segment_bytes = -1;     // -1 = inherit broker default.
  Compression compression = Compression::kNone;

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

struct CreateTopicResponse {
  ResponseHeader header;
  std::int32_t partitions = 0;

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

struct DeleteTopicRequest {
  std::string topic;

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

struct DeleteTopicResponse {
  ResponseHeader header;

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

struct BrokerEndpoint {
  BrokerId id{0};
  std::string host;
  std::uint16_t port = 0;

  [[nodiscard]] std::string ToString() const {
    return std::to_string(id.value()) + "@" + host + ":" + std::to_string(port);
  }
};

struct PartitionMetadata {
  PartitionIndex index{0};
  BrokerId leader{-1};
  LeaderEpoch leader_epoch = 0;
  std::vector<BrokerId> replicas;
  std::vector<BrokerId> in_sync_replicas;
};

struct TopicMetadata {
  std::string name;
  std::vector<PartitionMetadata> partitions;
};

struct MetadataRequest {
  // Empty means "every topic this broker knows about".
  std::vector<std::string> topics;

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

struct MetadataResponse {
  ResponseHeader header;
  BrokerId controller_id{-1};
  std::vector<BrokerEndpoint> brokers;
  std::vector<TopicMetadata> topics;

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

struct ListTopicsRequest {
  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

struct TopicSummary {
  std::string name;
  std::int32_t partitions = 0;
  std::int64_t total_bytes = 0;
  std::int64_t total_records = 0;
};

struct ListTopicsResponse {
  ResponseHeader header;
  std::vector<TopicSummary> topics;

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

struct HealthRequest {
  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

struct HealthResponse {
  ResponseHeader header;
  BrokerId broker_id{-1};
  std::int64_t uptime_ms = 0;
  std::int32_t hosted_partitions = 0;
  std::int32_t leader_partitions = 0;
  bool ready = false;
  std::string version;

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

// ---------------------------------------------------------------------------
// Control plane: consumer groups
// ---------------------------------------------------------------------------

enum class AssignmentStrategy : std::uint8_t {
  kRange = 0,
  kRoundRobin = 1,
};

[[nodiscard]] std::string_view AssignmentStrategyName(AssignmentStrategy s) noexcept;

[[nodiscard]] bool ParseAssignmentStrategy(std::string_view text, AssignmentStrategy& out) noexcept;

struct JoinGroupRequest {
  std::string group_id;
  std::string member_id;  // Empty on first join; broker allocates one.
  std::vector<std::string> topics;
  std::int32_t session_timeout_ms = 10'000;
  AssignmentStrategy strategy = AssignmentStrategy::kRange;

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

struct JoinGroupResponse {
  ResponseHeader header;
  Generation generation{-1};
  std::string member_id;
  std::string leader_id;  // Member designated as group leader.
  std::vector<TopicPartition> assignment;

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

struct HeartbeatRequest {
  std::string group_id;
  std::string member_id;
  Generation generation{-1};

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

struct HeartbeatResponse {
  ResponseHeader header;
  Generation generation{-1};
  // Set when the coordinator wants the member to re-join (membership changed).
  bool rejoin_required = false;

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

struct LeaveGroupRequest {
  std::string group_id;
  std::string member_id;

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

struct LeaveGroupResponse {
  ResponseHeader header;

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

struct CommitOffsetRequest {
  std::string group_id;
  std::string member_id;
  Generation generation{-1};
  std::string topic;
  PartitionIndex partition{0};
  Offset offset = kInvalidOffset;  // Next offset to consume, not last consumed.
  std::string metadata;

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

struct CommitOffsetResponse {
  ResponseHeader header;

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

struct FetchOffsetRequest {
  std::string group_id;
  std::string topic;
  PartitionIndex partition{0};

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

struct FetchOffsetResponse {
  ResponseHeader header;
  Offset offset = kInvalidOffset;  // kInvalidOffset when nothing is committed.
  std::string metadata;

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

// ---------------------------------------------------------------------------
// Inter-broker replication
// ---------------------------------------------------------------------------

struct ReplicateRequest {
  std::string topic;
  PartitionIndex partition{0};
  BrokerId leader_id{-1};
  LeaderEpoch leader_epoch = 0;
  Offset base_offset = kInvalidOffset;  // Offset of the first record here.
  // Offset immediately before base_offset. A follower whose log end offset
  // does not match refuses the batch and asks for a catch-up fetch, which is
  // what prevents a silent hole in the log.
  Offset prev_offset = kInvalidOffset;
  Offset leader_high_water_mark = kInvalidOffset;
  Offset leader_log_start_offset = 0;
  std::uint32_t record_count = 0;
  ByteSpan records;  // Stored-format records, already checksummed.

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

struct ReplicateResponse {
  ResponseHeader header;
  BrokerId follower_id{-1};
  Offset log_end_offset = kInvalidOffset;   // Follower's next free offset.
  Offset flushed_offset = kInvalidOffset;   // Durable up to (exclusive).
  LeaderEpoch leader_epoch = 0;

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

struct ReplicaFetchRequest {
  std::string topic;
  PartitionIndex partition{0};
  BrokerId follower_id{-1};
  LeaderEpoch leader_epoch = 0;
  Offset fetch_offset = 0;
  std::uint32_t max_bytes = 1U << 20U;

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

struct ReplicaFetchResponse {
  ResponseHeader header;
  LeaderEpoch leader_epoch = 0;
  Offset high_water_mark = kInvalidOffset;
  Offset log_start_offset = 0;
  Offset base_offset = kInvalidOffset;
  std::uint32_t record_count = 0;
  ByteSpan records;

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

struct ReplicaAckRequest {
  std::string topic;
  PartitionIndex partition{0};
  BrokerId follower_id{-1};
  LeaderEpoch leader_epoch = 0;
  Offset log_end_offset = kInvalidOffset;
  Offset flushed_offset = kInvalidOffset;

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

struct ReplicaAckResponse {
  ResponseHeader header;
  Offset high_water_mark = kInvalidOffset;

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

struct DescribeClusterRequest {
  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

struct DescribeClusterResponse {
  ResponseHeader header;
  BrokerId controller_id{-1};
  std::vector<BrokerEndpoint> brokers;
  std::vector<BrokerId> live_brokers;

  void Encode(PayloadWriter& w) const;
  [[nodiscard]] bool Decode(PayloadReader& r);
};

// Builds an error-only response payload for any opcode. Used when a request
// fails before its typed response can be constructed (bad payload, unknown
// topic, backpressure).
void EncodeErrorResponse(ByteBuffer& out, ErrorCode code, std::string_view message);

}  // namespace pulselog::protocol

#endif  // PULSELOG_PROTOCOL_MESSAGES_H_
