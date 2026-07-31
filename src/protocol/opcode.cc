#include "pulselog/protocol/opcode.h"

namespace pulselog::protocol {

std::string_view OpCodeName(OpCode op) noexcept {
  switch (op) {
    case OpCode::kUnknown:
      return "UNKNOWN";
    case OpCode::kProduce:
      return "PRODUCE";
    case OpCode::kFetch:
      return "FETCH";
    case OpCode::kListOffsets:
      return "LIST_OFFSETS";
    case OpCode::kCreateTopic:
      return "CREATE_TOPIC";
    case OpCode::kMetadata:
      return "METADATA";
    case OpCode::kCommitOffset:
      return "COMMIT_OFFSET";
    case OpCode::kFetchOffset:
      return "FETCH_OFFSET";
    case OpCode::kJoinGroup:
      return "JOIN_GROUP";
    case OpCode::kHeartbeat:
      return "HEARTBEAT";
    case OpCode::kLeaveGroup:
      return "LEAVE_GROUP";
    case OpCode::kHealth:
      return "HEALTH";
    case OpCode::kListTopics:
      return "LIST_TOPICS";
    case OpCode::kReplicate:
      return "REPLICATE";
    case OpCode::kReplicaAck:
      return "REPLICA_ACK";
    case OpCode::kReplicaFetch:
      return "REPLICA_FETCH";
    case OpCode::kDeleteTopic:
      return "DELETE_TOPIC";
    case OpCode::kDescribeCluster:
      return "DESCRIBE_CLUSTER";
  }
  return "INVALID";
}

bool IsKnownOpCode(std::uint16_t raw) noexcept {
  switch (static_cast<OpCode>(raw)) {
    case OpCode::kProduce:
    case OpCode::kFetch:
    case OpCode::kListOffsets:
    case OpCode::kCreateTopic:
    case OpCode::kMetadata:
    case OpCode::kCommitOffset:
    case OpCode::kFetchOffset:
    case OpCode::kJoinGroup:
    case OpCode::kHeartbeat:
    case OpCode::kLeaveGroup:
    case OpCode::kHealth:
    case OpCode::kListTopics:
    case OpCode::kReplicate:
    case OpCode::kReplicaAck:
    case OpCode::kReplicaFetch:
    case OpCode::kDeleteTopic:
    case OpCode::kDescribeCluster:
      return true;
    case OpCode::kUnknown:
      return false;
  }
  return false;
}

}  // namespace pulselog::protocol
