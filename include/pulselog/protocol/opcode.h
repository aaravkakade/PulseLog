// Protocol operation codes.
//
// Numbering is permanent. A code is never reused for a different operation and
// never renumbered; obsolete operations are retired by leaving a gap. Ranges:
//
//   1..19    client data plane (produce, fetch, offsets)
//   20..39   client control plane (groups, metadata, health)
//   40..59   inter-broker (replication)
//   60..79   admin
#ifndef PULSELOG_PROTOCOL_OPCODE_H_
#define PULSELOG_PROTOCOL_OPCODE_H_

#include <cstdint>
#include <string_view>

namespace pulselog::protocol {

enum class OpCode : std::uint16_t {
  kUnknown = 0,

  // Data plane.
  kProduce = 1,        // Append a batch of records to one partition.
  kFetch = 2,          // Read records from one partition starting at an offset.
  kListOffsets = 3,    // Resolve earliest/latest/timestamp to a concrete offset.

  // Control plane.
  kCreateTopic = 20,
  kMetadata = 21,      // Topic -> partition -> leader/replica routing table.
  kCommitOffset = 22,  // Persist a consumer group's position.
  kFetchOffset = 23,   // Read back a committed position.
  kJoinGroup = 24,     // Enter a group; returns generation + assignment.
  kHeartbeat = 25,     // Keep a group session alive.
  kLeaveGroup = 26,
  kHealth = 27,        // Liveness + basic broker state.
  kListTopics = 28,

  // Inter-broker replication.
  kReplicate = 40,     // Leader -> follower: ordered log entries.
  kReplicaAck = 41,    // Follower -> leader: persisted offset acknowledgement.
  kReplicaFetch = 42,  // Follower -> leader: catch-up pull after reconnect.

  // Admin.
  kDeleteTopic = 60,
  kDescribeCluster = 61,
};

[[nodiscard]] std::string_view OpCodeName(OpCode op) noexcept;

// True when `raw` corresponds to a defined operation. The frame decoder uses
// this to reject unknown opcodes before allocating a payload buffer.
[[nodiscard]] bool IsKnownOpCode(std::uint16_t raw) noexcept;

[[nodiscard]] inline bool IsInterBroker(OpCode op) noexcept {
  const auto raw = static_cast<std::uint16_t>(op);
  return raw >= 40 && raw < 60;
}

}  // namespace pulselog::protocol

#endif  // PULSELOG_PROTOCOL_OPCODE_H_
