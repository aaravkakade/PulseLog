// Consumer-group coordination and durable offset storage.
//
// Delivery semantics
// ------------------
// **At-least-once.** A consumer fetches records, processes them, then commits
// the offset it wants to resume from. A crash between processing and commit
// replays the uncommitted records. There is no transactional coupling between
// processing and the commit, so exactly-once is not provided and is not
// claimed anywhere.
//
// Exclusivity
// -----------
// Within one generation the coordinator assigns each partition to exactly one
// member (enforced by the assignor and checked in tests). A member whose
// generation is stale has its commits rejected with ILLEGAL_GENERATION, which
// is what fences a consumer that was partitioned away and has kept working.
//
// What this does *not* do: the FETCH path carries no group identity, so a
// zombie consumer can still read a partition it no longer owns. Its commits
// cannot land, so it cannot corrupt the group's position -- but it will do
// duplicate work. This is the same fencing model Kafka uses without
// transactions, and it is stated here rather than glossed over.
//
// Offsets are durable: every commit is appended to an internal log through the
// same storage engine as user data, and replayed at start-up.
#ifndef PULSELOG_CONSUMER_GROUP_COORDINATOR_H_
#define PULSELOG_CONSUMER_GROUP_COORDINATOR_H_

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "pulselog/base/status.h"
#include "pulselog/base/types.h"
#include "pulselog/consumer/assignor.h"
#include "pulselog/consumer/offset_store.h"
#include "pulselog/protocol/messages.h"

namespace pulselog::consumer {

struct GroupMember {
  std::string member_id;
  std::vector<std::string> topics;
  std::int64_t session_timeout_ms = 10'000;
  std::int64_t last_heartbeat_ms = 0;
  std::vector<TopicPartition> assignment;
};

enum class GroupState : std::uint8_t {
  kEmpty,        // No members.
  kRebalancing,  // Membership changed; assignments are being recomputed.
  kStable,       // Every member has an assignment for the current generation.
};

[[nodiscard]] std::string_view GroupStateName(GroupState state) noexcept;

struct GroupSnapshot {
  std::string group_id;
  GroupState state = GroupState::kEmpty;
  Generation generation{0};
  std::string leader_id;
  AssignmentStrategy strategy = AssignmentStrategy::kRange;
  std::vector<GroupMember> members;
};

// Resolves the partitions a group's subscribed topics contain. Supplied by the
// broker so the coordinator does not depend on the metadata module directly.
using PartitionLookup = std::function<std::vector<TopicPartition>(const std::vector<std::string>&)>;

class GroupCoordinator {
 public:
  GroupCoordinator(std::unique_ptr<OffsetStore> offsets,
                   PartitionLookup lookup,
                   std::int64_t default_session_timeout_ms);

  GroupCoordinator(const GroupCoordinator&) = delete;
  GroupCoordinator& operator=(const GroupCoordinator&) = delete;

  ~GroupCoordinator();

  [[nodiscard]] protocol::JoinGroupResponse Join(const protocol::JoinGroupRequest& request,
                                                 std::int64_t now_ms);

  [[nodiscard]] protocol::HeartbeatResponse Heartbeat(const protocol::HeartbeatRequest& request,
                                                      std::int64_t now_ms);

  [[nodiscard]] protocol::LeaveGroupResponse Leave(const protocol::LeaveGroupRequest& request,
                                                   std::int64_t now_ms);

  [[nodiscard]] protocol::CommitOffsetResponse Commit(const protocol::CommitOffsetRequest& request,
                                                      std::int64_t now_ms);

  [[nodiscard]] protocol::FetchOffsetResponse FetchOffset(
      const protocol::FetchOffsetRequest& request);

  // Removes members whose session has expired and rebalances their groups.
  // Returns how many members were evicted.
  std::size_t ExpireSessions(std::int64_t now_ms);

  [[nodiscard]] std::size_t GroupCount() const;

  [[nodiscard]] std::vector<GroupSnapshot> Describe() const;

  // Committed position of every partition a group owns; used for the
  // consumer-lag metric.
  [[nodiscard]] std::map<TopicPartition, Offset> CommittedOffsets(
      const std::string& group_id) const;

  [[nodiscard]] Status Flush();

  void Close();

 private:
  struct Group {
    std::string group_id;
    GroupState state = GroupState::kEmpty;
    Generation generation{0};
    std::string leader_id;
    AssignmentStrategy strategy = AssignmentStrategy::kRange;
    std::map<std::string, GroupMember> members;
    std::int64_t last_rebalance_ms = 0;
  };

  // Caller must hold `mutex_`. Bumps the generation and recomputes every
  // member's assignment.
  void RebalanceLocked(Group& group, std::int64_t now_ms);

  [[nodiscard]] std::string AllocateMemberId(const std::string& group_id);

  mutable std::mutex mutex_;
  std::unordered_map<std::string, Group> groups_;
  std::unique_ptr<OffsetStore> offsets_;
  PartitionLookup lookup_;
  std::int64_t default_session_timeout_ms_;
  std::uint64_t member_counter_ = 0;
};

}  // namespace pulselog::consumer

#endif  // PULSELOG_CONSUMER_GROUP_COORDINATOR_H_
