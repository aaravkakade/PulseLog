#include "pulselog/consumer/group_coordinator.h"

#include <algorithm>

#include "pulselog/base/logging.h"

namespace pulselog::consumer {
namespace {

constexpr std::string_view kComponent = "consumer.group";

}  // namespace

std::string_view GroupStateName(GroupState state) noexcept {
  switch (state) {
    case GroupState::kEmpty:
      return "empty";
    case GroupState::kRebalancing:
      return "rebalancing";
    case GroupState::kStable:
      return "stable";
  }
  return "unknown";
}

GroupCoordinator::GroupCoordinator(std::unique_ptr<OffsetStore> offsets,
                                   PartitionLookup lookup,
                                   std::int64_t default_session_timeout_ms)
    : offsets_(std::move(offsets)),
      lookup_(std::move(lookup)),
      default_session_timeout_ms_(default_session_timeout_ms) {}

GroupCoordinator::~GroupCoordinator() {
  Close();
}

std::string GroupCoordinator::AllocateMemberId(const std::string& group_id) {
  // Members are identified by a coordinator-assigned ID rather than anything
  // the client supplies, so a client cannot impersonate another member or
  // resurrect an expired session by reusing an old name.
  return group_id + "-" + std::to_string(++member_counter_);
}

void GroupCoordinator::RebalanceLocked(Group& group, std::int64_t now_ms) {
  // The generation is what fences stale members. It must increase on every
  // membership change, including the one that empties the group.
  group.generation = Generation{group.generation.value() + 1};
  group.last_rebalance_ms = now_ms;

  if (group.members.empty()) {
    group.state = GroupState::kEmpty;
    group.leader_id.clear();
    return;
  }

  std::vector<std::string> member_ids;
  std::vector<std::string> topics;
  member_ids.reserve(group.members.size());
  for (const auto& [id, member] : group.members) {
    member_ids.push_back(id);
    topics.insert(topics.end(), member.topics.begin(), member.topics.end());
  }
  std::sort(topics.begin(), topics.end());
  topics.erase(std::unique(topics.begin(), topics.end()), topics.end());

  const std::vector<TopicPartition> partitions =
      lookup_ ? lookup_(topics) : std::vector<TopicPartition>{};
  const Assignment assignment = Assign(group.strategy, member_ids, partitions);

  for (auto& [id, member] : group.members) {
    const auto it = assignment.find(id);
    member.assignment = it == assignment.end() ? std::vector<TopicPartition>{} : it->second;
  }

  // The lowest member ID leads. Deterministic, so every member agrees.
  group.leader_id = *std::min_element(member_ids.begin(), member_ids.end());
  group.state = GroupState::kStable;

  PL_INFO(kComponent) << "rebalanced group"
                      << " group=" << group.group_id << " generation=" << group.generation.value()
                      << " members=" << group.members.size() << " partitions=" << partitions.size()
                      << " strategy=" << AssignmentStrategyName(group.strategy);
}

protocol::JoinGroupResponse GroupCoordinator::Join(const protocol::JoinGroupRequest& request,
                                                   std::int64_t now_ms) {
  protocol::JoinGroupResponse response;
  if (request.group_id.empty()) {
    response.header.error = ErrorCode::kInvalidArgument;
    response.header.error_message = "group id must not be empty";
    return response;
  }
  if (request.topics.empty()) {
    response.header.error = ErrorCode::kInvalidArgument;
    response.header.error_message = "a member must subscribe to at least one topic";
    return response;
  }

  std::lock_guard<std::mutex> lock(mutex_);
  Group& group = groups_[request.group_id];
  if (group.group_id.empty()) {
    group.group_id = request.group_id;
    group.strategy = request.strategy;
  }

  std::string member_id = request.member_id;
  const bool is_new = member_id.empty() || group.members.find(member_id) == group.members.end();
  if (is_new) {
    member_id = AllocateMemberId(request.group_id);
  }

  GroupMember& member = group.members[member_id];
  const bool subscription_changed = member.topics != request.topics;
  member.member_id = member_id;
  member.topics = request.topics;
  member.session_timeout_ms =
      request.session_timeout_ms > 0 ? request.session_timeout_ms : default_session_timeout_ms_;
  member.last_heartbeat_ms = now_ms;

  // Rebalance only when the group's shape actually changed. A member
  // re-joining with the same subscription after a rebalance must not trigger
  // another one, or a group can rebalance forever.
  if (is_new || subscription_changed || group.state != GroupState::kStable) {
    RebalanceLocked(group, now_ms);
  }

  response.generation = group.generation;
  response.member_id = member_id;
  response.leader_id = group.leader_id;
  response.assignment = group.members[member_id].assignment;
  return response;
}

protocol::HeartbeatResponse GroupCoordinator::Heartbeat(const protocol::HeartbeatRequest& request,
                                                        std::int64_t now_ms) {
  protocol::HeartbeatResponse response;
  std::lock_guard<std::mutex> lock(mutex_);

  const auto group_it = groups_.find(request.group_id);
  if (group_it == groups_.end()) {
    response.header.error = ErrorCode::kNotFound;
    response.header.error_message = "unknown group '" + request.group_id + "'";
    return response;
  }
  Group& group = group_it->second;

  const auto member_it = group.members.find(request.member_id);
  if (member_it == group.members.end()) {
    // The session expired and the member was evicted. Re-joining is the only
    // way back, and it must get a fresh member ID.
    response.header.error = ErrorCode::kUnknownMember;
    response.header.error_message = "member '" + request.member_id + "' is not in the group";
    response.rejoin_required = true;
    response.generation = group.generation;
    return response;
  }

  if (request.generation != group.generation) {
    // The member missed a rebalance. Its assignment is stale, so it must stop
    // and re-join before doing anything else.
    response.header.error = ErrorCode::kRebalanceInProgress;
    response.header.error_message = "generation " + std::to_string(request.generation.value()) +
                                    " is stale; current is " +
                                    std::to_string(group.generation.value());
    response.rejoin_required = true;
    response.generation = group.generation;
    return response;
  }

  member_it->second.last_heartbeat_ms = now_ms;
  response.generation = group.generation;
  return response;
}

protocol::LeaveGroupResponse GroupCoordinator::Leave(const protocol::LeaveGroupRequest& request,
                                                     std::int64_t now_ms) {
  protocol::LeaveGroupResponse response;
  std::lock_guard<std::mutex> lock(mutex_);

  const auto group_it = groups_.find(request.group_id);
  if (group_it == groups_.end()) {
    response.header.error = ErrorCode::kNotFound;
    response.header.error_message = "unknown group";
    return response;
  }
  Group& group = group_it->second;
  if (group.members.erase(request.member_id) == 0) {
    response.header.error = ErrorCode::kUnknownMember;
    response.header.error_message = "member is not in the group";
    return response;
  }

  PL_INFO(kComponent) << "member left"
                      << " group=" << request.group_id << " member=" << request.member_id
                      << " remaining=" << group.members.size();
  RebalanceLocked(group, now_ms);
  return response;
}

protocol::CommitOffsetResponse GroupCoordinator::Commit(
    const protocol::CommitOffsetRequest& request, std::int64_t now_ms) {
  protocol::CommitOffsetResponse response;
  if (request.offset < 0) {
    response.header.error = ErrorCode::kInvalidArgument;
    response.header.error_message = "committed offset must not be negative";
    return response;
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto group_it = groups_.find(request.group_id);
    // A commit with no member ID is an unmanaged consumer positioning itself
    // manually. That is allowed; group fencing simply does not apply to it.
    if (!request.member_id.empty()) {
      if (group_it == groups_.end()) {
        response.header.error = ErrorCode::kNotFound;
        response.header.error_message = "unknown group '" + request.group_id + "'";
        return response;
      }
      const Group& group = group_it->second;
      const auto member_it = group.members.find(request.member_id);
      if (member_it == group.members.end()) {
        response.header.error = ErrorCode::kUnknownMember;
        response.header.error_message = "member is not in the group";
        return response;
      }
      if (request.generation != group.generation) {
        // This is the fence. A consumer that was partitioned away and kept
        // processing cannot move the group's committed position.
        response.header.error = ErrorCode::kIllegalGeneration;
        response.header.error_message = "generation " + std::to_string(request.generation.value()) +
                                        " is stale; current is " +
                                        std::to_string(group.generation.value());
        return response;
      }
      const auto& assignment = member_it->second.assignment;
      const TopicPartition target{request.topic, request.partition};
      if (std::find(assignment.begin(), assignment.end(), target) == assignment.end()) {
        response.header.error = ErrorCode::kIllegalGeneration;
        response.header.error_message =
            "partition " + target.ToString() + " is not assigned to this member";
        return response;
      }
    }
  }

  OffsetKey key;
  key.group_id = request.group_id;
  key.topic = request.topic;
  key.partition = request.partition;

  const Status status = offsets_->Commit(key, request.offset, request.metadata, now_ms);
  if (!status.ok()) {
    response.header.error = status.code();
    response.header.error_message = status.message();
  }
  return response;
}

protocol::FetchOffsetResponse GroupCoordinator::FetchOffset(
    const protocol::FetchOffsetRequest& request) {
  protocol::FetchOffsetResponse response;
  OffsetKey key;
  key.group_id = request.group_id;
  key.topic = request.topic;
  key.partition = request.partition;

  const auto committed = offsets_->Get(key);
  if (!committed.has_value()) {
    // Not an error: a group that has never committed simply has no position.
    response.offset = kInvalidOffset;
    return response;
  }
  response.offset = committed->offset;
  response.metadata = committed->metadata;
  return response;
}

std::size_t GroupCoordinator::ExpireSessions(std::int64_t now_ms) {
  std::size_t evicted = 0;
  std::lock_guard<std::mutex> lock(mutex_);

  for (auto& [group_id, group] : groups_) {
    std::vector<std::string> expired;
    for (const auto& [member_id, member] : group.members) {
      if (now_ms - member.last_heartbeat_ms > member.session_timeout_ms) {
        expired.push_back(member_id);
      }
    }
    if (expired.empty()) continue;

    for (const auto& member_id : expired) {
      PL_WARN(kComponent) << "evicting member: session expired"
                          << " group=" << group_id << " member=" << member_id
                          << " silent_ms=" << (now_ms - group.members[member_id].last_heartbeat_ms);
      group.members.erase(member_id);
      ++evicted;
    }
    RebalanceLocked(group, now_ms);
  }
  return evicted;
}

std::size_t GroupCoordinator::GroupCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return groups_.size();
}

std::vector<GroupSnapshot> GroupCoordinator::Describe() const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<GroupSnapshot> snapshots;
  snapshots.reserve(groups_.size());
  for (const auto& [group_id, group] : groups_) {
    GroupSnapshot snapshot;
    snapshot.group_id = group_id;
    snapshot.state = group.state;
    snapshot.generation = group.generation;
    snapshot.leader_id = group.leader_id;
    snapshot.strategy = group.strategy;
    snapshot.members.reserve(group.members.size());
    for (const auto& [member_id, member] : group.members) snapshot.members.push_back(member);
    snapshots.push_back(std::move(snapshot));
  }
  return snapshots;
}

std::map<TopicPartition, Offset> GroupCoordinator::CommittedOffsets(
    const std::string& group_id) const {
  std::map<TopicPartition, Offset> result;
  for (const auto& [key, value] : offsets_->ForGroup(group_id)) {
    result[TopicPartition{key.topic, key.partition}] = value.offset;
  }
  return result;
}

Status GroupCoordinator::Flush() {
  return offsets_ ? offsets_->Flush() : OkStatus();
}

void GroupCoordinator::Close() {
  if (offsets_ == nullptr) return;
  const Status status = offsets_->Close();
  if (!status.ok()) {
    PL_ERROR(kComponent) << "closing the offset store failed: " << status.ToString();
  }
}

}  // namespace pulselog::consumer
