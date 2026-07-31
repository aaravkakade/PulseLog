// Codecs for consumer-group and replication messages.
#include "pulselog/protocol/messages.h"

namespace pulselog::protocol {
namespace {

[[nodiscard]] std::uint32_t ElementBudget(const PayloadReader& r, std::size_t min_element_bytes) {
  const std::size_t budget = r.Remaining() / (min_element_bytes == 0 ? 1 : min_element_bytes);
  return static_cast<std::uint32_t>(budget < kMaxArrayElements ? budget : kMaxArrayElements);
}

}  // namespace

// --- JoinGroup -------------------------------------------------------------

void JoinGroupRequest::Encode(PayloadWriter& w) const {
  w.PutString(group_id);
  w.PutString(member_id);
  w.PutArrayLen(topics.size());
  for (const auto& t : topics) w.PutString(t);
  w.PutI32(session_timeout_ms);
  w.PutU8(static_cast<std::uint8_t>(strategy));
}

bool JoinGroupRequest::Decode(PayloadReader& r) {
  if (!r.GetString(group_id)) return false;
  if (!r.GetString(member_id)) return false;

  std::uint32_t count = 0;
  if (!r.GetArrayLen(count, ElementBudget(r, 2))) return false;
  topics.clear();
  topics.reserve(count);
  for (std::uint32_t i = 0; i < count; ++i) {
    std::string name;
    if (!r.GetString(name)) return false;
    topics.push_back(std::move(name));
  }

  if (!r.GetI32(session_timeout_ms)) return false;
  std::uint8_t strategy_raw = 0;
  if (!r.GetU8(strategy_raw)) return false;
  if (strategy_raw > static_cast<std::uint8_t>(AssignmentStrategy::kRoundRobin)) return false;
  strategy = static_cast<AssignmentStrategy>(strategy_raw);
  return true;
}

void JoinGroupResponse::Encode(PayloadWriter& w) const {
  header.Encode(w);
  w.PutI32(generation.value());
  w.PutString(member_id);
  w.PutString(leader_id);
  w.PutArrayLen(assignment.size());
  for (const auto& tp : assignment) {
    w.PutString(tp.topic);
    w.PutI32(tp.partition.value());
  }
}

bool JoinGroupResponse::Decode(PayloadReader& r) {
  if (!header.Decode(r)) return false;
  std::int32_t generation_raw = 0;
  if (!r.GetI32(generation_raw)) return false;
  generation = Generation{generation_raw};
  if (!r.GetString(member_id)) return false;
  if (!r.GetString(leader_id)) return false;

  std::uint32_t count = 0;
  if (!r.GetArrayLen(count, ElementBudget(r, 6))) return false;
  assignment.clear();
  assignment.reserve(count);
  for (std::uint32_t i = 0; i < count; ++i) {
    TopicPartition tp;
    std::int32_t partition_raw = 0;
    if (!r.GetString(tp.topic)) return false;
    if (!r.GetI32(partition_raw)) return false;
    if (partition_raw < 0) return false;
    tp.partition = PartitionIndex{partition_raw};
    assignment.push_back(std::move(tp));
  }
  return true;
}

// --- Heartbeat / LeaveGroup ------------------------------------------------

void HeartbeatRequest::Encode(PayloadWriter& w) const {
  w.PutString(group_id);
  w.PutString(member_id);
  w.PutI32(generation.value());
}

bool HeartbeatRequest::Decode(PayloadReader& r) {
  if (!r.GetString(group_id)) return false;
  if (!r.GetString(member_id)) return false;
  std::int32_t raw = 0;
  if (!r.GetI32(raw)) return false;
  generation = Generation{raw};
  return true;
}

void HeartbeatResponse::Encode(PayloadWriter& w) const {
  header.Encode(w);
  w.PutI32(generation.value());
  w.PutBool(rejoin_required);
}

bool HeartbeatResponse::Decode(PayloadReader& r) {
  if (!header.Decode(r)) return false;
  std::int32_t raw = 0;
  if (!r.GetI32(raw)) return false;
  generation = Generation{raw};
  return r.GetBool(rejoin_required);
}

void LeaveGroupRequest::Encode(PayloadWriter& w) const {
  w.PutString(group_id);
  w.PutString(member_id);
}

bool LeaveGroupRequest::Decode(PayloadReader& r) {
  if (!r.GetString(group_id)) return false;
  return r.GetString(member_id);
}

void LeaveGroupResponse::Encode(PayloadWriter& w) const { header.Encode(w); }

bool LeaveGroupResponse::Decode(PayloadReader& r) { return header.Decode(r); }

// --- Offset commit / fetch -------------------------------------------------

void CommitOffsetRequest::Encode(PayloadWriter& w) const {
  w.PutString(group_id);
  w.PutString(member_id);
  w.PutI32(generation.value());
  w.PutString(topic);
  w.PutI32(partition.value());
  w.PutI64(offset);
  w.PutString(metadata);
}

bool CommitOffsetRequest::Decode(PayloadReader& r) {
  if (!r.GetString(group_id)) return false;
  if (!r.GetString(member_id)) return false;
  std::int32_t generation_raw = 0;
  if (!r.GetI32(generation_raw)) return false;
  generation = Generation{generation_raw};
  if (!r.GetString(topic)) return false;
  std::int32_t partition_raw = 0;
  if (!r.GetI32(partition_raw)) return false;
  if (partition_raw < 0) return false;
  partition = PartitionIndex{partition_raw};
  if (!r.GetI64(offset)) return false;
  return r.GetString(metadata);
}

void CommitOffsetResponse::Encode(PayloadWriter& w) const { header.Encode(w); }

bool CommitOffsetResponse::Decode(PayloadReader& r) { return header.Decode(r); }

void FetchOffsetRequest::Encode(PayloadWriter& w) const {
  w.PutString(group_id);
  w.PutString(topic);
  w.PutI32(partition.value());
}

bool FetchOffsetRequest::Decode(PayloadReader& r) {
  if (!r.GetString(group_id)) return false;
  if (!r.GetString(topic)) return false;
  std::int32_t partition_raw = 0;
  if (!r.GetI32(partition_raw)) return false;
  if (partition_raw < 0) return false;
  partition = PartitionIndex{partition_raw};
  return true;
}

void FetchOffsetResponse::Encode(PayloadWriter& w) const {
  header.Encode(w);
  w.PutI64(offset);
  w.PutString(metadata);
}

bool FetchOffsetResponse::Decode(PayloadReader& r) {
  if (!header.Decode(r)) return false;
  if (!r.GetI64(offset)) return false;
  return r.GetString(metadata);
}

// --- Replication -----------------------------------------------------------

void ReplicateRequest::Encode(PayloadWriter& w) const {
  w.PutString(topic);
  w.PutI32(partition.value());
  w.PutI32(leader_id.value());
  w.PutI64(leader_epoch);
  w.PutI64(base_offset);
  w.PutI64(prev_offset);
  w.PutI64(leader_high_water_mark);
  w.PutI64(leader_log_start_offset);
  w.PutU32(record_count);
  w.PutU32(static_cast<std::uint32_t>(records.size()));
  w.PutRaw(records);
}

bool ReplicateRequest::Decode(PayloadReader& r) {
  if (!r.GetString(topic)) return false;
  std::int32_t partition_raw = 0;
  std::int32_t leader_raw = 0;
  if (!r.GetI32(partition_raw)) return false;
  if (!r.GetI32(leader_raw)) return false;
  if (partition_raw < 0) return false;
  partition = PartitionIndex{partition_raw};
  leader_id = BrokerId{leader_raw};
  if (!r.GetI64(leader_epoch)) return false;
  if (!r.GetI64(base_offset)) return false;
  if (!r.GetI64(prev_offset)) return false;
  if (!r.GetI64(leader_high_water_mark)) return false;
  if (!r.GetI64(leader_log_start_offset)) return false;
  if (!r.GetU32(record_count)) return false;
  std::uint32_t records_len = 0;
  if (!r.GetU32(records_len)) return false;
  if (r.Remaining() < records_len) return false;
  records = r.GetRemaining().subspan(0, records_len);
  return true;
}

void ReplicateResponse::Encode(PayloadWriter& w) const {
  header.Encode(w);
  w.PutI32(follower_id.value());
  w.PutI64(log_end_offset);
  w.PutI64(flushed_offset);
  w.PutI64(leader_epoch);
}

bool ReplicateResponse::Decode(PayloadReader& r) {
  if (!header.Decode(r)) return false;
  std::int32_t follower_raw = 0;
  if (!r.GetI32(follower_raw)) return false;
  follower_id = BrokerId{follower_raw};
  if (!r.GetI64(log_end_offset)) return false;
  if (!r.GetI64(flushed_offset)) return false;
  return r.GetI64(leader_epoch);
}

void ReplicaFetchRequest::Encode(PayloadWriter& w) const {
  w.PutString(topic);
  w.PutI32(partition.value());
  w.PutI32(follower_id.value());
  w.PutI64(leader_epoch);
  w.PutI64(fetch_offset);
  w.PutU32(max_bytes);
}

bool ReplicaFetchRequest::Decode(PayloadReader& r) {
  if (!r.GetString(topic)) return false;
  std::int32_t partition_raw = 0;
  std::int32_t follower_raw = 0;
  if (!r.GetI32(partition_raw)) return false;
  if (!r.GetI32(follower_raw)) return false;
  if (partition_raw < 0) return false;
  partition = PartitionIndex{partition_raw};
  follower_id = BrokerId{follower_raw};
  if (!r.GetI64(leader_epoch)) return false;
  if (!r.GetI64(fetch_offset)) return false;
  return r.GetU32(max_bytes);
}

void ReplicaFetchResponse::Encode(PayloadWriter& w) const {
  header.Encode(w);
  w.PutI64(leader_epoch);
  w.PutI64(high_water_mark);
  w.PutI64(log_start_offset);
  w.PutI64(base_offset);
  w.PutU32(record_count);
  w.PutU32(static_cast<std::uint32_t>(records.size()));
  w.PutRaw(records);
}

bool ReplicaFetchResponse::Decode(PayloadReader& r) {
  if (!header.Decode(r)) return false;
  if (!r.GetI64(leader_epoch)) return false;
  if (!r.GetI64(high_water_mark)) return false;
  if (!r.GetI64(log_start_offset)) return false;
  if (!r.GetI64(base_offset)) return false;
  if (!r.GetU32(record_count)) return false;
  std::uint32_t records_len = 0;
  if (!r.GetU32(records_len)) return false;
  if (r.Remaining() < records_len) return false;
  records = r.GetRemaining().subspan(0, records_len);
  return true;
}

void ReplicaAckRequest::Encode(PayloadWriter& w) const {
  w.PutString(topic);
  w.PutI32(partition.value());
  w.PutI32(follower_id.value());
  w.PutI64(leader_epoch);
  w.PutI64(log_end_offset);
  w.PutI64(flushed_offset);
}

bool ReplicaAckRequest::Decode(PayloadReader& r) {
  if (!r.GetString(topic)) return false;
  std::int32_t partition_raw = 0;
  std::int32_t follower_raw = 0;
  if (!r.GetI32(partition_raw)) return false;
  if (!r.GetI32(follower_raw)) return false;
  if (partition_raw < 0) return false;
  partition = PartitionIndex{partition_raw};
  follower_id = BrokerId{follower_raw};
  if (!r.GetI64(leader_epoch)) return false;
  if (!r.GetI64(log_end_offset)) return false;
  return r.GetI64(flushed_offset);
}

void ReplicaAckResponse::Encode(PayloadWriter& w) const {
  header.Encode(w);
  w.PutI64(high_water_mark);
}

bool ReplicaAckResponse::Decode(PayloadReader& r) {
  if (!header.Decode(r)) return false;
  return r.GetI64(high_water_mark);
}

}  // namespace pulselog::protocol
