// Codecs for the control-plane messages: topics, metadata, health.
#include "pulselog/protocol/messages.h"

namespace pulselog::protocol {
namespace {

// Array lengths are bounded by the bytes actually remaining, so a claimed
// count of a million elements in a 20-byte payload is rejected before any
// allocation happens.
[[nodiscard]] std::uint32_t ElementBudget(const PayloadReader& r, std::size_t min_element_bytes) {
  const std::size_t budget = r.Remaining() / (min_element_bytes == 0 ? 1 : min_element_bytes);
  return static_cast<std::uint32_t>(budget < kMaxArrayElements ? budget : kMaxArrayElements);
}

void EncodeBrokerIds(PayloadWriter& w, const std::vector<BrokerId>& ids) {
  w.PutArrayLen(ids.size());
  for (const BrokerId id : ids) w.PutI32(id.value());
}

[[nodiscard]] bool DecodeBrokerIds(PayloadReader& r, std::vector<BrokerId>& out) {
  std::uint32_t count = 0;
  if (!r.GetArrayLen(count, ElementBudget(r, 4))) return false;
  out.clear();
  out.reserve(count);
  for (std::uint32_t i = 0; i < count; ++i) {
    std::int32_t raw = 0;
    if (!r.GetI32(raw)) return false;
    out.emplace_back(raw);
  }
  return true;
}

}  // namespace

std::string_view AssignmentStrategyName(AssignmentStrategy s) noexcept {
  switch (s) {
    case AssignmentStrategy::kRange:
      return "range";
    case AssignmentStrategy::kRoundRobin:
      return "roundrobin";
  }
  return "unknown";
}

bool ParseAssignmentStrategy(std::string_view text, AssignmentStrategy& out) noexcept {
  if (text == "range") {
    out = AssignmentStrategy::kRange;
    return true;
  }
  if (text == "roundrobin" || text == "round-robin") {
    out = AssignmentStrategy::kRoundRobin;
    return true;
  }
  return false;
}

// --- CreateTopic / DeleteTopic ---------------------------------------------

void CreateTopicRequest::Encode(PayloadWriter& w) const {
  w.PutString(topic);
  w.PutI32(partitions);
  w.PutI16(replication_factor);
  w.PutI64(retention_ms);
  w.PutI64(segment_bytes);
  w.PutU8(static_cast<std::uint8_t>(compression));
  w.PutBool(from_controller);
}

bool CreateTopicRequest::Decode(PayloadReader& r) {
  std::uint8_t compression_raw = 0;
  if (!r.GetString(topic)) return false;
  if (!r.GetI32(partitions)) return false;
  if (!r.GetI16(replication_factor)) return false;
  if (!r.GetI64(retention_ms)) return false;
  if (!r.GetI64(segment_bytes)) return false;
  if (!r.GetU8(compression_raw)) return false;
  if (compression_raw > static_cast<std::uint8_t>(Compression::kLz4Like)) return false;
  compression = static_cast<Compression>(compression_raw);
  return r.GetBool(from_controller);
}

void CreateTopicResponse::Encode(PayloadWriter& w) const {
  header.Encode(w);
  w.PutI32(partitions);
}

bool CreateTopicResponse::Decode(PayloadReader& r) {
  if (!header.Decode(r)) return false;
  return r.GetI32(partitions);
}

void DeleteTopicRequest::Encode(PayloadWriter& w) const { w.PutString(topic); }

bool DeleteTopicRequest::Decode(PayloadReader& r) { return r.GetString(topic); }

void DeleteTopicResponse::Encode(PayloadWriter& w) const { header.Encode(w); }

bool DeleteTopicResponse::Decode(PayloadReader& r) { return header.Decode(r); }

// --- Metadata --------------------------------------------------------------

void MetadataRequest::Encode(PayloadWriter& w) const {
  w.PutArrayLen(topics.size());
  for (const auto& t : topics) w.PutString(t);
}

bool MetadataRequest::Decode(PayloadReader& r) {
  std::uint32_t count = 0;
  if (!r.GetArrayLen(count, ElementBudget(r, 2))) return false;
  topics.clear();
  topics.reserve(count);
  for (std::uint32_t i = 0; i < count; ++i) {
    std::string name;
    if (!r.GetString(name)) return false;
    topics.push_back(std::move(name));
  }
  return true;
}

void MetadataResponse::Encode(PayloadWriter& w) const {
  header.Encode(w);
  w.PutI32(controller_id.value());

  w.PutArrayLen(brokers.size());
  for (const auto& b : brokers) {
    w.PutI32(b.id.value());
    w.PutString(b.host);
    w.PutU16(b.port);
  }

  w.PutArrayLen(topics.size());
  for (const auto& t : topics) {
    w.PutString(t.name);
    w.PutArrayLen(t.partitions.size());
    for (const auto& p : t.partitions) {
      w.PutI32(p.index.value());
      w.PutI32(p.leader.value());
      w.PutI64(p.leader_epoch);
      EncodeBrokerIds(w, p.replicas);
      EncodeBrokerIds(w, p.in_sync_replicas);
    }
  }
}

bool MetadataResponse::Decode(PayloadReader& r) {
  if (!header.Decode(r)) return false;
  std::int32_t controller_raw = 0;
  if (!r.GetI32(controller_raw)) return false;
  controller_id = BrokerId{controller_raw};

  std::uint32_t broker_count = 0;
  if (!r.GetArrayLen(broker_count, ElementBudget(r, 8))) return false;
  brokers.clear();
  brokers.reserve(broker_count);
  for (std::uint32_t i = 0; i < broker_count; ++i) {
    BrokerEndpoint endpoint;
    std::int32_t id_raw = 0;
    if (!r.GetI32(id_raw)) return false;
    endpoint.id = BrokerId{id_raw};
    if (!r.GetString(endpoint.host)) return false;
    if (!r.GetU16(endpoint.port)) return false;
    brokers.push_back(std::move(endpoint));
  }

  std::uint32_t topic_count = 0;
  if (!r.GetArrayLen(topic_count, ElementBudget(r, 6))) return false;
  topics.clear();
  topics.reserve(topic_count);
  for (std::uint32_t i = 0; i < topic_count; ++i) {
    TopicMetadata topic;
    if (!r.GetString(topic.name)) return false;
    std::uint32_t partition_count = 0;
    if (!r.GetArrayLen(partition_count, ElementBudget(r, 24))) return false;
    topic.partitions.reserve(partition_count);
    for (std::uint32_t p = 0; p < partition_count; ++p) {
      PartitionMetadata meta;
      std::int32_t index_raw = 0;
      std::int32_t leader_raw = 0;
      if (!r.GetI32(index_raw)) return false;
      if (!r.GetI32(leader_raw)) return false;
      if (!r.GetI64(meta.leader_epoch)) return false;
      if (!DecodeBrokerIds(r, meta.replicas)) return false;
      if (!DecodeBrokerIds(r, meta.in_sync_replicas)) return false;
      meta.index = PartitionIndex{index_raw};
      meta.leader = BrokerId{leader_raw};
      topic.partitions.push_back(std::move(meta));
    }
    topics.push_back(std::move(topic));
  }
  return true;
}

// --- ListTopics ------------------------------------------------------------

void ListTopicsRequest::Encode(PayloadWriter&) const {}

bool ListTopicsRequest::Decode(PayloadReader&) { return true; }

void ListTopicsResponse::Encode(PayloadWriter& w) const {
  header.Encode(w);
  w.PutArrayLen(topics.size());
  for (const auto& t : topics) {
    w.PutString(t.name);
    w.PutI32(t.partitions);
    w.PutI64(t.total_bytes);
    w.PutI64(t.total_records);
  }
}

bool ListTopicsResponse::Decode(PayloadReader& r) {
  if (!header.Decode(r)) return false;
  std::uint32_t count = 0;
  if (!r.GetArrayLen(count, ElementBudget(r, 22))) return false;
  topics.clear();
  topics.reserve(count);
  for (std::uint32_t i = 0; i < count; ++i) {
    TopicSummary summary;
    if (!r.GetString(summary.name)) return false;
    if (!r.GetI32(summary.partitions)) return false;
    if (!r.GetI64(summary.total_bytes)) return false;
    if (!r.GetI64(summary.total_records)) return false;
    topics.push_back(std::move(summary));
  }
  return true;
}

// --- Health / DescribeCluster ----------------------------------------------

void HealthRequest::Encode(PayloadWriter&) const {}

bool HealthRequest::Decode(PayloadReader&) { return true; }

void HealthResponse::Encode(PayloadWriter& w) const {
  header.Encode(w);
  w.PutI32(broker_id.value());
  w.PutI64(uptime_ms);
  w.PutI32(hosted_partitions);
  w.PutI32(leader_partitions);
  w.PutBool(ready);
  w.PutString(version);
}

bool HealthResponse::Decode(PayloadReader& r) {
  if (!header.Decode(r)) return false;
  std::int32_t id_raw = 0;
  if (!r.GetI32(id_raw)) return false;
  broker_id = BrokerId{id_raw};
  if (!r.GetI64(uptime_ms)) return false;
  if (!r.GetI32(hosted_partitions)) return false;
  if (!r.GetI32(leader_partitions)) return false;
  if (!r.GetBool(ready)) return false;
  return r.GetString(version);
}

void DescribeClusterRequest::Encode(PayloadWriter&) const {}

bool DescribeClusterRequest::Decode(PayloadReader&) { return true; }

void DescribeClusterResponse::Encode(PayloadWriter& w) const {
  header.Encode(w);
  w.PutI32(controller_id.value());
  w.PutArrayLen(brokers.size());
  for (const auto& b : brokers) {
    w.PutI32(b.id.value());
    w.PutString(b.host);
    w.PutU16(b.port);
  }
  EncodeBrokerIds(w, live_brokers);
}

bool DescribeClusterResponse::Decode(PayloadReader& r) {
  if (!header.Decode(r)) return false;
  std::int32_t controller_raw = 0;
  if (!r.GetI32(controller_raw)) return false;
  controller_id = BrokerId{controller_raw};

  std::uint32_t count = 0;
  if (!r.GetArrayLen(count, ElementBudget(r, 8))) return false;
  brokers.clear();
  brokers.reserve(count);
  for (std::uint32_t i = 0; i < count; ++i) {
    BrokerEndpoint endpoint;
    std::int32_t id_raw = 0;
    if (!r.GetI32(id_raw)) return false;
    endpoint.id = BrokerId{id_raw};
    if (!r.GetString(endpoint.host)) return false;
    if (!r.GetU16(endpoint.port)) return false;
    brokers.push_back(std::move(endpoint));
  }
  return DecodeBrokerIds(r, live_brokers);
}

}  // namespace pulselog::protocol
