#include <algorithm>

#include "pulselog/base/clock.h"
#include "pulselog/client/client.h"
#include "pulselog/protocol/codec.h"

namespace pulselog::client {

ClientContext::ClientContext(ClientConfig config) : config_(std::move(config)) {
  if (config_.bootstrap_servers.empty()) {
    config_.bootstrap_servers.emplace_back("127.0.0.1:9092");
  }
}

ClientContext::~ClientContext() {
  CloseAll();
}

void ClientContext::CloseAll() {
  for (auto& [key, connection] : connections_) connection->Close();
  connections_.clear();
}

Result<net::SyncClient*> ClientContext::ConnectTo(const net::Endpoint& endpoint) {
  const std::string key = endpoint.ToString();
  const auto it = connections_.find(key);
  if (it != connections_.end() && it->second->connected()) return it->second.get();

  net::SyncClientOptions options;
  options.connect_timeout_ms = config_.connect_timeout_ms;
  options.request_timeout_ms = config_.request_timeout_ms;
  options.max_frame_bytes = config_.max_frame_bytes;

  auto connection = std::make_unique<net::SyncClient>(options);
  PL_RETURN_IF_ERROR(connection->Connect(endpoint));

  net::SyncClient* raw = connection.get();
  connections_[key] = std::move(connection);
  return raw;
}

Result<net::SyncClient*> ClientContext::AnyBroker() {
  // Prefer a broker we already know about; fall back to the bootstrap list.
  std::vector<net::Endpoint> candidates;
  candidates.reserve(brokers_.size() + config_.bootstrap_servers.size());
  for (const auto& [id, broker] : brokers_) {
    candidates.push_back(net::Endpoint{broker.host, broker.port});
  }
  for (const auto& spec : config_.bootstrap_servers) {
    auto endpoint = net::Endpoint::Parse(spec);
    if (endpoint.ok()) candidates.push_back(std::move(endpoint).value());
  }
  if (candidates.empty()) return Unavailable("no brokers configured");

  // Rotate the starting point so a dead first broker does not make every
  // request pay a connect timeout before trying the others.
  const std::size_t start = round_robin_++ % candidates.size();
  Status last_error = Unavailable("no broker reachable");
  for (std::size_t i = 0; i < candidates.size(); ++i) {
    auto connection = ConnectTo(candidates[(start + i) % candidates.size()]);
    if (connection.ok()) return connection;
    last_error = connection.status();
  }
  return last_error;
}

Result<net::SyncClient*> ClientContext::CoordinatorFor(const std::string& group_id) {
  if (brokers_.empty()) {
    PL_RETURN_IF_ERROR(RefreshMetadata({}));
  }
  if (brokers_.empty()) return Unavailable("no brokers known; cannot locate a coordinator");

  // The broker computes the same function over the same id-sorted list, so
  // both sides agree without an extra round trip.
  std::vector<protocol::BrokerEndpoint> sorted;
  sorted.reserve(brokers_.size());
  for (const auto& [id, broker] : brokers_) sorted.push_back(broker);
  std::sort(sorted.begin(),
            sorted.end(),
            [](const protocol::BrokerEndpoint& a, const protocol::BrokerEndpoint& b) {
              return a.id < b.id;
            });

  const BrokerId coordinator = metadata::CoordinatorForGroup(group_id, sorted);
  const auto it = brokers_.find(coordinator.value());
  if (it == brokers_.end()) {
    return Unavailable("no endpoint known for coordinator " + std::to_string(coordinator.value()));
  }
  return ConnectTo(net::Endpoint{it->second.host, it->second.port});
}

Status ClientContext::RefreshMetadata(const std::vector<std::string>& topics) {
  PL_ASSIGN_OR_RETURN(net::SyncClient * connection, AnyBroker());

  protocol::MetadataRequest request;
  request.topics = topics;

  ByteBuffer payload;
  protocol::PayloadWriter writer(payload);
  request.Encode(writer);

  auto frame = connection->Call(protocol::OpCode::kMetadata, NextRequestId(), payload.Readable());
  if (!frame.ok()) {
    // A broken connection must not stay in the pool; the next attempt should
    // reconnect rather than fail on the same dead socket.
    CloseAll();
    return frame.status();
  }

  protocol::MetadataResponse response;
  PL_RETURN_IF_ERROR(DecodeResponse(frame->payload, response));

  brokers_.clear();
  for (const auto& broker : response.brokers) brokers_[broker.id.value()] = broker;
  for (const auto& topic : response.topics) topics_[topic.name] = topic;
  last_refresh_ms_ = MonotonicNanos() / 1'000'000;
  return OkStatus();
}

void ClientContext::InvalidateTopic(const std::string& topic) {
  topics_.erase(topic);
}

Result<std::int32_t> ClientContext::PartitionCount(const std::string& topic) {
  auto it = topics_.find(topic);
  if (it == topics_.end()) {
    PL_RETURN_IF_ERROR(RefreshMetadata({topic}));
    it = topics_.find(topic);
    if (it == topics_.end()) return NotFound("unknown topic '" + topic + "'");
  }
  return static_cast<std::int32_t>(it->second.partitions.size());
}

Result<net::SyncClient*> ClientContext::LeaderFor(const std::string& topic,
                                                  PartitionIndex partition) {
  const std::int64_t now_ms = MonotonicNanos() / 1'000'000;
  const bool stale = now_ms - last_refresh_ms_ > config_.metadata_refresh_ms;

  auto it = topics_.find(topic);
  if (it == topics_.end() || stale) {
    PL_RETURN_IF_ERROR(RefreshMetadata({topic}));
    it = topics_.find(topic);
    if (it == topics_.end()) return NotFound("unknown topic '" + topic + "'");
  }

  const auto index = static_cast<std::size_t>(partition.value());
  if (partition.value() < 0 || index >= it->second.partitions.size()) {
    return NotFound("topic '" + topic + "' has no partition " + std::to_string(partition.value()));
  }

  const BrokerId leader = it->second.partitions[index].leader;
  const auto broker = brokers_.find(leader.value());
  if (broker == brokers_.end()) {
    return Unavailable("no endpoint known for broker " + std::to_string(leader.value()));
  }
  return ConnectTo(net::Endpoint{broker->second.host, broker->second.port});
}

Result<net::SyncClient*> ClientContext::LeaderOrAny(const std::string& topic,
                                                    PartitionIndex partition) {
  auto leader = LeaderFor(topic, partition);
  if (leader.ok()) return leader;
  // An unknown topic is not a dead end: any broker can accept the request and
  // either auto-create the topic or answer NOT_LEADER with the real owner.
  if (leader.status().code() != ErrorCode::kNotFound) return leader;
  return AnyBroker();
}

// --- AdminClient ------------------------------------------------------------

Status AdminClient::CreateTopic(const std::string& topic,
                                std::int32_t partitions,
                                std::int16_t replication_factor) {
  PL_ASSIGN_OR_RETURN(net::SyncClient * connection, context_.AnyBroker());

  protocol::CreateTopicRequest request;
  request.topic = topic;
  request.partitions = partitions;
  request.replication_factor = replication_factor;

  ByteBuffer payload;
  protocol::PayloadWriter writer(payload);
  request.Encode(writer);

  PL_ASSIGN_OR_RETURN(
      auto frame,
      connection->Call(
          protocol::OpCode::kCreateTopic, context_.NextRequestId(), payload.Readable()));
  protocol::CreateTopicResponse response;
  PL_RETURN_IF_ERROR(ClientContext::DecodeResponse(frame.payload, response));

  // The routing table changed; drop the cached view so the next produce
  // resolves the new leaders.
  context_.InvalidateTopic(topic);
  return OkStatus();
}

Status AdminClient::DeleteTopic(const std::string& topic) {
  PL_ASSIGN_OR_RETURN(net::SyncClient * connection, context_.AnyBroker());

  protocol::DeleteTopicRequest request;
  request.topic = topic;
  ByteBuffer payload;
  protocol::PayloadWriter writer(payload);
  request.Encode(writer);

  PL_ASSIGN_OR_RETURN(
      auto frame,
      connection->Call(
          protocol::OpCode::kDeleteTopic, context_.NextRequestId(), payload.Readable()));
  protocol::DeleteTopicResponse response;
  PL_RETURN_IF_ERROR(ClientContext::DecodeResponse(frame.payload, response));
  context_.InvalidateTopic(topic);
  return OkStatus();
}

Result<protocol::MetadataResponse> AdminClient::GetMetadata(
    const std::vector<std::string>& topics) {
  PL_ASSIGN_OR_RETURN(net::SyncClient * connection, context_.AnyBroker());

  protocol::MetadataRequest request;
  request.topics = topics;
  ByteBuffer payload;
  protocol::PayloadWriter writer(payload);
  request.Encode(writer);

  PL_ASSIGN_OR_RETURN(
      auto frame,
      connection->Call(protocol::OpCode::kMetadata, context_.NextRequestId(), payload.Readable()));
  protocol::MetadataResponse response;
  PL_RETURN_IF_ERROR(ClientContext::DecodeResponse(frame.payload, response));
  return response;
}

Result<protocol::HealthResponse> AdminClient::Health() {
  PL_ASSIGN_OR_RETURN(net::SyncClient * connection, context_.AnyBroker());
  PL_ASSIGN_OR_RETURN(
      auto frame,
      connection->Call(protocol::OpCode::kHealth, context_.NextRequestId(), ByteSpan{}));
  protocol::HealthResponse response;
  PL_RETURN_IF_ERROR(ClientContext::DecodeResponse(frame.payload, response));
  return response;
}

Result<protocol::ListTopicsResponse> AdminClient::ListTopics() {
  PL_ASSIGN_OR_RETURN(net::SyncClient * connection, context_.AnyBroker());
  PL_ASSIGN_OR_RETURN(
      auto frame,
      connection->Call(protocol::OpCode::kListTopics, context_.NextRequestId(), ByteSpan{}));
  protocol::ListTopicsResponse response;
  PL_RETURN_IF_ERROR(ClientContext::DecodeResponse(frame.payload, response));
  return response;
}

Result<protocol::DescribeClusterResponse> AdminClient::DescribeCluster() {
  PL_ASSIGN_OR_RETURN(net::SyncClient * connection, context_.AnyBroker());
  PL_ASSIGN_OR_RETURN(
      auto frame,
      connection->Call(protocol::OpCode::kDescribeCluster, context_.NextRequestId(), ByteSpan{}));
  protocol::DescribeClusterResponse response;
  PL_RETURN_IF_ERROR(ClientContext::DecodeResponse(frame.payload, response));
  return response;
}

}  // namespace pulselog::client
