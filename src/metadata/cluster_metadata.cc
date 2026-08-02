#include "pulselog/metadata/cluster_metadata.h"

#include <algorithm>
#include <charconv>
#include <fstream>
#include <sstream>

#include "pulselog/base/crc32c.h"
#include "pulselog/base/logging.h"

namespace pulselog::metadata {
namespace {

constexpr std::string_view kComponent = "metadata";

constexpr std::string_view kFileHeader = "pulselog-metadata v1";

std::vector<std::string> Split(std::string_view text, char delimiter) {
  std::vector<std::string> parts;
  std::size_t start = 0;
  while (start <= text.size()) {
    const auto end = text.find(delimiter, start);
    if (end == std::string_view::npos) {
      parts.emplace_back(text.substr(start));
      break;
    }
    parts.emplace_back(text.substr(start, end - start));
    start = end + 1;
  }
  return parts;
}

template<typename T>
bool ParseNumber(std::string_view text, T& out) {
  const auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), out);
  return ec == std::errc{} && ptr == text.data() + text.size();
}

}  // namespace

bool PartitionAssignment::HasReplica(BrokerId broker) const {
  return std::find(replicas.begin(), replicas.end(), broker) != replicas.end();
}

PartitionIndex PartitionForKey(ByteSpan key, std::int32_t partition_count) noexcept {
  if (partition_count <= 1) return PartitionIndex{0};
  // Masking the sign bit keeps the modulus non-negative without a branch.
  const std::uint32_t hash = Crc32c(key) & 0x7FFFFFFFU;
  return PartitionIndex{
      static_cast<std::int32_t>(hash % static_cast<std::uint32_t>(partition_count))};
}

BrokerId CoordinatorForGroup(std::string_view group_id,
                             const std::vector<BrokerEndpoint>& brokers) noexcept {
  if (brokers.empty()) return BrokerId{-1};
  const std::uint32_t hash = Crc32c(AsBytes(group_id));
  return brokers[hash % brokers.size()].id;
}

PartitionIndex PartitionRoundRobin(std::uint64_t counter, std::int32_t partition_count) noexcept {
  if (partition_count <= 1) return PartitionIndex{0};
  return PartitionIndex{
      static_cast<std::int32_t>(counter % static_cast<std::uint64_t>(partition_count))};
}

Status ClusterMetadata::SetBrokersFromSpec(const std::vector<std::string>& specs) {
  std::vector<BrokerEndpoint> brokers;
  brokers.reserve(specs.size());

  for (const auto& spec : specs) {
    const auto at = spec.find('@');
    if (at == std::string::npos) {
      return InvalidArgument("broker spec must be id@host:port, got '" + spec + "'");
    }
    std::int32_t id = 0;
    if (!ParseNumber(std::string_view(spec).substr(0, at), id) || id < 0) {
      return InvalidArgument("broker spec has an invalid id: '" + spec + "'");
    }
    const std::string address = spec.substr(at + 1);
    const auto colon = address.rfind(':');
    if (colon == std::string::npos) {
      return InvalidArgument("broker spec must be id@host:port, got '" + spec + "'");
    }
    std::uint32_t port = 0;
    if (!ParseNumber(std::string_view(address).substr(colon + 1), port) || port == 0 ||
        port > 65535) {
      return InvalidArgument("broker spec has an invalid port: '" + spec + "'");
    }

    BrokerEndpoint endpoint;
    endpoint.id = BrokerId{id};
    endpoint.host = address.substr(0, colon);
    endpoint.port = static_cast<std::uint16_t>(port);
    brokers.push_back(std::move(endpoint));
  }

  // Assignment is positional, so every broker must agree on the order.
  std::sort(brokers.begin(), brokers.end(), [](const BrokerEndpoint& a, const BrokerEndpoint& b) {
    return a.id < b.id;
  });
  for (std::size_t i = 1; i < brokers.size(); ++i) {
    if (brokers[i].id == brokers[i - 1].id) {
      return InvalidArgument("duplicate broker id " + std::to_string(brokers[i].id.value()));
    }
  }

  SetBrokers(std::move(brokers));
  return OkStatus();
}

void ClusterMetadata::SetBrokers(std::vector<BrokerEndpoint> brokers) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  brokers_ = std::move(brokers);
}

std::vector<BrokerEndpoint> ClusterMetadata::Brokers() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return brokers_;
}

std::optional<BrokerEndpoint> ClusterMetadata::FindBroker(BrokerId id) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  const auto it = std::find_if(
      brokers_.begin(), brokers_.end(), [id](const BrokerEndpoint& b) { return b.id == id; });
  if (it == brokers_.end()) return std::nullopt;
  return *it;
}

BrokerId ClusterMetadata::ControllerId() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  // The lowest-numbered broker is the controller by convention. With static
  // assignment the controller only answers admin requests, so a fixed choice
  // is adequate; an election implementation would replace this.
  return brokers_.empty() ? BrokerId{-1} : brokers_.front().id;
}

BrokerId ClusterMetadata::CoordinatorFor(std::string_view group_id) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  // brokers_ is kept sorted by id, so every broker computes the same answer.
  return CoordinatorForGroup(group_id, brokers_);
}

std::vector<PartitionAssignment> ClusterMetadata::ComputeAssignments(
    const TopicConfig& config) const {
  std::vector<PartitionAssignment> assignments;
  assignments.reserve(static_cast<std::size_t>(config.partition_count));

  const std::size_t broker_count = brokers_.size();
  if (broker_count == 0) return assignments;

  const auto replication =
      std::min<std::size_t>(static_cast<std::size_t>(config.replication_factor), broker_count);
  // Offsetting by a hash of the topic name stops every topic from putting its
  // partition-0 leader on the same broker.
  // size_t throughout: the offset is only ever used as an index into the
  // broker list, and computing it in uint32_t narrows the modulus result.
  const std::size_t topic_offset = Crc32c(AsBytes(config.name)) % broker_count;

  for (std::int32_t p = 0; p < config.partition_count; ++p) {
    PartitionAssignment assignment;
    assignment.index = PartitionIndex{p};
    assignment.leader_epoch = 0;
    assignment.replicas.reserve(replication);
    for (std::size_t r = 0; r < replication; ++r) {
      const std::size_t slot = (static_cast<std::size_t>(p) + r + topic_offset) % broker_count;
      assignment.replicas.push_back(brokers_[slot].id);
    }
    assignment.leader = assignment.replicas.front();
    assignment.in_sync_replicas = assignment.replicas;
    assignments.push_back(std::move(assignment));
  }
  return assignments;
}

Result<TopicDescriptor> ClusterMetadata::CreateTopic(const TopicConfig& config, bool* created) {
  if (created != nullptr) *created = false;
  if (!IsValidTopicName(config.name)) {
    return InvalidArgument("invalid topic name '" + config.name +
                           "' (allowed: letters, digits, . _ -)");
  }
  if (config.partition_count <= 0) {
    return InvalidArgument("partition count must be positive, got " +
                           std::to_string(config.partition_count));
  }
  if (config.replication_factor <= 0) {
    return InvalidArgument("replication factor must be positive");
  }

  std::unique_lock<std::shared_mutex> lock(mutex_);
  if (brokers_.empty()) {
    return Unavailable("cluster membership is not configured");
  }
  if (config.replication_factor > static_cast<std::int16_t>(brokers_.size())) {
    return InvalidArgument("replication factor " + std::to_string(config.replication_factor) +
                           " exceeds the broker count " + std::to_string(brokers_.size()));
  }

  const auto existing = topics_.find(config.name);
  if (existing != topics_.end()) {
    // Idempotent creation, but only when the request matches. A silently
    // different partition count would reroute keys.
    if (existing->second.config.partition_count != config.partition_count ||
        existing->second.config.replication_factor != config.replication_factor) {
      return AlreadyExists("topic '" + config.name + "' exists with " +
                           std::to_string(existing->second.config.partition_count) +
                           " partitions and replication factor " +
                           std::to_string(existing->second.config.replication_factor));
    }
    return existing->second;
  }

  TopicDescriptor descriptor;
  descriptor.config = config;
  descriptor.partitions = ComputeAssignments(config);
  topics_.emplace(config.name, descriptor);
  if (created != nullptr) *created = true;

  PL_INFO(kComponent) << "created topic"
                      << " topic=" << config.name << " partitions=" << config.partition_count
                      << " replication_factor=" << config.replication_factor;
  return descriptor;
}

Status ClusterMetadata::UpsertTopic(TopicDescriptor descriptor) {
  if (!IsValidTopicName(descriptor.config.name)) {
    return InvalidArgument("invalid topic name '" + descriptor.config.name + "'");
  }
  std::unique_lock<std::shared_mutex> lock(mutex_);
  topics_[descriptor.config.name] = std::move(descriptor);
  return OkStatus();
}

Status ClusterMetadata::DeleteTopic(const std::string& name) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  if (topics_.erase(name) == 0) return NotFound("topic '" + name + "' does not exist");
  PL_INFO(kComponent) << "deleted topic topic=" << name;
  return OkStatus();
}

bool ClusterMetadata::HasTopic(const std::string& name) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return topics_.find(name) != topics_.end();
}

Result<TopicDescriptor> ClusterMetadata::GetTopic(const std::string& name) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  const auto it = topics_.find(name);
  if (it == topics_.end()) return NotFound("unknown topic '" + name + "'");
  return it->second;
}

std::vector<TopicDescriptor> ClusterMetadata::ListTopics() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  std::vector<TopicDescriptor> topics;
  topics.reserve(topics_.size());
  for (const auto& [name, descriptor] : topics_) topics.push_back(descriptor);
  return topics;
}

Result<PartitionAssignment> ClusterMetadata::GetPartition(const std::string& topic,
                                                          PartitionIndex partition) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  const auto it = topics_.find(topic);
  if (it == topics_.end()) return NotFound("unknown topic '" + topic + "'");

  const auto index = static_cast<std::size_t>(partition.value());
  if (partition.value() < 0 || index >= it->second.partitions.size()) {
    return NotFound("topic '" + topic + "' has no partition " + std::to_string(partition.value()));
  }
  return it->second.partitions[index];
}

std::vector<TopicPartition> ClusterMetadata::PartitionsLedBy(BrokerId broker) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  std::vector<TopicPartition> result;
  for (const auto& [name, descriptor] : topics_) {
    for (const auto& partition : descriptor.partitions) {
      if (partition.leader == broker) result.push_back(TopicPartition{name, partition.index});
    }
  }
  return result;
}

std::vector<TopicPartition> ClusterMetadata::PartitionsHostedBy(BrokerId broker) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  std::vector<TopicPartition> result;
  for (const auto& [name, descriptor] : topics_) {
    for (const auto& partition : descriptor.partitions) {
      if (partition.HasReplica(broker)) result.push_back(TopicPartition{name, partition.index});
    }
  }
  return result;
}

Status ClusterMetadata::UpdateInSyncReplicas(const std::string& topic,
                                             PartitionIndex partition,
                                             std::vector<BrokerId> in_sync) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  const auto it = topics_.find(topic);
  if (it == topics_.end()) return NotFound("unknown topic '" + topic + "'");
  const auto index = static_cast<std::size_t>(partition.value());
  if (index >= it->second.partitions.size()) return NotFound("unknown partition");
  it->second.partitions[index].in_sync_replicas = std::move(in_sync);
  return OkStatus();
}

Status ClusterMetadata::SetLeader(const std::string& topic,
                                  PartitionIndex partition,
                                  BrokerId leader) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  const auto it = topics_.find(topic);
  if (it == topics_.end()) return NotFound("unknown topic '" + topic + "'");
  const auto index = static_cast<std::size_t>(partition.value());
  if (index >= it->second.partitions.size()) return NotFound("unknown partition");

  PartitionAssignment& assignment = it->second.partitions[index];
  if (!assignment.HasReplica(leader)) {
    return InvalidArgument("broker " + std::to_string(leader.value()) + " is not a replica of " +
                           topic + "-" + std::to_string(partition.value()));
  }
  if (assignment.leader == leader) return OkStatus();

  assignment.leader = leader;
  // The epoch is what lets a follower reject writes from a leader that has
  // been superseded. It must increase on every leadership change.
  ++assignment.leader_epoch;
  PL_INFO(kComponent) << "leader changed"
                      << " topic=" << topic << " partition=" << partition.value()
                      << " leader=" << leader.value() << " epoch=" << assignment.leader_epoch;
  return OkStatus();
}

Status ClusterMetadata::SaveTo(const std::filesystem::path& path) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  // Write to a temporary then rename: a crash mid-write must not leave a
  // half-written metadata file that the next start-up cannot parse.
  const std::filesystem::path temp = path.string() + ".tmp";
  {
    std::ofstream out(temp, std::ios::trunc);
    if (!out) return IoError("cannot write metadata to " + temp.string());

    out << kFileHeader << '\n';
    for (const auto& [name, descriptor] : topics_) {
      const auto& config = descriptor.config;
      out << "topic\t" << config.name << '\t' << config.partition_count << '\t'
          << config.replication_factor << '\t' << config.retention_ms << '\t'
          << config.segment_bytes << '\t' << static_cast<int>(config.compression) << '\n';
      for (const auto& partition : descriptor.partitions) {
        out << "partition\t" << config.name << '\t' << partition.index.value() << '\t'
            << partition.leader.value() << '\t' << partition.leader_epoch << '\t';
        for (std::size_t i = 0; i < partition.replicas.size(); ++i) {
          if (i > 0) out << ',';
          out << partition.replicas[i].value();
        }
        out << '\n';
      }
    }
    if (!out) return IoError("failed while writing " + temp.string());
  }

  std::error_code ec;
  std::filesystem::rename(temp, path, ec);
  if (ec) return IoError("cannot rename metadata file: " + ec.message());
  return OkStatus();
}

Status ClusterMetadata::LoadFrom(const std::filesystem::path& path) {
  std::ifstream in(path);
  if (!in) return NotFound("metadata file not found: " + path.string());

  std::string line;
  if (!std::getline(in, line) || line != kFileHeader) {
    return Corruption("metadata file has an unrecognised header: '" + line + "'");
  }

  std::map<std::string, TopicDescriptor, std::less<>> loaded;
  int line_number = 1;
  while (std::getline(in, line)) {
    ++line_number;
    if (line.empty()) continue;
    const auto fields = Split(line, '\t');

    if (fields[0] == "topic") {
      if (fields.size() < 7) {
        return Corruption(path.string() + ":" + std::to_string(line_number) +
                          ": malformed topic line");
      }
      TopicDescriptor descriptor;
      descriptor.config.name = fields[1];
      int compression = 0;
      if (!ParseNumber(fields[2], descriptor.config.partition_count) ||
          !ParseNumber(fields[3], descriptor.config.replication_factor) ||
          !ParseNumber(fields[4], descriptor.config.retention_ms) ||
          !ParseNumber(fields[5], descriptor.config.segment_bytes) ||
          !ParseNumber(fields[6], compression)) {
        return Corruption(path.string() + ":" + std::to_string(line_number) +
                          ": unparseable topic fields");
      }
      descriptor.config.compression = static_cast<Compression>(compression);
      loaded[descriptor.config.name] = std::move(descriptor);
    } else if (fields[0] == "partition") {
      if (fields.size() < 6) {
        return Corruption(path.string() + ":" + std::to_string(line_number) +
                          ": malformed partition line");
      }
      const auto it = loaded.find(fields[1]);
      if (it == loaded.end()) {
        return Corruption(path.string() + ":" + std::to_string(line_number) +
                          ": partition for unknown topic '" + fields[1] + "'");
      }
      PartitionAssignment assignment;
      std::int32_t index = 0;
      std::int32_t leader = 0;
      if (!ParseNumber(fields[2], index) || !ParseNumber(fields[3], leader) ||
          !ParseNumber(fields[4], assignment.leader_epoch)) {
        return Corruption(path.string() + ":" + std::to_string(line_number) +
                          ": unparseable partition fields");
      }
      assignment.index = PartitionIndex{index};
      assignment.leader = BrokerId{leader};
      for (const auto& replica : Split(fields[5], ',')) {
        std::int32_t id = 0;
        if (!replica.empty() && ParseNumber(replica, id)) {
          assignment.replicas.emplace_back(id);
        }
      }
      assignment.in_sync_replicas = assignment.replicas;
      it->second.partitions.push_back(std::move(assignment));
    }
    // Unknown record types are skipped so a newer broker's file stays
    // loadable by an older one.
  }

  // Partitions must be dense and in index order; lookups index directly.
  for (auto& [name, descriptor] : loaded) {
    std::sort(descriptor.partitions.begin(),
              descriptor.partitions.end(),
              [](const PartitionAssignment& a, const PartitionAssignment& b) {
                return a.index < b.index;
              });
    for (std::size_t i = 0; i < descriptor.partitions.size(); ++i) {
      if (descriptor.partitions[i].index.value() != static_cast<std::int32_t>(i)) {
        return Corruption("topic '" + name + "' has a gap at partition index " + std::to_string(i));
      }
    }
  }

  std::unique_lock<std::shared_mutex> lock(mutex_);
  topics_ = std::move(loaded);
  PL_INFO(kComponent) << "loaded metadata topics=" << topics_.size()
                      << " path=" << path.filename().string();
  return OkStatus();
}

protocol::MetadataResponse ClusterMetadata::BuildMetadataResponse(
    const std::vector<std::string>& requested) const {
  protocol::MetadataResponse response;
  std::shared_lock<std::shared_mutex> lock(mutex_);

  response.controller_id = brokers_.empty() ? BrokerId{-1} : brokers_.front().id;
  response.brokers = brokers_;

  const auto append = [&response](const TopicDescriptor& descriptor) {
    protocol::TopicMetadata topic;
    topic.name = descriptor.config.name;
    topic.partitions.reserve(descriptor.partitions.size());
    for (const auto& partition : descriptor.partitions) {
      protocol::PartitionMetadata meta;
      meta.index = partition.index;
      meta.leader = partition.leader;
      meta.leader_epoch = partition.leader_epoch;
      meta.replicas = partition.replicas;
      meta.in_sync_replicas = partition.in_sync_replicas;
      topic.partitions.push_back(std::move(meta));
    }
    response.topics.push_back(std::move(topic));
  };

  if (requested.empty()) {
    for (const auto& [name, descriptor] : topics_) append(descriptor);
  } else {
    for (const auto& name : requested) {
      const auto it = topics_.find(name);
      if (it != topics_.end()) append(it->second);
    }
  }
  return response;
}

}  // namespace pulselog::metadata
