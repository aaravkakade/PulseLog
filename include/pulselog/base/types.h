// Core value types shared across every PulseLog module.
#ifndef PULSELOG_BASE_TYPES_H_
#define PULSELOG_BASE_TYPES_H_

#include <compare>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

namespace pulselog {

// A phantom-tagged integer. Prevents the classic bug of passing a partition
// index where a broker ID is expected, at zero runtime cost. Arithmetic is
// deliberately not provided: these are identities, not quantities.
template <typename Tag, typename Underlying = std::int32_t>
class StrongId {
 public:
  using UnderlyingType = Underlying;

  constexpr StrongId() noexcept = default;

  constexpr explicit StrongId(Underlying value) noexcept : value_(value) {}

  [[nodiscard]] constexpr Underlying value() const noexcept { return value_; }

  [[nodiscard]] constexpr bool valid() const noexcept { return value_ >= 0; }

  friend constexpr auto operator<=>(const StrongId&, const StrongId&) noexcept = default;

 private:
  Underlying value_ = -1;
};

struct BrokerIdTag {};

struct PartitionIndexTag {};

struct GenerationTag {};

// Identifies a broker process within a cluster. Assigned statically in the
// cluster configuration file.
using BrokerId = StrongId<BrokerIdTag, std::int32_t>;

// Zero-based partition index inside a topic.
using PartitionIndex = StrongId<PartitionIndexTag, std::int32_t>;

// Consumer-group generation, bumped on every completed rebalance.
using Generation = StrongId<GenerationTag, std::int32_t>;

// Monotonic position of a record inside a partition log. Offsets are dense:
// every appended record consumes exactly one. Kept as a plain integer because
// offset arithmetic (deltas, ranges, index lookups) is pervasive.
using Offset = std::int64_t;

// Sentinel meaning "no valid offset".
inline constexpr Offset kInvalidOffset = -1;

// Special fetch positions understood by the fetch API.
inline constexpr Offset kEarliestOffset = -2;
inline constexpr Offset kLatestOffset = -1;

// Milliseconds since the Unix epoch.
using TimestampMs = std::int64_t;

// Leader epoch: incremented every time leadership for a partition changes.
// Followers reject replication traffic from an epoch older than the one they
// have already observed, which is what makes stale-leader writes detectable.
using LeaderEpoch = std::int64_t;

// Monotonic per-connection request identifier used to correlate responses.
using RequestId = std::uint64_t;

// Identifies one partition of one topic.
struct TopicPartition {
  std::string topic;
  PartitionIndex partition;

  friend bool operator==(const TopicPartition& lhs, const TopicPartition& rhs) noexcept {
    return lhs.partition == rhs.partition && lhs.topic == rhs.topic;
  }

  // Ordering is topic-then-partition so an ordered container iterates a
  // topic's partitions consecutively and in index order.
  friend bool operator<(const TopicPartition& lhs, const TopicPartition& rhs) noexcept {
    if (lhs.topic != rhs.topic) return lhs.topic < rhs.topic;
    return lhs.partition < rhs.partition;
  }

  [[nodiscard]] std::string ToString() const {
    return topic + "-" + std::to_string(partition.value());
  }
};

// Durability level requested by a producer for a single publish.
//
//   kNone   : broker replies as soon as the request is parsed. No durability
//             guarantee at all; the record may be lost on broker crash.
//   kLeader : broker replies once the record is in the leader's log according
//             to the partition's configured flush policy. Survives process
//             crash if fsync has run; survives machine crash only with
//             flush_interval == 0 (synchronous fsync).
//   kQuorum : broker replies once a majority of the replica set (leader
//             included) has persisted the record. Survives the loss of any
//             minority of replicas.
enum class AckMode : std::uint8_t {
  kNone = 0,
  kLeader = 1,
  kQuorum = 2,
};

[[nodiscard]] std::string_view AckModeName(AckMode mode) noexcept;

// Parses "none" / "leader" / "quorum" (case-sensitive). Returns false on
// unknown input and leaves `out` untouched.
[[nodiscard]] bool ParseAckMode(std::string_view text, AckMode& out) noexcept;

// Compression applied to a record batch payload.
enum class Compression : std::uint8_t {
  kNone = 0,
  kLz4Like = 1,  // Built-in byte-oriented LZ77 codec; see src/storage/compression.cc.
};

[[nodiscard]] std::string_view CompressionName(Compression c) noexcept;

[[nodiscard]] bool ParseCompression(std::string_view text, Compression& out) noexcept;

// Validation shared by the client SDK and the broker. Topic names are limited
// so they can be used directly as directory names.
[[nodiscard]] bool IsValidTopicName(std::string_view name) noexcept;

inline constexpr std::size_t kMaxTopicNameLength = 200;

// Cache line size on the target platforms (64 B on x86-64 and Apple silicon's
// 128 B L1 line is handled by padding to 128 where measured to matter).
inline constexpr std::size_t kCacheLineSize = 64;

}  // namespace pulselog

namespace std {

template <typename Tag, typename Underlying>
struct hash<::pulselog::StrongId<Tag, Underlying>> {
  std::size_t operator()(const ::pulselog::StrongId<Tag, Underlying>& id) const noexcept {
    return std::hash<Underlying>{}(id.value());
  }
};

template <>
struct hash<::pulselog::TopicPartition> {
  std::size_t operator()(const ::pulselog::TopicPartition& tp) const noexcept {
    const std::size_t h1 = std::hash<std::string>{}(tp.topic);
    const std::size_t h2 = static_cast<std::size_t>(tp.partition.value());
    return h1 ^ (h2 * 0x9E3779B97F4A7C15ULL);
  }
};

}  // namespace std

#endif  // PULSELOG_BASE_TYPES_H_
