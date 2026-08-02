// PulseLog C++ client SDK.
//
// Three types, each with one job:
//
//   AdminClient   topic creation, metadata, health, cluster description
//   Producer      publish records, with optional client-side batching
//   Consumer      read by offset, optionally as a member of a consumer group
//
// All three are synchronous and single-threaded by design. A producer that
// wants concurrency creates one Producer per thread; that keeps the
// per-connection state lock-free and makes the ordering guarantee easy to
// state (records from one Producer to one partition are appended in the order
// the Producer sent them).
//
// The client maintains a metadata cache so it can route directly to the leader
// of each partition rather than paying a redirect. On NOT_LEADER it refreshes
// and retries according to the retry policy.
#ifndef PULSELOG_CLIENT_CLIENT_H_
#define PULSELOG_CLIENT_CLIENT_H_

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "pulselog/base/buffer.h"
#include "pulselog/base/status.h"
#include "pulselog/base/types.h"
#include "pulselog/metadata/cluster_metadata.h"
#include "pulselog/net/sync_client.h"
#include "pulselog/protocol/messages.h"
#include "pulselog/protocol/record.h"

namespace pulselog::client {

struct RetryPolicy {
  int max_attempts = 3;
  std::int64_t initial_backoff_ms = 50;
  std::int64_t max_backoff_ms = 1000;
  // Multiplier applied to the backoff after each failed attempt.
  double backoff_multiplier = 2.0;
};

struct ClientConfig {
  // Any broker; the client discovers the rest through metadata.
  std::vector<std::string> bootstrap_servers = {"127.0.0.1:9092"};
  std::int64_t connect_timeout_ms = 5000;
  std::int64_t request_timeout_ms = 10'000;
  std::int64_t metadata_refresh_ms = 30'000;
  RetryPolicy retry;
  std::uint32_t max_frame_bytes = 64U * 1024 * 1024;
};

// A record as the caller supplies it. `key` decides the partition; a null key
// round-robins.
struct OutboundRecord {
  std::string_view key;
  bool key_is_null = true;
  std::string_view value;
  TimestampMs timestamp = 0;  // 0 lets the broker stamp append time.
};

struct DeliveryResult {
  std::string topic;
  PartitionIndex partition{0};
  Offset base_offset = kInvalidOffset;
  Offset last_offset = kInvalidOffset;
  Offset high_water_mark = kInvalidOffset;
  std::uint32_t record_count = 0;
  std::size_t bytes = 0;
  std::int64_t latency_nanos = 0;
};

// A record as it comes back from a fetch. The views point into the consumer's
// internal buffer and stay valid until the next Poll().
struct InboundRecord {
  Offset offset = kInvalidOffset;
  TimestampMs timestamp = 0;
  bool key_is_null = true;
  std::string_view key;
  std::string_view value;
};

// Shared connection pool and metadata cache. Producers and consumers hold a
// reference; one per thread.
class ClientContext {
 public:
  explicit ClientContext(ClientConfig config);

  ClientContext(const ClientContext&) = delete;
  ClientContext& operator=(const ClientContext&) = delete;

  ~ClientContext();

  // Fetches metadata for `topics` (empty = all) and updates the cache.
  [[nodiscard]] Status RefreshMetadata(const std::vector<std::string>& topics);

  // Connection to the broker leading `topic`/`partition`, refreshing metadata
  // if the route is unknown.
  [[nodiscard]] Result<net::SyncClient*> LeaderFor(const std::string& topic,
                                                   PartitionIndex partition);

  // Connection to any reachable broker, for operations that are not
  // partition-scoped.
  [[nodiscard]] Result<net::SyncClient*> AnyBroker();

  // Like LeaderFor, but falls back to any broker when the topic is not yet
  // known -- the broker will auto-create it or redirect.
  [[nodiscard]] Result<net::SyncClient*> LeaderOrAny(const std::string& topic,
                                                     PartitionIndex partition);

  [[nodiscard]] Result<std::int32_t> PartitionCount(const std::string& topic);

  void InvalidateTopic(const std::string& topic);

  [[nodiscard]] RequestId NextRequestId() noexcept { return ++request_id_; }

  [[nodiscard]] const ClientConfig& config() const noexcept { return config_; }

  // Decodes a response payload, mapping the embedded error into a Status.
  //
  // The header is decoded first, on its own. An error response carries *only*
  // a header -- there is no meaningful body to send when the operation failed
  // -- so running the full typed decoder over it would fail on a truncated
  // body and report PROTOCOL_ERROR instead of the real error. Reading the
  // header first is what makes PROTOCOL.md's promise true: a client can always
  // surface the error, even for a response body it cannot parse.
  template <typename Response>
  [[nodiscard]] static Status DecodeResponse(ByteSpan payload, Response& response) {
    protocol::PayloadReader header_reader(payload);
    if (!response.header.Decode(header_reader)) {
      return ProtocolError("malformed response header");
    }
    if (!response.header.ok()) return response.header.ToStatus();

    protocol::PayloadReader reader(payload);
    if (!response.Decode(reader)) {
      return ProtocolError("malformed response payload");
    }
    return OkStatus();
  }

  void CloseAll();

 private:
  [[nodiscard]] Result<net::SyncClient*> ConnectTo(const net::Endpoint& endpoint);

  ClientConfig config_;
  std::map<std::string, std::unique_ptr<net::SyncClient>> connections_;
  std::map<std::string, protocol::TopicMetadata> topics_;
  std::map<std::int32_t, protocol::BrokerEndpoint> brokers_;
  std::int64_t last_refresh_ms_ = 0;
  RequestId request_id_ = 0;
  std::uint64_t round_robin_ = 0;
};

class AdminClient {
 public:
  explicit AdminClient(ClientContext& context) : context_(context) {}

  [[nodiscard]] Status CreateTopic(const std::string& topic, std::int32_t partitions,
                                   std::int16_t replication_factor = 1);

  [[nodiscard]] Status DeleteTopic(const std::string& topic);

  [[nodiscard]] Result<protocol::MetadataResponse> GetMetadata(
      const std::vector<std::string>& topics = {});

  [[nodiscard]] Result<protocol::HealthResponse> Health();

  [[nodiscard]] Result<protocol::ListTopicsResponse> ListTopics();

  [[nodiscard]] Result<protocol::DescribeClusterResponse> DescribeCluster();

 private:
  ClientContext& context_;
};

struct ProducerConfig {
  AckMode acks = AckMode::kLeader;
  std::int32_t request_timeout_ms = 5000;
  // Client-side batching. Records accumulate until either bound is reached or
  // Flush() is called. `linger_ms = 0` with `batch_size = 1` sends
  // immediately, which is the lowest-latency and lowest-throughput setting.
  std::size_t batch_records = 1;
  std::size_t batch_bytes = 64 * 1024;
  std::int64_t linger_ms = 0;
  // Explicit partition, or -1 to route by key.
  std::int32_t forced_partition = -1;
};

class Producer {
 public:
  Producer(ClientContext& context, ProducerConfig config = {});

  Producer(const Producer&) = delete;
  Producer& operator=(const Producer&) = delete;

  ~Producer();

  // Publishes one record. With batching enabled this may only buffer it; the
  // returned result is empty until the batch is actually sent.
  [[nodiscard]] Result<DeliveryResult> Send(const std::string& topic,
                                            const OutboundRecord& record);

  // Publishes a batch to one partition in a single request.
  [[nodiscard]] Result<DeliveryResult> SendBatch(const std::string& topic,
                                                 PartitionIndex partition,
                                                 const std::vector<OutboundRecord>& records);

  // Sends anything buffered. Returns the result of the last batch sent.
  [[nodiscard]] Result<DeliveryResult> Flush();

  [[nodiscard]] std::size_t PendingRecords() const noexcept { return pending_count_; }

  struct Stats {
    std::uint64_t records_sent = 0;
    std::uint64_t batches_sent = 0;
    std::uint64_t bytes_sent = 0;
    std::uint64_t retries = 0;
    std::uint64_t failures = 0;
  };

  [[nodiscard]] const Stats& stats() const noexcept { return stats_; }

 private:
  [[nodiscard]] Result<DeliveryResult> SendEncoded(const std::string& topic,
                                                   PartitionIndex partition, ByteSpan records,
                                                   std::uint32_t record_count);

  [[nodiscard]] Result<PartitionIndex> RouteFor(const std::string& topic,
                                                const OutboundRecord& record);

  ClientContext& context_;
  ProducerConfig config_;

  // Buffered batch state.
  std::string pending_topic_;
  PartitionIndex pending_partition_{0};
  ByteBuffer pending_;
  std::size_t pending_count_ = 0;
  std::int64_t first_buffered_nanos_ = 0;

  ByteBuffer scratch_;
  Stats stats_;
  std::uint64_t round_robin_ = 0;
};

struct ConsumerConfig {
  std::uint32_t max_bytes = 1U << 20U;
  std::uint32_t min_bytes = 1;
  std::int32_t max_wait_ms = 100;
  protocol::IsolationLevel isolation = protocol::IsolationLevel::kReadReplicated;
  // Consumer-group membership. Empty group id means a standalone consumer that
  // manages its own offsets.
  std::string group_id;
  std::vector<std::string> topics;
  std::int32_t session_timeout_ms = 10'000;
  protocol::AssignmentStrategy strategy = protocol::AssignmentStrategy::kRange;
  std::int64_t heartbeat_interval_ms = 3000;
  // Where to start when a group has no committed offset.
  bool start_from_earliest = true;
};

class Consumer {
 public:
  Consumer(ClientContext& context, ConsumerConfig config = {});

  Consumer(const Consumer&) = delete;
  Consumer& operator=(const Consumer&) = delete;

  ~Consumer();

  // --- standalone use -------------------------------------------------------

  // Reads from one partition starting at `offset`. Records are valid until the
  // next call.
  [[nodiscard]] Result<std::vector<InboundRecord>> Fetch(const std::string& topic,
                                                         PartitionIndex partition, Offset offset);

  [[nodiscard]] Result<Offset> ListOffset(const std::string& topic, PartitionIndex partition,
                                          TimestampMs timestamp);

  // --- group use ------------------------------------------------------------

  // Joins the configured group and receives an assignment.
  [[nodiscard]] Status Join();

  [[nodiscard]] Status Leave();

  // Fetches from the next assigned partition, round-robin, and sends a
  // heartbeat when one is due. Returns an empty vector when nothing is
  // available within `max_wait_ms`.
  [[nodiscard]] Result<std::vector<InboundRecord>> Poll();

  // Commits the position for the partition the last Poll() returned records
  // from. The committed offset is the next one to consume.
  [[nodiscard]] Status Commit();

  [[nodiscard]] Status CommitOffset(const std::string& topic, PartitionIndex partition,
                                    Offset offset);

  [[nodiscard]] Result<Offset> CommittedOffset(const std::string& topic,
                                               PartitionIndex partition);

  [[nodiscard]] Status Heartbeat();

  [[nodiscard]] const std::vector<TopicPartition>& assignment() const noexcept {
    return assignment_;
  }

  [[nodiscard]] Generation generation() const noexcept { return generation_; }

  [[nodiscard]] const std::string& member_id() const noexcept { return member_id_; }

  // Position the consumer will read from next, per partition.
  [[nodiscard]] const std::map<TopicPartition, Offset>& positions() const noexcept {
    return positions_;
  }

  struct Stats {
    std::uint64_t records_received = 0;
    std::uint64_t bytes_received = 0;
    std::uint64_t fetches = 0;
    std::uint64_t empty_fetches = 0;
    std::uint64_t rebalances = 0;
    std::uint64_t commits = 0;
  };

  [[nodiscard]] const Stats& stats() const noexcept { return stats_; }

 private:
  [[nodiscard]] Result<std::vector<InboundRecord>> FetchInto(const std::string& topic,
                                                             PartitionIndex partition,
                                                             Offset offset);

  ClientContext& context_;
  ConsumerConfig config_;

  ByteBuffer scratch_;
  ByteBuffer response_buffer_;

  std::string member_id_;
  Generation generation_{-1};
  std::vector<TopicPartition> assignment_;
  std::map<TopicPartition, Offset> positions_;
  std::size_t next_partition_ = 0;
  std::int64_t last_heartbeat_ms_ = 0;
  TopicPartition last_fetched_{};
  Offset last_fetch_next_offset_ = kInvalidOffset;
  Stats stats_;
};

}  // namespace pulselog::client

#endif  // PULSELOG_CLIENT_CLIENT_H_
