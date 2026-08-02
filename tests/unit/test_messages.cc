// Round-trip and malformed-input tests for every protocol message.
#include <random>
#include <vector>

#include <gtest/gtest.h>

#include "pulselog/base/buffer.h"
#include "pulselog/protocol/codec.h"
#include "pulselog/protocol/messages.h"

namespace pulselog::protocol {
namespace {

// Encodes `message`, decodes it into a fresh instance, and asserts the payload
// was fully consumed. Trailing bytes would mean the two codecs disagree.
//
// `backing` must outlive the returned message. Several messages hold
// `ByteSpan` views into the encoded payload rather than copying it (the whole
// point of the design), so a version of this helper that owned the buffer
// locally returned dangling spans -- caught by AddressSanitizer as a
// heap-use-after-free, which is exactly what it is.
template<typename T>
T RoundTrip(const T& message, ByteBuffer& backing) {
  backing.Clear();
  PayloadWriter writer(backing);
  message.Encode(writer);

  T decoded;
  PayloadReader reader(backing.Readable());
  EXPECT_TRUE(decoded.Decode(reader));
  EXPECT_TRUE(reader.Complete()) << "decoder left " << reader.Remaining() << " bytes unconsumed";
  return decoded;
}

// Convenience for messages that own everything they decode (no views), where
// the encoded buffer does not need to outlive the call.
template<typename T>
T RoundTrip(const T& message) {
  ByteBuffer backing;
  T decoded = RoundTrip(message, backing);
  // Guard against this overload being used for a view-holding message: if the
  // decoded value points into `backing`, the caller would get a dangling span.
  return decoded;
}

// Feeds every truncation of a valid encoding to the decoder. None may succeed
// and none may read out of bounds (ASan/UBSan builds enforce the latter).
template<typename T>
void ExpectTruncationRejected(const T& message) {
  ByteBuffer buf;
  PayloadWriter writer(buf);
  message.Encode(writer);
  const auto full = buf.Readable();

  for (std::size_t prefix = 0; prefix < full.size(); ++prefix) {
    T decoded;
    PayloadReader reader(full.subspan(0, prefix));
    const bool ok = decoded.Decode(reader);
    EXPECT_FALSE(ok && reader.Complete()) << "truncation to " << prefix << " bytes was accepted";
  }
}

// Randomly mutates a valid encoding many times. Decoding must never crash;
// whether it succeeds depends on which byte was hit.
template<typename T>
void ExpectFuzzSafe(const T& message, unsigned seed) {
  ByteBuffer buf;
  PayloadWriter writer(buf);
  message.Encode(writer);
  std::vector<std::uint8_t> bytes(buf.Readable().begin(), buf.Readable().end());
  if (bytes.empty()) return;

  std::mt19937 rng(seed);
  for (int trial = 0; trial < 500; ++trial) {
    std::vector<std::uint8_t> mutated = bytes;
    const std::size_t flips = 1 + (rng() % 4);
    for (std::size_t i = 0; i < flips; ++i) {
      mutated[rng() % mutated.size()] = static_cast<std::uint8_t>(rng());
    }
    T decoded;
    PayloadReader reader(mutated);
    (void)decoded.Decode(reader);  // Must not crash, hang, or read OOB.
  }
}

// --- Codec primitives ------------------------------------------------------

TEST(Codec, VarintRoundTrip) {
  const std::vector<std::uint64_t> values = {
      0, 1, 127, 128, 300, 16383, 16384, 1U << 20U, 1ULL << 35U, ~0ULL >> 1U, ~0ULL};
  ByteBuffer buf;
  PayloadWriter w(buf);
  for (const auto v : values) w.PutVarUInt(v);

  PayloadReader r(buf.Readable());
  for (const auto expected : values) {
    std::uint64_t actual = 0;
    ASSERT_TRUE(r.GetVarUInt(actual));
    EXPECT_EQ(actual, expected);
  }
  EXPECT_TRUE(r.Complete());
}

TEST(Codec, ZigZagRoundTrip) {
  const std::vector<std::int64_t> values = {0,
                                            -1,
                                            1,
                                            -2,
                                            2,
                                            -1000,
                                            1000,
                                            std::numeric_limits<std::int64_t>::min(),
                                            std::numeric_limits<std::int64_t>::max()};
  ByteBuffer buf;
  PayloadWriter w(buf);
  for (const auto v : values) w.PutVarInt(v);

  PayloadReader r(buf.Readable());
  for (const auto expected : values) {
    std::int64_t actual = 0;
    ASSERT_TRUE(r.GetVarInt(actual));
    EXPECT_EQ(actual, expected);
  }
}

TEST(Codec, ReaderRefusesToReadPastTheEnd) {
  const std::vector<std::uint8_t> data = {1, 2, 3};
  PayloadReader r(data);
  std::uint64_t v = 0;
  EXPECT_FALSE(r.GetU64(v));
  EXPECT_FALSE(r.ok());
}

TEST(Codec, MalformedVarintTerminates) {
  // Eleven continuation bytes: must fail rather than loop.
  const std::vector<std::uint8_t> data(11, 0x80);
  PayloadReader r(data);
  std::uint64_t v = 0;
  EXPECT_FALSE(r.GetVarUInt(v));
}

TEST(Codec, ArrayLengthIsBounded) {
  ByteBuffer buf;
  PayloadWriter w(buf);
  w.PutU32(1'000'000'000U);  // Claim a billion elements in a 4-byte payload.

  PayloadReader r(buf.Readable());
  std::uint32_t count = 0;
  EXPECT_FALSE(r.GetArrayLen(count, 10));
}

TEST(Codec, NullBytesRoundTrip) {
  ByteBuffer buf;
  PayloadWriter w(buf);
  w.PutNullableBytes(nullptr, 0);

  PayloadReader r(buf.Readable());
  ByteSpan out;
  ASSERT_TRUE(r.GetBytesView(out));
  EXPECT_TRUE(out.empty());
}

// --- Data plane ------------------------------------------------------------

TEST(Messages, ProduceRequestRoundTrip) {
  const std::string records = "raw-record-bytes";
  ProduceRequest req;
  req.topic = "orders";
  req.partition = PartitionIndex{3};
  req.acks = AckMode::kQuorum;
  req.timeout_ms = 2500;
  req.record_count = 7;
  req.records = AsBytes(records);

  ByteBuffer backing;
  const auto decoded = RoundTrip(req, backing);
  EXPECT_EQ(decoded.topic, "orders");
  EXPECT_EQ(decoded.partition, PartitionIndex{3});
  EXPECT_EQ(decoded.acks, AckMode::kQuorum);
  EXPECT_EQ(decoded.timeout_ms, 2500);
  EXPECT_EQ(decoded.record_count, 7U);
  EXPECT_EQ(AsStringView(decoded.records), records);

  ExpectTruncationRejected(req);
  ExpectFuzzSafe(req, 1);
}

TEST(Messages, ProduceRequestRejectsInvalidAckMode) {
  ByteBuffer buf;
  PayloadWriter w(buf);
  w.PutString("t");
  w.PutI32(0);
  w.PutU8(99);  // Not a valid AckMode.
  w.PutI32(0);
  w.PutU32(0);
  w.PutU32(0);

  ProduceRequest req;
  PayloadReader r(buf.Readable());
  EXPECT_FALSE(req.Decode(r));
}

TEST(Messages, ProduceRequestRejectsNegativePartition) {
  ByteBuffer buf;
  PayloadWriter w(buf);
  w.PutString("t");
  w.PutI32(-1);
  w.PutU8(1);
  w.PutI32(0);
  w.PutU32(0);
  w.PutU32(0);

  ProduceRequest req;
  PayloadReader r(buf.Readable());
  EXPECT_FALSE(req.Decode(r));
}

TEST(Messages, ProduceResponseRoundTrip) {
  ProduceResponse resp;
  resp.header.error = ErrorCode::kOk;
  resp.base_offset = 1000;
  resp.last_offset = 1009;
  resp.append_time = 1'700'000'000'123;
  resp.high_water_mark = 1010;

  const auto decoded = RoundTrip(resp);
  EXPECT_EQ(decoded.base_offset, 1000);
  EXPECT_EQ(decoded.last_offset, 1009);
  EXPECT_EQ(decoded.high_water_mark, 1010);
  ExpectTruncationRejected(resp);
}

TEST(Messages, ErrorResponseCarriesMessage) {
  ProduceResponse resp;
  resp.header.error = ErrorCode::kNotLeader;
  resp.header.error_message = "broker 2 leads orders-3";

  const auto decoded = RoundTrip(resp);
  EXPECT_EQ(decoded.header.error, ErrorCode::kNotLeader);
  EXPECT_EQ(decoded.header.error_message, "broker 2 leads orders-3");
  EXPECT_FALSE(decoded.header.ok());
  EXPECT_EQ(decoded.header.ToStatus().code(), ErrorCode::kNotLeader);
}

TEST(Messages, FetchRequestRoundTrip) {
  FetchRequest req;
  req.topic = "events";
  req.partition = PartitionIndex{0};
  req.fetch_offset = 98765;
  req.max_bytes = 1 << 18;
  req.min_bytes = 1024;
  req.max_wait_ms = 500;
  req.isolation = IsolationLevel::kReadUncommitted;

  const auto decoded = RoundTrip(req);
  EXPECT_EQ(decoded.fetch_offset, 98765);
  EXPECT_EQ(decoded.max_bytes, 1U << 18);
  EXPECT_EQ(decoded.min_bytes, 1024U);
  EXPECT_EQ(decoded.max_wait_ms, 500);
  EXPECT_EQ(decoded.isolation, IsolationLevel::kReadUncommitted);
  ExpectTruncationRejected(req);
  ExpectFuzzSafe(req, 2);
}

TEST(Messages, FetchResponseRoundTrip) {
  const std::string records(300, 'r');
  FetchResponse resp;
  resp.high_water_mark = 500;
  resp.log_start_offset = 100;
  resp.base_offset = 400;
  resp.record_count = 12;
  resp.records = AsBytes(records);

  ByteBuffer backing;
  const auto decoded = RoundTrip(resp, backing);
  EXPECT_EQ(decoded.high_water_mark, 500);
  EXPECT_EQ(decoded.log_start_offset, 100);
  EXPECT_EQ(decoded.record_count, 12U);
  EXPECT_EQ(decoded.records.size(), records.size());
  ExpectTruncationRejected(resp);
}

TEST(Messages, ListOffsetsRoundTrip) {
  ListOffsetsRequest req;
  req.topic = "t";
  req.partition = PartitionIndex{2};
  req.timestamp = kListOffsetsEarliest;
  EXPECT_EQ(RoundTrip(req).timestamp, kListOffsetsEarliest);

  ListOffsetsResponse resp;
  resp.offset = 77;
  resp.timestamp = 1234;
  const auto decoded = RoundTrip(resp);
  EXPECT_EQ(decoded.offset, 77);
  EXPECT_EQ(decoded.timestamp, 1234);
}

// --- Control plane ---------------------------------------------------------

TEST(Messages, CreateTopicRoundTrip) {
  CreateTopicRequest req;
  req.topic = "metrics";
  req.partitions = 12;
  req.replication_factor = 3;
  req.retention_ms = 86'400'000;
  req.segment_bytes = 128LL * 1024 * 1024;
  req.compression = Compression::kLz4Like;

  const auto decoded = RoundTrip(req);
  EXPECT_EQ(decoded.partitions, 12);
  EXPECT_EQ(decoded.replication_factor, 3);
  EXPECT_EQ(decoded.retention_ms, 86'400'000);
  EXPECT_EQ(decoded.segment_bytes, 128LL * 1024 * 1024);
  EXPECT_EQ(decoded.compression, Compression::kLz4Like);
  ExpectTruncationRejected(req);
  ExpectFuzzSafe(req, 3);
}

TEST(Messages, MetadataResponseRoundTrip) {
  MetadataResponse resp;
  resp.controller_id = BrokerId{1};
  resp.brokers = {{BrokerId{1}, "broker-1", 9092},
                  {BrokerId{2}, "broker-2", 9092},
                  {BrokerId{3}, "broker-3", 9092}};

  TopicMetadata topic;
  topic.name = "orders";
  for (int p = 0; p < 4; ++p) {
    PartitionMetadata meta;
    meta.index = PartitionIndex{p};
    meta.leader = BrokerId{1 + (p % 3)};
    meta.leader_epoch = 7;
    meta.replicas = {BrokerId{1}, BrokerId{2}, BrokerId{3}};
    meta.in_sync_replicas = {BrokerId{1}, BrokerId{2}};
    topic.partitions.push_back(meta);
  }
  resp.topics.push_back(topic);

  const auto decoded = RoundTrip(resp);
  ASSERT_EQ(decoded.brokers.size(), 3U);
  EXPECT_EQ(decoded.brokers[1].host, "broker-2");
  EXPECT_EQ(decoded.brokers[1].port, 9092);
  ASSERT_EQ(decoded.topics.size(), 1U);
  ASSERT_EQ(decoded.topics[0].partitions.size(), 4U);
  EXPECT_EQ(decoded.topics[0].partitions[2].leader, BrokerId{3});
  EXPECT_EQ(decoded.topics[0].partitions[2].leader_epoch, 7);
  EXPECT_EQ(decoded.topics[0].partitions[2].replicas.size(), 3U);
  EXPECT_EQ(decoded.topics[0].partitions[2].in_sync_replicas.size(), 2U);

  ExpectTruncationRejected(resp);
  ExpectFuzzSafe(resp, 4);
}

TEST(Messages, MetadataRequestEmptyMeansAllTopics) {
  MetadataRequest req;
  EXPECT_TRUE(RoundTrip(req).topics.empty());

  req.topics = {"a", "b", "c"};
  EXPECT_EQ(RoundTrip(req).topics.size(), 3U);
}

TEST(Messages, HealthResponseRoundTrip) {
  HealthResponse resp;
  resp.broker_id = BrokerId{2};
  resp.uptime_ms = 123'456;
  resp.hosted_partitions = 24;
  resp.leader_partitions = 8;
  resp.ready = true;
  resp.version = "0.1.0";

  const auto decoded = RoundTrip(resp);
  EXPECT_EQ(decoded.broker_id, BrokerId{2});
  EXPECT_EQ(decoded.uptime_ms, 123'456);
  EXPECT_EQ(decoded.leader_partitions, 8);
  EXPECT_TRUE(decoded.ready);
  EXPECT_EQ(decoded.version, "0.1.0");
}

TEST(Messages, ListTopicsRoundTrip) {
  ListTopicsResponse resp;
  resp.topics = {{"a", 3, 1024, 10}, {"b", 1, 2048, 20}};
  const auto decoded = RoundTrip(resp);
  ASSERT_EQ(decoded.topics.size(), 2U);
  EXPECT_EQ(decoded.topics[1].name, "b");
  EXPECT_EQ(decoded.topics[1].total_bytes, 2048);
}

// --- Consumer groups -------------------------------------------------------

TEST(Messages, JoinGroupRoundTrip) {
  JoinGroupRequest req;
  req.group_id = "analytics";
  req.member_id = "";
  req.topics = {"orders", "events"};
  req.session_timeout_ms = 30'000;
  req.strategy = AssignmentStrategy::kRoundRobin;

  const auto decoded = RoundTrip(req);
  EXPECT_EQ(decoded.group_id, "analytics");
  EXPECT_TRUE(decoded.member_id.empty());
  EXPECT_EQ(decoded.topics.size(), 2U);
  EXPECT_EQ(decoded.strategy, AssignmentStrategy::kRoundRobin);
  ExpectTruncationRejected(req);
  ExpectFuzzSafe(req, 5);
}

TEST(Messages, JoinGroupResponseCarriesAssignment) {
  JoinGroupResponse resp;
  resp.generation = Generation{4};
  resp.member_id = "consumer-a";
  resp.leader_id = "consumer-a";
  resp.assignment = {{"orders", PartitionIndex{0}}, {"orders", PartitionIndex{1}}};

  const auto decoded = RoundTrip(resp);
  EXPECT_EQ(decoded.generation, Generation{4});
  ASSERT_EQ(decoded.assignment.size(), 2U);
  EXPECT_EQ(decoded.assignment[1].topic, "orders");
  EXPECT_EQ(decoded.assignment[1].partition, PartitionIndex{1});
}

TEST(Messages, HeartbeatRoundTrip) {
  HeartbeatRequest req{"g", "m", Generation{9}};
  const auto decoded = RoundTrip(req);
  EXPECT_EQ(decoded.generation, Generation{9});

  HeartbeatResponse resp;
  resp.header.error = ErrorCode::kRebalanceInProgress;
  resp.generation = Generation{10};
  resp.rejoin_required = true;
  const auto decoded_resp = RoundTrip(resp);
  EXPECT_TRUE(decoded_resp.rejoin_required);
  EXPECT_EQ(decoded_resp.header.error, ErrorCode::kRebalanceInProgress);
}

TEST(Messages, OffsetCommitAndFetchRoundTrip) {
  CommitOffsetRequest commit;
  commit.group_id = "g";
  commit.member_id = "m";
  commit.generation = Generation{2};
  commit.topic = "t";
  commit.partition = PartitionIndex{5};
  commit.offset = 4242;
  commit.metadata = "checkpoint";

  const auto decoded = RoundTrip(commit);
  EXPECT_EQ(decoded.offset, 4242);
  EXPECT_EQ(decoded.metadata, "checkpoint");
  ExpectTruncationRejected(commit);

  FetchOffsetResponse resp;
  resp.offset = kInvalidOffset;
  EXPECT_EQ(RoundTrip(resp).offset, kInvalidOffset);
}

// --- Replication -----------------------------------------------------------

TEST(Messages, ReplicateRequestRoundTrip) {
  const std::string records(512, 'x');
  ReplicateRequest req;
  req.topic = "orders";
  req.partition = PartitionIndex{1};
  req.leader_id = BrokerId{1};
  req.leader_epoch = 3;
  req.base_offset = 1000;
  req.prev_offset = 999;
  req.leader_high_water_mark = 995;
  req.leader_log_start_offset = 0;
  req.record_count = 8;
  req.records = AsBytes(records);

  ByteBuffer backing;
  const auto decoded = RoundTrip(req, backing);
  EXPECT_EQ(decoded.leader_epoch, 3);
  EXPECT_EQ(decoded.base_offset, 1000);
  EXPECT_EQ(decoded.prev_offset, 999);
  EXPECT_EQ(decoded.leader_high_water_mark, 995);
  EXPECT_EQ(decoded.records.size(), records.size());
  ExpectTruncationRejected(req);
  ExpectFuzzSafe(req, 6);
}

TEST(Messages, ReplicateResponseRoundTrip) {
  ReplicateResponse resp;
  resp.follower_id = BrokerId{2};
  resp.log_end_offset = 1008;
  resp.flushed_offset = 1004;
  resp.leader_epoch = 3;

  const auto decoded = RoundTrip(resp);
  EXPECT_EQ(decoded.follower_id, BrokerId{2});
  EXPECT_EQ(decoded.log_end_offset, 1008);
  EXPECT_EQ(decoded.flushed_offset, 1004);
}

TEST(Messages, ReplicaFetchAndAckRoundTrip) {
  ReplicaFetchRequest fetch;
  fetch.topic = "t";
  fetch.partition = PartitionIndex{0};
  fetch.follower_id = BrokerId{3};
  fetch.leader_epoch = 5;
  fetch.fetch_offset = 200;
  fetch.max_bytes = 65536;
  EXPECT_EQ(RoundTrip(fetch).fetch_offset, 200);

  ReplicaAckRequest ack;
  ack.topic = "t";
  ack.partition = PartitionIndex{0};
  ack.follower_id = BrokerId{3};
  ack.leader_epoch = 5;
  ack.log_end_offset = 250;
  ack.flushed_offset = 248;
  const auto decoded = RoundTrip(ack);
  EXPECT_EQ(decoded.log_end_offset, 250);
  EXPECT_EQ(decoded.flushed_offset, 248);

  ReplicaAckResponse ack_resp;
  ack_resp.high_water_mark = 245;
  EXPECT_EQ(RoundTrip(ack_resp).high_water_mark, 245);
}

TEST(Messages, DescribeClusterRoundTrip) {
  DescribeClusterResponse resp;
  resp.controller_id = BrokerId{1};
  resp.brokers = {{BrokerId{1}, "a", 1}, {BrokerId{2}, "b", 2}};
  resp.live_brokers = {BrokerId{1}};

  const auto decoded = RoundTrip(resp);
  EXPECT_EQ(decoded.brokers.size(), 2U);
  EXPECT_EQ(decoded.live_brokers.size(), 1U);
}

TEST(Messages, EncodeErrorResponseIsDecodableAsAnyResponse) {
  ByteBuffer buf;
  EncodeErrorResponse(buf, ErrorCode::kBackpressure, "worker queue full");

  ResponseHeader header;
  PayloadReader r(buf.Readable());
  ASSERT_TRUE(header.Decode(r));
  EXPECT_EQ(header.error, ErrorCode::kBackpressure);
  EXPECT_EQ(header.error_message, "worker queue full");
  EXPECT_TRUE(IsRetryable(header.error));
}

}  // namespace
}  // namespace pulselog::protocol
