// Multi-broker tests: replication, the high-water mark, quorum
// acknowledgements, follower failure, leader restart and catch-up.
#include <algorithm>
#include <chrono>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "test_support/cluster_fixture.h"
#include <gtest/gtest.h>

namespace pulselog {
namespace {

using testing::ClusterFixture;
using testing::ConsumeAll;
using testing::ProduceRecords;
using testing::WaitUntil;

// Sums the log end offsets a broker holds for one topic partition, or -1 when
// it does not host it.
Offset LocalLogEnd(broker::Broker* instance, const std::string& topic, std::int32_t partition) {
  if (instance == nullptr) return -1;
  auto* replica = instance->partitions().Find(TopicPartition{topic, PartitionIndex{partition}});
  return replica == nullptr ? -1 : replica->log().LogEndOffset();
}

class ReplicationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ClusterFixture::Options options;
    options.broker_count = 3;
    options.default_partitions = 3;
    options.default_replication_factor = 3;
    options.replication_interval_ms = 1;
    cluster_ = std::make_unique<ClusterFixture>(options);
    ASSERT_TRUE(cluster_->StartAll().ok());
    context_ = cluster_->MakeClient();
  }

  void TearDown() override {
    context_.reset();
    cluster_.reset();
  }

  std::unique_ptr<ClusterFixture> cluster_;
  std::unique_ptr<client::ClientContext> context_;
};

TEST_F(ReplicationTest, LeadershipIsSpreadAcrossBrokers) {
  client::AdminClient admin(*context_);
  ASSERT_TRUE(admin.CreateTopic("spread", 6, 3).ok());

  auto metadata = admin.GetMetadata({"spread"});
  ASSERT_TRUE(metadata.ok());
  ASSERT_EQ(metadata->topics.size(), 1U);

  std::set<std::int32_t> leaders;
  for (const auto& partition : metadata->topics[0].partitions) {
    leaders.insert(partition.leader.value());
    EXPECT_EQ(partition.replicas.size(), 3U) << "replication factor 3 means 3 replicas";
    EXPECT_EQ(partition.replicas[0], partition.leader) << "the leader must be the first replica";
  }
  EXPECT_EQ(leaders.size(), 3U) << "6 partitions over 3 brokers should use all three as leaders";
}

TEST_F(ReplicationTest, RecordsReachEveryFollower) {
  client::AdminClient admin(*context_);
  ASSERT_TRUE(admin.CreateTopic("replicated", 1, 3).ok());

  client::Producer producer(*context_);
  ASSERT_TRUE(ProduceRecords(producer, "replicated", PartitionIndex{0}, 200).ok());

  // Every broker must eventually hold all 200 records in its own local log.
  ASSERT_TRUE(WaitUntil([&] {
    for (std::size_t i = 0; i < 3; ++i) {
      if (LocalLogEnd(cluster_->broker(i), "replicated", 0) != 200) return false;
    }
    return true;
  })) << "followers did not converge on the leader's log";

  for (std::size_t i = 0; i < 3; ++i) {
    EXPECT_EQ(LocalLogEnd(cluster_->broker(i), "replicated", 0), 200)
        << "broker " << i << " is missing records";
  }
}

TEST_F(ReplicationTest, ReplicatedBytesAreIdentical) {
  client::AdminClient admin(*context_);
  ASSERT_TRUE(admin.CreateTopic("identical", 1, 3).ok());

  client::Producer producer(*context_);
  ASSERT_TRUE(ProduceRecords(producer, "identical", PartitionIndex{0}, 100, "payload", 64).ok());
  ASSERT_TRUE(WaitUntil([&] {
    for (std::size_t i = 0; i < 3; ++i) {
      if (LocalLogEnd(cluster_->broker(i), "identical", 0) != 100) return false;
    }
    return true;
  }));

  // Read the records straight out of each broker's own log and compare. Byte
  // equality is the real property -- offsets, timestamps and checksums all
  // have to match, not just the payloads.
  std::vector<std::string> per_broker;
  for (std::size_t i = 0; i < 3; ++i) {
    auto* replica =
        cluster_->broker(i)->partitions().Find(TopicPartition{"identical", PartitionIndex{0}});
    ASSERT_NE(replica, nullptr);
    ByteBuffer buffer;
    auto read = replica->log().Read(0, 1 << 22, buffer);
    ASSERT_TRUE(read.ok()) << read.status().ToString();
    EXPECT_EQ(read->record_count, 100U) << "broker " << i;
    per_broker.emplace_back(AsStringView(buffer.Readable()));
  }
  EXPECT_EQ(per_broker[0], per_broker[1]) << "follower 1 diverged from the leader";
  EXPECT_EQ(per_broker[0], per_broker[2]) << "follower 2 diverged from the leader";
}

TEST_F(ReplicationTest, HighWaterMarkTracksReplication) {
  client::AdminClient admin(*context_);
  ASSERT_TRUE(admin.CreateTopic("hwm", 1, 3).ok());

  client::Producer producer(*context_);
  ASSERT_TRUE(ProduceRecords(producer, "hwm", PartitionIndex{0}, 50).ok());

  // Find the leader and check the high-water mark converges to the log end.
  auto metadata = admin.GetMetadata({"hwm"});
  ASSERT_TRUE(metadata.ok());
  const std::int32_t leader_id = metadata->topics[0].partitions[0].leader.value();
  auto* leader = cluster_->broker(static_cast<std::size_t>(leader_id));
  ASSERT_NE(leader, nullptr);
  auto* replica = leader->partitions().Find(TopicPartition{"hwm", PartitionIndex{0}});
  ASSERT_NE(replica, nullptr);

  ASSERT_TRUE(WaitUntil([&] { return replica->HighWaterMark() == 50; }))
      << "high-water mark stuck at " << replica->HighWaterMark();
  EXPECT_LE(replica->HighWaterMark(), replica->log().LogEndOffset())
      << "the high-water mark may never exceed the leader's own log";
}

TEST_F(ReplicationTest, QuorumAckSucceedsWithAllReplicasUp) {
  client::AdminClient admin(*context_);
  ASSERT_TRUE(admin.CreateTopic("quorum", 1, 3).ok());

  client::ProducerConfig config;
  config.acks = AckMode::kQuorum;
  client::Producer producer(*context_, config);

  auto last = ProduceRecords(producer, "quorum", PartitionIndex{0}, 100);
  ASSERT_TRUE(last.ok()) << last.status().ToString();
  EXPECT_EQ(last.value(), 99);

  // A quorum acknowledgement means a majority persisted it. Verify directly.
  auto metadata = admin.GetMetadata({"quorum"});
  ASSERT_TRUE(metadata.ok());
  const std::int32_t leader_id = metadata->topics[0].partitions[0].leader.value();
  auto* replica = cluster_->broker(static_cast<std::size_t>(leader_id))
                      ->partitions()
                      .Find(TopicPartition{"quorum", PartitionIndex{0}});
  ASSERT_NE(replica, nullptr);
  EXPECT_GE(replica->PersistedReplicaCount(99), 2U)
      << "a quorum of 3 is 2; fewer than that means the ack was premature";
}

TEST_F(ReplicationTest, ConsumersReadThroughAnyReplicaSetSize) {
  client::AdminClient admin(*context_);
  ASSERT_TRUE(admin.CreateTopic("readable", 3, 3).ok());

  client::Producer producer(*context_);
  for (std::int32_t p = 0; p < 3; ++p) {
    ASSERT_TRUE(ProduceRecords(producer, "readable", PartitionIndex{p}, 40).ok());
  }

  client::Consumer consumer(*context_);
  for (std::int32_t p = 0; p < 3; ++p) {
    auto values = ConsumeAll(consumer, "readable", PartitionIndex{p}, 0, 40);
    ASSERT_TRUE(values.ok()) << "partition " << p << ": " << values.status().ToString();
    EXPECT_EQ(values->size(), 40U) << "partition " << p;
  }
}

TEST_F(ReplicationTest, LeaderAckWritesContinueAfterAFollowerDies) {
  client::AdminClient admin(*context_);
  ASSERT_TRUE(admin.CreateTopic("survive", 1, 3).ok());

  client::Producer producer(*context_);
  ASSERT_TRUE(ProduceRecords(producer, "survive", PartitionIndex{0}, 50).ok());

  auto metadata = admin.GetMetadata({"survive"});
  ASSERT_TRUE(metadata.ok());
  const std::int32_t leader_id = metadata->topics[0].partitions[0].leader.value();

  // Stop a broker that is a follower for this partition.
  std::size_t victim = 0;
  for (std::size_t i = 0; i < 3; ++i) {
    if (static_cast<std::int32_t>(i) != leader_id) {
      victim = i;
      break;
    }
  }
  cluster_->StopBroker(victim);

  // Leader acks must keep working: they never depended on that follower.
  auto after = ProduceRecords(producer, "survive", PartitionIndex{0}, 50, "after");
  ASSERT_TRUE(after.ok()) << after.status().ToString();
  EXPECT_EQ(after.value(), 99);

  auto* replica = cluster_->broker(static_cast<std::size_t>(leader_id))
                      ->partitions()
                      .Find(TopicPartition{"survive", PartitionIndex{0}});
  ASSERT_NE(replica, nullptr);
  EXPECT_EQ(replica->log().LogEndOffset(), 100);

  // The dead follower must eventually leave the in-sync set, or the
  // high-water mark would be pinned behind it forever.
  ASSERT_TRUE(WaitUntil([&] { return replica->HighWaterMark() == 100; }, std::chrono::seconds(20)))
      << "high-water mark stayed at " << replica->HighWaterMark()
      << "; a dead follower must be evicted from the in-sync set";
}

TEST_F(ReplicationTest, FollowerCatchesUpAfterRestart) {
  client::AdminClient admin(*context_);
  ASSERT_TRUE(admin.CreateTopic("catchup", 1, 3).ok());

  auto metadata = admin.GetMetadata({"catchup"});
  ASSERT_TRUE(metadata.ok());
  const std::int32_t leader_id = metadata->topics[0].partitions[0].leader.value();
  std::size_t follower = 0;
  for (std::size_t i = 0; i < 3; ++i) {
    if (static_cast<std::int32_t>(i) != leader_id) {
      follower = i;
      break;
    }
  }

  client::Producer producer(*context_);
  ASSERT_TRUE(ProduceRecords(producer, "catchup", PartitionIndex{0}, 50).ok());
  ASSERT_TRUE(
      WaitUntil([&] { return LocalLogEnd(cluster_->broker(follower), "catchup", 0) == 50; }));

  // Take the follower down, write while it is away, then bring it back.
  cluster_->StopBroker(follower);
  ASSERT_TRUE(ProduceRecords(producer, "catchup", PartitionIndex{0}, 150, "while-down").ok());
  ASSERT_TRUE(cluster_->StartBroker(follower).ok());

  ASSERT_TRUE(
      WaitUntil([&] { return LocalLogEnd(cluster_->broker(follower), "catchup", 0) == 200; },
                std::chrono::seconds(30)))
      << "follower stalled at " << LocalLogEnd(cluster_->broker(follower), "catchup", 0)
      << " of 200";

  // And the catch-up must produce byte-identical content, not just the count.
  auto* leader_replica = cluster_->broker(static_cast<std::size_t>(leader_id))
                             ->partitions()
                             .Find(TopicPartition{"catchup", PartitionIndex{0}});
  auto* follower_replica =
      cluster_->broker(follower)->partitions().Find(TopicPartition{"catchup", PartitionIndex{0}});
  ASSERT_NE(leader_replica, nullptr);
  ASSERT_NE(follower_replica, nullptr);

  ByteBuffer leader_bytes;
  ByteBuffer follower_bytes;
  ASSERT_TRUE(leader_replica->log().Read(0, 1 << 22, leader_bytes).ok());
  ASSERT_TRUE(follower_replica->log().Read(0, 1 << 22, follower_bytes).ok());
  EXPECT_EQ(AsStringView(leader_bytes.Readable()), AsStringView(follower_bytes.Readable()));
}

TEST_F(ReplicationTest, LeaderRestartPreservesReplicatedData) {
  client::AdminClient admin(*context_);
  ASSERT_TRUE(admin.CreateTopic("leader-restart", 1, 3).ok());

  auto metadata = admin.GetMetadata({"leader-restart"});
  ASSERT_TRUE(metadata.ok());
  const auto leader_id = static_cast<std::size_t>(metadata->topics[0].partitions[0].leader.value());

  {
    client::ProducerConfig config;
    config.acks = AckMode::kQuorum;
    client::Producer producer(*context_, config);
    ASSERT_TRUE(ProduceRecords(producer, "leader-restart", PartitionIndex{0}, 120).ok());
  }

  context_.reset();
  ASSERT_TRUE(cluster_->RestartBroker(leader_id).ok());
  context_ = cluster_->MakeClient();

  // Leadership is static, so the same broker leads again after restart. What
  // must hold is that no acknowledged record was lost.
  EXPECT_EQ(LocalLogEnd(cluster_->broker(leader_id), "leader-restart", 0), 120);

  client::Consumer consumer(*context_);
  auto values = ConsumeAll(consumer, "leader-restart", PartitionIndex{0}, 0, 120);
  ASSERT_TRUE(values.ok()) << values.status().ToString();
  EXPECT_EQ(values->size(), 120U);

  // And writes resume from where they stopped.
  client::Producer producer(*context_);
  auto last = ProduceRecords(producer, "leader-restart", PartitionIndex{0}, 10, "post");
  ASSERT_TRUE(last.ok()) << last.status().ToString();
  EXPECT_EQ(last.value(), 129);
}

TEST_F(ReplicationTest, DuplicateReplicationIsIdempotent) {
  // A retried batch must not be appended twice. The follower rejects any batch
  // whose base offset does not equal its log end offset, so a resend is a
  // no-op -- this asserts that end to end by producing while the network path
  // is exercised repeatedly.
  client::AdminClient admin(*context_);
  ASSERT_TRUE(admin.CreateTopic("idempotent", 1, 3).ok());

  client::Producer producer(*context_);
  ASSERT_TRUE(ProduceRecords(producer, "idempotent", PartitionIndex{0}, 100).ok());
  ASSERT_TRUE(WaitUntil([&] {
    for (std::size_t i = 0; i < 3; ++i) {
      if (LocalLogEnd(cluster_->broker(i), "idempotent", 0) != 100) return false;
    }
    return true;
  }));

  // Let replication run well past the point where everything is delivered.
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  for (std::size_t i = 0; i < 3; ++i) {
    EXPECT_EQ(LocalLogEnd(cluster_->broker(i), "idempotent", 0), 100)
        << "broker " << i << " appended duplicates";
  }
}

TEST_F(ReplicationTest, QuorumFailsRatherThanDegradingWhenReplicasAreDown) {
  client::AdminClient admin(*context_);
  ASSERT_TRUE(admin.CreateTopic("strict", 1, 3).ok());

  auto metadata = admin.GetMetadata({"strict"});
  ASSERT_TRUE(metadata.ok());
  const auto leader_id = static_cast<std::size_t>(metadata->topics[0].partitions[0].leader.value());

  // Take both followers down: a quorum of 3 needs 2, and only the leader is
  // left.
  for (std::size_t i = 0; i < 3; ++i) {
    if (i != leader_id) cluster_->StopBroker(i);
  }

  auto* replica =
      cluster_->broker(leader_id)->partitions().Find(TopicPartition{"strict", PartitionIndex{0}});
  ASSERT_NE(replica, nullptr);
  // Wait for the leader to notice both followers are gone.
  ASSERT_TRUE(
      WaitUntil([&] { return replica->GetStats().in_sync_replicas < 2; }, std::chrono::seconds(20)))
      << "leader still believes " << replica->GetStats().in_sync_replicas
      << " replicas are in sync";

  client::ClientConfig strict_config;
  strict_config.bootstrap_servers = {cluster_->BootstrapFor(leader_id)};
  strict_config.retry.max_attempts = 1;  // Observe the refusal, do not retry it.
  strict_config.request_timeout_ms = 3000;
  client::ClientContext direct(strict_config);

  client::ProducerConfig producer_config;
  producer_config.acks = AckMode::kQuorum;
  producer_config.request_timeout_ms = 2000;
  client::Producer producer(direct, producer_config);

  client::OutboundRecord record;
  record.value = "needs a quorum";
  auto result = producer.Send("strict", record);
  ASSERT_FALSE(result.ok()) << "quorum must not be acknowledged by the leader alone";
  EXPECT_TRUE(result.status().code() == ErrorCode::kNotEnoughReplicas ||
              result.status().code() == ErrorCode::kTimeout)
      << "expected an explicit refusal, got " << result.status().ToString();

  // Leader acks must still work: they promise less, and can still deliver it.
  client::ProducerConfig relaxed;
  relaxed.acks = AckMode::kLeader;
  client::Producer leader_producer(direct, relaxed);
  auto leader_result = leader_producer.Send("strict", record);
  EXPECT_TRUE(leader_result.ok()) << leader_result.status().ToString();
}

TEST_F(ReplicationTest, ProduceToANonLeaderIsRedirectedNotSilentlyAccepted) {
  client::AdminClient admin(*context_);
  ASSERT_TRUE(admin.CreateTopic("routing", 3, 3).ok());

  auto metadata = admin.GetMetadata({"routing"});
  ASSERT_TRUE(metadata.ok());

  // Find a partition whose leader is not broker 0, then talk only to broker 0.
  std::int32_t partition = -1;
  for (const auto& meta : metadata->topics[0].partitions) {
    if (meta.leader.value() != 0) {
      partition = meta.index.value();
      break;
    }
  }
  ASSERT_GE(partition, 0) << "expected at least one partition not led by broker 0";

  // A raw client pinned to broker 0 with no metadata routing.
  net::SyncClient raw;
  ASSERT_TRUE(raw.Connect(net::Endpoint::Parse(cluster_->BootstrapFor(0)).value()).ok());

  ByteBuffer records;
  protocol::AppendRecord(records, 0, 0, 0, true, ByteSpan{}, AsBytes("misrouted"));

  protocol::ProduceRequest request;
  request.topic = "routing";
  request.partition = PartitionIndex{partition};
  request.acks = AckMode::kLeader;
  request.record_count = 1;
  request.records = records.Readable();

  ByteBuffer payload;
  protocol::PayloadWriter writer(payload);
  request.Encode(writer);

  auto frame = raw.Call(protocol::OpCode::kProduce, 1, payload.Readable());
  ASSERT_TRUE(frame.ok()) << frame.status().ToString();

  protocol::ResponseHeader header;
  protocol::PayloadReader reader(frame->payload);
  ASSERT_TRUE(header.Decode(reader));
  EXPECT_EQ(header.error, ErrorCode::kNotLeader)
      << "a non-leader must refuse the write, not accept it into a divergent log";
  EXPECT_NE(header.error_message.find("leads"), std::string::npos)
      << "the error should name the actual leader: " << header.error_message;
}

}  // namespace
}  // namespace pulselog
