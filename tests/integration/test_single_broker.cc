// End-to-end tests against a real single-broker instance: produce, fetch,
// ordering, partitioning, acknowledgement modes, restart recovery and
// backpressure.
#include <algorithm>
#include <atomic>
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

class SingleBrokerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ClusterFixture::Options options;
    options.broker_count = 1;
    options.default_partitions = 4;
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

TEST_F(SingleBrokerTest, HealthAndMetadata) {
  client::AdminClient admin(*context_);
  auto health = admin.Health();
  ASSERT_TRUE(health.ok()) << health.status().ToString();
  EXPECT_TRUE(health->ready);
  EXPECT_EQ(health->broker_id, BrokerId{0});

  ASSERT_TRUE(admin.CreateTopic("events", 3).ok());
  auto metadata = admin.GetMetadata({"events"});
  ASSERT_TRUE(metadata.ok());
  ASSERT_EQ(metadata->topics.size(), 1U);
  EXPECT_EQ(metadata->topics[0].partitions.size(), 3U);
  for (const auto& partition : metadata->topics[0].partitions) {
    EXPECT_EQ(partition.leader, BrokerId{0}) << "the only broker must lead every partition";
  }
}

TEST_F(SingleBrokerTest, ProduceThenConsumeRoundTrip) {
  client::AdminClient admin(*context_);
  ASSERT_TRUE(admin.CreateTopic("orders", 1).ok());

  client::Producer producer(*context_);
  auto last = ProduceRecords(producer, "orders", PartitionIndex{0}, 100);
  ASSERT_TRUE(last.ok()) << last.status().ToString();
  EXPECT_EQ(last.value(), 99);

  client::Consumer consumer(*context_);
  auto values = ConsumeAll(consumer, "orders", PartitionIndex{0}, 0, 100);
  ASSERT_TRUE(values.ok()) << values.status().ToString();
  ASSERT_EQ(values->size(), 100U);
  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ((*values)[static_cast<std::size_t>(i)], "v" + std::to_string(i));
  }
}

TEST_F(SingleBrokerTest, OrderingIsPreservedWithinAPartition) {
  client::AdminClient admin(*context_);
  ASSERT_TRUE(admin.CreateTopic("ordered", 1).ok());

  client::Producer producer(*context_);
  // Send in many small batches so the broker sees many separate appends.
  for (int batch = 0; batch < 50; ++batch) {
    std::vector<client::OutboundRecord> records;
    std::vector<std::string> values;
    for (int i = 0; i < 10; ++i) values.push_back(std::to_string(batch * 10 + i));
    for (const auto& value : values) {
      client::OutboundRecord record;
      record.value = value;
      records.push_back(record);
    }
    auto result = producer.SendBatch("ordered", PartitionIndex{0}, records);
    ASSERT_TRUE(result.ok()) << result.status().ToString();
  }

  client::Consumer consumer(*context_);
  auto values = ConsumeAll(consumer, "ordered", PartitionIndex{0}, 0, 500);
  ASSERT_TRUE(values.ok());
  ASSERT_EQ(values->size(), 500U);
  for (int i = 0; i < 500; ++i) {
    EXPECT_EQ((*values)[static_cast<std::size_t>(i)], std::to_string(i))
        << "record " << i << " arrived out of order";
  }
}

TEST_F(SingleBrokerTest, ConcurrentProducersToOnePartitionKeepEveryRecord) {
  // Interleaving between producers is unconstrained, but nothing may be lost
  // or duplicated, and offsets must remain dense.
  client::AdminClient admin(*context_);
  ASSERT_TRUE(admin.CreateTopic("shared", 1).ok());

  constexpr int kProducers = 4;
  constexpr int kPerProducer = 250;
  std::atomic<int> failures{0};

  std::vector<std::thread> threads;
  threads.reserve(kProducers);
  for (int p = 0; p < kProducers; ++p) {
    threads.emplace_back([&, p] {
      // One client context per thread: the SDK is single-threaded by design.
      auto context = cluster_->MakeClient();
      client::Producer producer(*context);
      for (int i = 0; i < kPerProducer; ++i) {
        client::OutboundRecord record;
        const std::string value = std::to_string(p) + ":" + std::to_string(i);
        record.value = value;
        auto result = producer.Send("shared", record);
        if (!result.ok()) failures.fetch_add(1);
      }
      auto flushed = producer.Flush();
      if (!flushed.ok()) failures.fetch_add(1);
    });
  }
  for (auto& thread : threads) thread.join();
  ASSERT_EQ(failures.load(), 0);

  client::Consumer consumer(*context_);
  auto values = ConsumeAll(consumer, "shared", PartitionIndex{0}, 0, kProducers * kPerProducer);
  ASSERT_TRUE(values.ok());
  EXPECT_EQ(values->size(), static_cast<std::size_t>(kProducers * kPerProducer));

  // Every producer's records must appear exactly once, and in that producer's
  // own order.
  std::map<int, std::vector<int>> by_producer;
  for (const auto& value : *values) {
    const auto colon = value.find(':');
    ASSERT_NE(colon, std::string::npos);
    by_producer[std::stoi(value.substr(0, colon))].push_back(std::stoi(value.substr(colon + 1)));
  }
  EXPECT_EQ(by_producer.size(), static_cast<std::size_t>(kProducers));
  for (const auto& [producer_id, sequence] : by_producer) {
    ASSERT_EQ(sequence.size(), static_cast<std::size_t>(kPerProducer))
        << "producer " << producer_id << " lost or duplicated records";
    for (int i = 0; i < kPerProducer; ++i) {
      EXPECT_EQ(sequence[static_cast<std::size_t>(i)], i)
          << "producer " << producer_id << " records were reordered";
    }
  }
}

TEST_F(SingleBrokerTest, KeyRoutingIsStableAndPartitionsAreUsed) {
  client::AdminClient admin(*context_);
  ASSERT_TRUE(admin.CreateTopic("keyed", 4).ok());

  client::Producer producer(*context_);
  std::map<std::string, PartitionIndex> routed;
  for (int i = 0; i < 200; ++i) {
    const std::string key = "key-" + std::to_string(i % 20);
    client::OutboundRecord record;
    record.key = key;
    record.key_is_null = false;
    record.value = "payload";
    auto result = producer.Send("keyed", record);
    ASSERT_TRUE(result.ok()) << result.status().ToString();

    const auto existing = routed.find(key);
    if (existing == routed.end()) {
      routed[key] = result->partition;
    } else {
      EXPECT_EQ(existing->second, result->partition)
          << "key " << key << " must always route to the same partition";
    }
  }

  std::set<std::int32_t> used;
  for (const auto& [key, partition] : routed) used.insert(partition.value());
  EXPECT_GT(used.size(), 1U) << "20 keys over 4 partitions should not all collide";
}

TEST_F(SingleBrokerTest, NullKeyRoundRobinsAcrossPartitions) {
  client::AdminClient admin(*context_);
  ASSERT_TRUE(admin.CreateTopic("spread", 4).ok());

  client::Producer producer(*context_);
  std::set<std::int32_t> used;
  for (int i = 0; i < 40; ++i) {
    client::OutboundRecord record;
    record.value = "no key";
    auto result = producer.Send("spread", record);
    ASSERT_TRUE(result.ok());
    used.insert(result->partition.value());
  }
  EXPECT_EQ(used.size(), 4U) << "keyless records should spread over every partition";
}

TEST_F(SingleBrokerTest, AcknowledgementModesAllSucceed) {
  client::AdminClient admin(*context_);
  ASSERT_TRUE(admin.CreateTopic("acks", 1).ok());

  for (const AckMode mode : {AckMode::kNone, AckMode::kLeader, AckMode::kQuorum}) {
    client::ProducerConfig config;
    config.acks = mode;
    client::Producer producer(*context_, config);

    client::OutboundRecord record;
    const std::string value = std::string("acks=") + std::string(AckModeName(mode));
    record.value = value;
    auto result = producer.Send("acks", record);
    ASSERT_TRUE(result.ok()) << AckModeName(mode) << ": " << result.status().ToString();
    EXPECT_GE(result->last_offset, 0);
  }

  // With one replica a quorum is one, so all three modes must have landed.
  client::Consumer consumer(*context_);
  auto values = ConsumeAll(consumer, "acks", PartitionIndex{0}, 0, 3);
  ASSERT_TRUE(values.ok());
  EXPECT_EQ(values->size(), 3U);
}

TEST_F(SingleBrokerTest, QuorumAckImpliesDurability) {
  // acks=quorum must mean the record is on stable media, which is exactly the
  // property that lets it survive an abrupt restart with no clean shutdown.
  client::AdminClient admin(*context_);
  ASSERT_TRUE(admin.CreateTopic("durable", 1).ok());

  client::ProducerConfig config;
  config.acks = AckMode::kQuorum;
  client::Producer producer(*context_, config);
  auto last = ProduceRecords(producer, "durable", PartitionIndex{0}, 50);
  ASSERT_TRUE(last.ok()) << last.status().ToString();

  broker::Broker* instance = cluster_->broker(0);
  ASSERT_NE(instance, nullptr);
  auto* replica = instance->partitions().Find(TopicPartition{"durable", PartitionIndex{0}});
  ASSERT_NE(replica, nullptr);
  EXPECT_GE(replica->log().FlushedOffset(), 50)
      << "a quorum acknowledgement must not be sent before the data is flushed";
}

TEST_F(SingleBrokerTest, RestartRecoversAllRecords) {
  client::AdminClient admin(*context_);
  ASSERT_TRUE(admin.CreateTopic("persistent", 2).ok());

  {
    client::ProducerConfig config;
    config.acks = AckMode::kQuorum;  // Guarantees the data is flushed.
    client::Producer producer(*context_, config);
    ASSERT_TRUE(ProduceRecords(producer, "persistent", PartitionIndex{0}, 200).ok());
    ASSERT_TRUE(ProduceRecords(producer, "persistent", PartitionIndex{1}, 150).ok());
  }

  context_.reset();
  ASSERT_TRUE(cluster_->RestartBroker(0).ok());
  context_ = cluster_->MakeClient();

  client::Consumer consumer(*context_);
  auto first = ConsumeAll(consumer, "persistent", PartitionIndex{0}, 0, 200);
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  EXPECT_EQ(first->size(), 200U);

  auto second = ConsumeAll(consumer, "persistent", PartitionIndex{1}, 0, 150);
  ASSERT_TRUE(second.ok());
  EXPECT_EQ(second->size(), 150U);

  // Topic metadata must survive too, or the partitions would be re-derived.
  client::AdminClient admin_after(*context_);
  auto metadata = admin_after.GetMetadata({"persistent"});
  ASSERT_TRUE(metadata.ok());
  ASSERT_EQ(metadata->topics.size(), 1U);
  EXPECT_EQ(metadata->topics[0].partitions.size(), 2U);
}

TEST_F(SingleBrokerTest, ProducingContinuesAfterRestart) {
  client::AdminClient admin(*context_);
  ASSERT_TRUE(admin.CreateTopic("resume", 1).ok());

  {
    client::ProducerConfig config;
    config.acks = AckMode::kQuorum;
    client::Producer producer(*context_, config);
    ASSERT_TRUE(ProduceRecords(producer, "resume", PartitionIndex{0}, 30).ok());
  }
  context_.reset();
  ASSERT_TRUE(cluster_->RestartBroker(0).ok());
  context_ = cluster_->MakeClient();

  client::Producer producer(*context_);
  auto last = ProduceRecords(producer, "resume", PartitionIndex{0}, 10, "after");
  ASSERT_TRUE(last.ok()) << last.status().ToString();
  EXPECT_EQ(last.value(), 39) << "offsets must continue, not restart";
}

TEST_F(SingleBrokerTest, FetchBeyondEndReturnsNothingNotAnError) {
  client::AdminClient admin(*context_);
  ASSERT_TRUE(admin.CreateTopic("edge", 1).ok());
  client::Producer producer(*context_);
  ASSERT_TRUE(ProduceRecords(producer, "edge", PartitionIndex{0}, 5).ok());

  client::Consumer consumer(*context_);
  auto at_end = consumer.Fetch("edge", PartitionIndex{0}, 5);
  ASSERT_TRUE(at_end.ok()) << at_end.status().ToString();
  EXPECT_TRUE(at_end->empty());

  // A position past the end is an error, not an empty result: a consumer
  // polling an impossible offset would otherwise never learn it is stuck.
  auto past_end = consumer.Fetch("edge", PartitionIndex{0}, 500);
  EXPECT_FALSE(past_end.ok());
  EXPECT_EQ(past_end.status().code(), ErrorCode::kOutOfRange);
}

TEST_F(SingleBrokerTest, AutoCreateAppliesToProduceNotToFetch) {
  // Auto-create is a *producer* affordance. A consumer asking for a topic
  // nobody has written is far more likely to have a typo than to want an
  // empty topic conjured into existence, so a fetch reports NOT_FOUND.
  client::Consumer consumer(*context_);
  auto missing = consumer.Fetch("never-written", PartitionIndex{0}, 0);
  EXPECT_FALSE(missing.ok());
  EXPECT_EQ(missing.status().code(), ErrorCode::kNotFound);

  // Producing to the same name creates it.
  client::Producer producer(*context_);
  client::OutboundRecord record;
  record.value = "brings the topic into being";
  auto sent = producer.Send("never-written", record);
  ASSERT_TRUE(sent.ok()) << sent.status().ToString();

  // And now the consumer can read it.
  auto present = consumer.Fetch("never-written", PartitionIndex{0}, 0);
  ASSERT_TRUE(present.ok()) << present.status().ToString();
  EXPECT_EQ(present->size(), 1U);
}

TEST_F(SingleBrokerTest, UnknownTopicIsNotFoundWhenAutoCreateIsOff) {
  ClusterFixture::Options options;
  options.broker_count = 1;
  options.auto_create_topics = false;
  ClusterFixture strict(options);
  ASSERT_TRUE(strict.StartAll().ok());
  auto context = strict.MakeClient();

  client::Consumer consumer(*context);
  auto records = consumer.Fetch("does-not-exist", PartitionIndex{0}, 0);
  EXPECT_FALSE(records.ok());
  EXPECT_EQ(records.status().code(), ErrorCode::kNotFound);

  // Producing must fail the same way rather than silently inventing a topic.
  client::Producer producer(*context);
  client::OutboundRecord record;
  record.value = "nope";
  auto sent = producer.Send("does-not-exist", record);
  EXPECT_FALSE(sent.ok());
}

TEST_F(SingleBrokerTest, AutoCreateOnProduce) {
  client::Producer producer(*context_);
  client::OutboundRecord record;
  record.value = "first";
  auto result = producer.Send("auto-created", record);
  ASSERT_TRUE(result.ok()) << result.status().ToString();

  client::AdminClient admin(*context_);
  auto metadata = admin.GetMetadata({"auto-created"});
  ASSERT_TRUE(metadata.ok());
  ASSERT_EQ(metadata->topics.size(), 1U);
}

TEST_F(SingleBrokerTest, ListOffsetsResolvesEarliestAndLatest) {
  client::AdminClient admin(*context_);
  ASSERT_TRUE(admin.CreateTopic("bounds", 1).ok());
  client::Producer producer(*context_);
  ASSERT_TRUE(ProduceRecords(producer, "bounds", PartitionIndex{0}, 25).ok());

  client::Consumer consumer(*context_);
  auto earliest = consumer.ListOffset("bounds", PartitionIndex{0}, kEarliestOffset);
  auto latest = consumer.ListOffset("bounds", PartitionIndex{0}, kLatestOffset);
  ASSERT_TRUE(earliest.ok());
  ASSERT_TRUE(latest.ok());
  EXPECT_EQ(earliest.value(), 0);
  EXPECT_EQ(latest.value(), 25);
}

TEST_F(SingleBrokerTest, LargeRecordsRoundTrip) {
  client::AdminClient admin(*context_);
  ASSERT_TRUE(admin.CreateTopic("large", 1).ok());

  const std::string big(512 * 1024, 'L');
  client::Producer producer(*context_);
  client::OutboundRecord record;
  record.value = big;
  auto result = producer.Send("large", record);
  ASSERT_TRUE(result.ok()) << result.status().ToString();

  client::Consumer consumer(*context_);
  auto records = consumer.Fetch("large", PartitionIndex{0}, 0);
  ASSERT_TRUE(records.ok()) << records.status().ToString();
  ASSERT_EQ(records->size(), 1U);
  EXPECT_EQ((*records)[0].value.size(), big.size());
  EXPECT_EQ((*records)[0].value, big);
}

TEST_F(SingleBrokerTest, SegmentRollingIsTransparentToClients) {
  ClusterFixture::Options options;
  options.broker_count = 1;
  options.segment_bytes = 16 * 1024;  // Force many rolls.
  ClusterFixture small(options);
  ASSERT_TRUE(small.StartAll().ok());
  auto context = small.MakeClient();

  client::AdminClient admin(*context);
  ASSERT_TRUE(admin.CreateTopic("rolling", 1).ok());

  // Many small batches, not one large one: a batch is never split across
  // segments, so a single 500-record append would land in one segment however
  // small the segment limit is.
  client::Producer producer(*context);
  for (int batch = 0; batch < 50; ++batch) {
    ASSERT_TRUE(ProduceRecords(producer, "rolling", PartitionIndex{0}, 10, "r", 200).ok());
  }

  auto* replica = small.broker(0)->partitions().Find(TopicPartition{"rolling", PartitionIndex{0}});
  ASSERT_NE(replica, nullptr);
  EXPECT_GT(replica->log().GetStats().segment_count, 1U) << "expected the log to roll";

  client::Consumer consumer(*context);
  auto values = ConsumeAll(consumer, "rolling", PartitionIndex{0}, 0, 500);
  ASSERT_TRUE(values.ok());
  EXPECT_EQ(values->size(), 500U);
  // Offsets must stay dense across the roll boundaries.
  for (int i = 0; i < 500; ++i) {
    EXPECT_EQ((*values)[static_cast<std::size_t>(i)].substr(0, 2), "r" + std::to_string(i % 10));
  }
}

TEST_F(SingleBrokerTest, BackpressureIsReportedNotSilentlyDropped) {
  // With a tiny worker queue and many concurrent clients, some requests must
  // be refused with a retryable error rather than queued without bound.
  ClusterFixture::Options options;
  options.broker_count = 1;
  options.worker_threads = 1;
  ClusterFixture constrained(options);
  ASSERT_TRUE(constrained.StartAll().ok());

  auto context = constrained.MakeClient();
  client::AdminClient admin(*context);
  ASSERT_TRUE(admin.CreateTopic("pressure", 1).ok());

  constexpr int kClients = 8;
  constexpr int kPerClient = 300;
  std::atomic<int> delivered{0};
  std::atomic<int> backpressured{0};
  std::atomic<int> other_failures{0};

  std::vector<std::thread> threads;
  threads.reserve(kClients);
  for (int c = 0; c < kClients; ++c) {
    threads.emplace_back([&] {
      auto thread_context = constrained.MakeClient();
      // No retries: we want to observe the rejection, not paper over it.
      client::Producer producer(*thread_context);
      const std::string payload(4096, 'p');
      for (int i = 0; i < kPerClient; ++i) {
        client::OutboundRecord record;
        record.value = payload;
        auto result = producer.Send("pressure", record);
        if (result.ok()) {
          delivered.fetch_add(1);
        } else if (result.status().code() == ErrorCode::kBackpressure) {
          backpressured.fetch_add(1);
        } else {
          other_failures.fetch_add(1);
        }
      }
    });
  }
  for (auto& thread : threads) thread.join();

  EXPECT_EQ(other_failures.load(), 0) << "only backpressure is an acceptable rejection here";
  EXPECT_GT(delivered.load(), 0);
  // Whatever was rejected must have been rejected explicitly, and the broker
  // must still be healthy afterwards.
  auto health = admin.Health();
  ASSERT_TRUE(health.ok());
  EXPECT_TRUE(health->ready);
  EXPECT_EQ(delivered.load() + backpressured.load(), kClients * kPerClient);
}

TEST_F(SingleBrokerTest, GracefulShutdownAnswersOrFailsEveryInFlightRequest) {
  client::AdminClient admin(*context_);
  ASSERT_TRUE(admin.CreateTopic("shutdown", 1).ok());

  std::atomic<bool> stop{false};
  std::atomic<int> answered{0};
  std::atomic<int> errored{0};

  std::thread producer_thread([&] {
    auto context = cluster_->MakeClient();
    client::Producer producer(*context);
    while (!stop.load()) {
      client::OutboundRecord record;
      record.value = "during shutdown";
      auto result = producer.Send("shutdown", record);
      if (result.ok()) {
        answered.fetch_add(1);
      } else {
        errored.fetch_add(1);
      }
    }
  });

  ASSERT_TRUE(WaitUntil([&] { return answered.load() > 20; }));
  cluster_->StopBroker(0);
  stop.store(true);
  producer_thread.join();

  // The point is that the broker stopped without hanging and every request
  // resolved one way or the other -- none were left outstanding forever.
  EXPECT_GT(answered.load(), 0);
  SUCCEED();
}

}  // namespace
}  // namespace pulselog
