// Concurrency-primitive tests.
//
// These run under ThreadSanitizer in CI; the multi-threaded cases are sized so
// a TSan build still finishes in seconds. Correctness is checked by conserved
// quantities (every produced item is consumed exactly once, sums match) rather
// than by timing, so the tests are deterministic in outcome even though the
// interleavings are not.
#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <numeric>
#include <thread>
#include <vector>

#include "pulselog/concurrency/backoff.h"
#include "pulselog/concurrency/blocking_queue.h"
#include "pulselog/concurrency/mpmc_queue.h"
#include "pulselog/concurrency/spsc_ring.h"

namespace pulselog {
namespace {

TEST(SpscRing, CapacityIsRoundedUp) {
  SpscRing<int> ring(5);
  EXPECT_GE(ring.Capacity(), 5U);
  EXPECT_EQ(ring.Capacity() & (ring.Capacity() + 1), 0U) << "capacity+1 must be a power of two";
}

TEST(SpscRing, PushPopSingleThreaded) {
  SpscRing<int> ring(4);
  int out = 0;
  EXPECT_FALSE(ring.TryPop(out));

  EXPECT_TRUE(ring.TryPush(1));
  EXPECT_TRUE(ring.TryPush(2));
  EXPECT_EQ(ring.SizeApprox(), 2U);

  ASSERT_TRUE(ring.TryPop(out));
  EXPECT_EQ(out, 1);
  ASSERT_TRUE(ring.TryPop(out));
  EXPECT_EQ(out, 2);
  EXPECT_FALSE(ring.TryPop(out));
}

TEST(SpscRing, RejectsPushWhenFull) {
  SpscRing<int> ring(3);
  int pushed = 0;
  while (ring.TryPush(pushed)) ++pushed;
  EXPECT_EQ(static_cast<std::size_t>(pushed), ring.Capacity());

  int out = 0;
  ASSERT_TRUE(ring.TryPop(out));
  EXPECT_TRUE(ring.TryPush(99)) << "a freed slot must become usable again";
}

TEST(SpscRing, PreservesFifoOrderAcrossThreads) {
  constexpr int kItems = 200'000;
  SpscRing<int> ring(1024);

  std::thread producer([&] {
    Backoff backoff;
    for (int i = 0; i < kItems; ++i) {
      while (!ring.TryPush(i)) backoff.Pause();
      backoff.Reset();
    }
  });

  int expected = 0;
  Backoff backoff;
  while (expected < kItems) {
    int value = 0;
    if (ring.TryPop(value)) {
      ASSERT_EQ(value, expected) << "SPSC ring must preserve FIFO order";
      ++expected;
      backoff.Reset();
    } else {
      backoff.Pause();
    }
  }
  producer.join();
  EXPECT_EQ(expected, kItems);
}

TEST(SpscRing, DestroysUndrainedElements) {
  auto alive = std::make_shared<std::atomic<int>>(0);
  struct Counted {
    std::shared_ptr<std::atomic<int>> counter;

    Counted() = default;

    explicit Counted(std::shared_ptr<std::atomic<int>> c) : counter(std::move(c)) {
      if (counter) counter->fetch_add(1);
    }

    Counted(Counted&& other) noexcept : counter(std::move(other.counter)) {}

    Counted& operator=(Counted&& other) noexcept {
      if (counter) counter->fetch_sub(1);
      counter = std::move(other.counter);
      return *this;
    }

    Counted(const Counted&) = delete;
    Counted& operator=(const Counted&) = delete;

    ~Counted() {
      if (counter) counter->fetch_sub(1);
    }
  };

  {
    SpscRing<Counted> ring(8);
    for (int i = 0; i < 4; ++i) {
      ASSERT_TRUE(ring.TryPush(Counted(alive)));
    }
    EXPECT_EQ(alive->load(), 4);
  }
  EXPECT_EQ(alive->load(), 0) << "ring destructor must destroy undrained slots";
}

TEST(MpmcQueue, SingleThreadedBasics) {
  BoundedMpmcQueue<int> queue(4);
  int out = 0;
  EXPECT_FALSE(queue.TryPop(out));

  for (int i = 0; i < 4; ++i) EXPECT_TRUE(queue.TryPush(i));
  EXPECT_FALSE(queue.TryPush(99)) << "bounded queue must reject when full";

  for (int i = 0; i < 4; ++i) {
    ASSERT_TRUE(queue.TryPop(out));
    EXPECT_EQ(out, i);
  }
  EXPECT_FALSE(queue.TryPop(out));
}

TEST(MpmcQueue, ConservesItemsUnderContention) {
  constexpr int kProducers = 4;
  constexpr int kConsumers = 3;
  constexpr int kPerProducer = 20'000;

  BoundedMpmcQueue<std::int64_t> queue(512);
  std::atomic<std::int64_t> consumed_sum{0};
  std::atomic<int> consumed_count{0};
  std::atomic<bool> producers_done{false};

  std::vector<std::thread> producers;
  producers.reserve(kProducers);
  for (int p = 0; p < kProducers; ++p) {
    producers.emplace_back([&, p] {
      Backoff backoff;
      for (int i = 0; i < kPerProducer; ++i) {
        const std::int64_t value = static_cast<std::int64_t>(p) * kPerProducer + i;
        while (!queue.TryPush(value)) backoff.Pause();
        backoff.Reset();
      }
    });
  }

  std::vector<std::thread> consumers;
  consumers.reserve(kConsumers);
  for (int c = 0; c < kConsumers; ++c) {
    consumers.emplace_back([&] {
      Backoff backoff;
      for (;;) {
        std::int64_t value = 0;
        if (queue.TryPop(value)) {
          consumed_sum.fetch_add(value, std::memory_order_relaxed);
          consumed_count.fetch_add(1, std::memory_order_relaxed);
          backoff.Reset();
        } else if (producers_done.load(std::memory_order_acquire)) {
          // One more sweep: a producer may have pushed between our failed pop
          // and observing the done flag.
          if (!queue.TryPop(value)) break;
          consumed_sum.fetch_add(value, std::memory_order_relaxed);
          consumed_count.fetch_add(1, std::memory_order_relaxed);
        } else {
          backoff.Pause();
        }
      }
    });
  }

  for (auto& t : producers) t.join();
  producers_done.store(true, std::memory_order_release);
  for (auto& t : consumers) t.join();

  constexpr int kTotal = kProducers * kPerProducer;
  const std::int64_t expected_sum =
      (static_cast<std::int64_t>(kTotal) - 1) * static_cast<std::int64_t>(kTotal) / 2;
  EXPECT_EQ(consumed_count.load(), kTotal);
  EXPECT_EQ(consumed_sum.load(), expected_sum) << "no item may be lost or duplicated";
}

TEST(MpmcQueue, MoveOnlyPayload) {
  BoundedMpmcQueue<std::unique_ptr<int>> queue(4);
  ASSERT_TRUE(queue.TryPush(std::make_unique<int>(5)));
  std::unique_ptr<int> out;
  ASSERT_TRUE(queue.TryPop(out));
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(*out, 5);
}

TEST(BlockingQueue, RespectsCapacity) {
  BoundedBlockingQueue<int> queue(2);
  EXPECT_TRUE(queue.TryPush(1));
  EXPECT_TRUE(queue.TryPush(2));
  EXPECT_FALSE(queue.TryPush(3));
  EXPECT_EQ(queue.Size(), 2U);
}

TEST(BlockingQueue, PopBlocksUntilPush) {
  BoundedBlockingQueue<int> queue(4);
  std::thread producer([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_TRUE(queue.TryPush(7));
  });
  const auto value = queue.Pop();
  producer.join();
  ASSERT_TRUE(value.has_value());
  EXPECT_EQ(*value, 7);
}

TEST(BlockingQueue, CloseWakesWaitersAndDrains) {
  BoundedBlockingQueue<int> queue(4);
  EXPECT_TRUE(queue.TryPush(1));

  std::thread consumer([&] {
    EXPECT_EQ(queue.Pop().value_or(-1), 1);
    EXPECT_FALSE(queue.Pop().has_value()) << "closed and drained must return nullopt";
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  queue.Close();
  consumer.join();

  EXPECT_FALSE(queue.TryPush(2)) << "closed queue must reject pushes";
}

TEST(BlockingQueue, PopBatchDrainsMultiple) {
  BoundedBlockingQueue<int> queue(16);
  for (int i = 0; i < 5; ++i) EXPECT_TRUE(queue.TryPush(i));

  std::vector<int> drained;
  const std::size_t count = queue.PopBatch(drained, 10, std::chrono::milliseconds(50));
  EXPECT_EQ(count, 5U);
  EXPECT_EQ(drained.size(), 5U);
  EXPECT_EQ(std::accumulate(drained.begin(), drained.end(), 0), 10);
}

TEST(BlockingQueue, PopForTimesOut) {
  BoundedBlockingQueue<int> queue(4);
  const auto value = queue.PopFor(std::chrono::milliseconds(10));
  EXPECT_FALSE(value.has_value());
}

TEST(Backoff, EscalatesAndResets) {
  Backoff backoff;
  EXPECT_EQ(backoff.steps(), 0U);
  for (int i = 0; i < 5; ++i) backoff.Pause();
  EXPECT_EQ(backoff.steps(), 5U);
  backoff.Reset();
  EXPECT_EQ(backoff.steps(), 0U);
}

}  // namespace
}  // namespace pulselog
