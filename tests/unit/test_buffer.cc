#include <gtest/gtest.h>

#include <cstring>
#include <string>

#include "pulselog/base/buffer.h"

namespace pulselog {
namespace {

TEST(ByteBuffer, AppendAndConsume) {
  ByteBuffer buf;
  EXPECT_TRUE(buf.Empty());

  buf.Append(AsBytes("hello"));
  EXPECT_EQ(buf.ReadableBytes(), 5U);
  EXPECT_EQ(AsStringView(buf.Readable()), "hello");

  buf.Consume(2);
  EXPECT_EQ(AsStringView(buf.Readable()), "llo");

  buf.Consume(3);
  EXPECT_TRUE(buf.Empty());
}

TEST(ByteBuffer, CursorsResetWhenDrained) {
  ByteBuffer buf(64);
  buf.Append(AsBytes("abcdef"));
  buf.Consume(6);
  // A drained buffer must rewind so a long-lived connection buffer does not
  // creep forward and force repeated compactions.
  EXPECT_EQ(buf.ReadPtr(), buf.WritePtr());
  buf.Append(AsBytes("xyz"));
  EXPECT_EQ(AsStringView(buf.Readable()), "xyz");
}

TEST(ByteBuffer, CompactPreservesUnreadBytes) {
  ByteBuffer buf(16);
  buf.Append(AsBytes("0123456789"));
  buf.Consume(4);
  buf.Compact();
  EXPECT_EQ(AsStringView(buf.Readable()), "456789");
}

TEST(ByteBuffer, GrowsWhenCompactionIsInsufficient) {
  ByteBuffer buf(8);
  const std::string payload(100, 'x');
  buf.Append(AsBytes(payload));
  EXPECT_EQ(buf.ReadableBytes(), 100U);
  EXPECT_GE(buf.Capacity(), 100U);
  EXPECT_EQ(AsStringView(buf.Readable()), payload);
}

TEST(ByteBuffer, PartialWriteThenRead) {
  // Mirrors how the network layer fills a buffer: reserve, write into the raw
  // region, then commit only the bytes the kernel actually delivered.
  ByteBuffer buf;
  buf.EnsureWritable(1024);
  ASSERT_GE(buf.WritableBytes(), 1024U);
  std::memcpy(buf.WritePtr(), "frame", 5);
  buf.Commit(5);
  EXPECT_EQ(AsStringView(buf.Readable()), "frame");
}

TEST(ByteBuffer, ShrinkReleasesCapacity) {
  ByteBuffer buf(1024 * 1024);
  buf.Append(AsBytes("small"));
  buf.Shrink(4096);
  EXPECT_LE(buf.Capacity(), 4096U);
  EXPECT_EQ(AsStringView(buf.Readable()), "small");
}

TEST(BufferPool, ReusesBuffers) {
  BufferPool pool(BufferPool::Options{.max_pooled = 4, .default_capacity = 256});

  auto first = pool.Acquire();
  ByteBuffer* raw = first.get();
  pool.Release(std::move(first));

  auto second = pool.Acquire();
  EXPECT_EQ(second.get(), raw) << "expected the pooled buffer back";
  EXPECT_TRUE(second->Empty());

  const auto stats = pool.GetStats();
  EXPECT_EQ(stats.acquires, 2U);
  EXPECT_EQ(stats.hits, 1U);
  pool.Release(std::move(second));
}

TEST(BufferPool, StaysBounded) {
  BufferPool pool(BufferPool::Options{.max_pooled = 2, .default_capacity = 128});
  std::vector<std::unique_ptr<ByteBuffer>> buffers;
  for (int i = 0; i < 8; ++i) buffers.push_back(pool.Acquire());
  for (auto& b : buffers) pool.Release(std::move(b));

  const auto stats = pool.GetStats();
  EXPECT_EQ(stats.pooled, 2U);
  EXPECT_EQ(stats.discards, 6U);
}

TEST(BufferPool, OversizedBuffersAreShrunkNotPinned) {
  BufferPool pool(BufferPool::Options{
      .max_pooled = 4, .default_capacity = 1024, .max_retained_capacity = 8192});
  auto buf = pool.Acquire(64 * 1024);
  EXPECT_GE(buf->Capacity(), 64U * 1024);
  pool.Release(std::move(buf));

  auto again = pool.Acquire();
  EXPECT_LE(again->Capacity(), 8192U);
  pool.Release(std::move(again));
}

TEST(PooledBuffer, ReturnsOnScopeExit) {
  BufferPool pool(BufferPool::Options{.max_pooled = 2, .default_capacity = 64});
  {
    PooledBuffer handle(pool, pool.Acquire());
    ASSERT_TRUE(handle.valid());
    handle->Append(AsBytes("data"));
  }
  EXPECT_EQ(pool.GetStats().pooled, 1U);
}

TEST(PooledBuffer, MoveTransfersOwnershipExactlyOnce) {
  BufferPool pool(BufferPool::Options{.max_pooled = 2, .default_capacity = 64});
  {
    PooledBuffer a(pool, pool.Acquire());
    PooledBuffer b = std::move(a);
    EXPECT_FALSE(a.valid());  // NOLINT(bugprone-use-after-move) -- intentional.
    EXPECT_TRUE(b.valid());
  }
  EXPECT_EQ(pool.GetStats().releases, 1U);
}

}  // namespace
}  // namespace pulselog
