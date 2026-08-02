// Micro-benchmarks for the pieces on the hot path.
//
// These exist to justify specific implementation choices with numbers rather
// than intuition. Each one is paired with a claim made somewhere in the source
// or the docs; if the benchmark stops supporting the claim, the claim changes.
#include <cstring>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <benchmark/benchmark.h>

#include "pulselog/base/buffer.h"
#include "pulselog/base/crc32c.h"
#include "pulselog/concurrency/blocking_queue.h"
#include "pulselog/concurrency/mpmc_queue.h"
#include "pulselog/concurrency/spsc_ring.h"
#include "pulselog/protocol/frame.h"
#include "pulselog/protocol/record.h"

namespace {

using namespace pulselog;

std::vector<std::uint8_t> RandomBytes(std::size_t size, unsigned seed = 1234) {
  std::mt19937 rng(seed);
  std::vector<std::uint8_t> data(size);
  for (auto& byte : data) byte = static_cast<std::uint8_t>(rng());
  return data;
}

// --- checksum ---------------------------------------------------------------
// Claim (base/crc32c.h): the hardware path is worth dispatching to.

void BM_Crc32cHardware(benchmark::State& state) {
  const auto data = RandomBytes(static_cast<std::size_t>(state.range(0)));
  for (auto _ : state) {
    benchmark::DoNotOptimize(Crc32c(data));
  }
  state.SetBytesProcessed(state.iterations() * state.range(0));
  state.SetLabel(std::string(Crc32cImplementationName()));
}

BENCHMARK(BM_Crc32cHardware)->Arg(64)->Arg(256)->Arg(4096)->Arg(65536);

void BM_Crc32cSoftware(benchmark::State& state) {
  const auto data = RandomBytes(static_cast<std::size_t>(state.range(0)));
  for (auto _ : state) {
    benchmark::DoNotOptimize(Crc32cSoftware(data));
  }
  state.SetBytesProcessed(state.iterations() * state.range(0));
  state.SetLabel("software-slice-by-8");
}

BENCHMARK(BM_Crc32cSoftware)->Arg(64)->Arg(256)->Arg(4096)->Arg(65536);

// --- record encoding --------------------------------------------------------
// Claim (protocol/record.h): encoding is cheap enough to do per record.

void BM_RecordEncode(benchmark::State& state) {
  const std::size_t value_size = static_cast<std::size_t>(state.range(0));
  const std::string key = "user-1234";
  const std::string value(value_size, 'v');
  ByteBuffer buffer;

  for (auto _ : state) {
    buffer.Clear();
    protocol::AppendRecord(buffer, 42, 1'700'000'000'000, 0, false, AsBytes(key), AsBytes(value));
    benchmark::DoNotOptimize(buffer.ReadPtr());
  }
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(value_size));
}

BENCHMARK(BM_RecordEncode)->Arg(16)->Arg(128)->Arg(1024)->Arg(65536);

void BM_RecordParseWithCrc(benchmark::State& state) {
  const std::string key = "user-1234";
  const std::string value(static_cast<std::size_t>(state.range(0)), 'v');
  ByteBuffer buffer;
  protocol::AppendRecord(buffer, 42, 1'700'000'000'000, 0, false, AsBytes(key), AsBytes(value));

  for (auto _ : state) {
    protocol::RecordView view;
    std::size_t next = 0;
    benchmark::DoNotOptimize(
        protocol::ParseRecord(buffer.Readable(), 0, /*verify_crc=*/true, view, next));
  }
  state.SetBytesProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_RecordParseWithCrc)->Arg(16)->Arg(128)->Arg(1024)->Arg(65536);

void BM_RecordParseNoCrc(benchmark::State& state) {
  // The difference against the previous benchmark is exactly what verifying a
  // checksum on every read would cost -- which is why fetch does not do it.
  const std::string key = "user-1234";
  const std::string value(static_cast<std::size_t>(state.range(0)), 'v');
  ByteBuffer buffer;
  protocol::AppendRecord(buffer, 42, 1'700'000'000'000, 0, false, AsBytes(key), AsBytes(value));

  for (auto _ : state) {
    protocol::RecordView view;
    std::size_t next = 0;
    benchmark::DoNotOptimize(
        protocol::ParseRecord(buffer.Readable(), 0, /*verify_crc=*/false, view, next));
  }
  state.SetBytesProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_RecordParseNoCrc)->Arg(16)->Arg(128)->Arg(1024)->Arg(65536);

// --- framing ----------------------------------------------------------------
// Claim (protocol/frame.h): the two-checksum header is not a bottleneck.

void BM_FrameEncodeDecode(benchmark::State& state) {
  const std::string payload(static_cast<std::size_t>(state.range(0)), 'p');
  ByteBuffer buffer;

  for (auto _ : state) {
    buffer.Clear();
    protocol::EncodeFrame(buffer, protocol::OpCode::kProduce, 1, 0, AsBytes(payload));
    protocol::FrameDecoder decoder;
    protocol::FrameDecoder::Frame frame;
    benchmark::DoNotOptimize(decoder.Next(buffer.Readable(), frame));
  }
  state.SetBytesProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_FrameEncodeDecode)->Arg(64)->Arg(1024)->Arg(65536);

// --- buffers ----------------------------------------------------------------
// Claim (base/buffer.h): pooling buffers beats allocating per request.

void BM_BufferAllocateFresh(benchmark::State& state) {
  const std::size_t size = static_cast<std::size_t>(state.range(0));
  for (auto _ : state) {
    auto buffer = std::make_unique<ByteBuffer>(size);
    buffer->Append(AsBytes("payload"));
    benchmark::DoNotOptimize(buffer->ReadPtr());
  }
}

BENCHMARK(BM_BufferAllocateFresh)->Arg(4096)->Arg(65536)->Arg(262144);

void BM_BufferFromPool(benchmark::State& state) {
  BufferPoolOptions options;
  options.default_capacity = static_cast<std::size_t>(state.range(0));
  BufferPool pool(options);
  // Prime the pool so the benchmark measures steady state, not first touch.
  {
    auto warm = pool.Acquire();
    pool.Release(std::move(warm));
  }

  for (auto _ : state) {
    auto buffer = pool.Acquire();
    buffer->Append(AsBytes("payload"));
    benchmark::DoNotOptimize(buffer->ReadPtr());
    pool.Release(std::move(buffer));
  }
}

BENCHMARK(BM_BufferFromPool)->Arg(4096)->Arg(65536)->Arg(262144);

// --- queues -----------------------------------------------------------------
// Claim (concurrency/*.h): the SPSC ring is wait-free and faster than the
// bounded MPMC queue, which is in turn faster than mutex + condvar for the
// single-item handoff -- but the gap closes when the mutex variant batches.

void BM_SpscRingRoundTrip(benchmark::State& state) {
  SpscRing<std::int64_t> ring(1024);
  std::int64_t value = 0;
  for (auto _ : state) {
    benchmark::DoNotOptimize(ring.TryPush(std::int64_t{1}));
    benchmark::DoNotOptimize(ring.TryPop(value));
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_SpscRingRoundTrip);

void BM_MpmcQueueRoundTrip(benchmark::State& state) {
  BoundedMpmcQueue<std::int64_t> queue(1024);
  std::int64_t value = 0;
  for (auto _ : state) {
    benchmark::DoNotOptimize(queue.TryPush(std::int64_t{1}));
    benchmark::DoNotOptimize(queue.TryPop(value));
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_MpmcQueueRoundTrip);

void BM_BlockingQueueRoundTrip(benchmark::State& state) {
  BoundedBlockingQueue<std::int64_t> queue(1024);
  for (auto _ : state) {
    benchmark::DoNotOptimize(queue.TryPush(std::int64_t{1}));
    benchmark::DoNotOptimize(queue.PopFor(std::chrono::milliseconds(1)));
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_BlockingQueueRoundTrip);

// Contended: several producers into one queue, which is the shape the io
// loops actually create when they feed a partition worker.
void BM_MpmcQueueContended(benchmark::State& state) {
  static BoundedMpmcQueue<std::int64_t>* queue = nullptr;
  if (state.thread_index() == 0) queue = new BoundedMpmcQueue<std::int64_t>(4096);

  for (auto _ : state) {
    std::int64_t value = 0;
    if (!queue->TryPush(std::int64_t{1})) {
      benchmark::DoNotOptimize(queue->TryPop(value));
    }
  }
  state.SetItemsProcessed(state.iterations());

  if (state.thread_index() == 0) {
    delete queue;
    queue = nullptr;
  }
}

BENCHMARK(BM_MpmcQueueContended)->Threads(1)->Threads(2)->Threads(4);

}  // namespace

BENCHMARK_MAIN();
