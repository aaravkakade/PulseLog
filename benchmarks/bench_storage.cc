// Storage-engine micro-benchmarks.
//
// The write-mode comparison here is what selects the default in
// docs/STORAGE_ENGINE.md. It runs against a real temporary directory on the
// real filesystem, because the whole point is to measure the filesystem.
#include <benchmark/benchmark.h>

#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <random>
#include <string>
#include <vector>

#include "pulselog/base/buffer.h"
#include "pulselog/protocol/record.h"
#include "pulselog/storage/file_util.h"
#include "pulselog/storage/partition_log.h"

namespace {

using namespace pulselog;

class TempDirectory {
 public:
  TempDirectory() {
    std::string templ = (std::filesystem::temp_directory_path() / "pulselog-bench-XXXXXX")
                            .string();
    const char* created = ::mkdtemp(templ.data());
    path_ = created != nullptr ? created : templ;
  }

  TempDirectory(const TempDirectory&) = delete;
  TempDirectory& operator=(const TempDirectory&) = delete;

  ~TempDirectory() {
    std::error_code ec;
    std::filesystem::remove_all(path_, ec);
  }

  [[nodiscard]] const std::filesystem::path& path() const { return path_; }

 private:
  std::filesystem::path path_;
};

storage::LogOptions MakeOptions(const std::filesystem::path& dir, storage::WriteMode mode,
                                bool sync_on_append) {
  storage::LogOptions options;
  options.directory = dir;
  options.segment_bytes = 256LL * 1024 * 1024;
  options.index_interval_bytes = 4096;
  options.preallocate = true;
  options.write_mode = mode;
  options.flush.sync_on_append = sync_on_append;
  options.flush.interval_ms = sync_on_append ? 0 : 1000;
  options.flush.max_unflushed_bytes = sync_on_append ? 0 : (64 << 20);
  options.flush.max_unflushed_records = sync_on_append ? 0 : 1'000'000;
  return options;
}

// Builds a batch of records with offset 0, exactly as a produce request has.
ByteBuffer MakeBatch(int count, std::size_t value_size) {
  ByteBuffer buffer;
  const std::string key = "k";
  const std::string value(value_size, 'v');
  for (int i = 0; i < count; ++i) {
    protocol::AppendRecord(buffer, 0, 1'700'000'000'000, 0, false, AsBytes(key),
                           AsBytes(value));
  }
  return buffer;
}

MutableByteSpan Mutable(ByteBuffer& buffer) {
  return {const_cast<std::uint8_t*>(buffer.ReadPtr()), buffer.ReadableBytes()};
}

// --- append throughput by write mode ---------------------------------------

void BM_LogAppend(benchmark::State& state) {
  const int batch = static_cast<int>(state.range(0));
  const std::size_t value_size = static_cast<std::size_t>(state.range(1));
  const auto mode = static_cast<storage::WriteMode>(state.range(2));

  TempDirectory dir;
  auto log = storage::PartitionLog::Open(TopicPartition{"bench", PartitionIndex{0}},
                                         MakeOptions(dir.path(), mode, false));
  if (!log.ok()) {
    state.SkipWithError(log.status().ToString().c_str());
    return;
  }

  ByteBuffer batch_bytes = MakeBatch(batch, value_size);
  const std::size_t batch_size = batch_bytes.ReadableBytes();

  for (auto _ : state) {
    // The append rewrites offsets in place, so the batch is rebuilt each time
    // to keep every iteration identical.
    state.PauseTiming();
    batch_bytes = MakeBatch(batch, value_size);
    state.ResumeTiming();

    auto result = log.value()->AppendAssigningOffsets(Mutable(batch_bytes),
                                                      static_cast<std::uint32_t>(batch));
    benchmark::DoNotOptimize(result.ok());
  }

  state.SetItemsProcessed(state.iterations() * batch);
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(batch_size));
  state.SetLabel(std::string(storage::WriteModeName(mode)));
}

// batch size, value size, write mode.
BENCHMARK(BM_LogAppend)
    ->Args({1, 128, static_cast<int>(storage::WriteMode::kWrite)})
    ->Args({100, 128, static_cast<int>(storage::WriteMode::kWrite)})
    ->Args({1000, 128, static_cast<int>(storage::WriteMode::kWrite)})
    ->Args({100, 16, static_cast<int>(storage::WriteMode::kWrite)})
    ->Args({100, 4096, static_cast<int>(storage::WriteMode::kWrite)})
    ->Unit(benchmark::kMicrosecond);

// --- the cost of durability -------------------------------------------------
// Claim (STORAGE_ENGINE.md): sync_on_append trades throughput for a real
// stable-media guarantee, and the size of that trade is device-dependent.

void BM_LogAppendSynchronous(benchmark::State& state) {
  const int batch = static_cast<int>(state.range(0));
  TempDirectory dir;
  auto log = storage::PartitionLog::Open(TopicPartition{"bench", PartitionIndex{0}},
                                         MakeOptions(dir.path(), storage::WriteMode::kWrite,
                                                     /*sync_on_append=*/true));
  if (!log.ok()) {
    state.SkipWithError(log.status().ToString().c_str());
    return;
  }

  for (auto _ : state) {
    state.PauseTiming();
    ByteBuffer batch_bytes = MakeBatch(batch, 128);
    state.ResumeTiming();
    auto result = log.value()->AppendAssigningOffsets(Mutable(batch_bytes),
                                                      static_cast<std::uint32_t>(batch));
    benchmark::DoNotOptimize(result.ok());
  }
  state.SetItemsProcessed(state.iterations() * batch);
  state.SetLabel("fsync per append (F_FULLFSYNC on macOS)");
}
BENCHMARK(BM_LogAppendSynchronous)->Arg(1)->Arg(100)->Unit(benchmark::kMicrosecond);

// --- read path --------------------------------------------------------------

void BM_LogRead(benchmark::State& state) {
  const std::size_t max_bytes = static_cast<std::size_t>(state.range(0));

  TempDirectory dir;
  auto log = storage::PartitionLog::Open(TopicPartition{"bench", PartitionIndex{0}},
                                         MakeOptions(dir.path(), storage::WriteMode::kWrite,
                                                     false));
  if (!log.ok()) {
    state.SkipWithError(log.status().ToString().c_str());
    return;
  }
  for (int i = 0; i < 200; ++i) {
    ByteBuffer batch_bytes = MakeBatch(500, 128);
    auto appended = log.value()->AppendAssigningOffsets(Mutable(batch_bytes), 500);
    if (!appended.ok()) {
      state.SkipWithError(appended.status().ToString().c_str());
      return;
    }
  }

  ByteBuffer out;
  Offset cursor = 0;
  const Offset end = log.value()->LogEndOffset();
  for (auto _ : state) {
    out.Clear();
    auto read = log.value()->Read(cursor, max_bytes, out);
    if (!read.ok() || read->record_count == 0) {
      cursor = 0;
      continue;
    }
    cursor += read->record_count;
    if (cursor >= end) cursor = 0;
    benchmark::DoNotOptimize(out.ReadPtr());
  }
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(max_bytes));
}
BENCHMARK(BM_LogRead)->Arg(4096)->Arg(65536)->Arg(1 << 20)->Unit(benchmark::kMicrosecond);

// --- offset lookup ----------------------------------------------------------
// Claim (STORAGE_ENGINE.md): a sparse index plus a bounded scan makes an
// arbitrary offset lookup cheap and, importantly, size-independent.

void BM_OffsetLookup(benchmark::State& state) {
  TempDirectory dir;
  auto options = MakeOptions(dir.path(), storage::WriteMode::kWrite, false);
  options.index_interval_bytes = static_cast<std::int64_t>(state.range(0));
  auto log = storage::PartitionLog::Open(TopicPartition{"bench", PartitionIndex{0}}, options);
  if (!log.ok()) {
    state.SkipWithError(log.status().ToString().c_str());
    return;
  }
  for (int i = 0; i < 100; ++i) {
    ByteBuffer batch_bytes = MakeBatch(500, 128);
    auto appended = log.value()->AppendAssigningOffsets(Mutable(batch_bytes), 500);
    if (!appended.ok()) {
      state.SkipWithError(appended.status().ToString().c_str());
      return;
    }
  }

  const Offset end = log.value()->LogEndOffset();
  std::mt19937 rng(99);
  ByteBuffer out;
  for (auto _ : state) {
    const Offset target = static_cast<Offset>(rng() % static_cast<std::uint32_t>(end));
    out.Clear();
    auto read = log.value()->Read(target, 256, out);
    benchmark::DoNotOptimize(read.ok());
  }
  state.SetLabel("index interval " + std::to_string(state.range(0)) + " B");
}
BENCHMARK(BM_OffsetLookup)->Arg(1024)->Arg(4096)->Arg(65536)->Unit(benchmark::kMicrosecond);

}  // namespace

BENCHMARK_MAIN();
