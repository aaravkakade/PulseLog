// Storage engine tests: append, read, segment rolling, indexing, recovery,
// corruption handling, truncation and retention.
#include <algorithm>
#include <random>
#include <string>
#include <vector>

#include "test_support/temp_dir.h"
#include <gtest/gtest.h>

#include "pulselog/base/buffer.h"
#include "pulselog/protocol/record.h"
#include "pulselog/storage/partition_log.h"
#include "pulselog/storage/segment.h"

namespace pulselog::storage {
namespace {

using protocol::AppendRecord;
using protocol::RecordIterator;
using protocol::RecordView;

// Builds a produce-style batch: records with offset 0, as a client would send.
ByteBuffer MakeBatch(int count,
                     std::string_view key_prefix,
                     std::size_t value_size,
                     TimestampMs timestamp = 0) {
  ByteBuffer buf;
  const std::string value(value_size, 'v');
  for (int i = 0; i < count; ++i) {
    const std::string key = std::string(key_prefix) + std::to_string(i);
    AppendRecord(buf, 0, timestamp, 0, /*key_is_null=*/false, AsBytes(key), AsBytes(value));
  }
  return buf;
}

MutableByteSpan Mutable(ByteBuffer& buf) {
  return {const_cast<std::uint8_t*>(buf.ReadPtr()), buf.ReadableBytes()};
}

LogOptions MakeOptions(const std::filesystem::path& dir, std::int64_t segment_bytes = 1 << 20) {
  LogOptions options;
  options.directory = dir;
  options.segment_bytes = segment_bytes;
  options.index_interval_bytes = 256;
  options.preallocate = false;  // Keeps test artefacts small and readable.
  options.flush.sync_on_append = false;
  options.flush.interval_ms = -1;
  options.flush.max_unflushed_bytes = -1;
  options.flush.max_unflushed_records = -1;
  return options;
}

std::unique_ptr<PartitionLog> OpenLog(const std::filesystem::path& dir,
                                      std::int64_t segment_bytes = 1 << 20,
                                      RecoveryReport* report = nullptr) {
  auto log = PartitionLog::Open(
      TopicPartition{"t", PartitionIndex{0}}, MakeOptions(dir, segment_bytes), report);
  EXPECT_TRUE(log.ok()) << log.status().ToString();
  return std::move(log).value();
}

// Reads every record from `from` and returns the values in order.
std::vector<std::string> ReadValues(const PartitionLog& log,
                                    Offset from,
                                    std::size_t max_bytes = 1 << 20) {
  ByteBuffer out;
  auto result = log.Read(from, max_bytes, out);
  EXPECT_TRUE(result.ok()) << result.status().ToString();
  std::vector<std::string> values;
  RecordIterator it(out.Readable(), /*verify_crc=*/true);
  RecordView view;
  while (it.Next(view)) values.emplace_back(view.value_str());
  EXPECT_TRUE(it.status().ok()) << it.status().ToString();
  return values;
}

// --- Basic append/read -----------------------------------------------------

TEST(PartitionLog, EmptyLogStartsAtZero) {
  testing::TempDir dir;
  auto log = OpenLog(dir.path());
  EXPECT_EQ(log->LogStartOffset(), 0);
  EXPECT_EQ(log->LogEndOffset(), 0);
  EXPECT_EQ(ReadValues(*log, 0).size(), 0U);
}

TEST(PartitionLog, AppendAssignsSequentialOffsets) {
  testing::TempDir dir;
  auto log = OpenLog(dir.path());

  ByteBuffer batch = MakeBatch(10, "k", 32);
  auto result = log->AppendAssigningOffsets(Mutable(batch), 10);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_EQ(result->base_offset, 0);
  EXPECT_EQ(result->last_offset, 9);
  EXPECT_EQ(result->record_count, 10U);
  EXPECT_EQ(log->LogEndOffset(), 10);

  ByteBuffer batch2 = MakeBatch(5, "k", 32);
  auto result2 = log->AppendAssigningOffsets(Mutable(batch2), 5);
  ASSERT_TRUE(result2.ok());
  EXPECT_EQ(result2->base_offset, 10);
  EXPECT_EQ(result2->last_offset, 14);
  EXPECT_EQ(log->LogEndOffset(), 15);
}

TEST(PartitionLog, AppendedRecordsCarryTheirAssignedOffsets) {
  testing::TempDir dir;
  auto log = OpenLog(dir.path());
  ByteBuffer batch = MakeBatch(20, "k", 16);
  ASSERT_TRUE(log->AppendAssigningOffsets(Mutable(batch), 20).ok());

  ByteBuffer out;
  auto read = log->Read(0, 1 << 20, out);
  ASSERT_TRUE(read.ok());
  EXPECT_EQ(read->record_count, 20U);

  RecordIterator it(out.Readable(), /*verify_crc=*/true);
  RecordView view;
  Offset expected = 0;
  while (it.Next(view)) {
    EXPECT_EQ(view.offset, expected) << "offsets must be dense and in order";
    ++expected;
  }
  EXPECT_TRUE(it.status().ok());
  EXPECT_EQ(expected, 20);
}

TEST(PartitionLog, BrokerStampsMissingTimestamps) {
  testing::TempDir dir;
  auto log = OpenLog(dir.path());
  ByteBuffer batch = MakeBatch(3, "k", 8, /*timestamp=*/0);
  auto result = log->AppendAssigningOffsets(Mutable(batch), 3);
  ASSERT_TRUE(result.ok());

  ByteBuffer out;
  ASSERT_TRUE(log->Read(0, 1 << 20, out).ok());
  RecordIterator it(out.Readable(), true);
  RecordView view;
  while (it.Next(view)) {
    EXPECT_GT(view.timestamp, 0) << "unset timestamps must get log-append time";
    EXPECT_EQ(view.timestamp, result->append_time);
  }
}

TEST(PartitionLog, ProducerTimestampsArePreserved) {
  testing::TempDir dir;
  auto log = OpenLog(dir.path());
  ByteBuffer batch = MakeBatch(3, "k", 8, /*timestamp=*/1'600'000'000'000);
  ASSERT_TRUE(log->AppendAssigningOffsets(Mutable(batch), 3).ok());

  ByteBuffer out;
  ASSERT_TRUE(log->Read(0, 1 << 20, out).ok());
  RecordIterator it(out.Readable(), true);
  RecordView view;
  while (it.Next(view)) EXPECT_EQ(view.timestamp, 1'600'000'000'000);
}

TEST(PartitionLog, ReadFromMiddleOffset) {
  testing::TempDir dir;
  auto log = OpenLog(dir.path());
  ByteBuffer batch = MakeBatch(100, "k", 40);
  ASSERT_TRUE(log->AppendAssigningOffsets(Mutable(batch), 100).ok());

  ByteBuffer out;
  auto read = log->Read(60, 1 << 20, out);
  ASSERT_TRUE(read.ok());
  EXPECT_EQ(read->record_count, 40U);

  RecordIterator it(out.Readable(), true);
  RecordView view;
  ASSERT_TRUE(it.Next(view));
  EXPECT_EQ(view.offset, 60);
}

TEST(PartitionLog, ReadRespectsMaxBytesAndStopsOnRecordBoundary) {
  testing::TempDir dir;
  auto log = OpenLog(dir.path());
  ByteBuffer batch = MakeBatch(200, "k", 100);
  ASSERT_TRUE(log->AppendAssigningOffsets(Mutable(batch), 200).ok());

  ByteBuffer out;
  auto read = log->Read(0, 1000, out);
  ASSERT_TRUE(read.ok());
  EXPECT_GT(read->record_count, 0U);
  EXPECT_LE(read->bytes, 1000U + 200);  // May overshoot by at most one record.

  // Whatever came back must parse cleanly: never a partial record.
  RecordIterator it(out.Readable(), true);
  RecordView view;
  std::uint32_t seen = 0;
  while (it.Next(view)) ++seen;
  EXPECT_TRUE(it.status().ok()) << it.status().ToString();
  EXPECT_EQ(seen, read->record_count);
}

TEST(PartitionLog, OversizedRecordIsStillReturned) {
  // A consumer with a small max_bytes must not wedge on a record larger than
  // its budget.
  testing::TempDir dir;
  auto log = OpenLog(dir.path());
  ByteBuffer batch = MakeBatch(1, "k", 64 * 1024);
  ASSERT_TRUE(log->AppendAssigningOffsets(Mutable(batch), 1).ok());

  ByteBuffer out;
  auto read = log->Read(0, 100, out);
  ASSERT_TRUE(read.ok());
  EXPECT_EQ(read->record_count, 1U);
  EXPECT_GT(read->bytes, 64U * 1024);
}

TEST(PartitionLog, ReadPastEndReturnsNothing) {
  testing::TempDir dir;
  auto log = OpenLog(dir.path());
  ByteBuffer batch = MakeBatch(5, "k", 8);
  ASSERT_TRUE(log->AppendAssigningOffsets(Mutable(batch), 5).ok());

  ByteBuffer out;
  auto read = log->Read(5, 1024, out);
  ASSERT_TRUE(read.ok());
  EXPECT_EQ(read->record_count, 0U);

  EXPECT_EQ(log->Read(6, 1024, out).status().code(), ErrorCode::kOutOfRange);
}

// --- Segment rolling and indexing ------------------------------------------

TEST(PartitionLog, RollsSegmentsWhenFull) {
  testing::TempDir dir;
  auto log = OpenLog(dir.path(), /*segment_bytes=*/4096);

  for (int i = 0; i < 40; ++i) {
    ByteBuffer batch = MakeBatch(10, "k", 100);
    ASSERT_TRUE(log->AppendAssigningOffsets(Mutable(batch), 10).ok());
  }
  EXPECT_EQ(log->LogEndOffset(), 400);

  const auto stats = log->GetStats();
  EXPECT_GT(stats.segment_count, 1U) << "expected the log to roll";
  EXPECT_EQ(stats.roll_count, stats.segment_count - 1);

  // Reads must cross segment boundaries transparently.
  const auto values = ReadValues(*log, 0, 1 << 22);
  EXPECT_EQ(values.size(), 400U);
}

TEST(PartitionLog, ReadSpansSegmentBoundaries) {
  testing::TempDir dir;
  auto log = OpenLog(dir.path(), /*segment_bytes=*/2048);
  for (int i = 0; i < 30; ++i) {
    ByteBuffer batch = MakeBatch(10, "k", 60);
    ASSERT_TRUE(log->AppendAssigningOffsets(Mutable(batch), 10).ok());
  }
  ASSERT_GT(log->GetStats().segment_count, 2U);

  // Start mid-segment and read through to the end.
  ByteBuffer out;
  auto read = log->Read(15, 1 << 22, out);
  ASSERT_TRUE(read.ok());
  EXPECT_EQ(read->record_count, 300U - 15);

  RecordIterator it(out.Readable(), true);
  RecordView view;
  Offset expected = 15;
  while (it.Next(view)) {
    EXPECT_EQ(view.offset, expected);
    ++expected;
  }
  EXPECT_EQ(expected, 300);
}

TEST(PartitionLog, EveryOffsetIsIndividuallyAddressable) {
  // Exercises the sparse index + bounded forward scan at every position.
  testing::TempDir dir;
  auto log = OpenLog(dir.path(), /*segment_bytes=*/8192);
  ByteBuffer batch = MakeBatch(500, "key", 50);
  ASSERT_TRUE(log->AppendAssigningOffsets(Mutable(batch), 500).ok());

  for (Offset offset = 0; offset < 500; ++offset) {
    ByteBuffer out;
    auto read = log->Read(offset, 200, out);
    ASSERT_TRUE(read.ok()) << "offset " << offset << ": " << read.status().ToString();
    ASSERT_GT(read->record_count, 0U) << "offset " << offset;

    RecordView view;
    std::size_t next = 0;
    ASSERT_TRUE(protocol::ParseRecord(out.Readable(), 0, true, view, next).ok());
    EXPECT_EQ(view.offset, offset);
  }
}

TEST(Segment, BaseOffsetFilenameRoundTrip) {
  for (const Offset offset : {Offset{0}, Offset{1}, Offset{42}, Offset{1'000'000'000'000}}) {
    const std::string stem = Segment::FormatBaseOffset(offset);
    EXPECT_EQ(stem.size(), 20U);
    const auto parsed = Segment::ParseBaseOffset(stem);
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(*parsed, offset);
  }
  EXPECT_FALSE(Segment::ParseBaseOffset("not-a-number").has_value());
  EXPECT_FALSE(Segment::ParseBaseOffset("123").has_value());
}

// --- Restart and recovery --------------------------------------------------

TEST(PartitionLog, ReopenRecoversAllRecords) {
  testing::TempDir dir;
  {
    auto log = OpenLog(dir.path(), 4096);
    for (int i = 0; i < 20; ++i) {
      ByteBuffer batch = MakeBatch(10, "k", 80);
      ASSERT_TRUE(log->AppendAssigningOffsets(Mutable(batch), 10).ok());
    }
    ASSERT_TRUE(log->Flush().ok());
    ASSERT_TRUE(log->Close().ok());
  }

  RecoveryReport report;
  auto log = OpenLog(dir.path(), 4096, &report);
  EXPECT_EQ(log->LogEndOffset(), 200);
  EXPECT_FALSE(report.truncated);
  EXPECT_EQ(ReadValues(*log, 0, 1 << 22).size(), 200U);
}

TEST(PartitionLog, ReopenWithoutCleanCloseRecovers) {
  // Simulates SIGKILL: no Close(), no final flush.
  testing::TempDir dir;
  {
    auto log = OpenLog(dir.path(), 8192);
    ByteBuffer batch = MakeBatch(150, "k", 60);
    ASSERT_TRUE(log->AppendAssigningOffsets(Mutable(batch), 150).ok());
    // Deliberately no Flush() and no Close(); the destructor will run, but the
    // data must be recoverable from the file contents alone.
  }

  RecoveryReport report;
  auto log = OpenLog(dir.path(), 8192, &report);
  EXPECT_EQ(log->LogEndOffset(), 150);
  EXPECT_EQ(ReadValues(*log, 0, 1 << 22).size(), 150U);
}

TEST(PartitionLog, AppendsContinueAfterRecovery) {
  testing::TempDir dir;
  {
    auto log = OpenLog(dir.path());
    ByteBuffer batch = MakeBatch(10, "k", 40);
    ASSERT_TRUE(log->AppendAssigningOffsets(Mutable(batch), 10).ok());
  }
  auto log = OpenLog(dir.path());
  ByteBuffer batch = MakeBatch(5, "k", 40);
  auto result = log->AppendAssigningOffsets(Mutable(batch), 5);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result->base_offset, 10) << "recovery must resume the offset sequence";
  EXPECT_EQ(log->LogEndOffset(), 15);
}

TEST(PartitionLog, TornWriteAtTailIsTruncated) {
  testing::TempDir dir;
  std::uint64_t good_size = 0;
  {
    auto log = OpenLog(dir.path());
    ByteBuffer batch = MakeBatch(50, "k", 100);
    ASSERT_TRUE(log->AppendAssigningOffsets(Mutable(batch), 50).ok());
    ASSERT_TRUE(log->Flush().ok());
    ASSERT_TRUE(log->Close().ok());
    good_size = std::filesystem::file_size(dir.Child("00000000000000000000.log"));
  }

  // Chop the file mid-record, exactly as a power cut would.
  ASSERT_TRUE(testing::TruncateFile(dir.Child("00000000000000000000.log"), good_size - 37));

  RecoveryReport report;
  auto log = OpenLog(dir.path(), 1 << 20, &report);
  EXPECT_TRUE(report.truncated);
  EXPECT_EQ(log->LogEndOffset(), 49) << "the torn record must be dropped, the rest kept";
  EXPECT_EQ(ReadValues(*log, 0).size(), 49U);

  // The log must still be writable, and continue from the recovered end.
  ByteBuffer batch = MakeBatch(1, "k", 10);
  auto result = log->AppendAssigningOffsets(Mutable(batch), 1);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result->base_offset, 49);
}

TEST(PartitionLog, CorruptRecordTruncatesFromThatPoint) {
  testing::TempDir dir;
  {
    auto log = OpenLog(dir.path());
    ByteBuffer batch = MakeBatch(30, "k", 64);
    ASSERT_TRUE(log->AppendAssigningOffsets(Mutable(batch), 30).ok());
    ASSERT_TRUE(log->Flush().ok());
    ASSERT_TRUE(log->Close().ok());
  }

  // Flip a bit inside the value of a record roughly halfway through.
  const auto path = dir.Child("00000000000000000000.log");
  const auto size = std::filesystem::file_size(path);
  ASSERT_TRUE(testing::CorruptFileAt(path, size / 2, 1, 0xFF));

  RecoveryReport report;
  auto log = OpenLog(dir.path(), 1 << 20, &report);
  EXPECT_TRUE(report.truncated);
  EXPECT_LT(log->LogEndOffset(), 30) << "records at and after the damage must be dropped";
  EXPECT_GT(log->LogEndOffset(), 0) << "records before the damage must survive";

  // Everything still readable must be intact.
  const auto values = ReadValues(*log, 0);
  EXPECT_EQ(static_cast<Offset>(values.size()), log->LogEndOffset());
}

TEST(PartitionLog, RecoveryIsIdempotent) {
  testing::TempDir dir;
  {
    auto log = OpenLog(dir.path(), 4096);
    ByteBuffer batch = MakeBatch(100, "k", 70);
    ASSERT_TRUE(log->AppendAssigningOffsets(Mutable(batch), 100).ok());
  }
  Offset first_end = 0;
  {
    auto log = OpenLog(dir.path(), 4096);
    first_end = log->LogEndOffset();
  }
  for (int i = 0; i < 3; ++i) {
    auto log = OpenLog(dir.path(), 4096);
    EXPECT_EQ(log->LogEndOffset(), first_end) << "repeated recovery must be stable";
  }
}

TEST(PartitionLog, GarbageAppendedToLogIsIgnored) {
  testing::TempDir dir;
  {
    auto log = OpenLog(dir.path());
    ByteBuffer batch = MakeBatch(10, "k", 32);
    ASSERT_TRUE(log->AppendAssigningOffsets(Mutable(batch), 10).ok());
    ASSERT_TRUE(log->Flush().ok());
    ASSERT_TRUE(log->Close().ok());
  }

  {
    std::ofstream out(dir.Child("00000000000000000000.log"), std::ios::binary | std::ios::app);
    const std::string garbage(500, '\xCC');
    out.write(garbage.data(), static_cast<std::streamsize>(garbage.size()));
  }

  RecoveryReport report;
  auto log = OpenLog(dir.path(), 1 << 20, &report);
  EXPECT_TRUE(report.truncated);
  EXPECT_EQ(log->LogEndOffset(), 10);
  EXPECT_EQ(ReadValues(*log, 0).size(), 10U);
}

// --- Replication-style appends ---------------------------------------------

TEST(PartitionLog, AppendWithOffsetsAcceptsContiguousBatch) {
  testing::TempDir dir;
  auto leader = OpenLog(dir.path());
  ByteBuffer batch = MakeBatch(10, "k", 40);
  ASSERT_TRUE(leader->AppendAssigningOffsets(Mutable(batch), 10).ok());

  ByteBuffer copy;
  ASSERT_TRUE(leader->Read(0, 1 << 20, copy).ok());

  testing::TempDir follower_dir;
  auto follower = OpenLog(follower_dir.path());
  auto result = follower->AppendWithOffsets(copy.Readable(), 10);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_EQ(follower->LogEndOffset(), 10);
  EXPECT_EQ(ReadValues(*follower, 0), ReadValues(*leader, 0));
}

TEST(PartitionLog, AppendWithOffsetsRejectsGaps) {
  testing::TempDir dir;
  auto leader = OpenLog(dir.path());
  ByteBuffer batch = MakeBatch(10, "k", 40);
  ASSERT_TRUE(leader->AppendAssigningOffsets(Mutable(batch), 10).ok());

  ByteBuffer tail;
  ASSERT_TRUE(leader->Read(5, 1 << 20, tail).ok());  // Starts at offset 5.

  testing::TempDir follower_dir;
  auto follower = OpenLog(follower_dir.path());
  auto result = follower->AppendWithOffsets(tail.Readable(), 5);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), ErrorCode::kOutOfRange)
      << "a follower must refuse a batch that would leave a hole";
  EXPECT_EQ(follower->LogEndOffset(), 0);
}

TEST(PartitionLog, AppendWithOffsetsRejectsCorruptRecords) {
  testing::TempDir dir;
  auto leader = OpenLog(dir.path());
  ByteBuffer batch = MakeBatch(10, "k", 40);
  ASSERT_TRUE(leader->AppendAssigningOffsets(Mutable(batch), 10).ok());

  ByteBuffer copy;
  ASSERT_TRUE(leader->Read(0, 1 << 20, copy).ok());
  // Corrupt a byte in flight, as a bad NIC or cable would.
  const_cast<std::uint8_t*>(copy.ReadPtr())[100] ^= 0xFF;

  testing::TempDir follower_dir;
  auto follower = OpenLog(follower_dir.path());
  auto result = follower->AppendWithOffsets(copy.Readable(), 10);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), ErrorCode::kCorruption);
  EXPECT_EQ(follower->LogEndOffset(), 0) << "nothing may be written from a corrupt batch";
}

TEST(PartitionLog, VectoredAppendWritesAllChunks) {
  testing::TempDir dir;
  auto source = OpenLog(dir.path());
  ByteBuffer first = MakeBatch(5, "a", 30);
  ASSERT_TRUE(source->AppendAssigningOffsets(Mutable(first), 5).ok());
  ByteBuffer second = MakeBatch(5, "b", 30);
  ASSERT_TRUE(source->AppendAssigningOffsets(Mutable(second), 5).ok());

  testing::TempDir target_dir;
  auto target = OpenLog(target_dir.path());
  const std::array<ByteSpan, 2> chunks{first.Readable(), second.Readable()};
  auto result = target->AppendVectoredWithOffsets(chunks, 10);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_EQ(result->record_count, 10U);
  EXPECT_EQ(target->LogEndOffset(), 10);
  EXPECT_EQ(ReadValues(*target, 0), ReadValues(*source, 0));
}

// --- Truncation and retention ----------------------------------------------

TEST(PartitionLog, TruncateToRemovesTail) {
  testing::TempDir dir;
  auto log = OpenLog(dir.path(), 4096);
  for (int i = 0; i < 10; ++i) {
    ByteBuffer batch = MakeBatch(20, "k", 60);
    ASSERT_TRUE(log->AppendAssigningOffsets(Mutable(batch), 20).ok());
  }
  ASSERT_EQ(log->LogEndOffset(), 200);

  ASSERT_TRUE(log->TruncateTo(150).ok());
  EXPECT_EQ(log->LogEndOffset(), 150);
  EXPECT_EQ(ReadValues(*log, 0, 1 << 22).size(), 150U);

  // Appends resume from the truncation point.
  ByteBuffer batch = MakeBatch(1, "z", 10);
  auto result = log->AppendAssigningOffsets(Mutable(batch), 1);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result->base_offset, 150);
}

TEST(PartitionLog, TruncationSurvivesRestart) {
  testing::TempDir dir;
  {
    auto log = OpenLog(dir.path(), 4096);
    for (int i = 0; i < 10; ++i) {
      ByteBuffer batch = MakeBatch(20, "k", 60);
      ASSERT_TRUE(log->AppendAssigningOffsets(Mutable(batch), 20).ok());
    }
    ASSERT_TRUE(log->TruncateTo(77).ok());
    ASSERT_TRUE(log->Close().ok());
  }
  auto log = OpenLog(dir.path(), 4096);
  EXPECT_EQ(log->LogEndOffset(), 77);
  EXPECT_EQ(ReadValues(*log, 0, 1 << 22).size(), 77U);
}

TEST(PartitionLog, RetentionBySizeDeletesOldSegments) {
  testing::TempDir dir;
  LogOptions options = MakeOptions(dir.path(), /*segment_bytes=*/2048);
  options.retention_bytes = 6000;
  auto opened = PartitionLog::Open(TopicPartition{"t", PartitionIndex{0}}, options);
  ASSERT_TRUE(opened.ok());
  auto log = std::move(opened).value();

  for (int i = 0; i < 40; ++i) {
    ByteBuffer batch = MakeBatch(10, "k", 60);
    ASSERT_TRUE(log->AppendAssigningOffsets(Mutable(batch), 10).ok());
  }
  const auto before = log->GetStats();
  ASSERT_GT(before.segment_count, 3U);

  auto deleted = log->EnforceRetention();
  ASSERT_TRUE(deleted.ok());
  EXPECT_GT(deleted.value(), 0U);

  const auto after = log->GetStats();
  EXPECT_LT(after.segment_count, before.segment_count);
  EXPECT_GT(log->LogStartOffset(), 0) << "log start must advance past deleted segments";

  // Reading a deleted offset must be an explicit error, not silent data loss.
  ByteBuffer out;
  EXPECT_EQ(log->Read(0, 1024, out).status().code(), ErrorCode::kOutOfRange);
  // Reading from the new start must still work.
  EXPECT_GT(ReadValues(*log, log->LogStartOffset(), 1 << 22).size(), 0U);
}

TEST(PartitionLog, RetentionNeverDeletesTheActiveSegment) {
  testing::TempDir dir;
  LogOptions options = MakeOptions(dir.path(), 1 << 20);
  options.retention_bytes = 1;  // Absurdly small on purpose.
  auto opened = PartitionLog::Open(TopicPartition{"t", PartitionIndex{0}}, options);
  ASSERT_TRUE(opened.ok());
  auto log = std::move(opened).value();

  ByteBuffer batch = MakeBatch(10, "k", 40);
  ASSERT_TRUE(log->AppendAssigningOffsets(Mutable(batch), 10).ok());
  ASSERT_TRUE(log->EnforceRetention().ok());

  EXPECT_EQ(log->GetStats().segment_count, 1U);
  EXPECT_EQ(ReadValues(*log, 0).size(), 10U);
}

// --- Flush accounting ------------------------------------------------------

TEST(PartitionLog, FlushAdvancesFlushedOffset) {
  testing::TempDir dir;
  auto log = OpenLog(dir.path());
  ByteBuffer batch = MakeBatch(10, "k", 40);
  ASSERT_TRUE(log->AppendAssigningOffsets(Mutable(batch), 10).ok());

  EXPECT_EQ(log->FlushedOffset(), 0) << "nothing is durable before a flush";
  ASSERT_TRUE(log->Flush().ok());
  EXPECT_EQ(log->FlushedOffset(), 10);
  EXPECT_GE(log->GetStats().flush_count, 1U);
}

TEST(PartitionLog, SyncOnAppendFlushesInline) {
  testing::TempDir dir;
  LogOptions options = MakeOptions(dir.path());
  options.flush.sync_on_append = true;
  auto opened = PartitionLog::Open(TopicPartition{"t", PartitionIndex{0}}, options);
  ASSERT_TRUE(opened.ok());
  auto log = std::move(opened).value();

  ByteBuffer batch = MakeBatch(5, "k", 40);
  ASSERT_TRUE(log->AppendAssigningOffsets(Mutable(batch), 5).ok());
  EXPECT_EQ(log->FlushedOffset(), 5) << "sync_on_append must make the append durable";
  EXPECT_FALSE(log->NeedsFlush(0));
}

TEST(PartitionLog, NeedsFlushHonoursThresholds) {
  testing::TempDir dir;
  LogOptions options = MakeOptions(dir.path());
  options.flush.max_unflushed_records = 5;
  auto opened = PartitionLog::Open(TopicPartition{"t", PartitionIndex{0}}, options);
  ASSERT_TRUE(opened.ok());
  auto log = std::move(opened).value();

  EXPECT_FALSE(log->NeedsFlush(0));
  ByteBuffer batch = MakeBatch(3, "k", 20);
  ASSERT_TRUE(log->AppendAssigningOffsets(Mutable(batch), 3).ok());
  EXPECT_FALSE(log->NeedsFlush(0));

  ByteBuffer more = MakeBatch(3, "k", 20);
  ASSERT_TRUE(log->AppendAssigningOffsets(Mutable(more), 3).ok());
  EXPECT_TRUE(log->NeedsFlush(0)) << "crossing the record threshold must request a flush";
}

// --- ListOffsets -----------------------------------------------------------

TEST(PartitionLog, OffsetForTimestampResolvesSpecialValues) {
  testing::TempDir dir;
  auto log = OpenLog(dir.path());
  ByteBuffer batch = MakeBatch(10, "k", 40);
  ASSERT_TRUE(log->AppendAssigningOffsets(Mutable(batch), 10).ok());

  auto earliest = log->OffsetForTimestamp(kEarliestOffset);
  ASSERT_TRUE(earliest.ok());
  EXPECT_EQ(earliest.value(), 0);

  auto latest = log->OffsetForTimestamp(kLatestOffset);
  ASSERT_TRUE(latest.ok());
  EXPECT_EQ(latest.value(), 10);
}

TEST(PartitionLog, OffsetForFutureTimestampIsLogEnd) {
  testing::TempDir dir;
  auto log = OpenLog(dir.path());
  ByteBuffer batch = MakeBatch(10, "k", 40, /*timestamp=*/1'600'000'000'000);
  ASSERT_TRUE(log->AppendAssigningOffsets(Mutable(batch), 10).ok());

  auto offset = log->OffsetForTimestamp(2'000'000'000'000);
  ASSERT_TRUE(offset.ok());
  EXPECT_EQ(offset.value(), 10);
}

// --- Rejections ------------------------------------------------------------

TEST(PartitionLog, RejectsMismatchedRecordCount) {
  testing::TempDir dir;
  auto log = OpenLog(dir.path());
  ByteBuffer batch = MakeBatch(5, "k", 20);
  auto result = log->AppendAssigningOffsets(Mutable(batch), 99);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), ErrorCode::kInvalidArgument);
}

TEST(PartitionLog, RejectsMalformedBatch) {
  testing::TempDir dir;
  auto log = OpenLog(dir.path());
  ByteBuffer batch = MakeBatch(3, "k", 20);
  // Damage the length field of the second record so the batch stops parsing.
  auto span = Mutable(batch);
  const std::size_t first_size = protocol::EncodedRecordSize(false, 2, 20);
  span[first_size] = 0xFF;
  span[first_size + 1] = 0xFF;

  auto result = log->AppendAssigningOffsets(span, 3);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(log->LogEndOffset(), 0) << "a rejected batch must leave the log untouched";
}

TEST(PartitionLog, RejectsEmptyAppend) {
  testing::TempDir dir;
  auto log = OpenLog(dir.path());
  ByteBuffer empty;
  auto result = log->AppendAssigningOffsets(Mutable(empty), 0);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), ErrorCode::kInvalidArgument);
}

TEST(PartitionLog, StatsReflectContent) {
  testing::TempDir dir;
  auto log = OpenLog(dir.path(), 4096);
  for (int i = 0; i < 10; ++i) {
    ByteBuffer batch = MakeBatch(10, "k", 100);
    ASSERT_TRUE(log->AppendAssigningOffsets(Mutable(batch), 10).ok());
  }
  const auto stats = log->GetStats();
  EXPECT_EQ(stats.log_end_offset, 100);
  EXPECT_EQ(stats.log_start_offset, 0);
  EXPECT_GT(stats.total_bytes, 100U * 100);
  EXPECT_GE(stats.segment_count, 1U);
}

}  // namespace
}  // namespace pulselog::storage
