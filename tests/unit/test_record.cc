#include <random>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "pulselog/base/buffer.h"
#include "pulselog/base/endian.h"
#include "pulselog/protocol/record.h"

namespace pulselog::protocol {
namespace {

std::size_t Append(ByteBuffer& buf,
                   Offset offset,
                   std::string_view key,
                   std::string_view value,
                   TimestampMs ts = 1'700'000'000'000,
                   std::uint8_t attributes = 0) {
  return AppendRecord(buf,
                      offset,
                      ts,
                      attributes,
                      /*key_is_null=*/key.data() == nullptr,
                      AsBytes(key),
                      AsBytes(value));
}

TEST(Record, RoundTripWithKey) {
  ByteBuffer buf;
  const std::size_t written = Append(buf, 42, "user-7", "payload");

  RecordView view;
  std::size_t next = 0;
  ASSERT_TRUE(ParseRecord(buf.Readable(), 0, true, view, next).ok());
  EXPECT_EQ(next, written);
  EXPECT_EQ(view.offset, 42);
  EXPECT_EQ(view.timestamp, 1'700'000'000'000);
  EXPECT_FALSE(view.key_is_null);
  EXPECT_EQ(view.key_str(), "user-7");
  EXPECT_EQ(view.value_str(), "payload");
  EXPECT_FALSE(view.tombstone());
}

TEST(Record, NullKey) {
  ByteBuffer buf;
  AppendRecord(buf, 1, 5, 0, /*key_is_null=*/true, ByteSpan{}, AsBytes("v"));

  RecordView view;
  std::size_t next = 0;
  ASSERT_TRUE(ParseRecord(buf.Readable(), 0, true, view, next).ok());
  EXPECT_TRUE(view.key_is_null);
  EXPECT_TRUE(view.key.empty());
  EXPECT_EQ(view.value_str(), "v");
}

TEST(Record, EmptyKeyIsDistinctFromNullKey) {
  ByteBuffer with_empty;
  AppendRecord(with_empty, 1, 5, 0, /*key_is_null=*/false, ByteSpan{}, AsBytes("v"));
  ByteBuffer with_null;
  AppendRecord(with_null, 1, 5, 0, /*key_is_null=*/true, ByteSpan{}, AsBytes("v"));

  RecordView a;
  RecordView b;
  std::size_t next = 0;
  ASSERT_TRUE(ParseRecord(with_empty.Readable(), 0, true, a, next).ok());
  ASSERT_TRUE(ParseRecord(with_null.Readable(), 0, true, b, next).ok());
  EXPECT_FALSE(a.key_is_null);
  EXPECT_TRUE(b.key_is_null);
}

TEST(Record, EmptyValueAndTombstone) {
  ByteBuffer buf;
  const auto tombstone = static_cast<std::uint8_t>(RecordAttribute::kTombstone);
  AppendRecord(buf, 9, 5, tombstone, false, AsBytes("k"), ByteSpan{});

  RecordView view;
  std::size_t next = 0;
  ASSERT_TRUE(ParseRecord(buf.Readable(), 0, true, view, next).ok());
  EXPECT_TRUE(view.tombstone());
  EXPECT_TRUE(view.value.empty());
}

TEST(Record, EncodedSizeMatchesActual) {
  for (const std::size_t key_len : {std::size_t{0},
                                    std::size_t{1},
                                    std::size_t{127},
                                    std::size_t{128},
                                    std::size_t{16383},
                                    std::size_t{16384}}) {
    for (const std::size_t value_len : {std::size_t{0}, std::size_t{100}, std::size_t{20000}}) {
      const std::string key(key_len, 'k');
      const std::string value(value_len, 'v');
      ByteBuffer buf;
      const std::size_t written = AppendRecord(buf, 0, 0, 0, false, AsBytes(key), AsBytes(value));
      EXPECT_EQ(written, EncodedRecordSize(false, key_len, value_len))
          << "key=" << key_len << " value=" << value_len;
      EXPECT_EQ(buf.ReadableBytes(), written);
    }
  }
}

TEST(Record, PerRecordOverheadIsWhatTheDocsClaim) {
  // docs/STORAGE_ENGINE.md quotes 27 bytes of framing for a small record
  // (25 fixed + 2 varints). If this changes, the doc changes with it.
  ByteBuffer buf;
  const std::size_t written = Append(buf, 0, "abcd", std::string(100, 'x'));
  EXPECT_EQ(written, 27U + 4 + 100);
}

TEST(Record, SequentialRecordsIterate) {
  ByteBuffer buf;
  for (int i = 0; i < 100; ++i) {
    Append(buf, i, "k" + std::to_string(i), "v" + std::to_string(i));
  }

  RecordIterator it(buf.Readable(), /*verify_crc=*/true);
  RecordView view;
  int seen = 0;
  while (it.Next(view)) {
    EXPECT_EQ(view.offset, seen);
    EXPECT_EQ(view.key_str(), "k" + std::to_string(seen));
    ++seen;
  }
  EXPECT_TRUE(it.status().ok());
  EXPECT_EQ(seen, 100);
  EXPECT_EQ(it.RecordsRead(), 100U);
}

TEST(Record, RewriteOffsetKeepsChecksumValid) {
  // The leader assigns offsets to records a producer sent with offset 0.
  ByteBuffer buf;
  const std::size_t size = Append(buf, 0, "key", "value");

  RewriteRecordOffset(buf.WritePtr() - size, size, 987'654);

  RecordView view;
  std::size_t next = 0;
  ASSERT_TRUE(ParseRecord(buf.Readable(), 0, /*verify_crc=*/true, view, next).ok());
  EXPECT_EQ(view.offset, 987'654);
  EXPECT_EQ(view.key_str(), "key");
  EXPECT_EQ(view.value_str(), "value");
}

// --- Corruption detection --------------------------------------------------

TEST(Record, ChecksumDetectsSingleBitFlipAnywhere) {
  ByteBuffer buf;
  Append(buf, 5, "key", "some value here");
  std::vector<std::uint8_t> bytes(buf.Readable().begin(), buf.Readable().end());

  // Bytes 0..3 are the length prefix (not covered by the CRC but validated
  // structurally); 4..7 are the CRC itself. Everything from 8 on is covered.
  for (std::size_t i = 8; i < bytes.size(); ++i) {
    bytes[i] ^= 0x01;
    RecordView view;
    std::size_t next = 0;
    const Status status = ParseRecord(bytes, 0, /*verify_crc=*/true, view, next);
    EXPECT_EQ(status.code(), ErrorCode::kCorruption) << "flip at byte " << i;
    bytes[i] ^= 0x01;
  }
}

TEST(Record, TruncatedRecordIsOutOfRangeNotCorruption) {
  // A torn write at the tail is normal after a crash; it must be reported as
  // "incomplete" so recovery truncates rather than declaring the log corrupt.
  ByteBuffer buf;
  Append(buf, 5, "key", std::string(200, 'v'));
  const auto full = buf.Readable();

  for (std::size_t prefix = 1; prefix < full.size(); ++prefix) {
    RecordView view;
    std::size_t next = 0;
    const Status status = ParseRecord(full.subspan(0, prefix), 0, true, view, next);
    ASSERT_FALSE(status.ok()) << "prefix " << prefix;
    EXPECT_EQ(status.code(), ErrorCode::kOutOfRange) << "prefix " << prefix;
  }
}

TEST(Record, AbsurdLengthIsRejected) {
  ByteBuffer buf;
  Append(buf, 0, "k", "v");
  std::vector<std::uint8_t> bytes(buf.Readable().begin(), buf.Readable().end());
  StoreLe<std::uint32_t>(bytes.data(), 0xFFFFFFFU);

  RecordView view;
  std::size_t next = 0;
  const Status status = ParseRecord(bytes, 0, true, view, next);
  EXPECT_EQ(status.code(), ErrorCode::kCorruption);
}

TEST(Record, LengthBelowMinimumIsRejected) {
  std::vector<std::uint8_t> bytes(64, 0);
  StoreLe<std::uint32_t>(bytes.data(), 3);
  RecordView view;
  std::size_t next = 0;
  EXPECT_EQ(ParseRecord(bytes, 0, true, view, next).code(), ErrorCode::kCorruption);
}

TEST(Record, InconsistentInnerLengthsAreRejected) {
  // A record whose key+value lengths do not exactly fill the declared record
  // must be rejected: silently accepting would let a corrupt varint produce a
  // span pointing at unrelated bytes.
  ByteBuffer buf;
  Append(buf, 0, "key", "value");
  std::vector<std::uint8_t> bytes(buf.Readable().begin(), buf.Readable().end());
  // Byte 25 is the key-length varint (4 = 3+1 for a 3-byte key).
  bytes[25] = 2;  // Now claims a 1-byte key, leaving a trailing byte.

  RecordView view;
  std::size_t next = 0;
  const Status status = ParseRecord(bytes, 0, /*verify_crc=*/false, view, next);
  EXPECT_EQ(status.code(), ErrorCode::kCorruption);
}

TEST(Record, IteratorStopsAtFirstBadRecord) {
  ByteBuffer buf;
  Append(buf, 0, "a", "1");
  const std::size_t first_size = buf.ReadableBytes();
  Append(buf, 1, "b", "2");
  Append(buf, 2, "c", "3");

  std::vector<std::uint8_t> bytes(buf.Readable().begin(), buf.Readable().end());
  bytes[first_size + 10] ^= 0xFF;  // Damage the second record.

  RecordIterator it(bytes, /*verify_crc=*/true);
  RecordView view;
  ASSERT_TRUE(it.Next(view));
  EXPECT_EQ(view.offset, 0);
  EXPECT_FALSE(it.Next(view));
  EXPECT_EQ(it.status().code(), ErrorCode::kCorruption);
  EXPECT_EQ(it.RecordsRead(), 1U);
}

TEST(Record, PeekLengthMatchesParsedSize) {
  ByteBuffer buf;
  const std::size_t size = Append(buf, 0, "kk", "vvvv");
  const auto peeked = PeekRecordLength(buf.Readable(), 0);
  ASSERT_TRUE(peeked.has_value());
  EXPECT_EQ(*peeked + 4, size);
  EXPECT_FALSE(PeekRecordLength(buf.Readable(), buf.ReadableBytes() - 2).has_value());
}

TEST(Record, RandomBytesAreNeverAcceptedWithCrcOn) {
  std::mt19937 rng(999);
  std::vector<std::uint8_t> noise(256);
  int accepted = 0;
  for (int trial = 0; trial < 5000; ++trial) {
    for (auto& b : noise) b = static_cast<std::uint8_t>(rng());
    RecordView view;
    std::size_t next = 0;
    if (ParseRecord(noise, 0, /*verify_crc=*/true, view, next).ok()) ++accepted;
  }
  EXPECT_EQ(accepted, 0);
}

TEST(Record, LargeValueRoundTrips) {
  const std::string value(1 << 20, 'L');
  ByteBuffer buf;
  const std::size_t written = Append(buf, 1, "big", value);

  RecordView view;
  std::size_t next = 0;
  ASSERT_TRUE(ParseRecord(buf.Readable(), 0, true, view, next).ok());
  EXPECT_EQ(next, written);
  EXPECT_EQ(view.value.size(), value.size());
  EXPECT_EQ(view.value_str(), value);
}

TEST(Record, NegativeAndLargeOffsetsRoundTrip) {
  for (const Offset offset : {Offset{0}, Offset{1}, Offset{1LL << 40}, Offset{-1}}) {
    ByteBuffer buf;
    Append(buf, offset, "k", "v");
    RecordView view;
    std::size_t next = 0;
    ASSERT_TRUE(ParseRecord(buf.Readable(), 0, true, view, next).ok());
    EXPECT_EQ(view.offset, offset);
  }
}

}  // namespace
}  // namespace pulselog::protocol
