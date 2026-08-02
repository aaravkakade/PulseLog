#include <random>
#include <vector>

#include <gtest/gtest.h>

#include "pulselog/base/buffer.h"
#include "pulselog/base/crc32c.h"
#include "pulselog/base/endian.h"
#include "pulselog/protocol/frame.h"

namespace pulselog::protocol {
namespace {

ByteBuffer MakeFrame(OpCode op, RequestId id, std::string_view payload, std::uint16_t flags = 0) {
  ByteBuffer buf;
  EncodeFrame(buf, op, id, flags, AsBytes(payload));
  return buf;
}

TEST(Frame, HeaderSizeIsFixed) {
  EXPECT_EQ(kFrameHeaderSize, 32U);
}

TEST(Frame, MagicIsPlsgInStreamOrder) {
  ByteBuffer buf = MakeFrame(OpCode::kHealth, 1, "");
  const auto bytes = buf.Readable();
  ASSERT_GE(bytes.size(), 4U);
  EXPECT_EQ(bytes[0], 'P');
  EXPECT_EQ(bytes[1], 'L');
  EXPECT_EQ(bytes[2], 'S');
  EXPECT_EQ(bytes[3], 'G');
}

TEST(Frame, RoundTrip) {
  const std::string payload = "the quick brown fox";
  ByteBuffer buf = MakeFrame(OpCode::kProduce, 0xDEADBEEFCAFEULL, payload);

  FrameDecoder decoder;
  FrameDecoder::Frame frame;
  ASSERT_EQ(decoder.Next(buf.Readable(), frame), FrameDecoder::State::kFrame);
  EXPECT_EQ(frame.header.opcode, OpCode::kProduce);
  EXPECT_EQ(frame.header.request_id, 0xDEADBEEFCAFEULL);
  EXPECT_EQ(frame.header.version, kProtocolVersion);
  EXPECT_EQ(frame.header.payload_len, payload.size());
  EXPECT_EQ(AsStringView(frame.payload), payload);
  EXPECT_EQ(frame.total_size, kFrameHeaderSize + payload.size());
}

TEST(Frame, EmptyPayload) {
  ByteBuffer buf = MakeFrame(OpCode::kHealth, 7, "");
  EXPECT_EQ(buf.ReadableBytes(), kFrameHeaderSize);

  FrameDecoder decoder;
  FrameDecoder::Frame frame;
  ASSERT_EQ(decoder.Next(buf.Readable(), frame), FrameDecoder::State::kFrame);
  EXPECT_EQ(frame.header.payload_len, 0U);
  EXPECT_EQ(frame.header.payload_crc, 0U);
  EXPECT_TRUE(frame.payload.empty());
}

TEST(Frame, ResponseFlagRoundTrips) {
  ByteBuffer buf =
      MakeFrame(OpCode::kProduce, 3, "x", static_cast<std::uint16_t>(FrameFlags::kResponse));
  FrameDecoder decoder;
  FrameDecoder::Frame frame;
  ASSERT_EQ(decoder.Next(buf.Readable(), frame), FrameDecoder::State::kFrame);
  EXPECT_TRUE(frame.header.is_response());
}

// --- Partial reads ---------------------------------------------------------

TEST(Frame, PartialHeaderNeedsMore) {
  ByteBuffer buf = MakeFrame(OpCode::kFetch, 1, "payload");
  FrameDecoder decoder;
  FrameDecoder::Frame frame;

  for (std::size_t prefix = 0; prefix < kFrameHeaderSize; ++prefix) {
    const auto partial = buf.Readable().subspan(0, prefix);
    EXPECT_EQ(decoder.Next(partial, frame), FrameDecoder::State::kNeedMore) << "prefix " << prefix;
  }
}

TEST(Frame, PartialPayloadNeedsMore) {
  const std::string payload(500, 'z');
  ByteBuffer buf = MakeFrame(OpCode::kFetch, 1, payload);
  FrameDecoder decoder;
  FrameDecoder::Frame frame;

  const auto full = buf.Readable();
  for (std::size_t prefix = kFrameHeaderSize; prefix < full.size(); ++prefix) {
    EXPECT_EQ(decoder.Next(full.subspan(0, prefix), frame), FrameDecoder::State::kNeedMore)
        << "prefix " << prefix;
  }
  EXPECT_EQ(decoder.Next(full, frame), FrameDecoder::State::kFrame);
}

TEST(Frame, ByteAtATimeDelivery) {
  // The pathological socket: one byte per read. The decoder must not consume,
  // mis-parse, or lose anything.
  ByteBuffer wire;
  for (int i = 0; i < 3; ++i) {
    EncodeFrame(wire,
                OpCode::kProduce,
                static_cast<RequestId>(i),
                0,
                AsBytes("record-" + std::to_string(i)));
  }

  FrameDecoder decoder;
  ByteBuffer incoming;
  int decoded = 0;
  const auto all = wire.Readable();
  for (std::size_t i = 0; i < all.size(); ++i) {
    incoming.Append(all.subspan(i, 1));
    FrameDecoder::Frame frame;
    while (decoder.Next(incoming.Readable(), frame) == FrameDecoder::State::kFrame) {
      EXPECT_EQ(frame.header.request_id, static_cast<RequestId>(decoded));
      EXPECT_EQ(AsStringView(frame.payload), "record-" + std::to_string(decoded));
      incoming.Consume(frame.total_size);
      ++decoded;
    }
  }
  EXPECT_EQ(decoded, 3);
  EXPECT_TRUE(incoming.Empty());
}

TEST(Frame, MultipleFramesInOneRead) {
  ByteBuffer wire;
  for (int i = 0; i < 10; ++i) {
    EncodeFrame(wire, OpCode::kProduce, static_cast<RequestId>(i), 0, AsBytes("abc"));
  }

  FrameDecoder decoder;
  ByteSpan remaining = wire.Readable();
  int decoded = 0;
  FrameDecoder::Frame frame;
  while (decoder.Next(remaining, frame) == FrameDecoder::State::kFrame) {
    EXPECT_EQ(frame.header.request_id, static_cast<RequestId>(decoded));
    remaining = remaining.subspan(frame.total_size);
    ++decoded;
  }
  EXPECT_EQ(decoded, 10);
}

TEST(Frame, BytesNeededGuidesTheNextRead) {
  const std::string payload(1000, 'q');
  ByteBuffer buf = MakeFrame(OpCode::kFetch, 1, payload);
  FrameDecoder decoder;

  EXPECT_EQ(decoder.BytesNeeded(ByteSpan{}), kFrameHeaderSize);
  EXPECT_EQ(decoder.BytesNeeded(buf.Readable().subspan(0, 10)), kFrameHeaderSize - 10);
  EXPECT_EQ(decoder.BytesNeeded(buf.Readable().subspan(0, kFrameHeaderSize)), payload.size());
  EXPECT_EQ(decoder.BytesNeeded(buf.Readable()), 0U);
}

// --- Malformed input -------------------------------------------------------

TEST(Frame, BadMagicIsRejected) {
  ByteBuffer buf = MakeFrame(OpCode::kProduce, 1, "data");
  std::vector<std::uint8_t> bytes(buf.Readable().begin(), buf.Readable().end());
  bytes[0] = 'X';

  FrameDecoder decoder;
  FrameDecoder::Frame frame;
  EXPECT_EQ(decoder.Next(bytes, frame), FrameDecoder::State::kError);
  EXPECT_EQ(decoder.error().code(), ErrorCode::kProtocolError);
}

TEST(Frame, CorruptHeaderIsDetectedBeforeLengthIsTrusted) {
  ByteBuffer buf = MakeFrame(OpCode::kProduce, 1, "data");
  std::vector<std::uint8_t> bytes(buf.Readable().begin(), buf.Readable().end());
  // Corrupt the payload length itself. Without a header CRC this would make
  // the decoder wait for (or accept) the wrong number of bytes forever.
  bytes[16] ^= 0xFF;

  FrameDecoder decoder;
  FrameDecoder::Frame frame;
  EXPECT_EQ(decoder.Next(bytes, frame), FrameDecoder::State::kError);
  EXPECT_EQ(decoder.error().code(), ErrorCode::kCorruption);
}

TEST(Frame, CorruptPayloadIsDetected) {
  ByteBuffer buf = MakeFrame(OpCode::kProduce, 1, "important data");
  std::vector<std::uint8_t> bytes(buf.Readable().begin(), buf.Readable().end());
  bytes[kFrameHeaderSize + 2] ^= 0x01;

  FrameDecoder decoder;
  FrameDecoder::Frame frame;
  EXPECT_EQ(decoder.Next(bytes, frame), FrameDecoder::State::kError);
  EXPECT_EQ(decoder.error().code(), ErrorCode::kCorruption);
}

TEST(Frame, UnknownOpcodeIsRejected) {
  FrameHeader header;
  header.opcode = static_cast<OpCode>(9999);
  std::vector<std::uint8_t> bytes(kFrameHeaderSize);
  EncodeFrameHeader(header, bytes.data());

  const auto decoded = DecodeFrameHeader(bytes.data(), kMaxPayloadBytesDefault);
  ASSERT_FALSE(decoded.ok());
  EXPECT_EQ(decoded.status().code(), ErrorCode::kProtocolError);
}

TEST(Frame, UnsupportedVersionIsRejected) {
  FrameHeader header;
  header.opcode = OpCode::kHealth;
  header.version = 99;
  std::vector<std::uint8_t> bytes(kFrameHeaderSize);
  EncodeFrameHeader(header, bytes.data());

  const auto decoded = DecodeFrameHeader(bytes.data(), kMaxPayloadBytesDefault);
  ASSERT_FALSE(decoded.ok());
  EXPECT_EQ(decoded.status().code(), ErrorCode::kUnsupportedVersion);
}

TEST(Frame, ReservedBitsMustBeZero) {
  ByteBuffer buf = MakeFrame(OpCode::kHealth, 1, "");
  std::vector<std::uint8_t> bytes(buf.Readable().begin(), buf.Readable().end());
  bytes[22] = 0x01;
  // Repair the header CRC so the reserved-bit check is what fires, not the CRC.
  StoreLe<std::uint32_t>(bytes.data() + 28, Crc32c(bytes.data(), 28));

  const auto decoded = DecodeFrameHeader(bytes.data(), kMaxPayloadBytesDefault);
  ASSERT_FALSE(decoded.ok());
  EXPECT_EQ(decoded.status().code(), ErrorCode::kProtocolError);
}

TEST(Frame, OversizedPayloadIsRejectedBeforeAllocation) {
  FrameHeader header;
  header.opcode = OpCode::kProduce;
  header.payload_len = 900U * 1024 * 1024;
  std::vector<std::uint8_t> bytes(kFrameHeaderSize);
  EncodeFrameHeader(header, bytes.data());

  const auto decoded = DecodeFrameHeader(bytes.data(), kMaxPayloadBytesDefault);
  ASSERT_FALSE(decoded.ok());
  EXPECT_EQ(decoded.status().code(), ErrorCode::kResourceExhausted);
}

TEST(Frame, DecoderStaysInErrorState) {
  std::vector<std::uint8_t> garbage(kFrameHeaderSize * 2, 0xAB);
  FrameDecoder decoder;
  FrameDecoder::Frame frame;
  EXPECT_EQ(decoder.Next(garbage, frame), FrameDecoder::State::kError);
  // A stream protocol has no safe resynchronisation point, so once the decoder
  // has seen garbage it must keep saying so until explicitly reset.
  ByteBuffer good = MakeFrame(OpCode::kHealth, 1, "");
  EXPECT_EQ(decoder.Next(good.Readable(), frame), FrameDecoder::State::kError);
  decoder.Reset();
  EXPECT_EQ(decoder.Next(good.Readable(), frame), FrameDecoder::State::kFrame);
}

TEST(Frame, RandomBytesNeverCrashOrSucceedSpuriously) {
  // Every 32-byte window of random data must be rejected. The chance of a
  // random CRC-32C match is 2^-32 per attempt, so a failure here means a real
  // validation gap, not bad luck.
  std::mt19937 rng(12345);
  std::vector<std::uint8_t> noise(kFrameHeaderSize + 64);
  int accepted = 0;
  for (int trial = 0; trial < 2000; ++trial) {
    for (auto& b : noise) b = static_cast<std::uint8_t>(rng());
    FrameDecoder decoder;
    FrameDecoder::Frame frame;
    if (decoder.Next(noise, frame) == FrameDecoder::State::kFrame) ++accepted;
  }
  EXPECT_EQ(accepted, 0);
}

}  // namespace
}  // namespace pulselog::protocol
