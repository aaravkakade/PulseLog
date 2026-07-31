#include "pulselog/protocol/frame.h"

#include <cstring>

#include "pulselog/base/crc32c.h"
#include "pulselog/base/endian.h"

namespace pulselog::protocol {
namespace {

constexpr std::size_t kOffMagic = 0;
constexpr std::size_t kOffVersion = 4;
constexpr std::size_t kOffOpcode = 6;
constexpr std::size_t kOffRequestId = 8;
constexpr std::size_t kOffPayloadLen = 16;
constexpr std::size_t kOffFlags = 20;
constexpr std::size_t kOffReserved = 22;
constexpr std::size_t kOffPayloadCrc = 24;
constexpr std::size_t kOffHeaderCrc = 28;

static_assert(kOffHeaderCrc + 4 == kFrameHeaderSize, "header layout drifted");

}  // namespace

void EncodeFrameHeader(const FrameHeader& header, std::uint8_t* dst) noexcept {
  StoreLe<std::uint32_t>(dst + kOffMagic, kFrameMagic);
  StoreLe<std::uint16_t>(dst + kOffVersion, header.version);
  StoreLe<std::uint16_t>(dst + kOffOpcode, static_cast<std::uint16_t>(header.opcode));
  StoreLe<std::uint64_t>(dst + kOffRequestId, header.request_id);
  StoreLe<std::uint32_t>(dst + kOffPayloadLen, header.payload_len);
  StoreLe<std::uint16_t>(dst + kOffFlags, header.flags);
  StoreLe<std::uint16_t>(dst + kOffReserved, 0);
  StoreLe<std::uint32_t>(dst + kOffPayloadCrc, header.payload_crc);
  const std::uint32_t header_crc = Crc32c(dst, kOffHeaderCrc);
  StoreLe<std::uint32_t>(dst + kOffHeaderCrc, header_crc);
}

Result<FrameHeader> DecodeFrameHeader(const std::uint8_t* src, std::uint32_t max_payload) {
  const std::uint32_t magic = LoadLe<std::uint32_t>(src + kOffMagic);
  if (magic != kFrameMagic) {
    return ProtocolError("bad frame magic: expected 0x" + std::to_string(kFrameMagic) + ", got 0x" +
                         std::to_string(magic));
  }

  // The header CRC is checked before any other field is trusted. In
  // particular, acting on a corrupt payload_len would desynchronise the
  // stream permanently.
  const std::uint32_t stored_crc = LoadLe<std::uint32_t>(src + kOffHeaderCrc);
  const std::uint32_t computed_crc = Crc32c(src, kOffHeaderCrc);
  if (stored_crc != computed_crc) {
    return Status{ErrorCode::kCorruption, "frame header checksum mismatch"};
  }

  const std::uint16_t reserved = LoadLe<std::uint16_t>(src + kOffReserved);
  if (reserved != 0) {
    // Reserved bits are how a future version signals a change that an old
    // decoder must not silently ignore.
    return ProtocolError("reserved header bits must be zero (got " + std::to_string(reserved) +
                         ")");
  }

  FrameHeader header;
  header.version = LoadLe<std::uint16_t>(src + kOffVersion);
  if (header.version < kMinSupportedVersion || header.version > kProtocolVersion) {
    return Status{ErrorCode::kUnsupportedVersion,
                  "unsupported protocol version " + std::to_string(header.version) +
                      " (this build speaks " + std::to_string(kMinSupportedVersion) + ".." +
                      std::to_string(kProtocolVersion) + ")"};
  }

  const std::uint16_t raw_opcode = LoadLe<std::uint16_t>(src + kOffOpcode);
  if (!IsKnownOpCode(raw_opcode)) {
    return ProtocolError("unknown opcode " + std::to_string(raw_opcode));
  }
  header.opcode = static_cast<OpCode>(raw_opcode);

  header.request_id = LoadLe<std::uint64_t>(src + kOffRequestId);
  header.payload_len = LoadLe<std::uint32_t>(src + kOffPayloadLen);
  if (header.payload_len > max_payload) {
    return Status{ErrorCode::kResourceExhausted,
                  "payload length " + std::to_string(header.payload_len) + " exceeds limit " +
                      std::to_string(max_payload)};
  }

  header.flags = LoadLe<std::uint16_t>(src + kOffFlags);
  header.payload_crc = LoadLe<std::uint32_t>(src + kOffPayloadCrc);
  return header;
}

void EncodeFrame(ByteBuffer& out, const FrameHeader& header, ByteSpan payload) {
  FrameHeader complete = header;
  complete.payload_len = static_cast<std::uint32_t>(payload.size());
  complete.payload_crc = payload.empty() ? 0U : Crc32c(payload);

  out.EnsureWritable(kFrameHeaderSize + payload.size());
  EncodeFrameHeader(complete, out.WritePtr());
  out.Commit(kFrameHeaderSize);
  if (!payload.empty()) out.Append(payload);
}

void EncodeFrame(ByteBuffer& out, OpCode opcode, RequestId request_id, std::uint16_t flags,
                 ByteSpan payload) {
  FrameHeader header;
  header.opcode = opcode;
  header.request_id = request_id;
  header.flags = flags;
  EncodeFrame(out, header, payload);
}

FrameDecoder::State FrameDecoder::Next(ByteSpan input, Frame& out) {
  if (!error_.ok()) return State::kError;
  if (input.size() < kFrameHeaderSize) return State::kNeedMore;

  auto header = DecodeFrameHeader(input.data(), max_payload_);
  if (!header.ok()) {
    error_ = header.status();
    return State::kError;
  }

  const std::size_t total = kFrameHeaderSize + header.value().payload_len;
  if (input.size() < total) return State::kNeedMore;

  const ByteSpan payload = input.subspan(kFrameHeaderSize, header.value().payload_len);
  if (!payload.empty()) {
    const std::uint32_t computed = Crc32c(payload);
    if (computed != header.value().payload_crc) {
      error_ = Status{ErrorCode::kCorruption,
                      "frame payload checksum mismatch (opcode " +
                          std::string(OpCodeName(header.value().opcode)) + ", " +
                          std::to_string(payload.size()) + " bytes)"};
      return State::kError;
    }
  } else if (header.value().payload_crc != 0) {
    error_ = ProtocolError("empty payload must carry a zero checksum");
    return State::kError;
  }

  out.header = header.value();
  out.payload = payload;
  out.total_size = total;
  return State::kFrame;
}

std::size_t FrameDecoder::BytesNeeded(ByteSpan input) const noexcept {
  if (input.size() < kFrameHeaderSize) return kFrameHeaderSize - input.size();
  const std::uint32_t payload_len = LoadLe<std::uint32_t>(input.data() + kOffPayloadLen);
  const std::size_t total = kFrameHeaderSize + payload_len;
  return input.size() >= total ? 0 : total - input.size();
}

}  // namespace pulselog::protocol
