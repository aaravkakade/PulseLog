// PulseLog frame layout.
//
// Every message on every connection -- client to broker, broker to broker --
// is a frame: a fixed 32-byte header followed by a variable-length payload.
// All integers are little-endian. See docs/PROTOCOL.md for the normative
// description; this header is the implementation of it.
//
//   offset  size  field         notes
//   ------  ----  ------------  ----------------------------------------------
//        0     4  magic         0x50 0x4C 0x53 0x47 ("PLSG") in stream order
//        4     2  version       protocol version, currently 1
//        6     2  opcode        see opcode.h
//        8     8  request_id    echoed verbatim in the response
//       16     4  payload_len   payload bytes following the header
//       20     2  flags         bit 0 = response, bit 1 = payload compressed
//       22     2  reserved      must be zero; rejected otherwise
//       24     4  payload_crc   CRC-32C of the payload (0 when payload_len==0)
//       28     4  header_crc    CRC-32C of bytes [0, 28)
//       32     …  payload
//
// Why the header carries its own CRC: a corrupt `payload_len` on a stream
// socket is unrecoverable -- the decoder would resynchronise at the wrong
// place and treat payload bytes as a header forever after. Validating the
// header before trusting its length field turns that into a clean connection
// reset. The payload CRC is separate so a large payload can be checksummed
// once, after it has fully arrived, rather than incrementally.
#ifndef PULSELOG_PROTOCOL_FRAME_H_
#define PULSELOG_PROTOCOL_FRAME_H_

#include <cstddef>
#include <cstdint>

#include "pulselog/base/buffer.h"
#include "pulselog/base/status.h"
#include "pulselog/protocol/opcode.h"

namespace pulselog::protocol {

inline constexpr std::size_t kFrameHeaderSize = 32;

// "PLSG" read as a little-endian u32 from the first four stream bytes.
inline constexpr std::uint32_t kFrameMagic = 0x47534C50U;

inline constexpr std::uint16_t kProtocolVersion = 1;

// The oldest version this build can decode. Bumped only on a breaking change.
inline constexpr std::uint16_t kMinSupportedVersion = 1;

// Hard ceiling on a single frame's payload. Enforced before any allocation, so
// a hostile or corrupt length field cannot drive the broker out of memory.
// Configurable downward per listener via `net.max_frame_bytes`.
inline constexpr std::uint32_t kMaxPayloadBytesDefault = 64U * 1024 * 1024;

enum class FrameFlags : std::uint16_t {
  kNone = 0,
  kResponse = 1U << 0U,     // Set on broker -> client frames.
  kCompressed = 1U << 1U,   // Payload is compressed; codec named inside.
  kMoreFollows = 1U << 2U,  // Part of a multi-frame response stream.
};

[[nodiscard]] inline std::uint16_t operator|(FrameFlags a, FrameFlags b) noexcept {
  return static_cast<std::uint16_t>(static_cast<std::uint16_t>(a) | static_cast<std::uint16_t>(b));
}

[[nodiscard]] inline bool HasFlag(std::uint16_t flags, FrameFlags f) noexcept {
  return (flags & static_cast<std::uint16_t>(f)) != 0;
}

struct FrameHeader {
  std::uint16_t version = kProtocolVersion;
  OpCode opcode = OpCode::kUnknown;
  RequestId request_id = 0;
  std::uint32_t payload_len = 0;
  std::uint16_t flags = 0;
  std::uint32_t payload_crc = 0;

  [[nodiscard]] bool is_response() const noexcept { return HasFlag(flags, FrameFlags::kResponse); }
};

// Serialises `header` into exactly kFrameHeaderSize bytes at `dst`.
// `dst` must have kFrameHeaderSize bytes available.
void EncodeFrameHeader(const FrameHeader& header, std::uint8_t* dst) noexcept;

// Validates and parses a header from `src` (kFrameHeaderSize bytes).
// Returns kProtocolError for bad magic, bad header CRC, unknown opcode,
// non-zero reserved bits, or a payload length above `max_payload`.
[[nodiscard]] Result<FrameHeader> DecodeFrameHeader(const std::uint8_t* src,
                                                    std::uint32_t max_payload);

// Writes a complete frame (header + payload) into `out`, computing both CRCs.
// The payload CRC is computed here, so callers must not mutate `payload`
// afterwards.
void EncodeFrame(ByteBuffer& out, const FrameHeader& header, ByteSpan payload);

// Convenience for the common case: build a frame from an already-encoded
// payload living in a scratch buffer.
void EncodeFrame(
    ByteBuffer& out, OpCode opcode, RequestId request_id, std::uint16_t flags, ByteSpan payload);

// Incremental frame reader for stream sockets.
//
// Feed it a connection's read buffer; it returns one complete, CRC-verified
// frame at a time and leaves partial data in place. It never copies the
// payload: `Frame::payload` points into the caller's buffer and stays valid
// until the caller consumes those bytes.
class FrameDecoder {
 public:
  struct Frame {
    FrameHeader header;
    ByteSpan payload;  // View into the caller's buffer.
    std::size_t total_size = 0;
  };

  enum class State : std::uint8_t {
    kNeedMore,  // Not enough bytes yet; keep reading from the socket.
    kFrame,     // A complete frame is available.
    kError,     // Unrecoverable: caller must close the connection.
  };

  explicit FrameDecoder(std::uint32_t max_payload = kMaxPayloadBytesDefault)
      : max_payload_(max_payload) {}

  // Attempts to decode one frame from the front of `input`.
  // On kFrame the caller must consume `out.total_size` bytes before calling
  // again. On kError, `error()` explains why and the connection must be
  // closed -- there is no safe resynchronisation point in a stream protocol.
  [[nodiscard]] State Next(ByteSpan input, Frame& out);

  [[nodiscard]] const Status& error() const noexcept { return error_; }

  // Bytes still needed to make progress. Lets the io loop size its next read
  // exactly instead of guessing.
  [[nodiscard]] std::size_t BytesNeeded(ByteSpan input) const noexcept;

  void Reset() noexcept { error_ = OkStatus(); }

 private:
  std::uint32_t max_payload_;
  Status error_;
};

}  // namespace pulselog::protocol

#endif  // PULSELOG_PROTOCOL_FRAME_H_
