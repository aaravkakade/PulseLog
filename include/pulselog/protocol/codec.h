// Payload encoding primitives.
//
// `PayloadWriter` appends to a ByteBuffer; `PayloadReader` reads from a span
// with bounds checking on every field. Reads never throw and never read past
// the end -- a truncated or hostile payload yields `false` and the caller
// turns that into a PROTOCOL_ERROR response.
//
// Integer widths are fixed rather than varint for header-like fields (offsets,
// partition indices) because those are read by the broker on every request and
// a predictable layout is worth more than a few bytes. Varints are used where
// the values are small and numerous (per-record key/value lengths).
#ifndef PULSELOG_PROTOCOL_CODEC_H_
#define PULSELOG_PROTOCOL_CODEC_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "pulselog/base/buffer.h"
#include "pulselog/base/endian.h"
#include "pulselog/base/types.h"

namespace pulselog::protocol {

// Strings are length-prefixed with a u16; the protocol has no NUL-terminated
// fields. 64 KiB is far above any legitimate topic or group name.
inline constexpr std::size_t kMaxStringLength = 65535;

class PayloadWriter {
 public:
  explicit PayloadWriter(ByteBuffer& out) : out_(out) {}

  void PutU8(std::uint8_t v) { out_.AppendByte(v); }

  void PutU16(std::uint16_t v) {
    out_.EnsureWritable(2);
    StoreLe<std::uint16_t>(out_.WritePtr(), v);
    out_.Commit(2);
  }

  void PutU32(std::uint32_t v) {
    out_.EnsureWritable(4);
    StoreLe<std::uint32_t>(out_.WritePtr(), v);
    out_.Commit(4);
  }

  void PutU64(std::uint64_t v) {
    out_.EnsureWritable(8);
    StoreLe<std::uint64_t>(out_.WritePtr(), v);
    out_.Commit(8);
  }

  void PutI16(std::int16_t v) { PutU16(static_cast<std::uint16_t>(v)); }

  void PutI32(std::int32_t v) { PutU32(static_cast<std::uint32_t>(v)); }

  void PutI64(std::int64_t v) { PutU64(static_cast<std::uint64_t>(v)); }

  void PutBool(bool v) { PutU8(v ? 1U : 0U); }

  // LEB128, 1-10 bytes.
  void PutVarUInt(std::uint64_t v) {
    out_.EnsureWritable(10);
    std::uint8_t* p = out_.WritePtr();
    std::size_t n = 0;
    while (v >= 0x80U) {
      p[n++] = static_cast<std::uint8_t>(v) | 0x80U;
      v >>= 7U;
    }
    p[n++] = static_cast<std::uint8_t>(v);
    out_.Commit(n);
  }

  void PutVarInt(std::int64_t v) { PutVarUInt(ZigZagEncode(v)); }

  void PutString(std::string_view s) {
    PutU16(static_cast<std::uint16_t>(s.size()));
    out_.Append(AsBytes(s));
  }

  // u32-prefixed opaque bytes. A length of 0xFFFFFFFF encodes "null".
  void PutBytes(ByteSpan b) {
    PutU32(static_cast<std::uint32_t>(b.size()));
    out_.Append(b);
  }

  void PutNullableBytes(const std::uint8_t* data, std::size_t size) {
    if (data == nullptr) {
      PutU32(0xFFFFFFFFU);
      return;
    }
    PutU32(static_cast<std::uint32_t>(size));
    out_.Append(data, size);
  }

  // Raw append with no length prefix; used when the length is implied by the
  // frame (e.g. a fetch response's trailing log bytes).
  void PutRaw(ByteSpan b) { out_.Append(b); }

  void PutArrayLen(std::size_t n) { PutU32(static_cast<std::uint32_t>(n)); }

  [[nodiscard]] std::size_t BytesWritten() const noexcept { return out_.ReadableBytes(); }

  [[nodiscard]] ByteBuffer& buffer() noexcept { return out_; }

 private:
  ByteBuffer& out_;
};

class PayloadReader {
 public:
  explicit PayloadReader(ByteSpan input) : data_(input) {}

  [[nodiscard]] bool GetU8(std::uint8_t& out) noexcept {
    if (Remaining() < 1) return Fail();
    out = data_[pos_++];
    return true;
  }

  [[nodiscard]] bool GetU16(std::uint16_t& out) noexcept {
    if (Remaining() < 2) return Fail();
    out = LoadLe<std::uint16_t>(data_.data() + pos_);
    pos_ += 2;
    return true;
  }

  [[nodiscard]] bool GetU32(std::uint32_t& out) noexcept {
    if (Remaining() < 4) return Fail();
    out = LoadLe<std::uint32_t>(data_.data() + pos_);
    pos_ += 4;
    return true;
  }

  [[nodiscard]] bool GetU64(std::uint64_t& out) noexcept {
    if (Remaining() < 8) return Fail();
    out = LoadLe<std::uint64_t>(data_.data() + pos_);
    pos_ += 8;
    return true;
  }

  [[nodiscard]] bool GetI16(std::int16_t& out) noexcept {
    std::uint16_t raw = 0;
    if (!GetU16(raw)) return false;
    out = static_cast<std::int16_t>(raw);
    return true;
  }

  [[nodiscard]] bool GetI32(std::int32_t& out) noexcept {
    std::uint32_t raw = 0;
    if (!GetU32(raw)) return false;
    out = static_cast<std::int32_t>(raw);
    return true;
  }

  [[nodiscard]] bool GetI64(std::int64_t& out) noexcept {
    std::uint64_t raw = 0;
    if (!GetU64(raw)) return false;
    out = static_cast<std::int64_t>(raw);
    return true;
  }

  [[nodiscard]] bool GetBool(bool& out) noexcept {
    std::uint8_t raw = 0;
    if (!GetU8(raw)) return false;
    out = raw != 0;
    return true;
  }

  [[nodiscard]] bool GetVarUInt(std::uint64_t& out) noexcept {
    std::uint64_t value = 0;
    std::uint32_t shift = 0;
    while (shift <= 63) {
      if (Remaining() < 1) return Fail();
      const std::uint8_t byte = data_[pos_++];
      value |= static_cast<std::uint64_t>(byte & 0x7FU) << shift;
      if ((byte & 0x80U) == 0) {
        out = value;
        return true;
      }
      shift += 7;
    }
    return Fail();  // More than 10 continuation bytes: malformed.
  }

  [[nodiscard]] bool GetVarInt(std::int64_t& out) noexcept {
    std::uint64_t raw = 0;
    if (!GetVarUInt(raw)) return false;
    out = ZigZagDecode(raw);
    return true;
  }

  // Returns a view into the underlying buffer -- no allocation, valid as long
  // as the caller's buffer is.
  [[nodiscard]] bool GetStringView(std::string_view& out) noexcept {
    std::uint16_t len = 0;
    if (!GetU16(len)) return false;
    if (Remaining() < len) return Fail();
    out = std::string_view(reinterpret_cast<const char*>(data_.data() + pos_), len);
    pos_ += len;
    return true;
  }

  [[nodiscard]] bool GetString(std::string& out) {
    std::string_view view;
    if (!GetStringView(view)) return false;
    out.assign(view);
    return true;
  }

  [[nodiscard]] bool GetBytesView(ByteSpan& out) noexcept {
    std::uint32_t len = 0;
    if (!GetU32(len)) return false;
    if (len == 0xFFFFFFFFU) {  // Null.
      out = ByteSpan{};
      return true;
    }
    if (Remaining() < len) return Fail();
    out = data_.subspan(pos_, len);
    pos_ += len;
    return true;
  }

  [[nodiscard]] bool GetArrayLen(std::uint32_t& out, std::uint32_t max_elements) noexcept {
    if (!GetU32(out)) return false;
    // A hostile array length must not drive a reserve() of gigabytes. The
    // caller supplies a bound derived from the remaining payload size.
    if (out > max_elements) return Fail();
    return true;
  }

  // Consumes the rest of the payload as an opaque view.
  [[nodiscard]] ByteSpan GetRemaining() noexcept {
    ByteSpan rest = data_.subspan(pos_);
    pos_ = data_.size();
    return rest;
  }

  [[nodiscard]] bool Skip(std::size_t n) noexcept {
    if (Remaining() < n) return Fail();
    pos_ += n;
    return true;
  }

  [[nodiscard]] std::size_t Remaining() const noexcept { return data_.size() - pos_; }

  [[nodiscard]] std::size_t Position() const noexcept { return pos_; }

  [[nodiscard]] bool ok() const noexcept { return !failed_; }

  // True when every byte was consumed and no read failed. Requests are
  // required to be fully consumed so trailing garbage is rejected rather than
  // silently ignored -- that keeps version skew loud.
  [[nodiscard]] bool Complete() const noexcept { return !failed_ && Remaining() == 0; }

 private:
  bool Fail() noexcept {
    failed_ = true;
    return false;
  }

  ByteSpan data_;
  std::size_t pos_ = 0;
  bool failed_ = false;
};

}  // namespace pulselog::protocol

#endif  // PULSELOG_PROTOCOL_CODEC_H_
