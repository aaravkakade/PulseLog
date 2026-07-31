#include "pulselog/protocol/record.h"

#include <cstring>

#include "pulselog/base/crc32c.h"
#include "pulselog/base/endian.h"

namespace pulselog::protocol {
namespace {

constexpr std::size_t kOffLength = 0;
constexpr std::size_t kOffCrc = 4;
constexpr std::size_t kOffOffset = 8;
constexpr std::size_t kOffTimestamp = 16;
constexpr std::size_t kOffAttributes = 24;
constexpr std::size_t kVarintStart = 25;

static_assert(kVarintStart == kRecordFixedPrefix, "record prefix layout drifted");

[[nodiscard]] std::size_t VarUIntSize(std::uint64_t v) noexcept {
  std::size_t n = 1;
  while (v >= 0x80U) {
    v >>= 7U;
    ++n;
  }
  return n;
}

std::size_t WriteVarUInt(std::uint8_t* dst, std::uint64_t v) noexcept {
  std::size_t n = 0;
  while (v >= 0x80U) {
    dst[n++] = static_cast<std::uint8_t>(v) | 0x80U;
    v >>= 7U;
  }
  dst[n++] = static_cast<std::uint8_t>(v);
  return n;
}

// Returns the number of bytes consumed, or 0 on malformed input.
[[nodiscard]] std::size_t ReadVarUInt(ByteSpan data, std::size_t pos, std::uint64_t& out) noexcept {
  std::uint64_t value = 0;
  std::uint32_t shift = 0;
  std::size_t consumed = 0;
  while (shift <= 63 && pos + consumed < data.size()) {
    const std::uint8_t byte = data[pos + consumed];
    ++consumed;
    value |= static_cast<std::uint64_t>(byte & 0x7FU) << shift;
    if ((byte & 0x80U) == 0) {
      out = value;
      return consumed;
    }
    shift += 7;
  }
  return 0;
}

}  // namespace

std::size_t EncodedRecordSize(bool key_is_null, std::size_t key_len,
                              std::size_t value_len) noexcept {
  const std::uint64_t key_field = key_is_null ? 0 : static_cast<std::uint64_t>(key_len) + 1;
  return kRecordFixedPrefix + VarUIntSize(key_field) +
         VarUIntSize(static_cast<std::uint64_t>(value_len)) + (key_is_null ? 0 : key_len) +
         value_len;
}

std::size_t AppendRecord(ByteBuffer& out, Offset offset, TimestampMs timestamp,
                         std::uint8_t attributes, bool key_is_null, ByteSpan key, ByteSpan value) {
  const std::size_t total =
      EncodedRecordSize(key_is_null, key.size(), value.size());
  out.EnsureWritable(total);
  std::uint8_t* dst = out.WritePtr();

  StoreLe<std::uint32_t>(dst + kOffLength, static_cast<std::uint32_t>(total - 4));
  StoreLeI64(dst + kOffOffset, offset);
  StoreLeI64(dst + kOffTimestamp, timestamp);
  dst[kOffAttributes] = attributes;

  std::size_t pos = kVarintStart;
  const std::uint64_t key_field = key_is_null ? 0 : static_cast<std::uint64_t>(key.size()) + 1;
  pos += WriteVarUInt(dst + pos, key_field);
  pos += WriteVarUInt(dst + pos, static_cast<std::uint64_t>(value.size()));
  if (!key_is_null && !key.empty()) {
    std::memcpy(dst + pos, key.data(), key.size());
    pos += key.size();
  }
  if (!value.empty()) {
    std::memcpy(dst + pos, value.data(), value.size());
    pos += value.size();
  }

  // CRC covers everything after the CRC field itself: offset, timestamp,
  // attributes, lengths, key and value.
  const std::uint32_t crc = Crc32c(dst + kOffOffset, total - kOffOffset);
  StoreLe<std::uint32_t>(dst + kOffCrc, crc);

  out.Commit(total);
  return total;
}

void RewriteRecordOffset(std::uint8_t* record_start, std::size_t record_size, Offset offset) {
  StoreLeI64(record_start + kOffOffset, offset);
  const std::uint32_t crc = Crc32c(record_start + kOffOffset, record_size - kOffOffset);
  StoreLe<std::uint32_t>(record_start + kOffCrc, crc);
}

std::optional<std::uint32_t> PeekRecordLength(ByteSpan data, std::size_t pos) noexcept {
  if (pos + 4 > data.size()) return std::nullopt;
  return LoadLe<std::uint32_t>(data.data() + pos);
}

Status ParseRecord(ByteSpan data, std::size_t pos, bool verify_crc, RecordView& out,
                   std::size_t& next_pos) {
  if (pos + kRecordFixedPrefix > data.size()) {
    return OutOfRange("truncated record header at byte " + std::to_string(pos));
  }

  const std::uint32_t length = LoadLe<std::uint32_t>(data.data() + pos + kOffLength);
  if (length < kRecordFixedPrefix - 4) {
    return Corruption("record length " + std::to_string(length) + " is below the minimum");
  }
  if (length > kMaxRecordBytes) {
    return Corruption("record length " + std::to_string(length) + " exceeds the maximum");
  }
  const std::size_t total = static_cast<std::size_t>(length) + 4;
  if (pos + total > data.size()) {
    return OutOfRange("record at byte " + std::to_string(pos) + " claims " +
                      std::to_string(total) + " bytes but only " +
                      std::to_string(data.size() - pos) + " remain");
  }

  const std::uint8_t* base = data.data() + pos;
  if (verify_crc) {
    const std::uint32_t stored = LoadLe<std::uint32_t>(base + kOffCrc);
    const std::uint32_t computed = Crc32c(base + kOffOffset, total - kOffOffset);
    if (stored != computed) {
      return Corruption("record checksum mismatch at byte " + std::to_string(pos));
    }
  }

  out.offset = LoadLeI64(base + kOffOffset);
  out.timestamp = LoadLeI64(base + kOffTimestamp);
  out.attributes = base[kOffAttributes];

  std::size_t cursor = pos + kVarintStart;
  std::uint64_t key_field = 0;
  std::size_t consumed = ReadVarUInt(data, cursor, key_field);
  if (consumed == 0) return Corruption("malformed key length varint");
  cursor += consumed;

  std::uint64_t value_len = 0;
  consumed = ReadVarUInt(data, cursor, value_len);
  if (consumed == 0) return Corruption("malformed value length varint");
  cursor += consumed;

  out.key_is_null = (key_field == 0);
  const std::uint64_t key_len = out.key_is_null ? 0 : key_field - 1;

  // Every length is validated against the record's own declared end, so a
  // corrupt varint cannot produce a span pointing outside the buffer.
  const std::size_t record_end = pos + total;
  if (cursor + key_len > record_end || cursor + key_len + value_len > record_end) {
    return Corruption("record key/value lengths overflow the record at byte " +
                      std::to_string(pos));
  }
  if (cursor + key_len + value_len != record_end) {
    return Corruption("record has " + std::to_string(record_end - cursor - key_len - value_len) +
                      " trailing bytes at byte " + std::to_string(pos));
  }

  out.key = out.key_is_null ? ByteSpan{} : data.subspan(cursor, key_len);
  cursor += key_len;
  out.value = data.subspan(cursor, value_len);

  next_pos = record_end;
  return OkStatus();
}

bool RecordIterator::Next(RecordView& out) {
  if (!status_.ok()) return false;
  if (pos_ >= data_.size()) return false;

  std::size_t next = 0;
  Status status = ParseRecord(data_, pos_, verify_crc_, out, next);
  if (!status.ok()) {
    status_ = std::move(status);
    return false;
  }
  pos_ = next;
  ++records_read_;
  return true;
}

}  // namespace pulselog::protocol
