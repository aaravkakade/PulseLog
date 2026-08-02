// The record format, used identically on the wire and on disk.
//
// There is exactly one record encoding in PulseLog. A produce request carries
// records without offsets (the broker has not assigned them yet); the log
// stores the same records with the offset field filled in; a fetch response
// ships those stored bytes back untouched. That means the broker never
// re-encodes a record to serve a read -- it copies a byte range out of the
// segment and frames it. (This is a copy through user space, not a zero-copy
// sendfile path. See docs/STORAGE_ENGINE.md.)
//
// Layout of one record:
//
//   offset  size  field        notes
//   ------  ----  -----------  -------------------------------------------
//        0     4  length       u32, bytes following this field
//        4     4  crc32c       over bytes [8, 4+length)
//        8     8  offset       i64, absolute; 0 in a produce request
//       16     8  timestamp    i64, milliseconds since the Unix epoch
//       24     1  attributes   bit 0-1 compression, bit 2 tombstone
//       25     …  key_len      varuint; 0 means null, otherwise len+1
//        …     …  value_len    varuint
//        …     …  key bytes
//        …     …  value bytes
//
// The fixed part is 25 bytes plus two varints, so a record with a 4-byte key
// and a 100-byte value occupies 25 + 1 + 1 + 4 + 100 = 131 bytes. The offset
// is stored explicitly rather than implied by position because it makes the
// log self-describing: recovery and a hex dump can both tell exactly which
// offsets survived a torn write without consulting the index.
#ifndef PULSELOG_PROTOCOL_RECORD_H_
#define PULSELOG_PROTOCOL_RECORD_H_

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>

#include "pulselog/base/buffer.h"
#include "pulselog/base/status.h"
#include "pulselog/base/types.h"

namespace pulselog::protocol {

// Bytes before the varint length fields.
inline constexpr std::size_t kRecordFixedPrefix = 25;

// Largest a single record may be. Beyond this a producer must split.
inline constexpr std::uint32_t kMaxRecordBytes = 16U * 1024 * 1024;

enum class RecordAttribute : std::uint8_t {
  kCompressionMask = 0x03,
  kTombstone = 0x04,  // Null value; retained by compaction as a delete marker.
};

// A record that has been parsed or is about to be written. `key` and `value`
// are views into a caller-owned buffer -- nothing here allocates, which is
// what keeps the produce path free of per-record allocation.
struct RecordView {
  Offset offset = kInvalidOffset;
  TimestampMs timestamp = 0;
  std::uint8_t attributes = 0;
  bool key_is_null = true;
  ByteSpan key;
  ByteSpan value;

  [[nodiscard]] bool tombstone() const noexcept {
    return (attributes & static_cast<std::uint8_t>(RecordAttribute::kTombstone)) != 0;
  }

  [[nodiscard]] std::string_view key_str() const noexcept { return AsStringView(key); }

  [[nodiscard]] std::string_view value_str() const noexcept { return AsStringView(value); }
};

// Exact encoded size of a record with the given key/value sizes.
[[nodiscard]] std::size_t EncodedRecordSize(bool key_is_null,
                                            std::size_t key_len,
                                            std::size_t value_len) noexcept;

// Appends one fully-formed record (including CRC) to `out`.
// Returns the number of bytes written.
std::size_t AppendRecord(ByteBuffer& out,
                         Offset offset,
                         TimestampMs timestamp,
                         std::uint8_t attributes,
                         bool key_is_null,
                         ByteSpan key,
                         ByteSpan value);

// Overwrites the offset field of a record already encoded at `record_start`
// and repairs the CRC. Used by the leader when it assigns offsets to a batch
// the producer sent with offset 0. The whole record is re-checksummed because
// CRC-32C cannot be patched incrementally for a mid-message edit.
void RewriteRecordOffset(std::uint8_t* record_start, std::size_t record_size, Offset offset);

// Same, but also replaces the timestamp. Used when a producer leaves the
// timestamp unset and the broker stamps log-append time.
void RewriteRecordHeader(std::uint8_t* record_start,
                         std::size_t record_size,
                         Offset offset,
                         TimestampMs timestamp);

// Reads a record's timestamp without validating anything else. The caller must
// already know the record is structurally sound.
[[nodiscard]] TimestampMs PeekRecordTimestamp(const std::uint8_t* record_start) noexcept;

// Parses the record beginning at `data[pos]`.
//
// `verify_crc` is a deliberate knob: the storage layer verifies on recovery
// and on follower apply, but a leader serving a fetch from its own segment
// does not re-verify every record on every read (the data was checksummed when
// written and is re-checked by recovery). The cost of that choice is stated in
// docs/STORAGE_ENGINE.md.
//
// On success, `next_pos` receives the offset of the byte after this record.
[[nodiscard]] Status ParseRecord(
    ByteSpan data, std::size_t pos, bool verify_crc, RecordView& out, std::size_t& next_pos);

// Reads the `length` prefix without validating anything else. Returns nullopt
// when fewer than 4 bytes remain. Used by the recovery scanner to decide
// whether a whole record is present before parsing it.
[[nodiscard]] std::optional<std::uint32_t> PeekRecordLength(ByteSpan data,
                                                            std::size_t pos) noexcept;

// Forward iterator over a contiguous run of records (a segment range or a
// fetch response body). Stops at the first malformed record and reports why.
class RecordIterator {
 public:
  RecordIterator(ByteSpan data, bool verify_crc) : data_(data), verify_crc_(verify_crc) {}

  // Returns false when the run is exhausted or a record failed to parse;
  // check `status()` to distinguish the two.
  [[nodiscard]] bool Next(RecordView& out);

  [[nodiscard]] const Status& status() const noexcept { return status_; }

  [[nodiscard]] std::size_t Position() const noexcept { return pos_; }

  [[nodiscard]] std::size_t RecordsRead() const noexcept { return records_read_; }

 private:
  ByteSpan data_;
  bool verify_crc_;
  std::size_t pos_ = 0;
  std::size_t records_read_ = 0;
  Status status_;
};

}  // namespace pulselog::protocol

#endif  // PULSELOG_PROTOCOL_RECORD_H_
