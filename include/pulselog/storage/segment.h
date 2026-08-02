// One segment of a partition log: a data file plus its offset and time
// indexes.
//
// Files are named after the segment's base offset, zero-padded to 20 digits so
// lexical order equals numeric order:
//
//   00000000000000000000.log     records
//   00000000000000000000.index   sparse offset index
//   00000000000000000000.tindex  sparse timestamp index
//
// Threading
// ---------
// A segment has exactly one writer -- the worker thread that owns the
// partition. Readers (fetch handlers, replication) may run concurrently on
// other threads. That is safe because:
//   * appends only ever extend the file, never modify written bytes;
//   * `end_position_` is published with a release store after the write
//     completes, and readers acquire it before reading, so a reader can only
//     ever see fully-written bytes;
//   * the index is only read under the same acquire, and index entries are
//     appended before the position that would expose them.
// `Sync()` may be called from the flusher thread while the owner appends:
// fsync(2) on a descriptor is safe concurrently with pwrite(2) on the same
// descriptor, and it flushes whatever had been written when it started.
#ifndef PULSELOG_STORAGE_SEGMENT_H_
#define PULSELOG_STORAGE_SEGMENT_H_

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string>

#include "pulselog/base/buffer.h"
#include "pulselog/base/status.h"
#include "pulselog/base/types.h"
#include "pulselog/protocol/record.h"
#include "pulselog/storage/file_util.h"
#include "pulselog/storage/offset_index.h"

namespace pulselog::storage {

struct SegmentOptions {
  std::int64_t max_bytes = 128LL * 1024 * 1024;
  std::int64_t index_interval_bytes = 4096;
  bool preallocate = true;
  WriteMode write_mode = WriteMode::kWrite;
  SyncMode sync_mode = SyncMode::kFull;
};

// What a recovery scan found.
//
// `truncated` means real data was discarded -- a torn write or corruption.
// Trimming the unwritten remainder of a preallocated segment is routine and is
// reported separately as `preallocated_bytes`, because conflating the two
// would make every clean restart look like a data-loss event.
struct RecoveryReport {
  Offset next_offset = 0;         // First unused offset.
  std::uint64_t valid_bytes = 0;  // Bytes that survived validation.
  std::uint64_t truncated_bytes = 0;
  std::uint64_t preallocated_bytes = 0;
  std::uint64_t records_scanned = 0;
  bool truncated = false;  // True only when a damaged/torn tail was removed.
  std::string reason;      // Why truncation happened, for the log.
};

class Segment {
 public:
  Segment() = default;

  Segment(const Segment&) = delete;
  Segment& operator=(const Segment&) = delete;

  ~Segment() = default;

  // Opens or creates the segment based at `base_offset` inside `dir`.
  // Does not scan the contents -- call `Recover()` for that.
  [[nodiscard]] static Result<std::unique_ptr<Segment>> Open(const std::filesystem::path& dir,
                                                             Offset base_offset,
                                                             const SegmentOptions& options);

  // Scans forward from the last indexed position, validating every record.
  // Stops at the first record that is incomplete or fails its checksum and
  // truncates the file there. Returns what it found.
  //
  // Scanning from the last index entry rather than from byte 0 bounds recovery
  // work to `index_interval_bytes` in the common case where the index is
  // intact.
  [[nodiscard]] Result<RecoveryReport> Recover(bool full_scan);

  // --- writer thread only ---------------------------------------------------

  // Appends already-encoded records whose offsets are already correct.
  // `record_count` and `last_offset` must describe the buffer; the segment
  // trusts them (the caller has just built or validated the bytes).
  [[nodiscard]] Status AppendEncoded(ByteSpan records,
                                     Offset base_offset,
                                     Offset last_offset,
                                     std::uint32_t record_count,
                                     TimestampMs max_timestamp);

  // Same, from several non-contiguous buffers written with one writev(2).
  [[nodiscard]] Status AppendEncodedVectored(std::span<const ByteSpan> chunks,
                                             Offset base_offset,
                                             Offset last_offset,
                                             std::uint32_t record_count,
                                             TimestampMs max_timestamp);

  // Removes everything from `offset` onward. Used by follower log truncation
  // and by recovery.
  [[nodiscard]] Status TruncateFrom(Offset offset);

  // --- any thread -----------------------------------------------------------

  // Copies the records covering `[from, …]` into `out`, up to `max_bytes`,
  // always stopping on a record boundary. Returns the number of records and
  // the offset of the first one.
  struct ReadResult {
    Offset base_offset = kInvalidOffset;
    std::uint32_t record_count = 0;
    std::size_t bytes = 0;
    bool reached_end = false;
  };

  [[nodiscard]] Result<ReadResult> Read(Offset from, std::size_t max_bytes, ByteBuffer& out) const;

  // Resolves an offset to a file position via the index plus a bounded scan.
  [[nodiscard]] Result<std::uint64_t> PositionFor(Offset offset) const;

  [[nodiscard]] Status Sync();

  [[nodiscard]] Status Close();

  // Deletes the segment's three files. The segment must already be closed.
  [[nodiscard]] Status Delete();

  [[nodiscard]] Offset base_offset() const noexcept { return base_offset_; }

  // First offset not yet written.
  [[nodiscard]] Offset next_offset() const noexcept {
    return next_offset_.load(std::memory_order_acquire);
  }

  [[nodiscard]] std::uint64_t SizeBytes() const noexcept {
    return end_position_.load(std::memory_order_acquire);
  }

  [[nodiscard]] std::uint64_t SyncedBytes() const noexcept {
    return synced_position_.load(std::memory_order_acquire);
  }

  [[nodiscard]] bool IsFull(std::int64_t max_bytes) const noexcept {
    return static_cast<std::int64_t>(SizeBytes()) >= max_bytes;
  }

  [[nodiscard]] TimestampMs MaxTimestamp() const noexcept {
    return max_timestamp_.load(std::memory_order_relaxed);
  }

  [[nodiscard]] TimestampMs CreatedAtMs() const noexcept { return created_at_ms_; }

  [[nodiscard]] std::uint64_t RecordCount() const noexcept {
    return record_count_.load(std::memory_order_relaxed);
  }

  [[nodiscard]] const std::filesystem::path& log_path() const noexcept { return log_path_; }

  // First offset with `timestamp >= target` inside this segment, if any.
  [[nodiscard]] std::optional<Offset> OffsetForTimestamp(TimestampMs target) const;

  // Formats a base offset as the 20-digit file stem used on disk.
  [[nodiscard]] static std::string FormatBaseOffset(Offset base_offset);

  // Inverse of FormatBaseOffset; returns nullopt when the stem is not a
  // 20-digit number.
  [[nodiscard]] static std::optional<Offset> ParseBaseOffset(const std::string& stem);

 private:
  [[nodiscard]] Status FinishAppend(std::size_t bytes,
                                    Offset base_offset,
                                    Offset last_offset,
                                    std::uint32_t record_count,
                                    TimestampMs max_timestamp);

  std::filesystem::path log_path_;
  std::filesystem::path index_path_;
  std::filesystem::path time_index_path_;

  FileHandle file_;
  OffsetIndex index_;
  TimeIndex time_index_;
  SegmentOptions options_;

  Offset base_offset_ = 0;
  TimestampMs created_at_ms_ = 0;

  // Published with release; read with acquire. Together they define exactly
  // how much of the file a concurrent reader may look at.
  std::atomic<std::uint64_t> end_position_{0};
  std::atomic<Offset> next_offset_{0};

  std::atomic<std::uint64_t> synced_position_{0};
  std::atomic<TimestampMs> max_timestamp_{-1};
  std::atomic<std::uint64_t> record_count_{0};

  // Writer-thread-only state.
  std::uint64_t last_indexed_position_ = 0;
  std::uint64_t preallocated_to_ = 0;
};

}  // namespace pulselog::storage

#endif  // PULSELOG_STORAGE_SEGMENT_H_
