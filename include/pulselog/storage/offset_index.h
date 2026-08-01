// Sparse offset and timestamp indexes for one segment.
//
// Both indexes are *sparse*: one entry roughly every `index_interval_bytes` of
// log (4 KiB by default), not one entry per record. A 128 MiB segment
// therefore needs about 32 KiB of offset index. That is small enough to keep
// resident, so the index is loaded into a vector once at open and appended to
// a file as it grows -- no mmap, no page faults on the lookup path.
//
// Lookup is a binary search for the largest entry with `offset <= target`,
// giving a file position to start a short forward scan from. The scan reads at
// most `index_interval_bytes` of log, bounded by construction.
//
// Entries are fixed 8-byte pairs of 32-bit values, both *relative* to the
// segment's base: relative offset and file position. Relative values keep the
// entries at 8 bytes instead of 16, which halves the index and doubles the
// number of entries per cache line during the binary search. It also caps a
// segment at 4 GiB and 2^32 records, both far above the configured limits.
#ifndef PULSELOG_STORAGE_OFFSET_INDEX_H_
#define PULSELOG_STORAGE_OFFSET_INDEX_H_

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <vector>

#include "pulselog/base/status.h"
#include "pulselog/base/types.h"
#include "pulselog/storage/file_util.h"

namespace pulselog::storage {

inline constexpr std::size_t kIndexEntryBytes = 8;

struct OffsetIndexEntry {
  std::uint32_t relative_offset = 0;
  std::uint32_t file_position = 0;
};

class OffsetIndex {
 public:
  OffsetIndex() = default;

  OffsetIndex(const OffsetIndex&) = delete;
  OffsetIndex& operator=(const OffsetIndex&) = delete;
  OffsetIndex(OffsetIndex&&) noexcept = default;
  OffsetIndex& operator=(OffsetIndex&&) noexcept = default;

  // Opens (creating if needed) the index for the segment based at
  // `base_offset`. A trailing partial entry -- possible after a crash -- is
  // discarded and the file truncated to a whole number of entries.
  [[nodiscard]] Status Open(const std::filesystem::path& path, Offset base_offset);

  // Records that `offset` begins at `position` in the segment file. Callers
  // invoke this only when at least `interval_bytes` have accumulated since the
  // previous entry; the index itself does not enforce spacing.
  [[nodiscard]] Status Append(Offset offset, std::uint64_t position);

  // Largest indexed entry with `entry.offset <= target`. Returns nullopt when
  // `target` precedes the first entry (i.e. scan from position 0).
  [[nodiscard]] std::optional<OffsetIndexEntry> LookupFloor(Offset target) const;

  // Drops every entry at or after `offset`. Used when recovery truncates a
  // damaged tail.
  [[nodiscard]] Status TruncateFrom(Offset offset);

  [[nodiscard]] Status Sync() const;

  void Close();

  [[nodiscard]] std::size_t EntryCount() const noexcept { return entries_.size(); }

  [[nodiscard]] Offset base_offset() const noexcept { return base_offset_; }

  [[nodiscard]] std::uint64_t BytesOnDisk() const noexcept {
    return entries_.size() * kIndexEntryBytes;
  }

  // Position of the last indexed entry, or 0 when empty. Recovery starts its
  // scan here instead of at the beginning of the segment.
  [[nodiscard]] std::uint64_t LastIndexedPosition() const noexcept {
    return entries_.empty() ? 0 : entries_.back().file_position;
  }

  [[nodiscard]] Offset LastIndexedOffset() const noexcept {
    return entries_.empty() ? base_offset_
                            : base_offset_ + static_cast<Offset>(entries_.back().relative_offset);
  }

 private:
  std::filesystem::path path_;
  FileHandle file_;
  std::vector<OffsetIndexEntry> entries_;
  Offset base_offset_ = 0;
};

struct TimeIndexEntry {
  TimestampMs timestamp = 0;
  std::uint32_t relative_offset = 0;
};

// Sparse timestamp -> offset index, used only by LIST_OFFSETS with a
// timestamp. Entries are appended with monotonically non-decreasing
// timestamps; an out-of-order timestamp is ignored rather than breaking the
// search invariant (producers can and do send skewed clocks).
class TimeIndex {
 public:
  TimeIndex() = default;

  TimeIndex(const TimeIndex&) = delete;
  TimeIndex& operator=(const TimeIndex&) = delete;
  TimeIndex(TimeIndex&&) noexcept = default;
  TimeIndex& operator=(TimeIndex&&) noexcept = default;

  [[nodiscard]] Status Open(const std::filesystem::path& path, Offset base_offset);

  [[nodiscard]] Status Append(TimestampMs timestamp, Offset offset);

  // First offset whose timestamp is >= `timestamp`, or nullopt when every
  // indexed record is older.
  [[nodiscard]] std::optional<Offset> LookupCeiling(TimestampMs timestamp) const;

  [[nodiscard]] Status TruncateFrom(Offset offset);

  [[nodiscard]] Status Sync() const;

  void Close();

  [[nodiscard]] std::size_t EntryCount() const noexcept { return entries_.size(); }

  [[nodiscard]] TimestampMs MaxTimestamp() const noexcept {
    return entries_.empty() ? -1 : entries_.back().timestamp;
  }

 private:
  std::filesystem::path path_;
  FileHandle file_;
  std::vector<TimeIndexEntry> entries_;
  Offset base_offset_ = 0;
};

}  // namespace pulselog::storage

#endif  // PULSELOG_STORAGE_OFFSET_INDEX_H_
