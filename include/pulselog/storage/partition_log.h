// The append-only log for one partition: an ordered list of segments plus the
// policies that roll, flush and delete them.
//
// Threading
// ---------
// Appends, truncation and retention run on the partition's owning worker
// thread. Reads and `Flush()` may run on other threads. The segment list is
// guarded by a shared_mutex taken in shared mode by readers and in exclusive
// mode only when the list itself changes (a roll or a deletion), which is rare.
// Within a segment, the reader/writer safety argument is in segment.h.
#ifndef PULSELOG_STORAGE_PARTITION_LOG_H_
#define PULSELOG_STORAGE_PARTITION_LOG_H_

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <shared_mutex>
#include <span>
#include <string>
#include <vector>

#include "pulselog/base/buffer.h"
#include "pulselog/base/status.h"
#include "pulselog/base/types.h"
#include "pulselog/storage/segment.h"

namespace pulselog::storage {

// When the broker makes data durable.
//
//   * `sync_on_append = true` fsyncs inside the append call. A leader
//     acknowledgement then means "on stable media", at the cost of a full
//     device round trip per batch.
//   * otherwise the flusher thread syncs when either threshold trips. A leader
//     acknowledgement means "in the page cache": it survives a process crash
//     but not a machine crash within the flush window.
// docs/FAILURE_SEMANTICS.md states exactly what each mode does and does not
// promise.
struct FlushPolicy {
  bool sync_on_append = false;
  std::int64_t interval_ms = 200;
  std::int64_t max_unflushed_bytes = 4LL * 1024 * 1024;
  std::int64_t max_unflushed_records = 10'000;
};

struct LogOptions {
  std::filesystem::path directory;
  std::int64_t segment_bytes = 128LL * 1024 * 1024;
  std::int64_t segment_ms = -1;  // Roll by age; -1 disables.
  std::int64_t index_interval_bytes = 4096;
  std::int64_t retention_bytes = -1;  // -1 = keep everything.
  std::int64_t retention_ms = -1;
  std::int64_t min_free_disk_bytes = 64LL * 1024 * 1024;
  bool preallocate = true;
  WriteMode write_mode = WriteMode::kWrite;
  SyncMode sync_mode = SyncMode::kFull;
  FlushPolicy flush;
};

struct AppendResult {
  Offset base_offset = kInvalidOffset;
  Offset last_offset = kInvalidOffset;
  std::uint32_t record_count = 0;
  std::size_t bytes = 0;
  TimestampMs append_time = 0;
  bool rolled_segment = false;
};

struct LogReadResult {
  Offset base_offset = kInvalidOffset;
  std::uint32_t record_count = 0;
  std::size_t bytes = 0;
  Offset log_start_offset = 0;
  Offset log_end_offset = 0;
};

struct LogStats {
  Offset log_start_offset = 0;
  Offset log_end_offset = 0;
  Offset flushed_offset = 0;
  std::uint64_t total_bytes = 0;
  std::uint64_t record_count = 0;
  std::size_t segment_count = 0;
  std::uint64_t flush_count = 0;
  std::uint64_t flush_nanos_total = 0;
  std::uint64_t flush_nanos_max = 0;
  std::uint64_t roll_count = 0;
  std::uint64_t deleted_segments = 0;
};

class PartitionLog {
 public:
  PartitionLog(const PartitionLog&) = delete;
  PartitionLog& operator=(const PartitionLog&) = delete;

  ~PartitionLog();

  // Opens the log in `options.directory`, recovering any existing segments.
  // Reports what recovery did through `recovery_out` when provided.
  [[nodiscard]] static Result<std::unique_ptr<PartitionLog>> Open(
      TopicPartition topic_partition, LogOptions options, RecoveryReport* recovery_out = nullptr);

  // --- writer thread --------------------------------------------------------

  // Leader path. `records` is a *mutable* view of records the producer sent
  // with offset 0; this assigns real offsets in place (rewriting each record's
  // offset field and checksum) and appends the buffer with a single write.
  // No copy is made.
  //
  // Records whose timestamp is <= 0 are stamped with the broker's append time.
  [[nodiscard]] Result<AppendResult> AppendAssigningOffsets(MutableByteSpan records,
                                                            std::uint32_t record_count);

  // Follower path. `records` already carry their final offsets; the first must
  // equal the log end offset. Every record's checksum is verified before it is
  // written, because these bytes came from another machine.
  [[nodiscard]] Result<AppendResult> AppendWithOffsets(ByteSpan records,
                                                       std::uint32_t record_count);

  // Appends several independent buffers with one writev(2). Used by the
  // broker's request-coalescing path. Each chunk must already carry correct
  // offsets and be contiguous with the previous one.
  [[nodiscard]] Result<AppendResult> AppendVectoredWithOffsets(std::span<const ByteSpan> chunks,
                                                               std::uint32_t record_count);

  // Removes everything at or after `offset`. Used when a follower discovers it
  // diverged from the leader.
  [[nodiscard]] Status TruncateTo(Offset offset);

  // Applies retention (size and age) and deletes eligible segments. Never
  // deletes the active segment.
  [[nodiscard]] Result<std::size_t> EnforceRetention();

  // Rolls to a new segment even if the current one is not full.
  [[nodiscard]] Status RollSegment();

  // --- any thread -----------------------------------------------------------

  [[nodiscard]] Result<LogReadResult> Read(Offset from,
                                           std::size_t max_bytes,
                                           ByteBuffer& out) const;

  // fsyncs everything not yet durable and advances `flushed_offset`.
  [[nodiscard]] Status Flush();

  // True when the flush policy's thresholds have been crossed.
  [[nodiscard]] bool NeedsFlush(std::int64_t now_ms) const;

  [[nodiscard]] Offset LogStartOffset() const noexcept {
    return log_start_offset_.load(std::memory_order_acquire);
  }

  [[nodiscard]] Offset LogEndOffset() const noexcept {
    return log_end_offset_.load(std::memory_order_acquire);
  }

  [[nodiscard]] Offset FlushedOffset() const noexcept {
    return flushed_offset_.load(std::memory_order_acquire);
  }

  // Resolves earliest/latest/by-timestamp to a concrete offset.
  [[nodiscard]] Result<Offset> OffsetForTimestamp(TimestampMs timestamp) const;

  [[nodiscard]] LogStats GetStats() const;

  [[nodiscard]] const TopicPartition& topic_partition() const noexcept { return topic_partition_; }

  [[nodiscard]] const LogOptions& options() const noexcept { return options_; }

  [[nodiscard]] Status Close();

 private:
  PartitionLog(TopicPartition topic_partition, LogOptions options);

  [[nodiscard]] Status LoadSegments(RecoveryReport* recovery_out);

  [[nodiscard]] Status MaybeRoll(std::size_t incoming_bytes);

  [[nodiscard]] Status CheckDiskSpace();

  // Caller must hold at least a shared lock.
  [[nodiscard]] Segment* SegmentForOffsetLocked(Offset offset) const;

  [[nodiscard]] Status FinalizeAppend(const AppendResult& result);

  TopicPartition topic_partition_;
  LogOptions options_;

  mutable std::shared_mutex segments_mutex_;
  std::vector<std::unique_ptr<Segment>> segments_;
  Segment* active_ = nullptr;  // Owned by `segments_.back()`.

  std::atomic<Offset> log_start_offset_{0};
  std::atomic<Offset> log_end_offset_{0};
  std::atomic<Offset> flushed_offset_{0};

  // Flush accounting, written by the append path, read by the flusher.
  std::atomic<std::int64_t> unflushed_bytes_{0};
  std::atomic<std::int64_t> unflushed_records_{0};
  std::atomic<std::int64_t> last_flush_ms_{0};

  std::atomic<std::uint64_t> flush_count_{0};
  std::atomic<std::uint64_t> flush_nanos_total_{0};
  std::atomic<std::uint64_t> flush_nanos_max_{0};
  std::atomic<std::uint64_t> roll_count_{0};
  std::atomic<std::uint64_t> deleted_segments_{0};

  // Disk-space check result, refreshed periodically rather than per append.
  std::atomic<std::int64_t> disk_check_due_ms_{0};
  std::atomic<bool> disk_full_{false};

  std::atomic<bool> closed_{false};
};

}  // namespace pulselog::storage

#endif  // PULSELOG_STORAGE_PARTITION_LOG_H_
