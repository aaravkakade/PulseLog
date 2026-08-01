#include "pulselog/storage/partition_log.h"

#include <algorithm>

#include "pulselog/base/clock.h"
#include "pulselog/base/logging.h"

namespace pulselog::storage {
namespace {

constexpr std::string_view kComponent = "storage.log";

// How often the disk-space guard re-checks statvfs. Checking per append would
// add a syscall to the hot path for a condition that changes slowly.
constexpr std::int64_t kDiskCheckIntervalMs = 1000;

}  // namespace

PartitionLog::PartitionLog(TopicPartition topic_partition, LogOptions options)
    : topic_partition_(std::move(topic_partition)), options_(std::move(options)) {
  last_flush_ms_.store(MonotonicNanos() / 1'000'000, std::memory_order_relaxed);
}

PartitionLog::~PartitionLog() {
  const Status status = Close();
  if (!status.ok()) {
    PL_ERROR(kComponent) << "close failed during destruction"
                         << " partition=" << topic_partition_.ToString()
                         << " error=" << status.ToString();
  }
}

Result<std::unique_ptr<PartitionLog>> PartitionLog::Open(TopicPartition topic_partition,
                                                         LogOptions options,
                                                         RecoveryReport* recovery_out) {
  PL_RETURN_IF_ERROR(EnsureDirectory(options.directory));

  // `new` rather than make_unique because the constructor is private.
  std::unique_ptr<PartitionLog> log(
      new PartitionLog(std::move(topic_partition), std::move(options)));
  PL_RETURN_IF_ERROR(log->LoadSegments(recovery_out));
  return log;
}

Status PartitionLog::LoadSegments(RecoveryReport* recovery_out) {
  PL_ASSIGN_OR_RETURN(const auto files, ListFiles(options_.directory, ".log"));

  std::vector<Offset> base_offsets;
  base_offsets.reserve(files.size());
  for (const auto& file : files) {
    const auto base = Segment::ParseBaseOffset(file.stem().string());
    if (!base.has_value()) {
      PL_WARN(kComponent) << "ignoring unrecognised file in log directory"
                          << " path=" << file.filename().string();
      continue;
    }
    base_offsets.push_back(*base);
  }
  std::sort(base_offsets.begin(), base_offsets.end());

  SegmentOptions segment_options;
  segment_options.max_bytes = options_.segment_bytes;
  segment_options.index_interval_bytes = options_.index_interval_bytes;
  segment_options.preallocate = options_.preallocate;
  segment_options.write_mode = options_.write_mode;

  if (base_offsets.empty()) {
    PL_ASSIGN_OR_RETURN(auto segment, Segment::Open(options_.directory, 0, segment_options));
    active_ = segment.get();
    segments_.push_back(std::move(segment));
    log_start_offset_.store(0, std::memory_order_release);
    log_end_offset_.store(0, std::memory_order_release);
    flushed_offset_.store(0, std::memory_order_release);
    return OkStatus();
  }

  RecoveryReport combined;
  for (std::size_t i = 0; i < base_offsets.size(); ++i) {
    PL_ASSIGN_OR_RETURN(auto segment,
                        Segment::Open(options_.directory, base_offsets[i], segment_options));
    // Only the last segment can have a torn tail: earlier ones were sealed
    // when they rolled. Scanning them all anyway would make recovery time grow
    // with the whole log rather than with one segment.
    const bool is_last = (i + 1 == base_offsets.size());
    PL_ASSIGN_OR_RETURN(const RecoveryReport report, segment->Recover(/*full_scan=*/false));

    combined.records_scanned += report.records_scanned;
    combined.valid_bytes += report.valid_bytes;
    if (report.truncated) {
      combined.truncated = true;
      combined.truncated_bytes += report.truncated_bytes;
      combined.reason = report.reason;
      if (!is_last) {
        PL_ERROR(kComponent) << "damage found in a sealed segment; data after it is unreachable"
                             << " partition=" << topic_partition_.ToString()
                             << " segment=" << base_offsets[i] << " reason=\"" << report.reason
                             << "\"";
      }
    }
    combined.next_offset = report.next_offset;
    segments_.push_back(std::move(segment));
  }

  active_ = segments_.back().get();
  log_start_offset_.store(segments_.front()->base_offset(), std::memory_order_release);
  log_end_offset_.store(active_->next_offset(), std::memory_order_release);
  flushed_offset_.store(active_->next_offset(), std::memory_order_release);

  PL_INFO(kComponent) << "recovered partition"
                      << " partition=" << topic_partition_.ToString()
                      << " segments=" << segments_.size()
                      << " log_start=" << log_start_offset_.load(std::memory_order_relaxed)
                      << " log_end=" << log_end_offset_.load(std::memory_order_relaxed)
                      << " records=" << combined.records_scanned
                      << " truncated_bytes=" << combined.truncated_bytes;

  if (recovery_out != nullptr) *recovery_out = combined;
  return OkStatus();
}

Status PartitionLog::CheckDiskSpace() {
  const std::int64_t now = MonotonicNanos() / 1'000'000;
  if (now < disk_check_due_ms_.load(std::memory_order_relaxed)) {
    return disk_full_.load(std::memory_order_relaxed)
               ? ResourceExhausted("disk below the configured free-space floor")
               : OkStatus();
  }
  disk_check_due_ms_.store(now + kDiskCheckIntervalMs, std::memory_order_relaxed);

  auto available = AvailableBytes(options_.directory);
  if (!available.ok()) {
    // If we cannot tell, do not block writes on a diagnostic failure.
    PL_DEBUG(kComponent) << "statvfs failed: " << available.status().ToString();
    disk_full_.store(false, std::memory_order_relaxed);
    return OkStatus();
  }

  const bool full = static_cast<std::int64_t>(available.value()) < options_.min_free_disk_bytes;
  const bool was_full = disk_full_.exchange(full, std::memory_order_relaxed);
  if (full && !was_full) {
    PL_ERROR(kComponent) << "rejecting writes: free space below floor"
                         << " partition=" << topic_partition_.ToString()
                         << " available_bytes=" << available.value()
                         << " floor_bytes=" << options_.min_free_disk_bytes;
  } else if (!full && was_full) {
    PL_INFO(kComponent) << "free space recovered; accepting writes"
                        << " partition=" << topic_partition_.ToString()
                        << " available_bytes=" << available.value();
  }
  return full ? ResourceExhausted("disk below the configured free-space floor") : OkStatus();
}

Status PartitionLog::MaybeRoll(std::size_t incoming_bytes) {
  Segment* active = active_;
  if (active == nullptr) return Internal("partition log has no active segment");

  const std::uint64_t size = active->SizeBytes();
  const bool by_size =
      options_.segment_bytes > 0 &&
      static_cast<std::int64_t>(size + incoming_bytes) > options_.segment_bytes && size > 0;
  const bool by_age = options_.segment_ms > 0 && size > 0 &&
                      (WallClockMillis() - active->CreatedAtMs()) >= options_.segment_ms;
  // Segment positions are 32-bit in the index, so a segment can never exceed
  // 4 GiB regardless of configuration.
  const bool by_limit = size + incoming_bytes > 0xFFFFFFFFULL;

  if (!by_size && !by_age && !by_limit) return OkStatus();
  return RollSegment();
}

Status PartitionLog::RollSegment() {
  Segment* previous = active_;
  const Offset next_base = previous != nullptr ? previous->next_offset()
                                               : log_end_offset_.load(std::memory_order_acquire);

  SegmentOptions segment_options;
  segment_options.max_bytes = options_.segment_bytes;
  segment_options.index_interval_bytes = options_.index_interval_bytes;
  segment_options.preallocate = options_.preallocate;
  segment_options.write_mode = options_.write_mode;

  PL_ASSIGN_OR_RETURN(auto segment, Segment::Open(options_.directory, next_base, segment_options));

  // Seal the previous segment before publishing the new one: fsync its data
  // and trim its preallocated tail so its on-disk size is honest.
  if (previous != nullptr) {
    PL_RETURN_IF_ERROR(previous->Sync());
  }
  // Make the new file's directory entry durable; otherwise a power cut can
  // lose a segment whose contents were fsynced.
  const Status dir_sync = SyncDirectory(options_.directory);
  if (!dir_sync.ok()) {
    PL_DEBUG(kComponent) << "directory fsync unsupported here: " << dir_sync.ToString();
  }

  {
    std::unique_lock<std::shared_mutex> lock(segments_mutex_);
    active_ = segment.get();
    segments_.push_back(std::move(segment));
  }
  roll_count_.fetch_add(1, std::memory_order_relaxed);
  PL_DEBUG(kComponent) << "rolled segment"
                       << " partition=" << topic_partition_.ToString()
                       << " base_offset=" << next_base;
  return OkStatus();
}

Status PartitionLog::FinalizeAppend(const AppendResult& result) {
  log_end_offset_.store(result.last_offset + 1, std::memory_order_release);
  unflushed_bytes_.fetch_add(static_cast<std::int64_t>(result.bytes), std::memory_order_relaxed);
  unflushed_records_.fetch_add(result.record_count, std::memory_order_relaxed);

  if (options_.flush.sync_on_append) {
    return Flush();
  }
  return OkStatus();
}

Result<AppendResult> PartitionLog::AppendAssigningOffsets(MutableByteSpan records,
                                                          std::uint32_t record_count) {
  if (closed_.load(std::memory_order_acquire)) return Status{ErrorCode::kClosed, "log is closed"};
  if (records.empty() || record_count == 0) {
    return InvalidArgument("append with no records");
  }
  PL_RETURN_IF_ERROR(CheckDiskSpace());
  PL_RETURN_IF_ERROR(MaybeRoll(records.size()));

  Segment* active = active_;
  const Offset base = active->next_offset();
  const TimestampMs append_time = WallClockMillis();

  // Single pass over the batch: validate structure, stamp offsets, stamp any
  // missing timestamps, and recompute each record's checksum. The producer's
  // record checksums are not trusted or reused -- the frame's payload CRC
  // already proved transport integrity, and these bytes are about to change.
  Offset offset = base;
  TimestampMs max_timestamp = -1;
  std::size_t pos = 0;
  std::uint32_t seen = 0;
  const ByteSpan readable(records.data(), records.size());

  while (pos < records.size()) {
    protocol::RecordView view;
    std::size_t next = 0;
    const Status status = protocol::ParseRecord(readable, pos, /*verify_crc=*/false, view, next);
    if (!status.ok()) {
      return status.WithContext("record " + std::to_string(seen) + " in produce batch");
    }
    const TimestampMs timestamp = view.timestamp > 0 ? view.timestamp : append_time;
    protocol::RewriteRecordHeader(records.data() + pos, next - pos, offset, timestamp);
    max_timestamp = std::max(max_timestamp, timestamp);
    ++offset;
    ++seen;
    pos = next;
  }

  if (seen != record_count) {
    return InvalidArgument("produce batch declared " + std::to_string(record_count) +
                           " records but contains " + std::to_string(seen));
  }

  PL_RETURN_IF_ERROR(active->AppendEncoded(readable, base, offset - 1, seen, max_timestamp));

  AppendResult result;
  result.base_offset = base;
  result.last_offset = offset - 1;
  result.record_count = seen;
  result.bytes = records.size();
  result.append_time = append_time;
  PL_RETURN_IF_ERROR(FinalizeAppend(result));
  return result;
}

Result<AppendResult> PartitionLog::AppendWithOffsets(ByteSpan records,
                                                     std::uint32_t record_count) {
  if (closed_.load(std::memory_order_acquire)) return Status{ErrorCode::kClosed, "log is closed"};
  if (records.empty() || record_count == 0) return InvalidArgument("append with no records");
  PL_RETURN_IF_ERROR(CheckDiskSpace());

  const Offset expected = log_end_offset_.load(std::memory_order_acquire);

  // These bytes arrived over the network from another broker, so every record
  // is checksum-verified before it goes into this log. A follower that writes
  // corrupt bytes would propagate them to consumers as if they were valid.
  protocol::RecordIterator it(records, /*verify_crc=*/true);
  protocol::RecordView view;
  Offset offset = expected;
  TimestampMs max_timestamp = -1;
  std::uint32_t seen = 0;
  while (it.Next(view)) {
    if (view.offset != offset) {
      return OutOfRange("replicated record offset " + std::to_string(view.offset) +
                        " does not continue the log at " + std::to_string(offset));
    }
    max_timestamp = std::max(max_timestamp, view.timestamp);
    ++offset;
    ++seen;
  }
  if (!it.status().ok()) return it.status().WithContext("validating replicated batch");
  if (seen != record_count) {
    return InvalidArgument("replicated batch declared " + std::to_string(record_count) +
                           " records but contains " + std::to_string(seen));
  }

  PL_RETURN_IF_ERROR(MaybeRoll(records.size()));
  Segment* active = active_;
  if (active->next_offset() != expected) {
    return Internal("segment end " + std::to_string(active->next_offset()) +
                    " disagrees with log end " + std::to_string(expected));
  }
  PL_RETURN_IF_ERROR(active->AppendEncoded(records, expected, offset - 1, seen, max_timestamp));

  AppendResult result;
  result.base_offset = expected;
  result.last_offset = offset - 1;
  result.record_count = seen;
  result.bytes = records.size();
  result.append_time = WallClockMillis();
  PL_RETURN_IF_ERROR(FinalizeAppend(result));
  return result;
}

Result<AppendResult> PartitionLog::AppendVectoredWithOffsets(std::span<const ByteSpan> chunks,
                                                             std::uint32_t record_count) {
  if (closed_.load(std::memory_order_acquire)) return Status{ErrorCode::kClosed, "log is closed"};
  std::size_t total = 0;
  for (const auto& chunk : chunks) total += chunk.size();
  if (total == 0 || record_count == 0) return InvalidArgument("append with no records");
  PL_RETURN_IF_ERROR(CheckDiskSpace());
  PL_RETURN_IF_ERROR(MaybeRoll(total));

  Segment* active = active_;
  const Offset base = active->next_offset();

  // Validate continuity across chunk boundaries without copying: each chunk is
  // a self-contained run of records that must continue the previous one.
  Offset offset = base;
  TimestampMs max_timestamp = -1;
  std::uint32_t seen = 0;
  for (const auto& chunk : chunks) {
    protocol::RecordIterator it(chunk, /*verify_crc=*/false);
    protocol::RecordView view;
    while (it.Next(view)) {
      if (view.offset != offset) {
        return OutOfRange("vectored append is not contiguous at offset " + std::to_string(offset));
      }
      max_timestamp = std::max(max_timestamp, view.timestamp);
      ++offset;
      ++seen;
    }
    if (!it.status().ok()) return it.status().WithContext("validating vectored append");
  }
  if (seen != record_count) {
    return InvalidArgument("vectored append declared " + std::to_string(record_count) +
                           " records but contains " + std::to_string(seen));
  }

  PL_RETURN_IF_ERROR(
      active->AppendEncodedVectored(chunks, base, offset - 1, seen, max_timestamp));

  AppendResult result;
  result.base_offset = base;
  result.last_offset = offset - 1;
  result.record_count = seen;
  result.bytes = total;
  result.append_time = WallClockMillis();
  PL_RETURN_IF_ERROR(FinalizeAppend(result));
  return result;
}

Segment* PartitionLog::SegmentForOffsetLocked(Offset offset) const {
  if (segments_.empty()) return nullptr;
  // Largest segment whose base_offset <= offset.
  auto it = std::upper_bound(segments_.begin(), segments_.end(), offset,
                             [](Offset value, const std::unique_ptr<Segment>& segment) {
                               return value < segment->base_offset();
                             });
  if (it == segments_.begin()) return nullptr;
  return (it - 1)->get();
}

Result<LogReadResult> PartitionLog::Read(Offset from, std::size_t max_bytes,
                                         ByteBuffer& out) const {
  LogReadResult result;
  result.log_start_offset = log_start_offset_.load(std::memory_order_acquire);
  result.log_end_offset = log_end_offset_.load(std::memory_order_acquire);
  result.base_offset = from;

  if (from < result.log_start_offset) {
    return OutOfRange("offset " + std::to_string(from) + " was deleted by retention (log starts at " +
                      std::to_string(result.log_start_offset) + ")");
  }
  if (from > result.log_end_offset) {
    return OutOfRange("offset " + std::to_string(from) + " is beyond the log end " +
                      std::to_string(result.log_end_offset));
  }
  if (from == result.log_end_offset) return result;  // Caught up; nothing to send.

  std::shared_lock<std::shared_mutex> lock(segments_mutex_);
  Segment* segment = SegmentForOffsetLocked(from);
  if (segment == nullptr) {
    return OutOfRange("no segment holds offset " + std::to_string(from));
  }

  Offset cursor = from;
  while (result.bytes < max_bytes && segment != nullptr) {
    PL_ASSIGN_OR_RETURN(const Segment::ReadResult chunk,
                        segment->Read(cursor, max_bytes - result.bytes, out));
    result.record_count += chunk.record_count;
    result.bytes += chunk.bytes;
    cursor += chunk.record_count;

    if (chunk.record_count == 0) {
      // Exhausted this segment without filling the budget: continue into the
      // next one so a reader never stalls at a segment boundary.
      if (!chunk.reached_end) break;
      Segment* next = nullptr;
      for (std::size_t i = 0; i + 1 < segments_.size(); ++i) {
        if (segments_[i].get() == segment) {
          next = segments_[i + 1].get();
          break;
        }
      }
      if (next == nullptr) break;
      segment = next;
      continue;
    }
    if (cursor >= result.log_end_offset) break;
    if (cursor >= segment->next_offset()) {
      Segment* next = nullptr;
      for (std::size_t i = 0; i + 1 < segments_.size(); ++i) {
        if (segments_[i].get() == segment) {
          next = segments_[i + 1].get();
          break;
        }
      }
      segment = next;
    }
  }
  return result;
}

bool PartitionLog::NeedsFlush(std::int64_t now_ms) const {
  if (options_.flush.sync_on_append) return false;  // Already flushed inline.
  const std::int64_t bytes = unflushed_bytes_.load(std::memory_order_relaxed);
  if (bytes == 0) return false;
  if (options_.flush.max_unflushed_bytes > 0 && bytes >= options_.flush.max_unflushed_bytes) {
    return true;
  }
  const std::int64_t records = unflushed_records_.load(std::memory_order_relaxed);
  if (options_.flush.max_unflushed_records > 0 &&
      records >= options_.flush.max_unflushed_records) {
    return true;
  }
  if (options_.flush.interval_ms > 0 &&
      now_ms - last_flush_ms_.load(std::memory_order_relaxed) >= options_.flush.interval_ms) {
    return true;
  }
  return false;
}

Status PartitionLog::Flush() {
  // The offset is captured before the fsync so `flushed_offset` never claims
  // more than what the syscall actually covered.
  const Offset target = log_end_offset_.load(std::memory_order_acquire);
  if (target == flushed_offset_.load(std::memory_order_acquire)) return OkStatus();

  const std::int64_t started = MonotonicNanos();
  {
    std::shared_lock<std::shared_mutex> lock(segments_mutex_);
    for (auto& segment : segments_) {
      if (segment->SyncedBytes() < segment->SizeBytes()) {
        PL_RETURN_IF_ERROR(segment->Sync());
      }
    }
  }
  const auto elapsed = static_cast<std::uint64_t>(MonotonicNanos() - started);

  flushed_offset_.store(target, std::memory_order_release);
  unflushed_bytes_.store(0, std::memory_order_relaxed);
  unflushed_records_.store(0, std::memory_order_relaxed);
  last_flush_ms_.store(MonotonicNanos() / 1'000'000, std::memory_order_relaxed);

  flush_count_.fetch_add(1, std::memory_order_relaxed);
  flush_nanos_total_.fetch_add(elapsed, std::memory_order_relaxed);
  std::uint64_t previous_max = flush_nanos_max_.load(std::memory_order_relaxed);
  while (elapsed > previous_max &&
         !flush_nanos_max_.compare_exchange_weak(previous_max, elapsed,
                                                 std::memory_order_relaxed)) {
  }
  return OkStatus();
}

Status PartitionLog::TruncateTo(Offset offset) {
  const Offset end = log_end_offset_.load(std::memory_order_acquire);
  if (offset >= end) return OkStatus();
  const Offset start = log_start_offset_.load(std::memory_order_acquire);
  if (offset < start) {
    return OutOfRange("cannot truncate to " + std::to_string(offset) + "; log starts at " +
                      std::to_string(start));
  }

  std::unique_lock<std::shared_mutex> lock(segments_mutex_);
  // Drop whole segments that begin at or after the truncation point, then trim
  // the one that contains it.
  while (segments_.size() > 1 && segments_.back()->base_offset() >= offset) {
    auto segment = std::move(segments_.back());
    segments_.pop_back();
    PL_RETURN_IF_ERROR(segment->Close());
    PL_RETURN_IF_ERROR(segment->Delete());
    deleted_segments_.fetch_add(1, std::memory_order_relaxed);
  }
  active_ = segments_.back().get();
  PL_RETURN_IF_ERROR(active_->TruncateFrom(offset));

  log_end_offset_.store(offset, std::memory_order_release);
  if (flushed_offset_.load(std::memory_order_acquire) > offset) {
    flushed_offset_.store(offset, std::memory_order_release);
  }
  unflushed_bytes_.store(0, std::memory_order_relaxed);
  unflushed_records_.store(0, std::memory_order_relaxed);

  PL_INFO(kComponent) << "truncated log"
                      << " partition=" << topic_partition_.ToString() << " new_end=" << offset;
  return OkStatus();
}

Result<std::size_t> PartitionLog::EnforceRetention() {
  if (options_.retention_bytes <= 0 && options_.retention_ms <= 0) return std::size_t{0};

  const TimestampMs now = WallClockMillis();
  std::size_t deleted = 0;

  std::unique_lock<std::shared_mutex> lock(segments_mutex_);
  while (segments_.size() > 1) {
    // Total size excluding the candidate tells us whether deleting it would
    // still leave us over the size budget.
    std::uint64_t total = 0;
    for (const auto& segment : segments_) total += segment->SizeBytes();

    Segment* oldest = segments_.front().get();
    const bool too_old = options_.retention_ms > 0 && oldest->MaxTimestamp() > 0 &&
                         (now - oldest->MaxTimestamp()) > options_.retention_ms;
    const bool too_big = options_.retention_bytes > 0 &&
                         static_cast<std::int64_t>(total) > options_.retention_bytes;
    if (!too_old && !too_big) break;

    auto segment = std::move(segments_.front());
    segments_.erase(segments_.begin());
    const Offset new_start = segments_.front()->base_offset();
    PL_RETURN_IF_ERROR(segment->Close());
    PL_RETURN_IF_ERROR(segment->Delete());
    log_start_offset_.store(new_start, std::memory_order_release);
    ++deleted;
    deleted_segments_.fetch_add(1, std::memory_order_relaxed);

    PL_INFO(kComponent) << "deleted segment for retention"
                        << " partition=" << topic_partition_.ToString()
                        << " base_offset=" << segment->base_offset()
                        << " reason=" << (too_old ? "age" : "size")
                        << " new_log_start=" << new_start;
  }
  return deleted;
}

Result<Offset> PartitionLog::OffsetForTimestamp(TimestampMs timestamp) const {
  if (timestamp == kEarliestOffset) return log_start_offset_.load(std::memory_order_acquire);
  if (timestamp == kLatestOffset) return log_end_offset_.load(std::memory_order_acquire);

  std::shared_lock<std::shared_mutex> lock(segments_mutex_);
  for (const auto& segment : segments_) {
    if (segment->MaxTimestamp() >= 0 && segment->MaxTimestamp() < timestamp) continue;
    if (const auto offset = segment->OffsetForTimestamp(timestamp)) return *offset;
  }
  // Nothing at or after that time yet: the answer is "the next record written".
  return log_end_offset_.load(std::memory_order_acquire);
}

LogStats PartitionLog::GetStats() const {
  LogStats stats;
  stats.log_start_offset = log_start_offset_.load(std::memory_order_acquire);
  stats.log_end_offset = log_end_offset_.load(std::memory_order_acquire);
  stats.flushed_offset = flushed_offset_.load(std::memory_order_acquire);
  stats.flush_count = flush_count_.load(std::memory_order_relaxed);
  stats.flush_nanos_total = flush_nanos_total_.load(std::memory_order_relaxed);
  stats.flush_nanos_max = flush_nanos_max_.load(std::memory_order_relaxed);
  stats.roll_count = roll_count_.load(std::memory_order_relaxed);
  stats.deleted_segments = deleted_segments_.load(std::memory_order_relaxed);

  std::shared_lock<std::shared_mutex> lock(segments_mutex_);
  stats.segment_count = segments_.size();
  for (const auto& segment : segments_) {
    stats.total_bytes += segment->SizeBytes();
    stats.record_count += segment->RecordCount();
  }
  return stats;
}

Status PartitionLog::Close() {
  if (closed_.exchange(true, std::memory_order_acq_rel)) return OkStatus();

  Status result = OkStatus();
  std::unique_lock<std::shared_mutex> lock(segments_mutex_);
  for (auto& segment : segments_) {
    const Status status = segment->Close();
    if (result.ok() && !status.ok()) result = status;
  }
  active_ = nullptr;
  return result;
}

}  // namespace pulselog::storage
