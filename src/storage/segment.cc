#include "pulselog/storage/segment.h"

#include <algorithm>
#include <charconv>
#include <iomanip>
#include <sstream>

#include "pulselog/base/clock.h"
#include "pulselog/base/endian.h"
#include "pulselog/base/logging.h"

namespace pulselog::storage {
namespace {

constexpr std::string_view kComponent = "storage.segment";

constexpr std::size_t kBaseOffsetDigits = 20;

// Chunk size used while scanning a segment during recovery. Large enough to
// amortise syscalls, small enough that a corrupt length cannot make us
// allocate unboundedly.
constexpr std::size_t kRecoveryChunkBytes = 1U << 20U;

}  // namespace

std::string Segment::FormatBaseOffset(Offset base_offset) {
  std::ostringstream out;
  out << std::setw(static_cast<int>(kBaseOffsetDigits)) << std::setfill('0') << base_offset;
  return out.str();
}

std::optional<Offset> Segment::ParseBaseOffset(const std::string& stem) {
  if (stem.size() != kBaseOffsetDigits) return std::nullopt;
  Offset value = 0;
  const auto* first = stem.data();
  const auto* last = stem.data() + stem.size();
  const auto [ptr, ec] = std::from_chars(first, last, value);
  if (ec != std::errc{} || ptr != last) return std::nullopt;
  return value;
}

Result<std::unique_ptr<Segment>> Segment::Open(const std::filesystem::path& dir,
                                               Offset base_offset,
                                               const SegmentOptions& options) {
  auto segment = std::make_unique<Segment>();
  segment->options_ = options;
  segment->base_offset_ = base_offset;
  segment->next_offset_.store(base_offset, std::memory_order_relaxed);

  const std::string stem = FormatBaseOffset(base_offset);
  segment->log_path_ = dir / (stem + ".log");
  segment->index_path_ = dir / (stem + ".index");
  segment->time_index_path_ = dir / (stem + ".tindex");

  const bool existed = std::filesystem::exists(segment->log_path_);

  PL_ASSIGN_OR_RETURN(segment->file_, FileHandle::Open(segment->log_path_, /*create=*/true));
  segment->file_.HintSequential();
  PL_RETURN_IF_ERROR(segment->index_.Open(segment->index_path_, base_offset));
  PL_RETURN_IF_ERROR(segment->time_index_.Open(segment->time_index_path_, base_offset));

  PL_ASSIGN_OR_RETURN(const std::uint64_t file_size, segment->file_.Size());
  segment->preallocated_to_ = file_size;

  if (!existed && options.preallocate && options.max_bytes > 0) {
    // Preallocation trades a one-off allocation cost for fewer metadata
    // updates per append and less fragmentation. The file is left at its full
    // size on disk; `end_position_` is what defines valid content, and
    // recovery truncates the unused tail when the segment is rolled.
    const Status status = segment->file_.Preallocate(static_cast<std::uint64_t>(options.max_bytes));
    if (!status.ok()) {
      PL_WARN(kComponent) << "preallocation failed, continuing without it"
                          << " path=" << segment->log_path_.filename().string()
                          << " error=" << status.ToString();
    } else {
      segment->preallocated_to_ = static_cast<std::uint64_t>(options.max_bytes);
    }
  }

  segment->created_at_ms_ = WallClockMillis();
  segment->last_indexed_position_ = segment->index_.LastIndexedPosition();
  return segment;
}

Result<RecoveryReport> Segment::Recover(bool full_scan) {
  RecoveryReport report;
  report.next_offset = base_offset_;

  PL_ASSIGN_OR_RETURN(const std::uint64_t file_size, file_.Size());

  // Start from the last index entry: everything before it was validated when
  // it was written and re-validated by whichever recovery pass created that
  // entry. A full scan is available for the paranoid path (`--verify-full`)
  // and for the corruption tests.
  std::uint64_t position = 0;
  Offset offset = base_offset_;
  if (!full_scan && index_.EntryCount() > 0) {
    position = index_.LastIndexedPosition();
    offset = index_.LastIndexedOffset();
  }

  std::uint64_t records = 0;
  TimestampMs max_timestamp = -1;
  std::vector<std::uint8_t> chunk;
  std::string stop_reason;

  bool done = false;
  while (!done && position < file_size) {
    const std::size_t want = static_cast<std::size_t>(
        std::min<std::uint64_t>(kRecoveryChunkBytes, file_size - position));
    chunk.resize(want);
    PL_ASSIGN_OR_RETURN(const std::size_t got, file_.ReadAt(chunk.data(), want, position));
    if (got == 0) break;
    chunk.resize(got);

    std::size_t chunk_pos = 0;
    while (chunk_pos < chunk.size()) {
      protocol::RecordView view;
      std::size_t next = 0;
      const Status status =
          protocol::ParseRecord(chunk, chunk_pos, /*verify_crc=*/true, view, next);
      if (!status.ok()) {
        if (status.code() == ErrorCode::kOutOfRange && got == want && position + got < file_size) {
          // The record straddles the chunk boundary; re-read from here.
          break;
        }
        // ParseRecord reports a chunk-relative offset; recovery reads in
        // chunks, so translate it to an absolute file offset before it reaches
        // an operator who will go looking for that byte.
        stop_reason =
            status.ToString() + " (file offset " + std::to_string(position + chunk_pos) + ")";
        done = true;
        break;
      }
      // A preallocated tail reads as zeros, which parse as a zero-length,
      // zero-CRC record. Treat any record whose declared offset does not
      // continue the sequence as the end of real data.
      if (view.offset != offset) {
        stop_reason = "offset discontinuity: expected " + std::to_string(offset) + ", found " +
                      std::to_string(view.offset);
        done = true;
        break;
      }
      ++offset;
      ++records;
      max_timestamp = std::max(max_timestamp, view.timestamp);
      chunk_pos = next;
    }

    if (chunk_pos == 0 && !done) {
      // Not even one record fit in the chunk: the record is larger than the
      // scan window. Grow once rather than loop forever.
      if (want >= protocol::kMaxRecordBytes + 64) {
        stop_reason = "record larger than the maximum record size";
        break;
      }
      chunk.resize(protocol::kMaxRecordBytes + 64);
      continue;
    }
    position += chunk_pos;
  }

  report.next_offset = offset;
  report.valid_bytes = position;
  report.records_scanned = records;

  if (position < file_size) {
    // Distinguish the unwritten remainder of a preallocated segment from real
    // damage. Preallocated space reads as zeros, so its "record length" field
    // is 0 -- a value no real record can have.
    bool preallocated_tail = false;
    std::array<std::uint8_t, 4> peek{};
    const auto peeked = file_.ReadAt(peek.data(), peek.size(), position);
    if (peeked.ok() && peeked.value() == peek.size()) {
      preallocated_tail = LoadLe<std::uint32_t>(peek.data()) == 0;
    }

    if (preallocated_tail) {
      report.preallocated_bytes = file_size - position;
      PL_DEBUG(kComponent) << "trimming unwritten preallocated tail"
                           << " path=" << log_path_.filename().string()
                           << " valid_bytes=" << position
                           << " trimmed_bytes=" << report.preallocated_bytes;
    } else {
      report.truncated = true;
      report.truncated_bytes = file_size - position;
      report.reason = stop_reason.empty() ? "trailing bytes are not a valid record" : stop_reason;
      PL_WARN(kComponent) << "discarding damaged segment tail"
                          << " path=" << log_path_.filename().string()
                          << " valid_bytes=" << position
                          << " dropped_bytes=" << report.truncated_bytes << " reason=\""
                          << report.reason << "\"";
    }

    PL_RETURN_IF_ERROR(file_.Truncate(position));
    PL_RETURN_IF_ERROR(index_.TruncateFrom(offset));
    PL_RETURN_IF_ERROR(time_index_.TruncateFrom(offset));
    // The file is no longer preallocated past its content.
    preallocated_to_ = position;
  }

  end_position_.store(position, std::memory_order_release);
  synced_position_.store(position, std::memory_order_release);
  next_offset_.store(offset, std::memory_order_release);
  record_count_.store(records + (full_scan ? 0 : index_.EntryCount()), std::memory_order_relaxed);
  max_timestamp_.store(std::max(max_timestamp, time_index_.MaxTimestamp()),
                       std::memory_order_relaxed);
  last_indexed_position_ = index_.LastIndexedPosition();
  return report;
}

Status Segment::FinishAppend(std::size_t bytes,
                             Offset append_base,
                             Offset last_offset,
                             std::uint32_t record_count,
                             TimestampMs max_timestamp) {
  const std::uint64_t start = end_position_.load(std::memory_order_relaxed);

  // Index entry first, so a reader that observes the new end position can
  // always find a position at or before it. Index writes are cheap and sparse.
  if (start - last_indexed_position_ >= static_cast<std::uint64_t>(options_.index_interval_bytes) ||
      index_.EntryCount() == 0) {
    const Status status = index_.Append(append_base, start);
    if (!status.ok()) {
      // An index failure is not fatal: the log is the source of truth and
      // recovery can rebuild the index by scanning. Log it and continue.
      PL_WARN(kComponent) << "offset index append failed"
                          << " path=" << log_path_.filename().string()
                          << " error=" << status.ToString();
    } else {
      last_indexed_position_ = start;
      const Status time_status = time_index_.Append(max_timestamp, append_base);
      if (!time_status.ok()) {
        PL_DEBUG(kComponent) << "time index append failed: " << time_status.ToString();
      }
    }
  }

  end_position_.store(start + bytes, std::memory_order_release);
  next_offset_.store(last_offset + 1, std::memory_order_release);
  record_count_.fetch_add(record_count, std::memory_order_relaxed);

  TimestampMs previous = max_timestamp_.load(std::memory_order_relaxed);
  while (max_timestamp > previous && !max_timestamp_.compare_exchange_weak(
                                         previous, max_timestamp, std::memory_order_relaxed)) {
  }
  return OkStatus();
}

Status Segment::AppendEncoded(ByteSpan records,
                              Offset append_base,
                              Offset last_offset,
                              std::uint32_t record_count,
                              TimestampMs max_timestamp) {
  if (records.empty()) return OkStatus();
  const std::uint64_t start = end_position_.load(std::memory_order_relaxed);

  if (start + records.size() > preallocated_to_) {
    // Growing past the preallocated region is normal at the end of a segment's
    // life; the write itself extends the file.
    preallocated_to_ = start + records.size();
  }
  PL_RETURN_IF_ERROR(file_.WriteAllAt(records, start));
  return FinishAppend(records.size(), append_base, last_offset, record_count, max_timestamp);
}

Status Segment::AppendEncodedVectored(std::span<const ByteSpan> chunks,
                                      Offset append_base,
                                      Offset last_offset,
                                      std::uint32_t record_count,
                                      TimestampMs max_timestamp) {
  std::size_t total = 0;
  for (const auto& chunk : chunks) total += chunk.size();
  if (total == 0) return OkStatus();

  const std::uint64_t start = end_position_.load(std::memory_order_relaxed);
  if (start + total > preallocated_to_) preallocated_to_ = start + total;

  PL_RETURN_IF_ERROR(file_.WriteVectoredAt(chunks, start));
  return FinishAppend(total, append_base, last_offset, record_count, max_timestamp);
}

Result<std::uint64_t> Segment::PositionFor(Offset offset) const {
  if (offset < base_offset_) {
    return OutOfRange("offset " + std::to_string(offset) + " precedes segment base " +
                      std::to_string(base_offset_));
  }
  const Offset end = next_offset_.load(std::memory_order_acquire);
  if (offset >= end) {
    return OutOfRange("offset " + std::to_string(offset) + " is at or past segment end " +
                      std::to_string(end));
  }

  const std::uint64_t limit = end_position_.load(std::memory_order_acquire);
  std::uint64_t position = 0;
  Offset current = base_offset_;
  if (const auto entry = index_.LookupFloor(offset)) {
    position = entry->file_position;
    current = base_offset_ + static_cast<Offset>(entry->relative_offset);
  }

  // Bounded forward scan: at most `index_interval_bytes` of log, because the
  // index guarantees an entry at least that often.
  //
  // The scan reads a block at a time and walks it in memory. An earlier
  // version issued one 4-byte pread per record to read the length prefix,
  // which turned a single offset lookup into tens of syscalls -- measured at
  // ~170 us per lookup, against ~14 us after this change
  // (benchmarks/bench_storage.cc, BM_OffsetLookup).
  if (current >= offset) return position;

  // The scan is bounded by `index_interval_bytes`, so one block of that size
  // (with a floor, since a tiny interval would mean many reads) covers the
  // whole scan in the normal case. A record that straddles the block end is
  // handled by advancing and reading again, and a record larger than the block
  // grows it once.
  std::vector<std::uint8_t> block;
  std::size_t block_size =
      static_cast<std::size_t>(std::max<std::int64_t>(options_.index_interval_bytes, 8192));

  while (current < offset) {
    const std::size_t want =
        static_cast<std::size_t>(std::min<std::uint64_t>(block_size, limit - position));
    if (want < 4) {
      return Corruption("scan for offset " + std::to_string(offset) + " ran off the segment at " +
                        std::to_string(position));
    }
    block.resize(want);
    PL_ASSIGN_OR_RETURN(const std::size_t got, file_.ReadAt(block.data(), want, position));
    if (got < 4) {
      return Corruption("scan for offset " + std::to_string(offset) +
                        " hit the end of the segment at " + std::to_string(position));
    }

    std::size_t block_pos = 0;
    while (current < offset && block_pos + 4 <= got) {
      const std::uint32_t length = LoadLe<std::uint32_t>(block.data() + block_pos);
      const std::uint64_t record_size = static_cast<std::uint64_t>(length) + 4;
      if (record_size < protocol::kRecordFixedPrefix ||
          position + block_pos + record_size > limit) {
        return Corruption("scan for offset " + std::to_string(offset) + " ran off the segment at " +
                          std::to_string(position + block_pos));
      }
      // Stop if this record straddles the block; the next read starts here.
      if (block_pos + record_size > got) break;
      block_pos += static_cast<std::size_t>(record_size);
      ++current;
    }

    if (block_pos == 0) {
      // The record at `position` is larger than the block. Grow once to fit
      // it exactly rather than guessing.
      const std::uint32_t length = LoadLe<std::uint32_t>(block.data());
      const std::uint64_t record_size = static_cast<std::uint64_t>(length) + 4;
      if (record_size < protocol::kRecordFixedPrefix || record_size > protocol::kMaxRecordBytes ||
          position + record_size > limit) {
        return Corruption("scan for offset " + std::to_string(offset) +
                          " found an impossible record size at " + std::to_string(position));
      }
      if (static_cast<std::size_t>(record_size) <= block_size) {
        return Corruption("scan for offset " + std::to_string(offset) + " made no progress at " +
                          std::to_string(position));
      }
      block_size = static_cast<std::size_t>(record_size);
      continue;
    }
    position += block_pos;
  }
  return position;
}

Result<Segment::ReadResult> Segment::Read(Offset from,
                                          std::size_t max_bytes,
                                          ByteBuffer& out) const {
  ReadResult result;
  const Offset end = next_offset_.load(std::memory_order_acquire);
  const std::uint64_t limit = end_position_.load(std::memory_order_acquire);

  if (from >= end) {
    result.base_offset = from;
    result.reached_end = true;
    return result;
  }

  PL_ASSIGN_OR_RETURN(std::uint64_t position, PositionFor(from));
  result.base_offset = from;

  std::uint64_t cursor = position;
  Offset offset = from;

  // Read in bounded chunks and stop on the last whole record that fits within
  // `max_bytes`. A partial record is never returned: a client would have no
  // way to tell one apart from corruption.
  while (cursor < limit && offset < end) {
    const std::size_t budget_left = max_bytes > result.bytes ? max_bytes - result.bytes : 0;
    if (budget_left == 0) break;

    // Size of the record sitting at `cursor`, so the chunk can be made big
    // enough to contain it whole.
    std::array<std::uint8_t, 4> length_bytes{};
    PL_RETURN_IF_ERROR(file_.ReadExactAt(length_bytes.data(), length_bytes.size(), cursor));
    const std::size_t next_record_size =
        static_cast<std::size_t>(LoadLe<std::uint32_t>(length_bytes.data())) + 4;
    if (next_record_size < protocol::kRecordFixedPrefix || cursor + next_record_size > limit) {
      return Corruption("record at position " + std::to_string(cursor) +
                        " claims a size that runs past the segment end");
    }

    std::size_t want = static_cast<std::size_t>(
        std::min<std::uint64_t>({static_cast<std::uint64_t>(kRecoveryChunkBytes),
                                 limit - cursor,
                                 static_cast<std::uint64_t>(budget_left)}));
    if (want < next_record_size) {
      // The budget cannot hold this record. Returning at least one record
      // regardless is what stops an oversized record from wedging a consumer
      // forever; beyond the first, stop and let the client ask again.
      if (result.record_count > 0) break;
      want = next_record_size;
    }

    out.EnsureWritable(want);
    PL_ASSIGN_OR_RETURN(const std::size_t got, file_.ReadAt(out.WritePtr(), want, cursor));
    if (got < next_record_size) break;  // Truncated file; nothing more to give.

    const ByteSpan chunk(out.WritePtr(), got);
    std::size_t chunk_pos = 0;
    std::size_t accepted = 0;
    std::uint32_t accepted_records = 0;
    while (chunk_pos < chunk.size() && offset + accepted_records < end) {
      const auto length = protocol::PeekRecordLength(chunk, chunk_pos);
      if (!length.has_value()) break;
      const std::size_t record_size = static_cast<std::size_t>(*length) + 4;
      if (chunk_pos + record_size > chunk.size()) break;  // Straddles the chunk.
      const bool have_something = result.record_count + accepted_records > 0;
      if (have_something && result.bytes + accepted + record_size > max_bytes) break;
      accepted += record_size;
      chunk_pos += record_size;
      ++accepted_records;
    }

    if (accepted == 0) break;
    out.Commit(accepted);
    result.bytes += accepted;
    result.record_count += accepted_records;
    offset += accepted_records;
    cursor += accepted;
  }

  result.reached_end = (offset >= end);
  return result;
}

Status Segment::TruncateFrom(Offset offset) {
  if (offset >= next_offset_.load(std::memory_order_acquire)) return OkStatus();
  if (offset <= base_offset_) {
    PL_RETURN_IF_ERROR(file_.Truncate(0));
    PL_RETURN_IF_ERROR(index_.TruncateFrom(base_offset_));
    PL_RETURN_IF_ERROR(time_index_.TruncateFrom(base_offset_));
    end_position_.store(0, std::memory_order_release);
    next_offset_.store(base_offset_, std::memory_order_release);
    synced_position_.store(0, std::memory_order_release);
    record_count_.store(0, std::memory_order_relaxed);
    last_indexed_position_ = 0;
    preallocated_to_ = 0;
    return OkStatus();
  }

  PL_ASSIGN_OR_RETURN(const std::uint64_t position, PositionFor(offset));
  PL_RETURN_IF_ERROR(file_.Truncate(position));
  PL_RETURN_IF_ERROR(index_.TruncateFrom(offset));
  PL_RETURN_IF_ERROR(time_index_.TruncateFrom(offset));

  end_position_.store(position, std::memory_order_release);
  next_offset_.store(offset, std::memory_order_release);
  const std::uint64_t synced = synced_position_.load(std::memory_order_acquire);
  if (synced > position) synced_position_.store(position, std::memory_order_release);
  record_count_.store(static_cast<std::uint64_t>(offset - base_offset_), std::memory_order_relaxed);
  last_indexed_position_ = index_.LastIndexedPosition();
  preallocated_to_ = position;
  return OkStatus();
}

Status Segment::Sync() {
  // Capture the position first: anything written after this point is simply
  // covered by the next flush, and claiming more than was actually flushed
  // would be a durability lie.
  const std::uint64_t position = end_position_.load(std::memory_order_acquire);
  if (position == synced_position_.load(std::memory_order_acquire)) return OkStatus();

  PL_RETURN_IF_ERROR(file_.Sync(options_.sync_mode));
  PL_RETURN_IF_ERROR(index_.Sync());
  PL_RETURN_IF_ERROR(time_index_.Sync());
  synced_position_.store(position, std::memory_order_release);
  return OkStatus();
}

Status Segment::Close() {
  Status status = OkStatus();
  if (file_.valid()) {
    status = Sync();
    // Trim the preallocated tail so the on-disk size reflects real content.
    const std::uint64_t position = end_position_.load(std::memory_order_acquire);
    if (preallocated_to_ > position) {
      const Status truncate = file_.Truncate(position);
      if (status.ok() && !truncate.ok()) status = truncate;
    }
  }
  file_.Close();
  index_.Close();
  time_index_.Close();
  return status;
}

Status Segment::Delete() {
  std::error_code ec;
  std::filesystem::remove(log_path_, ec);
  std::filesystem::remove(index_path_, ec);
  std::filesystem::remove(time_index_path_, ec);
  if (ec) return IoError("removing segment " + log_path_.string() + ": " + ec.message());
  return OkStatus();
}

std::optional<Offset> Segment::OffsetForTimestamp(TimestampMs target) const {
  return time_index_.LookupCeiling(target);
}

}  // namespace pulselog::storage
