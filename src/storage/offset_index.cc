#include "pulselog/storage/offset_index.h"

#include <algorithm>
#include <array>

#include "pulselog/base/endian.h"
#include "pulselog/base/logging.h"

namespace pulselog::storage {
namespace {

constexpr std::string_view kComponent = "storage.index";

constexpr std::size_t kTimeIndexEntryBytes = 12;

}  // namespace

Status OffsetIndex::Open(const std::filesystem::path& path, Offset base_offset) {
  path_ = path;
  base_offset_ = base_offset;
  entries_.clear();

  PL_ASSIGN_OR_RETURN(file_, FileHandle::Open(path, /*create=*/true));
  PL_ASSIGN_OR_RETURN(const std::uint64_t size, file_.Size());

  const std::uint64_t whole = size - (size % kIndexEntryBytes);
  if (whole != size) {
    // A partial entry means the process died mid-append. The log itself is the
    // source of truth, so discarding the fragment is always safe.
    PL_WARN(kComponent) << "discarding partial index entry"
                        << " path=" << path.filename().string() << " bytes=" << (size - whole);
    PL_RETURN_IF_ERROR(file_.Truncate(whole));
  }
  if (whole == 0) return OkStatus();

  std::vector<std::uint8_t> raw(static_cast<std::size_t>(whole));
  PL_RETURN_IF_ERROR(file_.ReadExactAt(raw.data(), raw.size(), 0));

  entries_.reserve(raw.size() / kIndexEntryBytes);
  Offset previous_relative = -1;
  for (std::size_t pos = 0; pos + kIndexEntryBytes <= raw.size(); pos += kIndexEntryBytes) {
    OffsetIndexEntry entry;
    entry.relative_offset = LoadLe<std::uint32_t>(raw.data() + pos);
    entry.file_position = LoadLe<std::uint32_t>(raw.data() + pos + 4);
    // Entries must be strictly increasing. Anything else means a corrupt
    // index; the log can always rebuild it, so stop here rather than serve
    // wrong positions.
    if (static_cast<Offset>(entry.relative_offset) <= previous_relative) {
      PL_WARN(kComponent) << "index is not strictly increasing; truncating"
                          << " path=" << path.filename().string()
                          << " entries_kept=" << entries_.size();
      PL_RETURN_IF_ERROR(file_.Truncate(pos));
      break;
    }
    previous_relative = static_cast<Offset>(entry.relative_offset);
    entries_.push_back(entry);
  }
  return OkStatus();
}

Status OffsetIndex::Append(Offset offset, std::uint64_t position) {
  const Offset relative = offset - base_offset_;
  if (relative < 0 || relative > static_cast<Offset>(UINT32_MAX)) {
    return InvalidArgument("offset " + std::to_string(offset) + " is outside segment based at " +
                           std::to_string(base_offset_));
  }
  if (position > UINT32_MAX) {
    return InvalidArgument("segment position " + std::to_string(position) +
                           " exceeds the 4 GiB segment limit");
  }
  if (!entries_.empty() &&
      static_cast<std::uint32_t>(relative) <= entries_.back().relative_offset) {
    return InvalidArgument("index entries must strictly increase");
  }

  OffsetIndexEntry entry{static_cast<std::uint32_t>(relative),
                         static_cast<std::uint32_t>(position)};

  std::array<std::uint8_t, kIndexEntryBytes> buf{};
  StoreLe<std::uint32_t>(buf.data(), entry.relative_offset);
  StoreLe<std::uint32_t>(buf.data() + 4, entry.file_position);
  PL_RETURN_IF_ERROR(file_.WriteAllAt(buf, entries_.size() * kIndexEntryBytes));

  entries_.push_back(entry);
  return OkStatus();
}

std::optional<OffsetIndexEntry> OffsetIndex::LookupFloor(Offset target) const {
  if (entries_.empty() || target < base_offset_) return std::nullopt;
  const Offset relative = target - base_offset_;
  if (relative > static_cast<Offset>(UINT32_MAX)) return entries_.back();

  // Largest entry with relative_offset <= relative.
  const auto it = std::upper_bound(entries_.begin(),
                                   entries_.end(),
                                   static_cast<std::uint32_t>(relative),
                                   [](std::uint32_t value, const OffsetIndexEntry& entry) {
                                     return value < entry.relative_offset;
                                   });
  if (it == entries_.begin()) return std::nullopt;
  return *(it - 1);
}

Status OffsetIndex::TruncateFrom(Offset offset) {
  const Offset relative = offset - base_offset_;
  const auto it = std::lower_bound(
      entries_.begin(), entries_.end(), relative, [](const OffsetIndexEntry& entry, Offset value) {
        return static_cast<Offset>(entry.relative_offset) < value;
      });
  const std::size_t keep = static_cast<std::size_t>(it - entries_.begin());
  if (keep == entries_.size()) return OkStatus();

  entries_.resize(keep);
  return file_.Truncate(keep * kIndexEntryBytes);
}

Status OffsetIndex::Sync() const {
  if (!file_.valid()) return OkStatus();
  return file_.SyncData();
}

void OffsetIndex::Close() {
  file_.Close();
  entries_.clear();
}

// --- TimeIndex -------------------------------------------------------------

Status TimeIndex::Open(const std::filesystem::path& path, Offset base_offset) {
  path_ = path;
  base_offset_ = base_offset;
  entries_.clear();

  PL_ASSIGN_OR_RETURN(file_, FileHandle::Open(path, /*create=*/true));
  PL_ASSIGN_OR_RETURN(const std::uint64_t size, file_.Size());

  const std::uint64_t whole = size - (size % kTimeIndexEntryBytes);
  if (whole != size) {
    PL_RETURN_IF_ERROR(file_.Truncate(whole));
  }
  if (whole == 0) return OkStatus();

  std::vector<std::uint8_t> raw(static_cast<std::size_t>(whole));
  PL_RETURN_IF_ERROR(file_.ReadExactAt(raw.data(), raw.size(), 0));

  entries_.reserve(raw.size() / kTimeIndexEntryBytes);
  TimestampMs previous_timestamp = -1;
  for (std::size_t pos = 0; pos + kTimeIndexEntryBytes <= raw.size(); pos += kTimeIndexEntryBytes) {
    TimeIndexEntry entry;
    entry.timestamp = LoadLeI64(raw.data() + pos);
    entry.relative_offset = LoadLe<std::uint32_t>(raw.data() + pos + 8);
    if (entry.timestamp < previous_timestamp) {
      PL_RETURN_IF_ERROR(file_.Truncate(pos));
      break;
    }
    previous_timestamp = entry.timestamp;
    entries_.push_back(entry);
  }
  return OkStatus();
}

Status TimeIndex::Append(TimestampMs timestamp, Offset offset) {
  // Producer clocks are not trustworthy. Rather than let one skewed record
  // break the binary-search invariant for the whole segment, non-monotonic
  // timestamps are simply not indexed. Time lookups then resolve to a slightly
  // earlier offset, which is the safe direction: a consumer sees extra
  // records, never fewer.
  if (!entries_.empty() && timestamp < entries_.back().timestamp) return OkStatus();

  const Offset relative = offset - base_offset_;
  if (relative < 0 || relative > static_cast<Offset>(UINT32_MAX)) {
    return InvalidArgument("offset outside segment for time index");
  }

  TimeIndexEntry entry{timestamp, static_cast<std::uint32_t>(relative)};
  std::array<std::uint8_t, kTimeIndexEntryBytes> buf{};
  StoreLeI64(buf.data(), entry.timestamp);
  StoreLe<std::uint32_t>(buf.data() + 8, entry.relative_offset);
  PL_RETURN_IF_ERROR(file_.WriteAllAt(buf, entries_.size() * kTimeIndexEntryBytes));

  entries_.push_back(entry);
  return OkStatus();
}

std::optional<Offset> TimeIndex::LookupCeiling(TimestampMs timestamp) const {
  if (entries_.empty()) return std::nullopt;
  const auto it = std::lower_bound(
      entries_.begin(),
      entries_.end(),
      timestamp,
      [](const TimeIndexEntry& entry, TimestampMs value) { return entry.timestamp < value; });
  if (it == entries_.end()) return std::nullopt;
  return base_offset_ + static_cast<Offset>(it->relative_offset);
}

Status TimeIndex::TruncateFrom(Offset offset) {
  const Offset relative = offset - base_offset_;
  const auto it = std::lower_bound(
      entries_.begin(), entries_.end(), relative, [](const TimeIndexEntry& entry, Offset value) {
        return static_cast<Offset>(entry.relative_offset) < value;
      });
  const std::size_t keep = static_cast<std::size_t>(it - entries_.begin());
  if (keep == entries_.size()) return OkStatus();

  entries_.resize(keep);
  return file_.Truncate(keep * kTimeIndexEntryBytes);
}

Status TimeIndex::Sync() const {
  if (!file_.valid()) return OkStatus();
  return file_.SyncData();
}

void TimeIndex::Close() {
  file_.Close();
  entries_.clear();
}

}  // namespace pulselog::storage
