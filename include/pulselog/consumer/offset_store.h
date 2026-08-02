// Durable consumer-offset storage.
//
// Commits are appended to an internal partition log using the same storage
// engine, record format and checksums as user data. There is no second
// persistence mechanism to get wrong, and a corrupt commit is detected by the
// same CRC that protects everything else.
//
// The log is a *journal*: the in-memory map is the current state, rebuilt by
// replaying every record at start-up (last write wins). Compaction would bound
// replay time; it is not implemented, so start-up cost grows with the total
// number of commits ever made. The limitation is recorded in
// docs/FAILURE_SEMANTICS.md.
#ifndef PULSELOG_CONSUMER_OFFSET_STORE_H_
#define PULSELOG_CONSUMER_OFFSET_STORE_H_

#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "pulselog/base/status.h"
#include "pulselog/base/types.h"
#include "pulselog/storage/partition_log.h"

namespace pulselog::consumer {

struct CommittedOffset {
  Offset offset = kInvalidOffset;
  std::string metadata;
  TimestampMs commit_time = 0;
};

// Key identifying one committed position.
struct OffsetKey {
  std::string group_id;
  std::string topic;
  PartitionIndex partition{0};

  friend bool operator<(const OffsetKey& a, const OffsetKey& b) {
    if (a.group_id != b.group_id) return a.group_id < b.group_id;
    if (a.topic != b.topic) return a.topic < b.topic;
    return a.partition < b.partition;
  }
};

class OffsetStore {
 public:
  OffsetStore(const OffsetStore&) = delete;
  OffsetStore& operator=(const OffsetStore&) = delete;

  ~OffsetStore();

  // Opens (creating if needed) the offset log under `directory` and replays it.
  [[nodiscard]] static Result<std::unique_ptr<OffsetStore>> Open(
      const std::filesystem::path& directory, bool sync_on_commit);

  [[nodiscard]] Status Commit(const OffsetKey& key,
                              Offset offset,
                              std::string_view metadata,
                              TimestampMs now);

  [[nodiscard]] std::optional<CommittedOffset> Get(const OffsetKey& key) const;

  [[nodiscard]] std::map<OffsetKey, CommittedOffset> ForGroup(const std::string& group_id) const;

  [[nodiscard]] std::size_t Size() const;

  [[nodiscard]] Status Flush();

  [[nodiscard]] Status Close();

  // Number of records replayed at open, for the start-up log.
  [[nodiscard]] std::uint64_t replayed_records() const noexcept { return replayed_records_; }

 private:
  explicit OffsetStore(std::unique_ptr<storage::PartitionLog> log, bool sync_on_commit);

  [[nodiscard]] Status Replay();

  mutable std::mutex mutex_;
  std::unique_ptr<storage::PartitionLog> log_;
  std::map<OffsetKey, CommittedOffset> offsets_;
  bool sync_on_commit_ = false;
  std::uint64_t replayed_records_ = 0;
  ByteBuffer scratch_;
};

}  // namespace pulselog::consumer

#endif  // PULSELOG_CONSUMER_OFFSET_STORE_H_
