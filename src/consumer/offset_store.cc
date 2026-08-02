#include "pulselog/consumer/offset_store.h"

#include "pulselog/base/clock.h"
#include "pulselog/base/logging.h"
#include "pulselog/protocol/codec.h"
#include "pulselog/protocol/record.h"

namespace pulselog::consumer {
namespace {

constexpr std::string_view kComponent = "consumer.offsets";

// Key encoding: group, topic and partition, length-prefixed so a group named
// "a\0b" cannot collide with two separate fields.
void EncodeKey(ByteBuffer& out, const OffsetKey& key) {
  protocol::PayloadWriter writer(out);
  writer.PutString(key.group_id);
  writer.PutString(key.topic);
  writer.PutI32(key.partition.value());
}

[[nodiscard]] bool DecodeKey(ByteSpan bytes, OffsetKey& key) {
  protocol::PayloadReader reader(bytes);
  std::int32_t partition = 0;
  if (!reader.GetString(key.group_id)) return false;
  if (!reader.GetString(key.topic)) return false;
  if (!reader.GetI32(partition)) return false;
  if (partition < 0) return false;
  key.partition = PartitionIndex{partition};
  return reader.Complete();
}

void EncodeValue(ByteBuffer& out, Offset offset, std::string_view metadata, TimestampMs now) {
  protocol::PayloadWriter writer(out);
  writer.PutI64(offset);
  writer.PutI64(now);
  writer.PutString(metadata);
}

[[nodiscard]] bool DecodeValue(ByteSpan bytes, CommittedOffset& value) {
  protocol::PayloadReader reader(bytes);
  if (!reader.GetI64(value.offset)) return false;
  if (!reader.GetI64(value.commit_time)) return false;
  if (!reader.GetString(value.metadata)) return false;
  return reader.Complete();
}

}  // namespace

OffsetStore::OffsetStore(std::unique_ptr<storage::PartitionLog> log, bool sync_on_commit)
    : log_(std::move(log)), sync_on_commit_(sync_on_commit) {}

OffsetStore::~OffsetStore() {
  const Status status = Close();
  if (!status.ok()) {
    PL_ERROR(kComponent) << "offset store close failed: " << status.ToString();
  }
}

Result<std::unique_ptr<OffsetStore>> OffsetStore::Open(const std::filesystem::path& directory,
                                                       bool sync_on_commit) {
  storage::LogOptions options;
  options.directory = directory;
  // Small segments: the offset log is low volume and this keeps recovery
  // scans short.
  options.segment_bytes = 8LL * 1024 * 1024;
  options.index_interval_bytes = 4096;
  options.preallocate = false;
  options.retention_bytes = -1;  // Never delete: the journal is the state.
  options.retention_ms = -1;
  options.flush.sync_on_append = sync_on_commit;
  options.flush.interval_ms = 200;
  options.flush.max_unflushed_bytes = 256 * 1024;
  options.flush.max_unflushed_records = 100;

  PL_ASSIGN_OR_RETURN(auto log,
                      storage::PartitionLog::Open(
                          TopicPartition{"__offsets", PartitionIndex{0}}, options, nullptr));

  std::unique_ptr<OffsetStore> store(new OffsetStore(std::move(log), sync_on_commit));
  PL_RETURN_IF_ERROR(store->Replay());
  PL_INFO(kComponent) << "offset store ready"
                      << " entries=" << store->offsets_.size()
                      << " replayed_records=" << store->replayed_records_;
  return store;
}

Status OffsetStore::Replay() {
  const Offset end = log_->LogEndOffset();
  if (end == 0) return OkStatus();

  ByteBuffer buffer;
  Offset cursor = log_->LogStartOffset();
  while (cursor < end) {
    buffer.Clear();
    PL_ASSIGN_OR_RETURN(const storage::LogReadResult read, log_->Read(cursor, 1 << 20, buffer));
    if (read.record_count == 0) break;

    protocol::RecordIterator it(buffer.Readable(), /*verify_crc=*/true);
    protocol::RecordView view;
    while (it.Next(view)) {
      OffsetKey key;
      CommittedOffset value;
      if (!DecodeKey(view.key, key)) {
        PL_WARN(kComponent) << "skipping offset record with an unparseable key at offset "
                            << view.offset;
        continue;
      }
      if (view.tombstone()) {
        offsets_.erase(key);
      } else if (DecodeValue(view.value, value)) {
        // Last write wins: the log is replayed in order, so a later commit for
        // the same key simply overwrites the earlier one.
        offsets_[key] = value;
      } else {
        PL_WARN(kComponent) << "skipping offset record with an unparseable value at offset "
                            << view.offset;
      }
      ++replayed_records_;
    }
    if (!it.status().ok()) {
      // Recovery already truncated anything damaged, so this would mean a bug.
      return it.status().WithContext("replaying the offset log");
    }
    cursor += read.record_count;
  }
  return OkStatus();
}

Status OffsetStore::Commit(const OffsetKey& key,
                           Offset offset,
                           std::string_view metadata,
                           TimestampMs now) {
  std::lock_guard<std::mutex> lock(mutex_);

  ByteBuffer key_bytes;
  EncodeKey(key_bytes, key);
  ByteBuffer value_bytes;
  EncodeValue(value_bytes, offset, metadata, now);

  scratch_.Clear();
  protocol::AppendRecord(scratch_,
                         /*offset=*/0,
                         now,
                         /*attributes=*/0,
                         /*key_is_null=*/false,
                         key_bytes.Readable(),
                         value_bytes.Readable());

  const MutableByteSpan records(const_cast<std::uint8_t*>(scratch_.ReadPtr()),
                                scratch_.ReadableBytes());
  auto appended = log_->AppendAssigningOffsets(records, 1);
  if (!appended.ok()) return appended.status().WithContext("committing consumer offset");

  CommittedOffset value;
  value.offset = offset;
  value.metadata = std::string(metadata);
  value.commit_time = now;
  offsets_[key] = std::move(value);
  return OkStatus();
}

std::optional<CommittedOffset> OffsetStore::Get(const OffsetKey& key) const {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto it = offsets_.find(key);
  if (it == offsets_.end()) return std::nullopt;
  return it->second;
}

std::map<OffsetKey, CommittedOffset> OffsetStore::ForGroup(const std::string& group_id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::map<OffsetKey, CommittedOffset> result;
  for (const auto& [key, value] : offsets_) {
    if (key.group_id == group_id) result[key] = value;
  }
  return result;
}

std::size_t OffsetStore::Size() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return offsets_.size();
}

Status OffsetStore::Flush() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (log_ == nullptr) return OkStatus();
  return log_->Flush();
}

Status OffsetStore::Close() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (log_ == nullptr) return OkStatus();
  const Status flush = log_->Flush();
  const Status close = log_->Close();
  log_.reset();
  return flush.ok() ? close : flush;
}

}  // namespace pulselog::consumer
