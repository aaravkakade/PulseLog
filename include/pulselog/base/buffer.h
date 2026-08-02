// Byte buffers and the pooled allocator used by the networking and storage
// layers.
//
// `ByteBuffer` is a growable byte array with independent read and write
// cursors, which is what a stream socket needs: bytes arrive at the tail and
// are consumed from the head, and neither side can assume message boundaries.
// It deliberately does not use std::vector because the common operation --
// "consume the first N bytes and keep the rest" -- is a cursor bump plus an
// occasional compaction rather than an erase-from-front.
#ifndef PULSELOG_BASE_BUFFER_H_
#define PULSELOG_BASE_BUFFER_H_

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <span>
#include <string_view>
#include <vector>

#include "pulselog/base/types.h"

namespace pulselog {

using ByteSpan = std::span<const std::uint8_t>;

using MutableByteSpan = std::span<std::uint8_t>;

inline ByteSpan AsBytes(std::string_view s) noexcept {
  return {reinterpret_cast<const std::uint8_t*>(s.data()), s.size()};
}

inline std::string_view AsStringView(ByteSpan b) noexcept {
  return {reinterpret_cast<const char*>(b.data()), b.size()};
}

class ByteBuffer {
 public:
  ByteBuffer() = default;

  explicit ByteBuffer(std::size_t initial_capacity) { Reserve(initial_capacity); }

  ByteBuffer(const ByteBuffer&) = delete;
  ByteBuffer& operator=(const ByteBuffer&) = delete;
  ByteBuffer(ByteBuffer&&) noexcept = default;
  ByteBuffer& operator=(ByteBuffer&&) noexcept = default;
  ~ByteBuffer() = default;

  // --- readable region -----------------------------------------------------
  [[nodiscard]] const std::uint8_t* ReadPtr() const noexcept { return storage_.data() + read_pos_; }

  [[nodiscard]] std::size_t ReadableBytes() const noexcept { return write_pos_ - read_pos_; }

  [[nodiscard]] ByteSpan Readable() const noexcept { return {ReadPtr(), ReadableBytes()}; }

  [[nodiscard]] bool Empty() const noexcept { return read_pos_ == write_pos_; }

  // Marks `n` bytes as consumed. Resets both cursors when the buffer drains,
  // which keeps long-lived connection buffers from creeping forward forever.
  void Consume(std::size_t n) noexcept {
    read_pos_ += n;
    if (read_pos_ == write_pos_) {
      read_pos_ = 0;
      write_pos_ = 0;
    }
  }

  // --- writable region -----------------------------------------------------
  [[nodiscard]] std::uint8_t* WritePtr() noexcept { return storage_.data() + write_pos_; }

  [[nodiscard]] std::size_t WritableBytes() const noexcept { return storage_.size() - write_pos_; }

  [[nodiscard]] MutableByteSpan Writable() noexcept { return {WritePtr(), WritableBytes()}; }

  void Commit(std::size_t n) noexcept { write_pos_ += n; }

  // Guarantees at least `n` writable bytes, compacting before growing so that
  // a steady-state connection reuses one allocation.
  void EnsureWritable(std::size_t n);

  void Reserve(std::size_t capacity) {
    if (storage_.size() < capacity) storage_.resize(capacity);
  }

  // The empty check is not an optimisation. std::memcpy has undefined
  // behaviour when either pointer is null, *even when the length is zero*, and
  // an empty ByteSpan routinely carries a null data(). UndefinedBehaviorSanitizer
  // on Linux flags it ("null pointer passed as argument 2, which is declared to
  // never be null"); the macOS build happened not to hit the same call sites.
  void Append(ByteSpan data) {
    if (data.empty()) return;
    EnsureWritable(data.size());
    std::memcpy(WritePtr(), data.data(), data.size());
    Commit(data.size());
  }

  void Append(const void* data, std::size_t size) {
    if (size == 0 || data == nullptr) return;
    EnsureWritable(size);
    std::memcpy(WritePtr(), data, size);
    Commit(size);
  }

  void AppendByte(std::uint8_t b) {
    EnsureWritable(1);
    *WritePtr() = b;
    Commit(1);
  }

  // Drops all content but keeps the allocation.
  void Clear() noexcept {
    read_pos_ = 0;
    write_pos_ = 0;
  }

  // Releases memory back to the allocator. Used when a pooled buffer has grown
  // past the pool's retention threshold.
  void Shrink(std::size_t target_capacity) {
    Compact();
    if (storage_.size() > target_capacity) {
      storage_.resize(target_capacity);
      storage_.shrink_to_fit();
    }
  }

  [[nodiscard]] std::size_t Capacity() const noexcept { return storage_.size(); }

  // Moves unread bytes to the front. Exposed for tests; callers normally rely
  // on EnsureWritable doing this automatically.
  void Compact() noexcept;

 private:
  std::vector<std::uint8_t> storage_;
  std::size_t read_pos_ = 0;
  std::size_t write_pos_ = 0;
};

// A fixed-capacity pool of ByteBuffers.
//
// Rationale: every produce request needs a scratch buffer for the encoded
// response and every connection needs read/write buffers. Allocating them per
// request showed up clearly in profiles (see docs/PERFORMANCE_RESULTS.md,
// "buffer reuse"). The pool is mutex-protected because acquisition happens
// once per request, not once per record -- a lock-free free-list was measured
// and did not move the number, so the simpler structure was kept.
struct BufferPoolOptions {
  std::size_t max_pooled = 256;  // Buffers retained when idle.
  std::size_t default_capacity = 64 * 1024;
  std::size_t max_retained_capacity = 1024 * 1024;  // Larger buffers are shrunk.
};

class BufferPool {
 public:
  using Options = BufferPoolOptions;

  BufferPool() = default;

  explicit BufferPool(const Options& options) : options_(options) {}

  BufferPool(const BufferPool&) = delete;
  BufferPool& operator=(const BufferPool&) = delete;

  // Returns a cleared buffer with at least `min_capacity` bytes of storage.
  [[nodiscard]] std::unique_ptr<ByteBuffer> Acquire(std::size_t min_capacity = 0);

  void Release(std::unique_ptr<ByteBuffer> buffer);

  struct Stats {
    std::uint64_t acquires = 0;
    std::uint64_t hits = 0;
    std::uint64_t releases = 0;
    std::uint64_t discards = 0;
    std::size_t pooled = 0;
  };

  [[nodiscard]] Stats GetStats() const;

 private:
  mutable std::mutex mutex_;
  std::vector<std::unique_ptr<ByteBuffer>> free_list_;
  Options options_;
  Stats stats_;
};

// RAII handle that returns a buffer to its pool on destruction.
class PooledBuffer {
 public:
  PooledBuffer() = default;

  PooledBuffer(BufferPool& pool, std::unique_ptr<ByteBuffer> buffer)
      : pool_(&pool), buffer_(std::move(buffer)) {}

  PooledBuffer(const PooledBuffer&) = delete;
  PooledBuffer& operator=(const PooledBuffer&) = delete;

  PooledBuffer(PooledBuffer&& other) noexcept
      : pool_(other.pool_), buffer_(std::move(other.buffer_)) {
    other.pool_ = nullptr;
  }

  PooledBuffer& operator=(PooledBuffer&& other) noexcept {
    if (this != &other) {
      Reset();
      pool_ = other.pool_;
      buffer_ = std::move(other.buffer_);
      other.pool_ = nullptr;
    }
    return *this;
  }

  ~PooledBuffer() { Reset(); }

  [[nodiscard]] bool valid() const noexcept { return buffer_ != nullptr; }

  ByteBuffer& operator*() noexcept { return *buffer_; }

  ByteBuffer* operator->() noexcept { return buffer_.get(); }

  [[nodiscard]] ByteBuffer* get() noexcept { return buffer_.get(); }

  [[nodiscard]] const ByteBuffer* get() const noexcept { return buffer_.get(); }

  void Reset() {
    if (pool_ != nullptr && buffer_ != nullptr) {
      pool_->Release(std::move(buffer_));
    }
    buffer_.reset();
    pool_ = nullptr;
  }

 private:
  BufferPool* pool_ = nullptr;
  std::unique_ptr<ByteBuffer> buffer_;
};

}  // namespace pulselog

#endif  // PULSELOG_BASE_BUFFER_H_
