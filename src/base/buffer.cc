#include "pulselog/base/buffer.h"

#include <algorithm>

namespace pulselog {

void ByteBuffer::Compact() noexcept {
  if (read_pos_ == 0) return;
  const std::size_t readable = ReadableBytes();
  if (readable > 0) {
    std::memmove(storage_.data(), storage_.data() + read_pos_, readable);
  }
  read_pos_ = 0;
  write_pos_ = readable;
}

void ByteBuffer::EnsureWritable(std::size_t n) {
  if (WritableBytes() >= n) return;

  // Compaction first: a connection buffer that has drained most of its content
  // usually has enough room at the front, so growth is rare in steady state.
  if (read_pos_ > 0 && (storage_.size() - ReadableBytes()) >= n) {
    Compact();
    return;
  }

  const std::size_t required = ReadableBytes() + n;
  std::size_t new_size = storage_.empty() ? 1024 : storage_.size();
  while (new_size < required) new_size *= 2;

  Compact();
  storage_.resize(new_size);
}

std::unique_ptr<ByteBuffer> BufferPool::Acquire(std::size_t min_capacity) {
  const std::size_t capacity = std::max(min_capacity, options_.default_capacity);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    ++stats_.acquires;
    if (!free_list_.empty()) {
      auto buffer = std::move(free_list_.back());
      free_list_.pop_back();
      ++stats_.hits;
      stats_.pooled = free_list_.size();
      buffer->Clear();
      buffer->Reserve(capacity);
      return buffer;
    }
  }
  return std::make_unique<ByteBuffer>(capacity);
}

void BufferPool::Release(std::unique_ptr<ByteBuffer> buffer) {
  if (buffer == nullptr) return;
  buffer->Clear();

  std::lock_guard<std::mutex> lock(mutex_);
  ++stats_.releases;
  if (free_list_.size() >= options_.max_pooled) {
    ++stats_.discards;
    return;  // Buffer is freed here; the pool stays bounded.
  }
  if (buffer->Capacity() > options_.max_retained_capacity) {
    // An outlier request grew this buffer. Shrink rather than pin the peak.
    buffer->Shrink(options_.default_capacity);
  }
  free_list_.push_back(std::move(buffer));
  stats_.pooled = free_list_.size();
}

BufferPool::Stats BufferPool::GetStats() const {
  std::lock_guard<std::mutex> lock(mutex_);
  Stats copy = stats_;
  copy.pooled = free_list_.size();
  return copy;
}

}  // namespace pulselog
