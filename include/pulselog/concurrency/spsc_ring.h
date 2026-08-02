// Bounded single-producer / single-consumer ring buffer.
//
// This structure IS wait-free for both `TryPush` and `TryPop`: each operation
// executes a fixed number of instructions with no loops and no CAS. That
// property only holds under the stated constraint -- exactly one producer
// thread and exactly one consumer thread. Using it with more is undefined.
//
// Memory ordering
// ---------------
// The producer publishes with a release store to `write_index_`; the consumer
// reads it with acquire. That pairing makes the slot's construction visible
// before the index that exposes it. Symmetrically the consumer releases
// `read_index_` after destroying a slot so the producer cannot overwrite a
// slot that is still being read.
//
// Each index sits on its own cache line. Without the padding, the producer's
// store to `write_index_` invalidates the line holding `read_index_` that the
// consumer is reading, and vice versa -- classic false sharing. The effect was
// measured (benchmarks/bench_queues.cc); see docs/PERFORMANCE_RESULTS.md.
#ifndef PULSELOG_CONCURRENCY_SPSC_RING_H_
#define PULSELOG_CONCURRENCY_SPSC_RING_H_

#include <atomic>
#include <cstddef>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

#include "pulselog/base/types.h"

namespace pulselog {

template<typename T>
class SpscRing {
 public:
  // `capacity` is rounded up to a power of two so index wrapping is a mask.
  // One slot is reserved to distinguish full from empty, so the usable
  // capacity is (rounded capacity - 1).
  explicit SpscRing(std::size_t capacity) : mask_(RoundUpPow2(capacity + 1) - 1) {
    slots_ = std::make_unique<Slot[]>(mask_ + 1);
  }

  SpscRing(const SpscRing&) = delete;
  SpscRing& operator=(const SpscRing&) = delete;

  ~SpscRing() {
    // Destroy anything the consumer never drained.
    T item;
    while (TryPop(item)) {
    }
  }

  // Producer side. Returns false when the ring is full (the caller applies
  // backpressure rather than growing an unbounded queue).
  template<typename U>
  [[nodiscard]] bool TryPush(U&& value) {
    const std::size_t write = write_index_.load(std::memory_order_relaxed);
    const std::size_t next = (write + 1) & mask_;
    if (next == cached_read_index_) {
      // Refresh the cached consumer position only when we appear to be full.
      // This keeps the consumer's cache line out of the fast path.
      cached_read_index_ = read_index_.load(std::memory_order_acquire);
      if (next == cached_read_index_) return false;
    }
    slots_[write].Construct(std::forward<U>(value));
    write_index_.store(next, std::memory_order_release);
    return true;
  }

  // Consumer side. Returns false when empty.
  [[nodiscard]] bool TryPop(T& out) {
    const std::size_t read = read_index_.load(std::memory_order_relaxed);
    if (read == cached_write_index_) {
      cached_write_index_ = write_index_.load(std::memory_order_acquire);
      if (read == cached_write_index_) return false;
    }
    out = std::move(slots_[read].Get());
    slots_[read].Destroy();
    read_index_.store((read + 1) & mask_, std::memory_order_release);
    return true;
  }

  // Approximate size. Exact only when observed from a thread that is the sole
  // mutator of both indices, i.e. never in production use -- it exists for
  // metrics and tests.
  [[nodiscard]] std::size_t SizeApprox() const noexcept {
    const std::size_t write = write_index_.load(std::memory_order_acquire);
    const std::size_t read = read_index_.load(std::memory_order_acquire);
    return (write - read) & mask_;
  }

  [[nodiscard]] std::size_t Capacity() const noexcept { return mask_; }

  [[nodiscard]] bool EmptyApprox() const noexcept {
    return write_index_.load(std::memory_order_acquire) ==
           read_index_.load(std::memory_order_acquire);
  }

 private:
  // Manual storage so slots are not default-constructed and moved-from objects
  // are destroyed promptly rather than lingering until the ring dies.
  struct Slot {
    alignas(T) std::byte storage[sizeof(T)];

    template<typename U>
    void Construct(U&& value) {
      ::new (static_cast<void*>(storage)) T(std::forward<U>(value));
    }

    T& Get() noexcept { return *std::launder(reinterpret_cast<T*>(storage)); }

    void Destroy() noexcept { Get().~T(); }
  };

  static constexpr std::size_t RoundUpPow2(std::size_t v) noexcept {
    std::size_t result = 1;
    while (result < v) result <<= 1U;
    return result;
  }

  const std::size_t mask_;
  std::unique_ptr<Slot[]> slots_;

  alignas(kCacheLineSize) std::atomic<std::size_t> write_index_{0};
  // Producer-private cache of the consumer's position.
  alignas(kCacheLineSize) std::size_t cached_read_index_{0};

  alignas(kCacheLineSize) std::atomic<std::size_t> read_index_{0};
  // Consumer-private cache of the producer's position.
  alignas(kCacheLineSize) std::size_t cached_write_index_{0};

  // Trailing padding stops the last member from sharing a line with whatever
  // the allocator places after this object.
  alignas(kCacheLineSize) std::byte tail_padding_[kCacheLineSize]{};
};

}  // namespace pulselog

#endif  // PULSELOG_CONCURRENCY_SPSC_RING_H_
