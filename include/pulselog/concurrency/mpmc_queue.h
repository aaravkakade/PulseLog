// Bounded multi-producer / multi-consumer queue (Dmitry Vyukov's design).
//
// Lock-freedom claim, stated precisely: `TryPush` and `TryPop` are lock-free,
// not wait-free. Each contains a CAS retry loop, so an individual thread can
// be starved in principle, but system-wide progress is guaranteed because a
// failed CAS means some other thread succeeded. No operation ever blocks, and
// a thread suspended mid-operation never prevents others from completing --
// which is the property that matters here, since producers are network event
// loops that must not be blocked by a descheduled worker.
//
// Memory ordering
// ---------------
// Each cell carries a sequence number acting as a ticket:
//   * a cell is writable when sequence == position,
//   * readable when sequence == position + 1.
// Producers acquire the cell (acquire load on sequence), construct, then
// release-store sequence = position + 1, publishing the payload. Consumers do
// the mirror image and release-store sequence = position + capacity, handing
// the slot back for the next lap. The enqueue/dequeue position counters use
// relaxed loads and acq_rel CAS: their only job is to hand out distinct
// tickets, and all payload visibility comes from the per-cell sequence.
#ifndef PULSELOG_CONCURRENCY_MPMC_QUEUE_H_
#define PULSELOG_CONCURRENCY_MPMC_QUEUE_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <utility>

#include "pulselog/base/types.h"

namespace pulselog {

template <typename T>
class BoundedMpmcQueue {
 public:
  explicit BoundedMpmcQueue(std::size_t capacity) : mask_(RoundUpPow2(capacity) - 1) {
    cells_ = std::make_unique<Cell[]>(mask_ + 1);
    for (std::size_t i = 0; i <= mask_; ++i) {
      cells_[i].sequence.store(i, std::memory_order_relaxed);
    }
  }

  BoundedMpmcQueue(const BoundedMpmcQueue&) = delete;
  BoundedMpmcQueue& operator=(const BoundedMpmcQueue&) = delete;

  ~BoundedMpmcQueue() {
    T item;
    while (TryPop(item)) {
    }
  }

  template <typename U>
  [[nodiscard]] bool TryPush(U&& value) {
    Cell* cell = nullptr;
    std::size_t pos = enqueue_pos_.load(std::memory_order_relaxed);
    for (;;) {
      cell = &cells_[pos & mask_];
      const std::size_t seq = cell->sequence.load(std::memory_order_acquire);
      const auto diff = static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos);
      if (diff == 0) {
        // Cell is free and it is our turn: claim the ticket.
        if (enqueue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_acq_rel,
                                               std::memory_order_relaxed)) {
          break;
        }
        // CAS failure refreshed `pos`; retry with the new value.
      } else if (diff < 0) {
        return false;  // Queue full: the consumer has not caught up a full lap.
      } else {
        pos = enqueue_pos_.load(std::memory_order_relaxed);
      }
    }
    cell->Construct(std::forward<U>(value));
    cell->sequence.store(pos + 1, std::memory_order_release);
    return true;
  }

  [[nodiscard]] bool TryPop(T& out) {
    Cell* cell = nullptr;
    std::size_t pos = dequeue_pos_.load(std::memory_order_relaxed);
    for (;;) {
      cell = &cells_[pos & mask_];
      const std::size_t seq = cell->sequence.load(std::memory_order_acquire);
      const auto diff = static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos + 1);
      if (diff == 0) {
        if (dequeue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_acq_rel,
                                               std::memory_order_relaxed)) {
          break;
        }
      } else if (diff < 0) {
        return false;  // Empty.
      } else {
        pos = dequeue_pos_.load(std::memory_order_relaxed);
      }
    }
    out = std::move(cell->Get());
    cell->Destroy();
    cell->sequence.store(pos + mask_ + 1, std::memory_order_release);
    return true;
  }

  // Snapshot of the depth. Racy by nature; used for the queue-depth metric and
  // for the backpressure heuristic, both of which tolerate staleness.
  [[nodiscard]] std::size_t SizeApprox() const noexcept {
    const std::size_t enqueue = enqueue_pos_.load(std::memory_order_relaxed);
    const std::size_t dequeue = dequeue_pos_.load(std::memory_order_relaxed);
    return enqueue > dequeue ? enqueue - dequeue : 0;
  }

  [[nodiscard]] std::size_t Capacity() const noexcept { return mask_ + 1; }

 private:
  struct Cell {
    std::atomic<std::size_t> sequence;
    alignas(T) std::byte storage[sizeof(T)];

    template <typename U>
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
  std::unique_ptr<Cell[]> cells_;
  alignas(kCacheLineSize) std::atomic<std::size_t> enqueue_pos_{0};
  alignas(kCacheLineSize) std::atomic<std::size_t> dequeue_pos_{0};
  alignas(kCacheLineSize) std::byte tail_padding_[kCacheLineSize]{};
};

}  // namespace pulselog

#endif  // PULSELOG_CONCURRENCY_MPMC_QUEUE_H_
