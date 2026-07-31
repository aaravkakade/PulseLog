// Bounded blocking queue: mutex + condition variables.
//
// Used where a consumer genuinely has nothing else to do until work arrives
// (the flush thread, the replication sender) and as the baseline that the
// lock-free queues are measured against in benchmarks/bench_queues.cc.
//
// Bounded on purpose: `Push` fails or blocks when full instead of growing.
// Unbounded queues turn a transient producer/consumer imbalance into an
// out-of-memory kill, which is the failure mode this project is trying to
// avoid.
#ifndef PULSELOG_CONCURRENCY_BLOCKING_QUEUE_H_
#define PULSELOG_CONCURRENCY_BLOCKING_QUEUE_H_

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>
#include <optional>
#include <utility>

namespace pulselog {

template <typename T>
class BoundedBlockingQueue {
 public:
  explicit BoundedBlockingQueue(std::size_t capacity) : capacity_(capacity) {}

  BoundedBlockingQueue(const BoundedBlockingQueue&) = delete;
  BoundedBlockingQueue& operator=(const BoundedBlockingQueue&) = delete;

  // Non-blocking. Returns false when the queue is full or closed.
  template <typename U>
  [[nodiscard]] bool TryPush(U&& value) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (closed_ || items_.size() >= capacity_) return false;
      items_.emplace_back(std::forward<U>(value));
    }
    not_empty_.notify_one();
    return true;
  }

  // Blocks until space is available, the deadline passes, or the queue closes.
  template <typename U>
  [[nodiscard]] bool PushFor(U&& value, std::chrono::milliseconds timeout) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      if (!not_full_.wait_for(lock, timeout,
                              [this] { return closed_ || items_.size() < capacity_; })) {
        return false;
      }
      if (closed_) return false;
      items_.emplace_back(std::forward<U>(value));
    }
    not_empty_.notify_one();
    return true;
  }

  // Blocks until an item is available or the queue is closed and drained.
  [[nodiscard]] std::optional<T> Pop() {
    std::unique_lock<std::mutex> lock(mutex_);
    not_empty_.wait(lock, [this] { return closed_ || !items_.empty(); });
    if (items_.empty()) return std::nullopt;  // Closed and drained.
    T value = std::move(items_.front());
    items_.pop_front();
    lock.unlock();
    not_full_.notify_one();
    return value;
  }

  [[nodiscard]] std::optional<T> PopFor(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!not_empty_.wait_for(lock, timeout, [this] { return closed_ || !items_.empty(); })) {
      return std::nullopt;
    }
    if (items_.empty()) return std::nullopt;
    T value = std::move(items_.front());
    items_.pop_front();
    lock.unlock();
    not_full_.notify_one();
    return value;
  }

  // Drains up to `max_items` in one lock acquisition. Batching the handoff is
  // what makes the mutex variant competitive: one lock per batch rather than
  // one per item.
  [[nodiscard]] std::size_t PopBatch(std::vector<T>& out, std::size_t max_items,
                                     std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!not_empty_.wait_for(lock, timeout, [this] { return closed_ || !items_.empty(); })) {
      return 0;
    }
    const std::size_t count = std::min(max_items, items_.size());
    for (std::size_t i = 0; i < count; ++i) {
      out.emplace_back(std::move(items_.front()));
      items_.pop_front();
    }
    lock.unlock();
    if (count > 0) not_full_.notify_all();
    return count;
  }

  // Wakes every waiter. Consumers drain remaining items, then see nullopt.
  void Close() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      closed_ = true;
    }
    not_empty_.notify_all();
    not_full_.notify_all();
  }

  [[nodiscard]] bool closed() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return closed_;
  }

  [[nodiscard]] std::size_t Size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return items_.size();
  }

  [[nodiscard]] std::size_t Capacity() const noexcept { return capacity_; }

 private:
  mutable std::mutex mutex_;
  std::condition_variable not_empty_;
  std::condition_variable not_full_;
  std::deque<T> items_;
  const std::size_t capacity_;
  bool closed_ = false;
};

}  // namespace pulselog

#endif  // PULSELOG_CONCURRENCY_BLOCKING_QUEUE_H_
