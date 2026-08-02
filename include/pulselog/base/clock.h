// Time sources.
//
// Two distinct clocks are used and never mixed:
//   * `MonotonicNanos()` for durations, latency measurement and timeouts. It
//     is immune to wall-clock adjustments.
//   * `WallClockMillis()` for record timestamps that must be comparable across
//     machines and survive a restart.
#ifndef PULSELOG_BASE_CLOCK_H_
#define PULSELOG_BASE_CLOCK_H_

#include <atomic>
#include <chrono>
#include <cstdint>

#include "pulselog/base/types.h"

namespace pulselog {

[[nodiscard]] inline std::int64_t MonotonicNanos() noexcept {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

[[nodiscard]] inline std::int64_t MonotonicMicros() noexcept {
  return MonotonicNanos() / 1000;
}

[[nodiscard]] inline TimestampMs WallClockMillis() noexcept {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

// A coarse clock updated once per event-loop iteration.
//
// Timeout bookkeeping (connection idle deadlines, heartbeat expiry, batch
// linger) needs "roughly now", not a fresh syscall per connection. Reading the
// cached value turns an O(connections) sequence of clock reads per loop tick
// into one.
class CoarseClock {
 public:
  void Tick() noexcept { nanos_.store(MonotonicNanos(), std::memory_order_relaxed); }

  [[nodiscard]] std::int64_t Nanos() const noexcept {
    return nanos_.load(std::memory_order_relaxed);
  }

  [[nodiscard]] std::int64_t Millis() const noexcept { return Nanos() / 1'000'000; }

 private:
  std::atomic<std::int64_t> nanos_{MonotonicNanos()};
};

// Scoped stopwatch: reports elapsed nanoseconds since construction.
class Stopwatch {
 public:
  Stopwatch() noexcept : start_(MonotonicNanos()) {}

  [[nodiscard]] std::int64_t ElapsedNanos() const noexcept { return MonotonicNanos() - start_; }

  [[nodiscard]] double ElapsedMillis() const noexcept {
    return static_cast<double>(ElapsedNanos()) / 1e6;
  }

  void Reset() noexcept { start_ = MonotonicNanos(); }

 private:
  std::int64_t start_;
};

}  // namespace pulselog

#endif  // PULSELOG_BASE_CLOCK_H_
