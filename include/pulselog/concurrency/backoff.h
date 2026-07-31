// Spin/backoff primitives.
//
// Busy-waiting is used in exactly two places in PulseLog and both are bounded:
//   * the Vyukov MPMC queue's CAS retry loop, where contention is over a single
//     slot and the expected retry count is under a handful, and
//   * `SpinThenYield`, used by the flush thread when it is waiting for a very
//     short, known-bounded interval.
// Everything else blocks on a condition variable or on the event loop's poller.
#ifndef PULSELOG_CONCURRENCY_BACKOFF_H_
#define PULSELOG_CONCURRENCY_BACKOFF_H_

#include <cstdint>
#include <thread>

namespace pulselog {

// Emits the architecture's "this is a spin loop" hint. On x86 this is PAUSE
// (avoids a memory-order violation pipeline flush on loop exit); on AArch64 it
// is YIELD (a hint to SMT siblings, a no-op on Apple silicon but harmless).
inline void CpuRelax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
  __builtin_ia32_pause();
#elif defined(__aarch64__) || defined(__arm__)
  __asm__ __volatile__("yield" ::: "memory");
#else
  // Portable fallback: a compiler barrier so the loop is not hoisted.
  __asm__ __volatile__("" ::: "memory");
#endif
}

// Escalating backoff: spin a few times, then yield the CPU, then sleep.
// Bounded by construction -- callers that need to wait longer than a few
// microseconds should be blocking instead.
class Backoff {
 public:
  void Pause() noexcept {
    if (step_ < kSpinLimit) {
      for (std::uint32_t i = 0; i < (1U << step_); ++i) CpuRelax();
      ++step_;
      return;
    }
    if (step_ < kYieldLimit) {
      std::this_thread::yield();
      ++step_;
      return;
    }
    std::this_thread::sleep_for(std::chrono::microseconds(50));
  }

  void Reset() noexcept { step_ = 0; }

  [[nodiscard]] std::uint32_t steps() const noexcept { return step_; }

 private:
  static constexpr std::uint32_t kSpinLimit = 6;   // Up to 63 relax instructions.
  static constexpr std::uint32_t kYieldLimit = 12;

  std::uint32_t step_ = 0;
};

}  // namespace pulselog

#endif  // PULSELOG_CONCURRENCY_BACKOFF_H_
