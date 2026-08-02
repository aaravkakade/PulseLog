// Thread naming, pinning and a small RAII thread wrapper.
//
// PulseLog never detaches a thread. Every thread is owned by a component that
// joins it in its destructor, so shutdown is deterministic and a crash during
// teardown cannot leave a thread touching freed memory.
#ifndef PULSELOG_CONCURRENCY_THREAD_UTIL_H_
#define PULSELOG_CONCURRENCY_THREAD_UTIL_H_

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>
#include <thread>
#include <utility>

namespace pulselog {

// Sets the current thread's name (visible in `top -H`, perf, and lldb).
// Truncated to the platform limit; failures are ignored -- naming is
// diagnostics only.
void SetCurrentThreadName(const std::string& name);

// Pins the current thread to `cpu`. Returns false on platforms without CPU
// affinity control (macOS) or when the call fails. NUMA/affinity tuning is
// opt-in via the `broker.pin_workers` config key.
bool TryPinCurrentThread(int cpu);

[[nodiscard]] unsigned HardwareConcurrency() noexcept;

// A joined-on-destruction thread with a name. Copy is disabled, move is
// allowed so components can store these in vectors.
class NamedThread {
 public:
  NamedThread() = default;

  template<typename Fn>
  NamedThread(std::string name, Fn&& fn) : name_(std::move(name)) {
    thread_ = std::thread([n = name_, f = std::forward<Fn>(fn)]() mutable {
      SetCurrentThreadName(n);
      f();
    });
  }

  NamedThread(const NamedThread&) = delete;
  NamedThread& operator=(const NamedThread&) = delete;

  NamedThread(NamedThread&&) noexcept = default;
  NamedThread& operator=(NamedThread&&) noexcept = default;

  ~NamedThread() { Join(); }

  void Join() {
    if (thread_.joinable()) thread_.join();
  }

  [[nodiscard]] bool joinable() const noexcept { return thread_.joinable(); }

  [[nodiscard]] const std::string& name() const noexcept { return name_; }

 private:
  std::string name_;
  std::thread thread_;
};

}  // namespace pulselog

#endif  // PULSELOG_CONCURRENCY_THREAD_UTIL_H_
