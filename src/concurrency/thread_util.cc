#include "pulselog/concurrency/thread_util.h"

#include <pthread.h>

#if defined(__linux__)
#include <sched.h>
#endif

namespace pulselog {

void SetCurrentThreadName(const std::string& name) {
#if defined(__APPLE__)
  // macOS names the calling thread only, and truncates at 63 characters.
  ::pthread_setname_np(name.substr(0, 63).c_str());
#elif defined(__linux__)
  // Linux limits thread names to 15 characters plus NUL.
  ::pthread_setname_np(::pthread_self(), name.substr(0, 15).c_str());
#else
  (void)name;
#endif
}

bool TryPinCurrentThread([[maybe_unused]] int cpu) {
#if defined(__linux__)
  if (cpu < 0) return false;
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(static_cast<unsigned>(cpu), &set);
  return ::pthread_setaffinity_np(::pthread_self(), sizeof(set), &set) == 0;
#else
  // macOS exposes only affinity *hints* (thread_policy_set with
  // THREAD_AFFINITY_POLICY) and ignores them on Apple silicon, so pinning is
  // reported as unsupported rather than silently doing nothing.
  return false;
#endif
}

unsigned HardwareConcurrency() noexcept {
  const unsigned n = std::thread::hardware_concurrency();
  return n == 0 ? 1U : n;
}

}  // namespace pulselog
