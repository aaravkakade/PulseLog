// Readiness notification, abstracted over the platform mechanism.
//
// Implementations:
//   * `epoll` on Linux -- the deployment target.
//   * `kqueue` on macOS/BSD -- the development target.
//   * io_uring is a planned third backend. The interface below is level-
//     triggered readiness, which io_uring's completion model does not fit
//     directly; see docs/CONCURRENCY_MODEL.md for what that migration
//     involves. It is NOT implemented, and nothing in this repository
//     pretends otherwise.
//
// Level-triggered is deliberate. Edge-triggered readiness requires every
// handler to drain to EAGAIN or risk a permanent stall, which interacts badly
// with the bounded per-connection work limits used for fairness here. The
// measured difference at this connection count did not justify the extra
// failure mode.
#ifndef PULSELOG_NET_POLLER_H_
#define PULSELOG_NET_POLLER_H_

#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

#include "pulselog/base/status.h"

namespace pulselog::net {

enum class EventMask : std::uint8_t {
  kNone = 0,
  kRead = 1U << 0U,
  kWrite = 1U << 1U,
};

[[nodiscard]] inline EventMask operator|(EventMask a, EventMask b) noexcept {
  return static_cast<EventMask>(static_cast<std::uint8_t>(a) | static_cast<std::uint8_t>(b));
}

[[nodiscard]] inline bool HasEvent(EventMask mask, EventMask flag) noexcept {
  return (static_cast<std::uint8_t>(mask) & static_cast<std::uint8_t>(flag)) != 0;
}

struct PollEvent {
  int fd = -1;
  bool readable = false;
  bool writable = false;
  // Set for hangup or error conditions. The handler should attempt a read to
  // collect the error/EOF rather than assuming which one it is.
  bool error = false;
};

class Poller {
 public:
  Poller() = default;

  Poller(const Poller&) = delete;
  Poller& operator=(const Poller&) = delete;

  virtual ~Poller() = default;

  [[nodiscard]] virtual Status Add(int fd, EventMask events) = 0;

  [[nodiscard]] virtual Status Modify(int fd, EventMask events) = 0;

  [[nodiscard]] virtual Status Remove(int fd) = 0;

  // Waits up to `timeout_ms` (-1 blocks indefinitely) and appends ready
  // descriptors to `out`. `out` is cleared first. Returns the number ready.
  [[nodiscard]] virtual Result<std::size_t> Wait(std::vector<PollEvent>& out, int timeout_ms) = 0;

  [[nodiscard]] virtual std::string_view Name() const = 0;
};

// Creates the best poller for this platform.
[[nodiscard]] Result<std::unique_ptr<Poller>> CreatePoller(std::size_t max_events = 1024);

// Name of the backend this build will use, for the start-up banner and for
// benchmark result metadata.
[[nodiscard]] std::string_view PollerBackendName() noexcept;

}  // namespace pulselog::net

#endif  // PULSELOG_NET_POLLER_H_
