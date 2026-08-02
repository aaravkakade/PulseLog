// Poller factory plus the epoll and kqueue backends.
//
// Both backends live in one file because they are small, mutually exclusive,
// and share the interface contract documented in poller.h. Splitting them
// would mean two files of which one is always empty.
#include "pulselog/net/poller.h"

#include <array>
#include <cerrno>
#include <cstring>

#include <unistd.h>

#if defined(__linux__)
#include <sys/epoll.h>
#define PULSELOG_POLLER_EPOLL 1
#elif defined(__APPLE__) || defined(__FreeBSD__)
#include <sys/event.h>
#include <sys/time.h>
#include <sys/types.h>
#define PULSELOG_POLLER_KQUEUE 1
#else
#error "PulseLog requires epoll (Linux) or kqueue (macOS/BSD)"
#endif

namespace pulselog::net {
namespace {

#if PULSELOG_POLLER_EPOLL

class EpollPoller final : public Poller {
 public:
  explicit EpollPoller(std::size_t max_events) : events_(max_events) {}

  ~EpollPoller() override {
    if (epoll_fd_ >= 0) ::close(epoll_fd_);
  }

  [[nodiscard]] Status Init() {
    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) return ErrnoToStatus("epoll_create1", errno);
    return OkStatus();
  }

  Status Add(int fd, EventMask events) override { return Control(EPOLL_CTL_ADD, fd, events); }

  Status Modify(int fd, EventMask events) override { return Control(EPOLL_CTL_MOD, fd, events); }

  Status Remove(int fd) override {
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr) != 0) {
      // ENOENT means it was already gone, which happens on a racing close.
      if (errno == ENOENT || errno == EBADF) return OkStatus();
      return ErrnoToStatus("epoll_ctl(DEL)", errno);
    }
    return OkStatus();
  }

  Result<std::size_t> Wait(std::vector<PollEvent>& out, int timeout_ms) override {
    out.clear();
    int ready = 0;
    do {
      ready = ::epoll_wait(epoll_fd_, events_.data(), static_cast<int>(events_.size()), timeout_ms);
    } while (ready < 0 && errno == EINTR);

    if (ready < 0) return ErrnoToStatus("epoll_wait", errno);

    out.reserve(static_cast<std::size_t>(ready));
    for (int i = 0; i < ready; ++i) {
      PollEvent event;
      event.fd = events_[static_cast<std::size_t>(i)].data.fd;
      const std::uint32_t flags = events_[static_cast<std::size_t>(i)].events;
      event.readable = (flags & (EPOLLIN | EPOLLRDHUP)) != 0;
      event.writable = (flags & EPOLLOUT) != 0;
      event.error = (flags & (EPOLLERR | EPOLLHUP)) != 0;
      out.push_back(event);
    }
    return static_cast<std::size_t>(ready);
  }

  [[nodiscard]] std::string_view Name() const override { return "epoll"; }

 private:
  [[nodiscard]] Status Control(int op, int fd, EventMask events) {
    ::epoll_event event{};
    event.data.fd = fd;
    event.events = 0;
    if (HasEvent(events, EventMask::kRead)) event.events |= EPOLLIN;
    if (HasEvent(events, EventMask::kWrite)) event.events |= EPOLLOUT;
    // RDHUP tells us the peer half-closed without waiting for a read to
    // return 0, which lets a connection be reaped one iteration earlier.
    event.events |= EPOLLRDHUP;

    if (::epoll_ctl(epoll_fd_, op, fd, &event) != 0) {
      return ErrnoToStatus("epoll_ctl", errno);
    }
    return OkStatus();
  }

  int epoll_fd_ = -1;
  std::vector<::epoll_event> events_;
};

#endif  // PULSELOG_POLLER_EPOLL

#if PULSELOG_POLLER_KQUEUE

class KqueuePoller final : public Poller {
 public:
  explicit KqueuePoller(std::size_t max_events) : events_(max_events) {}

  ~KqueuePoller() override {
    if (kqueue_fd_ >= 0) ::close(kqueue_fd_);
  }

  [[nodiscard]] Status Init() {
    kqueue_fd_ = ::kqueue();
    if (kqueue_fd_ < 0) return ErrnoToStatus("kqueue", errno);
    return OkStatus();
  }

  Status Add(int fd, EventMask events) override { return Apply(fd, EventMask::kNone, events); }

  Status Modify(int fd, EventMask events) override {
    const EventMask previous = Registered(fd);
    return Apply(fd, previous, events);
  }

  Status Remove(int fd) override {
    const EventMask previous = Registered(fd);
    Status status = Apply(fd, previous, EventMask::kNone);
    if (static_cast<std::size_t>(fd) < registered_.size()) {
      registered_[static_cast<std::size_t>(fd)] = EventMask::kNone;
    }
    return status;
  }

  Result<std::size_t> Wait(std::vector<PollEvent>& out, int timeout_ms) override {
    out.clear();
    ::timespec timeout{};
    ::timespec* timeout_ptr = nullptr;
    if (timeout_ms >= 0) {
      timeout.tv_sec = timeout_ms / 1000;
      timeout.tv_nsec = static_cast<long>(timeout_ms % 1000) * 1'000'000L;
      timeout_ptr = &timeout;
    }

    int ready = 0;
    do {
      ready = ::kevent(
          kqueue_fd_, nullptr, 0, events_.data(), static_cast<int>(events_.size()), timeout_ptr);
    } while (ready < 0 && errno == EINTR);

    if (ready < 0) return ErrnoToStatus("kevent(wait)", errno);

    // kqueue reports read and write readiness as separate events for the same
    // descriptor; coalesce them so the loop dispatches each fd once.
    out.reserve(static_cast<std::size_t>(ready));
    for (int i = 0; i < ready; ++i) {
      const struct ::kevent& raw = events_[static_cast<std::size_t>(i)];
      const int fd = static_cast<int>(raw.ident);

      PollEvent* existing = nullptr;
      for (auto& candidate : out) {
        if (candidate.fd == fd) {
          existing = &candidate;
          break;
        }
      }
      if (existing == nullptr) {
        out.push_back(PollEvent{fd, false, false, false});
        existing = &out.back();
      }
      if (raw.filter == EVFILT_READ) existing->readable = true;
      if (raw.filter == EVFILT_WRITE) existing->writable = true;
      if ((raw.flags & EV_EOF) != 0) {
        // EV_EOF on a socket means the peer closed. Surface it as readable so
        // the handler collects EOF (or the pending error) through a normal
        // read, rather than guessing here.
        existing->readable = true;
        if (raw.fflags != 0) existing->error = true;
      }
      if ((raw.flags & EV_ERROR) != 0) existing->error = true;
    }
    return out.size();
  }

  [[nodiscard]] std::string_view Name() const override { return "kqueue"; }

 private:
  [[nodiscard]] EventMask Registered(int fd) const {
    if (static_cast<std::size_t>(fd) >= registered_.size()) return EventMask::kNone;
    return registered_[static_cast<std::size_t>(fd)];
  }

  // kqueue has no "modify" -- filters are added and deleted individually, so
  // the transition from the previous mask to the new one is computed here.
  [[nodiscard]] Status Apply(int fd, EventMask previous, EventMask desired) {
    std::array<struct ::kevent, 2> changes{};
    std::size_t count = 0;

    const bool want_read = HasEvent(desired, EventMask::kRead);
    const bool had_read = HasEvent(previous, EventMask::kRead);
    if (want_read != had_read) {
      EV_SET(&changes[count],
             static_cast<uintptr_t>(fd),
             EVFILT_READ,
             want_read ? EV_ADD : EV_DELETE,
             0,
             0,
             nullptr);
      ++count;
    }

    const bool want_write = HasEvent(desired, EventMask::kWrite);
    const bool had_write = HasEvent(previous, EventMask::kWrite);
    if (want_write != had_write) {
      EV_SET(&changes[count],
             static_cast<uintptr_t>(fd),
             EVFILT_WRITE,
             want_write ? EV_ADD : EV_DELETE,
             0,
             0,
             nullptr);
      ++count;
    }

    if (count > 0) {
      if (::kevent(kqueue_fd_, changes.data(), static_cast<int>(count), nullptr, 0, nullptr) != 0) {
        // ENOENT on delete means the filter was already gone (racing close).
        if (errno != ENOENT) return ErrnoToStatus("kevent(change)", errno);
      }
    }

    if (static_cast<std::size_t>(fd) >= registered_.size()) {
      registered_.resize(static_cast<std::size_t>(fd) + 1, EventMask::kNone);
    }
    registered_[static_cast<std::size_t>(fd)] = desired;
    return OkStatus();
  }

  int kqueue_fd_ = -1;
  std::vector<struct ::kevent> events_;
  // Indexed by fd. File descriptors are small dense integers, so a vector
  // beats a hash map both in memory and in lookup cost.
  std::vector<EventMask> registered_;
};

#endif  // PULSELOG_POLLER_KQUEUE

}  // namespace

Result<std::unique_ptr<Poller>> CreatePoller(std::size_t max_events) {
#if PULSELOG_POLLER_EPOLL
  auto poller = std::make_unique<EpollPoller>(max_events);
#else
  auto poller = std::make_unique<KqueuePoller>(max_events);
#endif
  PL_RETURN_IF_ERROR(poller->Init());
  return std::unique_ptr<Poller>(std::move(poller));
}

std::string_view PollerBackendName() noexcept {
#if PULSELOG_POLLER_EPOLL
  return "epoll";
#else
  return "kqueue";
#endif
}

}  // namespace pulselog::net
