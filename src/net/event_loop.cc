#include "pulselog/net/event_loop.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>

#if defined(__linux__)
#include <sys/eventfd.h>
#endif

#include "pulselog/base/logging.h"

namespace pulselog::net {
namespace {

constexpr std::string_view kComponent = "net.loop";

// A wakeup handler that exists only so the wakeup descriptor participates in
// the same dispatch path as everything else.
class WakeupHandler final : public EventHandler {
 public:
  WakeupHandler(int fd, std::function<void()> on_wake)
      : fd_(fd), on_wake_(std::move(on_wake)) {}

  void OnReadable() override { on_wake_(); }

  void OnWritable() override {}

  [[nodiscard]] int fd() const override { return fd_; }

 private:
  int fd_;
  std::function<void()> on_wake_;
};

}  // namespace

EventLoop::EventLoop(int index, const Options& options)
    : index_(index), options_(options), tasks_(options.task_queue_capacity) {
  handlers_.resize(64);
}

EventLoop::~EventLoop() {
  Stop();
  handlers_.clear();
  if (wake_read_fd_ >= 0) ::close(wake_read_fd_);
  if (wake_write_fd_ >= 0 && wake_write_fd_ != wake_read_fd_) ::close(wake_write_fd_);
}

Status EventLoop::Init() {
  PL_ASSIGN_OR_RETURN(poller_, CreatePoller(options_.max_events_per_wait));

#if defined(__linux__)
  // eventfd is one descriptor and one 8-byte counter, versus a pipe's two
  // descriptors and per-byte bookkeeping.
  const int fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (fd < 0) return ErrnoToStatus("eventfd", errno);
  wake_read_fd_ = fd;
  wake_write_fd_ = fd;
#else
  int pipe_fds[2] = {-1, -1};
  if (::pipe(pipe_fds) != 0) return ErrnoToStatus("pipe", errno);
  for (const int pipe_fd : pipe_fds) {
    const int flags = ::fcntl(pipe_fd, F_GETFL, 0);
    if (flags < 0 || ::fcntl(pipe_fd, F_SETFL, flags | O_NONBLOCK) < 0) {
      ::close(pipe_fds[0]);
      ::close(pipe_fds[1]);
      return ErrnoToStatus("fcntl on wakeup pipe", errno);
    }
    ::fcntl(pipe_fd, F_SETFD, FD_CLOEXEC);
  }
  wake_read_fd_ = pipe_fds[0];
  wake_write_fd_ = pipe_fds[1];
#endif

  auto handler = std::make_unique<WakeupHandler>(wake_read_fd_, [this] { DrainWakeup(); });
  PL_RETURN_IF_ERROR(AddHandler(std::move(handler), EventMask::kRead));
  return OkStatus();
}

Status EventLoop::AddHandler(std::unique_ptr<EventHandler> handler, EventMask events) {
  const int fd = handler->fd();
  if (fd < 0) return InvalidArgument("handler has an invalid descriptor");

  if (static_cast<std::size_t>(fd) >= handlers_.size()) {
    handlers_.resize(static_cast<std::size_t>(fd) * 2 + 1);
  }
  if (handlers_[static_cast<std::size_t>(fd)] != nullptr) {
    return AlreadyExists("descriptor " + std::to_string(fd) + " is already registered");
  }

  PL_RETURN_IF_ERROR(poller_->Add(fd, events));
  handlers_[static_cast<std::size_t>(fd)] = std::move(handler);
  ++handler_count_;
  return OkStatus();
}

Status EventLoop::UpdateEvents(int fd, EventMask events) { return poller_->Modify(fd, events); }

void EventLoop::CloseHandler(int fd) {
  if (fd < 0 || static_cast<std::size_t>(fd) >= handlers_.size()) return;
  if (handlers_[static_cast<std::size_t>(fd)] == nullptr) return;
  // Deferred: a handler routinely calls this from inside its own callback, and
  // destroying it there would pull the ground out from under the caller.
  if (std::find(pending_closes_.begin(), pending_closes_.end(), fd) == pending_closes_.end()) {
    pending_closes_.push_back(fd);
  }
}

void EventLoop::ProcessPendingCloses() {
  while (!pending_closes_.empty()) {
    // Swap out the list: a close callback may itself close other handlers.
    std::vector<int> batch;
    batch.swap(pending_closes_);
    for (const int fd : batch) {
      if (static_cast<std::size_t>(fd) >= handlers_.size()) continue;
      auto& slot = handlers_[static_cast<std::size_t>(fd)];
      if (slot == nullptr) continue;

      const Status status = poller_->Remove(fd);
      if (!status.ok()) {
        PL_DEBUG(kComponent) << "poller remove failed fd=" << fd << ": " << status.ToString();
      }
      slot->OnClosed();
      slot.reset();
      --handler_count_;
    }
  }
}

bool EventLoop::PostTask(Task task) {
  if (stopping_.load(std::memory_order_acquire)) return false;
  if (!tasks_.TryPush(std::move(task))) {
    tasks_rejected_.fetch_add(1, std::memory_order_relaxed);
    return false;
  }
  Wake();
  return true;
}

void EventLoop::Wake() {
  // Coalesce wakeups: if one write is already outstanding, the loop is about
  // to drain the queue anyway. This turns a burst of N posted tasks into one
  // syscall instead of N.
  if (wake_pending_.exchange(true, std::memory_order_acq_rel)) return;

  wakeups_.fetch_add(1, std::memory_order_relaxed);
#if defined(__linux__)
  const std::uint64_t one = 1;
  ssize_t wrote = 0;
  do {
    wrote = ::write(wake_write_fd_, &one, sizeof(one));
  } while (wrote < 0 && errno == EINTR);
#else
  const char byte = 1;
  ssize_t wrote = 0;
  do {
    wrote = ::write(wake_write_fd_, &byte, 1);
  } while (wrote < 0 && errno == EINTR);
#endif
  // A full pipe means a wakeup is already pending in the kernel, which is
  // exactly the condition the write was meant to create.
  (void)wrote;
}

void EventLoop::DrainWakeup() {
  std::array<std::uint8_t, 256> scratch{};
  for (;;) {
    const ssize_t got = ::read(wake_read_fd_, scratch.data(), scratch.size());
    if (got <= 0) {
      if (got < 0 && errno == EINTR) continue;
      break;
    }
    if (static_cast<std::size_t>(got) < scratch.size()) break;
  }
  wake_pending_.store(false, std::memory_order_release);
}

void EventLoop::DrainTasks() {
  // Bounded per iteration so a task flood cannot starve socket readiness.
  const std::size_t limit = tasks_.Capacity();
  std::size_t executed = 0;
  Task task;
  while (executed < limit && tasks_.TryPop(task)) {
    task();
    task = nullptr;
    ++executed;
  }
  if (executed > 0) tasks_executed_.fetch_add(executed, std::memory_order_relaxed);
}

TimerId EventLoop::ScheduleRepeating(std::int64_t interval_ms, Task task) {
  Timer timer;
  timer.id = next_timer_id_++;
  timer.interval_ms = std::max<std::int64_t>(1, interval_ms);
  timer.next_fire_ms = clock_.Millis() + timer.interval_ms;
  timer.task = std::move(task);
  timers_.push_back(std::move(timer));
  return timers_.back().id;
}

void EventLoop::CancelTimer(TimerId id) {
  for (auto& timer : timers_) {
    if (timer.id == id) timer.cancelled = true;
  }
}

void EventLoop::RunTimers() {
  const std::int64_t now = clock_.Millis();
  for (auto& timer : timers_) {
    if (timer.cancelled || now < timer.next_fire_ms) continue;
    timer.task();
    // Schedule from now rather than from the previous deadline: if a timer
    // over-ran, catching up by firing repeatedly would make things worse.
    timer.next_fire_ms = now + timer.interval_ms;
  }
  if (std::any_of(timers_.begin(), timers_.end(), [](const Timer& t) { return t.cancelled; })) {
    timers_.erase(std::remove_if(timers_.begin(), timers_.end(),
                                 [](const Timer& t) { return t.cancelled; }),
                  timers_.end());
  }
}

void EventLoop::Run() {
  running_.store(true, std::memory_order_release);
  PL_DEBUG(kComponent) << "loop started index=" << index_ << " backend=" << poller_->Name();

  while (!stopping_.load(std::memory_order_acquire)) {
    clock_.Tick();

    // Sleep only until the nearest timer needs attention.
    int timeout_ms = options_.max_poll_timeout_ms;
    const std::int64_t now = clock_.Millis();
    for (const auto& timer : timers_) {
      if (timer.cancelled) continue;
      const std::int64_t remaining = timer.next_fire_ms - now;
      timeout_ms = static_cast<int>(std::clamp<std::int64_t>(remaining, 0, timeout_ms));
    }

    auto ready = poller_->Wait(ready_, timeout_ms);
    if (!ready.ok()) {
      PL_ERROR(kComponent) << "poller wait failed: " << ready.status().ToString();
      break;
    }
    iterations_.fetch_add(1, std::memory_order_relaxed);
    if (ready.value() > 0) {
      events_processed_.fetch_add(ready.value(), std::memory_order_relaxed);
    }
    clock_.Tick();

    for (const PollEvent& event : ready_) {
      if (event.fd < 0 || static_cast<std::size_t>(event.fd) >= handlers_.size()) continue;
      EventHandler* handler = handlers_[static_cast<std::size_t>(event.fd)].get();
      if (handler == nullptr) continue;

      // Write first: draining the output queue may lift the read-side
      // backpressure that this same connection is waiting on.
      if (event.writable) handler->OnWritable();

      // The handler may have closed itself in OnWritable.
      if (handlers_[static_cast<std::size_t>(event.fd)] == nullptr) continue;
      if (std::find(pending_closes_.begin(), pending_closes_.end(), event.fd) !=
          pending_closes_.end()) {
        continue;
      }
      if (event.readable || event.error) handler->OnReadable();
    }

    DrainTasks();
    RunTimers();
    ProcessPendingCloses();
  }

  // Shutdown: run remaining tasks so queued responses get a chance to flush,
  // then tear every handler down on this thread.
  DrainTasks();
  for (std::size_t fd = 0; fd < handlers_.size(); ++fd) {
    if (handlers_[fd] != nullptr) pending_closes_.push_back(static_cast<int>(fd));
  }
  ProcessPendingCloses();

  running_.store(false, std::memory_order_release);
  PL_DEBUG(kComponent) << "loop stopped index=" << index_
                       << " iterations=" << iterations_.load(std::memory_order_relaxed);
}

void EventLoop::Stop() {
  if (stopping_.exchange(true, std::memory_order_acq_rel)) return;
  Wake();
}

EventLoop::Stats EventLoop::GetStats() const {
  Stats stats;
  stats.iterations = iterations_.load(std::memory_order_relaxed);
  stats.events_processed = events_processed_.load(std::memory_order_relaxed);
  stats.tasks_executed = tasks_executed_.load(std::memory_order_relaxed);
  stats.tasks_rejected = tasks_rejected_.load(std::memory_order_relaxed);
  stats.wakeups = wakeups_.load(std::memory_order_relaxed);
  stats.handlers = handler_count_;
  return stats;
}

}  // namespace pulselog::net
