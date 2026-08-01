// A single-threaded event loop: one poller, one set of descriptors, one
// thread.
//
// Ownership model
// ---------------
// Every descriptor registered with a loop is owned by that loop and touched
// only from the loop's thread. Other threads interact with a loop exclusively
// through `PostTask`, which enqueues onto a bounded MPSC queue and writes one
// byte to a wakeup descriptor. There is no lock around handler state because
// no other thread can reach it.
//
// Shutdown
// --------
// `Stop()` is safe from any thread. It sets a flag and wakes the loop; the
// loop finishes the current iteration, runs any pending close callbacks, and
// returns from `Run()`. Handlers are destroyed on the loop thread, never on
// the caller's.
#ifndef PULSELOG_NET_EVENT_LOOP_H_
#define PULSELOG_NET_EVENT_LOOP_H_

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

#include "pulselog/base/clock.h"
#include "pulselog/base/status.h"
#include "pulselog/concurrency/mpmc_queue.h"
#include "pulselog/net/poller.h"

namespace pulselog::net {

// Callbacks for one registered descriptor. Implementations run on the loop
// thread and must not block.
class EventHandler {
 public:
  EventHandler() = default;

  EventHandler(const EventHandler&) = delete;
  EventHandler& operator=(const EventHandler&) = delete;

  virtual ~EventHandler() = default;

  virtual void OnReadable() = 0;

  virtual void OnWritable() = 0;

  // Called once, on the loop thread, before the handler is destroyed.
  virtual void OnClosed() {}

  [[nodiscard]] virtual int fd() const = 0;
};

using TimerId = std::uint64_t;

struct EventLoopOptions {
  std::size_t max_events_per_wait = 1024;
  std::size_t task_queue_capacity = 8192;
  // Upper bound on how long the loop can sleep. Also the timer resolution.
  int max_poll_timeout_ms = 50;
};

class EventLoop {
 public:
  using Task = std::function<void()>;
  using Options = EventLoopOptions;

  explicit EventLoop(int index) : EventLoop(index, Options{}) {}

  EventLoop(int index, const Options& options);

  EventLoop(const EventLoop&) = delete;
  EventLoop& operator=(const EventLoop&) = delete;

  ~EventLoop();

  [[nodiscard]] Status Init();

  // Runs until Stop(). Must be called from the thread that owns this loop.
  void Run();

  // Safe from any thread and idempotent.
  void Stop();

  [[nodiscard]] bool running() const noexcept { return running_.load(std::memory_order_acquire); }

  // --- loop thread only -----------------------------------------------------

  // Takes ownership of `handler` and registers its descriptor.
  [[nodiscard]] Status AddHandler(std::unique_ptr<EventHandler> handler, EventMask events);

  [[nodiscard]] Status UpdateEvents(int fd, EventMask events);

  // Schedules removal and destruction of the handler for `fd`. Safe to call
  // from inside that handler's own callback: the destruction happens after the
  // current iteration, so `this` stays alive for the rest of the callback.
  void CloseHandler(int fd);

  [[nodiscard]] std::size_t HandlerCount() const noexcept { return handler_count_; }

  // --- any thread -----------------------------------------------------------

  // Enqueues `task` to run on the loop thread. Returns false when the bounded
  // queue is full -- the caller must apply backpressure rather than retry in a
  // spin loop.
  [[nodiscard]] bool PostTask(Task task);

  // Runs `task` every `interval_ms` on the loop thread. Timers are coarse:
  // resolution is bounded by `max_poll_timeout_ms`.
  TimerId ScheduleRepeating(std::int64_t interval_ms, Task task);

  void CancelTimer(TimerId id);

  [[nodiscard]] int index() const noexcept { return index_; }

  // Cached "now", refreshed once per iteration. Handlers use this instead of
  // calling the clock per connection.
  [[nodiscard]] std::int64_t NowMillis() const noexcept { return clock_.Millis(); }

  [[nodiscard]] std::int64_t NowNanos() const noexcept { return clock_.Nanos(); }

  struct Stats {
    std::uint64_t iterations = 0;
    std::uint64_t events_processed = 0;
    std::uint64_t tasks_executed = 0;
    std::uint64_t tasks_rejected = 0;
    std::uint64_t wakeups = 0;
    std::size_t handlers = 0;
  };

  [[nodiscard]] Stats GetStats() const;

 private:
  struct Timer {
    TimerId id = 0;
    std::int64_t interval_ms = 0;
    std::int64_t next_fire_ms = 0;
    Task task;
    bool cancelled = false;
  };

  void DrainTasks();

  void RunTimers();

  void ProcessPendingCloses();

  void Wake();

  void DrainWakeup();

  int index_;
  Options options_;

  std::unique_ptr<Poller> poller_;
  std::vector<PollEvent> ready_;

  // Indexed by fd; file descriptors are small dense integers.
  std::vector<std::unique_ptr<EventHandler>> handlers_;
  std::size_t handler_count_ = 0;
  std::vector<int> pending_closes_;

  // Cross-thread wakeup: eventfd on Linux, a self-pipe elsewhere.
  int wake_read_fd_ = -1;
  int wake_write_fd_ = -1;
  // Avoids a write(2) per posted task when one is already pending.
  std::atomic<bool> wake_pending_{false};

  BoundedMpmcQueue<Task> tasks_;
  std::vector<Timer> timers_;
  TimerId next_timer_id_ = 1;

  CoarseClock clock_;
  std::atomic<bool> running_{false};
  std::atomic<bool> stopping_{false};

  std::atomic<std::uint64_t> iterations_{0};
  std::atomic<std::uint64_t> events_processed_{0};
  std::atomic<std::uint64_t> tasks_executed_{0};
  std::atomic<std::uint64_t> tasks_rejected_{0};
  std::atomic<std::uint64_t> wakeups_{0};
};

}  // namespace pulselog::net

#endif  // PULSELOG_NET_EVENT_LOOP_H_
