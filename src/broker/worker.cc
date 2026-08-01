#include "pulselog/broker/worker.h"

#include <chrono>

#include "pulselog/base/logging.h"
#include "pulselog/concurrency/backoff.h"

namespace pulselog::broker {
namespace {

constexpr std::string_view kComponent = "broker.worker";

// How long to spin before sleeping. Under load the queue is rarely empty, so
// this path is mostly untaken; when it is taken, spinning avoids a
// futex round trip on the very next request.
constexpr std::uint32_t kSpinStepsBeforeSleep = 8;

// A missed notification costs at most this long. The alternative -- holding
// the sleep mutex on the submit path to make the wakeup airtight -- would put
// a lock on every io loop's hot path to save an interval this short.
constexpr auto kSleepSlice = std::chrono::microseconds(200);

}  // namespace

PartitionWorker::PartitionWorker(std::size_t index, std::size_t queue_capacity,
                                 RequestExecutor& executor, int pin_cpu)
    : index_(index), executor_(executor), pin_cpu_(pin_cpu), queue_(queue_capacity) {}

PartitionWorker::~PartitionWorker() { Stop(); }

void PartitionWorker::Start() {
  if (running_.exchange(true, std::memory_order_acq_rel)) return;
  stopping_.store(false, std::memory_order_release);
  thread_ = NamedThread("pl-worker-" + std::to_string(index_), [this] { Run(); });
}

void PartitionWorker::Stop() {
  if (!running_.exchange(false, std::memory_order_acq_rel)) return;
  stopping_.store(true, std::memory_order_release);
  {
    std::lock_guard<std::mutex> lock(sleep_mutex_);
    sleep_cv_.notify_all();
  }
  thread_.Join();
}

bool PartitionWorker::Submit(WorkerRequest&& request) {
  if (stopping_.load(std::memory_order_acquire)) return false;
  if (!queue_.TryPush(std::move(request))) {
    rejected_.fetch_add(1, std::memory_order_relaxed);
    return false;
  }
  // Only take the sleep mutex when the worker is actually asleep. In steady
  // state this branch is not taken and submission stays lock-free.
  if (sleeping_.load(std::memory_order_acquire)) {
    std::lock_guard<std::mutex> lock(sleep_mutex_);
    sleep_cv_.notify_one();
  }
  return true;
}

void PartitionWorker::Run() {
  if (pin_cpu_ >= 0 && !TryPinCurrentThread(pin_cpu_)) {
    PL_DEBUG(kComponent) << "cpu pinning unsupported here worker=" << index_;
  }
  PL_DEBUG(kComponent) << "worker started index=" << index_
                       << " queue_capacity=" << queue_.Capacity();

  Backoff backoff;
  for (;;) {
    WorkerRequest request;
    if (queue_.TryPop(request)) {
      executor_.Execute(request);
      processed_.fetch_add(1, std::memory_order_relaxed);
      backoff.Reset();
      continue;
    }

    if (stopping_.load(std::memory_order_acquire)) {
      // Drain whatever is left so in-flight requests get a real answer rather
      // than being dropped on shutdown.
      while (queue_.TryPop(request)) {
        executor_.Execute(request);
        processed_.fetch_add(1, std::memory_order_relaxed);
      }
      break;
    }

    if (backoff.steps() < kSpinStepsBeforeSleep) {
      backoff.Pause();
      continue;
    }

    std::unique_lock<std::mutex> lock(sleep_mutex_);
    sleeping_.store(true, std::memory_order_release);
    sleep_cv_.wait_for(lock, kSleepSlice, [this] {
      return stopping_.load(std::memory_order_acquire) || queue_.SizeApprox() > 0;
    });
    sleeping_.store(false, std::memory_order_release);
    backoff.Reset();
  }

  PL_DEBUG(kComponent) << "worker stopped index=" << index_
                       << " processed=" << processed_.load(std::memory_order_relaxed);
}

}  // namespace pulselog::broker
