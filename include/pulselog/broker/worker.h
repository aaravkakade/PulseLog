// Partition-affine worker threads.
//
// Each worker owns a disjoint set of partitions -- assigned by hashing the
// topic/partition pair -- and is the only thread that mutates their logs. That
// removes every lock from the append path; the ordering guarantee within a
// partition comes from the queue being FIFO and the worker being single.
//
// The queue is a bounded lock-free MPMC ring (many io loops push, one worker
// pops). When it is empty the worker spins briefly and then sleeps on a
// condition variable with a short timeout: spinning keeps latency low under
// load, and the timeout bounds the cost of a missed wakeup to that timeout
// rather than forever.
#ifndef PULSELOG_BROKER_WORKER_H_
#define PULSELOG_BROKER_WORKER_H_

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

#include "pulselog/base/buffer.h"
#include "pulselog/base/types.h"
#include "pulselog/concurrency/mpmc_queue.h"
#include "pulselog/concurrency/thread_util.h"
#include "pulselog/protocol/opcode.h"

namespace pulselog::broker {

// One unit of work handed from an io loop to a partition worker.
//
// `payload` is a pooled copy of the request frame's payload. The copy is made
// once, at the io-loop boundary, because the connection's read buffer is
// reused as soon as the frame callback returns. Everything downstream --
// decoding, offset assignment, the write to the segment -- operates on this
// buffer without copying again.
struct WorkerRequest {
  protocol::OpCode opcode = protocol::OpCode::kUnknown;
  RequestId request_id = 0;
  std::uint64_t connection_id = 0;
  std::size_t loop_index = 0;
  PooledBuffer payload;
  std::int64_t enqueued_nanos = 0;

  WorkerRequest() = default;

  WorkerRequest(const WorkerRequest&) = delete;
  WorkerRequest& operator=(const WorkerRequest&) = delete;
  WorkerRequest(WorkerRequest&&) noexcept = default;
  WorkerRequest& operator=(WorkerRequest&&) noexcept = default;
};

// Implemented by the broker; called on the worker thread.
class RequestExecutor {
 public:
  RequestExecutor() = default;

  RequestExecutor(const RequestExecutor&) = delete;
  RequestExecutor& operator=(const RequestExecutor&) = delete;

  virtual ~RequestExecutor() = default;

  virtual void Execute(WorkerRequest& request) = 0;
};

class PartitionWorker {
 public:
  PartitionWorker(std::size_t index,
                  std::size_t queue_capacity,
                  RequestExecutor& executor,
                  int pin_cpu = -1);

  PartitionWorker(const PartitionWorker&) = delete;
  PartitionWorker& operator=(const PartitionWorker&) = delete;

  ~PartitionWorker();

  void Start();

  // Signals the worker to finish its queue and exit, then joins it.
  void Stop();

  // Returns false when the queue is full. The caller answers the client with
  // BACKPRESSURE rather than blocking an io loop.
  [[nodiscard]] bool Submit(WorkerRequest&& request);

  [[nodiscard]] std::size_t QueueDepth() const noexcept { return queue_.SizeApprox(); }

  [[nodiscard]] std::size_t QueueCapacity() const noexcept { return queue_.Capacity(); }

  [[nodiscard]] std::uint64_t Processed() const noexcept {
    return processed_.load(std::memory_order_relaxed);
  }

  [[nodiscard]] std::uint64_t Rejected() const noexcept {
    return rejected_.load(std::memory_order_relaxed);
  }

  [[nodiscard]] std::size_t index() const noexcept { return index_; }

 private:
  void Run();

  std::size_t index_;
  RequestExecutor& executor_;
  int pin_cpu_;

  BoundedMpmcQueue<WorkerRequest> queue_;

  mutable std::mutex sleep_mutex_;
  std::condition_variable sleep_cv_;
  std::atomic<bool> sleeping_{false};
  std::atomic<bool> running_{false};
  std::atomic<bool> stopping_{false};

  std::atomic<std::uint64_t> processed_{0};
  std::atomic<std::uint64_t> rejected_{0};
  NamedThread thread_;
};

}  // namespace pulselog::broker

#endif  // PULSELOG_BROKER_WORKER_H_
