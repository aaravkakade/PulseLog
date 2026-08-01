// A multi-loop TCP server.
//
// One acceptor loop owns the listening socket; N io loops own connections.
// A new connection is handed to the loop with the fewest connections, and it
// stays there for its whole life -- so connection state needs no locking, and
// a connection's frames are always decoded on the same thread.
//
// This is deliberately not thread-per-connection: 10k connections would mean
// 10k stacks and a scheduler doing nothing but context switching. It is also
// not a single loop: one thread cannot saturate several NIC queues or several
// cores' worth of frame parsing and checksumming.
#ifndef PULSELOG_NET_TCP_SERVER_H_
#define PULSELOG_NET_TCP_SERVER_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "pulselog/base/buffer.h"
#include "pulselog/base/status.h"
#include "pulselog/concurrency/thread_util.h"
#include "pulselog/net/connection.h"
#include "pulselog/net/event_loop.h"
#include "pulselog/net/socket.h"

namespace pulselog::net {

struct ServerOptions {
  Endpoint bind;
  std::size_t io_threads = 2;
  int backlog = 512;
  // Refuses new connections beyond this count, so descriptor exhaustion is a
  // reported condition rather than a cascade of accept failures.
  std::size_t max_connections = 4096;
  ConnectionOptions connection;
  BufferPoolOptions buffer_pool;
  // How often idle connections are swept. 0 disables the sweep entirely.
  std::int64_t idle_sweep_interval_ms = 1000;
};

class TcpServer {
 public:
  TcpServer(ServerOptions options, FrameCallback on_frame, CloseCallback on_close);

  TcpServer(const TcpServer&) = delete;
  TcpServer& operator=(const TcpServer&) = delete;

  ~TcpServer();

  // Binds, creates the loops, and starts the threads.
  [[nodiscard]] Status Start();

  // Stops accepting, closes every connection, and joins every thread.
  void Stop();

  [[nodiscard]] bool running() const noexcept { return running_.load(std::memory_order_acquire); }

  // Actual bound endpoint; resolves an ephemeral port when 0 was requested.
  [[nodiscard]] const Endpoint& bound_endpoint() const noexcept { return bound_endpoint_; }

  [[nodiscard]] std::size_t ConnectionCount() const noexcept {
    return connection_count_.load(std::memory_order_relaxed);
  }

  [[nodiscard]] std::uint64_t AcceptedTotal() const noexcept {
    return accepted_total_.load(std::memory_order_relaxed);
  }

  [[nodiscard]] std::uint64_t RejectedTotal() const noexcept {
    return rejected_total_.load(std::memory_order_relaxed);
  }

  // Loops are exposed so the broker can post completions back to the loop that
  // owns a given connection.
  [[nodiscard]] std::size_t LoopCount() const noexcept { return loops_.size(); }

  [[nodiscard]] EventLoop& loop(std::size_t index) { return *loops_[index]; }

  struct AggregateStats {
    std::uint64_t iterations = 0;
    std::uint64_t events_processed = 0;
    std::uint64_t tasks_executed = 0;
    std::uint64_t tasks_rejected = 0;
    std::size_t handlers = 0;
  };

  [[nodiscard]] AggregateStats GetLoopStats() const;

 private:
  class Acceptor;

  void OnAccepted(TcpSocket socket);

  [[nodiscard]] std::size_t PickLoop() const;

  ServerOptions options_;
  FrameCallback on_frame_;
  CloseCallback on_close_;

  Endpoint bound_endpoint_;
  std::vector<std::unique_ptr<EventLoop>> loops_;
  std::vector<std::unique_ptr<BufferPool>> pools_;
  std::vector<NamedThread> threads_;

  // Per-loop connection counts, used to place new connections. Kept as
  // separate atomics rather than a shared counter because the acceptor reads
  // all of them and each loop updates only its own.
  std::vector<std::unique_ptr<std::atomic<std::size_t>>> loop_connections_;

  std::unique_ptr<EventLoop> accept_loop_;
  NamedThread accept_thread_;

  std::atomic<std::size_t> connection_count_{0};
  std::atomic<std::uint64_t> accepted_total_{0};
  std::atomic<std::uint64_t> rejected_total_{0};
  std::atomic<Connection::Id> next_connection_id_{1};
  std::atomic<bool> running_{false};
};

}  // namespace pulselog::net

#endif  // PULSELOG_NET_TCP_SERVER_H_
