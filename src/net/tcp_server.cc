#include "pulselog/net/tcp_server.h"

#include <algorithm>
#include <limits>

#include "pulselog/base/logging.h"

namespace pulselog::net {
namespace {

constexpr std::string_view kComponent = "net.server";

}  // namespace

// Owns the listening socket and lives on the accept loop.
class TcpServer::Acceptor final : public EventHandler {
 public:
  Acceptor(TcpSocket socket, TcpServer& server) : socket_(std::move(socket)), server_(server) {}

  void OnReadable() override {
    // Drain the backlog in one go, bounded so the accept loop still gets to
    // run its timers under a connection storm.
    constexpr int kMaxAcceptsPerEvent = 128;
    for (int i = 0; i < kMaxAcceptsPerEvent; ++i) {
      bool would_block = false;
      Endpoint peer;
      auto client = socket_.Accept(would_block, &peer);
      if (!client.ok()) {
        PL_WARN(kComponent) << "accept failed: " << client.status().ToString();
        return;
      }
      if (would_block) return;
      server_.OnAccepted(std::move(client).value());
    }
  }

  void OnWritable() override {}

  [[nodiscard]] int fd() const override { return socket_.fd(); }

 private:
  TcpSocket socket_;
  TcpServer& server_;
};

TcpServer::TcpServer(ServerOptions options, FrameCallback on_frame, CloseCallback on_close)
    : options_(std::move(options)),
      on_frame_(std::move(on_frame)),
      on_close_(std::move(on_close)) {}

TcpServer::~TcpServer() { Stop(); }

Status TcpServer::Start() {
  if (running_.load(std::memory_order_acquire)) return OkStatus();
  if (options_.io_threads == 0) options_.io_threads = 1;

  PL_ASSIGN_OR_RETURN(TcpSocket listener,
                      TcpSocket::Listen(options_.bind, options_.backlog, /*reuse_port=*/false));
  PL_ASSIGN_OR_RETURN(bound_endpoint_, listener.LocalEndpoint());

  // io loops first, so a connection accepted immediately after the acceptor
  // starts always has somewhere to go.
  loops_.reserve(options_.io_threads);
  pools_.reserve(options_.io_threads);
  loop_connections_.reserve(options_.io_threads);
  for (std::size_t i = 0; i < options_.io_threads; ++i) {
    auto loop = std::make_unique<EventLoop>(static_cast<int>(i));
    PL_RETURN_IF_ERROR(loop->Init());
    loops_.push_back(std::move(loop));
    pools_.push_back(std::make_unique<BufferPool>(options_.buffer_pool));
    loop_connections_.push_back(std::make_unique<std::atomic<std::size_t>>(0));
  }

  accept_loop_ = std::make_unique<EventLoop>(-1);
  PL_RETURN_IF_ERROR(accept_loop_->Init());
  PL_RETURN_IF_ERROR(
      accept_loop_->AddHandler(std::make_unique<Acceptor>(std::move(listener), *this),
                               EventMask::kRead));

  running_.store(true, std::memory_order_release);

  threads_.reserve(loops_.size());
  for (std::size_t i = 0; i < loops_.size(); ++i) {
    threads_.emplace_back("pl-io-" + std::to_string(i), [this, i] { loops_[i]->Run(); });
  }
  accept_thread_ = NamedThread("pl-accept", [this] { accept_loop_->Run(); });

  PL_INFO(kComponent) << "listening"
                      << " endpoint=" << bound_endpoint_.ToString()
                      << " io_threads=" << loops_.size() << " backend=" << PollerBackendName()
                      << " max_connections=" << options_.max_connections;
  return OkStatus();
}

void TcpServer::Stop() {
  if (!running_.exchange(false, std::memory_order_acq_rel)) return;

  // Stop accepting before tearing down the io loops, so no connection is
  // handed to a loop that is already shutting down.
  if (accept_loop_) accept_loop_->Stop();
  accept_thread_.Join();

  for (auto& loop : loops_) loop->Stop();
  for (auto& thread : threads_) thread.Join();
  threads_.clear();

  PL_INFO(kComponent) << "stopped"
                      << " accepted_total=" << accepted_total_.load(std::memory_order_relaxed)
                      << " rejected_total=" << rejected_total_.load(std::memory_order_relaxed);

  loops_.clear();
  pools_.clear();
  loop_connections_.clear();
  accept_loop_.reset();
}

std::size_t TcpServer::PickLoop() const {
  // Least-loaded placement. Round-robin is simpler but degrades badly when
  // connection lifetimes differ by orders of magnitude -- a long-lived
  // replication link and a one-shot metadata query are both "one connection"
  // to round-robin, and the imbalance persists for as long as the link lives.
  std::size_t best = 0;
  std::size_t best_count = std::numeric_limits<std::size_t>::max();
  for (std::size_t i = 0; i < loop_connections_.size(); ++i) {
    const std::size_t count = loop_connections_[i]->load(std::memory_order_relaxed);
    if (count < best_count) {
      best_count = count;
      best = i;
    }
  }
  return best;
}

void TcpServer::OnAccepted(TcpSocket socket) {
  if (!running_.load(std::memory_order_acquire)) return;

  if (connection_count_.load(std::memory_order_relaxed) >= options_.max_connections) {
    rejected_total_.fetch_add(1, std::memory_order_relaxed);
    PL_WARN(kComponent) << "rejecting connection: at the configured limit"
                        << " limit=" << options_.max_connections;
    socket.Close();  // Explicit refusal beats running out of descriptors later.
    return;
  }

  const std::size_t loop_index = PickLoop();
  EventLoop& loop = *loops_[loop_index];
  BufferPool& pool = *pools_[loop_index];
  std::atomic<std::size_t>& loop_count = *loop_connections_[loop_index];

  const Connection::Id id = next_connection_id_.fetch_add(1, std::memory_order_relaxed);
  connection_count_.fetch_add(1, std::memory_order_relaxed);
  loop_count.fetch_add(1, std::memory_order_relaxed);
  accepted_total_.fetch_add(1, std::memory_order_relaxed);

  // The socket must be registered on the loop's own thread, so ownership moves
  // across via a posted task. Everything after this point runs on that thread.
  auto* raw_socket = new TcpSocket(std::move(socket));
  const bool posted = loop.PostTask([this, raw_socket, id, &loop, &pool, &loop_count] {
    std::unique_ptr<TcpSocket> owned(raw_socket);
    auto connection = std::make_unique<Connection>(
        id, std::move(*owned), loop, pool, options_.connection, on_frame_,
        [this, &loop_count](Connection& conn, const Status& reason) {
          connection_count_.fetch_sub(1, std::memory_order_relaxed);
          loop_count.fetch_sub(1, std::memory_order_relaxed);
          if (on_close_) on_close_(conn, reason);
        });

    const Status status = loop.AddHandler(std::move(connection), EventMask::kRead);
    if (!status.ok()) {
      PL_ERROR(kComponent) << "failed to register connection: " << status.ToString();
      connection_count_.fetch_sub(1, std::memory_order_relaxed);
      loop_count.fetch_sub(1, std::memory_order_relaxed);
    }
  });

  if (!posted) {
    // The loop's task queue is full: the loop is saturated. Refusing the
    // connection is the honest response -- queueing it would be unbounded.
    delete raw_socket;
    connection_count_.fetch_sub(1, std::memory_order_relaxed);
    loop_count.fetch_sub(1, std::memory_order_relaxed);
    rejected_total_.fetch_add(1, std::memory_order_relaxed);
    PL_WARN(kComponent) << "rejecting connection: io loop task queue is full"
                        << " loop=" << loop_index;
  }
}

TcpServer::AggregateStats TcpServer::GetLoopStats() const {
  AggregateStats aggregate;
  for (const auto& loop : loops_) {
    const auto stats = loop->GetStats();
    aggregate.iterations += stats.iterations;
    aggregate.events_processed += stats.events_processed;
    aggregate.tasks_executed += stats.tasks_executed;
    aggregate.tasks_rejected += stats.tasks_rejected;
    aggregate.handlers += stats.handlers;
  }
  return aggregate;
}

}  // namespace pulselog::net
