// Networking tests: sockets, poller, event loop, framed connections,
// backpressure, and shutdown.
//
// These use real sockets on loopback with ephemeral ports rather than mocks.
// The behaviour under test -- partial reads, partial writes, EOF timing,
// poller edge cases -- is exactly what a mock would paper over.
#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "pulselog/net/connection.h"
#include "pulselog/net/event_loop.h"
#include "pulselog/net/poller.h"
#include "pulselog/net/socket.h"
#include "pulselog/net/sync_client.h"
#include "pulselog/net/tcp_server.h"
#include "pulselog/protocol/messages.h"

namespace pulselog::net {
namespace {

using namespace std::chrono_literals;

// Spins until `predicate` holds or the deadline passes. Returns whether it
// became true -- lets tests assert on outcome rather than on sleep duration.
template<typename Predicate>
bool WaitFor(Predicate predicate, std::chrono::milliseconds timeout = 5s) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) return true;
    std::this_thread::sleep_for(1ms);
  }
  return predicate();
}

// --- Endpoint --------------------------------------------------------------

TEST(Endpoint, ParsesHostPort) {
  auto endpoint = Endpoint::Parse("127.0.0.1:9092");
  ASSERT_TRUE(endpoint.ok()) << endpoint.status().ToString();
  EXPECT_EQ(endpoint->host, "127.0.0.1");
  EXPECT_EQ(endpoint->port, 9092);
}

TEST(Endpoint, ParsesIpv6Literal) {
  auto endpoint = Endpoint::Parse("[::1]:9092");
  ASSERT_TRUE(endpoint.ok());
  EXPECT_EQ(endpoint->host, "::1");
  EXPECT_EQ(endpoint->port, 9092);
}

TEST(Endpoint, ParsesHostname) {
  auto endpoint = Endpoint::Parse("broker-2:9092");
  ASSERT_TRUE(endpoint.ok());
  EXPECT_EQ(endpoint->host, "broker-2");
}

TEST(Endpoint, RejectsMalformed) {
  EXPECT_FALSE(Endpoint::Parse("").ok());
  EXPECT_FALSE(Endpoint::Parse("no-port").ok());
  EXPECT_FALSE(Endpoint::Parse("host:0").ok());
  EXPECT_FALSE(Endpoint::Parse("host:99999").ok());
  EXPECT_FALSE(Endpoint::Parse("host:abc").ok());
  EXPECT_FALSE(Endpoint::Parse("[::1:9092").ok());
}

// --- Sockets ---------------------------------------------------------------

TEST(TcpSocket, ListenAndConnect) {
  auto listener = TcpSocket::Listen(Endpoint{"127.0.0.1", 0});
  ASSERT_TRUE(listener.ok()) << listener.status().ToString();
  auto bound = listener->LocalEndpoint();
  ASSERT_TRUE(bound.ok());
  EXPECT_GT(bound->port, 0);

  auto client = TcpSocket::ConnectWithTimeout(bound.value(), 2000);
  ASSERT_TRUE(client.ok()) << client.status().ToString();

  bool would_block = true;
  Endpoint peer;
  // The connection may not have landed in the backlog yet.
  Result<TcpSocket> accepted = Status{ErrorCode::kUnknown, "not yet"};
  ASSERT_TRUE(WaitFor([&] {
    accepted = listener->Accept(would_block, &peer);
    return accepted.ok() && !would_block;
  }));
  ASSERT_TRUE(accepted.ok());
  EXPECT_TRUE(accepted->valid());
  EXPECT_EQ(peer.host, "127.0.0.1");
}

TEST(TcpSocket, ConnectToClosedPortFails) {
  // Bind then close, so the port is almost certainly unused.
  std::uint16_t port = 0;
  {
    auto listener = TcpSocket::Listen(Endpoint{"127.0.0.1", 0});
    ASSERT_TRUE(listener.ok());
    port = listener->LocalEndpoint()->port;
  }
  auto client = TcpSocket::ConnectWithTimeout(Endpoint{"127.0.0.1", port}, 500);
  EXPECT_FALSE(client.ok());
}

TEST(TcpSocket, ReadReportsWouldBlockNotError) {
  auto listener = TcpSocket::Listen(Endpoint{"127.0.0.1", 0});
  ASSERT_TRUE(listener.ok());
  auto client = TcpSocket::ConnectWithTimeout(listener->LocalEndpoint().value(), 2000);
  ASSERT_TRUE(client.ok());
  ASSERT_TRUE(client->SetNonBlocking(true).ok());

  ByteBuffer buffer;
  auto transfer = client->ReadInto(buffer, 1024);
  ASSERT_TRUE(transfer.ok());
  EXPECT_TRUE(transfer->would_block);
  EXPECT_EQ(transfer->bytes, 0U);
}

TEST(TcpSocket, EofIsReportedOnPeerClose) {
  auto listener = TcpSocket::Listen(Endpoint{"127.0.0.1", 0});
  ASSERT_TRUE(listener.ok());
  auto client = TcpSocket::ConnectWithTimeout(listener->LocalEndpoint().value(), 2000);
  ASSERT_TRUE(client.ok());

  bool would_block = true;
  Result<TcpSocket> server = Status{ErrorCode::kUnknown, "not yet"};
  ASSERT_TRUE(WaitFor([&] {
    server = listener->Accept(would_block);
    return server.ok() && !would_block;
  }));
  client->Close();

  ByteBuffer buffer;
  ASSERT_TRUE(WaitFor([&] {
    auto transfer = server->ReadInto(buffer, 1024);
    return transfer.ok() && transfer->eof;
  }));
}

TEST(TcpSocket, VectoredWriteSendsEverything) {
  auto listener = TcpSocket::Listen(Endpoint{"127.0.0.1", 0});
  ASSERT_TRUE(listener.ok());
  auto client = TcpSocket::ConnectWithTimeout(listener->LocalEndpoint().value(), 2000);
  ASSERT_TRUE(client.ok());

  bool would_block = true;
  Result<TcpSocket> server = Status{ErrorCode::kUnknown, "not yet"};
  ASSERT_TRUE(WaitFor([&] {
    server = listener->Accept(would_block);
    return server.ok() && !would_block;
  }));

  const std::string a = "hello ";
  const std::string b = "vectored ";
  const std::string c = "world";
  const std::array<ByteSpan, 3> chunks{AsBytes(a), AsBytes(b), AsBytes(c)};
  auto wrote = client->WriteVectored(chunks);
  ASSERT_TRUE(wrote.ok());
  EXPECT_EQ(wrote->bytes, a.size() + b.size() + c.size());

  ByteBuffer buffer;
  ASSERT_TRUE(WaitFor([&] {
    auto transfer = server->ReadInto(buffer, 1024);
    return transfer.ok() && buffer.ReadableBytes() == a.size() + b.size() + c.size();
  }));
  EXPECT_EQ(AsStringView(buffer.Readable()), "hello vectored world");
}

// --- Poller ----------------------------------------------------------------

TEST(Poller, ReportsReadiness) {
  auto poller = CreatePoller();
  ASSERT_TRUE(poller.ok());
  EXPECT_FALSE(poller.value()->Name().empty());

  auto listener = TcpSocket::Listen(Endpoint{"127.0.0.1", 0});
  ASSERT_TRUE(listener.ok());
  ASSERT_TRUE(poller.value()->Add(listener->fd(), EventMask::kRead).ok());

  std::vector<PollEvent> events;
  auto ready = poller.value()->Wait(events, 10);
  ASSERT_TRUE(ready.ok());
  EXPECT_EQ(ready.value(), 0U) << "nothing pending yet";

  auto client = TcpSocket::ConnectWithTimeout(listener->LocalEndpoint().value(), 2000);
  ASSERT_TRUE(client.ok());

  ASSERT_TRUE(WaitFor([&] {
    auto again = poller.value()->Wait(events, 50);
    return again.ok() && again.value() > 0;
  }));
  ASSERT_FALSE(events.empty());
  EXPECT_EQ(events[0].fd, listener->fd());
  EXPECT_TRUE(events[0].readable);

  EXPECT_TRUE(poller.value()->Remove(listener->fd()).ok());
}

TEST(Poller, ModifyChangesInterest) {
  auto poller = CreatePoller();
  ASSERT_TRUE(poller.ok());
  auto listener = TcpSocket::Listen(Endpoint{"127.0.0.1", 0});
  ASSERT_TRUE(listener.ok());
  auto client = TcpSocket::ConnectWithTimeout(listener->LocalEndpoint().value(), 2000);
  ASSERT_TRUE(client.ok());
  ASSERT_TRUE(client->SetNonBlocking(true).ok());

  // A connected socket with an empty send buffer is always writable.
  ASSERT_TRUE(poller.value()->Add(client->fd(), EventMask::kWrite).ok());
  std::vector<PollEvent> events;
  ASSERT_TRUE(WaitFor([&] {
    auto ready = poller.value()->Wait(events, 50);
    return ready.ok() && !events.empty() && events[0].writable;
  }));

  // After dropping write interest it must stop firing.
  ASSERT_TRUE(poller.value()->Modify(client->fd(), EventMask::kRead).ok());
  auto ready = poller.value()->Wait(events, 30);
  ASSERT_TRUE(ready.ok());
  for (const auto& event : events) EXPECT_FALSE(event.writable);
}

TEST(Poller, RemoveOfUnknownDescriptorIsNotAnError) {
  auto poller = CreatePoller();
  ASSERT_TRUE(poller.ok());
  auto listener = TcpSocket::Listen(Endpoint{"127.0.0.1", 0});
  ASSERT_TRUE(listener.ok());
  EXPECT_TRUE(poller.value()->Remove(listener->fd()).ok());
}

// --- Event loop ------------------------------------------------------------

TEST(EventLoop, RunsPostedTasks) {
  EventLoop loop(0);
  ASSERT_TRUE(loop.Init().ok());

  std::atomic<int> counter{0};
  std::thread thread([&] { loop.Run(); });
  ASSERT_TRUE(WaitFor([&] { return loop.running(); }));

  for (int i = 0; i < 100; ++i) {
    EXPECT_TRUE(loop.PostTask([&counter] { counter.fetch_add(1); }));
  }
  EXPECT_TRUE(WaitFor([&] { return counter.load() == 100; }));

  loop.Stop();
  thread.join();
  EXPECT_FALSE(loop.running());
}

TEST(EventLoop, TaskQueueIsBoundedAndRejects) {
  EventLoopOptions options;
  options.task_queue_capacity = 8;
  EventLoop loop(0, options);
  ASSERT_TRUE(loop.Init().ok());

  // The loop is not running, so nothing drains: pushes must start failing
  // rather than growing without bound.
  int accepted = 0;
  for (int i = 0; i < 100; ++i) {
    if (loop.PostTask([] {})) ++accepted;
  }
  EXPECT_GT(accepted, 0);
  EXPECT_LE(accepted, 8);
  EXPECT_GT(loop.GetStats().tasks_rejected, 0U);
}

TEST(EventLoop, RepeatingTimerFires) {
  EventLoopOptions options;
  options.max_poll_timeout_ms = 5;
  EventLoop loop(0, options);
  ASSERT_TRUE(loop.Init().ok());

  std::atomic<int> ticks{0};
  std::thread thread([&] { loop.Run(); });
  ASSERT_TRUE(WaitFor([&] { return loop.running(); }));
  ASSERT_TRUE(loop.PostTask([&] { loop.ScheduleRepeating(5, [&ticks] { ticks.fetch_add(1); }); }));

  EXPECT_TRUE(WaitFor([&] { return ticks.load() >= 3; }, 3s));
  loop.Stop();
  thread.join();
}

TEST(EventLoop, StopIsIdempotentAndThreadSafe) {
  EventLoop loop(0);
  ASSERT_TRUE(loop.Init().ok());
  std::thread thread([&] { loop.Run(); });
  ASSERT_TRUE(WaitFor([&] { return loop.running(); }));

  std::vector<std::thread> stoppers;
  stoppers.reserve(4);
  for (int i = 0; i < 4; ++i) stoppers.emplace_back([&] { loop.Stop(); });
  for (auto& stopper : stoppers) stopper.join();
  thread.join();
  SUCCEED();
}

TEST(EventLoop, RejectsTasksAfterStop) {
  EventLoop loop(0);
  ASSERT_TRUE(loop.Init().ok());
  loop.Stop();
  EXPECT_FALSE(loop.PostTask([] {}));
}

// --- Server + connection ---------------------------------------------------

// A test server that echoes each frame's payload back with the same request ID.
class EchoServer {
 public:
  explicit EchoServer(ServerOptions options = {}) {
    options.bind = Endpoint{"127.0.0.1", 0};
    if (options.io_threads == 0) options.io_threads = 2;
    server_ = std::make_unique<TcpServer>(
        std::move(options),
        [this](Connection& conn, const protocol::FrameDecoder::Frame& frame) {
          frames_.fetch_add(1, std::memory_order_relaxed);
          last_opcode_.store(static_cast<int>(frame.header.opcode), std::memory_order_relaxed);
          (void)conn.SendFrame(frame.header.opcode,
                               frame.header.request_id,
                               static_cast<std::uint16_t>(protocol::FrameFlags::kResponse),
                               frame.payload);
        },
        [this](Connection&, const Status&) { closed_.fetch_add(1, std::memory_order_relaxed); });
  }

  [[nodiscard]] Status Start() { return server_->Start(); }

  void Stop() { server_->Stop(); }

  [[nodiscard]] Endpoint endpoint() const { return server_->bound_endpoint(); }

  [[nodiscard]] TcpServer& server() { return *server_; }

  [[nodiscard]] std::uint64_t frames() const { return frames_.load(std::memory_order_relaxed); }

  [[nodiscard]] std::uint64_t closed() const { return closed_.load(std::memory_order_relaxed); }

 private:
  std::unique_ptr<TcpServer> server_;
  std::atomic<std::uint64_t> frames_{0};
  std::atomic<std::uint64_t> closed_{0};
  std::atomic<int> last_opcode_{0};
};

TEST(TcpServer, RoundTripsFrames) {
  EchoServer server;
  ASSERT_TRUE(server.Start().ok());

  SyncClient client;
  ASSERT_TRUE(client.Connect(server.endpoint()).ok());

  for (RequestId id = 1; id <= 20; ++id) {
    const std::string payload = "request-" + std::to_string(id);
    auto response = client.Call(protocol::OpCode::kHealth, id, AsBytes(payload));
    ASSERT_TRUE(response.ok()) << response.status().ToString();
    EXPECT_EQ(response->header.request_id, id);
    EXPECT_TRUE(response->header.is_response());
    EXPECT_EQ(AsStringView(response->payload), payload);
  }
  EXPECT_EQ(server.frames(), 20U);

  client.Close();
  server.Stop();
}

TEST(TcpServer, HandlesManyConcurrentConnections) {
  EchoServer server;
  ASSERT_TRUE(server.Start().ok());

  constexpr int kClients = 32;
  constexpr int kRequestsPerClient = 20;
  std::atomic<int> succeeded{0};

  std::vector<std::thread> threads;
  threads.reserve(kClients);
  for (int c = 0; c < kClients; ++c) {
    threads.emplace_back([&, c] {
      SyncClient client;
      if (!client.Connect(server.endpoint()).ok()) return;
      for (int i = 0; i < kRequestsPerClient; ++i) {
        const std::string payload = std::to_string(c) + ":" + std::to_string(i);
        auto response =
            client.Call(protocol::OpCode::kHealth, static_cast<RequestId>(i + 1), AsBytes(payload));
        if (response.ok() && AsStringView(response->payload) == payload) {
          succeeded.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }
  for (auto& thread : threads) thread.join();

  EXPECT_EQ(succeeded.load(), kClients * kRequestsPerClient);
  EXPECT_EQ(server.server().AcceptedTotal(), kClients);
  server.Stop();
}

TEST(TcpServer, ReassemblesFramesSplitAcrossReads) {
  EchoServer server;
  ASSERT_TRUE(server.Start().ok());

  auto socket = TcpSocket::ConnectWithTimeout(server.endpoint(), 2000);
  ASSERT_TRUE(socket.ok());

  // Send one frame one byte at a time. The server must not act until the
  // whole frame (and its checksum) has arrived.
  const std::string payload(200, 'x');
  ByteBuffer framed;
  protocol::EncodeFrame(framed, protocol::OpCode::kHealth, 99, 0, AsBytes(payload));

  const auto bytes = framed.Readable();
  for (std::size_t i = 0; i < bytes.size(); ++i) {
    auto wrote = socket->Write(bytes.subspan(i, 1));
    ASSERT_TRUE(wrote.ok());
    if (i + 1 < bytes.size()) {
      EXPECT_EQ(server.frames(), 0U) << "server acted on a partial frame at byte " << i;
    }
  }
  EXPECT_TRUE(WaitFor([&] { return server.frames() == 1U; }));
  server.Stop();
}

TEST(TcpServer, HandlesPipelinedFramesInOneWrite) {
  EchoServer server;
  ASSERT_TRUE(server.Start().ok());

  auto socket = TcpSocket::ConnectWithTimeout(server.endpoint(), 2000);
  ASSERT_TRUE(socket.ok());

  ByteBuffer batch;
  for (int i = 0; i < 50; ++i) {
    protocol::EncodeFrame(
        batch, protocol::OpCode::kHealth, static_cast<RequestId>(i), 0, AsBytes("pipelined"));
  }
  ByteSpan remaining = batch.Readable();
  while (!remaining.empty()) {
    auto wrote = socket->Write(remaining);
    ASSERT_TRUE(wrote.ok());
    remaining = remaining.subspan(wrote->bytes);
  }
  EXPECT_TRUE(WaitFor([&] { return server.frames() == 50U; }));
  server.Stop();
}

TEST(TcpServer, ClosesConnectionOnMalformedFrame) {
  EchoServer server;
  ASSERT_TRUE(server.Start().ok());

  auto socket = TcpSocket::ConnectWithTimeout(server.endpoint(), 2000);
  ASSERT_TRUE(socket.ok());

  const std::vector<std::uint8_t> garbage(64, 0xAB);
  auto wrote = socket->Write(garbage);
  ASSERT_TRUE(wrote.ok());

  // The server must drop the connection, not try to resynchronise.
  ByteBuffer buffer;
  EXPECT_TRUE(WaitFor([&] {
    auto transfer = socket->ReadInto(buffer, 256);
    return transfer.ok() && transfer->eof;
  }));
  EXPECT_TRUE(WaitFor([&] { return server.closed() >= 1U; }));
  server.Stop();
}

TEST(TcpServer, ClosesConnectionOnCorruptChecksum) {
  EchoServer server;
  ASSERT_TRUE(server.Start().ok());

  auto socket = TcpSocket::ConnectWithTimeout(server.endpoint(), 2000);
  ASSERT_TRUE(socket.ok());

  ByteBuffer framed;
  protocol::EncodeFrame(framed, protocol::OpCode::kHealth, 1, 0, AsBytes("payload"));
  std::vector<std::uint8_t> bytes(framed.Readable().begin(), framed.Readable().end());
  bytes.back() ^= 0xFF;  // Corrupt the payload after the header.

  auto wrote = socket->Write(bytes);
  ASSERT_TRUE(wrote.ok());

  ByteBuffer buffer;
  EXPECT_TRUE(WaitFor([&] {
    auto transfer = socket->ReadInto(buffer, 256);
    return transfer.ok() && transfer->eof;
  }));
  EXPECT_EQ(server.frames(), 0U) << "a corrupt frame must never reach the handler";
  server.Stop();
}

TEST(TcpServer, EnforcesMaxConnections) {
  ServerOptions options;
  options.max_connections = 2;
  options.io_threads = 1;
  EchoServer server(options);
  ASSERT_TRUE(server.Start().ok());

  std::vector<TcpSocket> sockets;
  for (int i = 0; i < 2; ++i) {
    auto socket = TcpSocket::ConnectWithTimeout(server.endpoint(), 2000);
    ASSERT_TRUE(socket.ok());
    sockets.push_back(std::move(socket).value());
  }
  ASSERT_TRUE(WaitFor([&] { return server.server().ConnectionCount() == 2; }));

  // The third is accepted by the kernel then explicitly refused by the server.
  auto extra = TcpSocket::ConnectWithTimeout(server.endpoint(), 2000);
  ASSERT_TRUE(extra.ok());
  EXPECT_TRUE(WaitFor([&] { return server.server().RejectedTotal() >= 1; }));
  EXPECT_LE(server.server().ConnectionCount(), 2U);
  server.Stop();
}

TEST(TcpServer, StopClosesEverythingAndJoins) {
  EchoServer server;
  ASSERT_TRUE(server.Start().ok());

  std::vector<TcpSocket> sockets;
  for (int i = 0; i < 5; ++i) {
    auto socket = TcpSocket::ConnectWithTimeout(server.endpoint(), 2000);
    ASSERT_TRUE(socket.ok());
    sockets.push_back(std::move(socket).value());
  }
  ASSERT_TRUE(WaitFor([&] { return server.server().ConnectionCount() == 5; }));

  server.Stop();
  EXPECT_FALSE(server.server().running());
  EXPECT_EQ(server.closed(), 5U) << "every connection must get its close callback";

  // Every peer must observe EOF, not a hang.
  for (auto& socket : sockets) {
    ByteBuffer buffer;
    EXPECT_TRUE(WaitFor([&] {
      auto transfer = socket.ReadInto(buffer, 64);
      return transfer.ok() && transfer->eof;
    }));
  }
}

TEST(TcpServer, StopIsSafeWithoutStart) {
  ServerOptions options;
  options.bind = Endpoint{"127.0.0.1", 0};
  TcpServer server(options, [](Connection&, const protocol::FrameDecoder::Frame&) {}, nullptr);
  server.Stop();
  SUCCEED();
}

TEST(TcpServer, RejectsFramesAboveTheConfiguredLimit) {
  ServerOptions options;
  options.connection.max_frame_bytes = 1024;
  EchoServer server(options);
  ASSERT_TRUE(server.Start().ok());

  auto socket = TcpSocket::ConnectWithTimeout(server.endpoint(), 2000);
  ASSERT_TRUE(socket.ok());

  // Only the header is sent: the server must reject on the declared length
  // alone, before waiting for (or allocating for) the body.
  protocol::FrameHeader header;
  header.opcode = protocol::OpCode::kProduce;
  header.request_id = 1;
  header.payload_len = 10 * 1024 * 1024;
  std::vector<std::uint8_t> bytes(protocol::kFrameHeaderSize);
  protocol::EncodeFrameHeader(header, bytes.data());
  auto wrote = socket->Write(bytes);
  ASSERT_TRUE(wrote.ok());

  ByteBuffer buffer;
  EXPECT_TRUE(WaitFor([&] {
    auto transfer = socket->ReadInto(buffer, 256);
    return transfer.ok() && transfer->eof;
  }));
  server.Stop();
}

// --- Backpressure ----------------------------------------------------------

TEST(Connection, PausesReadsWhenOutputBacksUp) {
  // A client that pipelines large requests and never reads the responses must
  // cause the server to stop reading, not to buffer without bound.
  ServerOptions options;
  options.io_threads = 1;
  options.connection.output_high_water_bytes = 64 * 1024;
  options.connection.output_low_water_bytes = 16 * 1024;
  options.connection.output_max_bytes = 1024 * 1024;
  // See the note in ClosesWhenOutputCeilingIsExceeded: the kernel buffer has to
  // be bounded or it absorbs everything and no backpressure is ever observed.
  options.connection.send_buffer_bytes = 8192;

  std::atomic<std::size_t> max_pending{0};
  std::atomic<std::uint64_t> read_pauses{0};
  std::atomic<int> frames_handled{0};
  TcpServer server(
      [&] {
        ServerOptions copy = options;
        copy.bind = Endpoint{"127.0.0.1", 0};
        return copy;
      }(),
      [&](Connection& conn, const protocol::FrameDecoder::Frame& frame) {
        frames_handled.fetch_add(1, std::memory_order_relaxed);
        // Reply several times per request so output outruns the peer, which
        // is the condition backpressure exists to survive.
        for (int i = 0; i < 8; ++i) {
          if (!conn.SendFrame(frame.header.opcode, frame.header.request_id, 0, frame.payload)) {
            break;
          }
        }
        std::size_t observed = max_pending.load(std::memory_order_relaxed);
        while (conn.PendingOutputBytes() > observed &&
               !max_pending.compare_exchange_weak(observed, conn.PendingOutputBytes())) {
        }
        read_pauses.store(conn.stats().read_pauses, std::memory_order_relaxed);
      },
      nullptr);
  ASSERT_TRUE(server.Start().ok());

  auto socket = TcpSocket::ConnectWithTimeout(server.bound_endpoint(), 2000);
  ASSERT_TRUE(socket.ok());
  ASSERT_TRUE(socket->SetNonBlocking(true).ok());
  // A small receive buffer makes the server's send buffer fill quickly.
  ASSERT_TRUE(socket->SetReceiveBufferSize(4096).ok());

  const std::string payload(32 * 1024, 'p');
  ByteBuffer framed;
  protocol::EncodeFrame(framed, protocol::OpCode::kProduce, 1, 0, AsBytes(payload));

  // Write without ever reading, until the socket refuses more.
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  bool socket_blocked = false;
  while (std::chrono::steady_clock::now() < deadline && !socket_blocked) {
    ByteSpan remaining = framed.Readable();
    while (!remaining.empty()) {
      auto wrote = socket->Write(remaining);
      if (!wrote.ok()) {
        socket_blocked = true;
        break;
      }
      if (wrote->would_block) {
        socket_blocked = true;
        break;
      }
      remaining = remaining.subspan(wrote->bytes);
    }
  }

  EXPECT_TRUE(socket_blocked) << "the client's own socket should block first";
  ASSERT_TRUE(WaitFor([&] { return frames_handled.load() > 0; }))
      << "the server never saw a request, so nothing was exercised";

  // Two properties, both load-bearing:
  //  1. queued output never exceeded the hard ceiling, so one connection
  //     cannot consume unbounded memory;
  //  2. the connection actually stopped reading, which is what pushes back on
  //     the client rather than just dropping work.
  EXPECT_LE(max_pending.load(), options.connection.output_max_bytes);
  EXPECT_TRUE(WaitFor([&] { return read_pauses.load() > 0; }))
      << "reads were never paused despite output exceeding the high-water mark";
  server.Stop();
}

TEST(Connection, ClosesWhenOutputCeilingIsExceeded) {
  ServerOptions options;
  options.bind = Endpoint{"127.0.0.1", 0};
  options.io_threads = 1;
  options.connection.output_high_water_bytes = 8 * 1024;
  options.connection.output_low_water_bytes = 4 * 1024;
  options.connection.output_max_bytes = 32 * 1024;
  // Without this the test measures the kernel, not the broker. Linux autotunes
  // the socket write buffer up to net.ipv4.tcp_wmem's maximum -- 4 MiB on a
  // stock kernel -- so it happily absorbs every response and the userspace
  // queue never reaches the ceiling under test.
  options.connection.send_buffer_bytes = 4096;

  std::atomic<int> closes{0};
  std::atomic<int> close_code{0};
  TcpServer server(
      options,
      [](Connection& conn, const protocol::FrameDecoder::Frame& frame) {
        // Reply many times over, far beyond the ceiling, to a peer that is not
        // reading.
        for (int i = 0; i < 100; ++i) {
          if (!conn.SendFrame(frame.header.opcode, frame.header.request_id, 0, frame.payload)) {
            break;
          }
        }
      },
      [&](Connection&, const Status& reason) {
        closes.fetch_add(1);
        close_code.store(static_cast<int>(reason.code()));
      });
  ASSERT_TRUE(server.Start().ok());

  auto socket = TcpSocket::ConnectWithTimeout(server.bound_endpoint(), 2000);
  ASSERT_TRUE(socket.ok());
  ASSERT_TRUE(socket->SetReceiveBufferSize(2048).ok());

  ByteBuffer framed;
  const std::string payload(16 * 1024, 'q');
  protocol::EncodeFrame(framed, protocol::OpCode::kProduce, 1, 0, AsBytes(payload));
  ByteSpan remaining = framed.Readable();
  while (!remaining.empty()) {
    auto wrote = socket->Write(remaining);
    ASSERT_TRUE(wrote.ok());
    remaining = remaining.subspan(wrote->bytes);
  }

  EXPECT_TRUE(WaitFor([&] { return closes.load() > 0; }));
  EXPECT_EQ(close_code.load(), static_cast<int>(ErrorCode::kResourceExhausted));
  server.Stop();
}

// --- Sync client -----------------------------------------------------------

TEST(SyncClient, ReportsTimeoutWhenBrokerIsSilent) {
  // A server that accepts but never replies.
  auto listener = TcpSocket::Listen(Endpoint{"127.0.0.1", 0});
  ASSERT_TRUE(listener.ok());
  const Endpoint endpoint = listener->LocalEndpoint().value();

  std::atomic<bool> stop{false};
  std::thread acceptor([&] {
    std::vector<TcpSocket> held;
    while (!stop.load()) {
      bool would_block = false;
      auto client = listener->Accept(would_block);
      if (client.ok() && !would_block) held.push_back(std::move(client).value());
      std::this_thread::sleep_for(1ms);
    }
  });

  SyncClientOptions options;
  options.request_timeout_ms = 200;
  SyncClient client(options);
  ASSERT_TRUE(client.Connect(endpoint).ok());

  auto response = client.Call(protocol::OpCode::kHealth, 1, AsBytes("ping"));
  EXPECT_FALSE(response.ok());
  EXPECT_EQ(response.status().code(), ErrorCode::kTimeout);

  stop.store(true);
  acceptor.join();
}

TEST(SyncClient, DetectsBrokerDisconnect) {
  EchoServer server;
  ASSERT_TRUE(server.Start().ok());

  SyncClient client;
  ASSERT_TRUE(client.Connect(server.endpoint()).ok());
  auto first = client.Call(protocol::OpCode::kHealth, 1, AsBytes("ok"));
  ASSERT_TRUE(first.ok());

  server.Stop();

  // The next call must fail cleanly rather than hang.
  auto second = client.Call(protocol::OpCode::kHealth, 2, AsBytes("after"));
  EXPECT_FALSE(second.ok());
  EXPECT_TRUE(second.status().code() == ErrorCode::kClosed ||
              second.status().code() == ErrorCode::kIoError)
      << second.status().ToString();
}

TEST(SyncClient, MeasuresRoundTrip) {
  EchoServer server;
  ASSERT_TRUE(server.Start().ok());
  SyncClient client;
  ASSERT_TRUE(client.Connect(server.endpoint()).ok());

  auto response = client.Call(protocol::OpCode::kHealth, 1, AsBytes("x"));
  ASSERT_TRUE(response.ok());
  EXPECT_GT(client.last_round_trip_nanos(), 0);
  EXPECT_LT(client.last_round_trip_nanos(), 5'000'000'000LL);
  server.Stop();
}

}  // namespace
}  // namespace pulselog::net
