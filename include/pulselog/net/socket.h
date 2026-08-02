// Socket and address primitives.
//
// Every socket the broker creates is non-blocking and every read/write path
// handles short transfers. `TcpSocket` owns its descriptor and closes it on
// destruction; there are no raw descriptors passed around by value.
#ifndef PULSELOG_NET_SOCKET_H_
#define PULSELOG_NET_SOCKET_H_

#include <cstdint>
#include <span>
#include <string>
#include <string_view>

#include "pulselog/base/buffer.h"
#include "pulselog/base/status.h"

namespace pulselog::net {

// A resolved IPv4/IPv6 endpoint.
struct Endpoint {
  std::string host = "0.0.0.0";
  std::uint16_t port = 0;

  [[nodiscard]] std::string ToString() const { return host + ":" + std::to_string(port); }

  // Parses "host:port". IPv6 literals must be bracketed: "[::1]:9092".
  [[nodiscard]] static Result<Endpoint> Parse(std::string_view text);
};

// Result of a non-blocking transfer.
struct TransferResult {
  std::size_t bytes = 0;
  bool would_block = false;  // Nothing more can move right now.
  bool eof = false;          // Peer closed its sending side.
};

class TcpSocket {
 public:
  TcpSocket() = default;

  explicit TcpSocket(int fd) noexcept : fd_(fd) {}

  TcpSocket(const TcpSocket&) = delete;
  TcpSocket& operator=(const TcpSocket&) = delete;

  TcpSocket(TcpSocket&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }

  TcpSocket& operator=(TcpSocket&& other) noexcept {
    if (this != &other) {
      Close();
      fd_ = other.fd_;
      other.fd_ = -1;
    }
    return *this;
  }

  ~TcpSocket() { Close(); }

  // Creates a listening socket bound to `endpoint`. Sets SO_REUSEADDR so a
  // restarting broker does not have to wait out TIME_WAIT.
  [[nodiscard]] static Result<TcpSocket> Listen(const Endpoint& endpoint,
                                                int backlog = 512,
                                                bool reuse_port = false);

  // Starts a non-blocking connect. Returns a connected socket, or one that is
  // still in progress -- check `in_progress`.
  [[nodiscard]] static Result<TcpSocket> ConnectAsync(const Endpoint& endpoint, bool& in_progress);

  // Blocking connect with a deadline. Used by the client SDK and by the
  // replication fetcher, both of which have a thread to spare.
  [[nodiscard]] static Result<TcpSocket> ConnectWithTimeout(const Endpoint& endpoint,
                                                            std::int64_t timeout_ms);

  // Accepts one pending connection. `would_block` is set when there are none.
  [[nodiscard]] Result<TcpSocket> Accept(bool& would_block, Endpoint* peer = nullptr) const;

  [[nodiscard]] bool valid() const noexcept { return fd_ >= 0; }

  [[nodiscard]] int fd() const noexcept { return fd_; }

  // Relinquishes ownership. The caller becomes responsible for closing.
  [[nodiscard]] int Release() noexcept {
    const int fd = fd_;
    fd_ = -1;
    return fd;
  }

  void Close() noexcept;

  [[nodiscard]] Status SetNonBlocking(bool enable) const;

  // Disables Nagle. Required: without it, a small request followed by a small
  // response can wait up to 40 ms for the delayed-ACK timer, which dominates
  // every latency number this project reports.
  [[nodiscard]] Status SetNoDelay(bool enable) const;

  [[nodiscard]] Status SetKeepAlive(bool enable) const;

  [[nodiscard]] Status SetSendBufferSize(int bytes) const;

  [[nodiscard]] Status SetReceiveBufferSize(int bytes) const;

  // Reads into `buffer`'s writable region, growing it to `max_bytes` first.
  [[nodiscard]] Result<TransferResult> ReadInto(ByteBuffer& buffer, std::size_t max_bytes) const;

  [[nodiscard]] Result<TransferResult> Write(ByteSpan data) const;

  // Scatter-gather write. Used to send a response header and body, or several
  // queued responses, in one syscall.
  [[nodiscard]] Result<TransferResult> WriteVectored(std::span<const ByteSpan> chunks) const;

  // Retrieves and clears SO_ERROR. Used to complete a non-blocking connect.
  [[nodiscard]] Status TakeSocketError() const;

  [[nodiscard]] Result<Endpoint> LocalEndpoint() const;

  [[nodiscard]] Result<Endpoint> PeerEndpoint() const;

  // Half-closes the sending side so the peer sees EOF while we drain reads.
  void ShutdownWrite() const noexcept;

 private:
  int fd_ = -1;
};

// Largest number of iovecs passed to one writev call. Above this the syscall
// gains nothing and the array stops fitting comfortably on the stack.
inline constexpr std::size_t kMaxWriteChunks = 64;

}  // namespace pulselog::net

#endif  // PULSELOG_NET_SOCKET_H_
