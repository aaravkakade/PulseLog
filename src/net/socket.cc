#include "pulselog/net/socket.h"

#include <array>
#include <cerrno>
#include <charconv>
#include <cstring>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

namespace pulselog::net {
namespace {

// Resolves an endpoint to a sockaddr. Numeric addresses are handled directly;
// names go through getaddrinfo (used for Docker service names).
Status ResolveEndpoint(const Endpoint& endpoint,
                       ::sockaddr_storage& out,
                       ::socklen_t& out_len,
                       int& family) {
  std::memset(&out, 0, sizeof(out));

  // Try IPv4 literal first: the common case in a benchmark loop.
  ::sockaddr_in v4{};
  if (::inet_pton(AF_INET, endpoint.host.c_str(), &v4.sin_addr) == 1) {
    v4.sin_family = AF_INET;
    v4.sin_port = htons(endpoint.port);
    std::memcpy(&out, &v4, sizeof(v4));
    out_len = sizeof(v4);
    family = AF_INET;
    return OkStatus();
  }

  ::sockaddr_in6 v6{};
  if (::inet_pton(AF_INET6, endpoint.host.c_str(), &v6.sin6_addr) == 1) {
    v6.sin6_family = AF_INET6;
    v6.sin6_port = htons(endpoint.port);
    std::memcpy(&out, &v6, sizeof(v6));
    out_len = sizeof(v6);
    family = AF_INET6;
    return OkStatus();
  }

  ::addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  ::addrinfo* result = nullptr;
  const std::string port = std::to_string(endpoint.port);
  const int rc = ::getaddrinfo(endpoint.host.c_str(), port.c_str(), &hints, &result);
  if (rc != 0 || result == nullptr) {
    return NotFound("cannot resolve " + endpoint.ToString() + ": " + ::gai_strerror(rc));
  }
  std::memcpy(&out, result->ai_addr, result->ai_addrlen);
  out_len = result->ai_addrlen;
  family = result->ai_family;
  ::freeaddrinfo(result);
  return OkStatus();
}

Result<Endpoint> EndpointFromSockaddr(const ::sockaddr_storage& addr) {
  std::array<char, INET6_ADDRSTRLEN> buffer{};
  Endpoint endpoint;
  if (addr.ss_family == AF_INET) {
    const auto* v4 = reinterpret_cast<const ::sockaddr_in*>(&addr);
    if (::inet_ntop(AF_INET, &v4->sin_addr, buffer.data(), buffer.size()) == nullptr) {
      return ErrnoToStatus("inet_ntop", errno);
    }
    endpoint.host = buffer.data();
    endpoint.port = ntohs(v4->sin_port);
  } else if (addr.ss_family == AF_INET6) {
    const auto* v6 = reinterpret_cast<const ::sockaddr_in6*>(&addr);
    if (::inet_ntop(AF_INET6, &v6->sin6_addr, buffer.data(), buffer.size()) == nullptr) {
      return ErrnoToStatus("inet_ntop", errno);
    }
    endpoint.host = buffer.data();
    endpoint.port = ntohs(v6->sin6_port);
  } else {
    return InvalidArgument("unsupported address family");
  }
  return endpoint;
}

// EAGAIN and EWOULDBLOCK are required to be distinct by POSIX but are the same
// value on Linux and on macOS. Testing both is the portable idiom; writing it
// as `a == EAGAIN || a == EWOULDBLOCK` makes GCC report a tautological 'or'.
[[nodiscard]] bool IsWouldBlock(int err) noexcept {
#if defined(EWOULDBLOCK) && EWOULDBLOCK != EAGAIN
  return err == EAGAIN || err == EWOULDBLOCK;
#else
  return err == EAGAIN;
#endif
}

// macOS has no MSG_NOSIGNAL; it uses a per-socket option instead. Without one
// of the two, writing to a socket the peer has closed raises SIGPIPE and kills
// the process -- which a broker must never do just because a client vanished.
void DisableSigPipe([[maybe_unused]] int fd) {
#ifdef SO_NOSIGPIPE
  int enable = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &enable, sizeof(enable));
#endif
}

}  // namespace

Result<Endpoint> Endpoint::Parse(std::string_view text) {
  if (text.empty()) return InvalidArgument("empty endpoint");

  Endpoint endpoint;
  std::string_view port_text;
  if (text.front() == '[') {
    const auto close = text.find(']');
    if (close == std::string_view::npos || close + 2 > text.size() || text[close + 1] != ':') {
      return InvalidArgument("malformed IPv6 endpoint: " + std::string(text));
    }
    endpoint.host = std::string(text.substr(1, close - 1));
    port_text = text.substr(close + 2);
  } else {
    const auto colon = text.rfind(':');
    if (colon == std::string_view::npos) {
      return InvalidArgument("endpoint must be host:port, got " + std::string(text));
    }
    endpoint.host = std::string(text.substr(0, colon));
    port_text = text.substr(colon + 1);
  }

  std::uint32_t port = 0;
  const auto [ptr, ec] =
      std::from_chars(port_text.data(), port_text.data() + port_text.size(), port);
  if (ec != std::errc{} || ptr != port_text.data() + port_text.size() || port == 0 ||
      port > 65535) {
    return InvalidArgument("invalid port in endpoint: " + std::string(text));
  }
  endpoint.port = static_cast<std::uint16_t>(port);
  if (endpoint.host.empty()) endpoint.host = "0.0.0.0";
  return endpoint;
}

void TcpSocket::Close() noexcept {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

Status TcpSocket::SetNonBlocking(bool enable) const {
  const int flags = ::fcntl(fd_, F_GETFL, 0);
  if (flags < 0) return ErrnoToStatus("fcntl(F_GETFL)", errno);
  const int updated = enable ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK);
  if (::fcntl(fd_, F_SETFL, updated) < 0) return ErrnoToStatus("fcntl(F_SETFL)", errno);
  return OkStatus();
}

Status TcpSocket::SetNoDelay(bool enable) const {
  const int value = enable ? 1 : 0;
  if (::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &value, sizeof(value)) != 0) {
    return ErrnoToStatus("setsockopt(TCP_NODELAY)", errno);
  }
  return OkStatus();
}

Status TcpSocket::SetKeepAlive(bool enable) const {
  const int value = enable ? 1 : 0;
  if (::setsockopt(fd_, SOL_SOCKET, SO_KEEPALIVE, &value, sizeof(value)) != 0) {
    return ErrnoToStatus("setsockopt(SO_KEEPALIVE)", errno);
  }
  return OkStatus();
}

Status TcpSocket::SetSendBufferSize(int bytes) const {
  if (::setsockopt(fd_, SOL_SOCKET, SO_SNDBUF, &bytes, sizeof(bytes)) != 0) {
    return ErrnoToStatus("setsockopt(SO_SNDBUF)", errno);
  }
  return OkStatus();
}

Status TcpSocket::SetReceiveBufferSize(int bytes) const {
  if (::setsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &bytes, sizeof(bytes)) != 0) {
    return ErrnoToStatus("setsockopt(SO_RCVBUF)", errno);
  }
  return OkStatus();
}

Result<TcpSocket> TcpSocket::Listen(const Endpoint& endpoint, int backlog, bool reuse_port) {
  ::sockaddr_storage addr{};
  ::socklen_t addr_len = 0;
  int family = AF_INET;
  PL_RETURN_IF_ERROR(ResolveEndpoint(endpoint, addr, addr_len, family));

  const int fd = ::socket(family, SOCK_STREAM, 0);
  if (fd < 0) return ErrnoToStatus("socket", errno);
  TcpSocket socket(fd);
  DisableSigPipe(fd);

  int enable = 1;
  if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable)) != 0) {
    return ErrnoToStatus("setsockopt(SO_REUSEADDR)", errno);
  }
#ifdef SO_REUSEPORT
  if (reuse_port && ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &enable, sizeof(enable)) != 0) {
    return ErrnoToStatus("setsockopt(SO_REUSEPORT)", errno);
  }
#else
  (void)reuse_port;
#endif

  PL_RETURN_IF_ERROR(socket.SetNonBlocking(true));

  if (::bind(fd, reinterpret_cast<::sockaddr*>(&addr), addr_len) != 0) {
    return ErrnoToStatus("bind " + endpoint.ToString(), errno);
  }
  if (::listen(fd, backlog) != 0) {
    return ErrnoToStatus("listen " + endpoint.ToString(), errno);
  }
  return socket;
}

Result<TcpSocket> TcpSocket::ConnectAsync(const Endpoint& endpoint, bool& in_progress) {
  in_progress = false;
  ::sockaddr_storage addr{};
  ::socklen_t addr_len = 0;
  int family = AF_INET;
  PL_RETURN_IF_ERROR(ResolveEndpoint(endpoint, addr, addr_len, family));

  const int fd = ::socket(family, SOCK_STREAM, 0);
  if (fd < 0) return ErrnoToStatus("socket", errno);
  TcpSocket socket(fd);
  DisableSigPipe(fd);
  PL_RETURN_IF_ERROR(socket.SetNonBlocking(true));
  PL_RETURN_IF_ERROR(socket.SetNoDelay(true));

  if (::connect(fd, reinterpret_cast<::sockaddr*>(&addr), addr_len) == 0) {
    return socket;
  }
  if (errno == EINPROGRESS || errno == EALREADY) {
    in_progress = true;
    return socket;
  }
  return ErrnoToStatus("connect " + endpoint.ToString(), errno);
}

Result<TcpSocket> TcpSocket::ConnectWithTimeout(const Endpoint& endpoint, std::int64_t timeout_ms) {
  bool in_progress = false;
  PL_ASSIGN_OR_RETURN(TcpSocket socket, ConnectAsync(endpoint, in_progress));
  if (!in_progress) return socket;

  ::pollfd pfd{};
  pfd.fd = socket.fd();
  pfd.events = POLLOUT;
  const int ready = ::poll(&pfd, 1, static_cast<int>(timeout_ms));
  if (ready == 0) {
    return TimedOut("connect to " + endpoint.ToString() + " timed out after " +
                    std::to_string(timeout_ms) + "ms");
  }
  if (ready < 0) return ErrnoToStatus("poll during connect", errno);

  PL_RETURN_IF_ERROR(socket.TakeSocketError());
  return socket;
}

Result<TcpSocket> TcpSocket::Accept(bool& would_block, Endpoint* peer) const {
  would_block = false;
  ::sockaddr_storage addr{};
  ::socklen_t addr_len = sizeof(addr);

  int client = -1;
  do {
    client = ::accept(fd_, reinterpret_cast<::sockaddr*>(&addr), &addr_len);
  } while (client < 0 && errno == EINTR);

  if (client < 0) {
    if (IsWouldBlock(errno)) {
      would_block = true;
      return TcpSocket{};
    }
    // ECONNABORTED means the peer went away between the SYN and the accept.
    // Routine on a busy listener; the caller retries rather than failing.
    if (errno == ECONNABORTED) {
      would_block = true;
      return TcpSocket{};
    }
    return ErrnoToStatus("accept", errno);
  }

  TcpSocket socket(client);
  DisableSigPipe(client);
  PL_RETURN_IF_ERROR(socket.SetNonBlocking(true));
  PL_RETURN_IF_ERROR(socket.SetNoDelay(true));
  if (peer != nullptr) {
    auto resolved = EndpointFromSockaddr(addr);
    if (resolved.ok()) *peer = std::move(resolved).value();
  }
  return socket;
}

Result<TransferResult> TcpSocket::ReadInto(ByteBuffer& buffer, std::size_t max_bytes) const {
  TransferResult result;
  buffer.EnsureWritable(max_bytes);

  ssize_t got = 0;
  do {
    got = ::recv(fd_, buffer.WritePtr(), max_bytes, 0);
  } while (got < 0 && errno == EINTR);

  if (got > 0) {
    buffer.Commit(static_cast<std::size_t>(got));
    result.bytes = static_cast<std::size_t>(got);
    return result;
  }
  if (got == 0) {
    result.eof = true;
    return result;
  }
  if (IsWouldBlock(errno)) {
    result.would_block = true;
    return result;
  }
  if (errno == ECONNRESET) {
    // A reset is a normal way for a client to disappear, not an error worth
    // logging at any level above debug.
    result.eof = true;
    return result;
  }
  return ErrnoToStatus("recv", errno);
}

Result<TransferResult> TcpSocket::Write(ByteSpan data) const {
  TransferResult result;
  if (data.empty()) return result;

  ssize_t wrote = 0;
  do {
#ifdef MSG_NOSIGNAL
    // Without this, writing to a closed socket raises SIGPIPE and kills the
    // process. macOS uses the SO_NOSIGPIPE socket option instead, set below.
    wrote = ::send(fd_, data.data(), data.size(), MSG_NOSIGNAL);
#else
    wrote = ::send(fd_, data.data(), data.size(), 0);
#endif
  } while (wrote < 0 && errno == EINTR);

  if (wrote >= 0) {
    result.bytes = static_cast<std::size_t>(wrote);
    return result;
  }
  if (IsWouldBlock(errno)) {
    result.would_block = true;
    return result;
  }
  if (errno == EPIPE || errno == ECONNRESET) {
    result.eof = true;
    return result;
  }
  return ErrnoToStatus("send", errno);
}

Result<TransferResult> TcpSocket::WriteVectored(std::span<const ByteSpan> chunks) const {
  TransferResult result;
  if (chunks.empty()) return result;

  std::array<::iovec, kMaxWriteChunks> vecs{};
  std::size_t count = 0;
  for (const auto& chunk : chunks) {
    if (chunk.empty()) continue;
    if (count == vecs.size()) break;
    vecs[count].iov_base = const_cast<std::uint8_t*>(chunk.data());
    vecs[count].iov_len = chunk.size();
    ++count;
  }
  if (count == 0) return result;

  ssize_t wrote = 0;
  do {
    ::msghdr message{};
    message.msg_iov = vecs.data();
    message.msg_iovlen = Narrow<decltype(message.msg_iovlen)>(count);
#ifdef MSG_NOSIGNAL
    wrote = ::sendmsg(fd_, &message, MSG_NOSIGNAL);
#else
    wrote = ::sendmsg(fd_, &message, 0);
#endif
  } while (wrote < 0 && errno == EINTR);

  if (wrote >= 0) {
    result.bytes = static_cast<std::size_t>(wrote);
    return result;
  }
  if (IsWouldBlock(errno)) {
    result.would_block = true;
    return result;
  }
  if (errno == EPIPE || errno == ECONNRESET) {
    result.eof = true;
    return result;
  }
  return ErrnoToStatus("sendmsg", errno);
}

Status TcpSocket::TakeSocketError() const {
  int error = 0;
  ::socklen_t len = sizeof(error);
  if (::getsockopt(fd_, SOL_SOCKET, SO_ERROR, &error, &len) != 0) {
    return ErrnoToStatus("getsockopt(SO_ERROR)", errno);
  }
  if (error != 0) return ErrnoToStatus("socket error", error);
  return OkStatus();
}

Result<Endpoint> TcpSocket::LocalEndpoint() const {
  ::sockaddr_storage addr{};
  ::socklen_t len = sizeof(addr);
  if (::getsockname(fd_, reinterpret_cast<::sockaddr*>(&addr), &len) != 0) {
    return ErrnoToStatus("getsockname", errno);
  }
  return EndpointFromSockaddr(addr);
}

Result<Endpoint> TcpSocket::PeerEndpoint() const {
  ::sockaddr_storage addr{};
  ::socklen_t len = sizeof(addr);
  if (::getpeername(fd_, reinterpret_cast<::sockaddr*>(&addr), &len) != 0) {
    return ErrnoToStatus("getpeername", errno);
  }
  return EndpointFromSockaddr(addr);
}

void TcpSocket::ShutdownWrite() const noexcept {
  if (fd_ >= 0) ::shutdown(fd_, SHUT_WR);
}

}  // namespace pulselog::net
