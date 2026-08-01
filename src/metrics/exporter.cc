#include "pulselog/metrics/exporter.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>

#include "pulselog/base/logging.h"
#include "pulselog/concurrency/thread_util.h"

namespace pulselog::metrics {
namespace {

constexpr std::string_view kComponent = "metrics.http";

// Requests are tiny; anything larger is not a scrape.
constexpr std::size_t kMaxRequestBytes = 8192;

// How long a single request may take before the server gives up on it. Keeps
// one stuck client from blocking the (single-threaded) scrape endpoint.
constexpr int kRequestTimeoutMs = 5000;

std::string StatusText(int code) {
  switch (code) {
    case 200:
      return "OK";
    case 400:
      return "Bad Request";
    case 404:
      return "Not Found";
    case 405:
      return "Method Not Allowed";
    default:
      return "Internal Server Error";
  }
}

}  // namespace

MetricsExporter::MetricsExporter(MetricRegistry& registry, std::string bind_address,
                                 std::uint16_t port)
    : registry_(registry), bind_address_(std::move(bind_address)), port_(port) {}

MetricsExporter::~MetricsExporter() { Stop(); }

void MetricsExporter::AddHandler(std::string path, HttpHandler handler) {
  handlers_.emplace(std::move(path), std::move(handler));
}

Status MetricsExporter::Start() {
  if (running_.load(std::memory_order_acquire)) return OkStatus();

  listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd_ < 0) return ErrnoToStatus("socket", errno);

  int enable = 1;
  ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable));

  ::sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port_);
  if (::inet_pton(AF_INET, bind_address_.c_str(), &addr.sin_addr) != 1) {
    ::close(listen_fd_);
    listen_fd_ = -1;
    return InvalidArgument("invalid metrics bind address: " + bind_address_);
  }

  if (::bind(listen_fd_, reinterpret_cast<::sockaddr*>(&addr), sizeof(addr)) != 0) {
    const Status status = ErrnoToStatus("bind metrics port " + std::to_string(port_), errno);
    ::close(listen_fd_);
    listen_fd_ = -1;
    return status;
  }
  if (::listen(listen_fd_, 16) != 0) {
    const Status status = ErrnoToStatus("listen", errno);
    ::close(listen_fd_);
    listen_fd_ = -1;
    return status;
  }

  ::sockaddr_in bound{};
  ::socklen_t bound_len = sizeof(bound);
  if (::getsockname(listen_fd_, reinterpret_cast<::sockaddr*>(&bound), &bound_len) == 0) {
    bound_port_ = ntohs(bound.sin_port);
  }

  running_.store(true, std::memory_order_release);
  stopping_.store(false, std::memory_order_release);
  thread_ = std::thread([this] {
    SetCurrentThreadName("pl-metrics");
    ServeLoop();
  });

  PL_INFO(kComponent) << "metrics endpoint listening"
                      << " address=" << bind_address_ << " port=" << bound_port_;
  return OkStatus();
}

void MetricsExporter::Stop() {
  if (!running_.exchange(false, std::memory_order_acq_rel)) return;
  stopping_.store(true, std::memory_order_release);

  // Shutting down the listening socket makes the blocked accept() return, so
  // the thread exits without needing a signal or a self-connect trick.
  if (listen_fd_ >= 0) {
    ::shutdown(listen_fd_, SHUT_RDWR);
    ::close(listen_fd_);
    listen_fd_ = -1;
  }
  if (thread_.joinable()) thread_.join();
}

void MetricsExporter::ServeLoop() {
  while (!stopping_.load(std::memory_order_acquire)) {
    // poll() with a timeout rather than a bare blocking accept, so shutdown is
    // prompt even on platforms where closing the fd does not wake accept().
    ::pollfd pfd{};
    pfd.fd = listen_fd_;
    pfd.events = POLLIN;
    const int ready = ::poll(&pfd, 1, 200);
    if (ready <= 0) {
      if (ready < 0 && errno != EINTR) break;
      continue;
    }
    if (listen_fd_ < 0) break;

    const int client = ::accept(listen_fd_, nullptr, nullptr);
    if (client < 0) {
      if (errno == EINTR || errno == EAGAIN) continue;
      break;
    }
    HandleConnection(client);
    ::close(client);
  }
}

void MetricsExporter::HandleConnection(int client_fd) {
  std::string request;
  std::array<char, 2048> buffer{};

  // Read until the end of the request headers.
  while (request.find("\r\n\r\n") == std::string::npos && request.size() < kMaxRequestBytes) {
    ::pollfd pfd{};
    pfd.fd = client_fd;
    pfd.events = POLLIN;
    if (::poll(&pfd, 1, kRequestTimeoutMs) <= 0) return;

    const ssize_t got = ::recv(client_fd, buffer.data(), buffer.size(), 0);
    if (got <= 0) return;
    request.append(buffer.data(), static_cast<std::size_t>(got));
  }

  HttpResponse response;
  const auto first_space = request.find(' ');
  const auto second_space = request.find(' ', first_space + 1);
  if (first_space == std::string::npos || second_space == std::string::npos) {
    response.status_code = 400;
    response.body = "malformed request line\n";
  } else if (request.compare(0, first_space, "GET") != 0) {
    response.status_code = 405;
    response.body = "only GET is supported\n";
  } else {
    std::string path = request.substr(first_space + 1, second_space - first_space - 1);
    const auto query = path.find('?');
    if (query != std::string::npos) path = path.substr(0, query);
    response = Dispatch(path);
  }

  std::string out;
  out.reserve(response.body.size() + 160);
  out += "HTTP/1.1 " + std::to_string(response.status_code) + ' ' +
         StatusText(response.status_code) + "\r\n";
  out += "Content-Type: " + response.content_type + "\r\n";
  out += "Content-Length: " + std::to_string(response.body.size()) + "\r\n";
  out += "Connection: close\r\n\r\n";
  out += response.body;

  std::size_t sent = 0;
  while (sent < out.size()) {
    const ssize_t wrote = ::send(client_fd, out.data() + sent, out.size() - sent, 0);
    if (wrote <= 0) {
      if (errno == EINTR) continue;
      break;
    }
    sent += static_cast<std::size_t>(wrote);
  }
}

HttpResponse MetricsExporter::Dispatch(const std::string& path) const {
  if (path == "/metrics") {
    return HttpResponse{200, "text/plain; version=0.0.4; charset=utf-8",
                        registry_.RenderPrometheus()};
  }
  if (path == "/metrics.json") {
    return HttpResponse{200, "application/json", registry_.RenderJson()};
  }
  if (path == "/health" || path == "/-/healthy") {
    return HttpResponse{200, "text/plain; charset=utf-8", "ok\n"};
  }

  const auto it = handlers_.find(path);
  if (it != handlers_.end()) return it->second();

  std::string body = "not found\n\navailable paths:\n  /metrics\n  /metrics.json\n  /health\n";
  for (const auto& [handler_path, unused] : handlers_) {
    body += "  " + handler_path + "\n";
  }
  return HttpResponse{404, "text/plain; charset=utf-8", std::move(body)};
}

}  // namespace pulselog::metrics
