// A minimal HTTP endpoint for metrics, health and the dashboard.
//
// This is deliberately NOT built on the broker's event loop. Scrapes arrive
// every few seconds and must keep working even when the data-plane loops are
// saturated -- an operator needs metrics most precisely when the broker is
// unhealthy. So it runs on its own thread with a blocking accept loop, one
// request at a time. That is the wrong design for the data path and the right
// one here.
//
// It speaks just enough HTTP/1.1 to satisfy Prometheus and a browser: GET
// only, no keep-alive, no chunked encoding, bounded request size.
#ifndef PULSELOG_METRICS_EXPORTER_H_
#define PULSELOG_METRICS_EXPORTER_H_

#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <thread>

#include "pulselog/base/status.h"
#include "pulselog/metrics/registry.h"

namespace pulselog::metrics {

// Returns (content_type, body) for a request path.
struct HttpResponse {
  int status_code = 200;
  std::string content_type = "text/plain; charset=utf-8";
  std::string body;
};

using HttpHandler = std::function<HttpResponse()>;

class MetricsExporter {
 public:
  MetricsExporter(MetricRegistry& registry, std::string bind_address, std::uint16_t port);

  MetricsExporter(const MetricsExporter&) = delete;
  MetricsExporter& operator=(const MetricsExporter&) = delete;

  ~MetricsExporter();

  // Registers a handler for an exact path, e.g. "/topology". Must be called
  // before Start().
  void AddHandler(std::string path, HttpHandler handler);

  [[nodiscard]] Status Start();

  void Stop();

  [[nodiscard]] bool running() const noexcept { return running_.load(std::memory_order_acquire); }

  // Actual bound port, useful when the caller passed 0 to get an ephemeral one.
  [[nodiscard]] std::uint16_t port() const noexcept { return bound_port_; }

 private:
  void ServeLoop();

  void HandleConnection(int client_fd);

  [[nodiscard]] HttpResponse Dispatch(const std::string& path) const;

  MetricRegistry& registry_;
  std::string bind_address_;
  std::uint16_t port_;
  std::uint16_t bound_port_ = 0;

  int listen_fd_ = -1;
  std::atomic<bool> running_{false};
  std::atomic<bool> stopping_{false};
  std::thread thread_;
  std::map<std::string, HttpHandler> handlers_;
};

}  // namespace pulselog::metrics

#endif  // PULSELOG_METRICS_EXPORTER_H_
