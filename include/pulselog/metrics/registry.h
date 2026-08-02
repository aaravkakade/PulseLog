// Metric registry and the concrete metric types.
//
// Metrics are created once at start-up and held by raw reference on the hot
// path, so recording a value is a single relaxed atomic operation with no map
// lookup and no lock. The registry owns everything and is only consulted when
// something scrapes it.
#ifndef PULSELOG_METRICS_REGISTRY_H_
#define PULSELOG_METRICS_REGISTRY_H_

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "pulselog/base/types.h"
#include "pulselog/metrics/histogram.h"

namespace pulselog::metrics {

// Label set attached to a metric, e.g. {{"topic","orders"},{"partition","3"}}.
using Labels = std::vector<std::pair<std::string, std::string>>;

// Monotonically increasing value. Never decreases; a restart resets it to 0,
// which is what Prometheus expects and handles.
class Counter {
 public:
  void Increment(std::uint64_t delta = 1) noexcept {
    value_.fetch_add(delta, std::memory_order_relaxed);
  }

  [[nodiscard]] std::uint64_t Value() const noexcept {
    return value_.load(std::memory_order_relaxed);
  }

  void Reset() noexcept { value_.store(0, std::memory_order_relaxed); }

 private:
  // Padded so counters updated by different threads do not share a cache line.
  alignas(kCacheLineSize) std::atomic<std::uint64_t> value_{0};
};

// A value that can go up and down (queue depth, connection count).
class Gauge {
 public:
  void Set(std::int64_t value) noexcept { value_.store(value, std::memory_order_relaxed); }

  void Increment(std::int64_t delta = 1) noexcept {
    value_.fetch_add(delta, std::memory_order_relaxed);
  }

  void Decrement(std::int64_t delta = 1) noexcept {
    value_.fetch_sub(delta, std::memory_order_relaxed);
  }

  [[nodiscard]] std::int64_t Value() const noexcept {
    return value_.load(std::memory_order_relaxed);
  }

 private:
  alignas(kCacheLineSize) std::atomic<std::int64_t> value_{0};
};

enum class MetricType : std::uint8_t { kCounter, kGauge, kHistogram };

struct MetricDescriptor {
  std::string name;
  std::string help;
  MetricType type = MetricType::kCounter;
  Labels labels;
};

class MetricRegistry {
 public:
  MetricRegistry() = default;

  MetricRegistry(const MetricRegistry&) = delete;
  MetricRegistry& operator=(const MetricRegistry&) = delete;

  // Returns the existing metric when one with the same name and labels is
  // already registered, so components can register defensively at start-up.
  Counter& GetCounter(std::string name, std::string help, Labels labels = {});

  Gauge& GetGauge(std::string name, std::string help, Labels labels = {});

  Histogram& GetHistogram(std::string name,
                          std::string help,
                          Labels labels = {},
                          std::int64_t max_trackable = 60'000'000'000,
                          int significant_digits = 3);

  // Removes every metric whose labels contain the given key/value. Used when a
  // topic or partition goes away so its series stop being exported.
  std::size_t RemoveByLabel(std::string_view key, std::string_view value);

  // Renders the whole registry in Prometheus text exposition format v0.0.4.
  [[nodiscard]] std::string RenderPrometheus() const;

  // Renders a compact JSON object, used by the dashboard and the benchmark
  // harness (which would otherwise have to parse the Prometheus format).
  [[nodiscard]] std::string RenderJson() const;

  [[nodiscard]] std::size_t Size() const;

 private:
  struct Entry {
    MetricDescriptor descriptor;
    std::unique_ptr<Counter> counter;
    std::unique_ptr<Gauge> gauge;
    std::unique_ptr<Histogram> histogram;
  };

  // Key is name + serialised labels, so the same metric name with different
  // labels yields distinct series.
  [[nodiscard]] static std::string MakeKey(const std::string& name, const Labels& labels);

  mutable std::mutex mutex_;
  std::map<std::string, Entry, std::less<>> entries_;
};

// The metrics a broker reports. Grouped in one struct so the hot paths hold
// direct references rather than looking anything up.
struct BrokerMetrics {
  explicit BrokerMetrics(MetricRegistry& registry);

  MetricRegistry& registry;

  // Throughput.
  Counter& messages_produced;
  Counter& bytes_produced;
  Counter& messages_fetched;
  Counter& bytes_fetched;
  Counter& produce_requests;
  Counter& fetch_requests;
  Counter& failed_requests;
  Counter& backpressure_rejections;

  // Latency, in nanoseconds.
  Histogram& produce_latency;
  Histogram& fetch_latency;
  Histogram& flush_latency;
  Histogram& replication_latency;
  Histogram& queue_wait;
  Histogram& produce_batch_size;

  // Produce-path stage breakdown, in nanoseconds. produce_latency alone says
  // a durable write was slow; it does not say whether the time went to the
  // worker queue, the append, the local fsync, or waiting for followers.
  // These four sum to roughly produce_latency, so a tail can be attributed
  // rather than guessed at.
  Histogram& produce_stage_queue;        // enqueue -> worker picked it up
  Histogram& produce_stage_append;       // time inside the log append
  Histogram& produce_stage_local_flush;  // append -> leader's own data on media
  Histogram& produce_stage_replication;  // leader flushed -> quorum flushed

  // State.
  Gauge& active_connections;
  Gauge& hosted_partitions;
  Gauge& leader_partitions;
  Gauge& request_queue_depth;
  Gauge& total_log_bytes;
  Gauge& replication_lag_max;
  Gauge& consumer_group_count;
  Gauge& resident_memory_bytes;
  Gauge& cpu_percent;

  // Connections and errors.
  Counter& connections_accepted;
  Counter& connections_closed;
  Counter& protocol_errors;
  Counter& corrupt_frames;
};

}  // namespace pulselog::metrics

#endif  // PULSELOG_METRICS_REGISTRY_H_
