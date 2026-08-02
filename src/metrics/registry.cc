#include "pulselog/metrics/registry.h"

#include <algorithm>
#include <sstream>

namespace pulselog::metrics {
namespace {

// Prometheus label values must escape backslash, quote and newline.
std::string EscapeLabelValue(std::string_view value) {
  std::string out;
  out.reserve(value.size());
  for (const char c : value) {
    switch (c) {
      case '\\':
        out += "\\\\";
        break;
      case '"':
        out += "\\\"";
        break;
      case '\n':
        out += "\\n";
        break;
      default:
        out.push_back(c);
    }
  }
  return out;
}

std::string EscapeJson(std::string_view value) {
  std::string out;
  out.reserve(value.size());
  for (const char c : value) {
    switch (c) {
      case '"':
        out += "\\\"";
        break;
      case '\\':
        out += "\\\\";
        break;
      case '\n':
        out += "\\n";
        break;
      case '\t':
        out += "\\t";
        break;
      default:
        out.push_back(c);
    }
  }
  return out;
}

void AppendLabels(std::ostringstream& out,
                  const Labels& labels,
                  const char* extra_key = nullptr,
                  const char* extra_value = nullptr) {
  if (labels.empty() && extra_key == nullptr) return;
  out << '{';
  bool first = true;
  for (const auto& [key, value] : labels) {
    if (!first) out << ',';
    first = false;
    out << key << "=\"" << EscapeLabelValue(value) << '"';
  }
  if (extra_key != nullptr) {
    if (!first) out << ',';
    out << extra_key << "=\"" << extra_value << '"';
  }
  out << '}';
}

}  // namespace

std::string MetricRegistry::MakeKey(const std::string& name, const Labels& labels) {
  std::string key = name;
  for (const auto& [label, value] : labels) {
    key += '\x1f';  // Unit separator: cannot appear in a metric or label name.
    key += label;
    key += '=';
    key += value;
  }
  return key;
}

Counter& MetricRegistry::GetCounter(std::string name, std::string help, Labels labels) {
  const std::string key = MakeKey(name, labels);
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = entries_.find(key);
  if (it != entries_.end() && it->second.counter != nullptr) return *it->second.counter;

  Entry entry;
  entry.descriptor =
      MetricDescriptor{std::move(name), std::move(help), MetricType::kCounter, std::move(labels)};
  entry.counter = std::make_unique<Counter>();
  Counter& ref = *entry.counter;
  entries_.emplace(key, std::move(entry));
  return ref;
}

Gauge& MetricRegistry::GetGauge(std::string name, std::string help, Labels labels) {
  const std::string key = MakeKey(name, labels);
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = entries_.find(key);
  if (it != entries_.end() && it->second.gauge != nullptr) return *it->second.gauge;

  Entry entry;
  entry.descriptor =
      MetricDescriptor{std::move(name), std::move(help), MetricType::kGauge, std::move(labels)};
  entry.gauge = std::make_unique<Gauge>();
  Gauge& ref = *entry.gauge;
  entries_.emplace(key, std::move(entry));
  return ref;
}

Histogram& MetricRegistry::GetHistogram(std::string name,
                                        std::string help,
                                        Labels labels,
                                        std::int64_t max_trackable,
                                        int significant_digits) {
  const std::string key = MakeKey(name, labels);
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = entries_.find(key);
  if (it != entries_.end() && it->second.histogram != nullptr) return *it->second.histogram;

  Entry entry;
  entry.descriptor =
      MetricDescriptor{std::move(name), std::move(help), MetricType::kHistogram, std::move(labels)};
  entry.histogram = std::make_unique<Histogram>(max_trackable, significant_digits);
  Histogram& ref = *entry.histogram;
  entries_.emplace(key, std::move(entry));
  return ref;
}

std::size_t MetricRegistry::RemoveByLabel(std::string_view key, std::string_view value) {
  std::lock_guard<std::mutex> lock(mutex_);
  std::size_t removed = 0;
  for (auto it = entries_.begin(); it != entries_.end();) {
    const auto& labels = it->second.descriptor.labels;
    const bool matches = std::any_of(labels.begin(), labels.end(), [&](const auto& label) {
      return label.first == key && label.second == value;
    });
    if (matches) {
      it = entries_.erase(it);
      ++removed;
    } else {
      ++it;
    }
  }
  return removed;
}

std::string MetricRegistry::RenderPrometheus() const {
  std::ostringstream out;
  std::lock_guard<std::mutex> lock(mutex_);

  // Prometheus requires HELP/TYPE once per metric family, not per series.
  std::string last_family;
  for (const auto& [key, entry] : entries_) {
    const auto& descriptor = entry.descriptor;
    if (descriptor.name != last_family) {
      out << "# HELP " << descriptor.name << ' ' << descriptor.help << '\n';
      out << "# TYPE " << descriptor.name << ' ';
      switch (descriptor.type) {
        case MetricType::kCounter:
          out << "counter";
          break;
        case MetricType::kGauge:
          out << "gauge";
          break;
        case MetricType::kHistogram:
          out << "summary";
          break;
      }
      out << '\n';
      last_family = descriptor.name;
    }

    switch (descriptor.type) {
      case MetricType::kCounter:
        out << descriptor.name;
        AppendLabels(out, descriptor.labels);
        out << ' ' << entry.counter->Value() << '\n';
        break;
      case MetricType::kGauge:
        out << descriptor.name;
        AppendLabels(out, descriptor.labels);
        out << ' ' << entry.gauge->Value() << '\n';
        break;
      case MetricType::kHistogram: {
        // Exported as a Prometheus summary: quantiles plus _sum and _count.
        // The underlying HDR histogram computes exact quantiles over the whole
        // observation set rather than an approximation over a sliding window.
        const HistogramSnapshot snapshot = entry.histogram->GetSnapshot();
        const std::pair<const char*, std::int64_t> quantiles[] = {{"0.5", snapshot.p50},
                                                                  {"0.9", snapshot.p90},
                                                                  {"0.95", snapshot.p95},
                                                                  {"0.99", snapshot.p99},
                                                                  {"0.999", snapshot.p999},
                                                                  {"1.0", snapshot.max}};
        for (const auto& [quantile, value] : quantiles) {
          out << descriptor.name;
          AppendLabels(out, descriptor.labels, "quantile", quantile);
          out << ' ' << value << '\n';
        }
        out << descriptor.name << "_sum";
        AppendLabels(out, descriptor.labels);
        out << ' ' << static_cast<std::int64_t>(snapshot.sum) << '\n';
        out << descriptor.name << "_count";
        AppendLabels(out, descriptor.labels);
        out << ' ' << snapshot.count << '\n';
        break;
      }
    }
  }
  return out.str();
}

std::string MetricRegistry::RenderJson() const {
  std::ostringstream out;
  std::lock_guard<std::mutex> lock(mutex_);
  out << "{\"metrics\":[";
  bool first = true;
  for (const auto& [key, entry] : entries_) {
    if (!first) out << ',';
    first = false;
    const auto& descriptor = entry.descriptor;
    out << "{\"name\":\"" << EscapeJson(descriptor.name) << "\",\"labels\":{";
    bool first_label = true;
    for (const auto& [label, value] : descriptor.labels) {
      if (!first_label) out << ',';
      first_label = false;
      out << '"' << EscapeJson(label) << "\":\"" << EscapeJson(value) << '"';
    }
    out << "},";
    switch (descriptor.type) {
      case MetricType::kCounter:
        out << "\"type\":\"counter\",\"value\":" << entry.counter->Value();
        break;
      case MetricType::kGauge:
        out << "\"type\":\"gauge\",\"value\":" << entry.gauge->Value();
        break;
      case MetricType::kHistogram: {
        const HistogramSnapshot snapshot = entry.histogram->GetSnapshot();
        out << "\"type\":\"histogram\",\"count\":" << snapshot.count << ",\"min\":" << snapshot.min
            << ",\"p50\":" << snapshot.p50 << ",\"p90\":" << snapshot.p90
            << ",\"p95\":" << snapshot.p95 << ",\"p99\":" << snapshot.p99
            << ",\"p999\":" << snapshot.p999 << ",\"max\":" << snapshot.max
            << ",\"mean\":" << static_cast<std::int64_t>(snapshot.mean);
        break;
      }
    }
    out << '}';
  }
  out << "]}";
  return out.str();
}

std::size_t MetricRegistry::Size() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return entries_.size();
}

BrokerMetrics::BrokerMetrics(MetricRegistry& reg)
    : registry(reg),
      messages_produced(
          reg.GetCounter("pulselog_messages_produced_total", "Records appended to any partition")),
      bytes_produced(reg.GetCounter("pulselog_bytes_produced_total", "Record bytes appended")),
      messages_fetched(
          reg.GetCounter("pulselog_messages_fetched_total", "Records returned to consumers")),
      bytes_fetched(reg.GetCounter("pulselog_bytes_fetched_total", "Record bytes returned")),
      produce_requests(
          reg.GetCounter("pulselog_produce_requests_total", "Produce requests served")),
      fetch_requests(reg.GetCounter("pulselog_fetch_requests_total", "Fetch requests served")),
      failed_requests(
          reg.GetCounter("pulselog_failed_requests_total", "Requests answered with an error")),
      backpressure_rejections(reg.GetCounter("pulselog_backpressure_rejections_total",
                                             "Requests rejected because a worker queue was full")),
      produce_latency(reg.GetHistogram("pulselog_produce_latency_nanos",
                                       "End-to-end produce handling latency in nanoseconds")),
      fetch_latency(reg.GetHistogram("pulselog_fetch_latency_nanos",
                                     "End-to-end fetch handling latency in nanoseconds")),
      flush_latency(
          reg.GetHistogram("pulselog_flush_latency_nanos", "fsync duration in nanoseconds")),
      replication_latency(reg.GetHistogram("pulselog_replication_latency_nanos",
                                           "Leader-to-follower acknowledgement latency")),
      queue_wait(
          reg.GetHistogram("pulselog_queue_wait_nanos", "Time a request spent in a worker queue")),
      produce_batch_size(reg.GetHistogram(
          "pulselog_produce_batch_records", "Records per produce request", {}, 1'000'000, 3)),
      produce_stage_queue(reg.GetHistogram("pulselog_produce_stage_queue_nanos",
                                           "Produce: time waiting in the worker queue")),
      produce_stage_append(reg.GetHistogram("pulselog_produce_stage_append_nanos",
                                            "Produce: time inside the log append")),
      produce_stage_local_flush(
          reg.GetHistogram("pulselog_produce_stage_local_flush_nanos",
                           "Produce: append until the leader's own data reached media")),
      produce_stage_replication(
          reg.GetHistogram("pulselog_produce_stage_replication_nanos",
                           "Produce: leader flushed until a quorum had flushed")),
      active_connections(
          reg.GetGauge("pulselog_active_connections", "Currently open client connections")),
      hosted_partitions(
          reg.GetGauge("pulselog_hosted_partitions", "Partitions hosted by this broker")),
      leader_partitions(reg.GetGauge("pulselog_leader_partitions", "Partitions this broker leads")),
      request_queue_depth(
          reg.GetGauge("pulselog_request_queue_depth", "Summed depth of all worker queues")),
      total_log_bytes(reg.GetGauge("pulselog_log_bytes", "Bytes stored across all partitions")),
      replication_lag_max(
          reg.GetGauge("pulselog_replication_lag_max_records", "Largest follower lag in records")),
      consumer_group_count(
          reg.GetGauge("pulselog_consumer_groups", "Consumer groups this broker coordinates")),
      resident_memory_bytes(
          reg.GetGauge("pulselog_resident_memory_bytes", "Process resident set size")),
      cpu_percent(reg.GetGauge("pulselog_cpu_percent", "Process CPU usage, percent of one core")),
      connections_accepted(
          reg.GetCounter("pulselog_connections_accepted_total", "Connections accepted")),
      connections_closed(reg.GetCounter("pulselog_connections_closed_total", "Connections closed")),
      protocol_errors(
          reg.GetCounter("pulselog_protocol_errors_total", "Frames rejected as malformed")),
      corrupt_frames(
          reg.GetCounter("pulselog_corrupt_frames_total", "Frames rejected by checksum")) {}

}  // namespace pulselog::metrics
