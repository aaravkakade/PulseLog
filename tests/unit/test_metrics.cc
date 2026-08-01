#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <random>
#include <thread>
#include <vector>

#include "pulselog/metrics/exporter.h"
#include "pulselog/metrics/histogram.h"
#include "pulselog/metrics/process_stats.h"
#include "pulselog/metrics/registry.h"
#include "test_support/http_client.h"

namespace pulselog::metrics {
namespace {

TEST(Histogram, EmptyReportsZeros) {
  Histogram h;
  EXPECT_EQ(h.Count(), 0U);
  EXPECT_EQ(h.ValueAtPercentile(50.0), 0);
  EXPECT_EQ(h.Min(), 0);
  EXPECT_EQ(h.Max(), 0);
  EXPECT_DOUBLE_EQ(h.Mean(), 0.0);
}

TEST(Histogram, TracksCountMinMax) {
  Histogram h;
  for (std::int64_t v : {5, 100, 3, 77, 1000}) h.Record(v);
  EXPECT_EQ(h.Count(), 5U);
  EXPECT_EQ(h.Min(), 3);
  EXPECT_EQ(h.Max(), 1000);
  EXPECT_NEAR(h.Mean(), (5 + 100 + 3 + 77 + 1000) / 5.0, 1.0);
}

TEST(Histogram, PercentilesOnUniformDistribution) {
  Histogram h;
  for (std::int64_t v = 1; v <= 100'000; ++v) h.Record(v);

  // With 3 significant digits the reported value must be within 0.1%.
  const auto within = [](std::int64_t actual, std::int64_t expected) {
    const double tolerance = std::max(2.0, static_cast<double>(expected) * 0.002);
    return std::abs(static_cast<double>(actual - expected)) <= tolerance;
  };
  EXPECT_TRUE(within(h.ValueAtPercentile(50.0), 50'000)) << h.ValueAtPercentile(50.0);
  EXPECT_TRUE(within(h.ValueAtPercentile(90.0), 90'000)) << h.ValueAtPercentile(90.0);
  EXPECT_TRUE(within(h.ValueAtPercentile(99.0), 99'000)) << h.ValueAtPercentile(99.0);
  EXPECT_TRUE(within(h.ValueAtPercentile(99.9), 99'900)) << h.ValueAtPercentile(99.9);
}

TEST(Histogram, PrecisionBoundHoldsAcrossTheRange) {
  // The claim in histogram.h is 0.1% relative error for 3 significant digits.
  // Verify it directly at many magnitudes.
  Histogram h(/*max_trackable=*/1'000'000'000'000, /*significant_digits=*/3);
  for (std::int64_t magnitude = 1; magnitude <= 1'000'000'000'000; magnitude *= 10) {
    for (const std::int64_t multiplier : {1, 3, 7}) {
      const std::int64_t value = magnitude * multiplier;
      if (value > 1'000'000'000'000) continue;
      Histogram single(1'000'000'000'000, 3);
      single.Record(value);
      const std::int64_t reported = single.ValueAtPercentile(50.0);
      const double error = std::abs(static_cast<double>(reported - value)) /
                           static_cast<double>(value);
      EXPECT_LE(error, 0.001) << "value " << value << " reported as " << reported;
    }
  }
  h.Record(1);
  EXPECT_EQ(h.Count(), 1U);
}

TEST(Histogram, ReportedPercentileNeverUnderstates) {
  // A latency histogram that reports lower than the truth is worse than
  // useless. Every reported value must be >= the true value at that rank.
  std::mt19937 rng(4242);
  std::vector<std::int64_t> samples;
  samples.reserve(20'000);
  Histogram h;
  for (int i = 0; i < 20'000; ++i) {
    const auto value = static_cast<std::int64_t>(rng() % 5'000'000);
    samples.push_back(value);
    h.Record(value);
  }
  std::sort(samples.begin(), samples.end());

  // Ranks are computed in integer arithmetic. Doing it in floating point is a
  // trap: 99.9/100.0*20000 evaluates to 19980.000000000004, so ceil() returns
  // 19981 and the "expected" value silently shifts by one sample.
  const std::size_t n = samples.size();
  for (const std::size_t permille : {500U, 900U, 990U, 999U}) {
    const std::size_t rank = (permille * n + 999) / 1000;  // ceil(p * n)
    const std::int64_t truth = samples[std::min(rank, n) - 1];
    const double percentile = static_cast<double>(permille) / 10.0;
    const std::int64_t reported = h.ValueAtPercentile(percentile);

    EXPECT_GE(reported, truth) << "p" << percentile << " understated";
    EXPECT_LE(static_cast<double>(reported - truth) /
                  static_cast<double>(std::max<std::int64_t>(truth, 1)),
              0.01)
        << "p" << percentile << " overstated by more than 1%";
  }
}

TEST(Histogram, OverflowIsCountedNotHidden) {
  Histogram h(/*max_trackable=*/1000, /*significant_digits=*/3);
  h.Record(500);
  h.Record(100'000);
  EXPECT_EQ(h.Count(), 2U);
  EXPECT_EQ(h.OverflowCount(), 1U);
  EXPECT_EQ(h.Max(), 1000) << "an overflowing value clamps to the trackable maximum";
}

TEST(Histogram, NegativeValuesAreClampedToZero) {
  Histogram h;
  h.Record(-5);
  EXPECT_EQ(h.Count(), 1U);
  EXPECT_EQ(h.Min(), 0);
}

TEST(Histogram, ResetClearsEverything) {
  Histogram h;
  for (int i = 0; i < 100; ++i) h.Record(i * 1000);
  h.Reset();
  EXPECT_EQ(h.Count(), 0U);
  EXPECT_EQ(h.Max(), 0);
  EXPECT_EQ(h.ValueAtPercentile(99.0), 0);
}

TEST(Histogram, AddMergesDistributions) {
  Histogram a;
  Histogram b;
  for (int i = 0; i < 1000; ++i) a.Record(100);
  for (int i = 0; i < 1000; ++i) b.Record(200);
  a.Add(b);
  EXPECT_EQ(a.Count(), 2000U);
  EXPECT_NEAR(a.Mean(), 150.0, 1.0);
  EXPECT_EQ(a.Max(), 200);
}

TEST(Histogram, ConcurrentRecordingConservesCount) {
  Histogram h;
  constexpr int kThreads = 4;
  constexpr int kPerThread = 50'000;

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&h, t] {
      for (int i = 0; i < kPerThread; ++i) h.Record(1000 + t);
    });
  }
  for (auto& thread : threads) thread.join();
  EXPECT_EQ(h.Count(), static_cast<std::uint64_t>(kThreads) * kPerThread);
}

TEST(Histogram, SnapshotIsSelfConsistent) {
  Histogram h;
  for (int i = 1; i <= 10'000; ++i) h.Record(i);
  const auto snapshot = h.GetSnapshot();
  EXPECT_EQ(snapshot.count, 10'000U);
  EXPECT_LE(snapshot.min, snapshot.p50);
  EXPECT_LE(snapshot.p50, snapshot.p90);
  EXPECT_LE(snapshot.p90, snapshot.p95);
  EXPECT_LE(snapshot.p95, snapshot.p99);
  EXPECT_LE(snapshot.p99, snapshot.p999);
  EXPECT_LE(snapshot.p999, snapshot.max);
  EXPECT_FALSE(snapshot.ToString().empty());
}

// --- Registry --------------------------------------------------------------

TEST(Registry, CounterAndGauge) {
  MetricRegistry registry;
  auto& counter = registry.GetCounter("requests_total", "requests");
  counter.Increment();
  counter.Increment(5);
  EXPECT_EQ(counter.Value(), 6U);

  auto& gauge = registry.GetGauge("connections", "open connections");
  gauge.Set(10);
  gauge.Increment(2);
  gauge.Decrement();
  EXPECT_EQ(gauge.Value(), 11);
}

TEST(Registry, SameNameAndLabelsReturnsSameInstance) {
  MetricRegistry registry;
  auto& first = registry.GetCounter("c", "help");
  auto& second = registry.GetCounter("c", "help");
  first.Increment(3);
  EXPECT_EQ(second.Value(), 3U);
  EXPECT_EQ(registry.Size(), 1U);
}

TEST(Registry, DifferentLabelsAreDistinctSeries) {
  MetricRegistry registry;
  auto& a = registry.GetCounter("bytes", "b", {{"topic", "orders"}});
  auto& b = registry.GetCounter("bytes", "b", {{"topic", "events"}});
  a.Increment(10);
  b.Increment(20);
  EXPECT_EQ(a.Value(), 10U);
  EXPECT_EQ(b.Value(), 20U);
  EXPECT_EQ(registry.Size(), 2U);
}

TEST(Registry, RemoveByLabelDropsSeries) {
  MetricRegistry registry;
  registry.GetCounter("bytes", "b", {{"topic", "gone"}});
  registry.GetGauge("size", "s", {{"topic", "gone"}});
  registry.GetCounter("bytes", "b", {{"topic", "kept"}});

  EXPECT_EQ(registry.RemoveByLabel("topic", "gone"), 2U);
  EXPECT_EQ(registry.Size(), 1U);
}

TEST(Registry, PrometheusOutputIsWellFormed) {
  MetricRegistry registry;
  registry.GetCounter("pulselog_requests_total", "Total requests").Increment(42);
  registry.GetGauge("pulselog_connections", "Open connections").Set(7);
  auto& latency = registry.GetHistogram("pulselog_latency_nanos", "Latency");
  for (int i = 1; i <= 100; ++i) latency.Record(i * 1000);

  const std::string text = registry.RenderPrometheus();
  EXPECT_NE(text.find("# HELP pulselog_requests_total Total requests"), std::string::npos);
  EXPECT_NE(text.find("# TYPE pulselog_requests_total counter"), std::string::npos);
  EXPECT_NE(text.find("pulselog_requests_total 42"), std::string::npos);
  EXPECT_NE(text.find("pulselog_connections 7"), std::string::npos);
  EXPECT_NE(text.find("pulselog_latency_nanos{quantile=\"0.99\"}"), std::string::npos);
  EXPECT_NE(text.find("pulselog_latency_nanos_count 100"), std::string::npos);
}

TEST(Registry, PrometheusEscapesLabelValues) {
  MetricRegistry registry;
  registry.GetCounter("m", "h", {{"name", "quote\"and\\slash"}}).Increment();
  const std::string text = registry.RenderPrometheus();
  EXPECT_NE(text.find("quote\\\"and\\\\slash"), std::string::npos);
}

TEST(Registry, JsonOutputContainsPercentiles) {
  MetricRegistry registry;
  auto& latency = registry.GetHistogram("lat", "latency");
  for (int i = 1; i <= 1000; ++i) latency.Record(i);
  registry.GetGauge("g", "gauge", {{"broker", "1"}}).Set(3);

  const std::string json = registry.RenderJson();
  EXPECT_NE(json.find("\"p99\":"), std::string::npos);
  EXPECT_NE(json.find("\"type\":\"histogram\""), std::string::npos);
  EXPECT_NE(json.find("\"broker\":\"1\""), std::string::npos);
  EXPECT_EQ(json.front(), '{');
  EXPECT_EQ(json.back(), '}');
}

TEST(BrokerMetrics, RegistersEverything) {
  MetricRegistry registry;
  BrokerMetrics metrics(registry);
  metrics.messages_produced.Increment(100);
  metrics.produce_latency.Record(50'000);
  metrics.active_connections.Set(3);

  EXPECT_GT(registry.Size(), 20U);
  const std::string text = registry.RenderPrometheus();
  EXPECT_NE(text.find("pulselog_messages_produced_total 100"), std::string::npos);
  EXPECT_NE(text.find("pulselog_active_connections 3"), std::string::npos);
}

// --- Process stats ---------------------------------------------------------

TEST(ProcessStats, ReportsResidentMemory) {
  ProcessStatsSampler sampler;
  const auto sample = sampler.Sample();
  EXPECT_GT(sample.resident_bytes, 0U) << "resident set size must be observable";
}

TEST(ProcessStats, CpuPercentIsDeltaBased) {
  ProcessStatsSampler sampler;
  (void)sampler.Sample();
  // Burn a little CPU so the second sample has something to report.
  std::atomic<std::uint64_t> sink{0};
  for (std::uint64_t i = 0; i < 20'000'000; ++i) sink.fetch_add(i, std::memory_order_relaxed);
  const auto sample = sampler.Sample();
  EXPECT_GE(sample.cpu_percent, 0.0);
  EXPECT_GT(sample.user_micros, 0U);
}

TEST(ProcessStats, DescribesHost) {
  const auto host = ProcessStatsSampler::DescribeHost();
  EXPECT_FALSE(host.os.empty());
  EXPECT_FALSE(host.architecture.empty());
  EXPECT_GT(host.cpu_count, 0U);
}

// --- HTTP exporter ---------------------------------------------------------

// Issues a bare HTTP GET and returns the whole response.
std::string HttpGet(std::uint16_t port, const std::string& path) {
  return ::pulselog::testing::HttpGetRaw(port, path);
}

TEST(Exporter, ServesMetricsAndHealth) {
  MetricRegistry registry;
  registry.GetCounter("test_counter", "a counter").Increment(11);

  MetricsExporter exporter(registry, "127.0.0.1", 0);
  exporter.AddHandler("/topology", [] {
    return HttpResponse{200, "application/json", R"({"brokers":[]})"};
  });
  ASSERT_TRUE(exporter.Start().ok());
  ASSERT_GT(exporter.port(), 0);

  const std::string metrics = HttpGet(exporter.port(), "/metrics");
  EXPECT_NE(metrics.find("200 OK"), std::string::npos);
  EXPECT_NE(metrics.find("test_counter 11"), std::string::npos);

  const std::string health = HttpGet(exporter.port(), "/health");
  EXPECT_NE(health.find("200 OK"), std::string::npos);

  const std::string topology = HttpGet(exporter.port(), "/topology");
  EXPECT_NE(topology.find(R"({"brokers":[]})"), std::string::npos);

  const std::string missing = HttpGet(exporter.port(), "/nope");
  EXPECT_NE(missing.find("404"), std::string::npos);

  exporter.Stop();
  EXPECT_FALSE(exporter.running());
}

TEST(Exporter, StopIsIdempotent) {
  MetricRegistry registry;
  MetricsExporter exporter(registry, "127.0.0.1", 0);
  ASSERT_TRUE(exporter.Start().ok());
  exporter.Stop();
  exporter.Stop();  // Must not hang or crash.
  SUCCEED();
}

TEST(Exporter, RejectsInvalidBindAddress) {
  MetricRegistry registry;
  MetricsExporter exporter(registry, "not-an-address", 0);
  EXPECT_EQ(exporter.Start().code(), ErrorCode::kInvalidArgument);
}

}  // namespace
}  // namespace pulselog::metrics
