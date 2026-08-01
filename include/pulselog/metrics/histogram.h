// A High Dynamic Range histogram.
//
// Latency distributions are what this project reports, and an average hides
// exactly the part that matters. A naive alternative -- keeping every sample
// and sorting -- costs unbounded memory and cannot be updated from a hot path.
//
// This is the standard HdrHistogram layout: values are bucketed by exponent
// (a power-of-two "bucket") and then linearly within it (a "sub-bucket"), so
// relative error is bounded by the configured number of significant digits
// across the whole range. Recording is a count of leading zeros, two shifts
// and one atomic increment -- constant time, no allocation, safe to call from
// a request handler.
//
// Precision guarantee: for `significant_digits = 3`, any recorded value is
// reported within 0.1% of its true magnitude. `tests/unit/test_metrics.cc`
// verifies that bound across the full range.
#ifndef PULSELOG_METRICS_HISTOGRAM_H_
#define PULSELOG_METRICS_HISTOGRAM_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace pulselog::metrics {

struct HistogramSnapshot {
  std::uint64_t count = 0;
  std::int64_t min = 0;
  std::int64_t max = 0;
  std::int64_t p50 = 0;
  std::int64_t p90 = 0;
  std::int64_t p95 = 0;
  std::int64_t p99 = 0;
  std::int64_t p999 = 0;
  double mean = 0.0;
  double sum = 0.0;

  [[nodiscard]] std::string ToString() const;
};

class Histogram {
 public:
  // `max_trackable` bounds the range; values above it are clamped to it and
  // counted in `overflow_count()` so a saturated histogram is visible rather
  // than silently wrong.
  explicit Histogram(std::int64_t max_trackable = 60'000'000'000,  // 60 s in ns
                     int significant_digits = 3);

  Histogram(const Histogram&) = delete;
  Histogram& operator=(const Histogram&) = delete;

  // Safe to call concurrently from any number of threads. Uses relaxed atomic
  // increments: a reader may observe a slightly stale distribution, which is
  // the right trade for keeping this off the critical path.
  void Record(std::int64_t value) noexcept;

  void RecordMany(std::int64_t value, std::uint64_t count) noexcept;

  [[nodiscard]] std::int64_t ValueAtPercentile(double percentile) const noexcept;

  [[nodiscard]] std::uint64_t Count() const noexcept {
    return total_count_.load(std::memory_order_relaxed);
  }

  [[nodiscard]] std::int64_t Min() const noexcept;

  [[nodiscard]] std::int64_t Max() const noexcept;

  [[nodiscard]] double Mean() const noexcept;

  [[nodiscard]] std::uint64_t OverflowCount() const noexcept {
    return overflow_count_.load(std::memory_order_relaxed);
  }

  [[nodiscard]] HistogramSnapshot GetSnapshot() const noexcept;

  void Reset() noexcept;

  // Merges `other` into this histogram. Both must have identical parameters.
  void Add(const Histogram& other) noexcept;

  [[nodiscard]] std::int64_t max_trackable() const noexcept { return max_trackable_; }

  [[nodiscard]] int significant_digits() const noexcept { return significant_digits_; }

  // Bucket boundaries and counts, for the Prometheus histogram exporter.
  struct Bucket {
    std::int64_t upper_bound = 0;
    std::uint64_t count = 0;
  };

  [[nodiscard]] std::vector<Bucket> NonEmptyBuckets() const;

 private:
  [[nodiscard]] std::size_t IndexFor(std::int64_t value) const noexcept;

  [[nodiscard]] std::int64_t ValueForIndex(std::size_t index) const noexcept;

  std::int64_t max_trackable_;
  int significant_digits_;

  std::int32_t sub_bucket_half_count_magnitude_ = 0;
  std::int32_t sub_bucket_count_ = 0;
  std::int32_t sub_bucket_half_count_ = 0;
  std::int64_t sub_bucket_mask_ = 0;
  std::int32_t bucket_count_ = 0;
  std::int32_t leading_zero_count_base_ = 0;

  std::unique_ptr<std::atomic<std::uint64_t>[]> counts_;
  std::size_t counts_len_ = 0;

  std::atomic<std::uint64_t> total_count_{0};
  std::atomic<std::uint64_t> overflow_count_{0};
  std::atomic<std::int64_t> min_{INT64_MAX};
  std::atomic<std::int64_t> max_{0};
  std::atomic<double> sum_{0.0};
};

}  // namespace pulselog::metrics

#endif  // PULSELOG_METRICS_HISTOGRAM_H_
