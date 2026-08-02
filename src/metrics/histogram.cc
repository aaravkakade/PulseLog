#include "pulselog/metrics/histogram.h"

#include <algorithm>
#include <bit>
#include <cmath>
#include <sstream>

#include "pulselog/base/types.h"

namespace pulselog::metrics {
namespace {

// Number of value bits needed to hold `significant_digits` decimal digits,
// plus one so the sub-bucket array can be halved (the standard HdrHistogram
// trick: the low half of every bucket above the first duplicates the previous
// bucket's high half, so only half is stored).
std::int32_t PrecisionBits(int significant_digits) {
  const auto largest = static_cast<std::int64_t>(std::pow(10.0, significant_digits));
  std::int32_t bits = 0;
  while ((std::int64_t{1} << bits) < largest * 2) ++bits;
  return bits;
}

}  // namespace

Histogram::Histogram(std::int64_t max_trackable, int significant_digits)
    : max_trackable_(max_trackable < 2 ? 2 : max_trackable),
      significant_digits_(std::clamp(significant_digits, 1, 5)) {
  const std::int32_t precision_bits = PrecisionBits(significant_digits_);
  sub_bucket_half_count_magnitude_ = precision_bits - 1;
  sub_bucket_count_ = std::int32_t{1} << precision_bits;
  sub_bucket_half_count_ = sub_bucket_count_ / 2;
  sub_bucket_mask_ = static_cast<std::int64_t>(sub_bucket_count_) - 1;

  // How many power-of-two buckets are needed to reach max_trackable.
  auto smallest_untrackable = static_cast<std::int64_t>(sub_bucket_count_);
  bucket_count_ = 1;
  while (smallest_untrackable <= max_trackable_) {
    if (smallest_untrackable > INT64_MAX / 2) break;
    smallest_untrackable <<= 1;
    ++bucket_count_;
  }

  leading_zero_count_base_ = 64 - static_cast<std::int32_t>(precision_bits);

  // Widen before multiplying, not after: casting the product would only
  // convert a value that had already overflowed in int32.
  counts_len_ = static_cast<std::size_t>(bucket_count_ + 1) *
                static_cast<std::size_t>(sub_bucket_half_count_);
  counts_ = std::make_unique<std::atomic<std::uint64_t>[]>(counts_len_);
  for (std::size_t i = 0; i < counts_len_; ++i) {
    counts_[i].store(0, std::memory_order_relaxed);
  }
}

std::size_t Histogram::IndexFor(std::int64_t value) const noexcept {
  auto masked = static_cast<std::uint64_t>(value | sub_bucket_mask_);
  const std::int32_t bucket_index =
      leading_zero_count_base_ - ::pulselog::Narrow<std::int32_t>(std::countl_zero(masked));
  const auto sub_bucket_index = static_cast<std::int32_t>(value >> bucket_index);

  // The low half of every bucket above zero repeats the previous bucket, so
  // only the high half is stored; subtracting sub_bucket_half_count_ lands in
  // the right slot for both cases.
  const std::int32_t bucket_base_index = (bucket_index + 1) << sub_bucket_half_count_magnitude_;
  const std::int32_t offset_in_bucket = sub_bucket_index - sub_bucket_half_count_;
  // The sum is computed in int32 deliberately -- both terms are bounded by the
  // bucket layout -- and widened once, after the add.
  const auto index =
      static_cast<std::size_t>(bucket_base_index) + static_cast<std::size_t>(offset_in_bucket);
  return index < counts_len_ ? index : counts_len_ - 1;
}

std::int64_t Histogram::ValueForIndex(std::size_t index) const noexcept {
  auto bucket_index = static_cast<std::int32_t>(
                          index >> static_cast<std::size_t>(sub_bucket_half_count_magnitude_)) -
                      1;
  auto sub_bucket_index =
      static_cast<std::int32_t>(index & static_cast<std::size_t>(sub_bucket_half_count_ - 1)) +
      sub_bucket_half_count_;
  if (bucket_index < 0) {
    sub_bucket_index -= sub_bucket_half_count_;
    bucket_index = 0;
  }
  return static_cast<std::int64_t>(sub_bucket_index) << bucket_index;
}

void Histogram::Record(std::int64_t value) noexcept {
  RecordMany(value, 1);
}

void Histogram::RecordMany(std::int64_t value, std::uint64_t count) noexcept {
  if (count == 0) return;
  if (value < 0) value = 0;
  if (value > max_trackable_) {
    overflow_count_.fetch_add(count, std::memory_order_relaxed);
    value = max_trackable_;
  }

  counts_[IndexFor(value)].fetch_add(count, std::memory_order_relaxed);
  total_count_.fetch_add(count, std::memory_order_relaxed);

  std::int64_t observed = min_.load(std::memory_order_relaxed);
  while (value < observed &&
         !min_.compare_exchange_weak(observed, value, std::memory_order_relaxed)) {
  }
  observed = max_.load(std::memory_order_relaxed);
  while (value > observed &&
         !max_.compare_exchange_weak(observed, value, std::memory_order_relaxed)) {
  }

  // A relaxed CAS loop on a double: the sum is only used for the mean, and
  // losing precision under contention is preferable to a lock here.
  const double delta = static_cast<double>(value) * static_cast<double>(count);
  double current = sum_.load(std::memory_order_relaxed);
  while (!sum_.compare_exchange_weak(current, current + delta, std::memory_order_relaxed)) {
  }
}

std::int64_t Histogram::ValueAtPercentile(double percentile) const noexcept {
  const std::uint64_t total = total_count_.load(std::memory_order_relaxed);
  if (total == 0) return 0;

  const double clamped = std::clamp(percentile, 0.0, 100.0);
  // Round up so p99 of 100 samples is the 99th, not the 98th. llround rather
  // than truncating (x + 0.5): the manual form rounds the wrong way for values
  // that are already exactly representable, and is undefined once the sum
  // exceeds the mantissa's exact-integer range.
  auto target =
      static_cast<std::uint64_t>(std::llround((clamped / 100.0) * static_cast<double>(total)));
  if (target == 0) target = 1;
  if (target > total) target = total;

  std::uint64_t running = 0;
  for (std::size_t i = 0; i < counts_len_; ++i) {
    running += counts_[i].load(std::memory_order_relaxed);
    if (running >= target) {
      // Report the highest value that maps to this slot. Reporting the bottom
      // would understate the latency, which is the one direction a latency
      // report must never err in.
      const auto bucket_index = static_cast<std::int32_t>(
          i >> static_cast<std::size_t>(sub_bucket_half_count_magnitude_));
      const std::int32_t exponent = bucket_index > 0 ? bucket_index - 1 : 0;
      const std::int64_t slot_width = std::int64_t{1} << exponent;
      return ValueForIndex(i) + slot_width - 1;
    }
  }
  return max_.load(std::memory_order_relaxed);
}

std::int64_t Histogram::Min() const noexcept {
  const std::int64_t value = min_.load(std::memory_order_relaxed);
  return value == INT64_MAX ? 0 : value;
}

std::int64_t Histogram::Max() const noexcept {
  return max_.load(std::memory_order_relaxed);
}

double Histogram::Mean() const noexcept {
  const std::uint64_t total = total_count_.load(std::memory_order_relaxed);
  if (total == 0) return 0.0;
  return sum_.load(std::memory_order_relaxed) / static_cast<double>(total);
}

HistogramSnapshot Histogram::GetSnapshot() const noexcept {
  HistogramSnapshot snapshot;
  snapshot.count = Count();
  if (snapshot.count == 0) return snapshot;
  snapshot.min = Min();
  snapshot.max = Max();
  snapshot.p50 = ValueAtPercentile(50.0);
  snapshot.p90 = ValueAtPercentile(90.0);
  snapshot.p95 = ValueAtPercentile(95.0);
  snapshot.p99 = ValueAtPercentile(99.0);
  snapshot.p999 = ValueAtPercentile(99.9);
  snapshot.mean = Mean();
  snapshot.sum = sum_.load(std::memory_order_relaxed);
  return snapshot;
}

void Histogram::Reset() noexcept {
  for (std::size_t i = 0; i < counts_len_; ++i) {
    counts_[i].store(0, std::memory_order_relaxed);
  }
  total_count_.store(0, std::memory_order_relaxed);
  overflow_count_.store(0, std::memory_order_relaxed);
  min_.store(INT64_MAX, std::memory_order_relaxed);
  max_.store(0, std::memory_order_relaxed);
  sum_.store(0.0, std::memory_order_relaxed);
}

void Histogram::Add(const Histogram& other) noexcept {
  if (other.counts_len_ != counts_len_) return;
  for (std::size_t i = 0; i < counts_len_; ++i) {
    const std::uint64_t count = other.counts_[i].load(std::memory_order_relaxed);
    if (count > 0) counts_[i].fetch_add(count, std::memory_order_relaxed);
  }
  total_count_.fetch_add(other.Count(), std::memory_order_relaxed);
  overflow_count_.fetch_add(other.OverflowCount(), std::memory_order_relaxed);

  std::int64_t observed = min_.load(std::memory_order_relaxed);
  const std::int64_t other_min = other.Min();
  while (other_min < observed &&
         !min_.compare_exchange_weak(observed, other_min, std::memory_order_relaxed)) {
  }
  observed = max_.load(std::memory_order_relaxed);
  const std::int64_t other_max = other.Max();
  while (other_max > observed &&
         !max_.compare_exchange_weak(observed, other_max, std::memory_order_relaxed)) {
  }

  const double delta = other.sum_.load(std::memory_order_relaxed);
  double current = sum_.load(std::memory_order_relaxed);
  while (!sum_.compare_exchange_weak(current, current + delta, std::memory_order_relaxed)) {
  }
}

std::vector<Histogram::Bucket> Histogram::NonEmptyBuckets() const {
  std::vector<Bucket> buckets;
  for (std::size_t i = 0; i < counts_len_; ++i) {
    const std::uint64_t count = counts_[i].load(std::memory_order_relaxed);
    if (count == 0) continue;
    buckets.push_back(Bucket{ValueForIndex(i), count});
  }
  return buckets;
}

std::string HistogramSnapshot::ToString() const {
  std::ostringstream out;
  out << "count=" << count << " min=" << min << " p50=" << p50 << " p95=" << p95 << " p99=" << p99
      << " p99.9=" << p999 << " max=" << max << " mean=" << static_cast<std::int64_t>(mean);
  return out.str();
}

}  // namespace pulselog::metrics
