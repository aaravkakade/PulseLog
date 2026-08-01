// Process-level resource usage, sampled by the metrics thread.
//
// CPU percentage is derived from the change in consumed CPU time between two
// samples, so the first call after construction reports 0 rather than an
// average since process start -- which would be misleading during a benchmark.
#ifndef PULSELOG_METRICS_PROCESS_STATS_H_
#define PULSELOG_METRICS_PROCESS_STATS_H_

#include <cstdint>
#include <string>

namespace pulselog::metrics {

struct ProcessSample {
  std::uint64_t resident_bytes = 0;
  std::uint64_t virtual_bytes = 0;
  double cpu_percent = 0.0;  // Percent of one core since the previous sample.
  std::uint64_t user_micros = 0;
  std::uint64_t system_micros = 0;
  std::int64_t open_files = -1;  // -1 when the platform does not expose it.
};

class ProcessStatsSampler {
 public:
  ProcessStatsSampler();

  // Samples current usage. Not thread-safe: one sampler per thread.
  [[nodiscard]] ProcessSample Sample();

  // Static snapshot of the host, recorded into benchmark result metadata.
  struct HostInfo {
    std::string os;
    std::string kernel;
    std::string architecture;
    std::string cpu_model;
    unsigned cpu_count = 0;
    std::uint64_t total_memory_bytes = 0;
  };

  [[nodiscard]] static HostInfo DescribeHost();

 private:
  std::uint64_t last_cpu_micros_ = 0;
  std::int64_t last_sample_nanos_ = 0;
};

}  // namespace pulselog::metrics

#endif  // PULSELOG_METRICS_PROCESS_STATS_H_
