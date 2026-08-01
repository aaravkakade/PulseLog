#include "pulselog/metrics/process_stats.h"

#include <sys/resource.h>
#include <sys/utsname.h>
#include <unistd.h>

#include <fstream>
#include <thread>

#include "pulselog/base/clock.h"

#if defined(__APPLE__)
#include <mach/mach.h>
#include <sys/sysctl.h>
#elif defined(__linux__)
#include <sys/sysinfo.h>
#endif

namespace pulselog::metrics {
namespace {

std::uint64_t ReadResidentBytes() {
#if defined(__APPLE__)
  ::mach_task_basic_info info{};
  mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
  if (::task_info(::mach_task_self(), MACH_TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&info),
                  &count) == KERN_SUCCESS) {
    return info.resident_size;
  }
  return 0;
#elif defined(__linux__)
  // statm reports pages: size, resident, shared, text, lib, data, dirty.
  std::ifstream statm("/proc/self/statm");
  std::uint64_t total_pages = 0;
  std::uint64_t resident_pages = 0;
  if (statm >> total_pages >> resident_pages) {
    return resident_pages * static_cast<std::uint64_t>(::sysconf(_SC_PAGESIZE));
  }
  return 0;
#else
  return 0;
#endif
}

std::uint64_t ReadVirtualBytes() {
#if defined(__APPLE__)
  ::mach_task_basic_info info{};
  mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
  if (::task_info(::mach_task_self(), MACH_TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&info),
                  &count) == KERN_SUCCESS) {
    return info.virtual_size;
  }
  return 0;
#elif defined(__linux__)
  std::ifstream statm("/proc/self/statm");
  std::uint64_t total_pages = 0;
  if (statm >> total_pages) {
    return total_pages * static_cast<std::uint64_t>(::sysconf(_SC_PAGESIZE));
  }
  return 0;
#else
  return 0;
#endif
}

}  // namespace

ProcessStatsSampler::ProcessStatsSampler() {
  ::rusage usage{};
  if (::getrusage(RUSAGE_SELF, &usage) == 0) {
    last_cpu_micros_ =
        static_cast<std::uint64_t>(usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) * 1'000'000 +
        static_cast<std::uint64_t>(usage.ru_utime.tv_usec + usage.ru_stime.tv_usec);
  }
  last_sample_nanos_ = MonotonicNanos();
}

ProcessSample ProcessStatsSampler::Sample() {
  ProcessSample sample;
  sample.resident_bytes = ReadResidentBytes();
  sample.virtual_bytes = ReadVirtualBytes();

  ::rusage usage{};
  if (::getrusage(RUSAGE_SELF, &usage) != 0) return sample;

  sample.user_micros = static_cast<std::uint64_t>(usage.ru_utime.tv_sec) * 1'000'000 +
                       static_cast<std::uint64_t>(usage.ru_utime.tv_usec);
  sample.system_micros = static_cast<std::uint64_t>(usage.ru_stime.tv_sec) * 1'000'000 +
                         static_cast<std::uint64_t>(usage.ru_stime.tv_usec);

  const std::uint64_t cpu_micros = sample.user_micros + sample.system_micros;
  const std::int64_t now = MonotonicNanos();
  const std::int64_t elapsed_nanos = now - last_sample_nanos_;
  if (elapsed_nanos > 0 && cpu_micros >= last_cpu_micros_) {
    const double cpu_nanos = static_cast<double>(cpu_micros - last_cpu_micros_) * 1000.0;
    sample.cpu_percent = 100.0 * cpu_nanos / static_cast<double>(elapsed_nanos);
  }
  last_cpu_micros_ = cpu_micros;
  last_sample_nanos_ = now;
  return sample;
}

ProcessStatsSampler::HostInfo ProcessStatsSampler::DescribeHost() {
  HostInfo info;
  ::utsname uts{};
  if (::uname(&uts) == 0) {
    info.os = uts.sysname;
    info.kernel = uts.release;
    info.architecture = uts.machine;
  }
  info.cpu_count = std::thread::hardware_concurrency();

#if defined(__APPLE__)
  {
    std::array<char, 256> brand{};
    std::size_t size = brand.size();
    if (::sysctlbyname("machdep.cpu.brand_string", brand.data(), &size, nullptr, 0) == 0) {
      info.cpu_model.assign(brand.data());
    }
    std::uint64_t memory = 0;
    size = sizeof(memory);
    if (::sysctlbyname("hw.memsize", &memory, &size, nullptr, 0) == 0) {
      info.total_memory_bytes = memory;
    }
  }
#elif defined(__linux__)
  {
    std::ifstream cpuinfo("/proc/cpuinfo");
    std::string line;
    while (std::getline(cpuinfo, line)) {
      if (line.rfind("model name", 0) == 0) {
        const auto colon = line.find(':');
        if (colon != std::string::npos) {
          info.cpu_model = line.substr(colon + 2);
        }
        break;
      }
    }
    struct ::sysinfo sys {};
    if (::sysinfo(&sys) == 0) {
      info.total_memory_bytes = static_cast<std::uint64_t>(sys.totalram) * sys.mem_unit;
    }
  }
#endif

  if (info.cpu_model.empty()) info.cpu_model = "unknown";
  return info;
}

}  // namespace pulselog::metrics
