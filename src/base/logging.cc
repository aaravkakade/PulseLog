#include "pulselog/base/logging.h"

#include <array>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <mutex>

namespace pulselog {

namespace logging_detail {

std::atomic<LogLevel> g_min_level{LogLevel::kInfo};

std::atomic<std::int32_t> g_broker_id{-1};

namespace {

// Output is serialised by a mutex. Log volume is intentionally low (start-up,
// topology changes, errors), so contention here is not on any measured path.
// The alternative -- a lock-free ring with a drain thread -- was not justified
// by any profile and would complicate crash-time flushing.
std::mutex& OutputMutex() {
  static std::mutex mutex;
  return mutex;
}

std::FILE*& OutputFile() {
  static std::FILE* file = stderr;
  return file;
}

void FormatTimestamp(std::array<char, 32>& out) {
  const auto now = std::chrono::system_clock::now();
  const auto secs = std::chrono::time_point_cast<std::chrono::seconds>(now);
  const auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(now - secs).count();
  const std::time_t tt = std::chrono::system_clock::to_time_t(secs);
  std::tm tm_buf{};
  (void)::gmtime_r(&tt, &tm_buf);
  // Return values discarded throughout this function: it formats a log line,
  // and there is nowhere useful to report a formatting failure to. A truncated
  // timestamp is strictly better than recursing into the logger.
  const std::size_t n = std::strftime(out.data(), out.size(), "%Y-%m-%dT%H:%M:%S", &tm_buf);
  (void)std::snprintf(out.data() + n, out.size() - n, ".%03dZ", static_cast<int>(millis));
}

}  // namespace

void Emit(LogLevel level, std::string_view component, std::string_view message) {
  std::array<char, 32> timestamp{};
  FormatTimestamp(timestamp);

  const std::int32_t broker = g_broker_id.load(std::memory_order_relaxed);
  const std::string_view level_name = LogLevelName(level);

  std::lock_guard<std::mutex> lock(OutputMutex());
  std::FILE* out = OutputFile();
  if (broker >= 0) {
    (void)std::fprintf(out,
                       "%s %-5.*s broker=%d [%.*s] %.*s\n",
                       timestamp.data(),
                       static_cast<int>(level_name.size()),
                       level_name.data(),
                       broker,
                       static_cast<int>(component.size()),
                       component.data(),
                       static_cast<int>(message.size()),
                       message.data());
  } else {
    (void)std::fprintf(out,
                       "%s %-5.*s [%.*s] %.*s\n",
                       timestamp.data(),
                       static_cast<int>(level_name.size()),
                       level_name.data(),
                       static_cast<int>(component.size()),
                       component.data(),
                       static_cast<int>(message.size()),
                       message.data());
  }
  // Errors are flushed immediately so a crash does not swallow the reason.
  if (level >= LogLevel::kError) (void)std::fflush(out);
}

}  // namespace logging_detail

std::string_view LogLevelName(LogLevel level) noexcept {
  switch (level) {
    case LogLevel::kTrace:
      return "TRACE";
    case LogLevel::kDebug:
      return "DEBUG";
    case LogLevel::kInfo:
      return "INFO";
    case LogLevel::kWarn:
      return "WARN";
    case LogLevel::kError:
      return "ERROR";
    case LogLevel::kOff:
      return "OFF";
  }
  return "?";
}

bool ParseLogLevel(std::string_view text, LogLevel& out) noexcept {
  if (text == "trace") {
    out = LogLevel::kTrace;
  } else if (text == "debug") {
    out = LogLevel::kDebug;
  } else if (text == "info") {
    out = LogLevel::kInfo;
  } else if (text == "warn" || text == "warning") {
    out = LogLevel::kWarn;
  } else if (text == "error") {
    out = LogLevel::kError;
  } else if (text == "off" || text == "none") {
    out = LogLevel::kOff;
  } else {
    return false;
  }
  return true;
}

void SetLogLevel(LogLevel level) noexcept {
  logging_detail::g_min_level.store(level, std::memory_order_relaxed);
}

LogLevel GetLogLevel() noexcept {
  return logging_detail::g_min_level.load(std::memory_order_relaxed);
}

void SetLogBrokerId(std::int32_t broker_id) noexcept {
  logging_detail::g_broker_id.store(broker_id, std::memory_order_relaxed);
}

bool SetLogFile(const std::string& path) {
  std::lock_guard<std::mutex> lock(logging_detail::OutputMutex());
  std::FILE*& out = logging_detail::OutputFile();
  if (path.empty()) {
    if (out != stderr && out != nullptr) (void)std::fclose(out);
    out = stderr;
    return true;
  }
  std::FILE* file = std::fopen(path.c_str(), "ae");
  if (file == nullptr) return false;
  if (out != stderr && out != nullptr) (void)std::fclose(out);
  out = file;
  return true;
}

void FlushLogs() {
  std::lock_guard<std::mutex> lock(logging_detail::OutputMutex());
  (void)std::fflush(logging_detail::OutputFile());
}

}  // namespace pulselog
