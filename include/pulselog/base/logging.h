// Structured logging.
//
// Every line is `key=value` pairs after a fixed prefix, which makes broker
// output greppable and machine-parseable without a JSON dependency:
//
//   2026-07-31T18:22:04.113Z INFO  broker=1 [storage] segment rolled
//       partition=orders-3 base_offset=918273 bytes=134217728
//
// Logging is not on the per-record path. Produce/fetch handling logs only on
// error or at debug level, which is compiled in but filtered by an atomic
// level check that costs a relaxed load.
#ifndef PULSELOG_BASE_LOGGING_H_
#define PULSELOG_BASE_LOGGING_H_

#include <atomic>
#include <cstdint>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>

namespace pulselog {

enum class LogLevel : std::uint8_t {
  kTrace = 0,
  kDebug = 1,
  kInfo = 2,
  kWarn = 3,
  kError = 4,
  kOff = 5,
};

[[nodiscard]] std::string_view LogLevelName(LogLevel level) noexcept;

[[nodiscard]] bool ParseLogLevel(std::string_view text, LogLevel& out) noexcept;

namespace logging_detail {

// Process-wide minimum level. Relaxed ordering is sufficient: a log line
// emitted slightly before or after a level change is harmless.
extern std::atomic<LogLevel> g_min_level;

extern std::atomic<std::int32_t> g_broker_id;

void Emit(LogLevel level, std::string_view component, std::string_view message);

}  // namespace logging_detail

void SetLogLevel(LogLevel level) noexcept;

[[nodiscard]] LogLevel GetLogLevel() noexcept;

// Tags every subsequent line with `broker=<id>`. Called once at start-up.
void SetLogBrokerId(std::int32_t broker_id) noexcept;

// Redirects output to a file. Empty path restores stderr. Returns false if the
// file cannot be opened.
bool SetLogFile(const std::string& path);

void FlushLogs();

// Builds one log line. Not intended for direct use -- go through the macros so
// that disabled levels cost only an atomic load.
class LogRecord {
 public:
  LogRecord(LogLevel level, std::string_view component) : level_(level), component_(component) {}

  LogRecord(const LogRecord&) = delete;
  LogRecord& operator=(const LogRecord&) = delete;

  ~LogRecord() { logging_detail::Emit(level_, component_, stream_.str()); }

  template <typename T>
  LogRecord& operator<<(const T& value) {
    stream_ << value;
    return *this;
  }

  // Appends ` key=value`.
  template <typename T>
  LogRecord& Field(std::string_view key, const T& value) {
    stream_ << ' ' << key << '=' << value;
    return *this;
  }

 private:
  LogLevel level_;
  std::string_view component_;
  std::ostringstream stream_;
};

}  // namespace pulselog

#define PL_LOG_ENABLED(level)                                                    \
  ((level) >= ::pulselog::logging_detail::g_min_level.load(std::memory_order_relaxed))

#define PL_LOG(level, component) \
  if (!PL_LOG_ENABLED(level)) {  \
  } else                         \
    ::pulselog::LogRecord(level, component)

#define PL_TRACE(component) PL_LOG(::pulselog::LogLevel::kTrace, component)
#define PL_DEBUG(component) PL_LOG(::pulselog::LogLevel::kDebug, component)
#define PL_INFO(component) PL_LOG(::pulselog::LogLevel::kInfo, component)
#define PL_WARN(component) PL_LOG(::pulselog::LogLevel::kWarn, component)
#define PL_ERROR(component) PL_LOG(::pulselog::LogLevel::kError, component)

#endif  // PULSELOG_BASE_LOGGING_H_
