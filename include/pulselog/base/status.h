// Error handling for PulseLog.
//
// The engine does not use exceptions on any hot path. Fallible operations
// return `Status` (no value) or `Result<T>` (value or error). Exceptions are
// reserved for genuinely unrecoverable programmer errors during start-up
// (bad configuration, failure to allocate) where unwinding to `main` and
// exiting is the correct response.
#ifndef PULSELOG_BASE_STATUS_H_
#define PULSELOG_BASE_STATUS_H_

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

namespace pulselog {

// Stable numeric error codes. These are part of the wire protocol: a broker
// echoes them back to clients in response frames, so values must never be
// reused or renumbered once released. Append new codes at the end.
enum class ErrorCode : std::uint16_t {
  kOk = 0,
  kUnknown = 1,
  kInvalidArgument = 2,
  kNotFound = 3,
  kAlreadyExists = 4,
  kOutOfRange = 5,          // Offset outside [log_start, log_end).
  kCorruption = 6,          // Checksum mismatch or malformed on-disk record.
  kIoError = 7,             // read/write/fsync failed.
  kUnavailable = 8,         // Broker shutting down or partition not ready.
  kTimeout = 9,             // Deadline exceeded.
  kBackpressure = 10,       // Bounded queue full; client should retry later.
  kNotLeader = 11,          // Request routed to a non-leader replica.
  kNotEnoughReplicas = 12,  // Quorum ack impossible with current ISR.
  kProtocolError = 13,      // Malformed frame, bad magic, unsupported version.
  kUnsupportedVersion = 14,
  kPermissionDenied = 15,
  kResourceExhausted = 16,  // Disk full, memory budget exceeded.
  kRebalanceInProgress = 17,
  kUnknownMember = 18,   // Consumer-group member ID not recognised.
  kIllegalGeneration = 19,
  kClosed = 20,          // Connection or component already closed.
  kWouldBlock = 21,      // Non-blocking operation cannot proceed yet.
  kInternal = 22,
};

// Human-readable, stable name for a code. Used in logs and metrics labels.
[[nodiscard]] std::string_view ErrorCodeName(ErrorCode code) noexcept;

// True when a client can reasonably retry the same request unchanged.
[[nodiscard]] bool IsRetryable(ErrorCode code) noexcept;

// A lightweight error carrier. `Status::Ok()` allocates nothing.
class Status {
 public:
  Status() noexcept = default;

  static Status Ok() noexcept { return Status{}; }

  Status(ErrorCode code, std::string message) : code_(code), message_(std::move(message)) {}

  explicit Status(ErrorCode code) : code_(code), message_(ErrorCodeName(code)) {}

  [[nodiscard]] bool ok() const noexcept { return code_ == ErrorCode::kOk; }

  [[nodiscard]] ErrorCode code() const noexcept { return code_; }

  [[nodiscard]] const std::string& message() const noexcept { return message_; }

  [[nodiscard]] std::string ToString() const;

  // Adds calling context while preserving the original code, e.g.
  //   return st.WithContext("append to segment 42");
  [[nodiscard]] Status WithContext(std::string_view context) const;

  friend bool operator==(const Status& lhs, const Status& rhs) noexcept {
    return lhs.code_ == rhs.code_;
  }

 private:
  ErrorCode code_ = ErrorCode::kOk;
  std::string message_;
};

// Convenience constructors keep call sites short.
inline Status OkStatus() noexcept { return Status::Ok(); }

inline Status InvalidArgument(std::string msg) {
  return Status{ErrorCode::kInvalidArgument, std::move(msg)};
}

inline Status NotFound(std::string msg) { return Status{ErrorCode::kNotFound, std::move(msg)}; }

inline Status AlreadyExists(std::string msg) {
  return Status{ErrorCode::kAlreadyExists, std::move(msg)};
}

inline Status OutOfRange(std::string msg) { return Status{ErrorCode::kOutOfRange, std::move(msg)}; }

inline Status Corruption(std::string msg) { return Status{ErrorCode::kCorruption, std::move(msg)}; }

inline Status IoError(std::string msg) { return Status{ErrorCode::kIoError, std::move(msg)}; }

inline Status Unavailable(std::string msg) {
  return Status{ErrorCode::kUnavailable, std::move(msg)};
}

inline Status TimedOut(std::string msg) { return Status{ErrorCode::kTimeout, std::move(msg)}; }

inline Status ProtocolError(std::string msg) {
  return Status{ErrorCode::kProtocolError, std::move(msg)};
}

inline Status Internal(std::string msg) { return Status{ErrorCode::kInternal, std::move(msg)}; }

inline Status ResourceExhausted(std::string msg) {
  return Status{ErrorCode::kResourceExhausted, std::move(msg)};
}

// Builds an IoError carrying the current errno and a strerror description.
[[nodiscard]] Status ErrnoToStatus(std::string_view what, int err);

// `Result<T>` holds either a value or a non-OK Status. It deliberately has no
// implicit conversion to bool to force explicit `.ok()` checks at call sites.
template <typename T>
class [[nodiscard]] Result {
 public:
  using ValueType = T;

  Result(T value) : value_(std::move(value)) {}  // NOLINT(google-explicit-constructor)

  Result(Status status)  // NOLINT(google-explicit-constructor)
      : status_(std::move(status)) {
    // A Result must never carry an OK status without a value.
    if (status_.ok()) {
      status_ = Status{ErrorCode::kInternal, "Result constructed from OK status without a value"};
    }
  }

  [[nodiscard]] bool ok() const noexcept { return value_.has_value(); }

  [[nodiscard]] const Status& status() const noexcept { return status_; }

  [[nodiscard]] ErrorCode code() const noexcept {
    return value_.has_value() ? ErrorCode::kOk : status_.code();
  }

  // Precondition: ok(). Calling these on an error Result is a programmer bug;
  // debug builds trap via the optional's own checks.
  [[nodiscard]] T& value() & { return *value_; }

  [[nodiscard]] const T& value() const& { return *value_; }

  [[nodiscard]] T&& value() && { return std::move(*value_); }

  [[nodiscard]] T value_or(T fallback) const& {
    return value_.has_value() ? *value_ : std::move(fallback);
  }

  T& operator*() & { return *value_; }

  const T& operator*() const& { return *value_; }

  T* operator->() { return &*value_; }

  const T* operator->() const { return &*value_; }

 private:
  std::optional<T> value_;
  Status status_;
};

}  // namespace pulselog

// Propagates an error Status out of the current function.
#define PL_RETURN_IF_ERROR(expr)                     \
  do {                                               \
    ::pulselog::Status pl_status_tmp_ = (expr);      \
    if (!pl_status_tmp_.ok()) return pl_status_tmp_; \
  } while (false)

// Propagates the error from a Result<T> out of a Status- or Result-returning
// function. `target` is a declaration, e.g. PL_ASSIGN_OR_RETURN(auto n, Read());
#define PL_ASSIGN_OR_RETURN_IMPL(tmp, target, expr) \
  auto tmp = (expr);                                \
  if (!tmp.ok()) return tmp.status();               \
  target = std::move(tmp).value()

#define PL_CONCAT_INNER(a, b) a##b
#define PL_CONCAT(a, b) PL_CONCAT_INNER(a, b)

#define PL_ASSIGN_OR_RETURN(target, expr) \
  PL_ASSIGN_OR_RETURN_IMPL(PL_CONCAT(pl_result_tmp_, __LINE__), target, expr)

#endif  // PULSELOG_BASE_STATUS_H_
