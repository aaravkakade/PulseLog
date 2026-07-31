#include "pulselog/base/status.h"

#include <array>
#include <cstring>
#include <string>

namespace pulselog {

namespace {

constexpr std::string_view kUnknownName = "UNKNOWN_CODE";

}  // namespace

std::string_view ErrorCodeName(ErrorCode code) noexcept {
  switch (code) {
    case ErrorCode::kOk:
      return "OK";
    case ErrorCode::kUnknown:
      return "UNKNOWN";
    case ErrorCode::kInvalidArgument:
      return "INVALID_ARGUMENT";
    case ErrorCode::kNotFound:
      return "NOT_FOUND";
    case ErrorCode::kAlreadyExists:
      return "ALREADY_EXISTS";
    case ErrorCode::kOutOfRange:
      return "OUT_OF_RANGE";
    case ErrorCode::kCorruption:
      return "CORRUPTION";
    case ErrorCode::kIoError:
      return "IO_ERROR";
    case ErrorCode::kUnavailable:
      return "UNAVAILABLE";
    case ErrorCode::kTimeout:
      return "TIMEOUT";
    case ErrorCode::kBackpressure:
      return "BACKPRESSURE";
    case ErrorCode::kNotLeader:
      return "NOT_LEADER";
    case ErrorCode::kNotEnoughReplicas:
      return "NOT_ENOUGH_REPLICAS";
    case ErrorCode::kProtocolError:
      return "PROTOCOL_ERROR";
    case ErrorCode::kUnsupportedVersion:
      return "UNSUPPORTED_VERSION";
    case ErrorCode::kPermissionDenied:
      return "PERMISSION_DENIED";
    case ErrorCode::kResourceExhausted:
      return "RESOURCE_EXHAUSTED";
    case ErrorCode::kRebalanceInProgress:
      return "REBALANCE_IN_PROGRESS";
    case ErrorCode::kUnknownMember:
      return "UNKNOWN_MEMBER";
    case ErrorCode::kIllegalGeneration:
      return "ILLEGAL_GENERATION";
    case ErrorCode::kClosed:
      return "CLOSED";
    case ErrorCode::kWouldBlock:
      return "WOULD_BLOCK";
    case ErrorCode::kInternal:
      return "INTERNAL";
  }
  return kUnknownName;
}

bool IsRetryable(ErrorCode code) noexcept {
  switch (code) {
    // Transient conditions: the same request may succeed later, either on this
    // broker or after the client refreshes metadata.
    case ErrorCode::kUnavailable:
    case ErrorCode::kTimeout:
    case ErrorCode::kBackpressure:
    case ErrorCode::kNotLeader:
    case ErrorCode::kNotEnoughReplicas:
    case ErrorCode::kRebalanceInProgress:
    case ErrorCode::kWouldBlock:
      return true;
    // Everything else indicates a durable condition (bad request, corrupt
    // data, permanent configuration problem). Retrying just burns capacity.
    default:
      return false;
  }
}

std::string Status::ToString() const {
  if (ok()) return "OK";
  std::string out;
  out.reserve(message_.size() + 24);
  out.append(ErrorCodeName(code_));
  if (!message_.empty()) {
    out.append(": ");
    out.append(message_);
  }
  return out;
}

Status Status::WithContext(std::string_view context) const {
  if (ok()) return *this;
  std::string combined;
  combined.reserve(context.size() + message_.size() + 3);
  combined.append(context);
  combined.append(": ");
  combined.append(message_);
  return Status{code_, std::move(combined)};
}

Status ErrnoToStatus(std::string_view what, int err) {
  std::array<char, 256> buf{};
  const char* description = nullptr;
#if defined(__APPLE__) || (defined(_POSIX_C_SOURCE) && !defined(_GNU_SOURCE))
  // XSI-compliant strerror_r returns int.
  description = (::strerror_r(err, buf.data(), buf.size()) == 0) ? buf.data() : "unknown error";
#else
  description = ::strerror_r(err, buf.data(), buf.size());
#endif
  std::string message;
  message.reserve(what.size() + 48);
  message.append(what);
  message.append(": ");
  message.append(description);
  message.append(" (errno=");
  message.append(std::to_string(err));
  message.push_back(')');

  ErrorCode code = ErrorCode::kIoError;
  switch (err) {
    case ENOENT:
      code = ErrorCode::kNotFound;
      break;
    case EEXIST:
      code = ErrorCode::kAlreadyExists;
      break;
    case EACCES:
    case EPERM:
      code = ErrorCode::kPermissionDenied;
      break;
    case ENOSPC:
    case EDQUOT:
    case EMFILE:
    case ENFILE:
      code = ErrorCode::kResourceExhausted;
      break;
    case EAGAIN:
#if EWOULDBLOCK != EAGAIN
    case EWOULDBLOCK:
#endif
      code = ErrorCode::kWouldBlock;
      break;
    case ETIMEDOUT:
      code = ErrorCode::kTimeout;
      break;
    case EINVAL:
      code = ErrorCode::kInvalidArgument;
      break;
    case ECONNRESET:
    case EPIPE:
    case ENOTCONN:
      code = ErrorCode::kClosed;
      break;
    default:
      break;
  }
  return Status{code, std::move(message)};
}

}  // namespace pulselog
