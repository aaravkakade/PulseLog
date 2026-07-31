#include "pulselog/base/types.h"

#include <algorithm>
#include <cctype>

namespace pulselog {

std::string_view AckModeName(AckMode mode) noexcept {
  switch (mode) {
    case AckMode::kNone:
      return "none";
    case AckMode::kLeader:
      return "leader";
    case AckMode::kQuorum:
      return "quorum";
  }
  return "unknown";
}

bool ParseAckMode(std::string_view text, AckMode& out) noexcept {
  if (text == "none" || text == "0") {
    out = AckMode::kNone;
    return true;
  }
  if (text == "leader" || text == "1") {
    out = AckMode::kLeader;
    return true;
  }
  if (text == "quorum" || text == "all" || text == "-1") {
    out = AckMode::kQuorum;
    return true;
  }
  return false;
}

std::string_view CompressionName(Compression c) noexcept {
  switch (c) {
    case Compression::kNone:
      return "none";
    case Compression::kLz4Like:
      return "lz4like";
  }
  return "unknown";
}

bool ParseCompression(std::string_view text, Compression& out) noexcept {
  if (text == "none" || text == "0") {
    out = Compression::kNone;
    return true;
  }
  if (text == "lz4like" || text == "lz4" || text == "1") {
    out = Compression::kLz4Like;
    return true;
  }
  return false;
}

bool IsValidTopicName(std::string_view name) noexcept {
  if (name.empty() || name.size() > kMaxTopicNameLength) return false;
  // Reserved by POSIX path semantics; a topic name becomes a directory name.
  if (name == "." || name == "..") return false;
  return std::all_of(name.begin(), name.end(), [](char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '.' ||
           c == '_' || c == '-';
  });
}

}  // namespace pulselog
