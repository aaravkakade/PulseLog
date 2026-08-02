#include "pulselog/base/config.h"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstdlib>
#include <fstream>
#include <sstream>

extern char** environ;

namespace pulselog {
namespace {

std::string_view Trim(std::string_view s) {
  const auto not_space = [](unsigned char c) { return std::isspace(c) == 0; };
  auto begin = std::find_if(s.begin(), s.end(), not_space);
  auto end = std::find_if(s.rbegin(), s.rend(), not_space).base();
  if (begin >= end) return {};
  return s.substr(static_cast<std::size_t>(begin - s.begin()),
                  static_cast<std::size_t>(end - begin));
}

std::string ToLower(std::string_view s) {
  std::string out(s);
  std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) {
    return static_cast<char>(std::tolower(c));
  });
  return out;
}

Result<std::int64_t> ParseInteger(std::string_view key, std::string_view text) {
  std::int64_t value = 0;
  const auto* first = text.data();
  const auto* last = text.data() + text.size();
  auto [ptr, ec] = std::from_chars(first, last, value);
  if (ec != std::errc{} || ptr != last) {
    return InvalidArgument("config key '" + std::string(key) + "': not an integer: '" +
                           std::string(text) + "'");
  }
  return value;
}

}  // namespace

Status ConfigStore::LoadFile(const std::string& path) {
  std::ifstream file(path);
  if (!file.is_open()) {
    return NotFound("config file not found: " + path);
  }

  std::string line;
  std::string section;
  int line_number = 0;
  while (std::getline(file, line)) {
    ++line_number;
    std::string_view view = Trim(line);
    if (view.empty() || view.front() == '#' || view.front() == ';') continue;

    if (view.front() == '[') {
      const auto close = view.find(']');
      if (close == std::string_view::npos) {
        return InvalidArgument(path + ":" + std::to_string(line_number) + ": unterminated section");
      }
      section = std::string(Trim(view.substr(1, close - 1)));
      if (!section.empty()) section.push_back('.');
      continue;
    }

    const auto eq = view.find('=');
    if (eq == std::string_view::npos) {
      return InvalidArgument(path + ":" + std::to_string(line_number) +
                             ": expected key = value, got '" + std::string(view) + "'");
    }
    std::string key = section + ToLower(Trim(view.substr(0, eq)));
    std::string value(Trim(view.substr(eq + 1)));
    // Strip optional surrounding quotes so values may contain spaces.
    if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
      value = value.substr(1, value.size() - 2);
    }
    entries_[std::move(key)] = std::move(value);
  }
  return OkStatus();
}

void ConfigStore::LoadEnvironment() {
  static constexpr std::string_view kPrefix = "PULSELOG_";
  for (char** env = environ; env != nullptr && *env != nullptr; ++env) {
    std::string_view entry(*env);
    if (entry.substr(0, kPrefix.size()) != kPrefix) continue;
    const auto eq = entry.find('=');
    if (eq == std::string_view::npos) continue;

    std::string_view raw_key = entry.substr(kPrefix.size(), eq - kPrefix.size());
    std::string key;
    key.reserve(raw_key.size());
    for (std::size_t i = 0; i < raw_key.size(); ++i) {
      if (raw_key[i] == '_') {
        if (i + 1 < raw_key.size() && raw_key[i + 1] == '_') {
          key.push_back('_');
          ++i;
        } else {
          key.push_back('.');
        }
      } else {
        key.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(raw_key[i]))));
      }
    }
    entries_[std::move(key)] = std::string(entry.substr(eq + 1));
  }
}

std::vector<std::string> ConfigStore::LoadCommandLine(int argc, const char* const argv[]) {
  std::vector<std::string> positional;
  for (int i = 1; i < argc; ++i) {
    std::string_view arg(argv[i]);
    if (arg.size() < 3 || arg.substr(0, 2) != "--") {
      positional.emplace_back(arg);
      continue;
    }
    arg.remove_prefix(2);
    const auto eq = arg.find('=');
    if (eq != std::string_view::npos) {
      entries_[ToLower(arg.substr(0, eq))] = std::string(arg.substr(eq + 1));
    } else {
      // Bare `--flag` means true. The `--key value` form is deliberately not
      // accepted: with an untyped store there is no way to tell a boolean flag
      // followed by a positional argument from a key/value pair, and guessing
      // silently swallows subcommands.
      entries_[ToLower(arg)] = "true";
    }
  }
  return positional;
}

void ConfigStore::Set(std::string key, std::string value) {
  entries_[ToLower(key)] = std::move(value);
}

bool ConfigStore::Contains(std::string_view key) const {
  return entries_.find(key) != entries_.end();
}

std::string ConfigStore::GetString(std::string_view key, std::string_view fallback) const {
  const auto it = entries_.find(key);
  return it == entries_.end() ? std::string(fallback) : it->second;
}

Result<std::string> ConfigStore::RequireString(std::string_view key) const {
  const auto it = entries_.find(key);
  if (it == entries_.end()) {
    return InvalidArgument("missing required config key: " + std::string(key));
  }
  return it->second;
}

Result<std::int64_t> ConfigStore::GetInt(std::string_view key, std::int64_t fallback) const {
  const auto it = entries_.find(key);
  if (it == entries_.end()) return fallback;
  return ParseInteger(key, it->second);
}

Result<double> ConfigStore::GetDouble(std::string_view key, double fallback) const {
  const auto it = entries_.find(key);
  if (it == entries_.end()) return fallback;
  // std::from_chars for double is not available in every libc++ shipped with
  // Apple clang 15, so strtod is used with explicit end-pointer validation.
  const std::string& text = it->second;
  char* end = nullptr;
  const double value = std::strtod(text.c_str(), &end);
  if (end == text.c_str() || *end != '\0') {
    return InvalidArgument("config key '" + std::string(key) + "': not a number: '" + text + "'");
  }
  return value;
}

Result<bool> ConfigStore::GetBool(std::string_view key, bool fallback) const {
  const auto it = entries_.find(key);
  if (it == entries_.end()) return fallback;
  const std::string value = ToLower(it->second);
  if (value == "true" || value == "1" || value == "yes" || value == "on") return true;
  if (value == "false" || value == "0" || value == "no" || value == "off") return false;
  return InvalidArgument("config key '" + std::string(key) + "': not a boolean: '" + it->second +
                         "'");
}

Result<std::int64_t> ConfigStore::GetBytes(std::string_view key, std::int64_t fallback) const {
  const auto it = entries_.find(key);
  if (it == entries_.end()) return fallback;

  std::string_view text = Trim(it->second);
  std::int64_t multiplier = 1;
  auto strip = [&text](std::size_t n) { text.remove_suffix(n); };
  const std::string lower = ToLower(text);
  if (lower.ends_with("kb") || lower.ends_with("kib")) {
    multiplier = 1024;
    strip(lower.ends_with("kib") ? 3 : 2);
  } else if (lower.ends_with("mb") || lower.ends_with("mib")) {
    multiplier = 1024LL * 1024;
    strip(lower.ends_with("mib") ? 3 : 2);
  } else if (lower.ends_with("gb") || lower.ends_with("gib")) {
    multiplier = 1024LL * 1024 * 1024;
    strip(lower.ends_with("gib") ? 3 : 2);
  } else if (lower.ends_with("k")) {
    multiplier = 1024;
    strip(1);
  } else if (lower.ends_with("m")) {
    multiplier = 1024LL * 1024;
    strip(1);
  } else if (lower.ends_with("g")) {
    multiplier = 1024LL * 1024 * 1024;
    strip(1);
  } else if (lower.ends_with("b")) {
    strip(1);
  }

  auto parsed = ParseInteger(key, Trim(text));
  if (!parsed.ok()) return parsed.status();
  return parsed.value() * multiplier;
}

Result<std::int64_t> ConfigStore::GetDurationMs(std::string_view key, std::int64_t fallback) const {
  const auto it = entries_.find(key);
  if (it == entries_.end()) return fallback;

  std::string_view text = Trim(it->second);
  const std::string lower = ToLower(text);
  std::int64_t multiplier = 1;
  if (lower.ends_with("ms")) {
    text.remove_suffix(2);
  } else if (lower.ends_with("us")) {
    // Sub-millisecond durations round down to 0 ms; callers that need
    // microsecond precision use dedicated keys.
    text.remove_suffix(2);
    auto parsed = ParseInteger(key, Trim(text));
    if (!parsed.ok()) return parsed.status();
    return parsed.value() / 1000;
  } else if (lower.ends_with("s")) {
    multiplier = 1000;
    text.remove_suffix(1);
  } else if (lower.ends_with("m")) {
    multiplier = 60'000;
    text.remove_suffix(1);
  } else if (lower.ends_with("h")) {
    multiplier = 3'600'000;
    text.remove_suffix(1);
  }

  auto parsed = ParseInteger(key, Trim(text));
  if (!parsed.ok()) return parsed.status();
  return parsed.value() * multiplier;
}

std::vector<std::string> ConfigStore::GetList(std::string_view key) const {
  std::vector<std::string> out;
  const auto it = entries_.find(key);
  if (it == entries_.end()) return out;

  std::string_view rest = it->second;
  while (!rest.empty()) {
    const auto comma = rest.find(',');
    std::string_view item = comma == std::string_view::npos ? rest : rest.substr(0, comma);
    item = Trim(item);
    if (!item.empty()) out.emplace_back(item);
    if (comma == std::string_view::npos) break;
    rest.remove_prefix(comma + 1);
  }
  return out;
}

std::string ConfigStore::Dump() const {
  std::ostringstream out;
  for (const auto& [key, value] : entries_) {
    out << key << '=' << value << '\n';
  }
  return out.str();
}

}  // namespace pulselog
