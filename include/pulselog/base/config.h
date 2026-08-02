// Configuration store.
//
// Values come from three sources, in increasing order of precedence:
//   1. a properties file (`key = value`, `#` comments, `[section]` headers
//      that prefix subsequent keys with `section.`),
//   2. environment variables of the form PULSELOG_<KEY_WITH_UNDERSCORES>,
//   3. command-line flags `--key=value`.
//
// Typed getters validate and report the offending key, so a broker refuses to
// start on a typo instead of silently running with a default. The store also
// records which keys were actually read, so a caller can refuse to start on a
// key nobody consumes -- see UnreadKeys().
#ifndef PULSELOG_BASE_CONFIG_H_
#define PULSELOG_BASE_CONFIG_H_

#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "pulselog/base/status.h"

namespace pulselog {

class ConfigStore {
 public:
  ConfigStore() = default;

  // Parses a properties file. Returns kNotFound if the file does not exist.
  Status LoadFile(const std::string& path);

  // Reads PULSELOG_* environment variables. `FOO_BAR` maps to key `foo.bar`
  // (single underscores become dots, double underscores become one underscore).
  void LoadEnvironment();

  // Consumes `--key=value` arguments; a bare `--key` sets the key to "true".
  // The `--key value` form is not supported (see config.cc for why).
  // Everything else is returned as a positional argument so callers can
  // implement subcommands.
  std::vector<std::string> LoadCommandLine(int argc, const char* const argv[]);

  void Set(std::string key, std::string value);

  [[nodiscard]] bool Contains(std::string_view key) const;

  [[nodiscard]] std::string GetString(std::string_view key, std::string_view fallback) const;

  [[nodiscard]] Result<std::string> RequireString(std::string_view key) const;

  [[nodiscard]] Result<std::int64_t> GetInt(std::string_view key, std::int64_t fallback) const;

  [[nodiscard]] Result<double> GetDouble(std::string_view key, double fallback) const;

  [[nodiscard]] Result<bool> GetBool(std::string_view key, bool fallback) const;

  // Parses sizes with optional K/M/G suffixes ("64MB", "1G", "4096").
  [[nodiscard]] Result<std::int64_t> GetBytes(std::string_view key, std::int64_t fallback) const;

  // Parses durations with optional ms/s/m/h suffixes; result is milliseconds.
  [[nodiscard]] Result<std::int64_t> GetDurationMs(std::string_view key,
                                                   std::int64_t fallback) const;

  // Comma-separated list.
  [[nodiscard]] std::vector<std::string> GetList(std::string_view key) const;

  [[nodiscard]] const std::map<std::string, std::string, std::less<>>& entries() const {
    return entries_;
  }

  // Renders the effective configuration, one `key=value` per line, for the
  // start-up log and for benchmark result metadata.
  [[nodiscard]] std::string Dump() const;

  // Keys that were set but never read by any getter.
  //
  // Validating values is not enough: a key nobody consumes is silently
  // ignored, and the process runs on defaults while appearing configured. A
  // three-broker Docker cluster once ran for weeks as three independent
  // single-broker clusters this way, because the compose file spelled
  // cluster.brokers with a separator this store maps to something else.
  //
  // Call after every component has read its configuration -- anything read
  // later will already have been reported as unread.
  [[nodiscard]] std::vector<std::string> UnreadKeys() const;

 private:
  void MarkRead(std::string_view key) const;

  std::map<std::string, std::string, std::less<>> entries_;
  // Mutable because reading configuration is logically const; the bookkeeping
  // is not part of the store's observable value.
  mutable std::set<std::string, std::less<>> read_keys_;
};

}  // namespace pulselog

#endif  // PULSELOG_BASE_CONFIG_H_
