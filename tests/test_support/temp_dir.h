// Test-only helpers for creating and removing temporary files and directories.
#ifndef PULSELOG_TESTS_TEST_SUPPORT_TEMP_DIR_H_
#define PULSELOG_TESTS_TEST_SUPPORT_TEMP_DIR_H_

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>

#include <unistd.h>

namespace pulselog::testing {

// Creates a uniquely named directory under the system temp location and
// removes it (recursively) on destruction.
class TempDir {
 public:
  explicit TempDir(const std::string& prefix = "pulselog-test") {
    std::filesystem::path templ = std::filesystem::temp_directory_path() / (prefix + "-XXXXXX");
    std::string buffer = templ.string();
    const char* created = ::mkdtemp(buffer.data());
    if (created == nullptr) {
      throw std::runtime_error("mkdtemp failed for " + buffer);
    }
    path_ = created;
  }

  TempDir(const TempDir&) = delete;
  TempDir& operator=(const TempDir&) = delete;

  ~TempDir() {
    std::error_code ec;
    std::filesystem::remove_all(path_, ec);
  }

  [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

  [[nodiscard]] std::string str() const { return path_.string(); }

  [[nodiscard]] std::filesystem::path Child(const std::string& name) const { return path_ / name; }

 private:
  std::filesystem::path path_;
};

// Creates a temp file with the given contents; removes it on destruction.
class TempFile {
 public:
  explicit TempFile(const std::string& contents, const std::string& suffix = ".tmp") {
    std::string buffer = (std::filesystem::temp_directory_path() / "pulselog-XXXXXX").string();
    const int fd = ::mkstemp(buffer.data());
    if (fd < 0) throw std::runtime_error("mkstemp failed");
    ::close(fd);
    path_ = buffer + suffix;
    std::filesystem::remove(buffer);
    std::ofstream out(path_, std::ios::binary);
    out << contents;
  }

  TempFile(const TempFile&) = delete;
  TempFile& operator=(const TempFile&) = delete;

  ~TempFile() {
    std::error_code ec;
    std::filesystem::remove(path_, ec);
  }

  [[nodiscard]] const std::string& path() const noexcept { return path_; }

 private:
  std::string path_;
};

// Reads a whole file into a byte vector. Returns an empty vector when absent.
inline std::vector<std::uint8_t> ReadFileBytes(const std::filesystem::path& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) return {};
  return std::vector<std::uint8_t>((std::istreambuf_iterator<char>(in)),
                                   std::istreambuf_iterator<char>());
}

// Overwrites `count` bytes at `offset` with `value`. Used by corruption tests.
inline bool CorruptFileAt(const std::filesystem::path& path,
                          std::uint64_t offset,
                          std::size_t count,
                          std::uint8_t value) {
  std::fstream file(path, std::ios::binary | std::ios::in | std::ios::out);
  if (!file) return false;
  file.seekp(static_cast<std::streamoff>(offset));
  const std::vector<char> filler(count, static_cast<char>(value));
  file.write(filler.data(), static_cast<std::streamsize>(filler.size()));
  return file.good();
}

// Truncates a file to `size` bytes, simulating a torn write at the log tail.
inline bool TruncateFile(const std::filesystem::path& path, std::uint64_t size) {
  std::error_code ec;
  std::filesystem::resize_file(path, size, ec);
  return !ec;
}

}  // namespace pulselog::testing

#endif  // PULSELOG_TESTS_TEST_SUPPORT_TEMP_DIR_H_
