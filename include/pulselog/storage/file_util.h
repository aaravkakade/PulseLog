// POSIX file primitives with RAII ownership and Status-based errors.
//
// Every syscall here retries on EINTR and reports partial transfers honestly:
// a short write is not an error, it is a condition the caller must loop on.
// Getting that wrong is the classic way a log engine silently truncates data.
#ifndef PULSELOG_STORAGE_FILE_UTIL_H_
#define PULSELOG_STORAGE_FILE_UTIL_H_

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <span>
#include <string>
#include <vector>

#include <sys/uio.h>

#include "pulselog/base/buffer.h"
#include "pulselog/base/status.h"

namespace pulselog::storage {

// How the engine issues appends. Selected by config; benchmarked in
// benchmarks/bench_storage.cc with the results in docs/PERFORMANCE_RESULTS.md.
enum class WriteMode : std::uint8_t {
  // One write(2) per append. Simplest; one syscall per batch.
  kWrite = 0,
  // One writev(2) for several batches at once. Lets the broker coalesce
  // multiple produce requests for the same partition into a single syscall
  // without first copying them into a common buffer.
  kWritev = 1,
  // Append into a memory-mapped window. Removes the syscall but makes flush
  // semantics (msync) and file growth considerably more delicate.
  kMmap = 2,
};

[[nodiscard]] std::string_view WriteModeName(WriteMode mode) noexcept;

// How hard a flush pushes.
//
//   kFull  On macOS this is F_FULLFSYNC, which asks the drive to commit to
//          stable media. It is the only mode on that platform where "flushed"
//          survives power loss, so it is the default and it is what quorum
//          acknowledgements are defined against. On Linux it is fsync(2).
//   kData  fdatasync(2) on Linux, fsync(2) on macOS. Data has left the page
//          cache and reached the device, which may still hold it in a
//          volatile write cache. Survives a process crash; a power cut may
//          still lose it.
//
// This is not a micro-optimisation knob. On this project's macOS test machine,
// background F_FULLFSYNC stalls concurrent appends to the same file badly
// enough to cut produce throughput from ~4.7M to ~1.8M records/s and push p99
// from 154 us to ~3 ms (docs/PERFORMANCE_RESULTS.md). The stall is in the
// filesystem, not in this code -- appends take no lock that a flush holds --
// so the only honest response is to make the trade explicit rather than pick
// silently.
enum class SyncMode : std::uint8_t {
  kFull = 0,
  kData = 1,
};

[[nodiscard]] std::string_view SyncModeName(SyncMode mode) noexcept;

[[nodiscard]] bool ParseSyncMode(std::string_view text, SyncMode& out) noexcept;

[[nodiscard]] bool ParseWriteMode(std::string_view text, WriteMode& out) noexcept;

// An owning file descriptor. Move-only; closes on destruction.
class FileHandle {
 public:
  FileHandle() = default;

  explicit FileHandle(int fd) noexcept : fd_(fd) {}

  FileHandle(const FileHandle&) = delete;
  FileHandle& operator=(const FileHandle&) = delete;

  FileHandle(FileHandle&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }

  FileHandle& operator=(FileHandle&& other) noexcept {
    if (this != &other) {
      Close();
      fd_ = other.fd_;
      other.fd_ = -1;
    }
    return *this;
  }

  ~FileHandle() { Close(); }

  // Opens for read+write, creating if requested. `append_only` sets O_APPEND.
  [[nodiscard]] static Result<FileHandle> Open(const std::filesystem::path& path,
                                               bool create,
                                               bool read_only = false);

  [[nodiscard]] bool valid() const noexcept { return fd_ >= 0; }

  [[nodiscard]] int fd() const noexcept { return fd_; }

  void Close() noexcept;

  // Writes all of `data` at `offset`, looping over short writes.
  [[nodiscard]] Status WriteAllAt(ByteSpan data, std::uint64_t offset) const;

  // Writes all of `chunks` at `offset` using writev(2), looping over short
  // writes by advancing into the iovec array.
  [[nodiscard]] Status WriteVectoredAt(std::span<const ByteSpan> chunks,
                                       std::uint64_t offset) const;

  // Reads exactly `size` bytes at `offset`. Returns kOutOfRange on EOF before
  // `size` bytes are available.
  [[nodiscard]] Status ReadExactAt(std::uint8_t* dst, std::size_t size, std::uint64_t offset) const;

  // Reads up to `size` bytes; returns how many were actually read (0 at EOF).
  [[nodiscard]] Result<std::size_t> ReadAt(std::uint8_t* dst,
                                           std::size_t size,
                                           std::uint64_t offset) const;

  // Flushes according to `mode`. See SyncMode for what each one promises.
  [[nodiscard]] Status Sync(SyncMode mode = SyncMode::kFull) const;

  // fdatasync where available: skips the metadata flush when only file data
  // changed, which is the common case for an append-only log whose size the
  // kernel must still record -- so this is only a win once the file has been
  // preallocated.
  [[nodiscard]] Status SyncData() const;

  [[nodiscard]] Status Truncate(std::uint64_t size) const;

  [[nodiscard]] Result<std::uint64_t> Size() const;

  // Reserves `size` bytes without writing them. Avoids per-append metadata
  // updates and reduces fragmentation. Falls back to ftruncate where the
  // platform has no allocation call.
  [[nodiscard]] Status Preallocate(std::uint64_t size) const;

  // Advises the kernel that this file will be read sequentially. Best-effort.
  void HintSequential() const noexcept;

 private:
  int fd_ = -1;
};

// A read-only memory mapping. Used for index files and for the mmap read path.
class MemoryMap {
 public:
  MemoryMap() = default;

  MemoryMap(const MemoryMap&) = delete;
  MemoryMap& operator=(const MemoryMap&) = delete;

  MemoryMap(MemoryMap&& other) noexcept
      : data_(other.data_), size_(other.size_), writable_(other.writable_) {
    other.data_ = nullptr;
    other.size_ = 0;
  }

  MemoryMap& operator=(MemoryMap&& other) noexcept {
    if (this != &other) {
      Unmap();
      data_ = other.data_;
      size_ = other.size_;
      writable_ = other.writable_;
      other.data_ = nullptr;
      other.size_ = 0;
    }
    return *this;
  }

  ~MemoryMap() { Unmap(); }

  [[nodiscard]] static Result<MemoryMap> Create(const FileHandle& file,
                                                std::uint64_t size,
                                                bool writable);

  [[nodiscard]] bool valid() const noexcept { return data_ != nullptr; }

  [[nodiscard]] std::uint8_t* data() noexcept { return data_; }

  [[nodiscard]] const std::uint8_t* data() const noexcept { return data_; }

  [[nodiscard]] std::size_t size() const noexcept { return size_; }

  [[nodiscard]] ByteSpan span() const noexcept { return {data_, size_}; }

  [[nodiscard]] Status Sync() const;

  void Unmap() noexcept;

 private:
  std::uint8_t* data_ = nullptr;
  std::size_t size_ = 0;
  bool writable_ = false;
};

// Creates `path` and every missing parent.
[[nodiscard]] Status EnsureDirectory(const std::filesystem::path& path);

// fsync on the *directory*, which is what makes a file creation or rename
// durable. Without it a freshly rolled segment can vanish on power loss even
// though its contents were fsynced.
[[nodiscard]] Status SyncDirectory(const std::filesystem::path& path);

// Lists regular files directly inside `dir` whose name ends with `suffix`.
[[nodiscard]] Result<std::vector<std::filesystem::path>> ListFiles(const std::filesystem::path& dir,
                                                                   std::string_view suffix);

// Free bytes on the filesystem holding `path`. Used by the disk-full guard.
[[nodiscard]] Result<std::uint64_t> AvailableBytes(const std::filesystem::path& path);

}  // namespace pulselog::storage

#endif  // PULSELOG_STORAGE_FILE_UTIL_H_
