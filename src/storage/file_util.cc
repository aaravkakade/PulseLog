#include "pulselog/storage/file_util.h"

#include <algorithm>
#include <cerrno>
#include <system_error>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>

namespace pulselog::storage {
namespace {

// Largest transfer attempted in one syscall. Some platforms cap writev at
// INT_MAX; staying well under keeps the loop arithmetic simple.
constexpr std::size_t kMaxTransfer = 1U << 30U;

constexpr int kMaxIovecs = 64;

}  // namespace

std::string_view WriteModeName(WriteMode mode) noexcept {
  switch (mode) {
    case WriteMode::kWrite:
      return "write";
    case WriteMode::kWritev:
      return "writev";
    case WriteMode::kMmap:
      return "mmap";
  }
  return "unknown";
}

std::string_view SyncModeName(SyncMode mode) noexcept {
  switch (mode) {
    case SyncMode::kFull:
      return "full";
    case SyncMode::kData:
      return "data";
  }
  return "unknown";
}

bool ParseSyncMode(std::string_view text, SyncMode& out) noexcept {
  if (text == "full" || text == "fullfsync") {
    out = SyncMode::kFull;
    return true;
  }
  if (text == "data" || text == "fdatasync") {
    out = SyncMode::kData;
    return true;
  }
  return false;
}

bool ParseWriteMode(std::string_view text, WriteMode& out) noexcept {
  if (text == "write") {
    out = WriteMode::kWrite;
    return true;
  }
  if (text == "writev") {
    out = WriteMode::kWritev;
    return true;
  }
  if (text == "mmap") {
    out = WriteMode::kMmap;
    return true;
  }
  return false;
}

Result<FileHandle> FileHandle::Open(const std::filesystem::path& path,
                                    bool create,
                                    bool read_only) {
  int flags = read_only ? O_RDONLY : O_RDWR;
  if (create) flags |= O_CREAT;
#ifdef O_CLOEXEC
  flags |= O_CLOEXEC;
#endif

  int fd = -1;
  do {
    fd = ::open(path.c_str(), flags, 0644);
  } while (fd < 0 && errno == EINTR);

  if (fd < 0) {
    return ErrnoToStatus("open " + path.string(), errno);
  }
  return FileHandle(fd);
}

void FileHandle::Close() noexcept {
  if (fd_ >= 0) {
    // close() failures are not actionable here -- the descriptor is gone
    // either way, and an error means data was already lost upstream. Callers
    // that need durability call Sync() first and check that.
    ::close(fd_);
    fd_ = -1;
  }
}

Status FileHandle::WriteAllAt(ByteSpan data, std::uint64_t offset) const {
  const std::uint8_t* p = data.data();
  std::size_t remaining = data.size();
  std::uint64_t pos = offset;

  while (remaining > 0) {
    const std::size_t chunk = std::min(remaining, kMaxTransfer);
    const ssize_t written = ::pwrite(fd_, p, chunk, static_cast<off_t>(pos));
    if (written < 0) {
      if (errno == EINTR) continue;
      return ErrnoToStatus("pwrite", errno);
    }
    if (written == 0) {
      return IoError("pwrite made no progress");
    }
    p += written;
    pos += static_cast<std::uint64_t>(written);
    remaining -= static_cast<std::size_t>(written);
  }
  return OkStatus();
}

Status FileHandle::WriteVectoredAt(std::span<const ByteSpan> chunks, std::uint64_t offset) const {
  if (chunks.empty()) return OkStatus();

  // A local copy of the iovec array, advanced as short writes consume it.
  std::vector<::iovec> vecs;
  vecs.reserve(chunks.size());
  for (const auto& chunk : chunks) {
    if (chunk.empty()) continue;
    vecs.push_back(::iovec{const_cast<std::uint8_t*>(chunk.data()), chunk.size()});
  }
  if (vecs.empty()) return OkStatus();

  std::size_t index = 0;
  std::uint64_t pos = offset;
  while (index < vecs.size()) {
    const int count = static_cast<int>(std::min<std::size_t>(vecs.size() - index, kMaxIovecs));
    const ssize_t written = ::pwritev(fd_, vecs.data() + index, count, static_cast<off_t>(pos));
    if (written < 0) {
      if (errno == EINTR) continue;
      return ErrnoToStatus("pwritev", errno);
    }
    if (written == 0) return IoError("pwritev made no progress");

    pos += static_cast<std::uint64_t>(written);
    // Consume whole iovecs, then partially consume the one we stopped inside.
    auto consumed = static_cast<std::size_t>(written);
    while (index < vecs.size() && consumed >= vecs[index].iov_len) {
      consumed -= vecs[index].iov_len;
      ++index;
    }
    if (consumed > 0 && index < vecs.size()) {
      vecs[index].iov_base = static_cast<std::uint8_t*>(vecs[index].iov_base) + consumed;
      vecs[index].iov_len -= consumed;
    }
  }
  return OkStatus();
}

Result<std::size_t> FileHandle::ReadAt(std::uint8_t* dst,
                                       std::size_t size,
                                       std::uint64_t offset) const {
  std::size_t total = 0;
  while (total < size) {
    const ssize_t got = ::pread(fd_, dst + total, size - total, static_cast<off_t>(offset + total));
    if (got < 0) {
      if (errno == EINTR) continue;
      return ErrnoToStatus("pread", errno);
    }
    if (got == 0) break;  // EOF.
    total += static_cast<std::size_t>(got);
  }
  return total;
}

Status FileHandle::ReadExactAt(std::uint8_t* dst, std::size_t size, std::uint64_t offset) const {
  auto got = ReadAt(dst, size, offset);
  if (!got.ok()) return got.status();
  if (got.value() != size) {
    return OutOfRange("short read: wanted " + std::to_string(size) + " bytes at offset " +
                      std::to_string(offset) + ", got " + std::to_string(got.value()));
  }
  return OkStatus();
}

Status FileHandle::Sync(SyncMode mode) const {
#if defined(__APPLE__)
  // On macOS, fsync only pushes data to the drive's cache. F_FULLFSYNC asks
  // the drive to commit to stable media, which is what "durable" has to mean
  // for a log. It is markedly slower, and that cost is real, not incidental --
  // see the flush-latency numbers in docs/PERFORMANCE_RESULTS.md.
  if (mode == SyncMode::kFull) {
    if (::fcntl(fd_, F_FULLFSYNC) == 0) return OkStatus();
    // Some filesystems (and every network mount) reject F_FULLFSYNC; fall back.
    if (errno != ENOTSUP && errno != EINVAL) {
      return ErrnoToStatus("fcntl(F_FULLFSYNC)", errno);
    }
  }
#elif defined(__linux__)
  if (mode == SyncMode::kData) {
    while (::fdatasync(fd_) != 0) {
      if (errno == EINTR) continue;
      return ErrnoToStatus("fdatasync", errno);
    }
    return OkStatus();
  }
#else
  (void)mode;
#endif
  while (::fsync(fd_) != 0) {
    if (errno == EINTR) continue;
    return ErrnoToStatus("fsync", errno);
  }
  return OkStatus();
}

Status FileHandle::SyncData() const {
#if defined(__linux__)
  while (::fdatasync(fd_) != 0) {
    if (errno == EINTR) continue;
    return ErrnoToStatus("fdatasync", errno);
  }
  return OkStatus();
#else
  return Sync();
#endif
}

Status FileHandle::Truncate(std::uint64_t size) const {
  while (::ftruncate(fd_, static_cast<off_t>(size)) != 0) {
    if (errno == EINTR) continue;
    return ErrnoToStatus("ftruncate", errno);
  }
  return OkStatus();
}

Result<std::uint64_t> FileHandle::Size() const {
  struct ::stat st{};
  if (::fstat(fd_, &st) != 0) {
    return ErrnoToStatus("fstat", errno);
  }
  return static_cast<std::uint64_t>(st.st_size);
}

Status FileHandle::Preallocate(std::uint64_t size) const {
#if defined(__linux__)
  const int rc = ::posix_fallocate(fd_, 0, static_cast<off_t>(size));
  if (rc == 0) return OkStatus();
  if (rc != EOPNOTSUPP && rc != ENOSYS) return ErrnoToStatus("posix_fallocate", rc);
#elif defined(__APPLE__)
  ::fstore_t store{};
  store.fst_flags = F_ALLOCATECONTIG;
  store.fst_posmode = F_PEOFPOSMODE;
  store.fst_offset = 0;
  store.fst_length = static_cast<off_t>(size);
  if (::fcntl(fd_, F_PREALLOCATE, &store) != 0) {
    // Contiguous allocation failed (fragmented volume); retry allowing any
    // arrangement before giving up on preallocation entirely.
    store.fst_flags = F_ALLOCATEALL;
    ::fcntl(fd_, F_PREALLOCATE, &store);
  }
#endif
  // ftruncate makes the size visible even when the allocation call was a
  // no-op, so callers always get a file of the expected length.
  return Truncate(size);
}

void FileHandle::HintSequential() const noexcept {
#if defined(__linux__)
  ::posix_fadvise(fd_, 0, 0, POSIX_FADV_SEQUENTIAL);
#elif defined(__APPLE__)
  ::fcntl(fd_, F_RDAHEAD, 1);
#endif
}

Result<MemoryMap> MemoryMap::Create(const FileHandle& file, std::uint64_t size, bool writable) {
  if (size == 0) return MemoryMap{};

  const int prot = writable ? (PROT_READ | PROT_WRITE) : PROT_READ;
  void* addr = ::mmap(nullptr, size, prot, MAP_SHARED, file.fd(), 0);
  if (addr == MAP_FAILED) {
    return ErrnoToStatus("mmap", errno);
  }

  MemoryMap map;
  map.data_ = static_cast<std::uint8_t*>(addr);
  map.size_ = size;
  map.writable_ = writable;
  return map;
}

Status MemoryMap::Sync() const {
  if (data_ == nullptr) return OkStatus();
  if (::msync(data_, size_, MS_SYNC) != 0) {
    return ErrnoToStatus("msync", errno);
  }
  return OkStatus();
}

void MemoryMap::Unmap() noexcept {
  if (data_ != nullptr) {
    ::munmap(data_, size_);
    data_ = nullptr;
    size_ = 0;
  }
}

Status EnsureDirectory(const std::filesystem::path& path) {
  std::error_code ec;
  std::filesystem::create_directories(path, ec);
  if (ec && !std::filesystem::is_directory(path)) {
    return IoError("create_directories " + path.string() + ": " + ec.message());
  }
  return OkStatus();
}

Status SyncDirectory(const std::filesystem::path& path) {
  int fd = -1;
  do {
    fd = ::open(path.c_str(), O_RDONLY);
  } while (fd < 0 && errno == EINTR);
  if (fd < 0) return ErrnoToStatus("open directory " + path.string(), errno);

  Status status = OkStatus();
  while (::fsync(fd) != 0) {
    if (errno == EINTR) continue;
    // Some filesystems refuse fsync on a directory fd. That is not a data
    // loss condition here, so it is reported at debug level by the caller
    // rather than failing the operation.
    status = ErrnoToStatus("fsync directory " + path.string(), errno);
    break;
  }
  ::close(fd);
  return status;
}

Result<std::vector<std::filesystem::path>> ListFiles(const std::filesystem::path& dir,
                                                     std::string_view suffix) {
  std::vector<std::filesystem::path> files;
  std::error_code ec;
  if (!std::filesystem::exists(dir, ec)) return files;

  for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
    if (ec) return IoError("directory_iterator " + dir.string() + ": " + ec.message());
    if (!entry.is_regular_file()) continue;
    const std::string name = entry.path().filename().string();
    if (name.size() >= suffix.size() &&
        name.compare(name.size() - suffix.size(), suffix.size(), suffix) == 0) {
      files.push_back(entry.path());
    }
  }
  std::sort(files.begin(), files.end());
  return files;
}

Result<std::uint64_t> AvailableBytes(const std::filesystem::path& path) {
  struct ::statvfs stat{};
  if (::statvfs(path.c_str(), &stat) != 0) {
    return ErrnoToStatus("statvfs " + path.string(), errno);
  }
  return static_cast<std::uint64_t>(stat.f_bavail) * stat.f_frsize;
}

}  // namespace pulselog::storage
