// CRC-32C (Castagnoli, polynomial 0x1EDC6F41) used for every frame header,
// frame payload and on-disk record in PulseLog.
//
// Two implementations are provided and selected once at start-up:
//   * a hardware path using the CRC32C instructions on AArch64 (__crc32c*)
//     and x86-64 (SSE4.2 _mm_crc32_*), and
//   * a portable slicing-by-8 table implementation.
//
// benchmarks/bench_checksum.cc measures both; the results are recorded in
// docs/PERFORMANCE_RESULTS.md. Both paths are verified against the RFC 3720
// test vectors in tests/unit/test_crc32c.cc.
#ifndef PULSELOG_BASE_CRC32C_H_
#define PULSELOG_BASE_CRC32C_H_

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

namespace pulselog {

// Computes CRC-32C over `data`, continuing from `seed` (pass 0 to start).
// Dispatches to the hardware implementation when the running CPU supports it.
[[nodiscard]] std::uint32_t Crc32c(std::span<const std::uint8_t> data,
                                   std::uint32_t seed = 0) noexcept;

[[nodiscard]] inline std::uint32_t Crc32c(const void* data,
                                          std::size_t size,
                                          std::uint32_t seed = 0) noexcept {
  return Crc32c(std::span<const std::uint8_t>(static_cast<const std::uint8_t*>(data), size), seed);
}

// Portable reference implementation. Exposed so benchmarks and tests can
// compare it against the dispatched version.
[[nodiscard]] std::uint32_t Crc32cSoftware(std::span<const std::uint8_t> data,
                                           std::uint32_t seed = 0) noexcept;

// True when the process selected the hardware implementation.
[[nodiscard]] bool Crc32cHardwareAvailable() noexcept;

// Name of the active implementation, for the metrics endpoint and benchmark
// metadata ("hardware-arm64", "hardware-sse42" or "software-slice-by-8").
[[nodiscard]] std::string_view Crc32cImplementationName() noexcept;

}  // namespace pulselog

#endif  // PULSELOG_BASE_CRC32C_H_
