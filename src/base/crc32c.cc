#include "pulselog/base/crc32c.h"

#include <array>

#include "pulselog/base/endian.h"

#if defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
#define PULSELOG_CRC32C_ARM 1
#include <arm_acle.h>
#endif

#if defined(__x86_64__) || defined(_M_X64)
#define PULSELOG_CRC32C_X86 1
#include <nmmintrin.h>
#endif

namespace pulselog {
namespace {

// Reflected CRC-32C polynomial (0x1EDC6F41 bit-reversed).
constexpr std::uint32_t kPoly = 0x82F63B78U;

using Tables = std::array<std::array<std::uint32_t, 256>, 8>;

// Slicing-by-8 tables, built at compile time so there is no start-up cost and
// no mutable global state.
constexpr Tables MakeTables() {
  Tables tables{};
  for (std::uint32_t i = 0; i < 256; ++i) {
    std::uint32_t crc = i;
    for (int bit = 0; bit < 8; ++bit) {
      // Branch-free: (~(crc & 1) + 1) is 0xFFFFFFFF when the low bit is set.
      crc = (crc >> 1U) ^ (kPoly & (~(crc & 1U) + 1U));
    }
    tables[0][i] = crc;
  }
  for (std::uint32_t i = 0; i < 256; ++i) {
    std::uint32_t crc = tables[0][i];
    for (std::size_t slice = 1; slice < 8; ++slice) {
      crc = tables[0][crc & 0xFFU] ^ (crc >> 8U);
      tables[slice][i] = crc;
    }
  }
  return tables;
}

constexpr Tables kTables = MakeTables();

#if PULSELOG_CRC32C_X86
bool HasSse42() {
  static const bool supported = __builtin_cpu_supports("sse4.2");
  return supported;
}

__attribute__((target("sse4.2"))) std::uint32_t Crc32cSse42(std::span<const std::uint8_t> data,
                                                            std::uint32_t seed) noexcept {
  std::uint32_t crc = ~seed;
  const std::uint8_t* p = data.data();
  std::size_t n = data.size();
  // Align to 8 bytes first so the 64-bit loop never straddles a cache line
  // unnecessarily.
  while (n > 0 && (reinterpret_cast<std::uintptr_t>(p) & 7U) != 0) {
    crc = _mm_crc32_u8(crc, *p++);
    --n;
  }
  while (n >= 8) {
    std::uint64_t word{};
    std::memcpy(&word, p, sizeof(word));
    crc = static_cast<std::uint32_t>(_mm_crc32_u64(crc, word));
    p += 8;
    n -= 8;
  }
  while (n-- > 0) crc = _mm_crc32_u8(crc, *p++);
  return ~crc;
}
#endif  // PULSELOG_CRC32C_X86

#if PULSELOG_CRC32C_ARM
std::uint32_t Crc32cArm(std::span<const std::uint8_t> data, std::uint32_t seed) noexcept {
  std::uint32_t crc = ~seed;
  const std::uint8_t* p = data.data();
  std::size_t n = data.size();
  while (n >= 8) {
    std::uint64_t word{};
    std::memcpy(&word, p, sizeof(word));
    crc = __crc32cd(crc, word);
    p += 8;
    n -= 8;
  }
  if (n >= 4) {
    std::uint32_t word{};
    std::memcpy(&word, p, sizeof(word));
    crc = __crc32cw(crc, word);
    p += 4;
    n -= 4;
  }
  if (n >= 2) {
    std::uint16_t half{};
    std::memcpy(&half, p, sizeof(half));
    crc = __crc32ch(crc, half);
    p += 2;
    n -= 2;
  }
  if (n == 1) crc = __crc32cb(crc, *p);
  return ~crc;
}
#endif  // PULSELOG_CRC32C_ARM

enum class Impl { kSoftware, kArm, kSse42 };

Impl SelectImpl() noexcept {
#if PULSELOG_CRC32C_ARM
  return Impl::kArm;
#elif PULSELOG_CRC32C_X86
  return HasSse42() ? Impl::kSse42 : Impl::kSoftware;
#else
  return Impl::kSoftware;
#endif
}

const Impl kImpl = SelectImpl();

}  // namespace

std::uint32_t Crc32cSoftware(std::span<const std::uint8_t> data, std::uint32_t seed) noexcept {
  std::uint32_t crc = ~seed;
  const std::uint8_t* p = data.data();
  std::size_t n = data.size();

  // Slicing-by-8: consumes 8 bytes per iteration with 8 independent table
  // lookups, which the out-of-order engine can overlap.
  while (n >= 8) {
    crc ^= LoadLe<std::uint32_t>(p);
    const std::uint32_t next = LoadLe<std::uint32_t>(p + 4);
    crc = kTables[7][crc & 0xFFU] ^ kTables[6][(crc >> 8U) & 0xFFU] ^
          kTables[5][(crc >> 16U) & 0xFFU] ^ kTables[4][(crc >> 24U) & 0xFFU] ^
          kTables[3][next & 0xFFU] ^ kTables[2][(next >> 8U) & 0xFFU] ^
          kTables[1][(next >> 16U) & 0xFFU] ^ kTables[0][(next >> 24U) & 0xFFU];
    p += 8;
    n -= 8;
  }
  while (n-- > 0) {
    crc = kTables[0][(crc ^ *p++) & 0xFFU] ^ (crc >> 8U);
  }
  return ~crc;
}

std::uint32_t Crc32c(std::span<const std::uint8_t> data, std::uint32_t seed) noexcept {
  switch (kImpl) {
#if PULSELOG_CRC32C_ARM
    case Impl::kArm:
      return Crc32cArm(data, seed);
#endif
#if PULSELOG_CRC32C_X86
    case Impl::kSse42:
      return Crc32cSse42(data, seed);
#endif
    default:
      return Crc32cSoftware(data, seed);
  }
}

bool Crc32cHardwareAvailable() noexcept { return kImpl != Impl::kSoftware; }

std::string_view Crc32cImplementationName() noexcept {
  switch (kImpl) {
    case Impl::kArm:
      return "hardware-arm64";
    case Impl::kSse42:
      return "hardware-sse42";
    case Impl::kSoftware:
      break;
  }
  return "software-slice-by-8";
}

}  // namespace pulselog
