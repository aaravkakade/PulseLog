// Endianness helpers for the wire protocol and on-disk format.
//
// PulseLog encodes every multi-byte integer as little-endian, matching the
// native layout of both supported architectures (x86-64 and AArch64) so the
// conversions compile to no-ops there. The functions below are written against
// byte arrays rather than reinterpret_cast so they stay well-defined on a
// big-endian host and never rely on unaligned access.
#ifndef PULSELOG_BASE_ENDIAN_H_
#define PULSELOG_BASE_ENDIAN_H_

#include <bit>
#include <cstdint>
#include <cstring>
#include <type_traits>

namespace pulselog {

template <typename T>
concept UnsignedIntegral = std::is_unsigned_v<T> && std::is_integral_v<T>;

// std::byteswap is C++23 and is not available in the libc++ shipped with
// Apple clang 15, so the builtins are used directly. Both GCC and Clang lower
// these to a single instruction.
template <UnsignedIntegral T>
[[nodiscard]] constexpr T ByteSwap(T value) noexcept {
  if constexpr (sizeof(T) == 1) {
    return value;
  } else if constexpr (sizeof(T) == 2) {
    return static_cast<T>(__builtin_bswap16(static_cast<std::uint16_t>(value)));
  } else if constexpr (sizeof(T) == 4) {
    return static_cast<T>(__builtin_bswap32(static_cast<std::uint32_t>(value)));
  } else {
    static_assert(sizeof(T) == 8, "unsupported integer width");
    return static_cast<T>(__builtin_bswap64(static_cast<std::uint64_t>(value)));
  }
}

// Stores `value` at `dst` in little-endian order. `dst` must have room for
// sizeof(T) bytes; bounds checking is the caller's responsibility (the codec
// layer does it once per frame rather than once per field).
template <UnsignedIntegral T>
inline void StoreLe(std::uint8_t* dst, T value) noexcept {
  if constexpr (std::endian::native == std::endian::big) {
    value = ByteSwap(value);
  }
  std::memcpy(dst, &value, sizeof(T));
}

template <UnsignedIntegral T>
[[nodiscard]] inline T LoadLe(const std::uint8_t* src) noexcept {
  T value{};
  std::memcpy(&value, src, sizeof(T));
  if constexpr (std::endian::native == std::endian::big) {
    value = ByteSwap(value);
  }
  return value;
}

// Signed helpers go through the unsigned representation. Casting through the
// unsigned type is well-defined in C++20 (two's complement is mandated).
inline void StoreLeI16(std::uint8_t* dst, std::int16_t v) noexcept {
  StoreLe<std::uint16_t>(dst, static_cast<std::uint16_t>(v));
}

inline void StoreLeI32(std::uint8_t* dst, std::int32_t v) noexcept {
  StoreLe<std::uint32_t>(dst, static_cast<std::uint32_t>(v));
}

inline void StoreLeI64(std::uint8_t* dst, std::int64_t v) noexcept {
  StoreLe<std::uint64_t>(dst, static_cast<std::uint64_t>(v));
}

[[nodiscard]] inline std::int16_t LoadLeI16(const std::uint8_t* src) noexcept {
  return static_cast<std::int16_t>(LoadLe<std::uint16_t>(src));
}

[[nodiscard]] inline std::int32_t LoadLeI32(const std::uint8_t* src) noexcept {
  return static_cast<std::int32_t>(LoadLe<std::uint32_t>(src));
}

[[nodiscard]] inline std::int64_t LoadLeI64(const std::uint8_t* src) noexcept {
  return static_cast<std::int64_t>(LoadLe<std::uint64_t>(src));
}

// Zig-zag transform used by the varint encoder so small negative numbers stay
// short on the wire.
[[nodiscard]] inline std::uint64_t ZigZagEncode(std::int64_t v) noexcept {
  return (static_cast<std::uint64_t>(v) << 1U) ^ static_cast<std::uint64_t>(v >> 63);
}

[[nodiscard]] inline std::int64_t ZigZagDecode(std::uint64_t v) noexcept {
  return static_cast<std::int64_t>((v >> 1U) ^ (~(v & 1U) + 1U));
}

}  // namespace pulselog

#endif  // PULSELOG_BASE_ENDIAN_H_
