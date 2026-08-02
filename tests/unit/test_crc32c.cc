#include <array>
#include <cstdint>
#include <numeric>
#include <random>
#include <vector>

#include <gtest/gtest.h>

#include "pulselog/base/crc32c.h"

namespace pulselog {
namespace {

// Test vectors from RFC 3720 appendix B.4 (iSCSI CRC-32C).
TEST(Crc32c, Rfc3720Vectors) {
  const std::vector<std::uint8_t> zeros(32, 0x00);
  EXPECT_EQ(Crc32c(zeros), 0x8A9136AAU);

  const std::vector<std::uint8_t> ones(32, 0xFF);
  EXPECT_EQ(Crc32c(ones), 0x62A8AB43U);

  std::vector<std::uint8_t> incrementing(32);
  std::iota(incrementing.begin(), incrementing.end(), std::uint8_t{0});
  EXPECT_EQ(Crc32c(incrementing), 0x46DD794EU);

  std::vector<std::uint8_t> decrementing(32);
  for (std::size_t i = 0; i < decrementing.size(); ++i) {
    decrementing[i] = static_cast<std::uint8_t>(31 - i);
  }
  EXPECT_EQ(Crc32c(decrementing), 0x113FDB5CU);
}

TEST(Crc32c, CheckString) {
  const std::string_view check = "123456789";
  EXPECT_EQ(Crc32c(check.data(), check.size()), 0xE3069283U);
}

TEST(Crc32c, EmptyInputIsZero) {
  EXPECT_EQ(Crc32c(std::span<const std::uint8_t>{}), 0U);
  EXPECT_EQ(Crc32cSoftware(std::span<const std::uint8_t>{}), 0U);
}

TEST(Crc32c, HardwareMatchesSoftwareAcrossLengths) {
  // Exercises every tail path of both implementations (8/4/2/1-byte steps).
  std::mt19937 rng(0xC0FFEE);
  std::vector<std::uint8_t> data(1024);
  for (auto& b : data) b = static_cast<std::uint8_t>(rng());

  for (std::size_t len = 0; len <= data.size(); ++len) {
    const std::span<const std::uint8_t> slice(data.data(), len);
    EXPECT_EQ(Crc32c(slice), Crc32cSoftware(slice)) << "length " << len;
  }
}

TEST(Crc32c, SeedChainingMatchesSinglePass) {
  std::mt19937 rng(7);
  std::vector<std::uint8_t> data(4096);
  for (auto& b : data) b = static_cast<std::uint8_t>(rng());

  const std::uint32_t whole = Crc32c(data);
  for (const std::size_t split :
       {std::size_t{1}, std::size_t{7}, std::size_t{64}, std::size_t{1000}, std::size_t{4095}}) {
    const std::uint32_t first = Crc32c(std::span(data.data(), split));
    const std::uint32_t chained =
        Crc32c(std::span(data.data() + split, data.size() - split), first);
    EXPECT_EQ(chained, whole) << "split at " << split;
  }
}

TEST(Crc32c, DetectsSingleBitFlips) {
  std::vector<std::uint8_t> data(128, 0xAB);
  const std::uint32_t base = Crc32c(data);
  for (std::size_t byte = 0; byte < data.size(); ++byte) {
    for (int bit = 0; bit < 8; ++bit) {
      data[byte] ^= static_cast<std::uint8_t>(1U << bit);
      EXPECT_NE(Crc32c(data), base) << "bit " << bit << " of byte " << byte;
      data[byte] ^= static_cast<std::uint8_t>(1U << bit);
    }
  }
}

TEST(Crc32c, ImplementationNameIsReported) {
  const auto name = Crc32cImplementationName();
  EXPECT_FALSE(name.empty());
  if (Crc32cHardwareAvailable()) {
    EXPECT_NE(name.find("hardware"), std::string_view::npos);
  } else {
    EXPECT_EQ(name, "software-slice-by-8");
  }
}

}  // namespace
}  // namespace pulselog
