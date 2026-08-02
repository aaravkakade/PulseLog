#include <string>

#include "test_support/temp_dir.h"
#include <gtest/gtest.h>

#include "pulselog/base/config.h"

namespace pulselog {
namespace {

TEST(Config, ParsesSectionsCommentsAndQuotes) {
  testing::TempFile file(R"(
# broker identity
[broker]
id = 3
host = "127.0.0.1"

[storage]
segment.bytes = 64MB
flush.interval = 5ms
)");

  ConfigStore config;
  ASSERT_TRUE(config.LoadFile(file.path()).ok());

  EXPECT_EQ(config.GetString("broker.host", ""), "127.0.0.1");
  auto id = config.GetInt("broker.id", -1);
  ASSERT_TRUE(id.ok());
  EXPECT_EQ(id.value(), 3);

  auto seg = config.GetBytes("storage.segment.bytes", 0);
  ASSERT_TRUE(seg.ok());
  EXPECT_EQ(seg.value(), 64LL * 1024 * 1024);

  auto flush = config.GetDurationMs("storage.flush.interval", -1);
  ASSERT_TRUE(flush.ok());
  EXPECT_EQ(flush.value(), 5);
}

TEST(Config, MissingFileIsNotFound) {
  ConfigStore config;
  EXPECT_EQ(config.LoadFile("/nonexistent/pulselog.conf").code(), ErrorCode::kNotFound);
}

TEST(Config, MalformedLineIsRejected) {
  testing::TempFile file("this line has no equals sign\n");
  ConfigStore config;
  const Status status = config.LoadFile(file.path());
  EXPECT_EQ(status.code(), ErrorCode::kInvalidArgument);
}

TEST(Config, CommandLineOverridesFile) {
  testing::TempFile file("[broker]\nid = 1\n");
  ConfigStore config;
  ASSERT_TRUE(config.LoadFile(file.path()).ok());

  const char* argv[] = {
      "pulselog-broker", "--broker.id=9", "--net.port=9092", "--verbose", "start"};
  const auto positional = config.LoadCommandLine(5, argv);

  ASSERT_EQ(positional.size(), 1U);
  EXPECT_EQ(positional[0], "start");
  EXPECT_EQ(config.GetInt("broker.id", 0).value(), 9);
  EXPECT_EQ(config.GetInt("net.port", 0).value(), 9092);
  EXPECT_TRUE(config.GetBool("verbose", false).value()) << "bare --flag must mean true";
}

TEST(Config, TypedGettersRejectGarbage) {
  ConfigStore config;
  config.Set("a.number", "twelve");
  config.Set("a.bool", "maybe");
  config.Set("a.bytes", "10 potatoes");

  EXPECT_EQ(config.GetInt("a.number", 0).status().code(), ErrorCode::kInvalidArgument);
  EXPECT_EQ(config.GetBool("a.bool", false).status().code(), ErrorCode::kInvalidArgument);
  EXPECT_EQ(config.GetBytes("a.bytes", 0).status().code(), ErrorCode::kInvalidArgument);
}

TEST(Config, DefaultsUsedWhenAbsent) {
  ConfigStore config;
  EXPECT_EQ(config.GetInt("missing", 17).value(), 17);
  EXPECT_EQ(config.GetString("missing", "fallback"), "fallback");
  EXPECT_TRUE(config.GetBool("missing", true).value());
  EXPECT_EQ(config.GetBytes("missing", 1024).value(), 1024);
}

TEST(Config, ByteSuffixes) {
  ConfigStore config;
  config.Set("k", "8K");
  config.Set("kb", "8KB");
  config.Set("mib", "2MiB");
  config.Set("g", "1G");
  config.Set("plain", "512");

  EXPECT_EQ(config.GetBytes("k", 0).value(), 8192);
  EXPECT_EQ(config.GetBytes("kb", 0).value(), 8192);
  EXPECT_EQ(config.GetBytes("mib", 0).value(), 2LL * 1024 * 1024);
  EXPECT_EQ(config.GetBytes("g", 0).value(), 1024LL * 1024 * 1024);
  EXPECT_EQ(config.GetBytes("plain", 0).value(), 512);
}

TEST(Config, DurationSuffixes) {
  ConfigStore config;
  config.Set("a", "250ms");
  config.Set("b", "2s");
  config.Set("c", "3m");
  config.Set("d", "1h");
  config.Set("e", "40");

  EXPECT_EQ(config.GetDurationMs("a", 0).value(), 250);
  EXPECT_EQ(config.GetDurationMs("b", 0).value(), 2000);
  EXPECT_EQ(config.GetDurationMs("c", 0).value(), 180'000);
  EXPECT_EQ(config.GetDurationMs("d", 0).value(), 3'600'000);
  EXPECT_EQ(config.GetDurationMs("e", 0).value(), 40);
}

TEST(Config, ListParsing) {
  ConfigStore config;
  config.Set("cluster.brokers", "1@host-a:9092, 2@host-b:9092 ,3@host-c:9092");
  const auto list = config.GetList("cluster.brokers");
  ASSERT_EQ(list.size(), 3U);
  EXPECT_EQ(list[0], "1@host-a:9092");
  EXPECT_EQ(list[1], "2@host-b:9092");
  EXPECT_EQ(list[2], "3@host-c:9092");
}

TEST(Config, RequireStringReportsMissingKey) {
  ConfigStore config;
  const auto missing = config.RequireString("broker.data.dir");
  ASSERT_FALSE(missing.ok());
  EXPECT_NE(missing.status().message().find("broker.data.dir"), std::string::npos);
}

}  // namespace
}  // namespace pulselog
