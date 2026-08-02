#include <cerrno>
#include <string>

#include <gtest/gtest.h>

#include "pulselog/base/status.h"

namespace pulselog {
namespace {

TEST(Status, DefaultIsOk) {
  Status s;
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(s.code(), ErrorCode::kOk);
  EXPECT_EQ(s.ToString(), "OK");
}

TEST(Status, CarriesCodeAndMessage) {
  const Status s = InvalidArgument("partition count must be positive");
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(s.code(), ErrorCode::kInvalidArgument);
  EXPECT_EQ(s.ToString(), "INVALID_ARGUMENT: partition count must be positive");
}

TEST(Status, WithContextPreservesCode) {
  const Status s = Corruption("crc mismatch").WithContext("recovering segment 12");
  EXPECT_EQ(s.code(), ErrorCode::kCorruption);
  EXPECT_EQ(s.message(), "recovering segment 12: crc mismatch");
}

TEST(Status, WithContextOnOkIsNoop) {
  const Status s = OkStatus().WithContext("ignored");
  EXPECT_TRUE(s.ok());
}

TEST(Status, ErrorCodeNamesAreUniqueAndStable) {
  // The names are used as metric label values, so a duplicate would silently
  // merge two error classes in dashboards.
  std::set<std::string_view> seen;
  for (std::uint16_t raw = 0; raw <= static_cast<std::uint16_t>(ErrorCode::kInternal); ++raw) {
    const auto name = ErrorCodeName(static_cast<ErrorCode>(raw));
    EXPECT_NE(name, "UNKNOWN_CODE") << "code " << raw << " has no name";
    EXPECT_TRUE(seen.insert(name).second) << "duplicate name " << name;
  }
}

TEST(Status, RetryClassification) {
  EXPECT_TRUE(IsRetryable(ErrorCode::kNotLeader));
  EXPECT_TRUE(IsRetryable(ErrorCode::kBackpressure));
  EXPECT_TRUE(IsRetryable(ErrorCode::kTimeout));
  EXPECT_FALSE(IsRetryable(ErrorCode::kInvalidArgument));
  EXPECT_FALSE(IsRetryable(ErrorCode::kCorruption));
  EXPECT_FALSE(IsRetryable(ErrorCode::kOk));
}

TEST(Status, ErrnoMapping) {
  EXPECT_EQ(ErrnoToStatus("open", ENOENT).code(), ErrorCode::kNotFound);
  EXPECT_EQ(ErrnoToStatus("write", ENOSPC).code(), ErrorCode::kResourceExhausted);
  EXPECT_EQ(ErrnoToStatus("read", EAGAIN).code(), ErrorCode::kWouldBlock);
  EXPECT_EQ(ErrnoToStatus("write", EPIPE).code(), ErrorCode::kClosed);
  const Status s = ErrnoToStatus("fsync", EIO);
  EXPECT_EQ(s.code(), ErrorCode::kIoError);
  EXPECT_NE(s.message().find("fsync"), std::string::npos);
  EXPECT_NE(s.message().find("errno=5"), std::string::npos);
}

TEST(Result, HoldsValue) {
  Result<int> r = 42;
  ASSERT_TRUE(r.ok());
  EXPECT_EQ(r.value(), 42);
  EXPECT_EQ(*r, 42);
  EXPECT_EQ(r.code(), ErrorCode::kOk);
}

TEST(Result, HoldsError) {
  Result<int> r = NotFound("topic");
  ASSERT_FALSE(r.ok());
  EXPECT_EQ(r.status().code(), ErrorCode::kNotFound);
  EXPECT_EQ(r.value_or(-1), -1);
}

TEST(Result, MoveOnlyPayload) {
  Result<std::unique_ptr<int>> r = std::make_unique<int>(7);
  ASSERT_TRUE(r.ok());
  auto ptr = std::move(r).value();
  EXPECT_EQ(*ptr, 7);
}

TEST(Result, OkStatusWithoutValueBecomesInternal) {
  // Guards against a caller writing `return OkStatus();` from a Result<T>
  // function, which would otherwise produce a value-less "success".
  Result<int> r = OkStatus();
  EXPECT_FALSE(r.ok());
  EXPECT_EQ(r.status().code(), ErrorCode::kInternal);
}

Status FailingStep() {
  return Unavailable("broker draining");
}

Result<int> FailingValueStep() {
  return TimedOut("deadline");
}

Status UsesReturnIfError() {
  PL_RETURN_IF_ERROR(FailingStep());
  return OkStatus();
}

Result<int> UsesAssignOrReturn() {
  PL_ASSIGN_OR_RETURN(const int v, FailingValueStep());
  return v * 2;
}

Result<int> AssignOrReturnSuccess() {
  PL_ASSIGN_OR_RETURN(const int v, Result<int>(21));
  return v * 2;
}

TEST(Macros, PropagateErrors) {
  EXPECT_EQ(UsesReturnIfError().code(), ErrorCode::kUnavailable);
  EXPECT_EQ(UsesAssignOrReturn().status().code(), ErrorCode::kTimeout);
  auto ok = AssignOrReturnSuccess();
  ASSERT_TRUE(ok.ok());
  EXPECT_EQ(ok.value(), 42);
}

}  // namespace
}  // namespace pulselog
