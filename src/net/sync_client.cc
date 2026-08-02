#include "pulselog/net/sync_client.h"

#include <algorithm>
#include <cerrno>

#include <poll.h>

#include "pulselog/base/clock.h"

namespace pulselog::net {

Status SyncClient::Connect(const Endpoint& endpoint) {
  Close();
  PL_ASSIGN_OR_RETURN(socket_,
                      TcpSocket::ConnectWithTimeout(endpoint, options_.connect_timeout_ms));
  PL_RETURN_IF_ERROR(socket_.SetNoDelay(true));
  endpoint_ = endpoint;
  input_.Clear();
  decoder_.Reset();
  consumed_ = 0;
  return OkStatus();
}

void SyncClient::Close() {
  socket_.Close();
  input_.Clear();
  decoder_.Reset();
  consumed_ = 0;
}

Status SyncClient::WaitFor(bool readable, std::int64_t deadline_nanos) {
  const std::int64_t remaining_nanos = deadline_nanos - MonotonicNanos();
  if (remaining_nanos <= 0) {
    return TimedOut("deadline exceeded waiting for " + std::string(readable ? "read" : "write") +
                    " on " + endpoint_.ToString());
  }

  ::pollfd pfd{};
  pfd.fd = socket_.fd();
  pfd.events = readable ? POLLIN : POLLOUT;
  const int timeout_ms = static_cast<int>(std::max<std::int64_t>(1, remaining_nanos / 1'000'000));

  int ready = 0;
  do {
    ready = ::poll(&pfd, 1, timeout_ms);
  } while (ready < 0 && errno == EINTR);

  if (ready < 0) return ErrnoToStatus("poll", errno);
  if (ready == 0) {
    return TimedOut("timed out waiting for " + std::string(readable ? "read" : "write") + " on " +
                    endpoint_.ToString());
  }
  return OkStatus();
}

Status SyncClient::SendRequest(protocol::OpCode opcode, RequestId request_id, ByteSpan payload) {
  if (!socket_.valid()) return Status{ErrorCode::kClosed, "client is not connected"};

  scratch_.Clear();
  protocol::EncodeFrame(scratch_, opcode, request_id, 0, payload);

  const std::int64_t deadline = MonotonicNanos() + options_.request_timeout_ms * 1'000'000;
  ByteSpan remaining = scratch_.Readable();
  while (!remaining.empty()) {
    auto transfer = socket_.Write(remaining);
    if (!transfer.ok()) {
      Close();
      return transfer.status();
    }
    if (transfer->eof) {
      Close();
      return Status{ErrorCode::kClosed, "broker closed the connection during send"};
    }
    if (transfer->would_block) {
      const Status status = WaitFor(/*readable=*/false, deadline);
      if (!status.ok()) return status;
      continue;
    }
    remaining = remaining.subspan(transfer->bytes);
  }
  last_send_nanos_ = MonotonicNanos();
  return OkStatus();
}

Result<protocol::FrameDecoder::Frame> SyncClient::ReadResponse(RequestId expected_request_id) {
  if (!socket_.valid()) return Status{ErrorCode::kClosed, "client is not connected"};

  // Drop the frame returned by the previous call before reusing the buffer.
  if (consumed_ > 0) {
    input_.Consume(consumed_);
    consumed_ = 0;
  }

  const std::int64_t deadline = MonotonicNanos() + options_.request_timeout_ms * 1'000'000;
  for (;;) {
    protocol::FrameDecoder::Frame frame;
    const auto state = decoder_.Next(input_.Readable(), frame);
    if (state == protocol::FrameDecoder::State::kError) {
      const Status error = decoder_.error();
      Close();
      return error;
    }
    if (state == protocol::FrameDecoder::State::kFrame) {
      if (frame.header.request_id == expected_request_id) {
        consumed_ = frame.total_size;
        last_round_trip_nanos_ = MonotonicNanos() - last_send_nanos_;
        return frame;
      }
      // A response to a request we already gave up on. Skip it rather than
      // returning a mismatched correlation ID to the caller.
      input_.Consume(frame.total_size);
      continue;
    }

    PL_RETURN_IF_ERROR(WaitFor(/*readable=*/true, deadline));
    auto transfer = socket_.ReadInto(input_, options_.read_chunk_bytes);
    if (!transfer.ok()) {
      Close();
      return transfer.status();
    }
    if (transfer->eof) {
      Close();
      return Status{ErrorCode::kClosed, "broker closed the connection"};
    }
  }
}

Result<protocol::FrameDecoder::Frame> SyncClient::Call(protocol::OpCode opcode,
                                                       RequestId request_id,
                                                       ByteSpan payload) {
  PL_RETURN_IF_ERROR(SendRequest(opcode, request_id, payload));
  return ReadResponse(request_id);
}

}  // namespace pulselog::net
