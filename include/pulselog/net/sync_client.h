// A blocking, framed client connection.
//
// The event loop is the right structure for a broker serving thousands of
// peers. It is the wrong structure for a producer thread that wants to send a
// batch and wait for the acknowledgement, or for a replication fetcher whose
// entire job is one outstanding request at a time. Those get this instead:
// one socket, one thread, explicit deadlines, no callbacks.
//
// Not thread-safe. One instance per thread.
#ifndef PULSELOG_NET_SYNC_CLIENT_H_
#define PULSELOG_NET_SYNC_CLIENT_H_

#include <cstdint>
#include <memory>
#include <string>

#include "pulselog/base/buffer.h"
#include "pulselog/base/status.h"
#include "pulselog/net/socket.h"
#include "pulselog/protocol/frame.h"

namespace pulselog::net {

struct SyncClientOptions {
  std::int64_t connect_timeout_ms = 5000;
  std::int64_t request_timeout_ms = 10'000;
  std::uint32_t max_frame_bytes = protocol::kMaxPayloadBytesDefault;
  std::size_t read_chunk_bytes = std::size_t{64} * 1024;
};

class SyncClient {
 public:
  explicit SyncClient(SyncClientOptions options = {})
      : options_(options), decoder_(options.max_frame_bytes) {}

  SyncClient(const SyncClient&) = delete;
  SyncClient& operator=(const SyncClient&) = delete;

  SyncClient(SyncClient&&) noexcept = default;
  SyncClient& operator=(SyncClient&&) noexcept = default;

  ~SyncClient() = default;

  [[nodiscard]] Status Connect(const Endpoint& endpoint);

  [[nodiscard]] bool connected() const noexcept { return socket_.valid(); }

  void Close();

  // Sends one request frame. Blocks until it is fully written or the deadline
  // passes.
  [[nodiscard]] Status SendRequest(protocol::OpCode opcode, RequestId request_id, ByteSpan payload);

  // Reads frames until one with `expected_request_id` arrives, discarding
  // earlier responses (which can only be stale ones from an abandoned
  // request). The returned payload view is valid until the next call.
  [[nodiscard]] Result<protocol::FrameDecoder::Frame> ReadResponse(RequestId expected_request_id);

  // Request/response in one call.
  [[nodiscard]] Result<protocol::FrameDecoder::Frame> Call(protocol::OpCode opcode,
                                                           RequestId request_id,
                                                           ByteSpan payload);

  [[nodiscard]] const Endpoint& endpoint() const noexcept { return endpoint_; }

  // Nanoseconds between the last request going out and its response arriving.
  [[nodiscard]] std::int64_t last_round_trip_nanos() const noexcept {
    return last_round_trip_nanos_;
  }

 private:
  // Waits for readability/writability with the configured deadline.
  [[nodiscard]] Status WaitFor(bool readable, std::int64_t deadline_nanos);

  SyncClientOptions options_;
  TcpSocket socket_;
  Endpoint endpoint_;
  ByteBuffer input_;
  ByteBuffer scratch_;
  protocol::FrameDecoder decoder_;
  std::size_t consumed_ = 0;
  std::int64_t last_send_nanos_ = 0;
  std::int64_t last_round_trip_nanos_ = 0;
};

}  // namespace pulselog::net

#endif  // PULSELOG_NET_SYNC_CLIENT_H_
