// A framed connection.
//
// Owns a socket, a read buffer, and a queue of outbound responses. Handles
// partial reads and partial writes, batches queued responses into a single
// writev, and applies backpressure in both directions.
//
// Backpressure, precisely
// -----------------------
// Two independent limits, both bounded and both observable:
//
//  * **Output high-water mark.** When queued response bytes exceed
//    `output_high_water_bytes`, the connection stops watching for readability.
//    A client that pipelines faster than it drains therefore stops being read
//    from, TCP's receive window closes, and the pressure propagates to the
//    client. Reading resumes below `output_low_water_bytes`. Hysteresis avoids
//    flapping the poller registration on every write.
//
//  * **Hard output ceiling.** If queued bytes still exceed
//    `output_max_bytes` -- a peer that has stopped reading entirely -- the
//    connection is closed. Memory used by one connection is bounded by this
//    number, full stop.
//
// Neither limit involves an unbounded buffer, and neither drops a response
// silently: a closed connection is counted and logged with its reason.
#ifndef PULSELOG_NET_CONNECTION_H_
#define PULSELOG_NET_CONNECTION_H_

#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <string>

#include "pulselog/base/buffer.h"
#include "pulselog/net/event_loop.h"
#include "pulselog/net/socket.h"
#include "pulselog/protocol/frame.h"

namespace pulselog::net {

class Connection;

// Invoked on the loop thread for each complete, checksum-verified frame.
// The payload view is valid only for the duration of the call.
using FrameCallback = std::function<void(Connection&, const protocol::FrameDecoder::Frame&)>;

// Invoked on the loop thread just before the connection is destroyed.
using CloseCallback = std::function<void(Connection&, const Status&)>;

struct ConnectionOptions {
  std::size_t read_chunk_bytes = 64 * 1024;
  std::size_t output_high_water_bytes = 4 * 1024 * 1024;
  std::size_t output_low_water_bytes = 1 * 1024 * 1024;
  std::size_t output_max_bytes = 64 * 1024 * 1024;
  std::uint32_t max_frame_bytes = protocol::kMaxPayloadBytesDefault;
  // A connection with no traffic for this long is closed. 0 disables.
  std::int64_t idle_timeout_ms = 0;
  // Frames handled per readable event before yielding back to the loop, so one
  // busy connection cannot starve the others.
  std::size_t max_frames_per_event = 64;
};

class Connection final : public EventHandler {
 public:
  using Id = std::uint64_t;

  Connection(Id id,
             TcpSocket socket,
             EventLoop& loop,
             BufferPool& pool,
             ConnectionOptions options,
             FrameCallback on_frame,
             CloseCallback on_close);

  ~Connection() override;

  // --- EventHandler ---------------------------------------------------------
  void OnReadable() override;
  void OnWritable() override;
  void OnClosed() override;

  [[nodiscard]] int fd() const override { return socket_.fd(); }

  // --- loop thread only -----------------------------------------------------

  // Queues an encoded frame. Returns false when the hard ceiling is hit, in
  // which case the connection is scheduled for closure.
  [[nodiscard]] bool SendFrame(protocol::OpCode opcode,
                               RequestId request_id,
                               std::uint16_t flags,
                               ByteSpan payload);

  // Queues raw pre-framed bytes. Used by paths that build the frame elsewhere.
  [[nodiscard]] bool SendRaw(ByteSpan framed);

  // Closes after the current output queue drains.
  void CloseWhenDrained();

  // Closes immediately, discarding queued output.
  void CloseNow(Status reason);

  [[nodiscard]] Id id() const noexcept { return id_; }

  [[nodiscard]] const Endpoint& peer() const noexcept { return peer_; }

  // The loop that owns this connection. Used to post responses back onto the
  // right thread from a worker.
  [[nodiscard]] EventLoop& loop() const noexcept { return loop_; }

  [[nodiscard]] std::size_t PendingOutputBytes() const noexcept { return output_bytes_; }

  [[nodiscard]] bool read_paused() const noexcept { return read_paused_; }

  [[nodiscard]] std::int64_t last_activity_ms() const noexcept { return last_activity_ms_; }

  // True once the connection has been scheduled for closure.
  [[nodiscard]] bool closing() const noexcept { return closing_; }

  struct Stats {
    std::uint64_t frames_received = 0;
    std::uint64_t frames_sent = 0;
    std::uint64_t bytes_received = 0;
    std::uint64_t bytes_sent = 0;
    std::uint64_t writev_calls = 0;
    std::uint64_t partial_writes = 0;
    std::uint64_t read_pauses = 0;
  };

  [[nodiscard]] const Stats& stats() const noexcept { return stats_; }

  // Attaches caller-owned state (the broker's per-connection session).
  void SetUserData(std::shared_ptr<void> data) { user_data_ = std::move(data); }

  [[nodiscard]] const std::shared_ptr<void>& user_data() const noexcept { return user_data_; }

 private:
  void FlushOutput();

  void UpdatePollRegistration();

  // One queued outbound buffer plus how much of it has already gone out.
  struct OutputChunk {
    PooledBuffer buffer;
    std::size_t sent = 0;
  };

  Id id_;
  TcpSocket socket_;
  EventLoop& loop_;
  BufferPool& pool_;
  ConnectionOptions options_;
  FrameCallback on_frame_;
  CloseCallback on_close_;
  Endpoint peer_;

  ByteBuffer input_;
  protocol::FrameDecoder decoder_;
  std::deque<OutputChunk> output_;
  std::size_t output_bytes_ = 0;

  bool read_paused_ = false;
  bool want_write_ = false;
  bool closing_ = false;
  bool close_when_drained_ = false;
  Status close_reason_;

  std::int64_t last_activity_ms_ = 0;
  Stats stats_;
  std::shared_ptr<void> user_data_;
};

}  // namespace pulselog::net

#endif  // PULSELOG_NET_CONNECTION_H_
