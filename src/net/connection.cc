#include "pulselog/net/connection.h"

#include <algorithm>
#include <array>

#include "pulselog/base/logging.h"

namespace pulselog::net {
namespace {

constexpr std::string_view kComponent = "net.conn";

}  // namespace

Connection::Connection(Id id,
                       TcpSocket socket,
                       EventLoop& loop,
                       BufferPool& pool,
                       ConnectionOptions options,
                       FrameCallback on_frame,
                       CloseCallback on_close)
    : id_(id),
      socket_(std::move(socket)),
      loop_(loop),
      pool_(pool),
      options_(options),
      on_frame_(std::move(on_frame)),
      on_close_(std::move(on_close)),
      decoder_(options.max_frame_bytes),
      last_activity_ms_(loop.NowMillis()) {
  auto peer = socket_.PeerEndpoint();
  if (peer.ok()) peer_ = std::move(peer).value();
  input_.Reserve(options_.read_chunk_bytes);

  // Socket buffer sizing is best-effort: the kernel is free to clamp or round
  // whatever it is asked for, and a failure here is a tuning miss rather than
  // a correctness problem.
  if (options_.send_buffer_bytes > 0) {
    const Status status = socket_.SetSendBufferSize(options_.send_buffer_bytes);
    if (!status.ok()) {
      PL_DEBUG(kComponent) << "could not set SO_SNDBUF: " << status.ToString();
    }
  }
  if (options_.receive_buffer_bytes > 0) {
    const Status status = socket_.SetReceiveBufferSize(options_.receive_buffer_bytes);
    if (!status.ok()) {
      PL_DEBUG(kComponent) << "could not set SO_RCVBUF: " << status.ToString();
    }
  }
}

Connection::~Connection() = default;

void Connection::OnReadable() {
  if (closing_) return;
  last_activity_ms_ = loop_.NowMillis();

  auto transfer = socket_.ReadInto(input_, options_.read_chunk_bytes);
  if (!transfer.ok()) {
    CloseNow(transfer.status().WithContext("reading from " + peer_.ToString()));
    return;
  }
  if (transfer->eof) {
    // Deliver whatever complete frames already arrived before tearing down, so
    // a client that writes-then-closes still gets its work done.
    // (Falls through to the decode loop below with `eof` recorded.)
    if (input_.Empty()) {
      CloseNow(Status{ErrorCode::kClosed, "peer closed the connection"});
      return;
    }
  }
  if (transfer->bytes > 0) {
    stats_.bytes_received += transfer->bytes;
  }

  std::size_t frames = 0;
  while (frames < options_.max_frames_per_event) {
    protocol::FrameDecoder::Frame frame;
    const auto state = decoder_.Next(input_.Readable(), frame);
    if (state == protocol::FrameDecoder::State::kNeedMore) break;
    if (state == protocol::FrameDecoder::State::kError) {
      // A framing error means the byte stream can no longer be trusted; there
      // is no safe place to resynchronise.
      CloseNow(decoder_.error().WithContext("framing error from " + peer_.ToString()));
      return;
    }

    ++frames;
    ++stats_.frames_received;
    on_frame_(*this, frame);
    input_.Consume(frame.total_size);
    if (closing_) return;
  }

  if (transfer->eof) {
    // Everything decodable has been handled; nothing more will arrive.
    CloseWhenDrained();
    return;
  }

  // Reclaim capacity from a buffer that a single large frame inflated.
  if (input_.Empty() && input_.Capacity() > options_.read_chunk_bytes * 4) {
    input_.Shrink(options_.read_chunk_bytes);
  }
  UpdatePollRegistration();
}

void Connection::OnWritable() {
  if (socket_.fd() < 0) return;
  FlushOutput();
}

void Connection::OnClosed() {
  if (on_close_) on_close_(*this, close_reason_);
  output_.clear();
  output_bytes_ = 0;
  socket_.Close();
}

bool Connection::SendFrame(protocol::OpCode opcode,
                           RequestId request_id,
                           std::uint16_t flags,
                           ByteSpan payload) {
  if (closing_) return false;

  auto buffer = pool_.Acquire(protocol::kFrameHeaderSize + payload.size());
  protocol::EncodeFrame(*buffer, opcode, request_id, flags, payload);
  const std::size_t size = buffer->ReadableBytes();

  if (output_bytes_ + size > options_.output_max_bytes) {
    pool_.Release(std::move(buffer));
    PL_WARN(kComponent) << "closing connection: output ceiling exceeded"
                        << " conn=" << id_ << " peer=" << peer_.ToString()
                        << " queued_bytes=" << output_bytes_
                        << " limit_bytes=" << options_.output_max_bytes;
    CloseNow(Status{ErrorCode::kResourceExhausted,
                    "peer is not draining; output queue exceeded " +
                        std::to_string(options_.output_max_bytes) + " bytes"});
    return false;
  }

  output_bytes_ += size;
  output_.push_back(OutputChunk{PooledBuffer(pool_, std::move(buffer)), 0});
  ++stats_.frames_sent;

  FlushOutput();
  return true;
}

bool Connection::SendRaw(ByteSpan framed) {
  if (closing_ || framed.empty()) return false;

  auto buffer = pool_.Acquire(framed.size());
  buffer->Append(framed);
  const std::size_t size = buffer->ReadableBytes();

  if (output_bytes_ + size > options_.output_max_bytes) {
    pool_.Release(std::move(buffer));
    CloseNow(Status{ErrorCode::kResourceExhausted, "output queue ceiling exceeded"});
    return false;
  }

  output_bytes_ += size;
  output_.push_back(OutputChunk{PooledBuffer(pool_, std::move(buffer)), 0});
  FlushOutput();
  return true;
}

void Connection::FlushOutput() {
  while (!output_.empty()) {
    // Coalesce as many queued buffers as one writev can carry. This is the
    // difference between one syscall per pipelined response and one syscall
    // per batch of them; see the writev results in PERFORMANCE_RESULTS.md.
    std::array<ByteSpan, kMaxWriteChunks> chunks{};
    std::size_t chunk_count = 0;
    for (const auto& chunk : output_) {
      if (chunk_count == chunks.size()) break;
      const ByteSpan readable = chunk.buffer.get()->Readable();
      chunks[chunk_count++] = readable.subspan(chunk.sent);
    }

    auto transfer = socket_.WriteVectored(std::span<const ByteSpan>(chunks.data(), chunk_count));
    if (!transfer.ok()) {
      CloseNow(transfer.status().WithContext("writing to " + peer_.ToString()));
      return;
    }
    if (transfer->eof) {
      CloseNow(Status{ErrorCode::kClosed, "peer closed while we were writing"});
      return;
    }
    ++stats_.writev_calls;

    if (transfer->would_block || transfer->bytes == 0) {
      want_write_ = true;
      ++stats_.partial_writes;
      UpdatePollRegistration();
      return;
    }

    // Consume whole buffers, then partially consume the one we stopped in.
    std::size_t remaining = transfer->bytes;
    stats_.bytes_sent += transfer->bytes;
    output_bytes_ -= transfer->bytes;
    while (remaining > 0 && !output_.empty()) {
      OutputChunk& front = output_.front();
      const std::size_t pending = front.buffer.get()->ReadableBytes() - front.sent;
      if (remaining >= pending) {
        remaining -= pending;
        output_.pop_front();
      } else {
        front.sent += remaining;
        remaining = 0;
        ++stats_.partial_writes;
      }
    }
    last_activity_ms_ = loop_.NowMillis();
  }

  want_write_ = false;
  if (close_when_drained_ && output_.empty()) {
    CloseNow(close_reason_.ok() ? Status{ErrorCode::kClosed, "closed after draining"}
                                : close_reason_);
    return;
  }
  UpdatePollRegistration();
}

void Connection::UpdatePollRegistration() {
  if (closing_) return;

  // Hysteresis: pause reading above the high-water mark, resume below the low
  // one. Toggling at a single threshold would re-register the descriptor on
  // nearly every write once the queue hovers around it.
  const bool should_pause = output_bytes_ >= options_.output_high_water_bytes;
  const bool should_resume = output_bytes_ <= options_.output_low_water_bytes;
  if (!read_paused_ && should_pause) {
    read_paused_ = true;
    ++stats_.read_pauses;
    PL_DEBUG(kComponent) << "pausing reads (backpressure)"
                         << " conn=" << id_ << " queued_bytes=" << output_bytes_;
  } else if (read_paused_ && should_resume) {
    read_paused_ = false;
  }

  EventMask desired = EventMask::kNone;
  if (!read_paused_) desired = desired | EventMask::kRead;
  if (want_write_) desired = desired | EventMask::kWrite;

  const Status status = loop_.UpdateEvents(socket_.fd(), desired);
  if (!status.ok()) {
    PL_DEBUG(kComponent) << "poller update failed conn=" << id_ << ": " << status.ToString();
  }
}

void Connection::CloseWhenDrained() {
  if (closing_) return;
  close_when_drained_ = true;
  if (output_.empty()) {
    CloseNow(Status{ErrorCode::kClosed, "closed after draining"});
  }
}

void Connection::CloseNow(Status reason) {
  if (closing_) return;
  closing_ = true;
  close_reason_ = std::move(reason);
  loop_.CloseHandler(socket_.fd());
}

}  // namespace pulselog::net
