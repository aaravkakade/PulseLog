// Codecs for the data-plane messages: produce, fetch, list-offsets.
#include "pulselog/protocol/messages.h"

namespace pulselog::protocol {

void ResponseHeader::Encode(PayloadWriter& w) const {
  w.PutU16(static_cast<std::uint16_t>(error));
  w.PutString(error_message);
}

bool ResponseHeader::Decode(PayloadReader& r) {
  std::uint16_t raw = 0;
  if (!r.GetU16(raw)) return false;
  // Unknown codes from a newer broker degrade to kUnknown rather than failing
  // the whole decode -- the message text still reaches the operator.
  error = raw <= static_cast<std::uint16_t>(ErrorCode::kInternal) ? static_cast<ErrorCode>(raw)
                                                                  : ErrorCode::kUnknown;
  return r.GetString(error_message);
}

// --- Produce ---------------------------------------------------------------

void ProduceRequest::Encode(PayloadWriter& w) const {
  w.PutString(topic);
  w.PutI32(partition.value());
  w.PutU8(static_cast<std::uint8_t>(acks));
  w.PutI32(timeout_ms);
  w.PutU32(record_count);
  w.PutU32(static_cast<std::uint32_t>(records.size()));
  w.PutRaw(records);
}

bool ProduceRequest::Decode(PayloadReader& r) {
  std::int32_t partition_raw = 0;
  std::uint8_t acks_raw = 0;
  std::uint32_t records_len = 0;
  if (!r.GetString(topic)) return false;
  if (!r.GetI32(partition_raw)) return false;
  if (!r.GetU8(acks_raw)) return false;
  if (!r.GetI32(timeout_ms)) return false;
  if (!r.GetU32(record_count)) return false;
  if (!r.GetU32(records_len)) return false;
  if (r.Remaining() < records_len) return false;

  if (acks_raw > static_cast<std::uint8_t>(AckMode::kQuorum)) return false;
  if (partition_raw < 0) return false;

  partition = PartitionIndex{partition_raw};
  acks = static_cast<AckMode>(acks_raw);
  records = r.GetRemaining().subspan(0, records_len);
  return true;
}

void ProduceResponse::Encode(PayloadWriter& w) const {
  header.Encode(w);
  w.PutI64(base_offset);
  w.PutI64(last_offset);
  w.PutI64(append_time);
  w.PutI64(high_water_mark);
}

bool ProduceResponse::Decode(PayloadReader& r) {
  if (!header.Decode(r)) return false;
  if (!r.GetI64(base_offset)) return false;
  if (!r.GetI64(last_offset)) return false;
  if (!r.GetI64(append_time)) return false;
  return r.GetI64(high_water_mark);
}

// --- Fetch -----------------------------------------------------------------

void FetchRequest::Encode(PayloadWriter& w) const {
  w.PutString(topic);
  w.PutI32(partition.value());
  w.PutI64(fetch_offset);
  w.PutU32(max_bytes);
  w.PutU32(min_bytes);
  w.PutI32(max_wait_ms);
  w.PutU8(static_cast<std::uint8_t>(isolation));
}

bool FetchRequest::Decode(PayloadReader& r) {
  std::int32_t partition_raw = 0;
  std::uint8_t isolation_raw = 0;
  if (!r.GetString(topic)) return false;
  if (!r.GetI32(partition_raw)) return false;
  if (!r.GetI64(fetch_offset)) return false;
  if (!r.GetU32(max_bytes)) return false;
  if (!r.GetU32(min_bytes)) return false;
  if (!r.GetI32(max_wait_ms)) return false;
  if (!r.GetU8(isolation_raw)) return false;

  if (partition_raw < 0) return false;
  if (isolation_raw > static_cast<std::uint8_t>(IsolationLevel::kReadReplicated)) return false;

  partition = PartitionIndex{partition_raw};
  isolation = static_cast<IsolationLevel>(isolation_raw);
  return true;
}

void FetchResponse::Encode(PayloadWriter& w) const {
  header.Encode(w);
  w.PutI64(high_water_mark);
  w.PutI64(log_start_offset);
  w.PutI64(base_offset);
  w.PutU32(record_count);
  w.PutU32(static_cast<std::uint32_t>(records.size()));
  w.PutRaw(records);
}

bool FetchResponse::Decode(PayloadReader& r) {
  std::uint32_t records_len = 0;
  if (!header.Decode(r)) return false;
  if (!r.GetI64(high_water_mark)) return false;
  if (!r.GetI64(log_start_offset)) return false;
  if (!r.GetI64(base_offset)) return false;
  if (!r.GetU32(record_count)) return false;
  if (!r.GetU32(records_len)) return false;
  if (r.Remaining() < records_len) return false;
  records = r.GetRemaining().subspan(0, records_len);
  return true;
}

// --- ListOffsets -----------------------------------------------------------

void ListOffsetsRequest::Encode(PayloadWriter& w) const {
  w.PutString(topic);
  w.PutI32(partition.value());
  w.PutI64(timestamp);
}

bool ListOffsetsRequest::Decode(PayloadReader& r) {
  std::int32_t partition_raw = 0;
  if (!r.GetString(topic)) return false;
  if (!r.GetI32(partition_raw)) return false;
  if (partition_raw < 0) return false;
  partition = PartitionIndex{partition_raw};
  return r.GetI64(timestamp);
}

void ListOffsetsResponse::Encode(PayloadWriter& w) const {
  header.Encode(w);
  w.PutI64(offset);
  w.PutI64(timestamp);
}

bool ListOffsetsResponse::Decode(PayloadReader& r) {
  if (!header.Decode(r)) return false;
  if (!r.GetI64(offset)) return false;
  return r.GetI64(timestamp);
}

void EncodeErrorResponse(ByteBuffer& out, ErrorCode code, std::string_view message) {
  PayloadWriter w(out);
  ResponseHeader header;
  header.error = code;
  header.error_message = std::string(message);
  header.Encode(w);
}

}  // namespace pulselog::protocol
