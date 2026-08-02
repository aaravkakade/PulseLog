# PulseLog Wire Protocol

Version 1. This document is normative: the implementation in
`include/pulselog/protocol/` follows it, and `tests/unit/test_frame.cc`,
`test_record.cc` and `test_messages.cc` enforce it.

## Conventions

* **Endianness.** Every multi-byte integer is little-endian. Both supported
  architectures (x86-64, AArch64) are little-endian, so the conversions in
  `base/endian.h` compile to nothing there; they are written against byte
  arrays rather than `reinterpret_cast` so the code stays correct and
  alignment-safe on a big-endian host.
* **Signedness.** `i16/i32/i64` are two's-complement signed; `u8/u16/u32/u64`
  unsigned. `varuint` is LEB128 (1–10 bytes). `varint` is LEB128 over a
  zig-zag-transformed value.
* **Strings** are `u16` length followed by that many UTF-8 bytes. No NUL
  terminators anywhere.
* **Byte arrays** are `u32` length followed by that many bytes. The length
  `0xFFFFFFFF` means "null", which is distinct from a zero-length array.
* **Arrays** are `u32` count followed by that many elements.

---

## 1. Frame

Every message in both directions is a frame.

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       magic  "PLSG"                           |  0
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|          version              |           opcode              |  4
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |  8
+                         request_id (u64)                      +
|                                                               | 12
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       payload_len (u32)                       | 16
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|          flags (u16)          |        reserved (u16)         | 20
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       payload_crc (u32)                       | 24
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       header_crc (u32)                        | 28
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                      payload (payload_len bytes)              | 32
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

| Field | Type | Notes |
|---|---|---|
| `magic` | `u32` | `0x47534C50`; bytes `P L S G` in stream order |
| `version` | `u16` | Currently `1` |
| `opcode` | `u16` | See §2 |
| `request_id` | `u64` | Chosen by the initiator; echoed verbatim in the response |
| `payload_len` | `u32` | Bytes of payload following the header |
| `flags` | `u16` | bit 0 `RESPONSE`, bit 1 `COMPRESSED`, bit 2 `MORE_FOLLOWS` |
| `reserved` | `u16` | Must be zero. A non-zero value is a hard error |
| `payload_crc` | `u32` | CRC-32C of the payload; `0` when `payload_len == 0` |
| `header_crc` | `u32` | CRC-32C of bytes `[0, 28)` |

### Why two checksums

The header carries its own CRC, checked before any other header field is
trusted. On a stream socket a corrupt `payload_len` is unrecoverable: the
decoder would resynchronise at the wrong byte and interpret payload bytes as a
header from then on, with no way to notice. Validating the header first turns
that into a clean, immediate connection reset.

The payload CRC is separate so that a large payload is checksummed once, after
it has fully arrived, instead of incrementally as bytes trickle in.

### Framing rules

* A receiver reads until it has 32 bytes, validates the header, then reads
  until it has `payload_len` more bytes.
* `payload_len` is checked against a configurable ceiling
  (`net.max_frame_bytes`, default 64 MiB) **before** any buffer is sized, so a
  hostile or corrupt length cannot drive the process out of memory.
* On any framing error the connection is closed. A stream protocol has no safe
  resynchronisation point, and the decoder deliberately latches into an error
  state rather than trying to find one (`FrameDecoder` stays in `kError` until
  explicitly `Reset()`).
* `request_id` values need only be unique among a connection's in-flight
  requests. A client that reuses one while the first is outstanding gets
  undefined correlation — the broker does not police this.

### Failure behaviour

| Condition | Result |
|---|---|
| Bad magic | `PROTOCOL_ERROR`, connection closed |
| Header CRC mismatch | `CORRUPTION`, connection closed |
| `reserved != 0` | `PROTOCOL_ERROR`, connection closed |
| Version outside `[1, 1]` | `UNSUPPORTED_VERSION`, connection closed |
| Unknown opcode | `PROTOCOL_ERROR`, connection closed |
| `payload_len` above the cap | `RESOURCE_EXHAUSTED`, connection closed |
| Payload CRC mismatch | `CORRUPTION`, connection closed |
| Payload fails to decode | `PROTOCOL_ERROR` response, connection **stays open** |

The last row is the important distinction: a malformed *payload* is a client
bug that the framing layer has already contained, so the broker answers with an
error and keeps serving. A malformed *frame* means the byte stream itself can
no longer be trusted.

---

## 2. Operations

Numbering is permanent. Codes are never reused or renumbered; retired
operations leave a gap.

| Code | Name | Direction |
|---|---|---|
| 1 | `PRODUCE` | client → broker |
| 2 | `FETCH` | client → broker |
| 3 | `LIST_OFFSETS` | client → broker |
| 20 | `CREATE_TOPIC` | client → broker |
| 21 | `METADATA` | client → broker |
| 22 | `COMMIT_OFFSET` | client → coordinator |
| 23 | `FETCH_OFFSET` | client → coordinator |
| 24 | `JOIN_GROUP` | client → coordinator |
| 25 | `HEARTBEAT` | client → coordinator |
| 26 | `LEAVE_GROUP` | client → coordinator |
| 27 | `HEALTH` | any → broker |
| 28 | `LIST_TOPICS` | client → broker |
| 40 | `REPLICATE` | leader → follower |
| 41 | `REPLICA_ACK` | follower → leader |
| 42 | `REPLICA_FETCH` | follower → leader |
| 60 | `DELETE_TOPIC` | client → broker |
| 61 | `DESCRIBE_CLUSTER` | client → broker |

Every **response** payload begins with:

```
error_code    u16   (see ErrorCode in base/status.h)
error_message string
```

so a client can surface a failure even for an operation whose success payload
it does not understand.

**An error response carries only the header.** When `error_code != 0` the
body is omitted entirely — there is no meaningful `base_offset` to report for
a produce that failed. A client must therefore decode the `ResponseHeader`
first and only run the typed body decoder when the code is `OK`. Running the
full decoder over an error response yields a truncation failure and would mask
the real error with `PROTOCOL_ERROR`.

---

## 3. Record format

One encoding is used both on the wire and on disk. A produce request carries
records with `offset = 0`; the leader assigns real offsets and stores the same
bytes; a fetch response ships those stored bytes back untouched. The broker
never re-encodes a record to serve a read.

```
offset  size      field        notes
------  --------  -----------  ---------------------------------------------
     0         4  length       u32, bytes following this field
     4         4  crc32c       CRC-32C over bytes [8, 4+length)
     8         8  offset       i64, absolute; 0 in a produce request
    16         8  timestamp    i64, milliseconds since the Unix epoch
    24         1  attributes   bits 0-1 compression, bit 2 tombstone
    25   varuint  key_len      0 = null key, else actual length + 1
     …   varuint  value_len
     …         …  key bytes
     …         …  value bytes
```

Fixed part: 25 bytes plus two varints. A record with a 4-byte key and a
100-byte value occupies 131 bytes, i.e. 27 bytes of framing.

Notes:

* The **length prefix is outside the CRC**. It is validated structurally
  instead (minimum size, maximum size, and the requirement that key and value
  lengths exactly fill the declared record). A corrupt length therefore fails
  before the CRC is even consulted.
* **`offset` is stored explicitly** rather than implied by position. It costs
  8 bytes per record and buys a self-describing log: recovery, a hex dump, and
  a follower validating a replicated batch can all tell exactly which offsets
  are present without consulting an index.
* **Null key ≠ empty key.** `key_len == 0` means null; a zero-length key
  encodes as `key_len == 1` with no bytes. Partition routing treats a null key
  as "round-robin", so the distinction is load-bearing.

---

## 4. Message payloads

Fields are listed in encoding order.

### PRODUCE (1)

Request:
```
topic         string
partition     i32
acks          u8     0 = none, 1 = leader, 2 = quorum
timeout_ms    i32
record_count  u32
records_len   u32
records       records_len bytes  (concatenated records, offset field = 0)
```
Response:
```
error_code, error_message
base_offset       i64   offset assigned to the first record
last_offset       i64   offset assigned to the last record
append_time       i64   broker wall-clock time of the append
high_water_mark   i64
```

### FETCH (2)

Request:
```
topic         string
partition     i32
fetch_offset  i64
max_bytes     u32
min_bytes     u32   broker waits until this much is available…
max_wait_ms   i32   …but no longer than this (long poll; 0 = return now)
isolation     u8    0 = read up to log end, 1 = read up to high-water mark
```
Response:
```
error_code, error_message
high_water_mark   i64
log_start_offset  i64
base_offset       i64   offset of the first record returned
record_count      u32
records_len       u32
records           records_len bytes  (stored format, verbatim)
```

A fetch never returns a partial record: the broker truncates the returned range
at a record boundary even if that means returning fewer bytes than `max_bytes`.
If the single next record is larger than `max_bytes`, it is returned anyway, so
a consumer can never wedge on an oversized record.

### LIST_OFFSETS (3)

Request: `topic string, partition i32, timestamp i64`
(`-2` = earliest, `-1` = latest, otherwise the first offset with
`record.timestamp >= timestamp`).
Response: `error…, offset i64, timestamp i64`.

### CREATE_TOPIC (20)

Request: `topic string, partitions i32, replication_factor i16,
retention_ms i64, segment_bytes i64, compression u8`
(`-1` for `retention_ms`/`segment_bytes` inherits the broker default).
Response: `error…, partitions i32`.

### METADATA (21)

Request: `topics [string]` — empty means every known topic.
Response:
```
error_code, error_message
controller_id  i32
brokers        [ id i32, host string, port u16 ]
topics         [ name string,
                 partitions [ index i32, leader i32, leader_epoch i64,
                              replicas [i32], in_sync_replicas [i32] ] ]
```

### COMMIT_OFFSET (22) / FETCH_OFFSET (23)

```
COMMIT  group_id string, member_id string, generation i32,
        topic string, partition i32, offset i64, metadata string
FETCH   group_id string, topic string, partition i32
        -> error…, offset i64, metadata string
```
The committed offset is the **next offset to consume**, not the last one
consumed. `offset == -1` in a fetch response means nothing is committed yet.

### JOIN_GROUP (24) / HEARTBEAT (25) / LEAVE_GROUP (26)

```
JOIN      group_id string, member_id string, topics [string],
          session_timeout_ms i32, strategy u8 (0 = range, 1 = round-robin)
          -> error…, generation i32, member_id string, leader_id string,
             assignment [ topic string, partition i32 ]
HEARTBEAT group_id string, member_id string, generation i32
          -> error…, generation i32, rejoin_required bool
LEAVE     group_id string, member_id string -> error…
```
`member_id` is empty on a first join; the coordinator allocates one.

### HEALTH (27)

Request: empty. Response:
```
error…, broker_id i32, uptime_ms i64, hosted_partitions i32,
leader_partitions i32, ready bool, version string
```

### REPLICATE (40)

Leader → follower.
```
topic                    string
partition                i32
leader_id                i32
leader_epoch             i64
base_offset              i64   offset of the first record in this batch
prev_offset              i64   offset immediately before base_offset
leader_high_water_mark   i64
leader_log_start_offset  i64
record_count             u32
records_len              u32
records                  stored-format bytes, already checksummed
```
Response: `error…, follower_id i32, log_end_offset i64, flushed_offset i64,
leader_epoch i64`.

`prev_offset` is the continuity check. A follower whose log end offset does not
equal `prev_offset + 1` refuses the batch with `OUT_OF_RANGE` and switches to
`REPLICA_FETCH` to catch up, which is what prevents a silent hole in the log
after a reconnect.

### REPLICA_ACK (41) / REPLICA_FETCH (42)

```
ACK    topic, partition, follower_id i32, leader_epoch i64,
       log_end_offset i64, flushed_offset i64 -> error…, high_water_mark i64
FETCH  topic, partition, follower_id i32, leader_epoch i64,
       fetch_offset i64, max_bytes u32
       -> error…, leader_epoch i64, high_water_mark i64, log_start_offset i64,
          base_offset i64, record_count u32, records_len u32, records
```

---

## 5. Compatibility strategy

**Adding a field.** Append it to the end of the payload and bump nothing. Old
decoders stop at the fields they know.

**Requests must be fully consumed.** A broker rejects a request payload with
trailing bytes it did not decode (`PayloadReader::Complete()` is asserted at
every request site). If a newer client sends a field an older broker does not
implement, the broker must fail loudly rather than silently ignore a directive
that changes semantics — imagine an older broker quietly dropping a future
`transactional_id`.

**Responses tolerate trailing bytes.** A client reading a response from a newer
broker ignores fields past the ones it knows. Responses are informational; a
field the client does not understand cannot cause it to violate a guarantee.

**Breaking changes** bump `version` and widen `kMinSupportedVersion`. A broker
rejects any frame outside `[kMinSupportedVersion, kProtocolVersion]` with
`UNSUPPORTED_VERSION`, which a client surfaces directly rather than retrying.

**Error codes** are numbered and stable. A client that receives a code it does
not recognise treats it as `UNKNOWN` (non-retryable) and surfaces the message
string.

---

## 6. Security posture

Version 1 has **no authentication, no authorisation and no transport
encryption**. Any process that can open a TCP connection to a broker port can
publish to and read from every topic, and can impersonate a follower. PulseLog
is intended to run on a trusted network or inside a single host. See
[SECURITY.md](../SECURITY.md).

The framing layer is nevertheless hardened against malformed input, because
corruption and bugs produce the same bytes as an attacker would: bounded
lengths, checksums checked before lengths are trusted, array counts bounded by
the bytes actually present, and no allocation sized from an unvalidated field.
`tests/unit/test_frame.cc` and `test_messages.cc` include randomised
malformed-input runs that assert this holds.
