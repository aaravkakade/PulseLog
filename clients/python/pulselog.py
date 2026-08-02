#!/usr/bin/env python3
"""A minimal PulseLog client in pure Python.

For demos, scripting and benchmark orchestration. The C++ SDK in `clients/cpp`
is the real client -- this one exists so a shell script or a notebook can talk
to a broker without a build step, and it makes no attempt to match the C++
client's performance.

What it supports: create/delete topic, metadata, health, produce (with keys and
batching), fetch, list offsets, and offset commit/fetch. What it does not:
consumer-group membership, retries, or connection pooling across brokers.

    from pulselog import Client

    with Client("127.0.0.1:9092") as client:
        client.create_topic("orders", partitions=4)
        client.produce("orders", value=b"hello", key=b"user-1")
        for record in client.fetch("orders", partition=0, offset=0):
            print(record.offset, record.value)
"""

from __future__ import annotations

import socket
import struct
from dataclasses import dataclass, field
from typing import Iterator

__all__ = ["Client", "Record", "PulseLogError", "PartitionInfo", "TopicInfo"]

MAGIC = b"PLSG"
VERSION = 1
FRAME_HEADER_SIZE = 32
RESPONSE_FLAG = 1

OP_PRODUCE = 1
OP_FETCH = 2
OP_LIST_OFFSETS = 3
OP_CREATE_TOPIC = 20
OP_METADATA = 21
OP_COMMIT_OFFSET = 22
OP_FETCH_OFFSET = 23
OP_HEALTH = 27
OP_LIST_TOPICS = 28
OP_DELETE_TOPIC = 60

OFFSET_EARLIEST = -2
OFFSET_LATEST = -1

ACKS = {"none": 0, "leader": 1, "quorum": 2}

# Mirrors ErrorCode in include/pulselog/base/status.h. These are wire values
# and never change meaning.
ERROR_NAMES = {
    0: "OK", 1: "UNKNOWN", 2: "INVALID_ARGUMENT", 3: "NOT_FOUND",
    4: "ALREADY_EXISTS", 5: "OUT_OF_RANGE", 6: "CORRUPTION", 7: "IO_ERROR",
    8: "UNAVAILABLE", 9: "TIMEOUT", 10: "BACKPRESSURE", 11: "NOT_LEADER",
    12: "NOT_ENOUGH_REPLICAS", 13: "PROTOCOL_ERROR", 14: "UNSUPPORTED_VERSION",
    15: "PERMISSION_DENIED", 16: "RESOURCE_EXHAUSTED", 17: "REBALANCE_IN_PROGRESS",
    18: "UNKNOWN_MEMBER", 19: "ILLEGAL_GENERATION", 20: "CLOSED",
    21: "WOULD_BLOCK", 22: "INTERNAL",
}

RETRYABLE = {8, 9, 10, 11, 12, 17, 21}


class PulseLogError(Exception):
    """An error returned by a broker, or a protocol violation."""

    def __init__(self, code: int, message: str):
        self.code = code
        self.name = ERROR_NAMES.get(code, f"CODE_{code}")
        self.retryable = code in RETRYABLE
        super().__init__(f"{self.name}: {message}")


def _crc32c_table() -> list[int]:
    poly = 0x82F63B78
    table = []
    for i in range(256):
        crc = i
        for _ in range(8):
            crc = (crc >> 1) ^ (poly if crc & 1 else 0)
        table.append(crc)
    return table


_CRC_TABLE = _crc32c_table()


def crc32c(data: bytes) -> int:
    """CRC-32C (Castagnoli), the checksum PulseLog uses everywhere.

    Table-driven and pure Python, so it is slow. That is acceptable here: this
    client exists for scripting, not throughput.
    """
    crc = 0xFFFFFFFF
    for byte in data:
        crc = _CRC_TABLE[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF


@dataclass
class Record:
    offset: int
    timestamp: int
    key: bytes | None
    value: bytes


@dataclass
class PartitionInfo:
    index: int
    leader: int
    leader_epoch: int
    replicas: list[int] = field(default_factory=list)
    in_sync_replicas: list[int] = field(default_factory=list)


@dataclass
class TopicInfo:
    name: str
    partitions: list[PartitionInfo] = field(default_factory=list)


class _Writer:
    """Builds a request payload. Mirrors protocol::PayloadWriter."""

    def __init__(self) -> None:
        self.buffer = bytearray()

    def u8(self, value: int) -> "_Writer":
        self.buffer.append(value & 0xFF)
        return self

    def u16(self, value: int) -> "_Writer":
        self.buffer += struct.pack("<H", value)
        return self

    def u32(self, value: int) -> "_Writer":
        self.buffer += struct.pack("<I", value)
        return self

    def i16(self, value: int) -> "_Writer":
        self.buffer += struct.pack("<h", value)
        return self

    def i32(self, value: int) -> "_Writer":
        self.buffer += struct.pack("<i", value)
        return self

    def i64(self, value: int) -> "_Writer":
        self.buffer += struct.pack("<q", value)
        return self

    def string(self, value: str) -> "_Writer":
        encoded = value.encode("utf-8")
        return self.u16(len(encoded)).raw(encoded)

    def raw(self, value: bytes) -> "_Writer":
        self.buffer += value
        return self

    def varuint(self, value: int) -> "_Writer":
        while value >= 0x80:
            self.buffer.append((value & 0x7F) | 0x80)
            value >>= 7
        self.buffer.append(value)
        return self

    def bytes(self) -> bytes:
        return bytes(self.buffer)


class _Reader:
    """Reads a response payload. Every read is bounds-checked."""

    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    def _take(self, count: int) -> bytes:
        if self.pos + count > len(self.data):
            raise PulseLogError(13, "response payload is truncated")
        chunk = self.data[self.pos:self.pos + count]
        self.pos += count
        return chunk

    def u8(self) -> int:
        return self._take(1)[0]

    def u16(self) -> int:
        return struct.unpack("<H", self._take(2))[0]

    def u32(self) -> int:
        return struct.unpack("<I", self._take(4))[0]

    def i32(self) -> int:
        return struct.unpack("<i", self._take(4))[0]

    def i64(self) -> int:
        return struct.unpack("<q", self._take(8))[0]

    def boolean(self) -> bool:
        return self.u8() != 0

    def string(self) -> str:
        return self._take(self.u16()).decode("utf-8")

    def varuint(self) -> int:
        value = 0
        shift = 0
        while True:
            byte = self.u8()
            value |= (byte & 0x7F) << shift
            if not byte & 0x80:
                return value
            shift += 7
            if shift > 63:
                raise PulseLogError(13, "malformed varint")

    def remaining(self) -> bytes:
        chunk = self.data[self.pos:]
        self.pos = len(self.data)
        return chunk

    def response_header(self) -> None:
        """Reads the header every response starts with, raising on error.

        An error response carries *only* the header, so this must run before
        any body is decoded -- see docs/PROTOCOL.md.
        """
        code = self.u16()
        message = self.string()
        if code != 0:
            raise PulseLogError(code, message)


def encode_record(key: bytes | None, value: bytes, timestamp: int = 0) -> bytes:
    """One record in PulseLog's on-disk/wire format, with offset 0.

    The broker assigns the real offset and recomputes the checksum, so the CRC
    written here only has to be well-formed.
    """
    body = struct.pack("<q", 0)          # offset, assigned by the broker
    body += struct.pack("<q", timestamp)
    body += bytes([0])                   # attributes

    lengths = _Writer()
    if key is None:
        lengths.varuint(0)
    else:
        lengths.varuint(len(key) + 1)
    lengths.varuint(len(value))
    body += lengths.bytes()

    if key is not None:
        body += key
    body += value

    return struct.pack("<I", len(body) + 4) + struct.pack("<I", crc32c(body)) + body


def decode_records(data: bytes) -> Iterator[Record]:
    """Walks a run of records, verifying each checksum."""
    pos = 0
    while pos + 25 <= len(data):
        (length,) = struct.unpack_from("<I", data, pos)
        total = length + 4
        if pos + total > len(data):
            break

        (stored_crc,) = struct.unpack_from("<I", data, pos + 4)
        body = data[pos + 8:pos + total]
        if crc32c(body) != stored_crc:
            raise PulseLogError(6, f"record checksum mismatch at byte {pos}")

        (offset,) = struct.unpack_from("<q", data, pos + 8)
        (timestamp,) = struct.unpack_from("<q", data, pos + 16)

        reader = _Reader(data[pos + 25:pos + total])
        key_field = reader.varuint()
        value_length = reader.varuint()
        key = None if key_field == 0 else reader._take(key_field - 1)
        value = reader._take(value_length)

        yield Record(offset=offset, timestamp=timestamp, key=key, value=value)
        pos += total


class Client:
    """A connection to one broker.

    Not thread-safe: one instance per thread. Requests and responses are
    correlated by request ID on a single connection, and this client keeps one
    request in flight at a time.
    """

    def __init__(self, endpoint: str = "127.0.0.1:9092", timeout: float = 10.0):
        host, _, port = endpoint.rpartition(":")
        self.host = host or "127.0.0.1"
        self.port = int(port)
        self.timeout = timeout
        self._socket: socket.socket | None = None
        self._request_id = 0

    def __enter__(self) -> "Client":
        self.connect()
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    def connect(self) -> None:
        if self._socket is not None:
            return
        self._socket = socket.create_connection((self.host, self.port), self.timeout)
        self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def close(self) -> None:
        if self._socket is not None:
            self._socket.close()
            self._socket = None

    # --- framing ------------------------------------------------------------

    def _call(self, opcode: int, payload: bytes) -> _Reader:
        if self._socket is None:
            self.connect()
        assert self._socket is not None

        self._request_id += 1
        header = bytearray(FRAME_HEADER_SIZE)
        header[0:4] = MAGIC
        struct.pack_into("<H", header, 4, VERSION)
        struct.pack_into("<H", header, 6, opcode)
        struct.pack_into("<Q", header, 8, self._request_id)
        struct.pack_into("<I", header, 16, len(payload))
        struct.pack_into("<H", header, 20, 0)
        struct.pack_into("<H", header, 22, 0)
        struct.pack_into("<I", header, 24, crc32c(payload) if payload else 0)
        struct.pack_into("<I", header, 28, crc32c(bytes(header[:28])))

        self._socket.sendall(bytes(header) + payload)

        response_header = self._read_exactly(FRAME_HEADER_SIZE)
        if response_header[0:4] != MAGIC:
            raise PulseLogError(13, "bad frame magic in response")
        if crc32c(response_header[:28]) != struct.unpack_from("<I", response_header, 28)[0]:
            raise PulseLogError(6, "response header checksum mismatch")

        (payload_length,) = struct.unpack_from("<I", response_header, 16)
        (payload_crc,) = struct.unpack_from("<I", response_header, 24)
        body = self._read_exactly(payload_length) if payload_length else b""
        if body and crc32c(body) != payload_crc:
            raise PulseLogError(6, "response payload checksum mismatch")
        return _Reader(body)

    def _read_exactly(self, count: int) -> bytes:
        assert self._socket is not None
        chunks = []
        remaining = count
        while remaining > 0:
            chunk = self._socket.recv(remaining)
            if not chunk:
                self.close()
                raise PulseLogError(20, "broker closed the connection")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    # --- operations ---------------------------------------------------------

    def health(self) -> dict:
        reader = self._call(OP_HEALTH, b"")
        reader.response_header()
        return {
            "broker_id": reader.i32(),
            "uptime_ms": reader.i64(),
            "hosted_partitions": reader.i32(),
            "leader_partitions": reader.i32(),
            "ready": reader.boolean(),
            "version": reader.string(),
        }

    def create_topic(self, topic: str, partitions: int = 1,
                     replication_factor: int = 1) -> int:
        payload = (_Writer().string(topic).i32(partitions).i16(replication_factor)
                   .i64(-1).i64(-1).u8(0).u8(0).bytes())
        reader = self._call(OP_CREATE_TOPIC, payload)
        reader.response_header()
        return reader.i32()

    def delete_topic(self, topic: str) -> None:
        reader = self._call(OP_DELETE_TOPIC, _Writer().string(topic).bytes())
        reader.response_header()

    def metadata(self, topics: list[str] | None = None) -> list[TopicInfo]:
        writer = _Writer()
        names = topics or []
        writer.u32(len(names))
        for name in names:
            writer.string(name)

        reader = self._call(OP_METADATA, writer.bytes())
        reader.response_header()
        reader.i32()  # controller id

        for _ in range(reader.u32()):  # brokers
            reader.i32()
            reader.string()
            reader.u16()

        result = []
        for _ in range(reader.u32()):
            topic = TopicInfo(name=reader.string())
            for _ in range(reader.u32()):
                partition = PartitionInfo(index=reader.i32(), leader=reader.i32(),
                                          leader_epoch=reader.i64())
                partition.replicas = [reader.i32() for _ in range(reader.u32())]
                partition.in_sync_replicas = [reader.i32() for _ in range(reader.u32())]
                topic.partitions.append(partition)
            result.append(topic)
        return result

    def produce(self, topic: str, value: bytes, key: bytes | None = None,
                partition: int = 0, acks: str = "leader", timeout_ms: int = 5000) -> dict:
        return self.produce_batch(topic, [(key, value)], partition, acks, timeout_ms)

    def produce_batch(self, topic: str, records: list[tuple[bytes | None, bytes]],
                      partition: int = 0, acks: str = "leader",
                      timeout_ms: int = 5000) -> dict:
        """Sends several records to one partition in a single request.

        Batching is the difference between round-trip-bound and
        throughput-bound; see docs/PERFORMANCE_RESULTS.md.
        """
        if acks not in ACKS:
            raise ValueError(f"acks must be one of {sorted(ACKS)}")

        encoded = b"".join(encode_record(key, value) for key, value in records)
        payload = (_Writer().string(topic).i32(partition).u8(ACKS[acks])
                   .i32(timeout_ms).u32(len(records)).u32(len(encoded))
                   .raw(encoded).bytes())

        reader = self._call(OP_PRODUCE, payload)
        reader.response_header()
        return {
            "base_offset": reader.i64(),
            "last_offset": reader.i64(),
            "append_time": reader.i64(),
            "high_water_mark": reader.i64(),
        }

    def fetch(self, topic: str, partition: int = 0, offset: int = 0,
              max_bytes: int = 1 << 20, max_wait_ms: int = 100) -> list[Record]:
        payload = (_Writer().string(topic).i32(partition).i64(offset)
                   .u32(max_bytes).u32(1).i32(max_wait_ms).u8(1).bytes())
        reader = self._call(OP_FETCH, payload)
        reader.response_header()
        reader.i64()  # high water mark
        reader.i64()  # log start offset
        reader.i64()  # base offset
        reader.u32()  # record count
        length = reader.u32()
        return list(decode_records(reader.remaining()[:length]))

    def list_offsets(self, topic: str, partition: int = 0,
                     timestamp: int = OFFSET_LATEST) -> int:
        payload = _Writer().string(topic).i32(partition).i64(timestamp).bytes()
        reader = self._call(OP_LIST_OFFSETS, payload)
        reader.response_header()
        return reader.i64()

    def commit_offset(self, group: str, topic: str, partition: int, offset: int,
                      metadata: str = "") -> None:
        # An empty member ID means an unmanaged consumer positioning itself by
        # hand; group generation fencing does not apply.
        payload = (_Writer().string(group).string("").i32(-1).string(topic)
                   .i32(partition).i64(offset).string(metadata).bytes())
        reader = self._call(OP_COMMIT_OFFSET, payload)
        reader.response_header()

    def fetch_offset(self, group: str, topic: str, partition: int) -> int:
        payload = _Writer().string(group).string(topic).i32(partition).bytes()
        reader = self._call(OP_FETCH_OFFSET, payload)
        reader.response_header()
        return reader.i64()

    def consume(self, topic: str, partition: int = 0, offset: int = 0,
                max_records: int | None = None) -> Iterator[Record]:
        """Yields records from `offset` until the log end is reached."""
        yielded = 0
        position = offset
        while True:
            records = self.fetch(topic, partition, position)
            if not records:
                return
            for record in records:
                yield record
                position = record.offset + 1
                yielded += 1
                if max_records is not None and yielded >= max_records:
                    return


def _demo() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="PulseLog Python client demo")
    parser.add_argument("--broker", default="127.0.0.1:9092")
    parser.add_argument("--topic", default="python-demo")
    parser.add_argument("--count", type=int, default=100)
    args = parser.parse_args()

    with Client(args.broker) as client:
        health = client.health()
        print(f"broker {health['broker_id']} v{health['version']}, "
              f"{health['hosted_partitions']} partitions hosted")

        client.create_topic(args.topic, partitions=2)
        print(f"created {args.topic}")

        batch = [(f"key-{i}".encode(), f"value-{i}".encode()) for i in range(args.count)]
        result = client.produce_batch(args.topic, batch, partition=0, acks="quorum")
        print(f"produced {args.count} records, offsets "
              f"{result['base_offset']}..{result['last_offset']}")

        received = list(client.consume(args.topic, partition=0, offset=0,
                                       max_records=args.count))
        print(f"consumed {len(received)} records back")
        assert len(received) == args.count, "round trip lost records"
        assert received[0].value == b"value-0"

        client.commit_offset("python-demo-group", args.topic, 0, len(received))
        committed = client.fetch_offset("python-demo-group", args.topic, 0)
        print(f"committed offset {committed}")

        for topic in client.metadata([args.topic]):
            for partition in topic.partitions:
                print(f"  {topic.name}-{partition.index} leader={partition.leader} "
                      f"replicas={partition.replicas}")
    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(_demo())
