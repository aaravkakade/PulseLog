#!/usr/bin/env python3
"""Failure injection for PulseLog.

Each command creates one specific failure and reports what the cluster did.
The point is not that the tools exist -- it is that the documented behaviour in
docs/FAILURE_SEMANTICS.md can be reproduced on demand.

Commands:
  kill-broker      SIGKILL a broker (no clean shutdown, no final flush)
  stop-broker      SIGSTOP a broker, simulating a frozen process / GC pause
  resume-broker    SIGCONT a stopped broker
  restart-broker   SIGKILL then start again, exercising recovery
  corrupt-tail     flip bytes at the end of a partition's newest segment
  truncate-tail    chop bytes off the end of a segment (a torn write)
  fill-disk        create a large file to push free space under the floor
  drop-connections exhaust a broker's connection limit
  overload         flood a broker to force backpressure

Examples:
  scripts/chaos.py kill-broker --pid 1234
  scripts/chaos.py corrupt-tail --data-dir ./pulselog-data --topic orders --partition 0
  scripts/chaos.py overload --broker 127.0.0.1:9092 --topic orders --connections 64
"""

from __future__ import annotations

import argparse
import os
import random
import signal
import socket
import struct
import subprocess
import sys
import time
import zlib
from pathlib import Path

FRAME_MAGIC = b"PLSG"
OP_HEALTH = 27
OP_PRODUCE = 1
FRAME_HEADER_SIZE = 32


def crc32c(data: bytes, seed: int = 0) -> int:
    """CRC-32C (Castagnoli). Implemented here so this script has no deps.

    Slow, but it only ever runs over a few frame headers.
    """
    table = crc32c._table  # type: ignore[attr-defined]
    crc = seed ^ 0xFFFFFFFF
    for byte in data:
        crc = table[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF


def _build_table() -> list[int]:
    poly = 0x82F63B78
    table = []
    for i in range(256):
        crc = i
        for _ in range(8):
            crc = (crc >> 1) ^ (poly if crc & 1 else 0)
        table.append(crc)
    return table


crc32c._table = _build_table()  # type: ignore[attr-defined]


def build_frame(opcode: int, request_id: int, payload: bytes) -> bytes:
    payload_crc = crc32c(payload) if payload else 0
    header = bytearray(FRAME_HEADER_SIZE)
    header[0:4] = FRAME_MAGIC
    struct.pack_into("<H", header, 4, 1)            # version
    struct.pack_into("<H", header, 6, opcode)
    struct.pack_into("<Q", header, 8, request_id)
    struct.pack_into("<I", header, 16, len(payload))
    struct.pack_into("<H", header, 20, 0)           # flags
    struct.pack_into("<H", header, 22, 0)           # reserved
    struct.pack_into("<I", header, 24, payload_crc)
    struct.pack_into("<I", header, 28, crc32c(bytes(header[:28])))
    return bytes(header) + payload


def parse_endpoint(text: str) -> tuple[str, int]:
    host, _, port = text.rpartition(":")
    return host or "127.0.0.1", int(port)


def newest_segment(data_dir: Path, topic: str, partition: int) -> Path | None:
    directory = data_dir / f"{topic}-{partition}"
    if not directory.is_dir():
        return None
    segments = sorted(directory.glob("*.log"))
    return segments[-1] if segments else None


def valid_extent(segment: Path) -> int:
    """Bytes of real records in `segment`, ignoring the preallocated tail.

    Segments are preallocated, so the file is usually far larger than its
    contents and the unused remainder reads as zeros. Corrupting or truncating
    relative to the *file size* would land in that dead zone and change
    nothing, which is exactly the mistake this function exists to prevent.

    The walk mirrors recovery: follow each record's 4-byte length prefix until
    one is implausible.
    """
    data = segment.read_bytes()
    position = 0
    while position + 4 <= len(data):
        (length,) = struct.unpack_from("<I", data, position)
        record_size = length + 4
        # 25 is the fixed record prefix; 16 MiB is the maximum record size.
        if length < 21 or record_size > (16 << 20) or position + record_size > len(data):
            break
        position += record_size
    return position


# --- commands ---------------------------------------------------------------


def cmd_kill_broker(args) -> int:
    os.kill(args.pid, signal.SIGKILL)
    print(f"sent SIGKILL to {args.pid}")
    print("expected: no clean shutdown, no final flush; on restart the log")
    print("          recovers to the last checksum-valid record and any")
    print("          unflushed acks=leader writes may be gone")
    return 0


def cmd_stop_broker(args) -> int:
    os.kill(args.pid, signal.SIGSTOP)
    print(f"sent SIGSTOP to {args.pid}")
    print("expected: the broker stops responding without closing its sockets.")
    print("          Leaders replicating to it evict it from the in-sync set")
    print("          after the liveness probe fails, and quorum writes then")
    print("          need the remaining replicas only")
    return 0


def cmd_resume_broker(args) -> int:
    os.kill(args.pid, signal.SIGCONT)
    print(f"sent SIGCONT to {args.pid}")
    print("expected: the follower reconnects and catches up from its own log")
    print("          end offset; the leader resumes streaming from there")
    return 0


def cmd_restart_broker(args) -> int:
    os.kill(args.pid, signal.SIGKILL)
    print(f"sent SIGKILL to {args.pid}; waiting for it to exit")
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            os.kill(args.pid, 0)
            time.sleep(0.1)
        except ProcessLookupError:
            break
    if not args.command_line:
        print("no --command-line given; start the broker yourself to observe recovery")
        return 0

    started = time.time()
    process = subprocess.Popen(args.command_line.split())
    host, port = parse_endpoint(args.broker)
    while time.time() - started < 30:
        try:
            with socket.create_connection((host, port), timeout=0.5) as sock:
                sock.sendall(build_frame(OP_HEALTH, 1, b""))
                if sock.recv(64):
                    print(f"broker answered again after {time.time() - started:.2f}s "
                          f"(pid {process.pid})")
                    return 0
        except OSError:
            time.sleep(0.1)
    print("broker did not come back within 30s", file=sys.stderr)
    return 1


def cmd_corrupt_tail(args) -> int:
    segment = newest_segment(Path(args.data_dir), args.topic, args.partition)
    if segment is None:
        print(f"no segment for {args.topic}-{args.partition} under {args.data_dir}",
              file=sys.stderr)
        return 1

    extent = valid_extent(segment)
    if extent < args.bytes + args.back_off + 32:
        print(f"{segment} holds only {extent} bytes of records; nothing to corrupt",
              file=sys.stderr)
        return 1

    # Relative to the end of real data, never the preallocated tail.
    offset = max(0, extent - args.bytes - args.back_off)
    with open(segment, "r+b") as handle:
        handle.seek(offset)
        original = handle.read(args.bytes)
        handle.seek(offset)
        handle.write(bytes(b ^ 0xFF for b in original))
    print(f"flipped {args.bytes} bytes at offset {offset} of {segment.name} "
          f"({extent} bytes of records, {segment.stat().st_size} bytes on disk)")
    print("expected: on restart, recovery stops at the first record whose CRC")
    print("          fails, truncates from there, and logs the reason at WARN.")
    print("          Records before the damage remain readable")
    return 0


def cmd_truncate_tail(args) -> int:
    segment = newest_segment(Path(args.data_dir), args.topic, args.partition)
    if segment is None:
        print(f"no segment for {args.topic}-{args.partition}", file=sys.stderr)
        return 1

    extent = valid_extent(segment)
    if extent <= args.bytes:
        print(f"{segment} holds only {extent} bytes of records", file=sys.stderr)
        return 1
    new_size = extent - args.bytes
    os.truncate(segment, new_size)
    print(f"truncated {segment.name} to {new_size} bytes "
          f"(was {extent} bytes of records in a {args.bytes + new_size}+ byte file)")
    print("expected: on restart, the torn record is dropped as incomplete")
    print("          (OUT_OF_RANGE, not CORRUPTION) and the rest is kept")
    return 0


def cmd_fill_disk(args) -> int:
    target = Path(args.data_dir) / "chaos-ballast.bin"
    target.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    chunk = b"\0" * (1024 * 1024)
    try:
        with open(target, "wb") as handle:
            while written < args.megabytes * 1024 * 1024:
                handle.write(chunk)
                written += len(chunk)
            handle.flush()
            os.fsync(handle.fileno())
    except OSError as error:
        print(f"stopped after {written / (1024 ** 2):.0f} MiB: {error}")

    print(f"wrote {written / (1024 ** 2):.0f} MiB to {target}")
    print("expected: once free space drops below storage.min.free.disk.bytes,")
    print("          appends are refused with RESOURCE_EXHAUSTED and the broker")
    print("          logs it at ERROR. Reads keep working.")
    print(f"remove it with: rm {target}")
    return 0


def cmd_drop_connections(args) -> int:
    host, port = parse_endpoint(args.broker)
    sockets = []
    refused = 0
    for _ in range(args.connections):
        try:
            sock = socket.create_connection((host, port), timeout=2)
            sockets.append(sock)
        except OSError:
            refused += 1

    print(f"opened {len(sockets)} connections, {refused} refused")
    if args.hold_seconds:
        print(f"holding for {args.hold_seconds}s...")
        time.sleep(args.hold_seconds)
    for sock in sockets:
        sock.close()
    print("closed all connections")
    print("expected: beyond net.max.connections the broker closes new sockets")
    print("          immediately and counts them, rather than running out of")
    print("          file descriptors")
    return 0


def cmd_overload(args) -> int:
    """Flood a broker with pipelined produce requests without reading replies.

    This is the backpressure test: the broker must refuse work explicitly
    rather than buffering it without bound.
    """
    host, port = parse_endpoint(args.broker)
    topic = args.topic.encode()
    value = b"x" * args.record_size

    # One record: length | crc | offset | timestamp | attributes | key_len |
    # value_len | value. Key is null (varint 0).
    def encode_record() -> bytes:
        body = struct.pack("<q", 0)                # offset
        body += struct.pack("<q", 0)               # timestamp
        body += bytes([0])                         # attributes
        body += bytes([0])                         # key_len varint: null
        length = len(value)
        varint = bytearray()
        while length >= 0x80:
            varint.append((length & 0x7F) | 0x80)
            length >>= 7
        varint.append(length)
        body += bytes(varint) + value
        crc = crc32c(body)
        return struct.pack("<I", len(body) + 4) + struct.pack("<I", crc) + body

    record = encode_record()
    records = record * args.batch
    payload = struct.pack("<H", len(topic)) + topic
    payload += struct.pack("<i", 0)                # partition
    payload += bytes([0])                          # acks = none
    payload += struct.pack("<i", 1000)             # timeout_ms
    payload += struct.pack("<I", args.batch)       # record_count
    payload += struct.pack("<I", len(records))
    payload += records
    frame = build_frame(OP_PRODUCE, 1, payload)

    sockets = []
    for _ in range(args.connections):
        try:
            sock = socket.create_connection((host, port), timeout=2)
            sock.setblocking(False)
            sockets.append(sock)
        except OSError as error:
            print(f"connect failed: {error}", file=sys.stderr)

    if not sockets:
        print("no connections established", file=sys.stderr)
        return 1

    print(f"flooding {len(sockets)} connections for {args.seconds}s "
          f"({args.batch} records x {args.record_size} B per request, replies ignored)")

    sent = 0
    blocked = 0
    deadline = time.time() + args.seconds
    while time.time() < deadline:
        for sock in sockets:
            try:
                sock.sendall(frame)
                sent += 1
            except (BlockingIOError, OSError):
                blocked += 1

    for sock in sockets:
        sock.close()

    print(f"sent {sent} requests; {blocked} sends blocked or failed")
    print("expected: the broker stops reading from connections whose response")
    print("          queue exceeds the high-water mark (which is why sends")
    print("          block here), and refuses work with BACKPRESSURE when a")
    print("          worker queue fills. Memory stays bounded; check")
    print("          pulselog_backpressure_rejections_total on /metrics")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    for name, handler in (("kill-broker", cmd_kill_broker),
                          ("stop-broker", cmd_stop_broker),
                          ("resume-broker", cmd_resume_broker)):
        p = sub.add_parser(name)
        p.add_argument("--pid", type=int, required=True)
        p.set_defaults(func=handler)

    p = sub.add_parser("restart-broker")
    p.add_argument("--pid", type=int, required=True)
    p.add_argument("--broker", default="127.0.0.1:9092")
    p.add_argument("--command-line", default="",
                   help="how to start the broker again, to measure recovery time")
    p.set_defaults(func=cmd_restart_broker)

    p = sub.add_parser("corrupt-tail")
    p.add_argument("--data-dir", required=True)
    p.add_argument("--topic", required=True)
    p.add_argument("--partition", type=int, default=0)
    p.add_argument("--bytes", type=int, default=16)
    p.add_argument("--back-off", type=int, default=0,
                   help="how far before the end of the file to corrupt")
    p.set_defaults(func=cmd_corrupt_tail)

    p = sub.add_parser("truncate-tail")
    p.add_argument("--data-dir", required=True)
    p.add_argument("--topic", required=True)
    p.add_argument("--partition", type=int, default=0)
    p.add_argument("--bytes", type=int, default=64)
    p.set_defaults(func=cmd_truncate_tail)

    p = sub.add_parser("fill-disk")
    p.add_argument("--data-dir", required=True)
    p.add_argument("--megabytes", type=int, default=1024)
    p.set_defaults(func=cmd_fill_disk)

    p = sub.add_parser("drop-connections")
    p.add_argument("--broker", default="127.0.0.1:9092")
    p.add_argument("--connections", type=int, default=100)
    p.add_argument("--hold-seconds", type=float, default=0.0)
    p.set_defaults(func=cmd_drop_connections)

    p = sub.add_parser("overload")
    p.add_argument("--broker", default="127.0.0.1:9092")
    p.add_argument("--topic", default="bench")
    p.add_argument("--connections", type=int, default=32)
    p.add_argument("--batch", type=int, default=200)
    p.add_argument("--record-size", type=int, default=1024)
    p.add_argument("--seconds", type=float, default=5.0)
    p.set_defaults(func=cmd_overload)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
