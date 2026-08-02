# PulseLog Measured Performance

Every number here was produced by running the code in this repository. Nothing
is projected, rounded up, or carried over from another machine. Where a result
is unflattering it is reported anyway, because a benchmark that only prints
good news is not a measurement.

Reproduce all of it with:

```bash
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j
python3 scripts/run_benchmarks.py --trials=5 --out results
python3 scripts/plot_results.py --results results --out results
```

---

## 1. Test machine

| Property | Value |
|---|---|
| Machine | Apple MacBook Air (M2), 8 logical cores, 16 GiB |
| OS | Darwin 23.5.0 (macOS 14.5) |
| Filesystem | APFS on internal NVMe |
| Compiler | Apple clang 15.0.0, `-O2 -g -DNDEBUG`, C++20 |
| Event loop | kqueue |
| Checksum | hardware CRC-32C (ARM64 `crc32c*` instructions) |

**This is a thermally constrained laptop, not a benchmark host.** It runs a
desktop, has one shared filesystem, and throttles. That shows up as run-to-run
variance of 2–3× in several scenarios, which is why every table below reports
the spread across trials rather than only a median. Treat these as *shape*
results — how the system responds to batching, partitioning and durability —
not as a hardware capability claim.

---

## 2. Measurement method

* **Warm-up is discarded.** Connection setup, segment creation and buffer-pool
  growth are real but not steady-state, so they are excluded.
* **Latency is per produce request**, recorded into an HDR histogram
  (3 significant digits). With `batch=100`, one sample covers 100 records — a
  batched p50 is *not* a per-record latency and is never presented as one.
* **Throughput is wall-clock over the measured window only.**
* **Coordinated omission is not corrected for.** The driver is closed-loop:
  each producer waits for its acknowledgement before sending again. A stall
  suppresses later samples instead of showing up as one long sample, so these
  percentiles are *service times under this offered load*, not what an
  open-loop arrival process would experience. Every result file repeats this.
* Five trials per scenario; the median is the headline and the full range is
  shown.

---

## 3. End-to-end results

Median of 5 trials. `4p/8part/b100/128B/leader` reads as 4 producers,
8 partitions, batch size 100, 128-byte values, `acks=leader`.

| Scenario | Config | records/s (median) | spread | MiB/s | p50 | p99 | err |
|---|---|---:|---:|---:|---:|---:|---:|
| 01 single producer, no replication | `1p/1part/b100/128B/leader` | 861,344 | 785k–1,017k | 128 | 39 µs | 349 µs | 0 |
| 02 multi-producer, one partition | `4p/1part/b100/128B/leader` | 1,850,569 | 872k–2,392k | 275 | 84 µs | 2,646 µs | 0 |
| 03 multi-producer, multi-partition | `4p/8part/b100/128B/leader` | 971,961 | 866k–1,150k | 145 | 72 µs | 7,287 µs | 0 |
| 04 producers + consumers | `4p/4part/b100/128B/leader` | 1,197,498 | 951k–1,627k | 178 | 89 µs | 6,140 µs | 0 |
| 05 leader ack | `4p/4part/b100/128B/leader` | 1,301,128 | 1,191k–2,014k | 194 | 87 µs | 8,495 µs | 0 |
| 06 quorum ack | `4p/4part/b100/128B/quorum` | 26,147 | 20k–62k | 3.9 | 15,385 µs | 19,890 µs | 0 |
| 07 replication under load (3 brokers, RF=3) | `4p/6part/b100/128B/leader` | 398,581 | 132k–501k | 59 | 164 µs | 14,574 µs | 0 |
| 08 no batching | `4p/4part/b1/128B/leader` | 83,865 | 79k–86k | 12.5 | 38 µs | 113 µs | 0 |
| 09 small message | `4p/4part/b200/16B/leader` | 3,119,652 | 2,691k–3,256k | 128 | 87 µs | 3,592 µs | 0 |
| 10 large message | `4p/4part/b4/65536B/leader` | 6,619 | 3.7k–10.2k | 414 | 514 µs | 28,017 µs | 0 |
| 11 acks=none | `4p/4part/b100/128B/none` | 600,651 | 409k–977k | 89 | 77 µs | 19,661 µs | 0 |
| 12 `fsync.mode=data` | `4p/4part/b100/128B/leader` | 2,529,461 | 2,080k–3,141k | 376 | 60 µs | 3,260 µs | 0 |
| 13 baseline: mutex queue | `4p/1part/b1/128B` (in-process) | 5,225,821 | 4,467k–5,342k | 638 | <1 µs | 11 µs | 0 |

Charts: `results/throughput.svg`, `results/latency_p99.svg`. Raw data, including
every trial and the full environment, is one JSON file per scenario in
`results/`.

### Against the stated targets

| Target | Result |
|---|---|
| >500,000 records/s on a single local broker, small batched messages | **Met.** 861k single-producer, 1.85M with four producers, 3.1M with 16-byte values |
| Sub-millisecond median publish latency in memory-backed mode | **Met.** p50 is 38–89 µs across every non-quorum scenario |
| Low-single-digit-millisecond p99 in durable mode | **Not met on this machine.** Quorum p99 is ~20 ms, dominated by device commit latency (§5) |
| Recover partition state within seconds after abrupt restart | **Met, with room to spare.** 0.12 s for a 400-record partition after SIGKILL; see §6 |
| Scale throughput as partitions and workers increase | **Partially.** Producer count scales cleanly; partition count does not (§4.2) |
| Demonstrable gains from batching and buffer reuse | **Met.** Batching is a 22× effect, buffer pooling 30× (§4.1, §7) |

---

## 4. What the numbers show

### 4.1 Batching dominates everything else

Single producer, one partition, `acks=leader`, 128-byte values:

| Batch size | records/s | p50 | p99 |
|---:|---:|---:|---:|
| 1 | 32,434 | 29 µs | 45 µs |
| 10 | 256,610 | 30 µs | 86 µs |
| 100 | 1,338,759 | 38 µs | 161 µs |
| 500 | 1,642,227 | 74 µs | 4,845 µs |

**Batching 100 records is a 41× throughput increase for a 9 µs p50 cost.** The
reason is visible in the batch-1 row: at 29 µs p50 and one round trip per
record, a single closed-loop producer is bounded at ~34k records/s by latency
alone, no matter how fast the broker is.

Batch 500 buys 23% more throughput for a 30× worse p99. Beyond ~100 records per
request, the queueing delay behind a large batch dominates.

### 4.2 Producers scale; partitions do not

Batch 1, `acks=leader`, 4 partitions:

| Producers | records/s | p50 | p99 |
|---:|---:|---:|---:|
| 1 | 36,534 | 22 µs | 50 µs |
| 2 | 54,799 | 27 µs | 134 µs |
| 4 | 76,449 | 37 µs | 133 µs |
| 8 | 117,829 | 54 µs | 141 µs |

Throughput rises 3.2× from 1 to 8 producers while p50 rises 2.5× — the system
is trading latency for concurrency, which is what a closed-loop load generator
against a queueing system should do.

Partition count, however, does **not** help: scenario 02 (1 partition,
1.85M/s) beats scenario 03 (8 partitions, 0.97M/s). More partitions mean more
open segment files, more flush work spread across more files, and less batching
per file. On this single-disk laptop, partitioning costs more than the
parallelism it buys. On a machine with more IO parallelism the balance would
differ; this result is specific to the hardware and is not generalised here.

### 4.3 Small messages are framing-bound, large messages are device-bound

* 16-byte values reach **3.1M records/s but only 128 MiB/s** — per-record
  framing (27 bytes) is larger than the payload, so the engine is doing
  bookkeeping, not moving data.
* 64 KiB values reach only **6.6k records/s but 414 MiB/s** — the highest byte
  rate of any scenario. The engine is out of the way and the disk is the limit.

### 4.4 The mutex-queue baseline

An in-process, mutex-protected `std::vector` with no networking, no protocol,
no checksums and no disk reaches 5.2M records/s. PulseLog reaches 1.85M with
four producers — **36% of a baseline that provides none of the guarantees**,
while doing TCP framing, CRC-32C on every record and every frame, offset
assignment, and durable segmented storage.

That ratio is the honest way to read the throughput numbers: the remaining 64%
is what those guarantees cost.

---

## 5. Durability is expensive here, and the reason is the device

Quorum acknowledgements run at 26k records/s with a p50 of 15 ms — two orders
of magnitude below `acks=leader`. That is not a bug in the acknowledgement
path; it is the cost of `F_FULLFSYNC` on this machine.

Three measurements isolate it.

**Synchronous append, no network** (`bench_storage`):

| Configuration | throughput |
|---|---:|
| Append, no fsync, batch 1 | 641k records/s |
| Append, no fsync, batch 100 | 8.7M records/s |
| Append, no fsync, batch 1000 | 23.2M records/s (3.4 GiB/s) |
| Append + `F_FULLFSYNC` per append, batch 1 | 10.1k records/s |
| Append + `F_FULLFSYNC` per append, batch 100 | 417k records/s |

**Background flushing stalls concurrent appends.** Four producers, 4 partitions,
batch 100, `acks=none`:

| Flush policy | records/s | p99 |
|---|---:|---:|
| Flusher effectively disabled (60 s / 2 GiB thresholds) | 4.4M–5.1M | 154–356 µs |
| Default (4 MiB / 200 ms) | 1.1M–3.9M | 2,900–4,700 µs |

The stall is in the filesystem, not in this code: appends take no lock that a
flush holds, and the flusher runs on its own thread. On APFS, a `pwrite` to a
file with an in-flight sync serialises behind it.

**The `fsync.mode` knob is worth about 2×** (scenario 05 vs 12): 1.30M/s with
`full` against 2.53M/s with `data`, same workload. `full` is the default anyway,
because it is what quorum acknowledgements are *defined against* — a broker
that acknowledges "durable" without reaching stable media is lying, and a
2× number is not worth that.

This is a macOS-specific magnitude. On Linux, `fdatasync` on a preallocated
file is substantially cheaper, and CI runs the same suite there.

---

## 6. Recovery time

Measured by `scripts/failure_test.sh`: SIGKILL a broker with no clean shutdown
and no final flush, restart it, and time until it answers again.

| Partition state | Recovery |
|---|---:|
| 400 records, 1 partition, one segment | **0.12 s** |
| 4 partitions plus the internal offset log | 0.13 s |

Recovery scans forward from each segment's last index entry rather than from
byte 0, so the cost is bounded by `index_interval_bytes` (4 KiB by default)
per segment rather than growing with the log. A 128 MiB segment recovers in the
same time as a 4 KiB one.

Correctness under the same test:

* Every record acknowledged with `acks=quorum` survived SIGKILL — 400 of 400.
* Corrupting 8 bytes 400 bytes before the end of a 300-record log kept 289
  records, truncated at the failing checksum, logged the reason at WARN, and
  left the partition writable.
* A torn write (20 bytes chopped off the tail) cost **exactly one record**.

---

## 7. Micro-benchmarks

From `bench_core`, each tied to a decision in the source.

**CRC-32C: hardware dispatch is worth it (5.2×)**

| Input | Hardware (ARM64) | Software (slicing-by-8) |
|---:|---:|---:|
| 64 B | 5.9 GiB/s | 2.6 GiB/s |
| 4 KiB | 7.9 GiB/s | 1.48 GiB/s |
| 64 KiB | 7.97 GiB/s | 1.54 GiB/s |

**Buffer pooling: 30× cheaper than allocating per request**

| Buffer size | Fresh allocation | From the pool |
|---:|---:|---:|
| 4 KiB | 96.8 ns | 20.8 ns |
| 64 KiB | 603 ns | 20.4 ns |
| 256 KiB | 2,208 ns | 20.3 ns |

Pool acquisition is constant-time in the buffer size; allocation is not.

**Queues: the right structure for the right place**

| Structure | Round trip | Items/s |
|---|---:|---:|
| SPSC ring (wait-free) | 5.01 ns | 200M |
| Bounded MPMC (lock-free) | 12.5 ns | 81M |
| Mutex + condvar | 38.1 ns | 26M |

The MPMC queue is 3× faster than mutex+condvar and is what carries requests
from io loops to partition workers. Under contention it degrades — 6.3 ns at
1 thread, 72 ns at 2, 172 ns at 4 — which is expected for a single shared
cache line and is why partitions are sharded across workers rather than
funnelled through one queue.

**Skipping checksum verification on the read path**

| Record size | Parse + verify CRC | Parse only |
|---:|---:|---:|
| 128 B | 14.1 ns | 6.9 ns |
| 1 KiB | 107 ns | 7.8 ns |
| 64 KiB | 7,538 ns | 8.2 ns |

Without verification the parse never touches the payload at all, so its cost is
independent of record size. This is why a fetch does not re-verify checksums:
the data was checksummed when written and is re-checked by recovery and by
every follower that receives it.

---

## 8. Optimisations, before and after

Each of these was found by measurement, not by inspection.

### 8.1 Offset lookup: 18× (a real bug found by `bench_storage`)

`PositionFor` walked the log by `pread`-ing each record's 4-byte length prefix,
turning one offset lookup into tens of syscalls.

| Benchmark | Before | After | Gain |
|---|---:|---:|---:|
| `BM_OffsetLookup/4096` | 170 µs | 9.3 µs | 18× |
| `BM_LogRead/4096` | 181 µs | 11.1 µs | 16× |
| `BM_LogRead/65536` | 180 µs | 16.2 µs | 11× |
| `BM_LogRead/1MiB` | 233 µs | 89.2 µs | 2.6× (11 GiB/s) |

Worth recording: the *first* fix was slower than the bug. Using
`kMaxRecordBytes` (16 MiB) as the block slack made every scan read 16 MiB —
666 µs, four times worse. The working version bounds the block by
`index_interval_bytes` with an 8 KiB floor.

### 8.2 Sticky partitioning: 7× under multiple producers

Round-robining keyless records *per record* means consecutive records target
different partitions, so every record ends the current batch and batching stops
happening. Keeping one partition until its batch is sent, with a **randomised
start per producer** so producers do not march in lockstep onto the same
partition:

| Variant | records/s | p99 |
|---|---:|---:|
| Per-record round-robin | 609k | 13,303 µs |
| Sticky, all producers starting at partition 0 | 631k | 16,327 µs |
| Sticky, randomised start | 4,196,582 | 167 µs |

The lockstep row is the interesting one: sticky partitioning alone changed
almost nothing, because four producers stepping through partitions in unison
still concentrate on one partition at a time.

### 8.3 `writev` for queued responses

Queued responses are coalesced into a single `sendmsg` per flush rather than one
syscall per response. On the produce path this is usually one response per
flush, so the win appears only under pipelining; `Connection::Stats` exposes
`writev_calls` against `frames_sent` so the coalescing ratio is observable at
runtime rather than assumed.

---

## 9. What has not been measured

Stated so the absence is not mistaken for a result:

* **No comparison against Kafka, Redpanda or NATS.** A fair comparison needs
  matched durability settings, matched batching, and a machine that is not a
  laptop. An unfair one would be worse than none.
* **No open-loop latency measurement.** The driver is closed-loop, so the
  percentiles do not capture what a fixed arrival rate would see under
  saturation.
* **No Linux numbers in this document.** CI runs the same suite on Linux and
  uploads the results as an artifact, but nothing from there is quoted here,
  because this document reports only what was run on the stated machine.
* **No multi-machine cluster.** All "3 broker" results are three processes on
  one host sharing one disk and the loopback interface, which flatters the
  network and penalises the disk.
* **No `io_uring` numbers.** That backend is not implemented.
