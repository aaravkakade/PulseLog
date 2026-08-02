# PulseLog Measured Performance

Every number here was produced by running the code in this repository. Nothing
is projected, rounded up, or carried over from another machine. Where a result
is unflattering it is reported anyway, because a benchmark that only prints
good news is not a measurement.

Reproduce all of it with one command:

```bash
./scripts/benchmark_release.sh
```

It verifies prerequisites, builds an optimised binary, records the hardware,
runs every scenario in `benchmarks/config/release.json`, measures replication
lag and recovery, and writes JSON, CSV, charts and a report. CI runs it on
every push and attaches the raw artifacts to the run.

---

## 1. Test machines

Two machines appear in this document and their results are **never combined**.

### 1.1 Linux x86-64 — the primary platform

| Property | Value |
|---|---|
| Machine | GitHub-hosted runner, AMD EPYC 9V74, 4 logical cores, 15.6 GiB |
| OS | Ubuntu 24.04.4 LTS, kernel 6.17.0-1020-azure |
| Filesystem | ext4 on an Azure virtual disk |
| Compiler | GCC 13.3.0, `-O2 -g -DNDEBUG`, C++20 |
| Event loop | epoll |
| Checksum | hardware CRC-32C (SSE 4.2) |

This is the deployment target and the only platform CI validates. It is a
shared virtual machine, so absolute numbers carry more variance than dedicated
hardware would — but in practice the spreads here are far *tighter* than on the
laptop below (mostly ±1–11%), because nothing else is competing for the disk.

The kernel reports the disk as rotational. That is what `sysfs` says and it is
recorded verbatim; Azure virtual disks commonly report this regardless of the
backing media, so it should not be read as a claim about the physical device.

### 1.2 Apple M2 — development machine only

| Property | Value |
|---|---|
| Machine | Apple MacBook Air (M2), 8 logical cores, 16 GiB |
| OS | Darwin 23.5.0 (macOS 14.5) |
| Filesystem | APFS on internal NVMe |
| Compiler | Apple clang 15.0.0, `-O2 -g -DNDEBUG`, C++20 |
| Event loop | kqueue |
| Checksum | hardware CRC-32C (ARM64 `crc32c*` instructions) |

**This is a thermally constrained laptop, not a benchmark host.** It runs a
desktop, has one shared filesystem, and throttles. Run-to-run variance reaches
2–3× in several scenarios.

More importantly, **durable-mode results are not comparable between the two
machines at all.** macOS `F_FULLFSYNC` and Linux `fsync` are different
operations with different guarantees; see
[PORTABILITY.md §2](PORTABILITY.md#2-durability-the-difference-that-matters-most).
The Apple results are confined to §3.2 and are labelled there.

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
  stops the thread that would have issued the next request, so the requests a
  queueing client would have sent during that stall are never measured. These
  percentiles are *service times under this offered load* and understate the
  tail an open-loop client would see. Every result file repeats this.
* **All traffic is over loopback.** There is no NIC and no switch, so these are
  an upper bound on what the same code does across a real network.
* Five trials per scenario; the median is the headline and the spread is always
  shown alongside it. **Spread is not decoration.** Where two configurations
  differ by less than their spreads, this document says no difference was
  measured rather than picking the larger number.

---

## 3. End-to-end results

`4p/8part/b100/128B/leader` reads as 4 producers, 8 partitions, batch size 100,
128-byte values, `acks=leader`.

### 3.1 Linux x86-64 (primary)

Median of 5 trials; spread is (max − min) / median.

| Scenario | Config | records/s | spread | p50 | p99 | p99.9 | err |
|---|---|---:|---:|---:|---:|---:|---:|
| 01 single producer, no replication | `1p/1part/b100/128B/leader` | 869,072 | ±1% | 100 µs | 157 µs | 493 µs | 0 |
| 02 multi-producer, one partition | `4p/1part/b100/128B/leader` | 2,392,197 | ±5% | 139 µs | 318 µs | 1,068 µs | 0 |
| 03 multi-producer, multi-partition | `4p/8part/b100/128B/leader` | 2,196,040 | ±14% | 147 µs | 492 µs | 655 µs | 0 |
| 04 producers + consumers | `4p/4part/b100/128B/leader` | 1,236,995 | ±89% | 183 µs | 2,578 µs | 2,888 µs | 0 |
| 05 leader ack | `4p/4part/b100/128B/leader` | 2,242,462 | ±2% | 144 µs | 507 µs | 646 µs | 0 |
| 06 quorum ack | `4p/4part/b100/128B/quorum` | 239,527 | ±2% | 1,564 µs | 2,560 µs | 2,652 µs | 0 |
| 07 replication under load (3 brokers, RF=3) | `4p/6part/b100/128B/leader` | 1,003,379 | ±3% | 303 µs | 1,344 µs | 2,708 µs | 0 |
| 08 no batching | `4p/4part/b1/128B/leader` | 47,905 | ±10% | 80 µs | 150 µs | 233 µs | 0 |
| 09 small message (16 B) | `4p/4part/b200/16B/leader` | 4,412,064 | ±2% | 135 µs | 361 µs | 489 µs | 0 |
| 10 large message (64 KiB) | `4p/4part/b4/65536B/leader` | 5,446 (340 MiB/s) | ±36% | 622 µs | 1,990 µs | 477,888 µs | 0 |
| 11 acks=none | `4p/4part/b100/128B/none` | 2,256,356 | ±1% | 142 µs | 456 µs | 668 µs | 0 |
| 12 `fsync.mode=data` | `4p/4part/b100/128B/leader` | 2,261,126 | ±2% | 144 µs | 441 µs | 606 µs | 0 |
| 13 baseline: mutex queue | `4p/1part/b1/128B` (in-process) | 3,277,785 | ±36% | 0 µs | 14 µs | 25 µs | 0 |

Scenario 04's ±89% spread means its median carries no useful precision. It is
listed for completeness and no claim is made from it.

Cluster-level measurements, 3 brokers at replication factor 3, 200,000 records:

| Metric | Measured |
|---|---|
| Crash recovery after `SIGKILL` | 0.051 s to accept connections, 0.052 s to serve its full log |
| Records before kill / after recovery | 205,000 / 205,000, intact |
| Replication lag under load | p50 3,400 records, p99 28,700, max 28,700 |
| Followers caught up once load stopped | yes, within 1 ms |
| Peak CPU, all three brokers | 58.8% of 4 cores (mean 47.6%) |
| Peak resident memory, all three brokers | 41.1 MiB |
| Disk written | 484 B per 128-byte record |

484 bytes per record is what replication factor 3 costs: roughly 155 bytes on
the wire (128-byte value plus 27 bytes of framing) written three times.

### 3.2 Apple M2 (development machine — reported separately)

Not comparable to §3.1 and never averaged with it. Spreads are shown as
absolute ranges because they are too wide for a percentage to be meaningful.

| Scenario | Config | records/s | spread | p50 | p99 |
|---|---|---:|---:|---:|---:|
| 01 single producer | `1p/1part/b100/128B/leader` | 1,036,382 | 823k–1,157k | 37 µs | 871 µs |
| 02 multi-producer, one partition | `4p/1part/b100/128B/leader` | 2,490,528 | 1,404k–2,795k | 76 µs | 2,427 µs |
| 06 quorum ack | `4p/4part/b100/128B/quorum` | 27,761 | 23k–40k | 13,730 µs | 22,495 µs |
| 09 small message (16 B) | `4p/4part/b200/16B/leader` | 4,009,087 | 3,821k–5,821k | 59 µs | 3,052 µs |
| 13 baseline: mutex queue | `4p/1part/b1/128B` (in-process) | 4,896,785 | 3,326k–5,370k | <1 µs | 14 µs |

The quorum figure is ~8× worse than Linux's. That is what `F_FULLFSYNC` costs
on APFS, and it is a property of the platform rather than of the engine.

### 3.3 Against the stated targets

Judged on the Linux x86-64 results only.

| Target | Result |
|---|---|
| >500,000 records/s on a single local broker, small batched messages | **Met.** 869k single-producer, 2.24M with four producers, 4.41M with 16-byte values |
| Sub-millisecond median publish latency in memory-backed mode | **Met.** p50 is 80–303 µs across every non-quorum scenario |
| Low-single-digit-millisecond p99 in durable mode | **Met on Linux.** Quorum p99 is 2.6 ms, p99.9 2.7 ms. Not met on macOS, where `F_FULLFSYNC` puts it at ~22 ms (§3.2) |
| Recover partition state within seconds after abrupt restart | **Met with room to spare.** 0.052 s to serve a 205,000-record log after `SIGKILL` |
| Scale throughput as partitions and workers increase | **Partially.** Producer count scales cleanly; partition count does not (§4.2) |
| Demonstrable gains from batching and buffer reuse | **Met.** Batching is a 47× effect on Linux, buffer pooling 30× (§4.1, §7) |

---

## 4. What the numbers show

Unless a subsection says otherwise, these are the Linux x86-64 results from
§3.1. Where a sweep was only ever run on the laptop, it is labelled.

### 4.1 Batching dominates everything else

From §3.1, four producers, 4 partitions, `acks=leader`, 128-byte values:

| Batch size | records/s | p50 | p99 |
|---:|---:|---:|---:|
| 1 | 47,905 | 80 µs | 150 µs |
| 100 | 2,242,462 | 144 µs | 507 µs |

**Batching 100 records is a 47× throughput increase for 64 µs of added p50.**
Nothing else measured here moves throughput by a comparable factor.

The reason is visible in the batch-1 row rather than in the broker: at 80 µs
p50 with one round trip per record, four closed-loop producers are bounded at
roughly 50k records/s by latency alone, however fast the broker is. Batching
is what removes the round trip from the per-record cost.

A finer sweep on the Apple M2 (single producer, one partition) shows where the
curve turns:

| Batch size | records/s | p50 | p99 |
|---:|---:|---:|---:|
| 1 | 32,434 | 29 µs | 45 µs |
| 10 | 256,610 | 30 µs | 86 µs |
| 100 | 1,338,759 | 38 µs | 161 µs |
| 500 | 1,642,227 | 74 µs | 4,845 µs |

Batch 500 buys 23% more throughput for a 30× worse p99. Beyond roughly 100
records per request, queueing delay behind a large batch dominates. This sweep
has not been repeated on Linux; the shape is what it is offered for.

### 4.2 Producers scale; partitions do not

Producer scaling, Apple M2, batch 1, `acks=leader`, 4 partitions:

| Producers | records/s | p50 | p99 |
|---:|---:|---:|---:|
| 1 | 36,534 | 22 µs | 50 µs |
| 2 | 54,799 | 27 µs | 134 µs |
| 4 | 76,449 | 37 µs | 133 µs |
| 8 | 117,829 | 54 µs | 141 µs |

Throughput rises 3.2× from 1 to 8 producers while p50 rises 2.5× — the system
trades latency for concurrency, which is what a closed-loop load generator
against a queueing system should do.

**Partition count buys nothing measurable on either platform**, but for
different reasons and by different margins:

| | 1 partition | 8 partitions |
|---|---:|---:|
| Linux x86-64 | 2,392,197 ±5% | 2,196,040 ±14% |
| Apple M2 | 2,490,528 | 1,142,777 |

On Linux the two are within their spreads — **no difference is claimed**. On
the laptop, 8 partitions were roughly half as fast, which is consistent with a
single shared disk: more open segment files, flush work spread across more of
them, less batching per file. The Linux host does not show that penalty, so it
was a property of that machine rather than of partitioning as such.

Neither result argues for many partitions on one broker. Partitions exist here
for distribution across brokers and for consumer-group parallelism, and this is
the measurement that says so.

### 4.3 Small messages are framing-bound, large messages are device-bound

* 16-byte values reach **4.41M records/s but only 181 MiB/s** — per-record
  framing (27 bytes) is larger than the payload, so the engine is doing
  bookkeeping, not moving data.
* 64 KiB values reach **5,446 records/s and 340 MiB/s** with a p50 of 622 µs,
  a p99 of 2.0 ms, and a **p99.9 of 478 ms**. Each request carries 256 KiB, so
  a segment rotation or a flush stall lands squarely on one request and shows
  up undiluted in the far tail. The ±36% spread is the widest of any scenario
  except 04. The p99.9 is reported rather than trimmed: it is what a client
  sending 64 KiB records would occasionally see.

### 4.4 The mutex-queue baseline, and what it is not

The suite includes an in-process, mutex-protected `std::vector` with no
networking, no protocol, no checksums and no disk. On the Linux x86-64 host it
reaches 3,277,785 records/s (±36%); PulseLog reaches 2,242,462 (±2%) on the
same host with `acks=leader`.

**This ratio is not a result and should not be quoted as one.** Three reasons:

1. The baseline provides none of the guarantees being measured — no
   durability, no replication, no ordering across processes, no network. A
   ratio against it is a ratio against a different problem.
2. The baseline carries a ±36% spread. Any ratio derived from it
   inherits both, which makes a two-significant-figure percentage meaningless.
3. The number moves with the baseline's own noise rather than with anything
   about PulseLog. A run where the mutex queue happened to be slow would
   "improve" the ratio while nothing about the engine changed.

What the baseline is genuinely for: bounding how much of the per-record cost is
unavoidable queueing overhead versus protocol, checksum and storage work. Read
alongside the stage breakdown in §5, it says the engine is not losing an order
of magnitude to its own bookkeeping. That is the whole claim.

---

## 5. Where durable-mode latency goes

On Linux, `acks=quorum` runs at 239,527 records/s with a p50 of 1.6 ms and a
p99 of 2.6 ms — about 9× less throughput and 5× the p99 of `acks=leader`. This
section says where that time actually goes, because "durable writes are slower"
is not an explanation and cannot be acted on.

### 5.1 The stage breakdown

The broker times four stages of the produce path into separate histograms, so a
tail can be attributed rather than guessed at. Linux x86-64, `acks=quorum`,
3 replicas, p99 per stage:

| Stage | p99 | Share of the total |
|---|---:|---|
| Worker queue wait | 275 µs | ~6% |
| Log append | 144 µs | ~3% |
| **Leader's own fsync** | **2,193 µs** | **~46%** |
| Waiting for a quorum to flush | 2,070 µs | ~44% |

The leader's own fsync is the largest single term and the replication wait —
which is itself mostly the *followers'* fsyncs — is nearly as large. Together
the disk accounts for roughly 90% of durable-mode latency, and their relative
split moves between runs while their sum does not. Queueing and the append
itself are noise by comparison.

That also means the remaining headroom is in the storage device, not in the
acknowledgement path, the protocol, or the thread handoffs.

### 5.2 What was tried, and what did not work

Five configurations were measured against the default, 5 trials each, on the
Linux host. None of them weakens a durability guarantee: group commit changes
how many records share one fsync, never what is acknowledged before being
flushed.

| Configuration | records/s | spread | p50 | p99 | verdict |
|---|---:|---:|---:|---:|---|
| Baseline: 2 ms / 200-record group commit | 138,196 | ±10% | 2,814 µs | 4,686 µs | — |
| Tighter group commit: 1 ms / 50 records | 148,994 | ±11% | 2,742 µs | 3,514 µs | no difference measured — see below |
| Wider group commit: 10 ms / 2000 records | 74,708 | ±33% | 4,964 µs | 10,863 µs | **46% slower** |
| `fdatasync` instead of `fsync` | 132,680 | ±3% | 2,818 µs | 6,443 µs | no improvement measured |
| Preallocation disabled | 137,172 | ±9% | 2,824 µs | 4,772 µs | no difference measured |
| fsync inside every append | 141,980 | ±13% | 1,530 µs | 22,168 µs | p99 5× worse |

**No configuration tested beat the default.** Three results deserve to be
stated rather than quietly dropped:

* **`fdatasync` did not help.** The expected saving is the metadata write that
  `fsync` performs and `fdatasync` skips. It did not appear above the noise
  (132,680 ±3% against 138,196 ±10% — the ranges overlap). Disabling
  preallocation also produced no measurable change, which points the same way:
  on this filesystem and virtual disk the data write dominates and the metadata
  cost is not what is limiting the result. **No speedup is claimed for
  `fdatasync` on this hardware.**

* **Widening the group-commit window made things worse.** A longer window puts
  more records behind one fsync, but every acknowledgement then waits longer
  for that fsync to *start*. For a closed-loop producer that is a straight
  loss — 46% throughput and a doubled p99. Tightening the window did nothing
  measurable in the other direction, which places the default near the useful
  end of the curve already.

* **`fsync` inside every append trades the median for the tail.** p50 improves
  to 1.53 ms because no request waits for a batching window, but p99 degrades
  to 22.2 ms because every request now pays a full device commit. The stage
  breakdown shows why: local flush drops to 78 µs while queue wait and append
  jump to ~21 ms each, since appends now serialise behind each other's syncs.

The durable p99 of 2.6 ms therefore stands as measured, with its cause
identified, rather than being tuned down by weakening what an acknowledgement
promises.

### 5.3 Supporting measurement: append and fsync in isolation

`bench_storage`, no network, on the Apple M2 (this micro-benchmark has not been
re-run on Linux, and the absolute values are macOS `F_FULLFSYNC` costs):

| Configuration | throughput |
|---|---:|
| Append, no fsync, batch 1 | 641k records/s |
| Append, no fsync, batch 100 | 8.7M records/s |
| Append, no fsync, batch 1000 | 23.2M records/s (3.4 GiB/s) |
| Append + `F_FULLFSYNC` per append, batch 1 | 10.1k records/s |
| Append + `F_FULLFSYNC` per append, batch 100 | 417k records/s |

The shape is the point: the append path itself sustains millions of records per
second, so anything slower than that in durable mode is the device, not the
engine. The Linux stage breakdown in §5.1 confirms this directly on the
platform that matters.

### 5.4 Why `full` remains the default

`storage.fsync.mode=full` is the default because it is what quorum
acknowledgements are *defined against*: a broker that acknowledges "durable"
without reaching stable media is lying. `data` is offered because on Linux
`fdatasync` is a genuinely different syscall with a weaker, well-understood
guarantee — but as measured above it bought nothing here, so selecting it
trades a guarantee for no gain.

On macOS the distinction is different again: `mode=data` falls through to plain
`fsync`, which does not survive power loss at all. See
[PORTABILITY.md §2](PORTABILITY.md#2-durability-the-difference-that-matters-most).

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

Each of these was found by measurement, not by inspection. The table indexes
every optimisation in the engine, including the ones where **no improvement was
measured** — those are listed for the same reason as the rest, because an
optimisation nobody can show a number for is a claim, not a result.

| Optimisation | Measured effect | Where |
|---|---|---|
| Producer batching (batch 1 → 100) | **47×** throughput, +64 µs p50 | §4.1 |
| Producer partitioning fix (sticky + randomised start) | **6.9×** throughput, p99 80× better | §8.2 |
| Sparse offset index + bounded block read | **18×** offset lookup, **16×** log read | §8.1 |
| Buffer pooling vs per-request allocation | **30×** cheaper acquire/release | §7 |
| Hardware CRC-32C vs software table | **5.2×** | §7 |
| Skipping payload re-verification on the read path | 7,538 ns → 8.2 ns at 64 KiB | §7 |
| `writev` response coalescing | **not isolated** — see §8.3 | §8.3 |
| Group commit window (tighter than default) | **no difference measured** | §5.2 |
| Group commit window (wider than default) | **47% slower** — the change was reverted | §5.2 |
| `fdatasync` instead of `fsync` | **no improvement measured** | §5.2 |
| Segment preallocation | **no difference measured** on the Linux host | §5.2 |
| Kernel socket buffer sizing | not a throughput change; bounds kernel memory per connection and makes backpressure testable | [PORTABILITY.md §4](PORTABILITY.md#4-sockets) |

Four entries say "no difference measured" rather than reporting a small number.
In each case the two configurations differed by less than their run-to-run
spreads across 5 trials, and repeated runs did not preserve the ordering. **No
speedup is claimed for any of them.**

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

### 8.3 `writev` for queued responses — no isolated measurement

Queued responses are coalesced into a single `sendmsg` per flush rather than one
syscall per response.

**This has not been measured in isolation and no speedup is claimed for it.**
On the produce path there is usually one response per flush, so there is
nothing to coalesce and the change is a no-op; the win only exists under
pipelining, and the benchmark suite's closed-loop producers do not pipeline.
Building a scenario that isolates it would mean writing an open-loop driver
that does, which has not been done.

What can be observed instead of asserted: `Connection::Stats` exposes
`writev_calls` against `frames_sent`, so the actual coalescing ratio is
readable at runtime on a real workload rather than assumed from the code.

---

## 9. What has not been measured

Stated so the absence is not mistaken for a result:

* **No comparison against Kafka, Redpanda or NATS.** A fair comparison needs
  matched durability settings, matched batching, and a machine that is not a
  laptop. An unfair one would be worse than none.
* **No open-loop latency measurement.** The driver is closed-loop, so the
  percentiles do not capture what a fixed arrival rate would see under
  saturation, and they understate the tail accordingly.
* **No multi-machine cluster.** All "3 broker" results are three processes on
  one host sharing one disk and the loopback interface, which flatters the
  network and penalises the disk. Replication lag and recovery figures should
  be read with that in mind.
* **No isolated `writev` measurement** (§8.3).
* **No physical network anywhere.** Everything is loopback, so every latency
  here excludes NIC and switch time.
* **No dedicated benchmark hardware.** The Linux figures come from a shared
  GitHub-hosted VM. The spreads are tight, but the machine is not isolated and
  the disk is virtual.
* **No `io_uring` numbers.** That backend is not implemented.
* **No big-endian results.** The byteswap paths compile but no CI runner
  exercises them.
