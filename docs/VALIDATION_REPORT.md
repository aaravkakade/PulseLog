# PulseLog Validation Report

What was actually run, on what, and what it produced. Every claim here points
at an artifact that exists: a GitHub Actions run, a test count from its log, or
a JSON file attached to it. Nothing is projected and nothing is carried over
from a different machine.

**Reference run:** [Actions run 30771073182](https://github.com/aaravkakade/PulseLog/actions/runs/30771073182)
at commit `183b9c6`, all ten jobs green. Benchmark figures come from the
artifact attached to that run.

---

## 1. Platform validation

### 1.1 GitHub Actions results

| Job | Result | What it establishes |
|---|---|---|
| build+test (GCC 13.3.0, RelWithDebInfo) | **pass** | Strict optimised build, 251/251 tests |
| build+test (GCC 13.3.0, Debug) | **pass** | Assertions live, 251/251 tests |
| build+test (Clang 18.1.3, RelWithDebInfo) | **pass** | Second toolchain, 251/251 tests |
| sanitizer (Address + UndefinedBehavior) | **pass** | 251/251 plus cluster failure injection, leak detection on |
| sanitizer (Thread) | **pass** | 251/251, no data races reported |
| clang-format | **pass** | Pinned clang-format 20.1.0, 105 files |
| clang-tidy | **pass** | LLVM 18.1.3, `--warnings-as-errors='*'`, zero findings |
| Linux smoke + failure injection | **pass** | 14 smoke + 9 single-broker + 17 cluster failure checks |
| Linux x86-64 benchmarks | **pass** | 13 scenarios × 5 trials plus durability profile |
| Docker image + 3-broker cluster | **pass** | Image builds, real cluster serves quorum writes |

Every build uses `-Werror`. Both compilers are whatever `ubuntu-latest` ships;
each job prints the version it used rather than pinning to one the image might
not have.

### 1.2 Linux is validated; macOS is developed on

The epoll backend, `fdatasync`, Linux socket-buffer autotuning and GCC's
warning set exist only on Linux, and all four produced real bugs that no macOS
run could have found (§4). CI asserts the broker actually selected epoll rather
than assuming it — a Linux build that somehow chose kqueue would pass every
test and validate nothing.

macOS is supported and exercised locally by `scripts/verify_release.sh`, but
the GCC build and the epoll path are not run there, and the script says so at
the end of every run so a green local result cannot be mistaken for Linux
validation.

Full platform differences: [PORTABILITY.md](PORTABILITY.md).

---

## 2. Test inventory

251 automated test cases, all passing under every configuration above.

| Suite | Cases | Covers |
|---|---:|---|
| `test_protocol` | 65 | Frame layout, CRCs, record codec, all 17 opcodes, malformed input |
| `test_base` | 43 | Status/Result, buffers, config parsing, CRC-32C, endianness |
| `test_storage` | 39 | Segments, sparse indexes, recovery, truncation, retention, flush accounting |
| `test_net` | 32 | Sockets, event loop, framing, backpressure, connection lifecycle |
| `test_metrics` | 25 | Counters, gauges, HDR histograms, Prometheus exposition |
| `test_concurrency` | 14 | SPSC ring, bounded MPMC queue, blocking queue, thread utilities |
| `test_integration_single_broker` | 19 | End-to-end produce/consume, groups, offsets, admin |
| `test_integration_replication` | 14 | Multi-broker replication, quorum, coordinator routing, failover |

Beyond the unit and integration suites:

| Suite | Checks | Scope |
|---|---:|---|
| `linux_smoke_test.sh` | 14 | 3-broker cluster: epoll, topics, quorum, groups, restart, SIGKILL recovery, backpressure, shutdown |
| `failure_cluster_test.sh` | 17 | Replica death, freeze, resume, reconnect, leader loss, malformed frames, mid-write kill, consumer RST, draining shutdown, restart cycles |
| `failure_test.sh` | 9 | Single broker: corruption, torn writes, connection storms, overload |

### 2.1 Sanitizer results

| Sanitizer | Scope | Result |
|---|---|---|
| AddressSanitizer + UBSan | 251 tests + 17 cluster failure checks | No findings. Leak detection enabled (Linux only — macOS ASan does not support it) |
| ThreadSanitizer | 251 tests | No data races reported |

The cluster failure suite runs under ASan deliberately: the failure paths — a
follower dying mid-batch, a connection reset while a response is queued,
shutdown while workers are draining — are the least covered by unit tests and
the most likely to touch freed memory.

### 2.2 Failure-injection results

All 17 cluster checks pass, on Linux and under ASan. Representative results
from the reference run:

| Property | Measured |
|---|---|
| Quorum with one of three replicas dead | completes |
| Quorum with one replica SIGSTOPped | completes in 0.19 s |
| Resumed follower rejoins and catches up | 800 records, no operator action |
| Restarted follower re-streams from its own log end | exactly 1,200 records, no gap or duplication |
| Durable write to a partition that then goes idle | acknowledged in 0.02 s |
| Writes after the leader dies | resolve in 0.01 s (as an error — there is no election) |
| Malformed frames (5 kinds) | refused; broker stays healthy and serving |
| Broker killed mid-write | record count never goes backwards; 1,271 → 2,521 |
| 20 consumers vanishing with RST | connections reclaimed, broker keeps serving |
| SIGTERM while 3,000 durable writes are in flight | clean shutdown, no hang |
| 5 start-stop cycles | stable; 9,748 KiB resident at the end |
| All replicas after every fault above | agree exactly: 3,521 records each |

---

## 3. Benchmark results

### 3.1 Hardware

| Property | Value |
|---|---|
| CPU | AMD EPYC 9V74, 4 logical cores |
| Memory | 15.6 GiB |
| Storage | ext4 on an Azure virtual disk (kernel reports rotational; virtualised) |
| OS | Ubuntu 24.04.4 LTS, kernel 6.17.0-1020-azure |
| Compiler | GCC 13.3.0, `-O2 -g -DNDEBUG`, C++20 |
| Event loop | epoll |
| Checksum | hardware CRC-32C (SSE 4.2) |
| Commit | `183b9c6`, clean tree |

Loopback only — no NIC, no switch. Median of 5 trials; spread is
(max − min) / median. Latency is **per produce request**: with `batch=100` one
sample covers 100 records.

### 3.2 Throughput and latency

| Scenario | records/s | spread | p50 | p95 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|---:|
| 4 producers, 16-byte values, batch 200 | 4,412,064 | ±2% | 135 µs | 245 µs | 361 µs | 489 µs |
| 4 producers, 4 partitions, batch 100 (`acks=leader`) | 2,242,462 | ±2% | 144 µs | 252 µs | 507 µs | 646 µs |
| 4 producers, `acks=none` | 2,256,356 | ±1% | 142 µs | 258 µs | 456 µs | 668 µs |
| 3 brokers, replication factor 3 | 1,003,379 | ±3% | 303 µs | 808 µs | 1,344 µs | 2,708 µs |
| Single producer, 1 partition, batch 100 | 869,072 | ±1% | 100 µs | 122 µs | 157 µs | 493 µs |
| **`acks=quorum`, 3 replicas (durable)** | **239,527** | **±2%** | **1,564 µs** | **2,490 µs** | **2,560 µs** | **2,652 µs** |
| No batching (batch 1) | 47,905 | ±10% | 80 µs | 120 µs | 151 µs | 233 µs |

### 3.3 Durable versus non-durable, leader versus quorum

| | `acks=none` | `acks=leader` | `acks=quorum` |
|---|---:|---:|---:|
| Throughput | 2,256,356 | 2,242,462 | 239,527 |
| p99 | 456 µs | 507 µs | 2,560 µs |
| Waits for | nothing | the record being in the leader's log | a majority to have **flushed** |

`acks=none` and `acks=leader` are indistinguishable — both inside ±2% of each
other. That is the definition, not a surprise: neither waits for a flush.
**`acks=leader` is not a durability guarantee.** Only `acks=quorum` is, and it
costs roughly 9× throughput and 5× the p99.

### 3.4 Where durable latency goes

Broker-side stage histograms, `acks=quorum`, p99 per stage:

| Stage | p99 | Share |
|---|---:|---|
| Worker queue wait | 275 µs | ~6% |
| Log append | 144 µs | ~3% |
| Leader's own fsync | 2,193 µs | ~46% |
| Waiting for a quorum to flush | 2,070 µs | ~44% |

The disk accounts for roughly 90%. Five tuning changes were measured against
the default and **none beat it**; `fdatasync`, preallocation, and a tighter
group-commit window all produced no measurable improvement, and a wider window
was 46% slower. Details, including a case where two runs reversed the ordering,
are in [PERFORMANCE_RESULTS.md §5.2](PERFORMANCE_RESULTS.md#52-what-was-tried-and-what-did-not-work).

### 3.5 Cluster behaviour

3 brokers, replication factor 3, 200,000 records:

| Metric | Measured |
|---|---|
| Crash recovery after `SIGKILL` | 0.051 s to accept connections, **0.052 s to serve its full log** |
| Records before kill / after recovery | 205,000 / 205,000, intact |
| Replication lag under load | p50 3,400 records, p99 28,700 |
| Followers caught up once load stopped | yes, within 1 ms |
| Peak CPU, all three brokers | 58.8% of 4 cores |
| Peak resident memory, all three brokers | 41.1 MiB |
| Disk written per 128-byte record | 484 B (3 replicas × ~155 B on the wire) |

### 3.6 Optimisations, before and after

| Optimisation | Effect |
|---|---|
| Producer batching (1 → 100) | **47×** throughput, +64 µs p50 |
| Producer partitioning fix (sticky + randomised start) | **6.9×** throughput, p99 80× better |
| Sparse index + bounded block read | **18×** offset lookup, **16×** log read |
| Buffer pooling | **30×** cheaper than per-request allocation |
| Hardware CRC-32C | **5.2×** the software table |
| `writev` response coalescing | **not measured** — closed-loop driver never pipelines |
| Group commit, `fdatasync`, preallocation | **no improvement measured** |

Four entries report no improvement rather than a small number: in each case the
configurations differed by less than their spreads across 5 trials, and
repeated runs did not preserve the ordering.

---

## 4. Bugs found by this work

Each was found by a specific tool and has a regression test.

| Bug | Found by | Consequence if shipped |
|---|---|---|
| **Flush accounting race**: `Flush()` zeroed the unflushed counters, wiping accounting for records appended during the fsync. The partition then read as clean while holding unflushed records | Durable tail-latency profiling — p99.9 pinned at almost exactly 30 s, which is a timeout, not a disk | A fully durable, fully replicated write answered with `TIMEOUT` |
| **Missed waiter wake-up**: a quorum waiter was only re-evaluated on a flush or a follower report, both of which can already have happened | Same investigation | Same |
| **Group coordinator never implemented**: clients sent group requests through a rotating broker list, so a consumer joined on one broker and committed on another | Linux smoke test on a real 3-broker cluster | Offsets silently never advanced; every restart replayed everything |
| **`memcpy` with a null pointer** — undefined even at length zero | UBSan on Linux | Undefined behaviour on an empty append |
| **Docker cluster was never a cluster**: every env var used a doubled underscore, which maps to a literal underscore, so all three brokers ran on defaults | CI, on the first attempt to create a replicated topic | Three independent single-broker clusters that passed health checks |
| **Unknown config keys silently ignored** | The above | Any typo runs on defaults while looking configured |
| **`.clang-tidy` disabled nothing**: comments inside a YAML folded scalar are literal text, so seven entries were garbage | CI reporting checks the file appeared to disable | 133 findings from checks meant to be off, hiding 50 real ones |
| **`Result` accessors never trapped**: the documented debug precondition relied on `optional::operator*` checking, which no standard library does by default | clang-tidy 18 | Undefined behaviour on a contract violation, in every build |
| **Docker image unbuildable from a working tree**: no `.dockerignore`, so local build directories entered the context and a stale `CMakeCache.txt` broke the build | Local release verification | Anyone building the image locally |

---

## 5. Reproducing all of it

```bash
# Everything CI runs, locally.
./scripts/verify_release.sh

# The full benchmark suite with hardware capture, charts and a report.
./scripts/benchmark_release.sh

# Where durable-mode latency goes, plus the tuning sweep.
python3 scripts/profile_durability.py --build-dir build --trials 5

# The end-to-end suites individually.
scripts/linux_smoke_test.sh
scripts/failure_cluster_test.sh
scripts/failure_test.sh
```

Scenario parameters live in `benchmarks/config/release.json`, in version
control, so a published number traces to a reviewable diff.

---

## 6. Limitations that remain

Unchanged by this work and not buried:

* **No leader election.** Leadership is statically assigned. A dead leader
  makes its partitions unavailable for writes until it returns. Verified to
  fail promptly rather than hang (0.01 s), but it does fail.
* **No exactly-once semantics.** Delivery is at-least-once. No idempotent
  producers, no transactions.
* **No TLS, no authentication, no authorisation.**
* **No log compaction**, no tiered storage.
* **No `io_uring`.** The Linux event loop is epoll.
* **Group state has a single owner and no replication.** The coordinator is
  chosen by hashing the group id; if that broker dies, that group is
  unavailable. This is the same limitation as the absence of leader election.
* **No multi-machine cluster measurement.** All 3-broker results are three
  processes on one host sharing a disk and loopback, which flatters the network
  and penalises the disk.
* **No comparison against Kafka, Redpanda or NATS.** A fair one needs matched
  durability and batching settings on comparable hardware; an unfair one would
  be worse than none.
* **Big-endian hosts** are handled in code but never tested.
* **Windows** is not supported.
