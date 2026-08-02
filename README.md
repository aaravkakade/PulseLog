# PulseLog

A low-latency distributed event-streaming engine in C++20. Partitioned
append-only logs, leader/follower replication, consumer groups, a custom binary
protocol, and a custom asynchronous networking layer — built from first
principles, not wrapped around an existing broker.

Every performance number below was measured by running the code in this
repository on the machine described in
[PERFORMANCE_RESULTS.md](docs/PERFORMANCE_RESULTS.md). Nothing is projected.

```
 producers ──▶┌──────────────────────────────────────────────┐
              │  accept ─▶ io loops ─▶ partition workers ─▶  │──▶ segmented
 consumers ──▶│              ▲              │      logs      │    append-only
              │              └── responses ─┘                │    log files
              │  flusher · replicator · maintenance · metrics│
              └───────────────────┬──────────────────────────┘
                                  │ leader → follower replication
                    ┌─────────────┴─────────────┐
                    ▼                           ▼
              ┌──────────┐                ┌──────────┐
              │ broker 2 │                │ broker 3 │
              └──────────┘                └──────────┘
```

---

## What it does

* **Partitioned topics** with hash-based key routing and per-partition ordering
* **Segmented append-only storage** with sparse offset/time indexes, CRC-32C on
  every record, batched `fsync`, retention and crash recovery
* **Leader/follower replication** with a high-water mark, in-sync replica
  tracking and quorum acknowledgements
* **Three durability levels** — `none`, `leader`, `quorum` — each promising
  something specific ([FAILURE_SEMANTICS.md](docs/FAILURE_SEMANTICS.md))
* **Consumer groups** with range and round-robin assignment, heartbeats,
  session expiry, generation fencing and durable offset commits
* **A custom binary protocol** with header and payload checksums, 17 operations
  and a documented compatibility strategy
* **An asynchronous networking layer** on epoll (Linux) or kqueue (macOS), with
  two-level backpressure and no thread-per-connection
* **Prometheus metrics**, structured logs and a live terminal dashboard
* **A benchmark suite** producing machine-readable JSON and charts, plus
  failure-injection tooling that asserts documented behaviour

### Status of each claim

Because "supports X" can mean four different things, this table says which one
applies. **Measured** means a number in
[PERFORMANCE_RESULTS.md](docs/PERFORMANCE_RESULTS.md) produced by running the
code; **verified** means an automated test asserts the behaviour; **implemented**
means it works and is exercised, but no number or dedicated test isolates it.

| Capability | Status | Evidence |
|---|---|---|
| Partitioned topics, key routing, per-partition ordering | verified | unit + integration tests |
| Segmented log, sparse indexes, CRC-32C, retention | measured | 18× offset lookup, 16× log read |
| Crash recovery from a torn tail | measured + verified | 0.052 s for 205k records; failure-injection suite |
| Leader/follower replication, high-water mark, ISR | measured + verified | 1.00M records/s at RF=3; cluster failure suite |
| Quorum acknowledgement (durable) | measured + verified | 240k records/s, p99 2.6 ms on Linux |
| `acks=none` / `acks=leader` (memory-backed) | measured | 2.26M / 2.24M records/s, indistinguishable — see below |
| Consumer groups, assignment, heartbeats, offset commits | verified | integration tests + Linux smoke test |
| Group coordinator routing | verified | regression test after a real bug |
| Binary protocol, dual checksums, 17 opcodes | verified | round-trip and malformed-frame tests |
| epoll backend | measured + verified | CI asserts `poller=epoll`; all Linux numbers |
| kqueue backend | implemented | used for all local development; not covered by CI |
| Two-level backpressure | verified | bounded-memory assertions under overload |
| Prometheus metrics, dashboard | implemented | scraped by the benchmark tooling |
| Raft, leader election | **design sketch only** | [REPLICATION.md §5](docs/REPLICATION.md) — not implemented |
| `io_uring` backend | **not implemented** | — |

**`acks=leader` is not a durability guarantee.** It means the record is in the
leader's log, not that it reached the disk — which is why it measures the same
as `acks=none` (2.19M vs 2.22M records/s, inside the spread). Only `acks=quorum`
waits for a flush on a majority, and it costs about 10× the throughput. The
three levels and what each actually promises are in
[FAILURE_SEMANTICS.md](docs/FAILURE_SEMANTICS.md).

## What it does not do

Stated up front so nothing here has to be inferred:

* **No leader election.** Leadership is statically assigned; a dead leader
  means its partitions are unavailable for writes until it returns.
* **No exactly-once.** Delivery is at-least-once; no idempotent producers, no
  transactions.
* **No security.** No TLS, no authentication, no authorisation.
* **No log compaction, no tiered storage, no `io_uring` backend.**

The full list is in [FAILURE_SEMANTICS.md](docs/FAILURE_SEMANTICS.md) §8.

---

## Quick start

```bash
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j
ctest --test-dir build --output-on-failure      # 244 tests

./build/bin/pulselog-broker --broker.data.dir=/tmp/pulselog &

./build/bin/pulselog-cli create-topic orders --partitions=4
./build/bin/pulselog-cli produce orders --key=user-1 --value=hello --count=1000 --batch=100
./build/bin/pulselog-cli consume orders --partition=0 --offset=earliest --max=10
./build/bin/pulselog-cli consume-group orders --group=analytics --max=100

curl -s localhost:9644/metrics | head
python3 scripts/dashboard.py --brokers=127.0.0.1:9644
```

Requires CMake ≥ 3.20 and a C++20 compiler (tested with Apple clang 15 and
GCC 13). GoogleTest and Google Benchmark are fetched at configure time; pass
`-DPULSELOG_OFFLINE=ON` to build the engine without them.

### A three-broker cluster

```bash
docker compose -f docker/docker-compose.yml up --build

docker compose -f docker/docker-compose.yml exec pulselog-1 \
  pulselog-cli create-topic events --partitions=6 --replication=3 --brokers=pulselog-1:9092
docker compose -f docker/docker-compose.yml exec pulselog-1 \
  pulselog-cli produce events --value=hi --count=1000 --acks=quorum --brokers=pulselog-1:9092
docker compose -f docker/docker-compose.yml exec pulselog-2 \
  pulselog-cli metadata events --brokers=pulselog-2:9092

python3 scripts/dashboard.py --brokers=127.0.0.1:9644,127.0.0.1:9645,127.0.0.1:9646
```

Or without Docker, on one host:

```bash
CLUSTER=1@127.0.0.1:9092,2@127.0.0.1:9093,3@127.0.0.1:9094
for i in 1 2 3; do
  ./build/bin/pulselog-broker --broker.id=$i \
    --net.listen=127.0.0.1:$((9091 + i)) --net.advertised.port=$((9091 + i)) \
    --broker.data.dir=/tmp/pulselog-$i --metrics.port=$((9643 + i)) \
    --cluster.brokers=$CLUSTER &
done
```

---

## Producing and consuming

**C++** ([clients/cpp](clients/cpp)):

```cpp
#include "pulselog/client/client.h"
using namespace pulselog;

client::ClientConfig config;
config.bootstrap_servers = {"127.0.0.1:9092"};
client::ClientContext context(config);

client::ProducerConfig producer_config;
producer_config.acks = AckMode::kQuorum;   // a majority has flushed it
producer_config.batch_records = 100;
client::Producer producer(context, producer_config);

client::OutboundRecord record;
record.key = "user-1";
record.key_is_null = false;
record.value = "order placed";
auto result = producer.Send("orders", record);
if (!result.ok()) return result.status();
```

```cpp
client::ConsumerConfig consumer_config;
consumer_config.group_id = "analytics";
consumer_config.topics = {"orders"};
client::Consumer consumer(context, consumer_config);
PL_RETURN_IF_ERROR(consumer.Join());

for (;;) {
  PL_ASSIGN_OR_RETURN(auto records, consumer.Poll());
  for (const auto& record : records) {
    Handle(record.key, record.value);
  }
  if (!records.empty()) PL_RETURN_IF_ERROR(consumer.Commit());
}
```

**Python** ([clients/python](clients/python)) — for scripting and demos:

```python
from pulselog import Client

with Client("127.0.0.1:9092") as client:
    client.create_topic("orders", partitions=4)
    client.produce("orders", value=b"hello", key=b"user-1", acks="quorum")
    for record in client.consume("orders", partition=0, offset=0, max_records=10):
        print(record.offset, record.value)
```

---

## Measured performance

Every number below was measured on the hardware named with it. Nothing is
transferred between platforms, and each figure carries the spread across
trials, because a median without a spread implies a precision these
measurements do not have.

### Linux x86-64 — the validated platform

GitHub-hosted runner: AMD EPYC 9V74, 4 logical cores, 15.6 GiB, ext4 on an
Azure virtual disk, Ubuntu 24.04, kernel 6.17, GCC 13.3, `-O2 -g -DNDEBUG`.
Median of 5 trials. Latency is **per produce request** — with `batch=100` one
request carries 100 records, so these are not per-record figures.

| Scenario | records/s | spread | p50 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|
| 4 producers, 4 partitions, batch 100 | 2,242,462 | ±2% | 144 µs | 507 µs | 646 µs |
| 4 producers, 16-byte values, batch 200 | 4,412,064 | ±2% | 135 µs | 361 µs | 489 µs |
| Single producer, 1 partition, batch 100 | 869,072 | ±1% | 100 µs | 157 µs | 493 µs |
| 3 brokers, replication factor 3 | 1,003,379 | ±3% | 303 µs | 1,344 µs | 2,708 µs |
| `acks=quorum` (durable, 3 replicas) | 239,527 | ±2% | 1,564 µs | 2,560 µs | 2,652 µs |
| No batching (batch 1) | 47,905 | ±10% | 80 µs | 150 µs | 233 µs |
| 4 producers, 64 KiB values | 5,446 (340 MiB/s) | ±36% | 622 µs | 1,990 µs | 478 ms |

Cluster behaviour, same hardware, 3 brokers at replication factor 3:

| Metric | Measured |
|---|---|
| Crash recovery after `SIGKILL` | 0.052 s to serve its full log again, 205,000 records intact |
| Replication lag under sustained load | p50 3,400 records, p99 28,700 |
| Followers caught up after load stopped | yes, within 1 ms |
| Peak CPU across all three brokers | 58.8% of 4 cores |
| Peak resident memory across all three | 41.1 MiB |
| Disk written per 128-byte record | 484 B (3 replicas × ~155 B on the wire) |

Reading these honestly:

* **Batching is the dominant effect.** 47,905 → 2,242,462 records/s from batch
  1 to batch 100, for 64 µs of added p50. Nothing else in this table moves
  throughput by a comparable factor.
* **`acks=leader` and `acks=none` are indistinguishable** (2,242,462 vs
  2,256,356, inside their spreads). That is not a surprise, it is the
  definition: `acks=leader` means the record is in the leader's log, and does
  not wait for a flush. Only `acks=quorum` waits for durability.
* **Durability costs about 9× throughput and about 5× p99** on this hardware
  (2.24M → 240k records/s, 507 µs → 2,560 µs). Where that time goes is broken
  down below.
* **More partitions did not help** — 2,196,040 across 8 partitions versus
  2,242,462 across 4 is inside the spread.
* **The 64 KiB p99.9 of 478 ms is real** and comes from segment rotation
  colliding with a large write on a virtual disk. It is reported rather than
  trimmed.

### Where durable-mode latency actually goes

The broker records the produce path in four stages, so a durable tail can be
attributed instead of guessed at. Linux x86-64, `acks=quorum`, 3 replicas,
p99 per stage:

| Stage | p99 | Share |
|---|---:|---|
| Worker queue wait | 275 µs | ~6% |
| Log append | 144 µs | ~3% |
| **Leader's own fsync** | **2,193 µs** | **~46%** |
| Waiting for a quorum to flush | 2,070 µs | ~44% |

The disk accounts for roughly 90% of it. Five tuning changes were measured
against this baseline, each over 5 trials:

| Change | records/s | vs baseline |
|---|---:|---|
| Baseline (2 ms / 200-record group commit) | 138,196 ±10% | — |
| Tighter group commit (1 ms / 50 records) | 148,994 ±11% | no change — direction reversed between runs |
| Wider group commit (10 ms / 2000 records) | 74,708 ±33% | **46% slower** |
| `fdatasync` instead of `fsync` | 132,680 ±3% | no improvement |
| Preallocation disabled | 137,172 ±9% | no change (inside spread) |
| fsync inside every append | 141,980 ±13% | p99 5× worse (22.2 ms) |

**No configuration tested beat the default.** Two results are worth stating
plainly rather than quietly dropping:

* **`fdatasync` did not help.** The expected saving is the metadata write that
  `fsync` performs and `fdatasync` skips; it did not show up above the noise
  here. Disabling preallocation also made no measurable difference, which
  points the same way — on this filesystem and virtual disk the data write
  dominates and the metadata cost is not the bottleneck.
* **Widening the group commit window made things worse, not better.** A longer
  window means more records share an fsync, but every acknowledgement then
  waits longer for that fsync to start. For a closed-loop producer that is a
  straight loss.
* **Tightening it produced opposite results in two consecutive CI runs**
  (136,810 vs a 138,427 baseline, then 148,994 vs 138,196). The ordering
  reversed and both gaps sit inside the spreads, so no speedup is claimed —
  what the pair shows is that the default sits on a flat part of the curve.

The durable p99 of 2.6 ms therefore stands as measured, with its cause
identified, rather than being tuned down by weakening what an acknowledgement
promises.

### Apple M2 — development machine, not a benchmark host

Reported separately and never merged with the Linux figures. This is a
thermally constrained laptop, spreads reach ±80%, and macOS `F_FULLFSYNC` is a
fundamentally different operation from Linux `fsync` (see
[PORTABILITY.md](docs/PORTABILITY.md#2-durability-the-difference-that-matters-most)),
so durable numbers are not comparable across the two at all.

Apple M2 (8 cores), macOS 14.5, APFS, Apple clang 15, `-O2`, median of 5 trials:

| Scenario | records/s | spread | p50 | p99 |
|---|---:|---:|---:|---:|
| 4 producers, 1 partition, batch 100 | 2,490,528 | 1,404k–2,795k | 76 µs | 2.4 ms |
| Single producer, 1 partition, batch 100 | 1,036,382 | 823k–1,157k | 37 µs | 871 µs |
| `acks=quorum` (durable) | 27,761 | 23k–40k | 13.7 ms | 22.5 ms |

The durable figure is roughly 8× worse than Linux's, which is what
`F_FULLFSYNC` costs on APFS. It is a property of the platform, not of the
engine.

### Baselines and micro-benchmarks

An in-process, mutex-protected queue with no networking, no protocol, no
checksums and no disk reaches 3,277,785 records/s on the same Linux host
(±36%). It is included in the suite as a ceiling to measure against, not as a
headline: it provides none of the guarantees PulseLog does, and the comparison
says nothing about either system's absolute performance. See
[BENCHMARKING.md](docs/BENCHMARKING.md) for what it does and does not tell you.

Micro-benchmarks behind specific decisions: hardware CRC-32C is **5.2×** the
software path, buffer pooling is **30×** cheaper than allocating per request,
and fixing a per-record `pread` in the offset scan made lookups **18×** faster.

### Reproducing all of it

```bash
./scripts/benchmark_release.sh
```

One command: verifies prerequisites, builds an optimised binary, records the
hardware, runs every scenario in `benchmarks/config/release.json` for 5 trials,
measures replication lag and recovery time, and writes JSON, CSV, charts and a
report. The Linux figures above come from this script running in CI on every
push; the raw artifacts are attached to each run.

---

## Failure guarantees

| Guarantee | Verified by |
|---|---|
| A record is either wholly present and checksum-valid, or gone | `failure_test.sh` case 2 |
| `acks=quorum` records survive SIGKILL | `failure_test.sh` case 1 — 400/400 survived |
| A torn write costs exactly one record | `failure_test.sh` case 3 |
| Recovery is bounded by index interval, not log size | 0.12 s for a 400-record partition |
| Order within a partition, per producer | `ConcurrentProducersToOnePartitionKeepEveryRecord` |
| Replicas are byte-identical | `ReplicatedBytesAreIdentical` |
| Quorum refuses rather than degrades | `QuorumFailsRatherThanDegradingWhenReplicasAreDown` |
| Overload is refused, memory stays bounded | `failure_test.sh` case 5 — 41 MiB growth, then flat |

```bash
scripts/smoke_test.sh      # end-to-end CLI exercise plus a restart
scripts/failure_test.sh    # asserts the documented failure behaviour
python3 scripts/chaos.py --help
```

---

## Repository layout

```
include/pulselog/   public headers, one directory per module
src/
  base/             Status/Result, buffers, CRC-32C, config, logging
  concurrency/      SPSC ring, bounded MPMC queue, blocking queue, threads
  protocol/         frame layout, opcodes, record format, message codecs
  storage/          segmented append-only log, indexes, recovery, retention
  net/              poller (epoll/kqueue), event loop, connections, framing
  metadata/         topics, partition assignment, cluster routing
  replication/      leader push, follower progress, high-water mark
  consumer/         group coordinator, assignors, durable offset store
  metrics/          HDR histogram, registry, Prometheus endpoint
  broker/           request routing, partition ownership, workers, lifecycle
clients/cpp/        C++ client SDK
clients/python/     Python client (scripting and demos)
apps/               pulselog-broker, pulselog-cli, pulselog-bench
tests/              unit, integration (real in-process brokers)
benchmarks/         Google Benchmark micro-benchmarks
scripts/            benchmarks, charts, chaos, dashboard, smoke/failure tests
docker/             image and three-broker compose topology
docs/               design documents
```

## Documentation

| Document | Contents |
|---|---|
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | Module layering, thread model, request lifecycle |
| [PROTOCOL.md](docs/PROTOCOL.md) | Byte-level frame layout, opcodes, compatibility |
| [STORAGE_ENGINE.md](docs/STORAGE_ENGINE.md) | Segment format, indexing, recovery, retention |
| [REPLICATION.md](docs/REPLICATION.md) | Leader/follower protocol, high-water mark, Raft sketch |
| [CONCURRENCY_MODEL.md](docs/CONCURRENCY_MODEL.md) | Ownership, memory ordering, shutdown |
| [BENCHMARKING.md](docs/BENCHMARKING.md) | How to reproduce every number |
| [PERFORMANCE_RESULTS.md](docs/PERFORMANCE_RESULTS.md) | Measured results with full conditions |
| [FAILURE_SEMANTICS.md](docs/FAILURE_SEMANTICS.md) | What survives what, and what does not |
| [PORTABILITY.md](docs/PORTABILITY.md) | Where Linux and macOS differ, and which guarantees change |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Build, test, style |
| [SECURITY.md](SECURITY.md) | Threat model and the absence of authn/authz |

## Build options

| Option | Default | Meaning |
|---|---|---|
| `PULSELOG_BUILD_TESTS` | `ON` | Unit and integration tests |
| `PULSELOG_BUILD_BENCHMARKS` | `ON` | Google Benchmark micro-benchmarks |
| `PULSELOG_BUILD_APPS` | `ON` | broker, CLI, benchmark driver |
| `PULSELOG_WERROR` | `OFF` | Warnings as errors (CI sets `ON`) |
| `PULSELOG_SANITIZER` | `""` | `address`, `thread`, `undefined`, `address+undefined` |
| `PULSELOG_OFFLINE` | `OFF` | Skip fetching test/benchmark dependencies |

## Roadmap

- [x] Base layer, concurrency primitives, binary protocol
- [x] Segmented storage with recovery and retention
- [x] Asynchronous networking with backpressure
- [x] Partitioning, metadata, single-owner topic creation
- [x] Leader/follower replication with quorum acknowledgements
- [x] Consumer groups with durable offsets
- [x] Metrics, benchmarks, failure injection, Docker, CI
- [ ] Raft leader election (designed in REPLICATION.md, not implemented)
- [ ] Idempotent producer IDs
- [ ] `io_uring` backend on Linux
- [ ] Log compaction
- [ ] TLS and authentication

## License

MIT — see [LICENSE](LICENSE).
