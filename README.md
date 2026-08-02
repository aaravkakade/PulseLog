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

Apple M2 (8 cores), macOS 14.5, APFS, Apple clang 15, `-O2`. Median of 5 trials;
the spread is shown because this is a thermally constrained laptop, not a
benchmark host. Latency is **per produce request** — with `batch=100` one
request carries 100 records.

| Scenario | records/s | spread | p50 | p99 |
|---|---:|---:|---:|---:|
| Single producer, 1 partition, batch 100 | 861,344 | 785k–1,017k | 39 µs | 349 µs |
| 4 producers, 1 partition, batch 100 | 1,850,569 | 872k–2,392k | 84 µs | 2.6 ms |
| 4 producers, 16-byte values, batch 200 | 3,119,652 | 2,691k–3,256k | 87 µs | 3.6 ms |
| 4 producers, 64 KiB values | 6,619 (414 MiB/s) | 3.7k–10.2k | 514 µs | 28 ms |
| No batching (batch 1) | 83,865 | 79k–86k | 38 µs | 113 µs |
| `acks=quorum` (durable) | 26,147 | 20k–62k | 15 ms | 20 ms |
| 3 brokers, replication factor 3 | 398,581 | 132k–501k | 164 µs | 14.6 ms |
| Baseline: in-process mutex queue | 5,225,821 | 4,467k–5,342k | <1 µs | 11 µs |

Reading these honestly:

* **Batching is the dominant effect**: 32k → 1.34M records/s from batch 1 to
  batch 100 on one producer, for 9 µs of added p50.
* PulseLog reaches **36% of a bare mutex-protected in-memory queue** while
  doing TCP framing, CRC-32C everywhere, offset assignment and durable
  segmented storage.
* **Durable mode is expensive on this machine** and misses the
  low-single-digit-millisecond p99 target: `F_FULLFSYNC` on APFS dominates, and
  background flushing measurably stalls concurrent appends
  ([PERFORMANCE_RESULTS.md §5](docs/PERFORMANCE_RESULTS.md)).
* **More partitions made throughput worse here** — one disk, more open files,
  less batching per file.

Micro-benchmarks behind specific decisions: hardware CRC-32C is **5.2×** the
software path, buffer pooling is **30×** cheaper than allocating per request,
and fixing a per-record `pread` in the offset scan made lookups **18×** faster.

```bash
python3 scripts/run_benchmarks.py --trials=5 --out results
python3 scripts/plot_results.py --results results --out results
```

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
