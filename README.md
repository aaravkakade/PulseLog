# PulseLog

A low-latency distributed event-streaming engine in C++20. Partitioned
append-only logs, leader/follower replication, consumer groups, a custom binary
protocol, and a custom asynchronous networking layer — implemented from first
principles rather than wrapping an existing broker.

> **Status:** under active construction. Sections marked _(pending)_ are not
> implemented yet. Performance numbers appear here only once the benchmark
> harness has produced them on real hardware; nothing in this file is
> aspirational.

---

## Building

```bash
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Requires CMake ≥ 3.20 and a C++20 compiler (tested with Apple clang 15 and
GCC 13). GoogleTest and Google Benchmark are fetched at configure time; pass
`-DPULSELOG_OFFLINE=ON` to build the engine without them.

### Build options

| Option | Default | Meaning |
|---|---|---|
| `PULSELOG_BUILD_TESTS` | `ON` | Unit, integration and stress tests |
| `PULSELOG_BUILD_BENCHMARKS` | `ON` | Google Benchmark micro-benchmarks |
| `PULSELOG_WERROR` | `OFF` | Treat warnings as errors (CI sets `ON`) |
| `PULSELOG_SANITIZER` | `""` | `address`, `thread`, `undefined`, `address+undefined` |
| `PULSELOG_USE_IO_URING` | `OFF` | io_uring event-loop backend (Linux) |

---

## Repository layout

```
include/pulselog/   public headers, one directory per module
src/                module implementations
  base/             Status/Result, buffers, CRC32C, config, logging
  concurrency/      SPSC ring, bounded MPMC queue, blocking queue, threads
  protocol/         frame layout, opcodes, request/response codecs
  storage/          segmented append-only log, indexes, recovery
  net/              poller abstraction, event loop, connections
  metadata/         topic/partition/cluster metadata
  replication/      leader push, follower apply, high-water mark
  consumer/         group coordinator, assignors, offset store
  metrics/          histograms, registry, Prometheus exporter
  broker/           request routing, partition ownership, lifecycle
clients/cpp/        C++ client SDK
clients/python/     Python client (demos and benchmark orchestration only)
apps/               broker, CLI, benchmark driver
tests/              unit, integration, stress
benchmarks/         micro-benchmarks
scripts/            benchmark orchestration, failure injection, plotting
docs/               design documents
docker/             images and multi-broker compose topology
```

---

## Documentation

| Document | Contents |
|---|---|
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | Module layering, thread model, request lifecycle |
| [PROTOCOL.md](docs/PROTOCOL.md) | Byte-level frame layout, opcodes, compatibility |
| [STORAGE_ENGINE.md](docs/STORAGE_ENGINE.md) | Segment format, indexing, recovery, retention |
| [REPLICATION.md](docs/REPLICATION.md) | Leader/follower protocol, high-water mark, acks |
| [CONCURRENCY_MODEL.md](docs/CONCURRENCY_MODEL.md) | Ownership, memory ordering, shutdown |
| [BENCHMARKING.md](docs/BENCHMARKING.md) | How to reproduce every published number |
| [FAILURE_SEMANTICS.md](docs/FAILURE_SEMANTICS.md) | What survives what, and what does not |
| [PERFORMANCE_RESULTS.md](docs/PERFORMANCE_RESULTS.md) | Measured results with full configuration |

---

## Roadmap

- [x] Base layer: error handling, buffers, CRC32C, configuration, logging
- [x] Concurrency primitives: SPSC ring, bounded MPMC queue, blocking queue
- [ ] Binary protocol and codec
- [ ] Segmented append-only storage engine
- [ ] Single-broker produce/fetch path
- [ ] Asynchronous networking layer
- [ ] Partitioning and metadata
- [ ] Leader/follower replication
- [ ] Consumer groups
- [ ] Metrics and Prometheus endpoint
- [ ] Benchmark harness and failure injection
- [ ] Docker Compose multi-broker cluster

## License

MIT — see [LICENSE](LICENSE).
