# PulseLog Benchmarking

How to reproduce every number in
[PERFORMANCE_RESULTS.md](PERFORMANCE_RESULTS.md), and the rules the harness
follows so that the numbers mean something.

---

## 1. Quick start

```bash
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j

# The full suite: 13 scenarios, 5 trials each, real broker processes.
python3 scripts/run_benchmarks.py --trials=5 --out results

# Charts and a Markdown table.
python3 scripts/plot_results.py --results results --out results

# Micro-benchmarks.
./build/bin/bench_core
./build/bin/bench_storage
```

A quick sanity run, with smaller record counts:

```bash
python3 scripts/run_benchmarks.py --quick --trials=1
```

One scenario at a time:

```bash
python3 scripts/run_benchmarks.py --only=06-quorum,07-replication --trials=3
```

---

## 2. Rules the harness follows

These are the difference between a measurement and a number.

**Warm-up is discarded.** The first records pay for connection setup, segment
creation, cold page cache and buffer-pool growth. Those costs are real but not
steady-state. Producers synchronise on a barrier after warm-up so the measured
window carries the full offered load from its first instant.

**Latency is per produce request, into an HDR histogram.** With `batch=100`,
one sample covers 100 records. A batched p50 is not a per-record latency and is
never presented as one; every result file carries a `latency_scope` field
saying so.

**Throughput is wall-clock over the measured window only**, from the first
post-warm-up send to the last acknowledgement.

**Coordinated omission is not corrected for.** The driver is closed-loop: each
producer waits for its acknowledgement before sending again. A stall therefore
suppresses subsequent samples rather than appearing as one long sample. The
reported percentiles are *service times under this offered load*, not what an
open-loop arrival process would see under saturation. Every result file states
this in its `method` block.

**Trials are repeated and the spread is published.** Several scenarios vary
2–3× run to run on a laptop. Reporting only a median would imply a precision
the measurement does not have, so `min–max` sits next to every median.

**Every result file is self-describing.** OS, kernel, architecture, CPU model,
core count, memory, compiler version, C++ standard, poller backend, checksum
implementation, and the complete scenario configuration. A number cannot be
quoted without the conditions that produced it.

---

## 3. Scenarios

| # | Name | What it isolates |
|---:|---|---|
| 01 | single producer, no replication | The floor: one producer, one partition |
| 02 | multi-producer, one partition | Contention on a single log |
| 03 | multi-producer, multi-partition | Whether partitioning helps on this hardware |
| 04 | producers + consumers | Read and write paths competing |
| 05 | leader ack | The default durability level |
| 06 | quorum ack | The cost of a real durability guarantee |
| 07 | replication under load | Three brokers, RF=3 |
| 08 | no batching | The baseline batching is measured against |
| 09 | small message (16 B) | Per-record framing overhead |
| 10 | large message (64 KiB) | Disk and socket bandwidth |
| 11 | acks=none | The throughput ceiling with no guarantee |
| 12 | `fsync.mode=data` | What the durability knob is worth |
| 13 | baseline: mutex queue | An in-process queue with none of the guarantees |

---

## 4. The driver directly

```bash
./build/bin/pulselog-bench \
  --scenario=produce \
  --brokers=127.0.0.1:9092 \
  --topic=bench \
  --partitions=4 --producers=4 \
  --records=200000 --record-size=128 --batch-size=100 \
  --acks=leader --warmup=10000 --trials=3 \
  --output=results/my-run.json
```

`--scenario` accepts `produce`, `produce-consume` and `baseline-mutex`.
`--keyed` routes by key hash instead of sticky round-robin. `pulselog-bench
--help` lists everything.

---

## 5. Micro-benchmarks

`bench_core` covers CRC-32C (hardware against software), record encode/parse
with and without checksum verification, frame encode/decode, buffer pooling
against fresh allocation, and the three queue implementations including a
contended MPMC case.

`bench_storage` covers append throughput by batch and record size, the cost of
synchronous appends, the read path, and offset lookup across index intervals.
This is the benchmark that found the 18× read-path bug documented in
PERFORMANCE_RESULTS.md §8.1.

```bash
./build/bin/bench_core --benchmark_filter=Crc32c
./build/bin/bench_storage --benchmark_min_time=0.5s
./build/bin/bench_core --benchmark_format=json > results/micro.json
```

---

## 6. Failure and recovery measurement

```bash
scripts/failure_test.sh     # asserts the documented failure behaviour
scripts/smoke_test.sh       # end-to-end CLI exercise plus a restart
```

`failure_test.sh` reports recovery time directly: it SIGKILLs a broker with no
clean shutdown, restarts it, and times how long until it answers again.

Individual failures can be injected by hand:

```bash
python3 scripts/chaos.py kill-broker --pid <pid>
python3 scripts/chaos.py corrupt-tail --data-dir ./pulselog-data --topic orders
python3 scripts/chaos.py overload --broker 127.0.0.1:9092 --topic orders
python3 scripts/chaos.py fill-disk --data-dir ./pulselog-data --megabytes 4096
```

---

## 7. Profiling

Frame pointers are kept in release builds (`-fno-omit-frame-pointer`), so
sampling profilers produce usable stacks without a special build.

Linux:

```bash
perf record -F 999 -g -- ./build/bin/pulselog-broker --broker.data.dir=/tmp/pl
perf script | stackcollapse-perf.pl | flamegraph.pl > flame.svg
```

macOS:

```bash
sample $(pgrep pulselog-broker) 10 -f /tmp/pulselog.sample
xcrun xctrace record --template 'Time Profiler' --attach $(pgrep pulselog-broker)
```

---

## 8. Reading a result honestly

* Compare like with like. `acks=none` and `acks=quorum` are different products,
  not a fast and a slow version of one.
* Check the spread before quoting a median.
* A batched p50 is a per-request number. Divide by the batch size only if you
  mean "amortised per record", and say so.
* The 3-broker scenarios are three processes on one host sharing one disk and
  the loopback interface. That flatters the network and penalises the disk.
* The machine matters more than anything else here: a thermally constrained
  laptop is not a server.
