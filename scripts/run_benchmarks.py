#!/usr/bin/env python3
"""Run the PulseLog benchmark suite and write machine-readable results.

Starts brokers, runs each scenario through `pulselog-bench`, and collects one
JSON file per scenario into the results directory. Every file records the host,
compiler, configuration and measurement method, so no number can be quoted
without the conditions that produced it.

Usage:
  scripts/run_benchmarks.py --build-dir build --out results
  scripts/run_benchmarks.py --only quorum-ack,small-message
  scripts/run_benchmarks.py --quick            # smaller record counts

The scenario list matches docs/BENCHMARKING.md. Scenarios that need more than
one broker start a real multi-broker cluster on loopback.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path


def free_port() -> int:
    """Bind an ephemeral port, note it, release it.

    There is a race between releasing and the broker binding, which is why
    callers verify the broker actually came up rather than assuming it did.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@dataclass
class Scenario:
    """One benchmark configuration.

    `brokers` and `replication` drive how many broker processes get started;
    everything else is passed through to pulselog-bench.
    """

    name: str
    description: str
    args: dict = field(default_factory=dict)
    brokers: int = 1
    replication: int = 1
    scenario: str = "produce"
    durable: bool = False


def build_scenarios(quick: bool) -> list[Scenario]:
    # Record counts are chosen so each scenario runs for a few seconds: long
    # enough that start-up effects are amortised, short enough that the whole
    # suite finishes in minutes.
    n = (lambda full, small: small if quick else full)

    return [
        Scenario(
            "01-single-producer-no-replication",
            "One producer, one broker, one partition, no replication",
            {"partitions": 1, "producers": 1, "records": n(200_000, 40_000),
             "record-size": 128, "batch-size": 100, "acks": "leader"},
        ),
        Scenario(
            "02-multi-producer-one-partition",
            "Four producers contending on a single partition",
            {"partitions": 1, "producers": 4, "records": n(200_000, 40_000),
             "record-size": 128, "batch-size": 100, "acks": "leader"},
        ),
        Scenario(
            "03-multi-producer-multi-partition",
            "Four producers across eight partitions",
            {"partitions": 8, "producers": 4, "records": n(400_000, 60_000),
             "record-size": 128, "batch-size": 100, "acks": "leader"},
        ),
        Scenario(
            "04-concurrent-producers-consumers",
            "Producers and consumers running at the same time",
            {"partitions": 4, "producers": 4, "consumers": 2,
             "records": n(200_000, 40_000), "record-size": 128,
             "batch-size": 100, "acks": "leader"},
            scenario="produce-consume",
        ),
        Scenario(
            "05-leader-ack",
            "acks=leader, the default durability level",
            {"partitions": 4, "producers": 4, "records": n(200_000, 40_000),
             "record-size": 128, "batch-size": 100, "acks": "leader"},
        ),
        Scenario(
            "06-quorum-ack",
            "acks=quorum: a majority must have persisted the record",
            {"partitions": 4, "producers": 4, "records": n(40_000, 10_000),
             "record-size": 128, "batch-size": 100, "acks": "quorum"},
            durable=True,
        ),
        Scenario(
            "07-replication-under-load",
            "Three brokers, replication factor 3, sustained produce",
            {"partitions": 6, "producers": 4, "records": n(200_000, 40_000),
             "record-size": 128, "batch-size": 100, "acks": "leader"},
            brokers=3, replication=3,
        ),
        Scenario(
            "08-no-batching",
            "Batch size 1: the cost the batching path is measured against",
            {"partitions": 4, "producers": 4, "records": n(60_000, 20_000),
             "record-size": 128, "batch-size": 1, "acks": "leader"},
        ),
        Scenario(
            "09-small-message",
            "16-byte values, where per-record framing dominates",
            {"partitions": 4, "producers": 4, "records": n(400_000, 60_000),
             "record-size": 16, "batch-size": 200, "acks": "leader"},
        ),
        Scenario(
            "10-large-message",
            "64 KiB values, where the disk and the socket dominate",
            {"partitions": 4, "producers": 4, "records": n(20_000, 5_000),
             "record-size": 65536, "batch-size": 4, "acks": "leader"},
        ),
        Scenario(
            "11-acks-none",
            "acks=none: no durability guarantee, the throughput ceiling",
            {"partitions": 4, "producers": 4, "records": n(400_000, 60_000),
             "record-size": 128, "batch-size": 100, "acks": "none"},
        ),
        Scenario(
            "12-baseline-mutex-queue",
            "In-process mutex-protected queue: no network, no protocol, no disk",
            {"producers": 4, "records": n(400_000, 60_000), "record-size": 128},
            scenario="baseline-mutex",
        ),
    ]


class BrokerCluster:
    """Starts and stops a cluster of real broker processes."""

    def __init__(self, binary: Path, count: int, data_root: Path, durable: bool,
                 verbose: bool):
        self.binary = binary
        self.count = count
        self.data_root = data_root
        self.durable = durable
        self.verbose = verbose
        self.ports = [free_port() for _ in range(count)]
        self.metrics_ports = [free_port() for _ in range(count)]
        self.processes: list[subprocess.Popen] = []
        self.logs: list[Path] = []

    def bootstrap(self) -> str:
        return ",".join(f"127.0.0.1:{port}" for port in self.ports)

    def start(self) -> None:
        cluster_spec = ",".join(
            f"{i}@127.0.0.1:{port}" for i, port in enumerate(self.ports)
        )
        for i in range(self.count):
            data_dir = self.data_root / f"broker-{i}"
            data_dir.mkdir(parents=True, exist_ok=True)
            log_path = self.data_root / f"broker-{i}.log"
            self.logs.append(log_path)

            args = [
                str(self.binary),
                f"--broker.id={i}",
                f"--net.listen=127.0.0.1:{self.ports[i]}",
                f"--net.advertised.host=127.0.0.1",
                f"--net.advertised.port={self.ports[i]}",
                f"--broker.data.dir={data_dir}",
                f"--metrics.port={self.metrics_ports[i]}",
                "--log.level=warn",
            ]
            if self.count > 1:
                args.append(f"--cluster.brokers={cluster_spec}")
            if self.durable:
                # Quorum acks are only meaningful when a flush actually
                # happens promptly; a 200 ms default would measure the timer,
                # not the engine.
                args.append("--storage.flush.interval=5ms")
                args.append("--storage.flush.max.records=500")

            log_file = open(log_path, "w")
            self.processes.append(
                subprocess.Popen(args, stdout=log_file, stderr=subprocess.STDOUT)
            )

        self._wait_ready()

    def _wait_ready(self, timeout: float = 20.0) -> None:
        deadline = time.time() + timeout
        for i, port in enumerate(self.ports):
            while time.time() < deadline:
                if self.processes[i].poll() is not None:
                    raise RuntimeError(
                        f"broker {i} exited during start-up; see {self.logs[i]}"
                    )
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                        break
                except OSError:
                    time.sleep(0.1)
            else:
                raise RuntimeError(f"broker {i} did not accept connections in time")
        # Give replication links a moment to establish before measuring.
        if self.count > 1:
            time.sleep(1.0)

    def stop(self) -> None:
        for process in self.processes:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
        for process in self.processes:
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        self.processes.clear()


def run_scenario(bench: Path, broker: Path, scenario: Scenario, out_dir: Path,
                 trials: int, verbose: bool) -> dict | None:
    print(f"\n=== {scenario.name} ===")
    print(f"    {scenario.description}")

    with tempfile.TemporaryDirectory(prefix="pulselog-bench-") as tmp:
        data_root = Path(tmp)
        cluster = None
        try:
            if scenario.scenario != "baseline-mutex":
                cluster = BrokerCluster(
                    broker, scenario.brokers, data_root, scenario.durable, verbose
                )
                cluster.start()

            output = out_dir / f"{scenario.name}.json"
            args = [
                str(bench),
                f"--scenario={scenario.scenario}",
                f"--topic=bench-{scenario.name.replace('-', '_')[:40]}",
                f"--label={scenario.description}",
                f"--output={output}",
                f"--trials={trials}",
                f"--replication={scenario.replication}",
            ]
            if cluster is not None:
                args.append(f"--brokers={cluster.bootstrap()}")
            for key, value in scenario.args.items():
                args.append(f"--{key}={value}")

            completed = subprocess.run(args, capture_output=not verbose, text=True)
            if completed.returncode != 0:
                print(f"    FAILED (exit {completed.returncode})")
                if not verbose and completed.stderr:
                    print(completed.stderr[-2000:])
                return None

            if not verbose and completed.stderr:
                # The driver prints its human-readable summary on stderr.
                for line in completed.stderr.splitlines():
                    if any(k in line for k in ("throughput", "latency", "errors", "records ")):
                        print(f"   {line.strip()}")

            with open(output) as handle:
                result = json.load(handle)
            # The driver knows its scenario *type* ("produce"); the suite knows
            # which numbered scenario this is. Record both.
            result["name"] = scenario.name
            result["description"] = scenario.description
            result["brokers_started"] = scenario.brokers
            with open(output, "w") as handle:
                json.dump(result, handle, indent=2)
            return result
        finally:
            if cluster is not None:
                cluster.stop()


def summarise(results: list[dict]) -> str:
    header = (
        f"{'scenario':<38} {'records/s':>12} {'MiB/s':>9} "
        f"{'p50 us':>9} {'p99 us':>10} {'errors':>7}"
    )
    lines = [header, "-" * len(header)]
    for result in results:
        trials = result["trials"]
        # The median trial is the headline; every trial is in the JSON.
        trials_sorted = sorted(trials, key=lambda t: t["records_per_second"])
        trial = trials_sorted[len(trials_sorted) // 2]
        lines.append(
            f"{result.get('name', result['scenario']):<38} "
            f"{trial['records_per_second']:>12,.0f} "
            f"{trial['megabytes_per_second']:>9,.1f} "
            f"{trial['latency_nanos']['p50'] / 1000:>9,.0f} "
            f"{trial['latency_nanos']['p99'] / 1000:>10,.0f} "
            f"{trial['errors']:>7}"
        )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--build-dir", default="build", type=Path)
    parser.add_argument("--out", default="results", type=Path)
    parser.add_argument("--trials", type=int, default=3,
                        help="repeats per scenario; the median is reported")
    parser.add_argument("--only", default="",
                        help="comma-separated substrings; run only matching scenarios")
    parser.add_argument("--quick", action="store_true",
                        help="smaller record counts, for a fast sanity run")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    bench = args.build_dir / "bin" / "pulselog-bench"
    broker = args.build_dir / "bin" / "pulselog-broker"
    for binary in (bench, broker):
        if not binary.exists():
            print(f"error: {binary} not found. Build first:", file=sys.stderr)
            print(f"  cmake -S . -B {args.build_dir} -DCMAKE_BUILD_TYPE=RelWithDebInfo",
                  file=sys.stderr)
            print(f"  cmake --build {args.build_dir} -j", file=sys.stderr)
            return 1

    args.out.mkdir(parents=True, exist_ok=True)

    scenarios = build_scenarios(args.quick)
    if args.only:
        wanted = [s.strip() for s in args.only.split(",") if s.strip()]
        scenarios = [s for s in scenarios if any(w in s.name for w in wanted)]
        if not scenarios:
            print(f"error: no scenario matches {args.only}", file=sys.stderr)
            return 1

    print(f"running {len(scenarios)} scenario(s), {args.trials} trial(s) each")
    started = time.time()
    results = []
    failures = []
    for scenario in scenarios:
        result = run_scenario(bench, broker, scenario, args.out, args.trials,
                              args.verbose)
        if result is None:
            failures.append(scenario.name)
        else:
            results.append(result)

    elapsed = time.time() - started
    print(f"\ncompleted in {elapsed:.1f}s\n")
    if results:
        print(summarise(results))
        combined = args.out / "summary.json"
        with open(combined, "w") as handle:
            json.dump(results, handle, indent=2)
        print(f"\nwrote {combined}")

    if failures:
        print(f"\nFAILED scenarios: {', '.join(failures)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
