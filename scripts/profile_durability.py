#!/usr/bin/env python3
"""Break down where a durable (acks=quorum) produce actually spends its time.

acks=quorum is the slow path, and an end-to-end latency figure gives no
purchase on it: a 3 ms p99 could be the worker queue, the leader's fsync, or
waiting for two followers to finish their own fsyncs. Those have completely
different fixes, so this reads the broker's per-stage histograms and reports
the split:

  queue        request enqueued -> a worker picked it up
  append       inside the log append
  local flush  append -> the leader's own data reached media
  replication  leader flushed -> a quorum had flushed

The four roughly sum to end-to-end latency. What is left over is protocol
decode, partition lookup, response encode and the socket write.

It also sweeps the settings that plausibly move the tail, so a claim about any
of them rests on a measurement rather than on intuition:

  flush interval and record threshold (group commit)
  fsync vs fdatasync
  preallocation
  leader ack vs quorum ack, and memory-backed vs durable

None of these weaken a durability guarantee. Group commit changes how many
records share one fsync; it does not acknowledge anything that has not been
flushed.

Usage:
  scripts/profile_durability.py --build-dir build --out results/durability.json
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

STAGES = ("queue", "append", "local_flush", "replication")


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def scrape(url: str) -> str | None:
    try:
        with urllib.request.urlopen(url, timeout=3) as response:
            return response.read().decode()
    except (urllib.error.URLError, OSError):
        return None


def parse_histograms(text: str) -> dict[str, dict[str, float]]:
    """Pull quantiles and counts out of the Prometheus exposition text.

    Lines look like:
      pulselog_produce_stage_append_nanos{quantile="0.99"} 41000
      pulselog_produce_stage_append_nanos_count 20000
    """
    found: dict[str, dict[str, float]] = {}
    for line in text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        match = re.match(r'^(\w+)\{quantile="([\d.]+)"\}\s+([\d.eE+-]+)$', line)
        if match:
            name, quantile, value = match.groups()
            found.setdefault(name, {})[f"p{quantile}"] = float(value)
            continue
        match = re.match(r"^(\w+)_(count|sum)\s+([\d.eE+-]+)$", line)
        if match:
            name, field, value = match.groups()
            found.setdefault(name, {})[field] = float(value)
    return found


class Cluster:
    """Three brokers, since a quorum of one broker measures nothing."""

    def __init__(self, binary: Path, root: Path, flags: list[str], count: int = 3):
        self.binary = binary
        self.root = root
        self.flags = flags
        self.count = count
        self.ports = [free_port() for _ in range(count)]
        self.metrics_ports = [free_port() for _ in range(count)]
        self.processes: list[subprocess.Popen] = []

    def bootstrap(self) -> str:
        return ",".join(f"127.0.0.1:{p}" for p in self.ports)

    def __enter__(self) -> "Cluster":
        spec = ",".join(f"{i}@127.0.0.1:{p}" for i, p in enumerate(self.ports))
        for i in range(self.count):
            data = self.root / f"broker-{i}"
            data.mkdir(parents=True, exist_ok=True)
            log = open(self.root / f"broker-{i}.log", "w")
            self.processes.append(subprocess.Popen(
                [str(self.binary), f"--broker.id={i}",
                 f"--net.listen=127.0.0.1:{self.ports[i]}",
                 "--net.advertised.host=127.0.0.1",
                 f"--net.advertised.port={self.ports[i]}",
                 f"--broker.data.dir={data}",
                 f"--metrics.port={self.metrics_ports[i]}",
                 f"--cluster.brokers={spec}", "--log.level=warn", *self.flags],
                stdout=log, stderr=subprocess.STDOUT))
        deadline = time.time() + 30
        for i in range(self.count):
            while time.time() < deadline:
                if self.processes[i].poll() is not None:
                    raise RuntimeError(
                        f"broker {i} exited at start-up: "
                        f"{(self.root / f'broker-{i}.log').read_text()[-500:]}")
                if scrape(f"http://127.0.0.1:{self.metrics_ports[i]}/metrics"):
                    break
                time.sleep(0.05)
            else:
                raise RuntimeError(f"broker {i} never became ready")
        time.sleep(1.0)
        return self

    def __exit__(self, *exc) -> None:
        for process in self.processes:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
        for process in self.processes:
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()

    def metrics(self, index: int = 0) -> dict:
        return parse_histograms(scrape(f"http://127.0.0.1:{self.metrics_ports[index]}/metrics") or "")


def run_case(build_dir: Path, name: str, description: str, broker_flags: list[str],
             acks: str, records: int, trials: int) -> dict:
    """One configuration, repeated, reporting the median trial's numbers."""
    bench = build_dir / "bin" / "pulselog-bench"
    broker = build_dir / "bin" / "pulselog-broker"
    cli = build_dir / "bin" / "pulselog-cli"

    throughputs: list[float] = []
    latencies: list[dict] = []
    stages: dict = {}

    for trial in range(trials):
        root = Path(tempfile.mkdtemp(prefix="pulselog-durability-"))
        try:
            with Cluster(broker, root, broker_flags) as cluster:
                topic = f"durability-{name}".replace("_", "-")[:40]
                subprocess.run(
                    [str(cli), "create-topic", topic, "--partitions=4",
                     "--replication=3", f"--brokers={cluster.bootstrap()}"],
                    check=True, capture_output=True, timeout=30)
                time.sleep(0.3)

                output = subprocess.run(
                    [str(bench), "--scenario=produce",
                     f"--brokers={cluster.bootstrap()}", f"--topic={topic}",
                     "--partitions=4", "--replication=3", "--producers=4",
                     f"--records={records}", "--record-size=128",
                     "--batch-size=100", f"--acks={acks}", "--warmup=5000",
                     "--trials=1"],
                    capture_output=True, text=True, timeout=600)
                if output.returncode != 0:
                    raise RuntimeError(f"bench failed: {output.stderr[-400:]}")

                result = json.loads(output.stdout)
                measured = result["trials"][0]
                throughputs.append(measured["records_per_second"])
                latencies.append(measured["latency_nanos"])

                # Read the stages from the broker that led the most partitions.
                # Any leader will do; they all ran the same configuration.
                if trial == trials // 2:
                    stages = cluster.metrics(0)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    ordered = sorted(range(len(throughputs)), key=lambda i: throughputs[i])
    median_index = ordered[len(ordered) // 2]

    breakdown = {}
    for stage in STAGES:
        histogram = stages.get(f"pulselog_produce_stage_{stage}_nanos", {})
        if histogram.get("count"):
            breakdown[stage] = {
                "p50_us": round(histogram.get("p0.5", 0) / 1000, 1),
                "p99_us": round(histogram.get("p0.99", 0) / 1000, 1),
                "count": int(histogram["count"]),
            }

    return {
        "name": name,
        "description": description,
        "broker_flags": broker_flags,
        "acks": acks,
        "trials": len(throughputs),
        "records_per_second_median": round(statistics.median(throughputs), 1),
        "records_per_second_min": round(min(throughputs), 1),
        "records_per_second_max": round(max(throughputs), 1),
        "relative_spread": round((max(throughputs) - min(throughputs))
                                 / statistics.median(throughputs), 4),
        "latency_us": {
            key: round(latencies[median_index][key] / 1000, 1)
            for key in ("p50", "p95", "p99", "p999", "max")
        },
        "stage_breakdown": breakdown,
    }


def build_cases(records: int) -> list[tuple]:
    """The configurations to compare.

    Each isolates one variable against the baseline. `flush.interval` and
    `flush.max.records` are group commit: how many records share one fsync.
    Raising them does not acknowledge anything unflushed -- it only makes each
    fsync cover more records.
    """
    baseline = ["--storage.flush.interval=2ms", "--storage.flush.max.records=200",
                "--storage.flusher.interval=1ms"]
    return [
        ("quorum-baseline", "acks=quorum, 2ms/200-record group commit",
         baseline, "quorum"),
        ("quorum-tight-commit", "acks=quorum, 1ms/50-record group commit (fsync more often)",
         ["--storage.flush.interval=1ms", "--storage.flush.max.records=50",
          "--storage.flusher.interval=1ms"], "quorum"),
        ("quorum-wide-commit", "acks=quorum, 10ms/2000-record group commit (fsync less often)",
         ["--storage.flush.interval=10ms", "--storage.flush.max.records=2000",
          "--storage.flusher.interval=1ms"], "quorum"),
        ("quorum-fdatasync", "acks=quorum, fdatasync instead of fsync",
         baseline + ["--storage.fsync.mode=data"], "quorum"),
        ("quorum-no-prealloc", "acks=quorum, segment preallocation disabled",
         baseline + ["--storage.preallocate=false"], "quorum"),
        ("quorum-sync-on-append", "acks=quorum, fsync inside every append (no group commit)",
         ["--storage.flush.sync.on.append=true", "--storage.flusher.interval=1ms"],
         "quorum"),
        ("leader-ack", "acks=leader: in the leader's log, flush not awaited",
         baseline, "leader"),
        ("acks-none", "acks=none: no durability guarantee at all",
         baseline, "none"),
    ]


def summarise(results: list[dict]) -> str:
    lines = [
        f"{'configuration':<26} {'records/s':>11} {'spread':>8} {'p50 us':>9} "
        f"{'p99 us':>9} {'p99.9 us':>10}",
        "-" * 76,
    ]
    for result in results:
        lines.append(
            f"{result['name']:<26} {result['records_per_second_median']:>11,.0f} "
            f"{result['relative_spread'] * 100:>7.0f}% "
            f"{result['latency_us']['p50']:>9,.0f} "
            f"{result['latency_us']['p99']:>9,.0f} "
            f"{result['latency_us']['p999']:>10,.0f}"
        )

    lines += ["", "stage breakdown (broker-side, median trial):",
              f"{'configuration':<26} {'queue':>10} {'append':>10} "
              f"{'local fsync':>13} {'replication':>13}", "-" * 76]
    for result in results:
        stages = result.get("stage_breakdown", {})
        if not stages:
            continue
        cells = []
        for stage in STAGES:
            value = stages.get(stage)
            cells.append(f"{value['p99_us']:>10,.0f}" if value else f"{'-':>10}")
        lines.append(f"{result['name']:<26} {cells[0]} {cells[1]} "
                     f"{cells[2]:>13} {cells[3]:>13}")
    lines.append("(p99 microseconds per stage; local fsync and replication are "
                 "recorded for acks=quorum only)")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--build-dir", default="build", type=Path)
    parser.add_argument("--out", default=None, type=Path)
    parser.add_argument("--records", type=int, default=40_000)
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument("--only", default="", help="comma-separated name substrings")
    args = parser.parse_args()

    for name in ("pulselog-bench", "pulselog-broker", "pulselog-cli"):
        if not (args.build_dir / "bin" / name).exists():
            print(f"error: {args.build_dir / 'bin' / name} not found; build first",
                  file=sys.stderr)
            return 1

    cases = build_cases(args.records)
    if args.only:
        wanted = [w.strip() for w in args.only.split(",") if w.strip()]
        cases = [c for c in cases if any(w in c[0] for w in wanted)]
        if not cases:
            print(f"error: no case matches {args.only}", file=sys.stderr)
            return 1

    results = []
    for name, description, flags, acks in cases:
        print(f"\n=== {name} ===")
        print(f"    {description}")
        try:
            result = run_case(args.build_dir, name, description, flags, acks,
                              args.records, args.trials)
        except Exception as error:  # noqa: BLE001
            print(f"    FAILED: {error}", file=sys.stderr)
            continue
        results.append(result)
        print(f"    {result['records_per_second_median']:,.0f} records/s "
              f"(±{result['relative_spread'] * 100:.0f}%), "
              f"p99 {result['latency_us']['p99']:,.0f} us")

    if not results:
        print("error: every case failed", file=sys.stderr)
        return 1

    print()
    print(summarise(results))

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        with open(args.out, "w") as handle:
            json.dump({"records_per_trial": args.records,
                       "trials_per_case": args.trials,
                       "cases": results}, handle, indent=2)
        print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
