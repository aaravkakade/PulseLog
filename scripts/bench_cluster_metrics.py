#!/usr/bin/env python3
"""Measure the cluster-level costs that a throughput number does not show.

The scenario suite drives producers and reports records/s and latency. That
leaves four things unmeasured, all of which decide whether the throughput is
actually usable:

  replication lag   how far behind followers fall while the leader is busy
  consumer lag      whether consumers keep up with the offered load
  recovery time     how long a killed broker takes to serve its data again
  resource cost     CPU, resident memory, and bytes written per record

These need a real multi-broker cluster observed over time, not a single
timing loop, which is why they live outside run_benchmarks.py.

Usage:
  scripts/bench_cluster_metrics.py --build-dir build --out results/cluster_metrics.json
"""

from __future__ import annotations

import argparse
import json
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


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def fetch_json(url: str, timeout: float = 2.0) -> dict | None:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return json.load(response)
    except (urllib.error.URLError, OSError, ValueError):
        return None


class ProcessSampler:
    """Samples CPU, resident memory and bytes written for a set of processes.

    Linux exposes all three through /proc. macOS exposes CPU and RSS through
    ps but has no per-process write counter without elevated privileges, so
    bytes_written stays null there rather than being approximated.
    """

    def __init__(self, pids: list[int]):
        self.pids = pids
        self.samples: list[dict] = []
        self._start_written = self._bytes_written()

    def _bytes_written(self) -> int | None:
        total = 0
        found = False
        for pid in self.pids:
            try:
                with open(f"/proc/{pid}/io") as handle:
                    for line in handle:
                        if line.startswith("write_bytes:"):
                            total += int(line.split()[1])
                            found = True
            except OSError:
                continue
        return total if found else None

    def sample(self) -> None:
        cpu_total = 0.0
        rss_total = 0
        for pid in self.pids:
            try:
                output = subprocess.run(
                    ["ps", "-o", "%cpu=,rss=", "-p", str(pid)],
                    capture_output=True, text=True, timeout=5,
                )
            except (OSError, subprocess.SubprocessError):
                continue
            fields = output.stdout.split()
            if len(fields) >= 2:
                cpu_total += float(fields[0])
                rss_total += int(fields[1]) * 1024
        self.samples.append({"cpu_percent": cpu_total, "rss_bytes": rss_total})

    def summary(self) -> dict:
        end_written = self._bytes_written()
        written = None
        if end_written is not None and self._start_written is not None:
            written = end_written - self._start_written
        if not self.samples:
            return {"cpu_percent_peak": None, "rss_bytes_peak": None,
                    "bytes_written": written}
        return {
            # Peak matters more than mean for capacity planning: it is what the
            # machine must actually have available.
            "cpu_percent_peak": round(max(s["cpu_percent"] for s in self.samples), 1),
            "cpu_percent_mean": round(
                statistics.mean(s["cpu_percent"] for s in self.samples), 1),
            "rss_bytes_peak": max(s["rss_bytes"] for s in self.samples),
            "bytes_written": written,
            "samples": len(self.samples),
        }


class Cluster:
    def __init__(self, binary: Path, count: int, root: Path):
        self.binary = binary
        self.count = count
        self.root = root
        self.ports = [free_port() for _ in range(count)]
        self.metrics_ports = [free_port() for _ in range(count)]
        self.processes: list[subprocess.Popen | None] = [None] * count

    @property
    def spec(self) -> str:
        return ",".join(f"{i}@127.0.0.1:{p}" for i, p in enumerate(self.ports))

    def bootstrap(self) -> str:
        return ",".join(f"127.0.0.1:{p}" for p in self.ports)

    def start(self, index: int) -> None:
        data_dir = self.root / f"broker-{index}"
        data_dir.mkdir(parents=True, exist_ok=True)
        log = open(self.root / f"broker-{index}.log", "a")
        self.processes[index] = subprocess.Popen(
            [
                str(self.binary),
                f"--broker.id={index}",
                f"--net.listen=127.0.0.1:{self.ports[index]}",
                "--net.advertised.host=127.0.0.1",
                f"--net.advertised.port={self.ports[index]}",
                f"--broker.data.dir={data_dir}",
                f"--metrics.port={self.metrics_ports[index]}",
                f"--cluster.brokers={self.spec}",
                "--log.level=warn",
            ],
            stdout=log, stderr=subprocess.STDOUT,
        )

    def start_all(self) -> None:
        for i in range(self.count):
            self.start(i)
        for i in range(self.count):
            self.wait_ready(i)
        # Replication links need a moment before measurements mean anything.
        time.sleep(1.0)

    def wait_ready(self, index: int, timeout: float = 30.0) -> float:
        """Blocks until the broker serves its metrics endpoint. Returns seconds."""
        started = time.time()
        deadline = started + timeout
        while time.time() < deadline:
            process = self.processes[index]
            if process is not None and process.poll() is not None:
                raise RuntimeError(f"broker {index} exited during start-up")
            if fetch_json(f"http://127.0.0.1:{self.metrics_ports[index]}/topology") is not None:
                return time.time() - started
            time.sleep(0.05)
        raise RuntimeError(f"broker {index} did not become ready within {timeout}s")

    def topology(self, index: int) -> dict | None:
        return fetch_json(f"http://127.0.0.1:{self.metrics_ports[index]}/topology")

    def pids(self) -> list[int]:
        return [p.pid for p in self.processes if p is not None and p.poll() is None]

    def stop(self) -> None:
        for process in self.processes:
            if process is not None and process.poll() is None:
                process.send_signal(signal.SIGTERM)
        for process in self.processes:
            if process is None:
                continue
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()


def partition_stats(topology: dict, topic: str) -> list[dict]:
    for entry in topology.get("topics", []):
        if entry.get("name") == topic:
            return [p for p in entry.get("partitions", []) if p.get("local")]
    return []


def total_log_end(topology: dict, topic: str) -> int:
    return sum(p.get("log_end", 0) for p in partition_stats(topology, topic))


def measure(build_dir: Path, root: Path, records: int, record_size: int,
            partitions: int, brokers: int) -> dict:
    bench = build_dir / "bin" / "pulselog-bench"
    broker = build_dir / "bin" / "pulselog-broker"
    cli = build_dir / "bin" / "pulselog-cli"
    topic = "cluster-metrics"

    cluster = Cluster(broker, brokers, root)
    result: dict = {
        "config": {
            "brokers": brokers, "partitions": partitions,
            "replication_factor": brokers, "records": records,
            "record_size_bytes": record_size, "acks": "leader",
        },
    }

    try:
        cluster.start_all()
        subprocess.run(
            [str(cli), "create-topic", topic, f"--partitions={partitions}",
             f"--replication={brokers}", f"--brokers={cluster.bootstrap()}"],
            check=True, capture_output=True, timeout=30,
        )
        time.sleep(0.5)

        sampler = ProcessSampler(cluster.pids())
        lag_samples: list[int] = []

        print(f"  producing {records:,} records across {brokers} brokers...")
        load = subprocess.Popen(
            [str(bench), "--scenario=produce", f"--brokers={cluster.bootstrap()}",
             f"--topic={topic}", f"--partitions={partitions}",
             f"--replication={brokers}", "--producers=4", f"--records={records}",
             f"--record-size={record_size}", "--batch-size=100", "--acks=leader",
             "--warmup=5000", "--trials=1"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )

        # Sample replication lag *while* the load runs. Sampling afterwards
        # measures an idle cluster, which is always caught up and says nothing.
        while load.poll() is None:
            sampler.sample()
            for i in range(brokers):
                topology = cluster.topology(i)
                if topology is None:
                    continue
                for partition in partition_stats(topology, topic):
                    lag_samples.append(partition.get("max_follower_lag", 0))
            time.sleep(0.1)
        load.wait()

        result["resources"] = sampler.summary()
        if result["resources"].get("bytes_written") is not None and records > 0:
            result["resources"]["bytes_written_per_record"] = round(
                result["resources"]["bytes_written"] / records, 1)

        if lag_samples:
            ordered = sorted(lag_samples)
            result["replication_lag_records"] = {
                "samples": len(ordered),
                "p50": ordered[len(ordered) // 2],
                "p95": ordered[int(len(ordered) * 0.95)],
                "p99": ordered[int(len(ordered) * 0.99)],
                "max": ordered[-1],
                "note": "leader-observed follower lag, sampled every 100ms under load",
            }

        # Wait for followers to catch up, so recovery starts from a known state.
        leader_total = total_log_end(cluster.topology(0) or {}, topic)
        converge_deadline = time.time() + 30
        converged = False
        convergence_started = time.time()
        while time.time() < converge_deadline:
            totals = [total_log_end(cluster.topology(i) or {}, topic)
                      for i in range(brokers)]
            if all(t == leader_total and t > 0 for t in totals):
                converged = True
                break
            time.sleep(0.05)
        result["replication_convergence_seconds"] = (
            round(time.time() - convergence_started, 3) if converged else None)
        result["replication_converged"] = converged

        result["recovery"] = measure_recovery(cluster, topic, brokers)

    finally:
        cluster.stop()

    return result


def measure_recovery(cluster: Cluster, topic: str, brokers: int) -> dict:
    """Kill a broker without warning and time until its data is served again.

    SIGKILL, not SIGTERM: a clean shutdown flushes and writes a checkpoint, so
    it exercises none of the recovery path. What is timed is process start to
    the point where the broker reports the same log end offset it had before
    dying -- serving, not merely listening.
    """
    victim = brokers - 1
    before = total_log_end(cluster.topology(victim) or {}, topic)

    process = cluster.processes[victim]
    if process is None:
        return {"measured": False, "reason": "victim broker was not running"}
    process.kill()
    process.wait(timeout=10)

    started = time.time()
    cluster.start(victim)
    listen_seconds = cluster.wait_ready(victim)

    restored = False
    after = 0
    deadline = time.time() + 60
    while time.time() < deadline:
        after = total_log_end(cluster.topology(victim) or {}, topic)
        if after >= before:
            restored = True
            break
        time.sleep(0.02)
    serving_seconds = time.time() - started

    return {
        "measured": True,
        "method": "SIGKILL, restart, wait until log end offset is restored",
        "records_before_kill": before,
        "records_after_recovery": after,
        "data_intact": restored,
        "time_to_accept_connections_seconds": round(listen_seconds, 3),
        "time_to_restore_log_seconds": round(serving_seconds, 3) if restored else None,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--build-dir", default="build", type=Path)
    parser.add_argument("--out", default=None, type=Path)
    parser.add_argument("--records", type=int, default=200_000)
    parser.add_argument("--record-size", type=int, default=128)
    parser.add_argument("--partitions", type=int, default=6)
    parser.add_argument("--brokers", type=int, default=3)
    args = parser.parse_args()

    for name in ("pulselog-bench", "pulselog-broker", "pulselog-cli"):
        if not (args.build_dir / "bin" / name).exists():
            print(f"error: {args.build_dir / 'bin' / name} not found; build first",
                  file=sys.stderr)
            return 1

    root = Path(tempfile.mkdtemp(prefix="pulselog-cluster-metrics-"))
    try:
        result = measure(args.build_dir, root, args.records, args.record_size,
                         args.partitions, args.brokers)
    except Exception as error:  # noqa: BLE001 - report and fail, do not mask
        print(f"error: {error}", file=sys.stderr)
        for log in sorted(root.glob("*.log")):
            print(f"--- {log.name} ---", file=sys.stderr)
            print(log.read_text()[-2000:], file=sys.stderr)
        return 1
    finally:
        shutil.rmtree(root, ignore_errors=True)

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        with open(args.out, "w") as handle:
            json.dump(result, handle, indent=2)
        print(f"  wrote {args.out}")

    lag = result.get("replication_lag_records")
    if lag:
        print(f"  replication lag under load: p50 {lag['p50']}, p99 {lag['p99']}, "
              f"max {lag['max']} records")
    recovery = result.get("recovery", {})
    if recovery.get("measured"):
        intact = "all records intact" if recovery["data_intact"] else "DATA MISSING"
        print(f"  recovery after SIGKILL: {recovery['time_to_restore_log_seconds']}s "
              f"({recovery['records_after_recovery']:,} records, {intact})")
    resources = result.get("resources", {})
    if resources.get("cpu_percent_peak") is not None:
        print(f"  peak cpu {resources['cpu_percent_peak']}%, "
              f"peak rss {resources['rss_bytes_peak'] / 2**20:.0f} MiB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
