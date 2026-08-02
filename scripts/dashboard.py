#!/usr/bin/env python3
"""A terminal dashboard for a PulseLog cluster.

Polls each broker's /topology and /metrics endpoints and redraws a live view of
the topology, partition state, throughput and lag.

Deliberately a terminal UI and deliberately small. The engine is the project;
a dashboard that needed a build step, a package manager and a browser would be
a second project competing with it for attention.

Usage:
  scripts/dashboard.py --brokers 127.0.0.1:9644,127.0.0.1:9645
  scripts/dashboard.py --brokers 127.0.0.1:9644 --once
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field

RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RED = "\033[31m"
CYAN = "\033[36m"
CLEAR = "\033[2J\033[H"


@dataclass
class BrokerView:
    endpoint: str
    reachable: bool = False
    topology: dict = field(default_factory=dict)
    metrics: dict = field(default_factory=dict)
    error: str = ""


def fetch_json(url: str, timeout: float) -> dict:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.loads(response.read().decode())


def poll(endpoints: list[str], timeout: float) -> list[BrokerView]:
    views = []
    for endpoint in endpoints:
        view = BrokerView(endpoint=endpoint)
        try:
            view.topology = fetch_json(f"http://{endpoint}/topology", timeout)
            metrics = fetch_json(f"http://{endpoint}/metrics.json", timeout)
            # Flatten to name -> entry for lookup.
            for entry in metrics.get("metrics", []):
                view.metrics.setdefault(entry["name"], entry)
            view.reachable = True
        except (urllib.error.URLError, OSError, json.JSONDecodeError, TimeoutError) as error:
            view.error = str(error)
        views.append(view)
    return views


def metric_value(view: BrokerView, name: str, default: float = 0.0) -> float:
    entry = view.metrics.get(name)
    if entry is None:
        return default
    return float(entry.get("value", default))


def metric_percentile(view: BrokerView, name: str, key: str) -> float:
    entry = view.metrics.get(name)
    if entry is None:
        return 0.0
    return float(entry.get(key, 0))


def human(value: float) -> str:
    if value >= 1_000_000_000:
        return f"{value / 1e9:.2f}G"
    if value >= 1_000_000:
        return f"{value / 1e6:.2f}M"
    if value >= 1_000:
        return f"{value / 1e3:.1f}k"
    return f"{value:.0f}"


def human_bytes(value: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{value:.0f} B"
        value /= 1024
    return f"{value:.1f} TiB"


def render(views: list[BrokerView], previous: dict, interval: float) -> str:
    width = shutil.get_terminal_size((110, 40)).columns
    out = [f"{BOLD}PulseLog{RESET}  {DIM}{time.strftime('%H:%M:%S')}{RESET}", ""]

    # --- brokers ---
    out.append(f"{BOLD}BROKERS{RESET}")
    out.append(f"  {'broker':<10}{'endpoint':<24}{'state':<12}{'uptime':<10}"
               f"{'partitions':<12}{'leads':<8}{'conns':<7}{'rss':<10}cpu")
    for view in views:
        if not view.reachable:
            out.append(f"  {RED}{'?':<10}{view.endpoint:<24}{'unreachable':<12}"
                       f"{DIM}{view.error[:40]}{RESET}")
            continue
        topology = view.topology
        broker_id = topology.get("broker_id", "?")
        uptime = topology.get("uptime_ms", 0) / 1000
        hosted = int(metric_value(view, "pulselog_hosted_partitions"))
        leading = int(metric_value(view, "pulselog_leader_partitions"))
        conns = int(metric_value(view, "pulselog_active_connections"))
        rss = metric_value(view, "pulselog_resident_memory_bytes")
        cpu = metric_value(view, "pulselog_cpu_percent")
        out.append(
            f"  {GREEN}{broker_id:<10}{RESET}{view.endpoint:<24}{GREEN}{'up':<12}{RESET}"
            f"{uptime:>6.0f}s   {hosted:<12}{leading:<8}{conns:<7}"
            f"{human_bytes(rss):<10}{cpu:.0f}%"
        )
    out.append("")

    # --- throughput, as a rate derived from counter deltas ---
    out.append(f"{BOLD}THROUGHPUT{RESET}  {DIM}(rate since the last refresh){RESET}")
    out.append(f"  {'broker':<10}{'produced/s':<14}{'fetched/s':<14}{'produce p50':<14}"
               f"{'produce p99':<14}{'flush p99':<14}errors")
    for view in views:
        if not view.reachable:
            continue
        broker_id = view.topology.get("broker_id", "?")
        key = f"{view.endpoint}"
        produced = metric_value(view, "pulselog_messages_produced_total")
        fetched = metric_value(view, "pulselog_messages_fetched_total")
        errors = int(metric_value(view, "pulselog_failed_requests_total"))

        last = previous.get(key, {})
        produced_rate = max(0.0, (produced - last.get("produced", produced)) / interval)
        fetched_rate = max(0.0, (fetched - last.get("fetched", fetched)) / interval)
        previous[key] = {"produced": produced, "fetched": fetched}

        p50 = metric_percentile(view, "pulselog_produce_latency_nanos", "p50") / 1000
        p99 = metric_percentile(view, "pulselog_produce_latency_nanos", "p99") / 1000
        flush99 = metric_percentile(view, "pulselog_flush_latency_nanos", "p99") / 1000
        error_colour = RED if errors else DIM
        out.append(
            f"  {broker_id:<10}{human(produced_rate):<14}{human(fetched_rate):<14}"
            f"{p50:>7.0f} us    {p99:>7.0f} us    {flush99:>7.0f} us    "
            f"{error_colour}{errors}{RESET}"
        )
    out.append("")

    # --- partitions ---
    out.append(f"{BOLD}PARTITIONS{RESET}")
    out.append(f"  {'topic-part':<22}{'leader':<8}{'epoch':<7}{'replicas':<12}{'isr':<6}"
               f"{'log end':<12}{'hwm':<12}{'flushed':<12}{'lag':<7}size")
    seen = set()
    for view in views:
        if not view.reachable:
            continue
        for topic in view.topology.get("topics", []):
            for partition in topic.get("partitions", []):
                if not partition.get("local"):
                    continue
                name = f"{topic['name']}-{partition['index']}"
                if name in seen:
                    continue
                seen.add(name)

                lag = partition.get("max_follower_lag", 0)
                lag_colour = GREEN if lag == 0 else (YELLOW if lag < 1000 else RED)
                replicas = ",".join(str(r) for r in partition.get("replicas", []))
                out.append(
                    f"  {name:<22}{partition['leader']:<8}{partition['epoch']:<7}"
                    f"{replicas:<12}{partition.get('in_sync_replicas', 1):<6}"
                    f"{partition.get('log_end', 0):<12}"
                    f"{partition.get('high_water_mark', 0):<12}"
                    f"{partition.get('flushed', 0):<12}"
                    f"{lag_colour}{lag:<7}{RESET}"
                    f"{human_bytes(partition.get('bytes', 0))}"
                )
    if not seen:
        out.append(f"  {DIM}no local partitions{RESET}")
    out.append("")

    # --- replication ---
    followers = [(view, f) for view in views if view.reachable
                 for f in view.topology.get("followers", [])]
    if followers:
        out.append(f"{BOLD}REPLICATION{RESET}")
        out.append(f"  {'from':<8}{'to':<8}{'state':<14}{'batches':<12}lag")
        for view, follower in followers:
            state = (f"{GREEN}connected{RESET}" if follower["connected"]
                     else f"{RED}disconnected{RESET}")
            lag = follower.get("max_lag_records", 0)
            out.append(
                f"  {view.topology.get('broker_id', '?'):<8}{follower['broker']:<8}"
                f"{state:<23}{follower.get('batches_sent', 0):<12}{lag}"
            )
        out.append("")

    # --- consumer groups ---
    groups = [(view, g) for view in views if view.reachable
              for g in view.topology.get("groups", [])]
    if groups:
        out.append(f"{BOLD}CONSUMER GROUPS{RESET}")
        out.append(f"  {'group':<24}{'state':<14}{'generation':<12}members")
        for _, group in groups:
            colour = GREEN if group["state"] == "stable" else YELLOW
            out.append(
                f"  {group['group']:<24}{colour}{group['state']:<14}{RESET}"
                f"{group['generation']:<12}{group['members']}"
            )
        out.append("")

    out.append(f"{DIM}{'-' * min(width, 100)}{RESET}")
    out.append(f"{DIM}Ctrl-C to exit{RESET}")
    return "\n".join(out)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--brokers", default="127.0.0.1:9644",
                        help="comma-separated metrics endpoints (host:port)")
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--timeout", type=float, default=1.5)
    parser.add_argument("--once", action="store_true", help="render once and exit")
    args = parser.parse_args()

    endpoints = [e.strip() for e in args.brokers.split(",") if e.strip()]
    if not endpoints:
        print("error: no endpoints given", file=sys.stderr)
        return 1

    previous: dict = {}
    try:
        while True:
            views = poll(endpoints, args.timeout)
            frame = render(views, previous, args.interval)
            if args.once:
                print(frame)
                return 0 if any(v.reachable for v in views) else 1
            sys.stdout.write(CLEAR + frame + "\n")
            sys.stdout.flush()
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print()
        return 0


if __name__ == "__main__":
    sys.exit(main())
