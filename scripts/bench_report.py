#!/usr/bin/env python3
"""Turn raw benchmark JSON into CSV and a readable report.

Consumes what the other scripts produce -- per-scenario JSON from
run_benchmarks.py, host metadata from bench_metadata.py, cluster measurements
from bench_cluster_metrics.py -- and emits:

  results.csv          one row per trial, so the raw data is analysable
  summary.csv          one row per scenario, medians and spread
  REPORT.md            the report, with methodology and hardware attached
  report.html          the same thing, standalone, charts inlined

Two rules the formatting follows. Every headline number carries its spread,
because several of these scenarios vary by more than 2x run to run and a bare
median implies a precision the measurement does not have. And nothing is
labelled an improvement here: this script reports what was measured, and
comparisons live in the documents that can state what changed between runs.

Usage:
  scripts/bench_report.py --results results --out results
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from plot_results import escape, median_trial  # noqa: E402


def load_results(directory: Path) -> list[dict]:
    """Load per-scenario files, skipping the aggregates this script writes."""
    skip = {"summary.json", "metadata.json", "cluster_metrics.json"}
    results = []
    for path in sorted(directory.glob("*.json")):
        if path.name in skip:
            continue
        try:
            with open(path) as handle:
                results.append(json.load(handle))
        except (OSError, json.JSONDecodeError):
            print(f"warning: skipping unreadable {path}", file=sys.stderr)
    return results


def load_optional(path: Path) -> dict | None:
    try:
        with open(path) as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None


def spread(result: dict) -> dict:
    """Throughput spread across trials.

    relative_spread is (max - min) / median. It is the number that decides
    whether a difference between two runs means anything: when it is 0.5, a
    20% "improvement" is noise.
    """
    rates = sorted(t["records_per_second"] for t in result["trials"])
    median = statistics.median(rates)
    return {
        "trials": len(rates),
        "min": rates[0],
        "max": rates[-1],
        "median": median,
        "mean": statistics.mean(rates),
        "stdev": statistics.stdev(rates) if len(rates) > 1 else 0.0,
        "relative_spread": (rates[-1] - rates[0]) / median if median else 0.0,
    }


def write_trial_csv(results: list[dict], path: Path) -> None:
    """One row per trial: the raw data, before any aggregation."""
    columns = [
        "scenario", "trial", "records", "bytes", "duration_seconds",
        "records_per_second", "megabytes_per_second", "errors",
        "latency_count", "latency_min_ns", "latency_p50_ns", "latency_p90_ns",
        "latency_p95_ns", "latency_p99_ns", "latency_p999_ns", "latency_max_ns",
        "latency_mean_ns", "cpu_percent", "peak_rss_bytes",
        "partitions", "producers", "consumers", "record_size_bytes",
        "batch_size", "acks", "brokers", "replication_factor",
    ]
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for result in results:
            config = result["config"]
            for index, trial in enumerate(result["trials"], start=1):
                latency = trial["latency_nanos"]
                writer.writerow({
                    "scenario": result.get("name", result["scenario"]),
                    "trial": index,
                    "records": trial["records"],
                    "bytes": trial["bytes"],
                    "duration_seconds": trial["duration_seconds"],
                    "records_per_second": trial["records_per_second"],
                    "megabytes_per_second": trial["megabytes_per_second"],
                    "errors": trial["errors"],
                    "latency_count": latency["count"],
                    "latency_min_ns": latency["min"],
                    "latency_p50_ns": latency["p50"],
                    "latency_p90_ns": latency["p90"],
                    "latency_p95_ns": latency["p95"],
                    "latency_p99_ns": latency["p99"],
                    "latency_p999_ns": latency["p999"],
                    "latency_max_ns": latency["max"],
                    "latency_mean_ns": latency["mean"],
                    "cpu_percent": trial.get("cpu_percent"),
                    "peak_rss_bytes": trial.get("peak_rss_bytes"),
                    "partitions": config.get("partitions"),
                    "producers": config.get("producers"),
                    "consumers": config.get("consumers"),
                    "record_size_bytes": config.get("record_size_bytes"),
                    "batch_size": config.get("batch_size"),
                    "acks": config.get("acks"),
                    "brokers": config.get("brokers"),
                    "replication_factor": config.get("replication_factor"),
                })


def write_summary_csv(results: list[dict], path: Path) -> None:
    """One row per scenario: median, spread, and the median trial's latencies."""
    columns = [
        "scenario", "description", "trials", "records_per_second_median",
        "records_per_second_min", "records_per_second_max",
        "records_per_second_stdev", "relative_spread", "megabytes_per_second",
        "p50_us", "p95_us", "p99_us", "p999_us", "max_us", "errors",
        "acks", "batch_size", "record_size_bytes", "partitions", "producers",
        "brokers", "replication_factor",
    ]
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for result in results:
            stats = spread(result)
            trial = median_trial(result)
            latency = trial["latency_nanos"]
            config = result["config"]
            writer.writerow({
                "scenario": result.get("name", result["scenario"]),
                "description": result.get("description", result.get("label", "")),
                "trials": stats["trials"],
                "records_per_second_median": round(stats["median"], 1),
                "records_per_second_min": round(stats["min"], 1),
                "records_per_second_max": round(stats["max"], 1),
                "records_per_second_stdev": round(stats["stdev"], 1),
                "relative_spread": round(stats["relative_spread"], 4),
                "megabytes_per_second": trial["megabytes_per_second"],
                "p50_us": round(latency["p50"] / 1000, 1),
                "p95_us": round(latency["p95"] / 1000, 1),
                "p99_us": round(latency["p99"] / 1000, 1),
                "p999_us": round(latency["p999"] / 1000, 1),
                "max_us": round(latency["max"] / 1000, 1),
                "errors": trial["errors"],
                "acks": config.get("acks"),
                "batch_size": config.get("batch_size"),
                "record_size_bytes": config.get("record_size_bytes"),
                "partitions": config.get("partitions"),
                "producers": config.get("producers"),
                "brokers": config.get("brokers"),
                "replication_factor": config.get("replication_factor"),
            })


def hardware_table(metadata: dict | None) -> list[str]:
    if not metadata:
        return ["_Host metadata was not captured for this run._"]

    host, cpu = metadata["host"], metadata["cpu"]
    storage, build, git = metadata["storage"], metadata["build"], metadata["git"]
    memory = metadata["memory"]["total_bytes"]

    def show(value, fallback: str = "unknown") -> str:
        return str(value) if value is not None else fallback

    disk = "unknown"
    if storage.get("rotational") is not None:
        disk = "rotational" if storage["rotational"] else "solid-state"
        if storage.get("model"):
            disk += f" ({storage['model']})"
    if storage.get("filesystem"):
        disk = f"{disk}, {storage['filesystem']}"

    rows = [
        "| Property | Value |",
        "|---|---|",
        f"| CPU | {show(cpu['model'])} |",
        f"| Logical cores | {show(cpu['logical_cores'])} |",
        f"| Memory | {f'{memory / 2**30:.1f} GiB' if memory else 'unknown'} |",
        f"| Storage | {disk} |",
        f"| Distribution | {show(host['distro'])} |",
        f"| Kernel | {show(host['kernel'])} |",
        f"| Architecture | {show(host['architecture'])} |",
        f"| Compiler | {show(build['id'])} {show(build['version'])} |",
        f"| Build type | {show(build['build_type'])} |",
        f"| Optimisation flags | `{show(build['cxx_flags'])}` |",
        f"| Commit | `{show(git['short_commit'])}`"
        f"{' **(uncommitted changes present)**' if git.get('dirty') else ''} |",
    ]
    if host.get("virtualisation"):
        rows.append(f"| Virtualisation | {host['virtualisation']} |")
    if cpu.get("governor"):
        rows.append(f"| CPU governor | {cpu['governor']} |")
    return rows


def results_table(results: list[dict]) -> list[str]:
    rows = [
        "| Scenario | Configuration | records/s (median) | Spread | MiB/s | p50 | p95 | p99 | p99.9 | max | Errors |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for result in results:
        stats = spread(result)
        trial = median_trial(result)
        latency = trial["latency_nanos"]
        config = result["config"]
        configuration = (
            f"{config.get('producers', '-')}p/"
            f"{config.get('partitions', '-')}part/"
            f"b{config.get('batch_size', '-')}/"
            f"{config.get('record_size_bytes', '-')}B/"
            f"{config.get('acks', '-')}"
        )
        spread_cell = (f"±{stats['relative_spread'] * 100:.0f}%"
                       if stats["trials"] > 1 else "n/a")
        rows.append(
            f"| {result.get('name', result['scenario'])} "
            f"| `{configuration}` "
            f"| {stats['median']:,.0f} "
            f"| {spread_cell} "
            f"| {trial['megabytes_per_second']:,.1f} "
            f"| {latency['p50'] / 1000:,.0f} us "
            f"| {latency['p95'] / 1000:,.0f} us "
            f"| {latency['p99'] / 1000:,.0f} us "
            f"| {latency['p999'] / 1000:,.0f} us "
            f"| {latency['max'] / 1000:,.0f} us "
            f"| {trial['errors']} |"
        )
    return rows


def cluster_section(cluster: dict | None) -> list[str]:
    if not cluster:
        return ["_Cluster metrics were not captured for this run._"]

    config = cluster.get("config", {})
    lines = [
        f"Measured on a {config.get('brokers', '?')}-broker cluster, "
        f"replication factor {config.get('replication_factor', '?')}, "
        f"{config.get('partitions', '?')} partitions, "
        f"{config.get('records', 0):,} records at "
        f"{config.get('record_size_bytes', '?')} B, acks={config.get('acks', '?')}.",
        "",
        "| Metric | Value |",
        "|---|---|",
    ]

    lag = cluster.get("replication_lag_records")
    if lag:
        lines += [
            f"| Replication lag p50 | {lag['p50']:,} records |",
            f"| Replication lag p99 | {lag['p99']:,} records |",
            f"| Replication lag max | {lag['max']:,} records |",
        ]
    converged = cluster.get("replication_converged")
    if converged is not None:
        lines.append(
            f"| Followers caught up after load | "
            f"{'yes, in ' + str(cluster['replication_convergence_seconds']) + ' s' if converged else 'NO'} |"
        )

    recovery = cluster.get("recovery", {})
    if recovery.get("measured"):
        lines += [
            f"| Recovery: accept connections | {recovery['time_to_accept_connections_seconds']} s |",
            f"| Recovery: log fully restored | "
            f"{recovery['time_to_restore_log_seconds'] if recovery['data_intact'] else 'NOT RESTORED'} s |",
            f"| Records before kill / after recovery | "
            f"{recovery['records_before_kill']:,} / {recovery['records_after_recovery']:,} |",
        ]

    resources = cluster.get("resources", {})
    if resources.get("cpu_percent_peak") is not None:
        lines.append(f"| Peak CPU (all brokers) | {resources['cpu_percent_peak']}% |")
    if resources.get("rss_bytes_peak"):
        lines.append(f"| Peak resident memory (all brokers) | "
                     f"{resources['rss_bytes_peak'] / 2**20:.0f} MiB |")
    if resources.get("bytes_written") is not None:
        lines.append(f"| Bytes written to disk | "
                     f"{resources['bytes_written'] / 2**20:,.1f} MiB |")
        if resources.get("bytes_written_per_record") is not None:
            lines.append(f"| Bytes written per record | "
                         f"{resources['bytes_written_per_record']} B |")
    else:
        lines.append("| Bytes written to disk | not available on this platform |")

    if recovery.get("measured"):
        lines += ["", f"Recovery method: {recovery['method']}."]
    return lines


def methodology_section(results: list[dict]) -> list[str]:
    method = results[0].get("method", {}) if results else {}
    return [
        "- **Load model**: "
        f"{method.get('loop', 'closed')} loop. Each producer thread waits for its "
        "acknowledgement before sending again, so throughput is bounded by the "
        "number of producer threads times the reciprocal of latency.",
        "- **Coordinated omission**: not corrected. A stalled request stops the "
        "thread that would have issued the next one, so the requests a queueing "
        "client would have sent during the stall are never measured. These "
        "figures understate the tail an open-loop client would see.",
        "- **Latency scope**: "
        f"{method.get('latency_scope', 'per produce request')}. A batch of 100 "
        "records is one measurement, not 100.",
        "- **Warm-up**: discarded before the measured window, so page-cache and "
        "allocator warm-up do not land in the results.",
        "- **Trials**: every scenario runs multiple times; the table reports the "
        "median and the full min-max spread. The spread is the number to check "
        "before believing any difference between two runs.",
        "- **Loopback**: all traffic is over 127.0.0.1. There is no physical "
        "network, so these numbers exclude NIC and switch latency and are an "
        "upper bound on what the same code would do across a real network.",
    ]


def build_markdown(results: list[dict], metadata: dict | None,
                   cluster: dict | None) -> str:
    git = (metadata or {}).get("git", {})
    host = (metadata or {}).get("host", {})
    platform_name = f"{host.get('os', 'unknown')} {host.get('architecture', '')}".strip()

    lines = [
        f"# PulseLog benchmark results ({platform_name})",
        "",
        "Generated by `scripts/benchmark_release.sh`. Every number here was "
        "measured on the hardware described below; none are transferred from "
        "another platform or another run.",
        "",
        f"Commit: `{git.get('short_commit', 'unknown')}`",
        "",
        "## Hardware and build",
        "",
        *hardware_table(metadata),
        "",
        "## Methodology",
        "",
        *methodology_section(results),
        "",
        "## Throughput and latency by scenario",
        "",
        *results_table(results),
        "",
        "Spread is (max - min) / median across trials. A scenario with a large "
        "spread cannot support a claim about a small difference.",
        "",
        "## Cluster behaviour: replication, recovery, resources",
        "",
        *cluster_section(cluster),
        "",
        "## Reproducing this",
        "",
        "```sh",
        "./scripts/benchmark_release.sh",
        "```",
        "",
        "Scenario parameters are in `benchmarks/config/release.json`. Raw "
        "per-trial data is in `results.csv`; per-scenario aggregates are in "
        "`summary.csv`.",
        "",
    ]
    return "\n".join(lines)


def markdown_to_html(markdown: str, charts: list[Path]) -> str:
    """A deliberately small Markdown subset: headings, tables, lists, code.

    Enough for this document and nothing else. Pulling in a Markdown library
    would make the report generator depend on something the benchmark host may
    not have, for a document whose structure this script controls entirely.
    """
    body: list[str] = []
    in_table = False
    in_code = False

    def close_table() -> None:
        nonlocal in_table
        if in_table:
            body.append("</tbody></table>")
            in_table = False

    def inline(text: str) -> str:
        text = escape(text)
        # Order matters: bold before italics, code last so its content is not
        # re-processed.
        while "**" in text:
            text = text.replace("**", "<strong>", 1).replace("**", "</strong>", 1)
        parts = text.split("`")
        rebuilt = []
        for index, part in enumerate(parts):
            rebuilt.append(f"<code>{part}</code>" if index % 2 else part)
        return "".join(rebuilt)

    lines = markdown.split("\n")
    index = 0
    while index < len(lines):
        line = lines[index]

        if line.startswith("```"):
            if in_code:
                body.append("</code></pre>")
                in_code = False
            else:
                close_table()
                body.append("<pre><code>")
                in_code = True
            index += 1
            continue
        if in_code:
            body.append(escape(line))
            index += 1
            continue

        if line.startswith("|"):
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            # The alignment row (---, ---:) is markup, not data.
            if all(set(c) <= set("-: ") and c for c in cells):
                index += 1
                continue
            if not in_table:
                body.append("<table><thead><tr>")
                body.extend(f"<th>{inline(c)}</th>" for c in cells)
                body.append("</tr></thead><tbody>")
                in_table = True
            else:
                body.append("<tr>")
                body.extend(f"<td>{inline(c)}</td>" for c in cells)
                body.append("</tr>")
            index += 1
            continue
        close_table()

        if line.startswith("#"):
            level = len(line) - len(line.lstrip("#"))
            body.append(f"<h{level}>{inline(line[level:].strip())}</h{level}>")
        elif line.startswith("- "):
            items = []
            while index < len(lines) and lines[index].startswith("- "):
                items.append(f"<li>{inline(lines[index][2:])}</li>")
                index += 1
            body.append("<ul>" + "".join(items) + "</ul>")
            continue
        elif line.strip():
            body.append(f"<p>{inline(line)}</p>")
        index += 1

    close_table()
    if in_code:
        body.append("</code></pre>")

    chart_html = ""
    for chart in charts:
        try:
            chart_html += f'<figure>{chart.read_text()}</figure>'
        except OSError:
            continue
    if chart_html:
        body.append("<h2>Charts</h2>")
        body.append(chart_html)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>PulseLog benchmark results</title>
<style>
  :root {{ color-scheme: light dark; }}
  body {{ font: 15px/1.6 -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
         max-width: 60rem; margin: 0 auto; padding: 2rem 1.25rem; }}
  h1 {{ font-size: 1.75rem; margin-bottom: .25rem; }}
  h2 {{ font-size: 1.2rem; margin-top: 2.5rem; padding-bottom: .3rem;
        border-bottom: 1px solid color-mix(in srgb, currentColor 20%, transparent); }}
  table {{ border-collapse: collapse; width: 100%; margin: 1rem 0; font-size: .86rem;
           display: block; overflow-x: auto; }}
  th, td {{ padding: .4rem .6rem; text-align: left; white-space: nowrap;
            border-bottom: 1px solid color-mix(in srgb, currentColor 15%, transparent); }}
  th {{ font-weight: 600; text-align: left; }}
  td:nth-child(n+3) {{ text-align: right; font-variant-numeric: tabular-nums; }}
  code {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: .85em;
          background: color-mix(in srgb, currentColor 8%, transparent);
          padding: .1em .35em; border-radius: 3px; }}
  pre {{ background: color-mix(in srgb, currentColor 6%, transparent); padding: 1rem;
         border-radius: 6px; overflow-x: auto; }}
  pre code {{ background: none; padding: 0; }}
  figure {{ margin: 1.5rem 0; overflow-x: auto; }}
  svg {{ max-width: 100%; height: auto; }}
</style>
</head>
<body>
{chr(10).join(body)}
</body>
</html>
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--results", default="results", type=Path)
    parser.add_argument("--out", default="results", type=Path)
    args = parser.parse_args()

    results = load_results(args.results)
    if not results:
        print(f"error: no benchmark results in {args.results}", file=sys.stderr)
        return 1

    args.out.mkdir(parents=True, exist_ok=True)
    metadata = load_optional(args.results / "metadata.json")
    cluster = load_optional(args.results / "cluster_metrics.json")

    write_trial_csv(results, args.out / "results.csv")
    write_summary_csv(results, args.out / "summary.csv")

    markdown = build_markdown(results, metadata, cluster)
    (args.out / "REPORT.md").write_text(markdown)

    charts = [p for p in (args.out / "throughput.svg", args.out / "latency_p99.svg")
              if p.exists()]
    (args.out / "report.html").write_text(markdown_to_html(markdown, charts))

    print(f"  wrote {args.out / 'results.csv'} ({sum(len(r['trials']) for r in results)} trials)")
    print(f"  wrote {args.out / 'summary.csv'} ({len(results)} scenarios)")
    print(f"  wrote {args.out / 'REPORT.md'}")
    print(f"  wrote {args.out / 'report.html'}")

    noisy = [r.get("name", r["scenario"]) for r in results
             if spread(r)["relative_spread"] > 0.5]
    if noisy:
        print(f"\n  note: high run-to-run spread (>50%) in: {', '.join(noisy)}")
        print("  differences smaller than the spread are not meaningful for these.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
