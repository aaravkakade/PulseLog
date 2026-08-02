#!/usr/bin/env python3
"""Turn benchmark JSON into charts and a Markdown results table.

Charts are emitted as self-contained SVG written by hand. matplotlib would be
the obvious choice, but a benchmark suite that cannot draw its own results
because a plotting library is missing is a benchmark suite nobody runs -- and
these are bar charts, which do not need a plotting library. If matplotlib is
installed it is used instead, since its output is nicer.

Usage:
  scripts/plot_results.py --results results --out results
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# A small, colour-blind-safe palette. Deliberately not a rainbow: adjacent bars
# in these charts are unrelated scenarios, not a sequence.
PALETTE = ["#2f6fdb", "#3fa35a", "#d98014", "#b3453f", "#7a5bb5", "#4c8f8f"]
TEXT = "#1b1f23"
MUTED = "#6a737d"
GRID = "#d8dee4"


def escape(text: str) -> str:
    return (text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            .replace('"', "&quot;"))


def format_number(value: float) -> str:
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"{value / 1_000:.0f}k"
    if value >= 10:
        return f"{value:.0f}"
    return f"{value:.2f}"


def bar_chart_svg(title: str, subtitle: str, labels: list[str], values: list[float],
                  value_suffix: str = "", log_scale: bool = False) -> str:
    """Horizontal bar chart. Horizontal because scenario names are long."""
    if not values:
        return ""

    row_height = 34
    top = 74
    left = 300
    right = 96
    width = 980
    height = top + row_height * len(values) + 34
    plot_width = width - left - right

    peak = max(values) if max(values) > 0 else 1.0

    def bar_width(value: float) -> float:
        if value <= 0:
            return 0.0
        if log_scale:
            import math
            floor = max(min(v for v in values if v > 0), 1.0)
            span = math.log10(peak / floor) or 1.0
            return max(2.0, plot_width * math.log10(max(value, floor) / floor) / span)
        return plot_width * value / peak

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" '
        f'width="{width}" height="{height}" font-family="-apple-system, Segoe UI, '
        f'Helvetica, Arial, sans-serif">',
        f'<rect width="{width}" height="{height}" fill="#ffffff"/>',
        f'<text x="24" y="34" font-size="18" font-weight="600" fill="{TEXT}">'
        f'{escape(title)}</text>',
        f'<text x="24" y="55" font-size="12" fill="{MUTED}">{escape(subtitle)}</text>',
    ]

    for i, (label, value) in enumerate(zip(labels, values)):
        y = top + i * row_height
        colour = PALETTE[i % len(PALETTE)]
        parts.append(
            f'<line x1="{left}" y1="{y + row_height - 4}" x2="{width - right + 60}" '
            f'y2="{y + row_height - 4}" stroke="{GRID}" stroke-width="1"/>'
        )
        parts.append(
            f'<text x="{left - 12}" y="{y + 18}" font-size="12" fill="{TEXT}" '
            f'text-anchor="end">{escape(label)}</text>'
        )
        parts.append(
            f'<rect x="{left}" y="{y + 5}" width="{bar_width(value):.1f}" height="18" '
            f'rx="3" fill="{colour}"/>'
        )
        parts.append(
            f'<text x="{left + bar_width(value) + 8:.1f}" y="{y + 18}" font-size="12" '
            f'fill="{MUTED}">{format_number(value)}{escape(value_suffix)}</text>'
        )

    if log_scale:
        parts.append(
            f'<text x="24" y="{height - 12}" font-size="11" fill="{MUTED}">'
            f'bar length is logarithmic; values span several orders of magnitude</text>'
        )
    parts.append("</svg>")
    return "\n".join(parts)


def try_matplotlib(title: str, subtitle: str, labels: list[str], values: list[float],
                   xlabel: str, path: Path) -> bool:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return False

    fig, ax = plt.subplots(figsize=(11, max(3.0, 0.45 * len(labels) + 1.6)))
    positions = range(len(labels))
    ax.barh(list(positions), values, color=PALETTE[0], height=0.6)
    ax.set_yticks(list(positions))
    ax.set_yticklabels(labels, fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel(xlabel)
    ax.set_title(f"{title}\n{subtitle}", fontsize=11, loc="left")
    ax.grid(axis="x", alpha=0.3)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    fig.tight_layout()
    fig.savefig(path, dpi=140)
    plt.close(fig)
    return True


def median_trial(result: dict) -> dict:
    trials = sorted(result["trials"], key=lambda t: t["records_per_second"])
    return trials[len(trials) // 2]


def load_results(directory: Path) -> list[dict]:
    # The results directory also holds aggregates and host metadata, which are
    # not scenarios and have no throughput to plot.
    skip = {"summary.json", "metadata.json", "cluster_metrics.json"}
    results = []
    for path in sorted(directory.glob("*.json")):
        if path.name in skip:
            continue
        with open(path) as handle:
            try:
                results.append(json.load(handle))
            except json.JSONDecodeError:
                print(f"warning: skipping unparseable {path}", file=sys.stderr)
    return results


def markdown_table(results: list[dict]) -> str:
    rows = [
        "| Scenario | Config | records/s (median) | spread | MiB/s | p50 | p99 | p99.9 | err |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for result in results:
        trial = median_trial(result)
        ordered = sorted(result["trials"], key=lambda t: t["records_per_second"])
        spread = (f"{ordered[0]['records_per_second']:,.0f}-"
                  f"{ordered[-1]['records_per_second']:,.0f}"
                  if len(ordered) > 1 else "-")
        config = result["config"]
        latency = trial["latency_nanos"]
        summary = (
            f"{config['producers']}p/{config['partitions']}part/"
            f"b{config['batch_size']}/{config['record_size_bytes']}B/"
            f"{config['acks']}"
        )
        rows.append(
            f"| {result.get('name', result['scenario'])} "
            f"| `{summary}` "
            f"| {trial['records_per_second']:,.0f} "
            f"| {spread} "
            f"| {trial['megabytes_per_second']:,.1f} "
            f"| {latency['p50'] / 1000:,.0f} us "
            f"| {latency['p99'] / 1000:,.0f} us "
            f"| {latency['p999'] / 1000:,.0f} us "
            f"| {trial['errors']} |"
        )
    return "\n".join(rows)


def environment_block(results: list[dict]) -> str:
    if not results:
        return ""
    env = results[0]["environment"]
    method = results[0].get("method", {})
    lines = [
        "| Property | Value |",
        "|---|---|",
        f"| OS | {env['os']} {env['kernel']} |",
        f"| Architecture | {env['architecture']} |",
        f"| CPU | {env['cpu_model']} ({env['cpu_count']} logical cores) |",
        f"| Memory | {env['total_memory_bytes'] / (1024 ** 3):.1f} GiB |",
        f"| Compiler | {env['compiler']} |",
        f"| Event loop | {env['poller']} |",
        f"| Checksum | {env['checksum']} |",
        f"| Load model | {method.get('loop', 'closed')} |",
        f"| Latency scope | {method.get('latency_scope', 'per request')} |",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--results", default="results", type=Path)
    parser.add_argument("--out", default="results", type=Path)
    args = parser.parse_args()

    results = load_results(args.results)
    if not results:
        print(f"error: no result files in {args.results}", file=sys.stderr)
        print("run scripts/run_benchmarks.py first", file=sys.stderr)
        return 1

    args.out.mkdir(parents=True, exist_ok=True)
    results.sort(key=lambda r: r.get("name", r["scenario"]))

    labels = [r.get("name", r["scenario"]) for r in results]
    throughput = [median_trial(r)["records_per_second"] for r in results]
    p99 = [median_trial(r)["latency_nanos"]["p99"] / 1000.0 for r in results]

    subtitle = (
        f"{results[0]['environment']['cpu_model']} - "
        f"{results[0]['environment']['os']} - "
        f"{results[0]['environment']['compiler']}"
    )

    charts = [
        ("throughput.svg", "Throughput by scenario", throughput, " rec/s",
         "records per second", True),
        ("latency_p99.svg", "p99 latency by scenario (lower is better)", p99, " us",
         "microseconds", True),
    ]
    for filename, title, values, suffix, xlabel, log_scale in charts:
        svg_path = args.out / filename
        svg_path.write_text(bar_chart_svg(title, subtitle, labels, values, suffix,
                                          log_scale))
        print(f"wrote {svg_path}")
        png_path = args.out / filename.replace(".svg", ".png")
        if try_matplotlib(title, subtitle, labels, values, xlabel, png_path):
            print(f"wrote {png_path}")

    table_path = args.out / "results_table.md"
    content = [
        "## Environment", "", environment_block(results), "",
        "## Results", "",
        "Median of all trials per scenario. Latency is per produce request; "
        "with batching a request carries many records.", "",
        markdown_table(results), "",
    ]
    table_path.write_text("\n".join(content))
    print(f"wrote {table_path}")
    print()
    print("\n".join(content))
    return 0


if __name__ == "__main__":
    sys.exit(main())
