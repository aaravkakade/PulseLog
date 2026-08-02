#!/usr/bin/env bash
# One command to produce a complete, reproducible benchmark report.
#
#   ./scripts/benchmark_release.sh
#
# Builds a release binary, records the machine it is about to measure, runs
# every scenario in benchmarks/config/release.json for the configured number of
# trials, measures the cluster-level costs a throughput number hides
# (replication lag, recovery time, CPU, memory, disk), and writes JSON, CSV,
# charts, and a report.
#
# The point of putting all of this behind one script is that a result nobody
# can reproduce is not a result. Everything that affects the numbers -- build
# flags, scenario parameters, trial count -- is either in version control or
# printed into the report.
#
# Options:
#   --trials N      repeats per scenario (default 5; below 3 is refused)
#   --out DIR       output directory (default results)
#   --build-dir DIR build directory (default build-release)
#   --quick         short runs, for checking the pipeline works; the results
#                   are explicitly marked as not publishable
#   --skip-build    reuse an existing build directory
#   --keep-going    run remaining steps even if a scenario fails
set -uo pipefail

TRIALS=5
OUT_DIR="results"
BUILD_DIR="build-release"
QUICK=0
SKIP_BUILD=0
KEEP_GOING=0
STEP=0
TOTAL_STEPS=14
FAILED=0

usage() { sed -n '2,26p' "$0" | sed 's/^# \{0,1\}//'; exit 0; }

while (( $# > 0 )); do
  case "$1" in
    --trials)     TRIALS="$2"; shift 2 ;;
    --trials=*)   TRIALS="${1#*=}"; shift ;;
    --out)        OUT_DIR="$2"; shift 2 ;;
    --out=*)      OUT_DIR="${1#*=}"; shift ;;
    --build-dir)  BUILD_DIR="$2"; shift 2 ;;
    --build-dir=*) BUILD_DIR="${1#*=}"; shift ;;
    --quick)      QUICK=1; shift ;;
    --skip-build) SKIP_BUILD=1; shift ;;
    --keep-going) KEEP_GOING=1; shift ;;
    -h|--help)    usage ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

cd "$(dirname "$0")/.."
LOG_DIR="$OUT_DIR/logs"

step() {
  STEP=$((STEP + 1))
  echo
  echo "[$STEP/$TOTAL_STEPS] $*"
}

die() {
  echo >&2
  echo "FAILED at step $STEP: $*" >&2
  preserve_logs
  exit 1
}

# Step 14. Anything that failed leaves evidence behind. A benchmark run that
# dies at minute nine with no logs costs another nine minutes to diagnose.
preserve_logs() {
  mkdir -p "$LOG_DIR"
  local found=0
  for source in /tmp/pulselog-bench-* /tmp/pulselog-cluster-metrics-*; do
    [[ -d "$source" ]] || continue
    find "$source" -name '*.log' -exec cp {} "$LOG_DIR/" \; 2>/dev/null && found=1
  done
  if (( found == 1 )); then
    echo "broker logs preserved in $LOG_DIR/" >&2
  fi
}

# --- 1. prerequisites -------------------------------------------------------

step "verifying prerequisites"
MISSING=()
for tool in cmake python3 git; do
  command -v "$tool" >/dev/null 2>&1 || MISSING+=("$tool")
done
(( ${#MISSING[@]} == 0 )) || die "missing required tools: ${MISSING[*]}"

PYTHON_OK=$(python3 -c 'import sys; print(1 if sys.version_info >= (3, 9) else 0)')
[[ "$PYTHON_OK" == "1" ]] || die "python3 >= 3.9 required, found $(python3 -V)"

if (( TRIALS < 3 )) && (( QUICK == 0 )); then
  die "at least 3 trials are required for a meaningful median and spread (got $TRIALS)"
fi

# A dirty tree means the binaries cannot be traced to a commit. Warn rather
# than refuse: measuring an uncommitted change is exactly what you want while
# optimising. The report marks the run as unreproducible.
if [[ -n "$(git status --porcelain 2>/dev/null)" ]]; then
  echo "  warning: uncommitted changes; results will be marked unreproducible"
fi
echo "  cmake:   $(cmake --version | head -1)"
echo "  python:  $(python3 -V)"
echo "  trials:  $TRIALS per scenario"
if (( QUICK == 1 )); then
  echo "  MODE:    --quick, NOT VALID FOR PUBLISHED NUMBERS"
fi

# --- 2. build ---------------------------------------------------------------

step "building an optimised binary"
if (( SKIP_BUILD == 1 )) && [[ -x "$BUILD_DIR/bin/pulselog-broker" ]]; then
  echo "  reusing $BUILD_DIR (--skip-build)"
else
  GENERATOR=()
  command -v ninja >/dev/null 2>&1 && GENERATOR=(-G Ninja)
  # RelWithDebInfo, not Release: identical optimisation, but keeps the frame
  # pointers and symbols that make a profile of a slow result readable. A
  # benchmark you cannot profile only tells you that something is slow.
  cmake -S . -B "$BUILD_DIR" "${GENERATOR[@]}" \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DPULSELOG_BUILD_TESTS=OFF \
    > "$BUILD_DIR-configure.log" 2>&1 \
    || { cat "$BUILD_DIR-configure.log" >&2; die "cmake configure failed"; }

  JOBS=$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)
  cmake --build "$BUILD_DIR" -j"$JOBS" > "$BUILD_DIR-build.log" 2>&1 \
    || { tail -40 "$BUILD_DIR-build.log" >&2; die "build failed"; }
  rm -f "$BUILD_DIR-configure.log" "$BUILD_DIR-build.log"
  echo "  built with $JOBS jobs"
fi

for binary in pulselog-broker pulselog-bench pulselog-cli; do
  [[ -x "$BUILD_DIR/bin/$binary" ]] || die "$BUILD_DIR/bin/$binary is missing after build"
done
echo "  binaries: $(ls "$BUILD_DIR"/bin | tr '\n' ' ')"

# --- 3. metadata ------------------------------------------------------------

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR" "$LOG_DIR"

step "capturing host and build metadata"
python3 scripts/bench_metadata.py \
  --build-dir "$BUILD_DIR" \
  --data-path "${TMPDIR:-/tmp}" \
  --out "$OUT_DIR/metadata.json" \
  || die "metadata capture failed"

# --- 4-8, 13. scenarios -----------------------------------------------------
#
# run_benchmarks.py owns the measurement loop: it starts a real broker cluster
# per scenario, waits until every broker accepts connections, runs the warm-up
# and discards it, runs the trials, records the full latency histogram, and
# shuts the cluster down cleanly. These steps are announced here so the output
# names what is happening, but the work is in one place rather than split
# across a shell script and a Python script.

step "starting broker clusters (one per scenario, from benchmarks/config/release.json)"
step "waiting for broker readiness before each measurement"
step "running warm-up, discarded before the measured window"
step "running $TRIALS measured trial(s) per scenario"
step "collecting raw latency histograms"

QUICK_FLAG=()
(( QUICK == 1 )) && QUICK_FLAG=(--quick)

BENCH_START=$(date +%s)
if ! python3 scripts/run_benchmarks.py \
      --build-dir "$BUILD_DIR" \
      --out "$OUT_DIR" \
      --trials "$TRIALS" \
      "${QUICK_FLAG[@]}"; then
  FAILED=1
  if (( KEEP_GOING == 0 )); then
    die "one or more scenarios failed"
  fi
  echo "  warning: some scenarios failed; continuing because --keep-going was given" >&2
fi
BENCH_ELAPSED=$(( $(date +%s) - BENCH_START ))
echo "  scenarios completed in ${BENCH_ELAPSED}s"

# --- 11. cluster-level metrics ----------------------------------------------

step "measuring replication lag, recovery time, CPU, memory and disk"
CLUSTER_RECORDS=200000
(( QUICK == 1 )) && CLUSTER_RECORDS=40000
if ! python3 scripts/bench_cluster_metrics.py \
      --build-dir "$BUILD_DIR" \
      --out "$OUT_DIR/cluster_metrics.json" \
      --records "$CLUSTER_RECORDS"; then
  FAILED=1
  echo "  warning: cluster metrics failed; the report will say so rather than omit it" >&2
fi

# --- 9, 10, 12. export and report -------------------------------------------

step "exporting JSON and CSV"
step "computing medians, percentiles and spread across trials"

step "generating charts and the report"
python3 scripts/plot_results.py --results "$OUT_DIR" --out "$OUT_DIR" \
  || echo "  warning: chart generation failed; the report will omit charts" >&2

python3 scripts/bench_report.py --results "$OUT_DIR" --out "$OUT_DIR" \
  || die "report generation failed"

# --- 13. shutdown -----------------------------------------------------------

step "verifying no broker processes were left behind"
STRAY=$(pgrep -f "$BUILD_DIR/bin/pulselog-broker" 2>/dev/null | wc -l | tr -d ' ')
if [[ "$STRAY" != "0" ]]; then
  echo "  warning: $STRAY broker process(es) still running; killing them" >&2
  pkill -f "$BUILD_DIR/bin/pulselog-broker" 2>/dev/null
  FAILED=1
else
  echo "  all broker processes exited cleanly"
fi

# --- 14. logs ---------------------------------------------------------------

step "finalising output"
if (( FAILED == 1 )); then
  preserve_logs
else
  rmdir "$LOG_DIR" 2>/dev/null
fi

echo
echo "results in $OUT_DIR/"
echo "  REPORT.md          the report"
echo "  report.html        standalone, charts inlined"
echo "  results.csv        one row per trial"
echo "  summary.csv        one row per scenario"
echo "  metadata.json      hardware and build"
echo "  cluster_metrics.json  replication lag, recovery, resources"

if (( QUICK == 1 )); then
  echo
  echo "REMINDER: --quick runs are too short to be stable. Do not publish these." >&2
fi

if (( FAILED == 1 )); then
  echo
  echo "completed with failures; see the warnings above" >&2
  exit 1
fi
echo
echo "benchmark run complete"
