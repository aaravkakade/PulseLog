#!/usr/bin/env bash
# Everything that must pass before calling a commit releasable.
#
#   ./scripts/verify_release.sh              # everything
#   ./scripts/verify_release.sh --quick      # skip sanitizers and benchmarks
#   ./scripts/verify_release.sh --list       # show the checks and exit
#
# This is the same set CI runs, in one place, so a failure can be reproduced
# locally without pushing. It is deliberately not a subset: a check that only
# ever runs in CI is a check nobody debugs.
#
# What it does NOT do, and why:
#   - It does not run the GCC build on macOS. GCC's warning set is a real
#     gate and it is Linux-only here; that lives in CI. The script says so
#     rather than skipping quietly.
#   - It does not claim a platform is validated. It reports what ran on the
#     machine it ran on.
set -uo pipefail

QUICK=0
FAILED=()
PASSED=()
SKIPPED=()
START_TIME=$SECONDS

cd "$(dirname "$0")/.."

CHECKS=(
  "formatting:clang-format against the pinned version"
  "build-strict:optimised build with -Werror"
  "build-debug:debug build with -Werror and assertions live"
  "unit:unit tests"
  "integration:integration tests"
  "asan:AddressSanitizer + UndefinedBehaviorSanitizer"
  "tsan:ThreadSanitizer"
  "tidy:clang-tidy (in a Linux container matching CI)"
  "smoke:single-broker smoke test"
  "failure:single-broker failure injection"
  "failure-cluster:three-broker failure injection"
  "failure-cluster-asan:three-broker failure injection under ASan"
  "bench-repro:benchmark pipeline runs end to end and is reproducible"
  "docker:image builds and a three-broker cluster serves traffic"
)

for arg in "$@"; do
  case "$arg" in
    --quick) QUICK=1 ;;
    --list)
      printf '%s\n' "${CHECKS[@]}" | sed 's/:/ -- /'
      exit 0 ;;
    -h|--help) sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown option: $arg" >&2; exit 2 ;;
  esac
done

banner() {
  echo
  echo "=============================================================="
  echo "  $*"
  echo "=============================================================="
}

record() {
  local name="$1" status="$2"
  case "$status" in
    pass) PASSED+=("$name"); echo "  ---> PASS: $name" ;;
    fail) FAILED+=("$name"); echo "  ---> FAIL: $name" >&2 ;;
    skip) SKIPPED+=("$name"); echo "  ---> SKIP: $name" ;;
  esac
}

run_check() {
  local name="$1"; shift
  banner "$name"
  if "$@"; then record "$name" pass; else record "$name" fail; fi
}

JOBS=$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)
IS_LINUX=0
[[ "$(uname -s)" == "Linux" ]] && IS_LINUX=1

# --- checks -----------------------------------------------------------------

check_formatting() { scripts/format.sh --check; }

check_build_strict() {
  cmake -S . -B build-verify -G Ninja \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo -DPULSELOG_WERROR=ON > /dev/null \
    && cmake --build build-verify -j"$JOBS"
}

check_build_debug() {
  # Debug matters on its own: it is the only configuration where the
  # assertions added to Result's accessors actually execute.
  cmake -S . -B build-verify-debug -G Ninja \
    -DCMAKE_BUILD_TYPE=Debug -DPULSELOG_WERROR=ON \
    -DPULSELOG_BUILD_BENCHMARKS=OFF > /dev/null \
    && cmake --build build-verify-debug -j"$JOBS" \
    && ctest --test-dir build-verify-debug --output-on-failure -j2 --timeout 300
}

check_unit() {
  ctest --test-dir build-verify --output-on-failure -j2 --timeout 300 -R '^test_(base|protocol|storage|net|metrics|concurrency)'
}

check_integration() {
  ctest --test-dir build-verify --output-on-failure -j1 --timeout 600 -R '^test_integration'
}

check_asan() {
  cmake -S . -B build-verify-asan -G Ninja \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo -DPULSELOG_SANITIZER=address+undefined \
    -DPULSELOG_BUILD_BENCHMARKS=OFF -DPULSELOG_WERROR=ON > /dev/null \
    && cmake --build build-verify-asan -j"$JOBS" || return 1
  # detect_leaks is unsupported by macOS ASan and aborts the process at
  # start-up, so it is only requested where it works.
  local asan_opts="halt_on_error=1:abort_on_error=1"
  (( IS_LINUX == 1 )) && asan_opts="detect_leaks=1:$asan_opts"
  ASAN_OPTIONS="$asan_opts" UBSAN_OPTIONS="print_stacktrace=1:halt_on_error=1" \
    ctest --test-dir build-verify-asan --output-on-failure -j1 --timeout 900
}

check_tsan() {
  cmake -S . -B build-verify-tsan -G Ninja \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo -DPULSELOG_SANITIZER=thread \
    -DPULSELOG_BUILD_BENCHMARKS=OFF -DPULSELOG_WERROR=ON > /dev/null \
    && cmake --build build-verify-tsan -j"$JOBS" || return 1
  TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1" \
    ctest --test-dir build-verify-tsan --output-on-failure -j1 --timeout 900
}

check_tidy() {
  # Run in the same image CI uses. clang-tidy's findings differ between major
  # versions, so a local run against a different one proves nothing about
  # whether CI will pass.
  command -v docker > /dev/null 2>&1 || return 2
  docker run --rm -v "$PWD":/src -w /src ubuntu:24.04 bash -c '
    set -e
    apt-get update -qq > /dev/null 2>&1
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends \
      build-essential cmake ninja-build git ca-certificates clang clang-tidy > /dev/null 2>&1
    cd /src
    cmake -S . -B /tidy -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo \
      -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ \
      -DPULSELOG_BUILD_TESTS=OFF -DPULSELOG_BUILD_BENCHMARKS=OFF > /dev/null 2>&1
    clang-tidy --version | head -2
    git config --global --add safe.directory /src
    git ls-files "src/*.cc" "clients/cpp/src/*.cc" "apps/*/*.cc" \
      | xargs clang-tidy -p /tidy --warnings-as-errors="*" --quiet
  '
}

check_smoke() { BUILD_DIR=build-verify scripts/smoke_test.sh; }

check_failure() { BUILD_DIR=build-verify scripts/failure_test.sh; }

check_failure_cluster() {
  BUILD_DIR=build-verify ROOT=/tmp/pulselog-verify-cluster scripts/failure_cluster_test.sh
}

check_failure_cluster_asan() {
  [[ -x build-verify-asan/bin/pulselog-broker ]] || return 2
  local asan_opts="halt_on_error=1:abort_on_error=1"
  (( IS_LINUX == 1 )) && asan_opts="detect_leaks=1:$asan_opts"
  ASAN_OPTIONS="$asan_opts" UBSAN_OPTIONS="print_stacktrace=1:halt_on_error=1" \
    BUILD_DIR=build-verify-asan ROOT=/tmp/pulselog-verify-cluster-asan \
    scripts/failure_cluster_test.sh
}

check_bench_repro() {
  # Two things are checked: that the pipeline runs end to end and produces
  # every artifact it claims to, and that a second run lands within the spread
  # the first one reported. A benchmark whose median leaves its own error bars
  # between runs is not measuring what it says it is.
  #
  # Deliberately NOT --quick. Quick runs are documented as too short to be
  # stable, so checking their reproducibility measures the shortening rather
  # than the harness -- an early version of this check failed on exactly that,
  # with acks=quorum landing at 25.6k then 8.0k records/s across two 10,000
  # record runs. A representative subset at full record counts instead, so the
  # check stays under about ten minutes while still measuring something real.
  local subset="01-single-producer,05-leader-ack,06-quorum-ack,08-no-batching"

  rm -rf /tmp/pulselog-repro-a /tmp/pulselog-repro-b

  for run in a b; do
    python3 scripts/run_benchmarks.py --build-dir build-verify \
        --out "/tmp/pulselog-repro-$run" --trials 3 --only "$subset" \
        > "/tmp/repro-$run.log" 2>&1 || {
          tail -25 "/tmp/repro-$run.log" >&2; return 1; }
  done

  # summary.csv is produced by bench_report.py, not by the runner, so both
  # runs need it before they can be compared.
  python3 scripts/bench_metadata.py --build-dir build-verify \
    --out /tmp/pulselog-repro-a/metadata.json > /dev/null || return 1
  for run in a b; do
    python3 scripts/bench_report.py --results "/tmp/pulselog-repro-$run" \
      --out "/tmp/pulselog-repro-$run" > /dev/null || return 1
  done

  for artifact in REPORT.md report.html results.csv summary.csv metadata.json; do
    [[ -f "/tmp/pulselog-repro-a/$artifact" ]] || {
      echo "missing artifact: $artifact" >&2; return 1; }
  done

  python3 - <<'PYEOF'
import csv, sys

def load(path):
    with open(path) as handle:
        return {r["scenario"]: r for r in csv.DictReader(handle)}

a = load("/tmp/pulselog-repro-a/summary.csv")
b = load("/tmp/pulselog-repro-b/summary.csv")

if set(a) != set(b):
    print(f"scenario sets differ: {set(a) ^ set(b)}", file=sys.stderr)
    sys.exit(1)

outside = []
print(f"  {'scenario':<36}{'run a':>12}{'run b':>12}{'delta':>8}{'tol':>8}")
for name in sorted(a):
    ma = float(a[name]["records_per_second_median"])
    mb = float(b[name]["records_per_second_median"])
    # Each run reports its own spread; allow the wider of the two, with a 25%
    # floor so a scenario that happened to look tight in both runs is not held
    # to a tolerance narrower than the harness can resolve.
    tol = max(float(a[name]["relative_spread"]), float(b[name]["relative_spread"]), 0.25)
    delta = abs(ma - mb) / max(ma, mb)
    flag = "  <-- outside" if delta > tol else ""
    print(f"  {name:<36}{ma:>12,.0f}{mb:>12,.0f}{delta*100:>7.1f}%{tol*100:>7.1f}%{flag}")
    if delta > tol:
        outside.append((name, ma, mb, delta, tol))

if outside:
    print(file=sys.stderr)
    print("These scenarios moved further between runs than the spread they "
          "themselves reported:", file=sys.stderr)
    for name, ma, mb, delta, tol in outside:
        print(f"  {name}: {ma:,.0f} then {mb:,.0f} "
              f"({delta*100:.1f}% apart, tolerance {tol*100:.1f}%)", file=sys.stderr)
    print(file=sys.stderr)
    print("That is a statement about the scenario, not necessarily a defect: it "
          "means\nits median cannot support a claim on this machine. Either the "
          "trial count\nis too low or the scenario is genuinely unstable here. Do "
          "not publish a\nnumber for it without saying so.", file=sys.stderr)
    sys.exit(1)
print("\n  both runs agree within the spread each of them reported")
PYEOF
}

check_docker() {
  command -v docker > /dev/null 2>&1 || return 2
  docker build -f docker/Dockerfile -t pulselog:verify . > /tmp/docker-build.log 2>&1 || {
    tail -25 /tmp/docker-build.log >&2; return 1; }
  docker compose -f docker/docker-compose.yml up -d --wait --wait-timeout 180 \
    > /tmp/docker-up.log 2>&1 || { tail -25 /tmp/docker-up.log >&2; return 1; }
  local result=0
  docker compose -f docker/docker-compose.yml exec -T pulselog-1 \
    pulselog-cli create-topic verify-topic --partitions=6 --replication=3 \
    --brokers=pulselog-1:9092 || result=1
  docker compose -f docker/docker-compose.yml exec -T pulselog-1 \
    pulselog-cli produce verify-topic --value=hello --count=500 --batch=50 \
    --acks=quorum --brokers=pulselog-1:9092 || result=1
  docker compose -f docker/docker-compose.yml exec -T pulselog-3 \
    pulselog-cli offsets verify-topic --brokers=pulselog-3:9092 || result=1
  docker compose -f docker/docker-compose.yml down -v > /dev/null 2>&1
  return $result
}

# A check returning 2 means "the tool is not here", which is a skip, not a
# failure. Anything else is a failure.
run_optional() {
  local name="$1"; shift
  banner "$name"
  "$@"
  local rc=$?
  case $rc in
    0) record "$name" pass ;;
    2) record "$name" skip ;;
    *) record "$name" fail ;;
  esac
}

# --- run --------------------------------------------------------------------

echo "PulseLog release verification"
echo "  platform: $(uname -s) $(uname -m)"
echo "  commit:   $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
[[ -n "$(git status --porcelain 2>/dev/null)" ]] && echo "  WARNING:  uncommitted changes present"
(( QUICK == 1 )) && echo "  mode:     --quick (sanitizers, clang-tidy, benchmarks and docker skipped)"

run_check "formatting" check_formatting
run_check "build-strict" check_build_strict
run_check "build-debug" check_build_debug
run_check "unit" check_unit
run_check "integration" check_integration
run_check "smoke" check_smoke
run_check "failure" check_failure
run_check "failure-cluster" check_failure_cluster

if (( QUICK == 0 )); then
  run_check "asan" check_asan
  run_check "tsan" check_tsan
  run_check "failure-cluster-asan" check_failure_cluster_asan
  run_optional "tidy" check_tidy
  run_check "bench-repro" check_bench_repro
  run_optional "docker" check_docker
else
  for name in asan tsan failure-cluster-asan tidy bench-repro docker; do
    SKIPPED+=("$name")
  done
fi

# --- report -----------------------------------------------------------------

ELAPSED=$((SECONDS - START_TIME))
banner "summary  (${ELAPSED}s)"

for name in "${PASSED[@]:-}";  do [[ -n "$name" ]] && echo "  PASS  $name"; done
for name in "${SKIPPED[@]:-}"; do [[ -n "$name" ]] && echo "  SKIP  $name"; done
for name in "${FAILED[@]:-}";  do [[ -n "$name" ]] && echo "  FAIL  $name"; done

echo
if (( IS_LINUX == 0 )); then
  echo "Note: this ran on $(uname -s). The GCC strict build and the epoll backend"
  echo "are Linux-only and were NOT exercised here -- CI covers both. Do not"
  echo "describe the Linux platform as validated on the strength of this run."
fi

echo
if (( ${#FAILED[@]:-0} > 0 )); then
  echo "${#FAILED[@]} check(s) failed" >&2
  exit 1
fi
echo "${#PASSED[@]:-0} check(s) passed, ${#SKIPPED[@]:-0} skipped"
