#!/usr/bin/env bash
# Linux end-to-end smoke test.
#
# Development happens on macOS, so this is the only place the epoll backend,
# Linux socket semantics and fdatasync are exercised end to end. It builds a
# real three-broker cluster on loopback and drives every subsystem through it:
#
#   epoll backend selection      multi-broker startup
#   topic creation               multiple partitions
#   producer + consumer traffic  leader-follower replication
#   quorum acknowledgements      consumer offset commits
#   broker restart               crash recovery (SIGKILL)
#   backpressure                 graceful shutdown
#
# Each check states the property it is verifying and fails loudly with the
# broker logs attached.
set -uo pipefail

BUILD_DIR="${BUILD_DIR:-build}"
BROKER="$BUILD_DIR/bin/pulselog-broker"
CLI="$BUILD_DIR/bin/pulselog-cli"
ROOT="${ROOT:-/tmp/pulselog-linux-smoke}"
FAILURES=0
declare -a PIDS=()
declare -a PORTS=()
declare -a METRICS_PORTS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    [[ -n "$pid" ]] && kill -9 "$pid" 2>/dev/null
  done
  wait 2>/dev/null
}
trap cleanup EXIT

pass() { echo "  PASS: $*"; }

fail() {
  echo "  FAIL: $*" >&2
  FAILURES=$((FAILURES + 1))
}

fatal() {
  echo "FATAL: $*" >&2
  for i in "${!PORTS[@]}"; do
    echo "--- broker $i log (tail) ---" >&2
    tail -30 "$ROOT/broker-$i.log" 2>/dev/null >&2
  done
  exit 1
}

step() { echo; echo "=== $* ==="; }

free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'
}

wait_ready() {
  local port="$1" deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    if "$CLI" health --brokers="127.0.0.1:$port" >/dev/null 2>&1; then return 0; fi
    sleep 0.2
  done
  return 1
}

start_broker() {
  local i="$1"
  "$BROKER" \
    --broker.id="$i" \
    --net.listen="127.0.0.1:${PORTS[$i]}" \
    --net.advertised.host=127.0.0.1 \
    --net.advertised.port="${PORTS[$i]}" \
    --broker.data.dir="$ROOT/data-$i" \
    --metrics.port="${METRICS_PORTS[$i]}" \
    --cluster.brokers="$CLUSTER" \
    --storage.flush.interval=5ms \
    --storage.flusher.interval=1ms \
    --log.level=info >> "$ROOT/broker-$i.log" 2>&1 &
  PIDS[$i]=$!
}

record_count() {
  "$CLI" offsets "$1" --brokers="127.0.0.1:${PORTS[0]}" 2>/dev/null \
    | awk 'NR>1 {sum += $3} END {print sum+0}'
}

# --- setup ------------------------------------------------------------------

for binary in "$BROKER" "$CLI"; do
  [[ -x "$binary" ]] || fatal "$binary not found; build first"
done
[[ "$(uname -s)" == "Linux" ]] || echo "warning: not running on Linux; epoll checks will not be meaningful"

rm -rf "$ROOT"
mkdir -p "$ROOT"

for i in 0 1 2; do
  PORTS[$i]=$(free_port)
  METRICS_PORTS[$i]=$(free_port)
done
CLUSTER="0@127.0.0.1:${PORTS[0]},1@127.0.0.1:${PORTS[1]},2@127.0.0.1:${PORTS[2]}"

step "1. multi-broker startup (3 brokers)"
for i in 0 1 2; do start_broker "$i"; done
for i in 0 1 2; do
  wait_ready "${PORTS[$i]}" || fatal "broker $i never became healthy"
done
pass "all three brokers accepted connections"

step "2. epoll backend selected"
if grep -q "poller=epoll" "$ROOT/broker-0.log"; then
  pass "broker reports poller=epoll"
elif [[ "$(uname -s)" != "Linux" ]]; then
  echo "  SKIP: not Linux ($(grep -o 'poller=[a-z]*' "$ROOT/broker-0.log" | head -1))"
else
  fail "expected poller=epoll, got: $(grep -o 'poller=[a-z]*' "$ROOT/broker-0.log" | head -1)"
fi

step "3. topic creation with multiple partitions and replication factor 3"
"$CLI" create-topic linux-smoke --partitions=6 --replication=3 \
  --brokers="127.0.0.1:${PORTS[0]}" || fatal "create-topic failed"
LEADERS=$("$CLI" metadata linux-smoke --brokers="127.0.0.1:${PORTS[1]}" \
  | awk '/partition/ {print $4}' | sort -u | wc -l | tr -d ' ')
[[ "$LEADERS" == "3" ]] \
  && pass "6 partitions spread across all 3 brokers as leaders" \
  || fail "expected leadership on 3 brokers, saw $LEADERS"

PART_COUNT=$("$CLI" metadata linux-smoke --brokers="127.0.0.1:${PORTS[2]}" | grep -c "partition ")
[[ "$PART_COUNT" == "6" ]] \
  && pass "every broker agrees on the partition count (metadata propagated)" \
  || fail "expected 6 partitions from broker 2, saw $PART_COUNT"

step "4. producer traffic with quorum acknowledgements"
"$CLI" produce linux-smoke --value=payload --count=600 --batch=50 --acks=quorum \
  --brokers="127.0.0.1:${PORTS[0]}" || fatal "quorum produce failed"
TOTAL=$(record_count linux-smoke)
[[ "$TOTAL" == "600" ]] \
  && pass "600 records acknowledged with acks=quorum" \
  || fail "expected 600 records, found $TOTAL"

step "5. leader-follower replication converged"
DEADLINE=$((SECONDS + 30))
CONVERGED=0
while (( SECONDS < DEADLINE )); do
  SIZES=""
  for i in 0 1 2; do
    SIZES="$SIZES $(curl -fsS "http://127.0.0.1:${METRICS_PORTS[$i]}/topology" 2>/dev/null \
      | python3 -c "
import json,sys
try: d=json.load(sys.stdin)
except Exception: print(-1); raise SystemExit
t=[x for x in d.get('topics',[]) if x['name']=='linux-smoke']
if not t: print(-1); raise SystemExit
print(sum(p.get('log_end',0) for p in t[0]['partitions'] if p.get('local')))
")"
  done
  # Every broker replicates every partition (RF=3), so each should hold 600.
  if [[ "$SIZES" == " 600 600 600" ]]; then CONVERGED=1; break; fi
  sleep 0.3
done
[[ "$CONVERGED" == "1" ]] \
  && pass "all 3 brokers hold all 600 records in their own logs" \
  || fail "replication did not converge; per-broker record counts:$SIZES"

step "6. consumer traffic and offset commits"
CONSUMED=$("$CLI" consume-group linux-smoke --group=linux-group --max=600 --timeout-ms=15000 \
  --brokers="127.0.0.1:${PORTS[0]}" | grep -c "payload")
(( CONSUMED > 0 )) \
  && pass "consumer group read $CONSUMED records" \
  || fail "consumer group read nothing"

COMMITTED=$("$CLI" consume-group linux-smoke --group=linux-group --max=5 --timeout-ms=3000 \
  --brokers="127.0.0.1:${PORTS[0]}" | grep -c "payload")
(( COMMITTED == 0 )) \
  && pass "committed offsets survived: a second run replayed nothing" \
  || fail "second run replayed $COMMITTED records; offsets were not committed"

step "7. graceful shutdown (SIGTERM) and restart recovery"
kill -TERM "${PIDS[1]}"
for _ in $(seq 1 100); do kill -0 "${PIDS[1]}" 2>/dev/null || break; sleep 0.1; done
if kill -0 "${PIDS[1]}" 2>/dev/null; then
  fail "broker 1 did not exit on SIGTERM"
else
  grep -q "broker stopped" "$ROOT/broker-1.log" \
    && pass "broker 1 shut down cleanly and logged it" \
    || fail "broker 1 exited without logging a clean stop"
fi

start_broker 1
wait_ready "${PORTS[1]}" || fatal "broker 1 did not come back"
pass "broker 1 restarted and is serving again"

step "8. crash recovery (SIGKILL, no clean shutdown)"
BEFORE=$(record_count linux-smoke)
kill -9 "${PIDS[0]}"
wait "${PIDS[0]}" 2>/dev/null
RESTART_START=$(python3 -c 'import time;print(time.time())')
start_broker 0
wait_ready "${PORTS[0]}" || fatal "broker 0 did not recover after SIGKILL"
RESTART_END=$(python3 -c 'import time;print(time.time())')
AFTER=$(record_count linux-smoke)
[[ "$BEFORE" == "$AFTER" ]] \
  && pass "every acks=quorum record survived SIGKILL ($AFTER)" \
  || fail "record count changed across SIGKILL: $BEFORE -> $AFTER"
python3 -c "print(f'  recovery took {($RESTART_END - $RESTART_START):.2f}s')"

step "9. backpressure under overload"
RSS_BEFORE=$(awk '/VmRSS/{print $2}' "/proc/${PIDS[0]}/status" 2>/dev/null || echo 0)
python3 scripts/chaos.py overload --broker="127.0.0.1:${PORTS[0]}" --topic=linux-smoke \
  --connections=16 --batch=200 --record-size=2048 --seconds=4 2>&1 | head -2
sleep 1
RSS_AFTER=$(awk '/VmRSS/{print $2}' "/proc/${PIDS[0]}/status" 2>/dev/null || echo 0)
if [[ "$RSS_BEFORE" != "0" && "$RSS_AFTER" != "0" ]]; then
  GROWTH=$(( RSS_AFTER - RSS_BEFORE ))
  (( GROWTH < 524288 )) \
    && pass "resident set grew ${GROWTH} KiB under overload and stayed bounded" \
    || fail "resident set grew ${GROWTH} KiB, which is not bounded"
else
  echo "  SKIP: /proc not available for RSS measurement"
fi
"$CLI" health --brokers="127.0.0.1:${PORTS[0]}" >/dev/null 2>&1 \
  && pass "broker still healthy after the overload" \
  || fail "broker unhealthy after overload"

step "10. graceful shutdown of the whole cluster"
for i in 0 1 2; do
  [[ -n "${PIDS[$i]:-}" ]] && kill -TERM "${PIDS[$i]}" 2>/dev/null
done
ALL_STOPPED=1
for i in 0 1 2; do
  for _ in $(seq 1 150); do kill -0 "${PIDS[$i]}" 2>/dev/null || break; sleep 0.1; done
  if kill -0 "${PIDS[$i]}" 2>/dev/null; then
    ALL_STOPPED=0
    fail "broker $i did not exit on SIGTERM"
  fi
done
(( ALL_STOPPED == 1 )) && pass "all brokers exited cleanly on SIGTERM"
PIDS=()

echo
if (( FAILURES == 0 )); then
  echo "LINUX SMOKE TEST PASSED"
  exit 0
fi
echo "$FAILURES LINUX SMOKE CHECK(S) FAILED" >&2
exit 1
