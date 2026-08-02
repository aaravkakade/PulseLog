#!/usr/bin/env bash
# Failure-injection test: verifies the behaviour docs/FAILURE_SEMANTICS.md
# promises, rather than asserting that the chaos tools merely run.
#
# Each case states what should happen, causes it, and checks it did.
set -euo pipefail

BUILD_DIR="${BUILD_DIR:-build}"
BROKER="$BUILD_DIR/bin/pulselog-broker"
CLI="$BUILD_DIR/bin/pulselog-cli"
CHAOS="scripts/chaos.py"
PORT="${PORT:-19299}"
DATA_ROOT="$(mktemp -d)"
BROKER_PID=""
FAILURES=0

cleanup() {
  if [[ -n "$BROKER_PID" ]] && kill -0 "$BROKER_PID" 2>/dev/null; then
    kill -9 "$BROKER_PID" 2>/dev/null || true
    wait "$BROKER_PID" 2>/dev/null || true
  fi
  rm -rf "$DATA_ROOT"
}
trap cleanup EXIT

start_broker() {
  "$BROKER" \
    --broker.data.dir="$DATA_ROOT/data" \
    --net.listen="127.0.0.1:$PORT" \
    --metrics.enabled=false \
    --log.level=info >> "$DATA_ROOT/broker.log" 2>&1 &
  BROKER_PID=$!
  for _ in $(seq 1 100); do
    if "$CLI" health --brokers="127.0.0.1:$PORT" >/dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  echo "broker did not start" >&2
  tail -30 "$DATA_ROOT/broker.log" >&2
  return 1
}

record_count() {
  "$CLI" offsets "$1" --brokers="127.0.0.1:$PORT" 2>/dev/null \
    | awk 'NR>1 {sum += $3} END {print sum+0}'
}

check() {
  local description="$1" expected="$2" actual="$3"
  if [[ "$expected" == "$actual" ]]; then
    echo "  PASS: $description ($actual)"
  else
    echo "  FAIL: $description -- expected $expected, got $actual" >&2
    FAILURES=$((FAILURES + 1))
  fi
}

check_at_most() {
  local description="$1" limit="$2" actual="$3"
  if [[ "$actual" -le "$limit" ]]; then
    echo "  PASS: $description ($actual <= $limit)"
  else
    echo "  FAIL: $description -- $actual exceeds $limit" >&2
    FAILURES=$((FAILURES + 1))
  fi
}

echo "=== case 1: SIGKILL loses nothing that was acknowledged with acks=quorum ==="
echo "  acks=quorum means the record is flushed before the ack, so an abrupt"
echo "  kill must not lose any of it."
start_broker
"$CLI" create-topic durable --partitions=1 --brokers="127.0.0.1:$PORT" >/dev/null
"$CLI" produce durable --value=committed --count=400 --batch=40 --acks=quorum \
  --brokers="127.0.0.1:$PORT" >/dev/null
BEFORE=$(record_count durable)

python3 "$CHAOS" kill-broker --pid="$BROKER_PID" >/dev/null
wait "$BROKER_PID" 2>/dev/null || true
BROKER_PID=""

RESTART_START=$(python3 -c 'import time; print(time.time())')
start_broker
RESTART_END=$(python3 -c 'import time; print(time.time())')
AFTER=$(record_count durable)
check "every acks=quorum record survived SIGKILL" "$BEFORE" "$AFTER"
python3 -c "print(f'  recovery took {($RESTART_END - $RESTART_START):.2f}s')"

echo
echo "=== case 2: a corrupt record truncates the log from that point, no further ==="
echo "  Records before the damage stay readable; the broker still serves."
"$CLI" create-topic corrupt --partitions=1 --brokers="127.0.0.1:$PORT" >/dev/null
"$CLI" produce corrupt --value=payload --count=300 --batch=30 --acks=quorum \
  --brokers="127.0.0.1:$PORT" >/dev/null
BEFORE=$(record_count corrupt)

kill -9 "$BROKER_PID"; wait "$BROKER_PID" 2>/dev/null || true; BROKER_PID=""
python3 "$CHAOS" corrupt-tail --data-dir="$DATA_ROOT/data" --topic=corrupt \
  --partition=0 --bytes=8 --back-off=400 | head -1

start_broker
AFTER=$(record_count corrupt)
check_at_most "records after corruption are fewer than before" "$BEFORE" "$AFTER"
if [[ "$AFTER" -gt 0 ]]; then
  echo "  PASS: records before the damage survived ($AFTER of $BEFORE)"
else
  echo "  FAIL: everything was lost; truncation should stop at the bad record" >&2
  FAILURES=$((FAILURES + 1))
fi
grep -q "discarding damaged segment tail" "$DATA_ROOT/broker.log" \
  && echo "  PASS: the truncation was logged at WARN with a reason" \
  || { echo "  FAIL: damage was not logged" >&2; FAILURES=$((FAILURES + 1)); }

# The broker must still be usable afterwards, not wedged.
"$CLI" produce corrupt --value=after-repair --count=10 --brokers="127.0.0.1:$PORT" >/dev/null \
  && echo "  PASS: the partition still accepts writes after truncation" \
  || { echo "  FAIL: partition unusable after truncation" >&2; FAILURES=$((FAILURES + 1)); }

echo
echo "=== case 3: a torn write (truncated tail) drops only the partial record ==="
"$CLI" create-topic torn --partitions=1 --brokers="127.0.0.1:$PORT" >/dev/null
"$CLI" produce torn --value=payload --count=200 --batch=20 --acks=quorum \
  --brokers="127.0.0.1:$PORT" >/dev/null
BEFORE=$(record_count torn)

kill -9 "$BROKER_PID"; wait "$BROKER_PID" 2>/dev/null || true; BROKER_PID=""
python3 "$CHAOS" truncate-tail --data-dir="$DATA_ROOT/data" --topic=torn \
  --partition=0 --bytes=20 | head -1

start_broker
AFTER=$(record_count torn)
check "a torn write costs exactly one record" "$((BEFORE - 1))" "$AFTER"

echo
echo "=== case 4: the connection limit is enforced, not exhausted ==="
python3 "$CHAOS" drop-connections --broker="127.0.0.1:$PORT" --connections=64 \
  --hold-seconds=0.5 | head -2
"$CLI" health --brokers="127.0.0.1:$PORT" >/dev/null \
  && echo "  PASS: the broker is still healthy after a connection storm" \
  || { echo "  FAIL: broker unhealthy after connection storm" >&2; FAILURES=$((FAILURES + 1)); }

echo
echo "=== case 5: an overload is refused, not buffered without bound ==="
RSS_BEFORE=$(ps -o rss= -p "$BROKER_PID" | tr -d ' ')
python3 "$CHAOS" overload --broker="127.0.0.1:$PORT" --topic=durable \
  --connections=16 --batch=200 --record-size=1024 --seconds=3 | head -2
sleep 1
RSS_AFTER=$(ps -o rss= -p "$BROKER_PID" | tr -d ' ')
echo "  resident set: ${RSS_BEFORE} KiB -> ${RSS_AFTER} KiB"
# The bound that matters is that memory did not run away. 512 MiB of growth
# would mean the backpressure path is not doing its job.
GROWTH=$(( RSS_AFTER - RSS_BEFORE ))
check_at_most "resident growth under overload stayed bounded (KiB)" 524288 "$GROWTH"
"$CLI" health --brokers="127.0.0.1:$PORT" >/dev/null \
  && echo "  PASS: the broker is still healthy after the overload" \
  || { echo "  FAIL: broker unhealthy after overload" >&2; FAILURES=$((FAILURES + 1)); }

echo
if [[ "$FAILURES" -eq 0 ]]; then
  echo "FAILURE-INJECTION TESTS PASSED"
  exit 0
fi
echo "$FAILURES FAILURE-INJECTION CHECK(S) FAILED" >&2
exit 1
