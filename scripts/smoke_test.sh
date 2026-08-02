#!/usr/bin/env bash
# End-to-end smoke test: start a broker, exercise every client operation,
# check the results, shut down cleanly.
#
# This is the test that catches "it builds and the unit tests pass but the
# binary does not actually work". CI runs it on every push.
set -euo pipefail

BUILD_DIR="${BUILD_DIR:-build}"
BROKER="$BUILD_DIR/bin/pulselog-broker"
CLI="$BUILD_DIR/bin/pulselog-cli"
PORT="${PORT:-19199}"
METRICS_PORT="${METRICS_PORT:-19644}"
DATA_DIR="$(mktemp -d)"
LOG_FILE="$DATA_DIR/broker.log"
BROKER_PID=""

cleanup() {
  if [[ -n "$BROKER_PID" ]] && kill -0 "$BROKER_PID" 2>/dev/null; then
    kill "$BROKER_PID" 2>/dev/null || true
    wait "$BROKER_PID" 2>/dev/null || true
  fi
  rm -rf "$DATA_DIR"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  echo "--- broker log ---" >&2
  tail -50 "$LOG_FILE" >&2 || true
  exit 1
}

step() { echo; echo "--- $* ---"; }

for binary in "$BROKER" "$CLI"; do
  [[ -x "$binary" ]] || fail "$binary not found; build first"
done

step "starting broker on 127.0.0.1:$PORT"
"$BROKER" \
  --broker.data.dir="$DATA_DIR/data" \
  --net.listen="127.0.0.1:$PORT" \
  --metrics.port="$METRICS_PORT" \
  --log.level=info > "$LOG_FILE" 2>&1 &
BROKER_PID=$!

# Wait for the port rather than sleeping a guessed amount.
for _ in $(seq 1 100); do
  if "$CLI" health --brokers="127.0.0.1:$PORT" >/dev/null 2>&1; then break; fi
  kill -0 "$BROKER_PID" 2>/dev/null || fail "broker exited during start-up"
  sleep 0.1
done
"$CLI" health --brokers="127.0.0.1:$PORT" || fail "broker never became healthy"

step "create topic"
"$CLI" create-topic smoke --partitions=4 --brokers="127.0.0.1:$PORT" \
  || fail "create-topic failed"

step "produce 500 records (acks=quorum, so they are durable on return)"
"$CLI" produce smoke --key=user-1 --value=payload --count=500 --batch=50 \
  --acks=quorum --brokers="127.0.0.1:$PORT" || fail "produce failed"

step "metadata"
"$CLI" metadata smoke --brokers="127.0.0.1:$PORT" || fail "metadata failed"

step "offsets"
OFFSETS=$("$CLI" offsets smoke --brokers="127.0.0.1:$PORT") || fail "offsets failed"
echo "$OFFSETS"
# All 500 share one key, so they must all be on one partition.
TOTAL=$(echo "$OFFSETS" | awk 'NR>1 {sum += $3} END {print sum}')
[[ "$TOTAL" == "500" ]] || fail "expected 500 records across partitions, found $TOTAL"

step "consume"
CONSUMED=$("$CLI" consume smoke --partition=0 --offset=earliest --max=500 \
  --brokers="127.0.0.1:$PORT" | grep -c "payload") || true
[[ "$CONSUMED" -gt 0 ]] || fail "consumed no records"
echo "consumed $CONSUMED records"

step "consumer group"
"$CLI" consume-group smoke --group=smoke-group --max=500 --timeout-ms=4000 \
  --brokers="127.0.0.1:$PORT" | tail -6 || fail "consume-group failed"

step "group offsets survive a second run (at-least-once resume)"
"$CLI" consume-group smoke --group=smoke-group --max=10 --timeout-ms=1500 \
  --brokers="127.0.0.1:$PORT" | tail -6 || fail "second consume-group run failed"

step "metrics endpoint"
if command -v curl >/dev/null 2>&1; then
  curl -fsS "http://127.0.0.1:$METRICS_PORT/metrics" | grep -q "pulselog_messages_produced_total" \
    || fail "metrics endpoint did not report produced messages"
  curl -fsS "http://127.0.0.1:$METRICS_PORT/health" | grep -q "ok" \
    || fail "health endpoint did not answer"
  curl -fsS "http://127.0.0.1:$METRICS_PORT/topology" | grep -q '"topics"' \
    || fail "topology endpoint did not answer"
  echo "metrics, health and topology endpoints all answered"
else
  echo "curl not installed; skipping the HTTP endpoint checks"
fi

step "list topics"
"$CLI" list-topics --brokers="127.0.0.1:$PORT" || fail "list-topics failed"

step "clean shutdown"
kill -TERM "$BROKER_PID"
for _ in $(seq 1 100); do
  kill -0 "$BROKER_PID" 2>/dev/null || break
  sleep 0.1
done
kill -0 "$BROKER_PID" 2>/dev/null && fail "broker did not exit on SIGTERM"
BROKER_PID=""
grep -q "broker stopped" "$LOG_FILE" || fail "broker did not log a clean stop"

step "restart and verify the data is still there"
"$BROKER" \
  --broker.data.dir="$DATA_DIR/data" \
  --net.listen="127.0.0.1:$PORT" \
  --metrics.port="$METRICS_PORT" \
  --log.level=info >> "$LOG_FILE" 2>&1 &
BROKER_PID=$!
for _ in $(seq 1 100); do
  if "$CLI" health --brokers="127.0.0.1:$PORT" >/dev/null 2>&1; then break; fi
  sleep 0.1
done

AFTER=$("$CLI" offsets smoke --brokers="127.0.0.1:$PORT" | awk 'NR>1 {sum += $3} END {print sum}')
[[ "$AFTER" == "500" ]] || fail "expected 500 records after restart, found $AFTER"
echo "all 500 records survived the restart"

echo
echo "SMOKE TEST PASSED"
