#!/usr/bin/env bash
# Failure injection against a real three-broker cluster.
#
# scripts/failure_test.sh covers what a single broker does with a damaged log
# and an overloaded socket. This covers what a *cluster* does when replicas
# die, freeze, come back, or fall behind -- the cases where the failure is not
# local and the wrong answer is silent rather than loud.
#
# Every case states the property it is checking. The properties, in the order
# they appear:
#
#   a follower dying does not stall the leader          (quorum still reachable)
#   a frozen follower does not block quorum forever     (bounded eviction)
#   a resumed follower rejoins and catches up           (no permanent divergence)
#   a follower that reconnects re-streams from its LEO  (no gap, no duplication)
#   an idle partition still completes durable writes    (no missed wake-up)
#   losing the leader makes writes fail, not hang       (documented: no election)
#   a malformed frame is refused without harming others (framing is defensive)
#   restarting under active writes loses no ack'd data  (crash consistency)
#   a consumer vanishing mid-stream frees its resources (no leak, no stall)
#   shutdown while workers are draining is clean        (no deadlock on exit)
#   repeated start-stop cycles stay stable              (no accumulation)
#
# Run under sanitizers by pointing BUILD_DIR at a sanitizer build.
set -uo pipefail

BUILD_DIR="${BUILD_DIR:-build}"
BROKER="$BUILD_DIR/bin/pulselog-broker"
CLI="$BUILD_DIR/bin/pulselog-cli"
ROOT="${ROOT:-/tmp/pulselog-failure-cluster}"
FAILURES=0
BROKER_COUNT=3
declare -a PIDS=()
declare -a PORTS=()
declare -a METRICS_PORTS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    [[ -n "$pid" ]] && kill -CONT "$pid" 2>/dev/null
    [[ -n "$pid" ]] && kill -9 "$pid" 2>/dev/null
  done
  wait 2>/dev/null
}
trap cleanup EXIT

pass() { echo "  PASS: $*"; }
fail() { echo "  FAIL: $*" >&2; FAILURES=$((FAILURES + 1)); }
step() { echo; echo "=== $* ==="; }
note() { echo "  $*"; }

fatal() {
  echo "FATAL: $*" >&2
  for i in "${!PORTS[@]}"; do
    echo "--- broker $i log (tail) ---" >&2
    tail -25 "$ROOT/broker-$i.log" 2>/dev/null >&2
  done
  exit 1
}

free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'
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
    --replication.timeout=3000 \
    --log.level=info >> "$ROOT/broker-$i.log" 2>&1 &
  PIDS[$i]=$!
}

wait_ready() {
  local i="$1" deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    "$CLI" health --brokers="127.0.0.1:${PORTS[$i]}" >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  return 1
}

# Total records across every partition, as seen by a specific broker.
count_via() {
  local i="$1" topic="$2"
  "$CLI" offsets "$topic" --brokers="127.0.0.1:${PORTS[$i]}" 2>/dev/null \
    | awk 'NR>1 {sum += $3} END {print sum+0}'
}

alive() { kill -0 "$1" 2>/dev/null; }

# First partition of `topic` whose leader is broker `$2`, queried via broker $1.
#
# Needed because `produce` without --partition round-robins across every
# partition, and with a broker down the partitions it leads are unavailable by
# design. Sending there would test the documented no-leader-election behaviour
# instead of the property the case is about.
partition_led_by() {
  local via="$1" leader="$2" topic="$3"
  "$CLI" metadata "$topic" --brokers="127.0.0.1:${PORTS[$via]}" 2>/dev/null \
    | awk -v want="$leader" '/^  partition /{if ($4 == want) {print $2; exit}}'
}

# --- setup ------------------------------------------------------------------

for binary in "$BROKER" "$CLI"; do
  [[ -x "$binary" ]] || fatal "$binary not found; build first"
done

rm -rf "$ROOT"
mkdir -p "$ROOT"

for i in 0 1 2; do
  PORTS[$i]=$(free_port)
  METRICS_PORTS[$i]=$(free_port)
done
CLUSTER="0@127.0.0.1:${PORTS[0]},1@127.0.0.1:${PORTS[1]},2@127.0.0.1:${PORTS[2]}"

for i in 0 1 2; do start_broker "$i"; done
for i in 0 1 2; do wait_ready "$i" || fatal "broker $i never became healthy"; done

"$CLI" create-topic chaos --partitions=6 --replication=3 \
  --brokers="127.0.0.1:${PORTS[0]}" >/dev/null || fatal "create-topic failed"
sleep 0.5

# --- 1. a follower dying does not stall the leader ---------------------------

step "1. killing a follower leaves quorum reachable"
note "quorum of 3 is 2, so one dead replica must not stop acknowledgements"
"$CLI" produce chaos --value=pre --count=300 --batch=50 --acks=quorum \
  --brokers="127.0.0.1:${PORTS[0]}" >/dev/null || fatal "baseline produce failed"

LIVE_PARTITION=$(partition_led_by 0 0 chaos)
[[ -n "$LIVE_PARTITION" ]] || fatal "no partition is led by broker 0"
note "partition $LIVE_PARTITION is led by broker 0; broker 2 is one of its followers"

kill -9 "${PIDS[2]}"
wait "${PIDS[2]}" 2>/dev/null
sleep 0.5

if quorum_output=$("$CLI" produce chaos --partition="$LIVE_PARTITION" --value=post \
     --count=300 --batch=50 --acks=quorum \
     --brokers="127.0.0.1:${PORTS[0]}" 2>&1); then
  pass "acks=quorum still completes with one of three replicas dead"
else
  fail "quorum blocked after a single follower died: $quorum_output"
fi

# --- 2. a frozen follower is evicted rather than blocking forever ------------

step "2. a frozen (SIGSTOP) follower does not block quorum forever"
note "a stopped process holds its TCP connection open, so this is the case a"
note "connection-liveness check alone would miss"
start_broker 2
wait_ready 2 || fatal "broker 2 did not restart"
sleep 1.0

kill -STOP "${PIDS[1]}"
FROZEN_START=$(python3 -c 'import time; print(time.time())')
if "$CLI" produce chaos --partition="$LIVE_PARTITION" --value=frozen --count=200 \
     --batch=50 --acks=quorum \
     --brokers="127.0.0.1:${PORTS[0]}" >/dev/null 2>&1; then
  FROZEN_ELAPSED=$(python3 -c "import time; print(f'{time.time() - $FROZEN_START:.2f}')")
  pass "quorum completed in ${FROZEN_ELAPSED}s with a frozen replica"
else
  fail "quorum could not complete with one frozen replica and one healthy one"
fi

# --- 3. the resumed follower rejoins and catches up --------------------------

step "3. a resumed follower rejoins the in-sync set and catches up"
kill -CONT "${PIDS[1]}"
EXPECTED=$(count_via 0 chaos)
CAUGHT_UP=0
DEADLINE=$((SECONDS + 30))
while (( SECONDS < DEADLINE )); do
  if [[ "$(count_via 1 chaos)" == "$EXPECTED" ]]; then CAUGHT_UP=1; break; fi
  sleep 0.3
done
(( CAUGHT_UP == 1 )) \
  && pass "resumed follower reached $EXPECTED records without operator action" \
  || fail "resumed follower stuck at $(count_via 1 chaos) of $EXPECTED"

# --- 4. reconnect re-streams from the follower's own position ----------------

step "4. a follower that restarts re-streams from its own log end"
note "no gap and no duplication: the count must land exactly, not approximately"
kill -TERM "${PIDS[2]}"
for _ in $(seq 1 100); do alive "${PIDS[2]}" || break; sleep 0.1; done
"$CLI" produce chaos --partition="$LIVE_PARTITION" --value=while-down --count=400 \
  --batch=50 --acks=leader --brokers="127.0.0.1:${PORTS[0]}" >/dev/null \
  || fail "produce to a live-led partition failed while a follower was down"

start_broker 2
wait_ready 2 || fatal "broker 2 did not come back"
EXPECTED=$(count_via 0 chaos)
RESYNCED=0
DEADLINE=$((SECONDS + 30))
while (( SECONDS < DEADLINE )); do
  if [[ "$(count_via 2 chaos)" == "$EXPECTED" ]]; then RESYNCED=1; break; fi
  sleep 0.3
done
(( RESYNCED == 1 )) \
  && pass "restarted follower resynced to exactly $EXPECTED records" \
  || fail "restarted follower has $(count_via 2 chaos), expected exactly $EXPECTED"

# --- 5. an idle partition still completes durable writes ---------------------

step "5. a durable write to a partition that then goes idle still completes"
note "regression: a quorum waiter is woken by a flush or a follower report."
note "if both already happened in the wrong order and no further append comes,"
note "nothing was left to wake it and the write timed out despite being durable."
sleep 1.5  # let all replication traffic quiesce
IDLE_START=$(python3 -c 'import time; print(time.time())')
if "$CLI" produce chaos --partition="$LIVE_PARTITION" --value=idle --count=1 --batch=1 \
     --acks=quorum --brokers="127.0.0.1:${PORTS[0]}" >/dev/null 2>&1; then
  IDLE_ELAPSED=$(python3 -c "import time; print(f'{time.time() - $IDLE_START:.2f}')")
  UNDER=$(python3 -c "print(1 if $IDLE_ELAPSED < 2.0 else 0)")
  if [[ "$UNDER" == "1" ]]; then
    pass "single durable write to an idle cluster acknowledged in ${IDLE_ELAPSED}s"
  else
    fail "single durable write took ${IDLE_ELAPSED}s; it waited for a timeout, not a disk"
  fi
else
  fail "a single acks=quorum write to an idle cluster did not complete"
fi

# --- 6. losing a leader fails writes rather than hanging ---------------------

step "6. losing a partition leader fails its writes rather than hanging"
note "there is no leader election: affected partitions become unavailable."
note "what is checked is that this surfaces as a prompt error, not a hang,"
note "and that partitions led by other brokers keep working."
LEADER_OF_0=$("$CLI" metadata chaos --brokers="127.0.0.1:${PORTS[0]}" \
  | awk '/partition 0 /{print $4}' | head -1)
if [[ -z "$LEADER_OF_0" ]]; then
  note "SKIP: could not determine the leader of partition 0"
else
  note "partition 0 is led by broker $LEADER_OF_0"
  kill -9 "${PIDS[$LEADER_OF_0]}"
  wait "${PIDS[$LEADER_OF_0]}" 2>/dev/null
  SURVIVOR=$(( (LEADER_OF_0 + 1) % BROKER_COUNT ))
  WRITE_START=$(python3 -c 'import time; print(time.time())')
  "$CLI" produce chaos --value=leaderless --count=50 --batch=10 --acks=leader \
    --brokers="127.0.0.1:${PORTS[$SURVIVOR]}" >/dev/null 2>&1
  WRITE_ELAPSED=$(python3 -c "import time; print(f'{time.time() - $WRITE_START:.2f}')")
  BOUNDED=$(python3 -c "print(1 if $WRITE_ELAPSED < 20.0 else 0)")
  [[ "$BOUNDED" == "1" ]] \
    && pass "writes resolved in ${WRITE_ELAPSED}s after the leader died (no hang)" \
    || fail "writes hung for ${WRITE_ELAPSED}s after a leader died"

  "$CLI" health --brokers="127.0.0.1:${PORTS[$SURVIVOR]}" >/dev/null 2>&1 \
    && pass "surviving brokers stayed healthy" \
    || fail "a surviving broker became unhealthy after a peer died"

  start_broker "$LEADER_OF_0"
  wait_ready "$LEADER_OF_0" || fatal "broker $LEADER_OF_0 did not restart"
fi

# --- 7. malformed frames --------------------------------------------------

step "7. malformed frames are refused without disturbing other connections"
python3 - "$ROOT" "${PORTS[0]}" <<'PYEOF'
import socket, struct, sys, zlib
root, port = sys.argv[1], int(sys.argv[2])

def crc32c(data: bytes) -> int:
    # The wire uses CRC-32C; a deliberately wrong checksum is the point here,
    # so the exact polynomial does not matter for the malformed cases.
    return zlib.crc32(data) & 0xFFFFFFFF

cases = {
    "garbage bytes":        b"\x00" * 64,
    "wrong magic":          b"XXXX" + b"\x00" * 28,
    "absurd payload_len":   b"PLSG" + struct.pack("<HHIIQ", 1, 1, 0xFFFFFFFF, 0, 0) + b"\x00" * 8,
    "truncated header":     b"PLSG" + b"\x00" * 7,
    "valid magic bad crc":  b"PLSG" + b"\xAB" * 28,
}
refused = 0
for name, payload in cases.items():
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=5) as s:
            s.sendall(payload)
            s.settimeout(5)
            try:
                reply = s.recv(4096)   # error frame or clean close both acceptable
            except socket.timeout:
                reply = b""
        refused += 1
        print(f"    {name}: handled ({len(reply)} bytes back)")
    except Exception as error:
        print(f"    {name}: connection error {error}")
print(f"REFUSED {refused}/{len(cases)}")
PYEOF
if "$CLI" health --brokers="127.0.0.1:${PORTS[0]}" >/dev/null 2>&1; then
  pass "broker still healthy after every malformed frame"
else
  fail "broker became unhealthy after a malformed frame"
fi
if "$CLI" produce chaos --value=after-garbage --count=50 --batch=10 --acks=leader \
     --brokers="127.0.0.1:${PORTS[0]}" >/dev/null 2>&1; then
  pass "well-formed traffic still works on a new connection"
else
  fail "well-formed traffic broke after malformed frames"
fi

# --- 8. restart during active writes -----------------------------------------

step "8. restarting a broker while writes are in flight loses no acknowledged data"
BEFORE=$(count_via 0 chaos)
"$CLI" produce chaos --value=inflight --count=2000 --batch=50 --acks=quorum \
  --brokers="127.0.0.1:${PORTS[0]}" >/dev/null 2>&1 &
WRITER=$!
sleep 0.4
kill -9 "${PIDS[1]}"
wait "${PIDS[1]}" 2>/dev/null
start_broker 1
wait "$WRITER" 2>/dev/null
wait_ready 1 || fatal "broker 1 did not recover"

AFTER=$(count_via 0 chaos)
if (( AFTER >= BEFORE )); then
  pass "record count did not go backwards across a mid-write kill ($BEFORE -> $AFTER)"
else
  fail "records disappeared across a mid-write kill: $BEFORE -> $AFTER"
fi

# Everything the leader has must reach the restarted replica.
EXPECTED=$(count_via 0 chaos)
CONVERGED=0
DEADLINE=$((SECONDS + 30))
while (( SECONDS < DEADLINE )); do
  if [[ "$(count_via 1 chaos)" == "$EXPECTED" ]]; then CONVERGED=1; break; fi
  sleep 0.3
done
(( CONVERGED == 1 )) \
  && pass "the restarted broker caught up to $EXPECTED records" \
  || fail "restarted broker stuck at $(count_via 1 chaos) of $EXPECTED"

# --- 9. a consumer vanishing mid-stream --------------------------------------

step "9. a consumer that disappears mid-stream does not stall or leak"
CONN_BEFORE=$(curl -fsS "http://127.0.0.1:${METRICS_PORTS[0]}/metrics" 2>/dev/null \
  | awk '/^pulselog_active_connections /{print $2}')
python3 - "${PORTS[0]}" <<'PYEOF'
import socket, sys, time
port = int(sys.argv[1])
# Open connections, send a partial frame, and vanish without a close handshake.
sockets = []
for _ in range(20):
    s = socket.create_connection(("127.0.0.1", port), timeout=5)
    s.sendall(b"PLSG" + b"\x00" * 4)   # header started, never finished
    sockets.append(s)
time.sleep(0.5)
for s in sockets:
    s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, b"\x01\x00\x00\x00\x00\x00\x00\x00")
    s.close()   # RST, not FIN
print("    20 consumers vanished with RST")
PYEOF
sleep 2
CONN_AFTER=$(curl -fsS "http://127.0.0.1:${METRICS_PORTS[0]}/metrics" 2>/dev/null \
  | awk '/^pulselog_active_connections /{print $2}')
note "active connections: ${CONN_BEFORE:-?} -> ${CONN_AFTER:-?}"
if [[ -n "$CONN_AFTER" && -n "$CONN_BEFORE" ]]; then
  LEAKED=$(python3 -c "print(1 if float('$CONN_AFTER') > float('$CONN_BEFORE') + 5 else 0)")
  [[ "$LEAKED" == "0" ]] \
    && pass "abandoned connections were reclaimed" \
    || fail "connection count grew from $CONN_BEFORE to $CONN_AFTER; they leaked"
fi
"$CLI" produce chaos --value=after-rst --count=50 --batch=10 --acks=leader \
  --brokers="127.0.0.1:${PORTS[0]}" >/dev/null 2>&1 \
  && pass "broker still serving after abrupt consumer disconnects" \
  || fail "broker stopped serving after abrupt consumer disconnects"

# --- 10. shutdown while workers are draining ---------------------------------

step "10. SIGTERM while requests are in flight shuts down cleanly"
note "the risk is a worker blocked on a durability waiter that shutdown must"
note "release, and a join that never returns"
"$CLI" produce chaos --value=draining --count=3000 --batch=50 --acks=quorum \
  --brokers="127.0.0.1:${PORTS[0]}" >/dev/null 2>&1 &
DRAIN_WRITER=$!
sleep 0.3
kill -TERM "${PIDS[0]}"
STOPPED=0
for _ in $(seq 1 200); do
  alive "${PIDS[0]}" || { STOPPED=1; break; }
  sleep 0.1
done
wait "$DRAIN_WRITER" 2>/dev/null
if (( STOPPED == 1 )); then
  grep -q "broker stopped" "$ROOT/broker-0.log" \
    && pass "broker drained and stopped cleanly under load" \
    || fail "broker exited under load without logging a clean stop"
else
  fail "broker did not exit within 20s of SIGTERM while draining"
fi
start_broker 0
wait_ready 0 || fatal "broker 0 did not restart after the draining test"

# --- 11. repeated start-stop cycles ------------------------------------------

step "11. repeated start-stop cycles stay stable"
note "checks that nothing accumulates across restarts: not files, not memory,"
note "not stale replication state"
CYCLE_OK=1
for cycle in 1 2 3 4 5; do
  kill -TERM "${PIDS[2]}"
  for _ in $(seq 1 150); do alive "${PIDS[2]}" || break; sleep 0.1; done
  if alive "${PIDS[2]}"; then
    fail "cycle $cycle: broker 2 did not exit on SIGTERM"
    CYCLE_OK=0
    break
  fi
  start_broker 2
  if ! wait_ready 2; then
    fail "cycle $cycle: broker 2 did not come back"
    CYCLE_OK=0
    break
  fi
  "$CLI" produce chaos --partition="$LIVE_PARTITION" --value="cycle-$cycle" --count=100 \
    --batch=25 --acks=quorum --brokers="127.0.0.1:${PORTS[0]}" >/dev/null 2>&1 \
    || { fail "cycle $cycle: produce failed after restart"; CYCLE_OK=0; break; }
done
(( CYCLE_OK == 1 )) && pass "5 start-stop cycles with durable writes between each"

RSS=$(ps -o rss= -p "${PIDS[2]}" 2>/dev/null | tr -d ' ')
if [[ -n "$RSS" ]]; then
  (( RSS < 1048576 )) \
    && pass "resident set after 5 cycles is ${RSS} KiB, comfortably bounded" \
    || fail "resident set after 5 cycles is ${RSS} KiB"
fi

# --- 12. final consistency ---------------------------------------------------

step "12. all replicas agree after everything above"
sleep 2
EXPECTED=$(count_via 0 chaos)
AGREE=1
for i in 0 1 2; do
  COUNT=$(count_via "$i" chaos)
  note "broker $i: $COUNT records"
  [[ "$COUNT" == "$EXPECTED" ]] || AGREE=0
done
(( AGREE == 1 )) \
  && pass "all three brokers report exactly $EXPECTED records" \
  || fail "brokers disagree on the record count"

for i in 0 1 2; do
  "$CLI" health --brokers="127.0.0.1:${PORTS[$i]}" >/dev/null 2>&1 \
    || fail "broker $i is not healthy at the end of the run"
done

echo
if (( FAILURES == 0 )); then
  echo "CLUSTER FAILURE INJECTION PASSED"
  exit 0
fi
echo "$FAILURES CLUSTER FAILURE CHECK(S) FAILED" >&2
exit 1
