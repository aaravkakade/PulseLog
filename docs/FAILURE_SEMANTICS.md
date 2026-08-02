# PulseLog Failure Semantics

What survives what. This document is the contract; `tests/integration/` and
`scripts/failure_test.sh` are the evidence. Where a guarantee does not hold,
that is stated rather than omitted.

---

## 1. Durability ladder

A produce request picks one of three acknowledgement modes. They are not
"weak, medium, strong" — they promise specific, different things.

| Mode | The acknowledgement means | Survives broker process crash | Survives machine power loss | Survives loss of the leader machine |
|---|---|---|---|---|
| `none` | The broker parsed and appended it | No | No | No |
| `leader` | It is in the leader's log, subject to the flush policy | Yes | Only if the flusher had run | No |
| `quorum` | A majority of the replica set has **flushed** it | Yes | Yes | Yes, if a surviving replica is in the majority |

Two details that are easy to get wrong:

* **`leader` does not mean fsynced.** With the default flush policy the record
  is in the page cache; a process crash cannot lose it, but pulling the power
  can. Set `storage.flush.sync.on.append=true` to make `leader` mean stable
  media at a large throughput cost (§ PERFORMANCE_RESULTS.md §5).
* **`quorum` counts flushed replicas, not replicas that received it.** A
  follower that has the bytes in memory but has not flushed does not count
  toward the quorum. This is why quorum acknowledgements wait for the flusher.

`quorum` with replication factor 1 degenerates to "flushed on the leader",
which is still a meaningful guarantee (it survives power loss) and is what the
single-broker durability test asserts.

---

## 2. Ordering

**Guaranteed:** records appended to one partition are read back in the order
they were appended, for all time. Offsets are dense — every appended record
consumes exactly one, with no gaps.

**Guaranteed:** records sent by one `Producer` instance to one partition are
appended in the order that producer sent them. One producer per thread, and the
SDK's synchronous design, is what makes this true without a sequence-number
protocol.

**Not guaranteed:** ordering *between* producers. Two producers writing
concurrently to the same partition interleave arbitrarily. Both producers'
records are all present, each producer's own sequence is intact, and the
interleaving is whatever the network and the worker queue produced.
`ConcurrentProducersToOnePartitionKeepEveryRecord` asserts exactly this.

**Not guaranteed:** ordering across partitions. A key routes to a fixed
partition, so records sharing a key are ordered; records with different keys
have no relative order.

---

## 3. Delivery semantics

**At-least-once.** A consumer fetches records, processes them, then commits the
offset it wants to resume from. A crash between processing and committing
replays the uncommitted records.

There is no transactional coupling between processing and the commit, so
**exactly-once is not provided** and is not claimed anywhere in this repository.

### Consumer-group fencing, precisely

Within one generation the coordinator assigns each partition to exactly one
member. A member whose generation is stale has its commits rejected with
`ILLEGAL_GENERATION`, which fences a consumer that was partitioned away and
kept working.

What this does **not** do: the `FETCH` path carries no group identity. A zombie
consumer can still *read* a partition it no longer owns. Its commits cannot
land, so it cannot move the group's position or corrupt anyone else's progress
— but it will duplicate work until it notices. This is the same fencing model
Kafka provides without transactions.

### Duplicates you should expect

| Situation | Result |
|---|---|
| Producer retries after a timeout | The record may be appended twice, at two offsets |
| Consumer crashes before committing | Records since the last commit are replayed |
| Rebalance mid-batch | The new owner resumes from the last committed offset |

There are no idempotent producer IDs, so a retried produce is a genuine
duplicate. A consumer that needs deduplication must do it on a key it controls.

---

## 4. Crash and restart

### Broker process crash (SIGKILL, panic, OOM kill)

On restart, each partition scans forward from its last index entry and stops at
the first record that is incomplete, fails its checksum, or has an offset that
does not continue the sequence. Everything from there is truncated.

> **A record is either wholly present and checksum-valid, or it is gone.** There
> is no state in which a partially written or corrupt record is served.

Verified: after SIGKILL with no clean shutdown, all 400 records acknowledged
with `acks=quorum` were present, and recovery took 0.12 s.

### Torn write

A write interrupted mid-record leaves a partial record at the tail. Recovery
reports it as `OUT_OF_RANGE` (incomplete, expected after a crash) rather than
`CORRUPTION`, drops it, and keeps everything before it. Verified: chopping 20
bytes off the tail cost **exactly one record**.

### Bit rot / corruption inside the log

Recovery reports `CORRUPTION`, logs the reason and the absolute file offset at
WARN, truncates from that record, and continues. The partition remains
writable. Verified: flipping 8 bytes 400 bytes before the end of a 300-record
log kept 289 records, and the partition accepted new writes immediately after.

Corruption found in a *sealed* (non-final) segment is logged at ERROR, because
everything after it becomes unreachable — that is a real data-loss event, not
routine cleanup.

### Clean shutdown (SIGTERM/SIGINT)

Stops accepting, drains in-flight requests, fails any outstanding
acknowledgement waiters with `UNAVAILABLE` rather than dropping them silently,
flushes every partition, persists metadata, and joins every thread. No thread is
detached, so there is no window in which a thread touches freed memory.

---

## 5. Replication failures

| Failure | Behaviour |
|---|---|
| Follower process dies | The leader's liveness probe fails within ~200 ms, the follower leaves the in-sync set, and the high-water mark advances without it. `acks=leader` is unaffected; `acks=quorum` continues if a majority remains |
| Follower is frozen (SIGSTOP) | Same as death: the probe times out. The connection stays open, which is why the probe exists rather than relying on connection state |
| Network interruption | The sender reconnects with exponential backoff (200 ms → 5 s) and resumes from the follower's reported log end offset |
| Follower falls behind retention | The leader logs the gap at WARN and restarts it from the current log start. That replica has a real hole; this is reported, not hidden |
| Duplicate replication batch | The follower rejects any batch whose base offset does not equal its log end offset, so a resend is a no-op rather than a duplicate append |
| Stale leader returns | Every batch carries the leader epoch; a follower that has seen a higher epoch rejects it with `NOT_LEADER` |
| Quorum impossible | The leader refuses the write with `NOT_ENOUGH_REPLICAS` rather than acknowledging on fewer replicas than requested |

### The big one: no leader election

**Leadership is statically assigned.** If a leader dies, its partitions are
unavailable for writes until that process returns. There is no failover, no
election, and no automatic reassignment.

Consequences:

* Writes to those partitions fail with `NOT_LEADER` or time out.
* Reads from a surviving follower are *not* served — followers reject client
  fetches for partitions they do not lead.
* Data acknowledged with `acks=quorum` is safe on the surviving replicas and
  becomes available again when the leader returns.

This is the single largest gap between PulseLog and a production broker. The
design that would close it is in [REPLICATION.md](REPLICATION.md); it is not
implemented, and nothing in this repository behaves as if it were.

---

## 6. Overload and resource exhaustion

| Condition | Behaviour |
|---|---|
| Worker queue full | Immediate `BACKPRESSURE` response, counted in `pulselog_backpressure_rejections_total`. Retryable |
| Connection output above the high-water mark | The connection stops being read from, so TCP's receive window closes and the pressure reaches the client |
| Connection output above the hard ceiling | The connection is closed with `RESOURCE_EXHAUSTED` and the reason is logged |
| Connection count at the limit | New connections are accepted by the kernel and closed immediately, counted in `RejectedTotal` |
| io loop task queue full | The connection is refused rather than queued |
| Free disk below `storage.min.free.disk.bytes` | Appends refused with `RESOURCE_EXHAUSTED`, logged at ERROR once per transition. Reads keep working |
| `ENOSPC` mid-write | The append fails; the partial write is truncated by the next recovery |
| Frame larger than `net.max.frame.bytes` | Rejected on the header alone, before any buffer is sized |

Every one of these is a bound, not a heuristic. Verified: a 3-second flood over
16 connections grew the broker's resident set by 41 MiB and stopped, and the
broker stayed healthy throughout.

---

## 7. Protocol-level failures

| Condition | Result |
|---|---|
| Bad magic, bad header CRC, unknown opcode, non-zero reserved bits | Connection closed. A stream protocol has no safe resynchronisation point |
| Payload CRC mismatch | Connection closed |
| Payload decodes badly | `PROTOCOL_ERROR` response, **connection stays open** — the framing layer already contained it |
| Unsupported protocol version | `UNSUPPORTED_VERSION`, connection closed |
| Request with trailing bytes | Rejected. Silently ignoring a field a newer client sent could mean ignoring a directive that changes semantics |

---

## 8. Known limitations

Collected so none of them has to be inferred:

| Area | Limitation |
|---|---|
| Leader election | None. Static assignment; a dead leader means unavailable partitions |
| Reads from followers | Not supported; only the leader serves a partition |
| Exactly-once | Not provided. No idempotent producer IDs, no transactions |
| Consumer rebalancing | Stop-the-world, not incremental or cooperative |
| Offset log compaction | None. The offset journal is replayed in full at start-up, so start-up cost grows with total commits ever made |
| Log compaction | None. Tombstones are stored and preserved but nothing consumes them |
| Tiered storage | None |
| Security | No TLS, no authentication, no authorisation. Anyone who can reach the port can do anything, including impersonate a follower |
| Multi-tenancy | No quotas, no per-client limits beyond the global connection cap |
| Partition reassignment | Partition count is fixed at creation; adding partitions would reroute existing keys and is refused |
| `io_uring` | Not implemented |
| Segment size | Capped at 4 GiB by the 32-bit index fields |
| Single batch vs segment | A batch is never split across segments, so a segment can exceed `segment_bytes` by up to one batch |
