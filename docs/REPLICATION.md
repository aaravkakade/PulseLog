# PulseLog Replication

Leader-follower replication with a high-water mark and quorum acknowledgements.
Leadership is **statically assigned**; there is no election. That limitation is
stated first because it shapes everything else.

---

## 1. Model

Every partition has a replica set computed deterministically at topic creation:
replica `r` of partition `p` lives on broker `(p + r + hash(topic)) % n`. The
first replica is the leader. The assignment is persisted and identical on every
broker, because every broker is configured with the same ordered broker list.

The **leader pushes**. One thread per peer broker maintains a single connection
and, for every partition this broker leads with that peer as a replica, streams
records from the follower's reported log end offset.

Why push rather than Kafka-style follower pull: with a small, statically
assigned cluster, push removes a round trip from the acknowledgement path — the
leader already knows what it wants to send. The catch-up case still needs a
pull (`REPLICA_FETCH`), and a follower asks for one when it detects a gap.

```
  leader                                        follower
  ------                                        --------
  append to local log
  wake the replication sender
        |
        +-- REPLICATE(base_offset, prev_offset, epoch, records) -->
        |                                       validate epoch
        |                                       validate base == log_end
        |                                       append with offsets
        |   <-- (log_end_offset, flushed_offset) --
        |
  record follower progress
  recompute the high-water mark
  complete any satisfied quorum waiters
```

---

## 2. The high-water mark

The high-water mark is the highest offset present on **every in-sync replica**,
so it is the minimum log end offset across that set (the leader included).

Consumers reading with the default isolation level never see past it. The
reason is precise: a record above the high-water mark exists only on some
replicas, so if the leader were lost it could vanish. Serving it would let a
consumer observe a record that later ceases to exist.

Two invariants:

* **It never moves backwards.** The recompute takes the maximum of the old and
  new value.
* **It is bounded by the leader's own log.** A follower cannot drag it past
  what the leader has.

When the in-sync set shrinks to just the leader, the mark tracks the leader's
log end offset — which is correct: with no in-sync followers, "on every in-sync
replica" means "on the leader".

---

## 3. Acknowledgement modes

| Mode | Waits for |
|---|---|
| `none` | Nothing beyond the append |
| `leader` | The record to be in the leader's log |
| `quorum` | A **majority of the replica set to have flushed it** |

Quorum counts *flushed* replicas, not replicas that merely received the bytes.
A follower holding data in its page cache does not count. That is what makes
`quorum` mean "survives power loss on a majority" rather than "arrived
somewhere".

A leader that cannot reach a quorum refuses the write with
`NOT_ENOUGH_REPLICAS` rather than acknowledging on fewer replicas than asked
for. Silently degrading would be the worst possible behaviour: the producer
would believe it had a guarantee it did not have.

### Quorum acknowledgement, end to end

```mermaid
sequenceDiagram
    autonumber
    participant P as Producer
    participant L as Leader (broker 0)
    participant LF as Leader flusher
    participant F1 as Follower 1
    participant F2 as Follower 2

    P->>L: produce(acks=quorum)
    L->>L: append; log_end = N+1
    L->>L: register waiter needing<br/>flushed > N on a majority

    Note over L,F2: replication and the local flush race;<br/>neither is ordered before the other

    par leader flushes its own copy
        LF->>LF: fsync
        LF->>L: flushed offset = N+1<br/>(counts as one replica)
    and leader ships to followers
        L->>F1: REPLICATE, prev_offset = N-1
        F1->>F1: append
        F1-->>L: log_end = N+1, flushed = M (stale)
        L->>F2: REPLICATE, prev_offset = N-1
        F2->>F2: append
        F2-->>L: log_end = N+1, flushed = M (stale)
    end

    Note over L,F1: followers are caught up but not yet durable,<br/>so the leader keeps probing

    L->>F1: REPLICATE, record_count = 0 (progress probe)
    F1->>F1: its flusher has since run
    F1-->>L: log_end = N+1, flushed = N+1

    L->>L: leader + follower 1 have flushed past N<br/>= 2 of 3 = quorum
    L-->>P: acknowledged
```

Two properties of this diagram are load-bearing.

**The local flush and replication are unordered.** Either can complete first,
so the waiter is re-evaluated on both events. A missed wake-up here is not a
slow acknowledgement, it is a `TIMEOUT` on a write that is fully durable
everywhere — which is exactly the bug fixed by re-checking waiters on
partitions the flusher finds nothing to flush on.

**The probe in step 11 is not an optimisation.** Without it the sequence stops
at step 10 with every replica durable and the leader unaware, and the producer
waits out its deadline.

### The progress probe

A follower reports its flushed offset in the *response* to a batch — but its own
flusher runs afterwards, so that number is always stale. With no further data to
send, the leader would never learn that the last batch became durable, and every
quorum acknowledgement would wait out its deadline.

So when a follower is caught up but not yet durable, the sender sends a
**zero-record probe**: a `REPLICATE` with `record_count = 0` at the follower's
current offset, which asks it to report where it is and how much is flushed.
This was a real bug; the test that catches it is
`QuorumAckSucceedsWithAllReplicasUp`.

---

## 4. Failure handling

| Situation | Handling |
|---|---|
| Follower falls behind | It leaves the in-sync set, the high-water mark advances without it, and quorum requirements are recomputed against the remaining replicas |
| Follower dies or freezes | The liveness probe (a `HEALTH` round trip every 200 ms on an otherwise idle connection) fails and the follower is evicted. Without this, a dead follower on an *idle* partition would stay in the in-sync set forever and block every quorum write |
| Network interruption | The sender reconnects with exponential backoff (200 ms → 5 s) and resumes from the follower's reported offset |
| Duplicate batch | The follower rejects any batch whose `base_offset` does not equal its log end offset, so a resend is a no-op rather than a duplicate append |
| Gap after reconnect | Same check fires with `OUT_OF_RANGE`; the response carries the follower's real offset and the leader resumes from there |
| Stale leader returns | Every batch carries the leader epoch. A follower that has seen a higher epoch rejects it with `NOT_LEADER`, and the stale leader stops trying |
| Follower behind retention | The leader logs the gap at WARN and restarts it from the current log start. That replica has a genuine hole, and it is reported rather than hidden |
| Corrupt batch in flight | The follower verifies every record's checksum before writing. A batch that fails anywhere is rejected wholesale — a follower that stored corruption would serve it to consumers as valid |

---

## 5. What is not implemented

**No leader election.** If a leader dies, its partitions are unavailable for
writes until that process returns. There is no failover, no election, no
automatic reassignment, and followers do not serve client reads.

The pieces that *would* be needed already exist and are used: leader epochs are
carried on every replication message and enforced, `ClusterMetadata::SetLeader`
bumps the epoch in one place, and the in-sync replica set is maintained. What is
missing is the part that decides.

### Raft design sketch

If this were implemented, the shape would be:

* **One Raft group per partition**, not per cluster. A cluster-wide group makes
  every partition's availability depend on one consensus quorum.
* **The existing log is already the Raft log**: dense offsets, per-record
  checksums, an explicit offset in every record, and truncate-to-offset support
  are exactly what `AppendEntries` consistency checking needs. `prev_offset` in
  `REPLICATE` is already the `prevLogIndex`/`prevLogTerm` check in all but name.
* **The leader epoch is already the term.** It is persisted with the partition
  assignment and rejected when stale.
* **What would be added**: persistent `votedFor` state per partition, a
  `RequestVote` RPC, randomised election timeouts, and a commit index distinct
  from the high-water mark (they would coincide for a log-only state machine).
* **What would have to change**: quorum acknowledgement currently counts
  *flushed* replicas, which is stronger than Raft's commit rule (a majority
  having *appended*). Keeping the stronger rule is a deliberate choice and
  would be kept.

This is a design sketch, not a plan of record. It is written down so the gap is
legible, not to imply the work is nearly done.

---

## 6. Verification

`tests/integration/test_replication.cc` runs a real three-broker cluster and
asserts:

* leadership spreads across all three brokers;
* every follower converges on the leader's log;
* replicas are **byte-identical**, not merely equal in record count — offsets,
  timestamps and checksums all match;
* the high-water mark converges and never exceeds the leader's log;
* a quorum acknowledgement means a majority genuinely persisted the record;
* `acks=leader` writes continue after a follower dies, and the high-water mark
  advances once the dead follower is evicted;
* a restarted follower catches up to byte-identical content;
* replication is idempotent — no duplicates after the leader keeps running;
* quorum **fails** rather than degrading when replicas are down;
* a non-leader refuses a produce with `NOT_LEADER` and names the real leader.
