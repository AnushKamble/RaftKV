# ADR-004: Read Operation Semantics and Linearizability

## Context
Distributed key–value stores must specify consistency guarantees for read operations (`Get`). Approaches include:
1. Follower local reads: Stale reads possible if follower is partitioned or behind.
2. Leader local reads: Stale reads possible if leader was deposed during a network partition and doesn't know it yet (split-brain).
3. Lease reads / ReadIndex: Requires bounded clock drift or heartbeat roundtrips; complex corner cases during partitions.
4. Logged reads: Submit `Get` operations through the Raft log as committed log entries.

## Decision
We mandate **Logged Reads** for `Get` operations in RaftKV:
1. A client `Get(key)` request proposes a Raft command through the consensus log.
2. The entry is replicated to a quorum and committed by the leader.
3. The state machine applies the entry in strict log order and evaluates `Get(key)`, returning the value and commit index.
4. If the proposing node loses quorum, the entry does not commit and the read does not falsely succeed.

## Consequences
- **Positive**: Strict linearizability is guaranteed by construction without relying on wall-clock synchrony or lease assumptions. Simplifies the sequential specification for the history-based linearizability checker.
- **Negative**: Read latency and throughput are bounded by consensus replication and WAL fsync. This is a deliberate design choice prioritizing correctness and verifiability over raw speed.
- **Status**: ACCEPTED.
