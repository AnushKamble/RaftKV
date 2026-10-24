# ADR-001: Project Scope and Consistency Model

## Context
RaftKV is designed as a standalone, internship-quality distributed key–value store implementing the Raft consensus algorithm. Two potential project directions were audited:
1. A minimal consensus core without snapshots or strict verification.
2. An over-ambitious multi-shard database claiming dynamic membership, ReadIndex, and high unverified throughput.

## Decision
We select a focused single-shard, three-node static cluster implementing:
- From-scratch Raft consensus (leader election, log replication, safety invariants).
- A framed binary write-ahead log (WAL) with CRC32 checksums and atomic recovery.
- Snapshotting and `InstallSnapshot` log compaction as an advanced storage milestone.
- Real gRPC networking and separate OS processes for integration tests.
- A deterministic network and time simulator for reproducible fault injection.
- A bounded history-based linearizability checker for Put/Get/Delete/CAS operations.

Explicitly out of scope:
- Dynamic cluster membership / joint consensus.
- Multi-sharding and distributed transactions.
- ReadIndex / lease-based reads.
- Byzantine fault tolerance.

## Consequences
- **Positive**: High code clarity, complete test coverage, deterministic failure reproducibility, and verifiable linearizability.
- **Negative**: Scalability is bounded to a single Raft group; membership changes require cluster restarts.
- **Status**: ACCEPTED.
