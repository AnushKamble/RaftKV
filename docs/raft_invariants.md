# RaftKV — Protocol Invariants & Safety Specification

This document codifies the core Raft invariants as defined in Diego Ongaro and John Ousterhout's Raft paper (Extended Version) and maps each invariant directly to implementing components and verification tests in RaftKV.

---

## 1. Core Raft Invariants

### INV-1: Election Safety
**Definition**: At most one leader can be elected in any given term.
**Mechanism**:
- A candidate must win votes from a strict majority ($\lfloor N/2 \rfloor + 1$) of nodes for that term.
- Each node casts at most one vote per term (`votedFor`).
- A node persists `currentTerm` and `votedFor` to durable storage *before* transmitting a `RequestVoteResponse(voteGranted=true)`.
- Reconstructed state on restart preserves `votedFor`, preventing duplicate votes upon reboot.
**Traceability**:
- Implemented in: `internal/raft/election.go`
- Tested by: `TestElectionSafety_OneLeaderPerTerm`, `TestVotePersistence_NoDoubleVoteAcrossRestart`

### INV-2: Leader Append-Only
**Definition**: A leader never overwrites or truncates its own log entries; it only appends new entries.
**Mechanism**:
- Leader event loop appends incoming client commands to the end of its log.
- Local log entries on the leader are immutable once appended.
**Traceability**:
- Implemented in: `internal/raft/raft.go`
- Tested by: `TestLeaderAppendOnly`

### INV-3: Log Matching Property
**Definition**: If two logs contain an entry with the same index and term, then:
1. The logs are identical in all entries through the given index.
2. The `prevLogIndex` and `prevLogTerm` consistency check ensures this inductively.
**Mechanism**:
- An `AppendEntries` RPC includes `prevLogIndex` and `prevLogTerm`.
- If the receiver does not find an entry at `prevLogIndex` matching `prevLogTerm`, it rejects the request (`Success = false`).
- On rejection, the leader decrements `nextIndex` for that follower and retries until a matching prefix is reached.
**Traceability**:
- Implemented in: `internal/raft/replication.go`
- Tested by: `TestLogMatching_BacktrackingRepair`, `TestLogMatching_PrevLogMismatchRejection`

### INV-4: Leader Completeness
**Definition**: If a log entry is committed in a given term, that entry will be present in the logs of the leaders for all higher-numbered terms.
**Mechanism**:
- RequestVote enforces the candidate log freshness rule:
  - If candidate's last log term $\neq$ receiver's last log term: candidate with higher term is fresher.
  - If terms are equal: candidate with longer log (greater `lastLogIndex`) is at least as fresh.
- A voter denies its vote if the candidate's log is less up-to-date than its own.
- Since a committed entry must reside on a majority of nodes, any newly elected leader must contain that entry.
**Traceability**:
- Implemented in: `internal/raft/election.go`
- Tested by: `TestLeaderCompleteness_StaleLogCandidateRejected`

### INV-5: State Machine Safety
**Definition**: If a server has applied a log entry at a given index to its state machine, no other server will ever apply a different log entry for the same index.
**Mechanism**:
- Followers only commit entries that match the leader's replicated log.
- Leaders only advance `commitIndex` once an entry has been acknowledged by a majority.
- **Current-Term Restriction**: A leader cannot commit a log entry from a *previous* term by counting replicas; it must commit an entry from its *current* term, which indirectly commits earlier entries (Raft Paper §5.4.2, Figure 8).
**Traceability**:
- Implemented in: `internal/raft/replication.go`
- Tested by: `TestCommitCurrentTermOnly_Figure8Scenario`, `TestStateMachineSafety_MonotonicApply`

### INV-6: Suffix Truncation and Committed Prefix Inviolability
**Definition**: A follower may truncate conflicting uncommitted entries when an incoming `AppendEntries` presents a conflicting term, but a committed entry must never be truncated or overwritten.
**Mechanism**:
- Truncation only targets `index > commitIndex`. If a conflict is discovered at `index <= commitIndex`, the node panics or terminates as an invariant violation.
- Suffix truncations are written durably to WAL before sending `Success = true`.
**Traceability**:
- Implemented in: `internal/raft/log.go`
- Tested by: `TestSuffixTruncation_CommittedPrefixPreserved`

---

## 2. Storage & Snapshot Invariants

### INV-7: Durable Persistence Before Response
**Definition**: A server must never emit a network RPC response that promises state that has not been synced to durable storage.
**Mechanism**:
- `currentTerm` & `votedFor` synced before `RequestVoteResponse`.
- Log entries and suffix truncations synced before `AppendEntriesResponse(success=true)`.
- State machine snapshots flushed and atomically renamed before acknowledging `InstallSnapshotResponse`.
**Traceability**:
- Implemented in: `internal/storage/wal.go`, `internal/raft/`
- Tested by: `TestDurableWrite_BeforeRPCResponse`

### INV-8: Snapshot Boundary Consistency
**Definition**: The snapshot boundary `(lastIncludedIndex, lastIncludedTerm)` represents a committed, applied prefix of the log.
**Mechanism**:
- `lastIncludedIndex <= lastApplied <= commitIndex`.
- Log entries with `index <= lastIncludedIndex` are eligible for compaction.
- Logical index resolution functions (`termAt(i)`, `entryAt(i)`) return valid results using snapshot base metadata when index equals `lastIncludedIndex`.
**Traceability**:
- Implemented in: `internal/raft/log.go`, `internal/storage/snapshot.go`
- Tested by: `TestSnapshotBoundary_LogCompactionResolution`

---

## 3. Client & State Machine Invariants

### INV-9: Deterministic State Machine Execution
**Definition**: Given identical sequence of applied committed entries, state machines on all nodes produce identical outputs and state.
**Mechanism**:
- State transitions (Put, Get, Delete, CAS) rely solely on the entry contents and internal key-value map.
- No dependence on wall-clock time, random number generators, or network status.
**Traceability**:
- Implemented in: `internal/statemachine/kv.go`
- Tested by: `TestStateMachine_DeterministicReplay`

### INV-10: Exactly-Once Mutation Effect (Deduplication)
**Definition**: A client retrying a request with the same `(client_id, request_id)` receives the previous outcome without applying the mutation a second time. Reusing a `request_id` with different command contents returns a deterministic error.
**Mechanism**:
- The state machine maintains a deduplication table indexed by `client_id`.
- Successful mutations cache `(request_id, command_hash, result)`.
- If `request_id` matches and hash matches: return cached result.
- If `request_id` matches but hash differs: return `ErrRequestIdConflict`.
**Traceability**:
- Implemented in: `internal/statemachine/dedupe.go`
- Tested by: `TestDedupe_RetryIdempotency`, `TestDedupe_PayloadMismatchRejection`
