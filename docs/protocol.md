# RaftKV — Protocol & RPC Contract Specification

## 1. Indexing & Log Boundary Semantics

### Index 0 Sentinel & Base Index
- Logical log indices start at `1`.
- Logical index `0` is the sentinel base index:
  - `termAt(0) = 0`
  - `entryAt(0)` returns no command payload.
- When no snapshots exist:
  - `lastIncludedIndex = 0`
  - `lastIncludedTerm = 0`
  - `firstIndex() = 1`
- When a snapshot exists up to `lastIncludedIndex = K`:
  - `termAt(K) = lastIncludedTerm`
  - For any `i < K`, `entryAt(i)` is compacted; querying returns `ErrCompacted`.
  - `firstIndex() = K + 1`
  - `lastIndex()` returns the index of the latest entry in the active log, or `K` if the log is completely compacted.

---

## 2. Protocol Messages (Raft Peer RPCs)

### 2.1 RequestVote
```protobuf
message RequestVoteArgs {
  uint64 term = 1;
  string candidate_id = 2;
  uint64 last_log_index = 3;
  uint64 last_log_term = 4;
}

message RequestVoteReply {
  uint64 term = 1;
  bool vote_granted = 2;
}
```
**Handling Rules**:
1. If `term < currentTerm`: return `vote_granted = false`.
2. If `term > currentTerm`: set `currentTerm = term`, `votedFor = ""`, transition to `Follower`, persist to WAL.
3. Candidate log is at least as up-to-date if:
   - `last_log_term > localLastLogTerm`, OR
   - `last_log_term == localLastLogTerm && last_log_index >= localLastLogIndex`.
4. If (`votedFor == ""` OR `votedFor == candidate_id`) AND candidate log is up-to-date:
   - set `votedFor = candidate_id`
   - sync to WAL
   - return `vote_granted = true`.
5. Otherwise: return `vote_granted = false`.

### 2.2 AppendEntries
```protobuf
message LogEntryProto {
  uint64 index = 1;
  uint64 term = 2;
  bytes command = 3;
}

message AppendEntriesArgs {
  uint64 term = 1;
  string leader_id = 2;
  uint64 prev_log_index = 3;
  uint64 prev_log_term = 4;
  repeated LogEntryProto entries = 5;
  uint64 leader_commit = 6;
}

message AppendEntriesReply {
  uint64 term = 1;
  bool success = 2;
  uint64 conflict_index = 3;
  uint64 conflict_term = 4;
}
```
**Conflict Backtracking Optimization**:
- If follower rejects because `prev_log_index > lastIndex()`:
  - `conflict_index = lastIndex() + 1`, `conflict_term = 0`.
- If follower has entry at `prev_log_index` with mismatched term `t != prev_log_term`:
  - `conflict_term = t`.
  - `conflict_index = first index in follower's log with term t`.
- Leader uses `(conflict_index, conflict_term)` to skip entire terms during backtracking.

### 2.3 InstallSnapshot
```protobuf
message InstallSnapshotArgs {
  uint64 term = 1;
  string leader_id = 2;
  uint64 last_included_index = 3;
  uint64 last_included_term = 4;
  bytes data = 5;
  bool done = 6;
}

message InstallSnapshotReply {
  uint64 term = 1;
}
```
**Handling Rules**:
1. If `term < currentTerm`: reply immediately.
2. If `term > currentTerm`: adopt term, transition to follower, persist to WAL.
3. Save snapshot file to disk atomically.
4. If local log contains entry at `last_included_index` with matching `last_included_term`: retain suffix `> last_included_index`; otherwise discard entire log.
5. Apply snapshot to state machine and deduplication table.
6. Set `lastApplied = last_included_index`, `commitIndex = max(commitIndex, last_included_index)`.

---

## 3. Client KV API Contract

```protobuf
enum OpType {
  OP_UNKNOWN = 0;
  OP_PUT = 1;
  OP_GET = 2;
  OP_DELETE = 3;
  OP_CAS = 4;
}

message CommandProto {
  string client_id = 1;
  uint64 request_id = 2;
  OpType op = 3;
  string key = 4;
  string value = 5;
  string expected_value = 6;
}

message ClientCommandReply {
  bool success = 1;
  string error_code = 2; // "OK", "NOT_LEADER", "KEY_NOT_FOUND", "CAS_MISMATCH", "PAYLOAD_MISMATCH"
  string leader_hint = 3;
  string value = 4;
  bool found = 5;
  bool swapped = 6;
  uint64 commit_index = 7;
}
```

### Command Semantics
- **Put(k, v)**: Sets key `k` to value `v`. Always succeeds once committed.
- **Get(k)**: Logged operation through consensus. Returns value and whether key was found.
- **Delete(k)**: Deletes key `k`. Returns `found = true` if key was present, `found = false` if key did not exist.
- **CompareAndSwap(k, expected, new)**:
  - If `expected == ""` and key does not exist: sets `k = new`, returns `swapped = true`.
  - If key exists and `current_value == expected`: sets `k = new`, returns `swapped = true`.
  - Otherwise: leaves key unchanged, returns `swapped = false` and observed `value`.

### Client Timeout & Unknown Outcome Policy
1. If a client receives a gRPC timeout or connection drop after submitting a command, the outcome is **UNKNOWN** (the command may or may not have committed).
2. The client **MUST** retry the operation using the **exact same** `(client_id, request_id, command)` tuple.
3. If the command was committed, the state machine returns the cached result without reapplying mutations.
4. Attempting to submit a new command with a previously used `(client_id, request_id)` returns `PAYLOAD_MISMATCH` deterministic error.

---

## 4. Bounded History-Based Linearizability Checker Semantics

### Sequential Specification
The sequential specification is a deterministic key–value store:
```
State: map[string]string

Step(State, Op) -> (NewState, Result)
```

### Real-Time Order & Bounded Search
An operation $O_1$ strictly precedes $O_2$ ($O_1 \prec_{RT} O_2$) if:
$$\text{CompleteTime}(O_1) < \text{InvokeTime}(O_2)$$

The checker searches for a total order $L = \langle O_1, O_2, \dots, O_m \rangle$ of all completed operations (plus a subset of timed-out operations) such that:
1. $L$ is a topological sort of $\prec_{RT}$ (respects real-time precedence).
2. For each prefix $L_k$, applying operations in order yields results matching observed outputs.
3. Search bound: Maximum search tree expansions capped at $M$ (e.g. 50,000 steps). If search exceeds $M$, return `INCONCLUSIVE`, never `PASS`.
