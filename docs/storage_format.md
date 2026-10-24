# RaftKV — Durable Storage & WAL Format Specification

## 1. Directory Structure

Each RaftKV node maintains its own isolated data directory:
```text
data/node-1/
  node.lock          # Advisory lock file ensuring single-process ownership
  raft.wal           # Framed binary append-only write-ahead log
  snapshot.snap      # Published state machine snapshot (atomic)
  snapshot.tmp       # In-progress snapshot being written (discarded on crash)
```

---

## 2. Framed WAL Binary Record Format

All persistent mutations to Raft hard state and consensus log are serialized into `raft.wal` as framed records.

### 2.1 Header Format (18 Bytes)
```text
+-------------------+----------------+-------------------+--------------------+-----------------+-------------------+
| Length (4 Bytes)  | Type (1 Byte)  | Version (1 Byte)  | Sequence (8 Bytes) | CRC32 (4 Bytes) | Payload (N Bytes) |
| uint32 (BigEndian)| uint8          | uint8 (0x01)      | uint64 (BigEndian) | uint32 (IEEE)   | []byte            |
+-------------------+----------------+-------------------+--------------------+-----------------+-------------------+
```

- **Length** (`uint32`): Byte length $N$ of the payload ($0 \le N \le 16 \text{ MB}$).
- **Type** (`uint8`):
  - `0x01` (`RecordHardState`): Contains updated `currentTerm` and `votedFor`.
  - `0x02` (`RecordLogAppend`): Contains one or more serialized `LogEntry` records.
  - `0x03` (`RecordTruncate`): Contains logical `fromIndex` from which all suffix entries are removed.
  - `0x04` (`RecordSnapshotMeta`): Contains compacted snapshot base index and term.
- **Version** (`uint8`): Protocol format version (`0x01`).
- **Sequence** (`uint64`): Monotonically increasing sequence number starting from 1.
- **CRC32** (`uint32`): IEEE CRC32 checksum computed over `Length + Type + Version + Sequence + Payload`.

---

## 3. Storage Operations & Durability Rules

1. **`SaveHardState(term, votedFor)`**:
   Encodes `RecordHardState`, appends frame to WAL, calls `file.Sync()`. Must complete before sending `RequestVoteReply` or starting a campaign.
2. **`AppendEntries(entries)`**:
   Encodes `RecordLogAppend`, appends frame to WAL, calls `file.Sync()`. Must complete before returning `AppendEntriesReply(success=true)`.
3. **`TruncateSuffix(fromIndex)`**:
   Encodes `RecordTruncate`, appends frame to WAL, calls `file.Sync()`. Removes all log entries $\ge \text{fromIndex}$.
4. **`SaveSnapshot(meta, data)`**:
   Writes snapshot data to `snapshot.tmp`, fsyncs `snapshot.tmp`, atomically renames to `snapshot.snap`.
5. **`CompactLog(compactIndex, compactTerm)`**:
   Creates `raft.wal.tmp`, writes `RecordSnapshotMeta`, copies retained log suffix ($> \text{compactIndex}$), fsyncs, and atomically renames to `raft.wal`.

---

## 4. Crash Recovery & Failure Policy

### 4.1 Torn-Tail Handling
If the process terminates abruptly during a disk write:
- The last frame in `raft.wal` may have an incomplete 18-byte header or truncated payload bytes at EOF.
- Recovery detects that EOF was reached mid-frame.
- The incomplete final frame is discarded and the WAL file is truncated back to the last valid byte offset.
- A recovery notice is logged with the discarded byte length.

### 4.2 Corrupt Middle Frame Policy
- If any record before the end of the file fails its CRC32 checksum, has an unrecognized record type, or has an invalid format version:
- The storage engine **fails closed** immediately and returns `ErrCorruptRecord`.
- The node will not start or proceed with corrupted state.

### 4.3 Directory Lock
- On opening a data directory, the engine acquires an exclusive lock on `node.lock`.
- If another process attempts to open the same directory, open fails immediately with `ErrDirectoryLocked`.
