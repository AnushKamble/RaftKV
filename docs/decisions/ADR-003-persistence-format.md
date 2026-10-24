# ADR-003: Durable Storage and Framed WAL Architecture

## Context
Raft requires durable persistence of `currentTerm`, `votedFor`, and log entries before responding to RequestVote or AppendEntries RPCs. On restart, the node must recover exact hard state and uncorrupted log entries, reject corruptions, and handle torn writes resulting from abrupt process termination.

## Decision
We implement a custom framed binary write-ahead log (WAL):
1. **Record Framing**:
   - `Length` (`uint32`): 4 bytes payload length.
   - `Type` (`uint8`): Record type (`RecordHardState = 1`, `RecordLogAppend = 2`, `RecordTruncate = 3`, `RecordSnapshotMeta = 4`).
   - `Version` (`uint8`): Format version (`0x01`).
   - `Sequence` (`uint64`): Monotonically increasing record sequence number.
   - `Payload` (`[]byte`): Serialized record contents.
   - `CRC32` (`uint32`): Castagnoli/IEEE CRC32 checksum over the header fields and payload.
2. **Durability Rules**:
   - Persist hard state (`currentTerm`, `votedFor`) before returning `voteGranted=true` or advancing terms.
   - Persist log appends and suffix truncations before returning `success=true` in `AppendEntries`.
   - Explicit `Sync()` call on WAL file handles before emitting dependent protocol replies.
3. **Recovery Policy**:
   - Sequential replay from sequence 0.
   - Middle-record corruption (CRC mismatch, invalid length, invalid type) fails closed with an unrecoverable error.
   - A torn write at the final record (incomplete header or payload truncated at EOF) is safely discarded with an explicit recovery warning.
4. **Snapshot and Compaction**:
   - Snapshot contains: `lastIncludedIndex`, `lastIncludedTerm`, format version, checksum, serialized KV state, and deduplication table.
   - Published via atomic rename (`snapshot.tmp` -> `snapshot.snap`).
   - WAL compaction creates a fresh WAL segment containing the snapshot marker and any uncompacted suffix entries, then replaces the previous WAL.
5. **Process Isolation**:
   - An advisory directory lock file (`node.lock`) prevents multiple processes from concurrently opening the same data directory.

## Consequences
- **Positive**: Strict crash safety, zero silent data corruption, explicit torn-tail recovery, transparent storage format.
- **Negative**: Fsync on every commit impacts raw write throughput; this trade-off is prioritized for distributed consensus correctness.
- **Status**: ACCEPTED.
