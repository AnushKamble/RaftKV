package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"raftkv/internal/protocol"
)

func TestStorage_HardStateAndLogReopen(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "raftkv-storage-hs-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	s, err := OpenDiskStorage(tmpDir)
	if err != nil {
		t.Fatalf("failed to open storage: %v", err)
	}

	// Persist hard state
	if err := s.SaveHardState(3, "node-2"); err != nil {
		t.Fatalf("failed to save hard state: %v", err)
	}

	// Append log entries
	entries := []protocol.LogEntry{
		{Index: 1, Term: 1, Command: protocol.Command{Op: protocol.OpPut, Key: "k1", Value: "v1"}},
		{Index: 2, Term: 2, Command: protocol.Command{Op: protocol.OpPut, Key: "k2", Value: "v2"}},
		{Index: 3, Term: 3, Command: protocol.Command{Op: protocol.OpPut, Key: "k3", Value: "v3"}},
	}
	if err := s.AppendEntries(entries); err != nil {
		t.Fatalf("failed to append entries: %v", err)
	}

	// Verify in-memory state before closing
	term, votedFor, err := s.LoadHardState()
	if err != nil || term != 3 || votedFor != "node-2" {
		t.Fatalf("unexpected hard state: term=%d, votedFor=%s", term, votedFor)
	}
	lastIdx, lastTerm, err := s.LastIndexAndTerm()
	if err != nil || lastIdx != 3 || lastTerm != 3 {
		t.Fatalf("unexpected last index/term: idx=%d, term=%d", lastIdx, lastTerm)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("failed to close storage: %v", err)
	}

	// Reopen storage and verify all state is reconstructed from WAL
	s2, err := OpenDiskStorage(tmpDir)
	if err != nil {
		t.Fatalf("failed to reopen storage: %v", err)
	}
	defer s2.Close()

	term, votedFor, err = s2.LoadHardState()
	if err != nil || term != 3 || votedFor != "node-2" {
		t.Fatalf("reopened hard state mismatch: term=%d, votedFor=%s", term, votedFor)
	}

	lastIdx, lastTerm, err = s2.LastIndexAndTerm()
	if err != nil || lastIdx != 3 || lastTerm != 3 {
		t.Fatalf("reopened last index/term mismatch: idx=%d, term=%d", lastIdx, lastTerm)
	}

	gotEntries, err := s2.Entries(1, 4)
	if err != nil || len(gotEntries) != 3 {
		t.Fatalf("failed to fetch entries after reopen: len=%d, err=%v", len(gotEntries), err)
	}
	if gotEntries[1].Command.Key != "k2" || gotEntries[1].Command.Value != "v2" {
		t.Fatalf("entry data mismatch: %+v", gotEntries[1])
	}
}

func TestStorage_DurableTruncationReopen(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "raftkv-storage-trunc-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	s, err := OpenDiskStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	entries := []protocol.LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
		{Index: 4, Term: 2},
	}
	if err := s.AppendEntries(entries); err != nil {
		t.Fatal(err)
	}

	// Truncate suffix from index 3 (removes 3 and 4)
	if err := s.TruncateSuffix(3); err != nil {
		t.Fatalf("failed to truncate suffix: %v", err)
	}

	lastIdx, _, err := s.LastIndexAndTerm()
	if err != nil || lastIdx != 2 {
		t.Fatalf("expected last index 2 after truncate, got %d", lastIdx)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: truncation must be preserved
	s2, err := OpenDiskStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	lastIdx, _, err = s2.LastIndexAndTerm()
	if err != nil || lastIdx != 2 {
		t.Fatalf("expected last index 2 after reopen, got %d", lastIdx)
	}
	if _, err := s2.Entry(3); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound for entry 3, got %v", err)
	}
}

func TestStorage_DirectoryLockExclusivity(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "raftkv-storage-lock-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	s1, err := OpenDiskStorage(tmpDir)
	if err != nil {
		t.Fatalf("failed to open storage 1: %v", err)
	}
	defer s1.Close()

	// Attempting to open the same directory concurrently must fail with ErrDirectoryLocked
	_, err = OpenDiskStorage(tmpDir)
	if err != ErrDirectoryLocked {
		t.Fatalf("expected ErrDirectoryLocked for concurrent open, got %v", err)
	}
}

func TestStorage_SnapshotAndCompaction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "raftkv-storage-snap-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	s, err := OpenDiskStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	entries := []protocol.LogEntry{
		{Index: 1, Term: 1, Command: protocol.Command{Op: protocol.OpPut, Key: "a", Value: "1"}},
		{Index: 2, Term: 1, Command: protocol.Command{Op: protocol.OpPut, Key: "b", Value: "2"}},
		{Index: 3, Term: 2, Command: protocol.Command{Op: protocol.OpPut, Key: "c", Value: "3"}},
		{Index: 4, Term: 2, Command: protocol.Command{Op: protocol.OpPut, Key: "d", Value: "4"}},
	}
	if err := s.AppendEntries(entries); err != nil {
		t.Fatal(err)
	}

	// Save snapshot at index 2, term 1
	snapData := []byte(`{"a":"1","b":"2"}`)
	meta := protocol.SnapshotMeta{
		LastIncludedIndex: 2,
		LastIncludedTerm:  1,
		FormatVersion:     1,
		DataChecksum:      42,
	}
	if err := s.SaveSnapshot(meta, snapData); err != nil {
		t.Fatalf("failed to save snapshot: %v", err)
	}

	// Compact log up to index 2
	if err := s.CompactLog(2, 1); err != nil {
		t.Fatalf("failed to compact log: %v", err)
	}

	// Query compacted entry 1 -> should return ErrCompacted
	if _, err := s.Entry(1); err != ErrCompacted {
		t.Fatalf("expected ErrCompacted for index 1, got %v", err)
	}

	// Query boundary index 2 -> Term(2) returns 1
	term2, err := s.Term(2)
	if err != nil || term2 != 1 {
		t.Fatalf("expected term 1 for boundary index 2, got %d, err=%v", term2, err)
	}

	// Query retained entry 3 -> should succeed
	e3, err := s.Entry(3)
	if err != nil || e3.Command.Key != "c" {
		t.Fatalf("expected entry 3, got %+v, err=%v", e3, err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen from disk: snapshot and suffix log must be restored
	s2, err := OpenDiskStorage(tmpDir)
	if err != nil {
		t.Fatalf("failed to reopen storage after compaction: %v", err)
	}
	defer s2.Close()

	loadedMeta, loadedData, err := s2.LoadSnapshot()
	if err != nil || loadedMeta == nil {
		t.Fatalf("failed to load snapshot after reopen: %v", err)
	}
	if loadedMeta.LastIncludedIndex != 2 || loadedMeta.LastIncludedTerm != 1 {
		t.Fatalf("loaded snapshot meta mismatch: %+v", loadedMeta)
	}
	if !bytes.Equal(loadedData, snapData) {
		t.Fatalf("loaded snapshot data mismatch: %s != %s", string(loadedData), string(snapData))
	}

	// Boundary queries after reopen
	if _, err := s2.Entry(1); err != ErrCompacted {
		t.Fatalf("expected ErrCompacted for index 1 after reopen, got %v", err)
	}
	e4, err := s2.Entry(4)
	if err != nil || e4.Command.Key != "d" {
		t.Fatalf("expected entry 4 after reopen, got %+v, err=%v", e4, err)
	}
}

func TestStorage_SnapshotCorruptionFailsClosed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "raftkv-storage-snap-corrupt-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	s, err := OpenDiskStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	meta := protocol.SnapshotMeta{
		LastIncludedIndex: 1,
		LastIncludedTerm:  1,
		FormatVersion:     1,
	}
	if err := s.SaveSnapshot(meta, []byte("valid snapshot data")); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()

	// Corrupt snapshot file bytes
	snapPath := filepath.Join(tmpDir, "snapshot.snap")
	data, err := os.ReadFile(snapPath)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-6] ^= 0xFF // corrupt data byte
	if err := os.WriteFile(snapPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	// Reopen: LoadSnapshot must return ErrCorruptRecord
	s2, err := OpenDiskStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	_, _, err = s2.LoadSnapshot()
	if err == nil {
		t.Fatal("expected error loading corrupted snapshot, got nil")
	}
}

func TestStorage_CompactionPreservesHardState(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "raftkv-storage-hardstate-compact-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	s, err := OpenDiskStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	// Save hard state term 5, votedFor "node-2"
	if err := s.SaveHardState(5, "node-2"); err != nil {
		t.Fatal(err)
	}

	entries := []protocol.LogEntry{
		{Index: 1, Term: 4, Command: protocol.Command{Op: protocol.OpPut, Key: "a", Value: "1"}},
		{Index: 2, Term: 5, Command: protocol.Command{Op: protocol.OpPut, Key: "b", Value: "2"}},
	}
	if err := s.AppendEntries(entries); err != nil {
		t.Fatal(err)
	}

	meta := protocol.SnapshotMeta{
		LastIncludedIndex: 1,
		LastIncludedTerm:  4,
		FormatVersion:     1,
	}
	if err := s.SaveSnapshot(meta, []byte("{}")); err != nil {
		t.Fatal(err)
	}

	if err := s.CompactLog(1, 4); err != nil {
		t.Fatal(err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen after compaction: term 5 and votedFor "node-2" MUST be preserved!
	s2, err := OpenDiskStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	term, votedFor, err := s2.LoadHardState()
	if err != nil {
		t.Fatal(err)
	}
	if term != 5 || votedFor != "node-2" {
		t.Fatalf("expected term 5, votedFor 'node-2' after compaction & restart, got term %d, votedFor %q", term, votedFor)
	}
}
