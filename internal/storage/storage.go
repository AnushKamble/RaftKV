package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"raftkv/internal/protocol"
)

// Storage defines the durable persistence interface required by Raft.
type Storage interface {
	SaveHardState(term protocol.Term, votedFor string) error
	LoadHardState() (protocol.Term, string, error)
	AppendEntries(entries []protocol.LogEntry) error
	TruncateSuffix(fromIndex protocol.Index) error
	Entries(from protocol.Index, to protocol.Index) ([]protocol.LogEntry, error)
	Entry(index protocol.Index) (protocol.LogEntry, error)
	Term(index protocol.Index) (protocol.Term, error)
	LastIndexAndTerm() (protocol.Index, protocol.Term, error)
	FirstIndex() (protocol.Index, error)
	SaveSnapshot(meta protocol.SnapshotMeta, data []byte) error
	LoadSnapshot() (*protocol.SnapshotMeta, []byte, error)
	CompactLog(compactIndex protocol.Index, compactTerm protocol.Term) error
	Close() error
}

// DiskStorage implements durable storage backed by framed WAL and snapshot files.
type DiskStorage struct {
	mu                sync.RWMutex
	dirPath           string
	lock              *DirLock
	wal               *WAL
	currentTerm       protocol.Term
	votedFor          string
	lastIncludedIndex protocol.Index
	lastIncludedTerm  protocol.Term
	entries           []protocol.LogEntry
}

// OpenDiskStorage opens or creates a durable storage instance in dirPath.
func OpenDiskStorage(dirPath string) (*DiskStorage, error) {
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, err
	}

	lockPath := filepath.Join(dirPath, "node.lock")
	lock, err := AcquireDirLock(lockPath)
	if err != nil {
		return nil, err
	}

	wal, records, err := OpenWAL(dirPath)
	if err != nil {
		_ = lock.Release()
		return nil, err
	}

	ds := &DiskStorage{
		dirPath: dirPath,
		lock:    lock,
		wal:     wal,
	}

	// Replay WAL records (this restores hard state, snapshot boundary, and active log suffix)
	if err := ds.replay(records); err != nil {
		_ = ds.Close()
		return nil, err
	}

	return ds, nil
}

// replay applies decoded WAL records into memory during initialization.
func (ds *DiskStorage) replay(records []Record) error {
	for _, rec := range records {
		switch rec.Header.Type {
		case RecordTypeHardState:
			var hs HardStateData
			if err := json.Unmarshal(rec.Payload, &hs); err != nil {
				return fmt.Errorf("failed to decode hard state: %w", err)
			}
			ds.currentTerm = hs.Term
			ds.votedFor = hs.VotedFor

		case RecordTypeLogAppend:
			entries, err := DecodeLogEntries(rec.Payload)
			if err != nil {
				return fmt.Errorf("failed to decode log append: %w", err)
			}
			ds.entries = append(ds.entries, entries...)

		case RecordTypeTruncate:
			var td TruncateData
			if err := json.Unmarshal(rec.Payload, &td); err != nil {
				return fmt.Errorf("failed to decode truncate: %w", err)
			}
			ds.applyTruncateInMemory(td.FromIndex)

		case RecordTypeSnapshotMeta:
			var smd SnapshotMetaData
			if err := json.Unmarshal(rec.Payload, &smd); err != nil {
				return fmt.Errorf("failed to decode snapshot meta: %w", err)
			}
			ds.lastIncludedIndex = smd.LastIncludedIndex
			ds.lastIncludedTerm = smd.LastIncludedTerm
			ds.applyCompactionInMemory(smd.LastIncludedIndex)
		}
	}
	return nil
}

func (ds *DiskStorage) applyTruncateInMemory(fromIndex protocol.Index) {
	if len(ds.entries) == 0 {
		return
	}
	firstIdx := ds.firstIndexLocked()
	if fromIndex < firstIdx {
		ds.entries = nil
		return
	}
	offset := int(fromIndex - firstIdx)
	if offset < len(ds.entries) {
		ds.entries = ds.entries[:offset]
	}
}

func (ds *DiskStorage) applyCompactionInMemory(compactIndex protocol.Index) {
	if len(ds.entries) == 0 {
		return
	}
	firstIdx := ds.firstIndexLocked()
	if compactIndex < firstIdx {
		return
	}
	offset := int(compactIndex - firstIdx + 1)
	if offset >= len(ds.entries) {
		ds.entries = nil
	} else {
		ds.entries = ds.entries[offset:]
	}
}

func (ds *DiskStorage) firstIndexLocked() protocol.Index {
	return ds.lastIncludedIndex + 1
}

// SaveHardState persists currentTerm and votedFor.
func (ds *DiskStorage) SaveHardState(term protocol.Term, votedFor string) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	data, err := json.Marshal(HardStateData{Term: term, VotedFor: votedFor})
	if err != nil {
		return err
	}

	if err := ds.wal.Append(RecordTypeHardState, data); err != nil {
		return err
	}

	ds.currentTerm = term
	ds.votedFor = votedFor
	return nil
}

// LoadHardState returns the currently persisted term and vote.
func (ds *DiskStorage) LoadHardState() (protocol.Term, string, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.currentTerm, ds.votedFor, nil
}

// AppendEntries appends and syncs log entries.
func (ds *DiskStorage) AppendEntries(entries []protocol.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	ds.mu.Lock()
	defer ds.mu.Unlock()

	data, err := EncodeLogEntries(entries)
	if err != nil {
		return err
	}

	if err := ds.wal.Append(RecordTypeLogAppend, data); err != nil {
		return err
	}

	ds.entries = append(ds.entries, entries...)
	return nil
}

// TruncateSuffix removes all log entries with index >= fromIndex.
func (ds *DiskStorage) TruncateSuffix(fromIndex protocol.Index) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	data, err := json.Marshal(TruncateData{FromIndex: fromIndex})
	if err != nil {
		return err
	}

	if err := ds.wal.Append(RecordTypeTruncate, data); err != nil {
		return err
	}

	ds.applyTruncateInMemory(fromIndex)
	return nil
}

// Entries returns a slice of entries in [from, to).
func (ds *DiskStorage) Entries(from protocol.Index, to protocol.Index) ([]protocol.LogEntry, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	if from > to {
		return nil, fmt.Errorf("invalid range: from %d > to %d", from, to)
	}
	if from <= ds.lastIncludedIndex {
		return nil, ErrCompacted
	}

	firstIdx := ds.firstIndexLocked()
	lastIdx, _ := ds.lastIndexAndTermLocked()
	if to-1 > lastIdx {
		return nil, ErrNotFound
	}

	startOffset := int(from - firstIdx)
	endOffset := int(to - firstIdx)

	if startOffset < 0 || endOffset > len(ds.entries) {
		return nil, ErrNotFound
	}

	result := make([]protocol.LogEntry, endOffset-startOffset)
	copy(result, ds.entries[startOffset:endOffset])
	return result, nil
}

// Entry returns a single entry at index.
func (ds *DiskStorage) Entry(index protocol.Index) (protocol.LogEntry, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	if index <= ds.lastIncludedIndex {
		return protocol.LogEntry{}, ErrCompacted
	}

	firstIdx := ds.firstIndexLocked()
	lastIdx, _ := ds.lastIndexAndTermLocked()
	if index > lastIdx {
		return protocol.LogEntry{}, ErrNotFound
	}

	offset := int(index - firstIdx)
	if offset < 0 || offset >= len(ds.entries) {
		return protocol.LogEntry{}, ErrNotFound
	}

	return ds.entries[offset], nil
}

// Term returns the term of the entry at index.
func (ds *DiskStorage) Term(index protocol.Index) (protocol.Term, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	if index == ds.lastIncludedIndex {
		return ds.lastIncludedTerm, nil
	}
	if index < ds.lastIncludedIndex {
		return 0, ErrCompacted
	}

	firstIdx := ds.firstIndexLocked()
	lastIdx, _ := ds.lastIndexAndTermLocked()
	if index > lastIdx {
		return 0, ErrNotFound
	}

	offset := int(index - firstIdx)
	if offset < 0 || offset >= len(ds.entries) {
		return 0, ErrNotFound
	}

	return ds.entries[offset].Term, nil
}

func (ds *DiskStorage) lastIndexAndTermLocked() (protocol.Index, protocol.Term) {
	if len(ds.entries) == 0 {
		return ds.lastIncludedIndex, ds.lastIncludedTerm
	}
	last := ds.entries[len(ds.entries)-1]
	return last.Index, last.Term
}

// LastIndexAndTerm returns the highest index and corresponding term.
func (ds *DiskStorage) LastIndexAndTerm() (protocol.Index, protocol.Term, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	idx, term := ds.lastIndexAndTermLocked()
	return idx, term, nil
}

// FirstIndex returns the lowest available index in the log.
func (ds *DiskStorage) FirstIndex() (protocol.Index, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.firstIndexLocked(), nil
}

// SaveSnapshot atomically writes snapshot metadata and data to disk.
func (ds *DiskStorage) SaveSnapshot(meta protocol.SnapshotMeta, data []byte) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	tmpPath := filepath.Join(ds.dirPath, "snapshot.tmp")
	snapPath := filepath.Join(ds.dirPath, "snapshot.snap")

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	metaBytes, err := json.Marshal(meta)
	if err != nil {
		_ = f.Close()
		return err
	}

	// Write snapshot file structure:
	// 4 bytes: meta len
	// meta json bytes
	// 4 bytes: data len
	// data bytes
	// 4 bytes: crc32 over metaBytes + data
	hdr := make([]byte, 8)
	binary.BigEndian.PutUint32(hdr[0:4], uint32(len(metaBytes)))
	binary.BigEndian.PutUint32(hdr[4:8], uint32(len(data)))

	h := crc32.NewIEEE()
	h.Write(metaBytes)
	h.Write(data)
	checksum := h.Sum32()

	chkBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(chkBuf, checksum)

	if _, err := f.Write(hdr); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := f.Write(metaBytes); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := f.Write(chkBuf); err != nil {
		_ = f.Close()
		return err
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, snapPath); err != nil {
		return fmt.Errorf("failed to publish snapshot: %w", err)
	}

	return nil
}

// LoadSnapshot reads and verifies the current snapshot from disk.
func (ds *DiskStorage) LoadSnapshot() (*protocol.SnapshotMeta, []byte, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.loadSnapshotFromDisk()
}

func (ds *DiskStorage) loadSnapshotFromDisk() (*protocol.SnapshotMeta, []byte, error) {
	snapPath := filepath.Join(ds.dirPath, "snapshot.snap")
	f, err := os.Open(snapPath)
	if os.IsNotExist(err) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	hdr := make([]byte, 8)
	if _, err := io.ReadFull(f, hdr); err != nil {
		return nil, nil, fmt.Errorf("failed to read snapshot header: %w", err)
	}

	metaLen := binary.BigEndian.Uint32(hdr[0:4])
	dataLen := binary.BigEndian.Uint32(hdr[4:8])

	metaBytes := make([]byte, metaLen)
	if _, err := io.ReadFull(f, metaBytes); err != nil {
		return nil, nil, fmt.Errorf("failed to read snapshot meta: %w", err)
	}

	data := make([]byte, dataLen)
	if _, err := io.ReadFull(f, data); err != nil {
		return nil, nil, fmt.Errorf("failed to read snapshot data: %w", err)
	}

	chkBuf := make([]byte, 4)
	if _, err := io.ReadFull(f, chkBuf); err != nil {
		return nil, nil, fmt.Errorf("failed to read snapshot checksum: %w", err)
	}
	expectedCRC := binary.BigEndian.Uint32(chkBuf)

	h := crc32.NewIEEE()
	h.Write(metaBytes)
	h.Write(data)
	if h.Sum32() != expectedCRC {
		return nil, nil, fmt.Errorf("%w: snapshot CRC mismatch", ErrCorruptRecord)
	}

	var meta protocol.SnapshotMeta
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return nil, nil, fmt.Errorf("corrupt snapshot meta json: %w", err)
	}

	return &meta, data, nil
}

// CompactLog physically compacts the WAL up to compactIndex.
func (ds *DiskStorage) CompactLog(compactIndex protocol.Index, compactTerm protocol.Term) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if compactIndex <= ds.lastIncludedIndex {
		return nil // already compacted
	}

	firstIdx := ds.firstIndexLocked()

	// Prepare new temporary WAL
	tmpWalPath := filepath.Join(ds.dirPath, "raft.wal.tmp")
	_ = os.Remove(tmpWalPath)

	newWalFile, err := os.OpenFile(tmpWalPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	// Write SnapshotMeta record
	snapMetaBytes, err := json.Marshal(SnapshotMetaData{
		LastIncludedIndex: compactIndex,
		LastIncludedTerm:  compactTerm,
	})
	if err != nil {
		_ = newWalFile.Close()
		return err
	}

	var seq uint64 = 1
	metaFrame, err := EncodeRecord(RecordTypeSnapshotMeta, seq, snapMetaBytes)
	if err != nil {
		_ = newWalFile.Close()
		return err
	}
	if _, err := newWalFile.Write(metaFrame); err != nil {
		_ = newWalFile.Close()
		return err
	}

	// Copy suffix entries (index > compactIndex)
	offset := int(compactIndex - firstIdx + 1)
	var retainedEntries []protocol.LogEntry
	if offset < len(ds.entries) {
		retainedEntries = ds.entries[offset:]
		seq++
		entriesBytes, err := EncodeLogEntries(retainedEntries)
		if err != nil {
			_ = newWalFile.Close()
			return err
		}
		appendFrame, err := EncodeRecord(RecordTypeLogAppend, seq, entriesBytes)
		if err != nil {
			_ = newWalFile.Close()
			return err
		}
		if _, err := newWalFile.Write(appendFrame); err != nil {
			_ = newWalFile.Close()
			return err
		}
	}

	if err := newWalFile.Sync(); err != nil {
		_ = newWalFile.Close()
		return err
	}
	if err := newWalFile.Close(); err != nil {
		return err
	}

	// Close old WAL
	if err := ds.wal.Close(); err != nil {
		return err
	}

	// Replace old WAL with new WAL
	realWalPath := filepath.Join(ds.dirPath, "raft.wal")
	if err := os.Rename(tmpWalPath, realWalPath); err != nil {
		return fmt.Errorf("failed to replace WAL during compaction: %w", err)
	}

	// Reopen compacted WAL
	wal, _, err := OpenWAL(ds.dirPath)
	if err != nil {
		return fmt.Errorf("failed to reopen WAL after compaction: %w", err)
	}

	ds.wal = wal
	ds.lastIncludedIndex = compactIndex
	ds.lastIncludedTerm = compactTerm
	ds.entries = retainedEntries
	return nil
}

// Close closes WAL and releases the directory lock.
func (ds *DiskStorage) Close() error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	var errs []error
	if ds.wal != nil {
		if err := ds.wal.Close(); err != nil {
			errs = append(errs, err)
		}
		ds.wal = nil
	}
	if ds.lock != nil {
		if err := ds.lock.Release(); err != nil {
			errs = append(errs, err)
		}
		ds.lock = nil
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing storage: %v", errs)
	}
	return nil
}
