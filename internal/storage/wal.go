package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// WAL manages the append-only write-ahead log file.
type WAL struct {
	mu          sync.Mutex
	file        *os.File
	path        string
	lastSeq     uint64
	validOffset int64
}

// OpenWAL opens or creates a WAL in dirPath, performing crash recovery.
func OpenWAL(dirPath string) (*WAL, []Record, error) {
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, nil, err
	}

	walPath := filepath.Join(dirPath, "raft.wal")
	f, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, nil, err
	}

	wal := &WAL{
		file: f,
		path: walPath,
	}

	records, validOffset, lastSeq, err := wal.recover(f)
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}

	// Seek to the end of valid records so future appends write at validOffset
	if _, err := f.Seek(validOffset, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, nil, fmt.Errorf("failed to seek WAL: %w", err)
	}

	wal.validOffset = validOffset
	wal.lastSeq = lastSeq
	return wal, records, nil
}

// recover scans the WAL file, collects valid records, detects torn tails at EOF,
// and fails closed on corrupted records.
func (w *WAL) recover(f *os.File) ([]Record, int64, uint64, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, 0, 0, err
	}

	var records []Record
	var validOffset int64 = 0
	var lastSeq uint64 = 0

	for {
		rec, n, err := ReadRecord(f)
		if err == io.EOF {
			// Clean end of file
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			// Torn write at EOF: truncate file back to validOffset
			fmt.Printf("[WAL RECOVERY] Incomplete final record (torn tail) detected at offset %d (%d bytes discarded). Truncating WAL.\n", validOffset, n)
			if truncErr := f.Truncate(validOffset); truncErr != nil {
				return nil, 0, 0, fmt.Errorf("failed to truncate torn tail: %w", truncErr)
			}
			if syncErr := f.Sync(); syncErr != nil {
				return nil, 0, 0, fmt.Errorf("failed to sync after torn tail truncation: %w", syncErr)
			}
			break
		}
		if err != nil {
			// Corrupt record (CRC mismatch or invalid header) -> fail closed
			return nil, 0, 0, fmt.Errorf("WAL recovery failed at offset %d: %w", validOffset, err)
		}

		records = append(records, *rec)
		validOffset += int64(n)
		lastSeq = rec.Header.Sequence
	}

	return records, validOffset, lastSeq, nil
}

// Append writes a framed record with synchronous fsync.
func (w *WAL) Append(recType uint8, payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.lastSeq++
	frame, err := EncodeRecord(recType, w.lastSeq, payload)
	if err != nil {
		w.lastSeq--
		return err
	}

	n, err := w.file.Write(frame)
	if err != nil {
		return fmt.Errorf("WAL write failed: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("WAL sync failed: %w", err)
	}

	w.validOffset += int64(n)
	return nil
}

// Close syncs and closes the WAL.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}
	_ = w.file.Sync()
	err := w.file.Close()
	w.file = nil
	return err
}

// Path returns the filesystem path of the WAL file.
func (w *WAL) Path() string {
	return w.path
}
