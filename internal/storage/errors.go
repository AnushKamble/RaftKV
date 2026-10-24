package storage

import "errors"

var (
	// ErrCorruptRecord indicates a checksum mismatch or corrupted record framing in the WAL.
	ErrCorruptRecord = errors.New("storage: corrupt WAL record")

	// ErrDirectoryLocked indicates the storage directory is locked by another running process.
	ErrDirectoryLocked = errors.New("storage: data directory is locked by another process")

	// ErrCompacted indicates the requested log index has been compacted into a snapshot.
	ErrCompacted = errors.New("storage: requested entry has been compacted")

	// ErrNotFound indicates the requested index was not found in the log.
	ErrNotFound = errors.New("storage: requested entry not found")

	// ErrNilStorage indicates storage has not been initialized.
	ErrNilStorage = errors.New("storage: storage engine not initialized")
)
