package storage

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestEncodeReadRecord(t *testing.T) {
	payload := []byte("hello raftkv durable world")
	recType := RecordTypeHardState
	seq := uint64(10)

	frame, err := EncodeRecord(recType, seq, payload)
	if err != nil {
		t.Fatalf("failed to encode: %v", err)
	}

	buf := bytes.NewReader(frame)
	rec, n, err := ReadRecord(buf)
	if err != nil {
		t.Fatalf("failed to read record: %v", err)
	}

	if n != len(frame) {
		t.Fatalf("expected bytes read %d, got %d", len(frame), n)
	}
	if rec.Header.Type != recType || rec.Header.Sequence != seq || rec.Header.Length != uint32(len(payload)) {
		t.Fatalf("header mismatch: %+v", rec.Header)
	}
	if !bytes.Equal(rec.Payload, payload) {
		t.Fatalf("payload mismatch: got %s, want %s", string(rec.Payload), string(payload))
	}
}

func TestTornFinalFrame_HeaderTruncated(t *testing.T) {
	frame, err := EncodeRecord(RecordTypeHardState, 1, []byte("data"))
	if err != nil {
		t.Fatal(err)
	}

	// Truncate header mid-way (e.g. only 10 bytes written of 18)
	tornFrame := frame[:10]
	buf := bytes.NewReader(tornFrame)
	_, _, err = ReadRecord(buf)
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("expected io.ErrUnexpectedEOF for torn header, got %v", err)
	}
}

func TestTornFinalFrame_PayloadTruncated(t *testing.T) {
	payload := []byte("some large payload that was interrupted during write")
	frame, err := EncodeRecord(RecordTypeLogAppend, 1, payload)
	if err != nil {
		t.Fatal(err)
	}

	// Truncate payload (header intact, half payload missing)
	tornFrame := frame[:RecordHeaderSize+5]
	buf := bytes.NewReader(tornFrame)
	_, _, err = ReadRecord(buf)
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("expected io.ErrUnexpectedEOF for torn payload, got %v", err)
	}
}

func TestCorruptMiddleRecord_FailsClosed(t *testing.T) {
	frame, err := EncodeRecord(RecordTypeHardState, 1, []byte("valid payload"))
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt a byte inside the payload
	frame[RecordHeaderSize+2] ^= 0xFF

	buf := bytes.NewReader(frame)
	_, _, err = ReadRecord(buf)
	if err == nil {
		t.Fatal("expected error on corrupt record, got nil")
	}
}

func TestWALRecovery_TornTailTruncation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "raftkv-wal-torn-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	wal, _, err := OpenWAL(tmpDir)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}

	// Write 2 valid records
	if err := wal.Append(RecordTypeHardState, []byte("record-1")); err != nil {
		t.Fatal(err)
	}
	if err := wal.Append(RecordTypeLogAppend, []byte("record-2")); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	// Append torn bytes (incomplete header) to raft.wal
	walPath := filepath.Join(tmpDir, "raft.wal")
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.Write([]byte{0x00, 0x00, 0x00, 0x08, 0x01}) // 5 bytes only
	_ = f.Sync()
	_ = f.Close()

	// Reopen WAL: recovery should detect torn tail, truncate it, and succeed
	wal2, records, err := OpenWAL(tmpDir)
	if err != nil {
		t.Fatalf("reopening WAL failed: %v", err)
	}
	defer wal2.Close()

	if len(records) != 2 {
		t.Fatalf("expected 2 valid recovered records, got %d", len(records))
	}

	// Append a 3rd record now: should succeed cleanly without corruption
	if err := wal2.Append(RecordTypeTruncate, []byte("record-3")); err != nil {
		t.Fatalf("failed to append after torn tail recovery: %v", err)
	}
}

func TestWALRecovery_MiddleCorruptionFailsClosed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "raftkv-wal-corrupt-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	wal, _, err := OpenWAL(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	if err := wal.Append(RecordTypeHardState, []byte("record-1")); err != nil {
		t.Fatal(err)
	}
	if err := wal.Append(RecordTypeLogAppend, []byte("record-2")); err != nil {
		t.Fatal(err)
	}
	_ = wal.Close()

	// Corrupt record 1 inside raft.wal
	walPath := filepath.Join(tmpDir, "raft.wal")
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	data[5] = 0x99 // invalidate version/type
	if err := os.WriteFile(walPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	// Reopen must fail closed
	_, _, err = OpenWAL(tmpDir)
	if err == nil {
		t.Fatal("expected error on corrupt middle frame, got nil")
	}
}
