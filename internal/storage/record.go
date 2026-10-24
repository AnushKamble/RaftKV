package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"

	"raftkv/internal/protocol"
)

const (
	RecordHeaderSize    = 18
	RecordFormatVersion = uint8(0x01)
	MaxPayloadSize      = 16 * 1024 * 1024 // 16 MB max frame size
)

const (
	RecordTypeHardState    uint8 = 0x01
	RecordTypeLogAppend    uint8 = 0x02
	RecordTypeTruncate     uint8 = 0x03
	RecordTypeSnapshotMeta uint8 = 0x04
)

// Header represents the 18-byte framed record header.
type Header struct {
	Length   uint32
	Type     uint8
	Version  uint8
	Sequence uint64
	CRC32    uint32
}

// Record represents a decoded WAL entry.
type Record struct {
	Header  Header
	Payload []byte
}

// HardStateData represents the serialized hard state in a RecordTypeHardState.
type HardStateData struct {
	Term     protocol.Term `json:"term"`
	VotedFor string        `json:"voted_for"`
}

// TruncateData represents the payload for RecordTypeTruncate.
type TruncateData struct {
	FromIndex protocol.Index `json:"from_index"`
}

// SnapshotMetaData represents the payload for RecordTypeSnapshotMeta.
type SnapshotMetaData struct {
	LastIncludedIndex protocol.Index `json:"last_included_index"`
	LastIncludedTerm  protocol.Term  `json:"last_included_term"`
}

// EncodeRecord builds a complete byte slice containing the 18-byte header and payload.
func EncodeRecord(recType uint8, sequence uint64, payload []byte) ([]byte, error) {
	if len(payload) > MaxPayloadSize {
		return nil, fmt.Errorf("payload size %d exceeds max %d", len(payload), MaxPayloadSize)
	}

	frame := make([]byte, RecordHeaderSize+len(payload))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload)))
	frame[4] = recType
	frame[5] = RecordFormatVersion
	binary.BigEndian.PutUint64(frame[6:14], sequence)
	copy(frame[18:], payload)

	// CRC32 computed over header [0:14] + payload
	h := crc32.NewIEEE()
	h.Write(frame[0:14])
	h.Write(payload)
	checksum := h.Sum32()

	binary.BigEndian.PutUint32(frame[14:18], checksum)
	return frame, nil
}

// ReadRecord reads and validates one framed record from r.
// Returns:
// - record, bytesRead, nil on success.
// - nil, 0, io.EOF on clean end of file.
// - nil, bytesRead, io.ErrUnexpectedEOF if an incomplete frame was found at EOF (torn tail).
// - nil, bytesRead, ErrCorruptRecord if frame is corrupted.
func ReadRecord(r io.Reader) (*Record, int, error) {
	hdrBuf := make([]byte, RecordHeaderSize)
	n, err := io.ReadFull(r, hdrBuf)
	if err == io.EOF {
		return nil, 0, io.EOF
	}
	if err == io.ErrUnexpectedEOF {
		// Incomplete header at EOF (torn tail)
		return nil, n, io.ErrUnexpectedEOF
	}
	if err != nil {
		return nil, n, err
	}

	length := binary.BigEndian.Uint32(hdrBuf[0:4])
	recType := hdrBuf[4]
	version := hdrBuf[5]
	sequence := binary.BigEndian.Uint64(hdrBuf[6:14])
	expectedCRC := binary.BigEndian.Uint32(hdrBuf[14:18])

	if version != RecordFormatVersion {
		return nil, n, fmt.Errorf("%w: unsupported version %d", ErrCorruptRecord, version)
	}
	if recType < RecordTypeHardState || recType > RecordTypeSnapshotMeta {
		return nil, n, fmt.Errorf("%w: invalid record type %d", ErrCorruptRecord, recType)
	}
	if length > MaxPayloadSize {
		return nil, n, fmt.Errorf("%w: invalid payload length %d", ErrCorruptRecord, length)
	}

	payload := make([]byte, length)
	pn, err := io.ReadFull(r, payload)
	totalRead := n + pn
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		// Incomplete payload at EOF (torn tail)
		return nil, totalRead, io.ErrUnexpectedEOF
	}
	if err != nil {
		return nil, totalRead, err
	}

	// Verify checksum
	h := crc32.NewIEEE()
	h.Write(hdrBuf[0:14])
	h.Write(payload)
	actualCRC := h.Sum32()

	if actualCRC != expectedCRC {
		return nil, totalRead, fmt.Errorf("%w: CRC mismatch (expected %08x, got %08x)", ErrCorruptRecord, expectedCRC, actualCRC)
	}

	rec := &Record{
		Header: Header{
			Length:   length,
			Type:     recType,
			Version:  version,
			Sequence: sequence,
			CRC32:    expectedCRC,
		},
		Payload: payload,
	}
	return rec, totalRead, nil
}

// EncodeLogEntries serializes a slice of protocol.LogEntry.
func EncodeLogEntries(entries []protocol.LogEntry) ([]byte, error) {
	return json.Marshal(entries)
}

// DecodeLogEntries deserializes a slice of protocol.LogEntry.
func DecodeLogEntries(data []byte) ([]protocol.LogEntry, error) {
	var entries []protocol.LogEntry
	err := json.Unmarshal(data, &entries)
	return entries, err
}
