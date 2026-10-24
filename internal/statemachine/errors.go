package statemachine

import "errors"

var (
	// ErrPayloadMismatch indicates a client reused a request ID with different command payload.
	ErrPayloadMismatch = errors.New("statemachine: request ID reused with mismatched payload")

	// ErrCorruptSnapshot indicates snapshot data failed deserialization or checksum.
	ErrCorruptSnapshot = errors.New("statemachine: corrupt snapshot payload")
)
