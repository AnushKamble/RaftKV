package statemachine

import (
	"sync"

	"raftkv/internal/protocol"
)

// ClientSession records deduplication state for a single client.
type ClientSession struct {
	LastRequestID   uint64                 `json:"last_request_id"`
	LastPayloadHash string                 `json:"last_payload_hash"`
	LastResult      protocol.CommandResult `json:"last_result"`
}

// DedupeTable maintains per-client idempotency records.
type DedupeTable struct {
	mu       sync.RWMutex
	Sessions map[string]ClientSession `json:"sessions"`
}

// NewDedupeTable constructs an empty deduplication table.
func NewDedupeTable() *DedupeTable {
	return &DedupeTable{
		Sessions: make(map[string]ClientSession),
	}
}

// Check inspects whether a request is a known duplicate or payload mismatch.
// Returns:
// - (*result, nil) if identical duplicate retry.
// - (nil, ErrPayloadMismatch) if request ID reused with mismatched payload.
// - (nil, nil) if request is fresh and should be applied.
func (dt *DedupeTable) Check(clientID string, requestID uint64, payloadHash string) (*protocol.CommandResult, error) {
	dt.mu.RLock()
	defer dt.mu.RUnlock()

	session, exists := dt.Sessions[clientID]
	if !exists {
		return nil, nil
	}

	if requestID == session.LastRequestID {
		if payloadHash == session.LastPayloadHash {
			return &session.LastResult, nil
		}
		return nil, ErrPayloadMismatch
	}

	if requestID < session.LastRequestID {
		// Old request ID already superseded
		return nil, ErrPayloadMismatch
	}

	return nil, nil
}

// Record updates the client session with the executed command's result.
func (dt *DedupeTable) Record(clientID string, requestID uint64, payloadHash string, res protocol.CommandResult) {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	dt.Sessions[clientID] = ClientSession{
		LastRequestID:   requestID,
		LastPayloadHash: payloadHash,
		LastResult:      res,
	}
}

// Clone creates a detached deep copy of sessions for snapshotting.
func (dt *DedupeTable) Clone() map[string]ClientSession {
	dt.mu.RLock()
	defer dt.mu.RUnlock()

	copyMap := make(map[string]ClientSession, len(dt.Sessions))
	for k, v := range dt.Sessions {
		copyMap[k] = v
	}
	return copyMap
}

// Restore populates sessions from a deserialized snapshot.
func (dt *DedupeTable) Restore(sessions map[string]ClientSession) {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	dt.Sessions = make(map[string]ClientSession, len(sessions))
	for k, v := range sessions {
		dt.Sessions[k] = v
	}
}
