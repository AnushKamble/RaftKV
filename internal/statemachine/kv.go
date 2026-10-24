package statemachine

import (
	"encoding/json"
	"sync"

	"raftkv/internal/protocol"
)

// SnapshotPayload defines the serialized representation of the state machine.
type SnapshotPayload struct {
	Data        map[string]string        `json:"data"`
	Sessions    map[string]ClientSession `json:"sessions"`
	LastApplied protocol.Index           `json:"last_applied"`
}

// KVStateMachine implements a deterministic replicated key-value state machine.
type KVStateMachine struct {
	mu          sync.RWMutex
	data        map[string]string
	dedupe      *DedupeTable
	lastApplied protocol.Index
}

// NewKVStateMachine creates a new initialized state machine.
func NewKVStateMachine() *KVStateMachine {
	return &KVStateMachine{
		data:   make(map[string]string),
		dedupe: NewDedupeTable(),
	}
}

// Apply executes a committed log entry deterministically in log order.
func (sm *KVStateMachine) Apply(entry protocol.LogEntry) protocol.CommandResult {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	cmd := entry.Command
	payloadHash := cmd.PayloadHash()

	// Check deduplication table for client mutations
	if cmd.ClientID != "" && cmd.RequestID > 0 {
		cachedResult, err := sm.dedupe.Check(cmd.ClientID, cmd.RequestID, payloadHash)
		if err == ErrPayloadMismatch {
			return protocol.CommandResult{
				Success:     false,
				ErrorCode:   "PAYLOAD_MISMATCH",
				CommitIndex: entry.Index,
			}
		}
		if cachedResult != nil {
			// Duplicate retry: return original result without re-applying mutation
			res := *cachedResult
			res.CommitIndex = entry.Index
			sm.lastApplied = entry.Index
			return res
		}
	}

	var res protocol.CommandResult
	res.CommitIndex = entry.Index
	res.Success = true

	switch cmd.Op {
	case protocol.OpPut:
		sm.data[cmd.Key] = cmd.Value

	case protocol.OpGet:
		val, found := sm.data[cmd.Key]
		res.Value = val
		res.Found = found

	case protocol.OpDelete:
		_, found := sm.data[cmd.Key]
		if found {
			delete(sm.data, cmd.Key)
		}
		res.Found = found

	case protocol.OpCAS:
		curVal, found := sm.data[cmd.Key]
		if !found {
			if cmd.ExpectedValue == "" {
				// Key does not exist and expected is empty -> swap succeeds
				sm.data[cmd.Key] = cmd.Value
				res.Swapped = true
				res.Value = cmd.Value
			} else {
				res.Swapped = false
				res.Value = ""
			}
		} else {
			if curVal == cmd.ExpectedValue {
				sm.data[cmd.Key] = cmd.Value
				res.Swapped = true
				res.Value = cmd.Value
			} else {
				res.Swapped = false
				res.Value = curVal
			}
		}

	default:
		res.Success = false
		res.ErrorCode = "UNKNOWN_OP"
	}

	// Record mutation in deduplication table
	if cmd.ClientID != "" && cmd.RequestID > 0 {
		sm.dedupe.Record(cmd.ClientID, cmd.RequestID, payloadHash, res)
	}

	sm.lastApplied = entry.Index
	return res
}

// LastApplied returns the highest index applied to the state machine.
func (sm *KVStateMachine) LastApplied() protocol.Index {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.lastApplied
}

// GetDirect reads directly from the state machine map (used for verification/testing).
func (sm *KVStateMachine) GetDirect(key string) (string, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	val, ok := sm.data[key]
	return val, ok
}

// Snapshot serializes the current state machine and dedupe records to JSON bytes.
func (sm *KVStateMachine) Snapshot() ([]byte, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	dataCopy := make(map[string]string, len(sm.data))
	for k, v := range sm.data {
		dataCopy[k] = v
	}

	payload := SnapshotPayload{
		Data:        dataCopy,
		Sessions:    sm.dedupe.Clone(),
		LastApplied: sm.lastApplied,
	}

	return json.Marshal(payload)
}

// Restore resets the state machine and restores from snapshot bytes.
func (sm *KVStateMachine) Restore(snapshotBytes []byte) error {
	var payload SnapshotPayload
	if err := json.Unmarshal(snapshotBytes, &payload); err != nil {
		return ErrCorruptSnapshot
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.data = make(map[string]string, len(payload.Data))
	for k, v := range payload.Data {
		sm.data[k] = v
	}

	sm.dedupe.Restore(payload.Sessions)
	sm.lastApplied = payload.LastApplied
	return nil
}
