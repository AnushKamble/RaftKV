// Package protocol defines the core domain types, roles, and commands for RaftKV.
package protocol

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

// NodeID uniquely identifies a node in the Raft cluster.
type NodeID string

// Term represents a Raft logical term counter.
type Term uint64

// Index represents a Raft logical log index.
type Index uint64

// Role represents the Raft consensus role of a node.
type Role string

const (
	RoleFollower  Role = "Follower"
	RoleCandidate Role = "Candidate"
	RoleLeader    Role = "Leader"
)

// OpType denotes the state machine operation type.
type OpType string

const (
	OpPut    OpType = "PUT"
	OpGet    OpType = "GET"
	OpDelete OpType = "DELETE"
	OpCAS    OpType = "CAS"
)

// Command represents a state machine command submitted by a client.
type Command struct {
	ClientID      string `json:"client_id"`
	RequestID     uint64 `json:"request_id"`
	Op            OpType `json:"op"`
	Key           string `json:"key"`
	Value         string `json:"value,omitempty"`
	ExpectedValue string `json:"expected_value,omitempty"`
}

// PayloadHash returns a deterministic cryptographic digest of the command payload.
func (c Command) PayloadHash() string {
	data := fmt.Sprintf("%s:%s:%s:%s", c.Op, c.Key, c.Value, c.ExpectedValue)
	h := sha256.Sum256([]byte(data))
	return fmt.Sprintf("%x", h)
}

// Encode serializes the command to JSON bytes.
func (c Command) Encode() ([]byte, error) {
	return json.Marshal(c)
}

// DecodeCommand deserializes a command from JSON bytes.
func DecodeCommand(data []byte) (Command, error) {
	var c Command
	err := json.Unmarshal(data, &c)
	return c, err
}

// LogEntry represents an entry stored in the Raft consensus log.
type LogEntry struct {
	Index   Index   `json:"index"`
	Term    Term    `json:"term"`
	Command Command `json:"command"`
}

// CommandResult represents the outcome of executing a command on the KV state machine.
type CommandResult struct {
	Success     bool   `json:"success"`
	ErrorCode   string `json:"error_code,omitempty"`
	Value       string `json:"value,omitempty"`
	Found       bool   `json:"found,omitempty"`
	Swapped     bool   `json:"swapped,omitempty"`
	CommitIndex Index  `json:"commit_index"`
}

// SnapshotMeta stores metadata about a compacted snapshot.
type SnapshotMeta struct {
	LastIncludedIndex Index  `json:"last_included_index"`
	LastIncludedTerm  Term   `json:"last_included_term"`
	FormatVersion     uint8  `json:"format_version"`
	DataChecksum      uint32 `json:"data_checksum"`
}
