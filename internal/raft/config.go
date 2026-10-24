package raft

import (
	"fmt"
	"time"

	"raftkv/internal/protocol"
	"raftkv/internal/storage"
	"raftkv/internal/transport"
)

// StateMachine abstracts the replicated state machine for snapshots and application.
type StateMachine interface {
	Apply(entry protocol.LogEntry) protocol.CommandResult
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}

// Config encapsulates configuration for a Raft node instance.
type Config struct {
	ID                  protocol.NodeID
	Peers               []protocol.NodeID
	Storage             storage.Storage
	Transport           transport.Transport
	Clock               transport.Clock
	StateMachine        StateMachine
	MinElectionTimeout  time.Duration
	MaxElectionTimeout  time.Duration
	HeartbeatInterval   time.Duration
	ApplyChan           chan protocol.LogEntry
}

// Validate ensures all required configuration options are provided.
func (c *Config) Validate() error {
	if c.ID == "" {
		return fmt.Errorf("node ID must not be empty")
	}
	if c.Storage == nil {
		return fmt.Errorf("storage engine must not be nil")
	}
	if c.Transport == nil {
		return fmt.Errorf("transport must not be nil")
	}
	if c.Clock == nil {
		c.Clock = transport.NewRealClock()
	}
	if c.MinElectionTimeout <= 0 {
		c.MinElectionTimeout = 150 * time.Millisecond
	}
	if c.MaxElectionTimeout <= c.MinElectionTimeout {
		c.MaxElectionTimeout = 300 * time.Millisecond
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 50 * time.Millisecond
	}
	return nil
}
