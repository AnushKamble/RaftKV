package raft

import (
	"raftkv/internal/protocol"
)

// Status provides an immutable snapshot of node consensus state.
type Status struct {
	ID               protocol.NodeID
	Role             protocol.Role
	Term             protocol.Term
	LeaderID         protocol.NodeID
	LastLogIndex     protocol.Index
	LastLogTerm      protocol.Term
	CommitIndex      protocol.Index
	LastApplied      protocol.Index
	SnapshotIndex    protocol.Index
	PeerMatchIndices map[string]protocol.Index
}
