package raft

import "errors"

var (
	// ErrNotLeader is returned when a non-leader node receives a client mutation.
	ErrNotLeader = errors.New("raft: node is not the leader")

	// ErrStopped indicates the Raft node is stopped.
	ErrStopped = errors.New("raft: node is stopped")
)
