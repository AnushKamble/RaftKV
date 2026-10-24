package raft

import (
	raftkvv1 "raftkv/gen/raftkv/v1"
	"raftkv/internal/protocol"
)

type Event interface {
	isEvent()
}

type electionTimeoutEvent struct{}

func (electionTimeoutEvent) isEvent() {}

type heartbeatTimeoutEvent struct{}

func (heartbeatTimeoutEvent) isEvent() {}

type requestVoteEvent struct {
	args    *raftkvv1.RequestVoteArgs
	replyCh chan *raftkvv1.RequestVoteReply
}

func (requestVoteEvent) isEvent() {}

type requestVoteReplyEvent struct {
	from        string
	term        protocol.Term
	voteGranted bool
}

func (requestVoteReplyEvent) isEvent() {}

type appendEntriesEvent struct {
	args    *raftkvv1.AppendEntriesArgs
	replyCh chan *raftkvv1.AppendEntriesReply
}

func (appendEntriesEvent) isEvent() {}

type appendEntriesReplyEvent struct {
	from          string
	term          protocol.Term
	success       bool
	matchIndex    protocol.Index
	conflictIndex protocol.Index
	conflictTerm  protocol.Term
}

func (appendEntriesReplyEvent) isEvent() {}

type installSnapshotEvent struct {
	args    *raftkvv1.InstallSnapshotArgs
	replyCh chan *raftkvv1.InstallSnapshotReply
}

func (installSnapshotEvent) isEvent() {}

type installSnapshotReplyEvent struct {
	from              string
	term              protocol.Term
	lastIncludedIndex protocol.Index
}

func (installSnapshotReplyEvent) isEvent() {}

type snapshotRequestEvent struct {
	compactIndex protocol.Index
	replyCh      chan error
}

func (snapshotRequestEvent) isEvent() {}

type clientCommandEvent struct {
	cmd     protocol.Command
	replyCh chan protocol.CommandResult
}

func (clientCommandEvent) isEvent() {}

type proposeEvent struct {
	cmd     protocol.Command
	replyCh chan proposeReply
}

func (proposeEvent) isEvent() {}

type proposeReply struct {
	index protocol.Index
	term  protocol.Term
	err   error
}

type statusRequestEvent struct {
	replyCh chan Status
}

func (statusRequestEvent) isEvent() {}
