package raft

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	raftkvv1 "raftkv/gen/raftkv/v1"
	"raftkv/internal/protocol"
	"raftkv/internal/storage"
	"raftkv/internal/transport"
)

// Raft implements the core Raft consensus protocol using a single-owner event loop.
type Raft struct {
	mu        sync.Mutex // only for external status queries if needed, state loop owns mutable state
	id        protocol.NodeID
	peers     []protocol.NodeID
	storage   storage.Storage
	transport transport.Transport
	clock     transport.Clock

	minElectionTimeout time.Duration
	maxElectionTimeout time.Duration
	heartbeatInterval  time.Duration

	// Protocol state owned exclusively by loop()
	currentTerm   protocol.Term
	votedFor      string
	role          protocol.Role
	leaderID      protocol.NodeID
	votesReceived map[string]bool

	// Replication progress tracking (Leaders only)
	nextIndex  map[string]protocol.Index
	matchIndex map[string]protocol.Index

	commitIndex protocol.Index
	lastApplied protocol.Index

	applyChan chan protocol.LogEntry

	// Timers
	electionTimer  transport.Timer
	heartbeatTimer transport.Timer

	mailbox chan Event
	stopCh  chan struct{}
	rnd     *rand.Rand
}

// NewRaft constructs a Raft instance from config.
func NewRaft(cfg Config) (*Raft, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	term, votedFor, err := cfg.Storage.LoadHardState()
	if err != nil {
		return nil, fmt.Errorf("failed to load hard state: %w", err)
	}

	r := &Raft{
		id:                 cfg.ID,
		peers:              cfg.Peers,
		storage:            cfg.Storage,
		transport:          cfg.Transport,
		clock:              cfg.Clock,
		minElectionTimeout: cfg.MinElectionTimeout,
		maxElectionTimeout: cfg.MaxElectionTimeout,
		heartbeatInterval:  cfg.HeartbeatInterval,
		currentTerm:        term,
		votedFor:           votedFor,
		role:               protocol.RoleFollower,
		leaderID:           "",
		votesReceived:      make(map[string]bool),
		nextIndex:          make(map[string]protocol.Index),
		matchIndex:         make(map[string]protocol.Index),
		applyChan:          cfg.ApplyChan,
		mailbox:            make(chan Event, 1000),
		stopCh:             make(chan struct{}),
		rnd:                rand.New(rand.NewSource(time.Now().UnixNano() + int64(len(cfg.ID)))),
	}

	// Initialize commit and applied indices based on snapshot if available
	lastIdx, _, _ := cfg.Storage.LastIndexAndTerm()
	firstIdx, _ := cfg.Storage.FirstIndex()
	if firstIdx > 1 {
		r.commitIndex = firstIdx - 1
		r.lastApplied = firstIdx - 1
	}
	_ = lastIdx

	r.electionTimer = r.clock.NewTimer(r.randomElectionTimeout())
	r.heartbeatTimer = r.clock.NewTimer(r.heartbeatInterval)
	r.heartbeatTimer.Stop()

	return r, nil
}

// Start launches the Raft actor event loop in the background.
func (r *Raft) Start() {
	go r.loop()
}

// Stop cleanly terminates the Raft node.
func (r *Raft) Stop() {
	close(r.stopCh)
}

func (r *Raft) loop() {
	for {
		select {
		case <-r.stopCh:
			r.electionTimer.Stop()
			r.heartbeatTimer.Stop()
			return

		case <-r.electionTimer.C():
			r.handleElectionTimeout()

		case <-r.heartbeatTimer.C():
			r.handleHeartbeatTimeout()

		case ev := <-r.mailbox:
			r.handleEvent(ev)
		}
	}
}

func (r *Raft) handleEvent(ev Event) {
	switch e := ev.(type) {
	case requestVoteEvent:
		reply := r.handleRequestVote(e.args)
		select {
		case e.replyCh <- reply:
		default:
		}

	case requestVoteReplyEvent:
		r.handleRequestVoteReply(e)

	case appendEntriesEvent:
		reply := r.handleAppendEntries(e.args)
		select {
		case e.replyCh <- reply:
		default:
		}

	case appendEntriesReplyEvent:
		r.handleAppendEntriesReply(e)

	case installSnapshotEvent:
		reply := r.handleInstallSnapshot(e.args)
		select {
		case e.replyCh <- reply:
		default:
		}

	case installSnapshotReplyEvent:
		r.handleInstallSnapshotReply(e)

	case clientCommandEvent:
		r.handleClientCommand(e)

	case statusRequestEvent:
		select {
		case e.replyCh <- r.getStatus():
		default:
		}
	}
}

func (r *Raft) randomElectionTimeout() time.Duration {
	spread := int64(r.maxElectionTimeout - r.minElectionTimeout)
	if spread <= 0 {
		return r.minElectionTimeout
	}
	delta := time.Duration(r.rnd.Int63n(spread))
	return r.minElectionTimeout + delta
}

func (r *Raft) resetElectionTimeout() {
	r.electionTimer.Stop()
	r.electionTimer.Reset(r.randomElectionTimeout())
}

func (r *Raft) becomeFollower(term protocol.Term, leader protocol.NodeID) {
	if term > r.currentTerm {
		r.currentTerm = term
		r.votedFor = ""
		_ = r.storage.SaveHardState(r.currentTerm, r.votedFor)
	}
	r.role = protocol.RoleFollower
	r.leaderID = leader
	r.heartbeatTimer.Stop()
	r.resetElectionTimeout()
}

func (r *Raft) becomeLeader() {
	r.role = protocol.RoleLeader
	r.leaderID = r.id
	r.electionTimer.Stop()

	lastIdx, _, _ := r.storage.LastIndexAndTerm()
	for _, p := range r.peers {
		r.nextIndex[string(p)] = lastIdx + 1
		r.matchIndex[string(p)] = 0
	}

	r.heartbeatTimer.Reset(r.heartbeatInterval)
	r.broadcastAppendEntries()
}

// PostEvent enqueues an event into the actor's mailbox.
func (r *Raft) PostEvent(ev Event) {
	select {
	case r.mailbox <- ev:
	case <-r.stopCh:
	}
}

// GetStatus returns the current status snapshot synchronously.
func (r *Raft) GetStatus(ctx context.Context) (Status, error) {
	replyCh := make(chan Status, 1)
	r.PostEvent(statusRequestEvent{replyCh: replyCh})
	select {
	case <-ctx.Done():
		return Status{}, ctx.Err()
	case <-r.stopCh:
		return Status{}, fmt.Errorf("node stopped")
	case st := <-replyCh:
		return st, nil
	}
}

func (r *Raft) getStatus() Status {
	lastIdx, lastTerm, _ := r.storage.LastIndexAndTerm()
	firstIdx, _ := r.storage.FirstIndex()
	snapIdx := protocol.Index(0)
	if firstIdx > 1 {
		snapIdx = firstIdx - 1
	}

	matchIndices := make(map[string]protocol.Index)
	for k, v := range r.matchIndex {
		matchIndices[k] = v
	}

	return Status{
		ID:               r.id,
		Role:             r.role,
		Term:             r.currentTerm,
		LeaderID:         r.leaderID,
		LastLogIndex:     lastIdx,
		LastLogTerm:      lastTerm,
		CommitIndex:      r.commitIndex,
		LastApplied:      r.lastApplied,
		SnapshotIndex:    snapIdx,
		PeerMatchIndices: matchIndices,
	}
}

// RequestVote handles an inbound RequestVote RPC.
func (r *Raft) RequestVote(ctx context.Context, args *raftkvv1.RequestVoteArgs) (*raftkvv1.RequestVoteReply, error) {
	replyCh := make(chan *raftkvv1.RequestVoteReply, 1)
	r.PostEvent(requestVoteEvent{args: args, replyCh: replyCh})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.stopCh:
		return nil, fmt.Errorf("node stopped")
	case reply := <-replyCh:
		return reply, nil
	}
}

// AppendEntries handles an inbound AppendEntries RPC.
func (r *Raft) AppendEntries(ctx context.Context, args *raftkvv1.AppendEntriesArgs) (*raftkvv1.AppendEntriesReply, error) {
	replyCh := make(chan *raftkvv1.AppendEntriesReply, 1)
	r.PostEvent(appendEntriesEvent{args: args, replyCh: replyCh})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.stopCh:
		return nil, fmt.Errorf("node stopped")
	case reply := <-replyCh:
		return reply, nil
	}
}

// InstallSnapshot handles an inbound InstallSnapshot RPC.
func (r *Raft) InstallSnapshot(ctx context.Context, args *raftkvv1.InstallSnapshotArgs) (*raftkvv1.InstallSnapshotReply, error) {
	replyCh := make(chan *raftkvv1.InstallSnapshotReply, 1)
	r.PostEvent(installSnapshotEvent{args: args, replyCh: replyCh})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.stopCh:
		return nil, fmt.Errorf("node stopped")
	case reply := <-replyCh:
		return reply, nil
	}
}

// RPCHandlers interface implementation for simulator and transport adapters
func (r *Raft) HandleRequestVote(ctx context.Context, args *raftkvv1.RequestVoteArgs) (*raftkvv1.RequestVoteReply, error) {
	return r.RequestVote(ctx, args)
}

func (r *Raft) HandleAppendEntries(ctx context.Context, args *raftkvv1.AppendEntriesArgs) (*raftkvv1.AppendEntriesReply, error) {
	return r.AppendEntries(ctx, args)
}

func (r *Raft) HandleInstallSnapshot(ctx context.Context, args *raftkvv1.InstallSnapshotArgs) (*raftkvv1.InstallSnapshotReply, error) {
	return r.InstallSnapshot(ctx, args)
}
