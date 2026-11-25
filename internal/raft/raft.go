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
	mu           sync.Mutex // only for external status queries if needed, state loop owns mutable state
	id           protocol.NodeID
	peers        []protocol.NodeID
	storage      storage.Storage
	transport    transport.Transport
	clock        transport.Clock
	stateMachine StateMachine

	minElectionTimeout time.Duration
	maxElectionTimeout time.Duration
	heartbeatInterval  time.Duration

	// Protocol state owned exclusively by loop()
	currentTerm   protocol.Term
	votedFor      string
	role          protocol.Role
	leaderID      protocol.NodeID
	votesReceived map[string]bool
	ackedPeers    map[string]bool

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

	var idSeed int64
	for _, b := range []byte(cfg.ID) {
		idSeed = (idSeed * 31) + int64(b)
	}

	r := &Raft{
		id:                 cfg.ID,
		peers:              cfg.Peers,
		storage:            cfg.Storage,
		transport:          cfg.Transport,
		clock:              cfg.Clock,
		stateMachine:       cfg.StateMachine,
		minElectionTimeout: cfg.MinElectionTimeout,
		maxElectionTimeout: cfg.MaxElectionTimeout,
		heartbeatInterval:  cfg.HeartbeatInterval,
		currentTerm:        term,
		votedFor:           votedFor,
		role:               protocol.RoleFollower,
		leaderID:           "",
		votesReceived:      make(map[string]bool),
		ackedPeers:         make(map[string]bool),
		nextIndex:          make(map[string]protocol.Index),
		matchIndex:         make(map[string]protocol.Index),
		applyChan:          cfg.ApplyChan,
		mailbox:            make(chan Event, 1000),
		stopCh:             make(chan struct{}),
		rnd:                rand.New(rand.NewSource(time.Now().UnixNano() ^ (idSeed << 24))),
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

	case snapshotRequestEvent:
		select {
		case e.replyCh <- r.handleSnapshotRequest(e.compactIndex):
		default:
		}

	case clientCommandEvent:
		r.handleClientCommand(e)

	case proposeEvent:
		r.handlePropose(e)

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
	r.ackedPeers = make(map[string]bool)
	r.resetElectionTimeout() // Keep running for CheckQuorum (§6.2)

	lastIdx, _, _ := r.storage.LastIndexAndTerm()
	for _, p := range r.peers {
		r.nextIndex[string(p)] = lastIdx + 1
		r.matchIndex[string(p)] = 0
	}

	r.heartbeatTimer.Reset(r.heartbeatInterval)
	r.broadcastAppendEntries()
}

func (r *Raft) handlePropose(e proposeEvent) {
	if r.role != protocol.RoleLeader {
		e.replyCh <- proposeReply{err: ErrNotLeader}
		return
	}

	lastIdx, _, _ := r.storage.LastIndexAndTerm()
	newIdx := lastIdx + 1
	entry := protocol.LogEntry{
		Index:   newIdx,
		Term:    r.currentTerm,
		Command: e.cmd,
	}

	if err := r.storage.AppendEntries([]protocol.LogEntry{entry}); err != nil {
		e.replyCh <- proposeReply{err: err}
		return
	}

	r.broadcastAppendEntries()
	e.replyCh <- proposeReply{index: newIdx, term: r.currentTerm, err: nil}
}

// Propose appends a client command to the Raft log on the leader and triggers replication.
func (r *Raft) Propose(ctx context.Context, cmd protocol.Command) (protocol.Index, protocol.Term, error) {
	replyCh := make(chan proposeReply, 1)
	r.PostEvent(proposeEvent{cmd: cmd, replyCh: replyCh})
	select {
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	case <-r.stopCh:
		return 0, 0, ErrStopped
	case res := <-replyCh:
		return res.index, res.term, res.err
	}
}

// PostEvent enqueues an event into the actor's mailbox.
func (r *Raft) PostEvent(ev Event) {
	select {
	case r.mailbox <- ev:
	case <-r.stopCh:
	}
}

// TakeSnapshot triggers state machine snapshot creation and WAL compaction up to compactIndex.
func (r *Raft) TakeSnapshot(compactIndex protocol.Index) error {
	replyCh := make(chan error, 1)
	r.PostEvent(snapshotRequestEvent{compactIndex: compactIndex, replyCh: replyCh})
	select {
	case <-r.stopCh:
		return ErrStopped
	case err := <-replyCh:
		return err
	}
}

func (r *Raft) handleSnapshotRequest(compactIndex protocol.Index) error {
	if compactIndex > r.lastApplied {
		return fmt.Errorf("cannot snapshot beyond lastApplied: %d > %d", compactIndex, r.lastApplied)
	}
	if r.stateMachine == nil {
		return fmt.Errorf("state machine not configured")
	}

	snapData, err := r.stateMachine.Snapshot()
	if err != nil {
		return fmt.Errorf("failed to take state machine snapshot: %w", err)
	}

	compactTerm, err := r.storage.Term(compactIndex)
	if err != nil {
		return fmt.Errorf("failed to get term for snapshot index %d: %w", compactIndex, err)
	}

	meta := protocol.SnapshotMeta{
		LastIncludedIndex: compactIndex,
		LastIncludedTerm:  compactTerm,
		FormatVersion:     1,
	}

	if err := r.storage.SaveSnapshot(meta, snapData); err != nil {
		return fmt.Errorf("failed to save snapshot: %w", err)
	}

	if err := r.storage.CompactLog(compactIndex, compactTerm); err != nil {
		return fmt.Errorf("failed to compact log: %w", err)
	}

	return nil
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
