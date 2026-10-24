package runtime

import (
	"context"
	"fmt"
	"net"
	"sync"

	"google.golang.org/grpc"

	raftkvv1 "raftkv/gen/raftkv/v1"
	"raftkv/internal/config"
	"raftkv/internal/protocol"
	"raftkv/internal/raft"
	"raftkv/internal/statemachine"
	"raftkv/internal/storage"
	grpcTransport "raftkv/internal/transport/grpc"
)

// Node represents an active RaftKV node running storage, consensus, and gRPC services.
type Node struct {
	mu           sync.Mutex
	cfg          *config.ParsedNodeConfig
	storage      *storage.DiskStorage
	stateMachine *statemachine.KVStateMachine
	raft         *raft.Raft
	transport    *grpcTransport.GRPCTransport
	grpcServer   *grpc.Server
	listener     net.Listener

	applyChan chan protocol.LogEntry
	waiters   map[protocol.Index]chan protocol.CommandResult
	stopCh    chan struct{}
}

// NewNode initializes a Node from parsed configuration.
func NewNode(cfg *config.ParsedNodeConfig) (*Node, error) {
	s, err := storage.OpenDiskStorage(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to open storage in %s: %w", cfg.DataDir, err)
	}

	sm := statemachine.NewKVStateMachine()

	// Load snapshot into state machine if one exists
	snapMeta, snapData, err := s.LoadSnapshot()
	if err == nil && snapMeta != nil && snapData != nil {
		if err := sm.Restore(snapData); err != nil {
			_ = s.Close()
			return nil, fmt.Errorf("failed to restore snapshot into state machine: %w", err)
		}
	}

	trans := grpcTransport.NewGRPCTransport(cfg.Peers)

	var peerIDs []protocol.NodeID
	for id := range cfg.Peers {
		peerIDs = append(peerIDs, protocol.NodeID(id))
	}

	applyChan := make(chan protocol.LogEntry, 1000)

	raftCfg := raft.Config{
		ID:                 protocol.NodeID(cfg.ID),
		Peers:              peerIDs,
		Storage:            s,
		Transport:          trans,
		StateMachine:       sm,
		MinElectionTimeout: cfg.MinElectionTimeout,
		MaxElectionTimeout: cfg.MaxElectionTimeout,
		HeartbeatInterval:  cfg.HeartbeatInterval,
		ApplyChan:          applyChan,
	}

	r, err := raft.NewRaft(raftCfg)
	if err != nil {
		_ = s.Close()
		_ = trans.Close()
		return nil, fmt.Errorf("failed to create Raft instance: %w", err)
	}

	return &Node{
		cfg:          cfg,
		storage:      s,
		stateMachine: sm,
		raft:         r,
		transport:    trans,
		applyChan:    applyChan,
		waiters:      make(map[protocol.Index]chan protocol.CommandResult),
		stopCh:       make(chan struct{}),
	}, nil
}

// Start opens the network listener, registers gRPC endpoints, and launches background loops.
func (n *Node) Start() error {
	lis, err := net.Listen("tcp", n.cfg.Address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", n.cfg.Address, err)
	}
	n.listener = lis

	n.grpcServer = grpc.NewServer()
	srv := grpcTransport.NewServer(n)
	raftkvv1.RegisterRaftServiceServer(n.grpcServer, srv)
	raftkvv1.RegisterKVServiceServer(n.grpcServer, srv)

	n.raft.Start()
	go n.applyLoop()
	go func() {
		_ = n.grpcServer.Serve(n.listener)
	}()

	return nil
}

// Stop cleanly terminates the node, closing network and storage.
func (n *Node) Stop() {
	n.mu.Lock()
	select {
	case <-n.stopCh:
		n.mu.Unlock()
		return
	default:
		close(n.stopCh)
	}

	// Cancel all pending waiters
	for idx, ch := range n.waiters {
		select {
		case ch <- protocol.CommandResult{Success: false, ErrorCode: "NODE_STOPPED"}:
		default:
		}
		delete(n.waiters, idx)
	}
	n.mu.Unlock()

	n.raft.Stop()
	if n.grpcServer != nil {
		n.grpcServer.GracefulStop()
	}
	if n.listener != nil {
		_ = n.listener.Close()
	}
	_ = n.transport.Close()
	_ = n.storage.Close()
}

func (n *Node) applyLoop() {
	for {
		select {
		case <-n.stopCh:
			return
		case entry := <-n.applyChan:
			res := n.stateMachine.Apply(entry)

			// Notify client waiter if one is registered
			n.mu.Lock()
			ch, exists := n.waiters[entry.Index]
			if exists {
				select {
				case ch <- res:
				default:
				}
				delete(n.waiters, entry.Index)
			}
			n.mu.Unlock()
		}
	}
}

// ExecuteCommand proposes a command to the Raft consensus log and awaits committed application.
func (n *Node) ExecuteCommand(ctx context.Context, cmd protocol.Command) (*protocol.CommandResult, string, error) {
	st, err := n.raft.GetStatus(ctx)
	if err != nil {
		return nil, "", err
	}
	if st.Role != protocol.RoleLeader {
		return nil, string(st.LeaderID), raft.ErrNotLeader
	}

	idx, _, err := n.raft.Propose(ctx, cmd)
	if err != nil {
		if err == raft.ErrNotLeader {
			st, _ := n.raft.GetStatus(ctx)
			return nil, string(st.LeaderID), raft.ErrNotLeader
		}
		return nil, "", err
	}

	replyCh := make(chan protocol.CommandResult, 1)

	n.mu.Lock()
	n.waiters[idx] = replyCh
	n.mu.Unlock()

	// Ensure waiter is removed on timeout/cancellation to prevent memory leak
	defer func() {
		n.mu.Lock()
		delete(n.waiters, idx)
		n.mu.Unlock()
	}()

	select {
	case <-ctx.Done():
		return nil, "", ctx.Err()
	case <-n.stopCh:
		return nil, "", fmt.Errorf("node stopped")
	case res := <-replyCh:
		return &res, "", nil
	}
}

// RequestVote delegates to the inner Raft actor.
func (n *Node) RequestVote(ctx context.Context, args *raftkvv1.RequestVoteArgs) (*raftkvv1.RequestVoteReply, error) {
	return n.raft.RequestVote(ctx, args)
}

// AppendEntries delegates to the inner Raft actor.
func (n *Node) AppendEntries(ctx context.Context, args *raftkvv1.AppendEntriesArgs) (*raftkvv1.AppendEntriesReply, error) {
	return n.raft.AppendEntries(ctx, args)
}

// InstallSnapshot delegates to the inner Raft actor.
func (n *Node) InstallSnapshot(ctx context.Context, args *raftkvv1.InstallSnapshotArgs) (*raftkvv1.InstallSnapshotReply, error) {
	return n.raft.InstallSnapshot(ctx, args)
}

// GetStatus delegates to the inner Raft actor.
func (n *Node) GetStatus(ctx context.Context) (raft.Status, error) {
	return n.raft.GetStatus(ctx)
}

// Raft returns the inner Raft engine (used for testing).
func (n *Node) Raft() *raft.Raft {
	return n.raft
}

// StateMachine returns the inner KV state machine (used for testing).
func (n *Node) StateMachine() *statemachine.KVStateMachine {
	return n.stateMachine
}
