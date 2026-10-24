package grpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	raftkvv1 "raftkv/gen/raftkv/v1"
	"raftkv/internal/transport"
)

// GRPCTransport implements transport.Transport using inter-node gRPC connections.
type GRPCTransport struct {
	mu        sync.RWMutex
	peerAddrs map[string]string // peerID -> host:port
	conns     map[string]*grpc.ClientConn
	clients   map[string]raftkvv1.RaftServiceClient
}

var _ transport.Transport = (*GRPCTransport)(nil)

// NewGRPCTransport creates a gRPC client transport for communicating with peers.
func NewGRPCTransport(peerAddrs map[string]string) *GRPCTransport {
	return &GRPCTransport{
		peerAddrs: peerAddrs,
		conns:     make(map[string]*grpc.ClientConn),
		clients:   make(map[string]raftkvv1.RaftServiceClient),
	}
}

func (gt *GRPCTransport) getClient(peerID string) (raftkvv1.RaftServiceClient, error) {
	gt.mu.RLock()
	client, exists := gt.clients[peerID]
	gt.mu.RUnlock()
	if exists {
		return client, nil
	}

	gt.mu.Lock()
	defer gt.mu.Unlock()

	// Double check
	if client, exists := gt.clients[peerID]; exists {
		return client, nil
	}

	addr, ok := gt.peerAddrs[peerID]
	if !ok {
		return nil, fmt.Errorf("unknown peer ID: %s", peerID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		// Non-blocking fallback if peer is temporarily down
		conn, err = grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("failed to dial peer %s at %s: %w", peerID, addr, err)
		}
	}

	gt.conns[peerID] = conn
	client = raftkvv1.NewRaftServiceClient(conn)
	gt.clients[peerID] = client
	return client, nil
}

func (gt *GRPCTransport) SendRequestVote(ctx context.Context, target string, args *raftkvv1.RequestVoteArgs) (*raftkvv1.RequestVoteReply, error) {
	client, err := gt.getClient(target)
	if err != nil {
		return nil, err
	}
	return client.RequestVote(ctx, args)
}

func (gt *GRPCTransport) SendAppendEntries(ctx context.Context, target string, args *raftkvv1.AppendEntriesArgs) (*raftkvv1.AppendEntriesReply, error) {
	client, err := gt.getClient(target)
	if err != nil {
		return nil, err
	}
	return client.AppendEntries(ctx, args)
}

func (gt *GRPCTransport) SendInstallSnapshot(ctx context.Context, target string, args *raftkvv1.InstallSnapshotArgs) (*raftkvv1.InstallSnapshotReply, error) {
	client, err := gt.getClient(target)
	if err != nil {
		return nil, err
	}
	return client.InstallSnapshot(ctx, args)
}

func (gt *GRPCTransport) Close() error {
	gt.mu.Lock()
	defer gt.mu.Unlock()

	for id, conn := range gt.conns {
		_ = conn.Close()
		delete(gt.conns, id)
		delete(gt.clients, id)
	}
	return nil
}
