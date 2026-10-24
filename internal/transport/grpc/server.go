package grpc

import (
	"context"

	raftkvv1 "raftkv/gen/raftkv/v1"
	"raftkv/internal/protocol"
	"raftkv/internal/raft"
)

// NodeRuntime exposes the Raft node methods required by the gRPC server adapters.
type NodeRuntime interface {
	RequestVote(ctx context.Context, args *raftkvv1.RequestVoteArgs) (*raftkvv1.RequestVoteReply, error)
	AppendEntries(ctx context.Context, args *raftkvv1.AppendEntriesArgs) (*raftkvv1.AppendEntriesReply, error)
	InstallSnapshot(ctx context.Context, args *raftkvv1.InstallSnapshotArgs) (*raftkvv1.InstallSnapshotReply, error)
	ExecuteCommand(ctx context.Context, cmd protocol.Command) (*protocol.CommandResult, string, error)
	GetStatus(ctx context.Context) (raft.Status, error)
}

// Server implements RaftService and KVService gRPC interfaces.
type Server struct {
	raftkvv1.UnimplementedRaftServiceServer
	raftkvv1.UnimplementedKVServiceServer
	node NodeRuntime
}

// NewServer wraps a NodeRuntime with gRPC services.
func NewServer(node NodeRuntime) *Server {
	return &Server{node: node}
}

// --- RaftService Methods ---

func (s *Server) RequestVote(ctx context.Context, args *raftkvv1.RequestVoteArgs) (*raftkvv1.RequestVoteReply, error) {
	return s.node.RequestVote(ctx, args)
}

func (s *Server) AppendEntries(ctx context.Context, args *raftkvv1.AppendEntriesArgs) (*raftkvv1.AppendEntriesReply, error) {
	return s.node.AppendEntries(ctx, args)
}

func (s *Server) InstallSnapshot(ctx context.Context, args *raftkvv1.InstallSnapshotArgs) (*raftkvv1.InstallSnapshotReply, error) {
	return s.node.InstallSnapshot(ctx, args)
}

// --- KVService Methods ---

func (s *Server) Put(ctx context.Context, req *raftkvv1.PutRequest) (*raftkvv1.PutReply, error) {
	cmd := protocol.Command{
		ClientID:  req.ClientId,
		RequestID: req.RequestId,
		Op:        protocol.OpPut,
		Key:       req.Key,
		Value:     req.Value,
	}

	res, leaderHint, err := s.node.ExecuteCommand(ctx, cmd)
	if err == raft.ErrNotLeader {
		return &raftkvv1.PutReply{
			Success:    false,
			ErrorCode:  "NOT_LEADER",
			LeaderHint: leaderHint,
		}, nil
	}
	if err != nil {
		return &raftkvv1.PutReply{
			Success:   false,
			ErrorCode: err.Error(),
		}, nil
	}

	return &raftkvv1.PutReply{
		Success:     res.Success,
		ErrorCode:   res.ErrorCode,
		CommitIndex: uint64(res.CommitIndex),
	}, nil
}

func (s *Server) Get(ctx context.Context, req *raftkvv1.GetRequest) (*raftkvv1.GetReply, error) {
	cmd := protocol.Command{
		ClientID:  req.ClientId,
		RequestID: req.RequestId,
		Op:        protocol.OpGet,
		Key:       req.Key,
	}

	res, leaderHint, err := s.node.ExecuteCommand(ctx, cmd)
	if err == raft.ErrNotLeader {
		return &raftkvv1.GetReply{
			Success:    false,
			ErrorCode:  "NOT_LEADER",
			LeaderHint: leaderHint,
		}, nil
	}
	if err != nil {
		return &raftkvv1.GetReply{
			Success:   false,
			ErrorCode: err.Error(),
		}, nil
	}

	return &raftkvv1.GetReply{
		Success:     res.Success,
		ErrorCode:   res.ErrorCode,
		Value:       res.Value,
		Found:       res.Found,
		CommitIndex: uint64(res.CommitIndex),
	}, nil
}

func (s *Server) Delete(ctx context.Context, req *raftkvv1.DeleteRequest) (*raftkvv1.DeleteReply, error) {
	cmd := protocol.Command{
		ClientID:  req.ClientId,
		RequestID: req.RequestId,
		Op:        protocol.OpDelete,
		Key:       req.Key,
	}

	res, leaderHint, err := s.node.ExecuteCommand(ctx, cmd)
	if err == raft.ErrNotLeader {
		return &raftkvv1.DeleteReply{
			Success:    false,
			ErrorCode:  "NOT_LEADER",
			LeaderHint: leaderHint,
		}, nil
	}
	if err != nil {
		return &raftkvv1.DeleteReply{
			Success:   false,
			ErrorCode: err.Error(),
		}, nil
	}

	return &raftkvv1.DeleteReply{
		Success:     res.Success,
		ErrorCode:   res.ErrorCode,
		Found:       res.Found,
		CommitIndex: uint64(res.CommitIndex),
	}, nil
}

func (s *Server) CompareAndSwap(ctx context.Context, req *raftkvv1.CompareAndSwapRequest) (*raftkvv1.CompareAndSwapReply, error) {
	cmd := protocol.Command{
		ClientID:      req.ClientId,
		RequestID:     req.RequestId,
		Op:            protocol.OpCAS,
		Key:           req.Key,
		ExpectedValue: req.ExpectedValue,
		Value:         req.NewValue,
	}

	res, leaderHint, err := s.node.ExecuteCommand(ctx, cmd)
	if err == raft.ErrNotLeader {
		return &raftkvv1.CompareAndSwapReply{
			Success:    false,
			ErrorCode:  "NOT_LEADER",
			LeaderHint: leaderHint,
		}, nil
	}
	if err != nil {
		return &raftkvv1.CompareAndSwapReply{
			Success:   false,
			ErrorCode: err.Error(),
		}, nil
	}

	return &raftkvv1.CompareAndSwapReply{
		Success:     res.Success,
		ErrorCode:   res.ErrorCode,
		Swapped:     res.Swapped,
		Value:       res.Value,
		CommitIndex: uint64(res.CommitIndex),
	}, nil
}

func (s *Server) GetStatus(ctx context.Context, _ *raftkvv1.GetStatusRequest) (*raftkvv1.GetStatusReply, error) {
	st, err := s.node.GetStatus(ctx)
	if err != nil {
		return nil, err
	}

	peerMatches := make(map[string]uint64)
	for k, v := range st.PeerMatchIndices {
		peerMatches[k] = uint64(v)
	}

	return &raftkvv1.GetStatusReply{
		NodeId:           string(st.ID),
		Role:             string(st.Role),
		Term:             uint64(st.Term),
		LeaderId:         string(st.LeaderID),
		LastLogIndex:     uint64(st.LastLogIndex),
		LastLogTerm:      uint64(st.LastLogTerm),
		CommitIndex:      uint64(st.CommitIndex),
		LastApplied:      uint64(st.LastApplied),
		SnapshotIndex:    uint64(st.SnapshotIndex),
		PeerMatchIndices: peerMatches,
	}, nil
}
