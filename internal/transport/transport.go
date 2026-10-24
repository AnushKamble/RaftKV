package transport

import (
	"context"

	raftkvv1 "raftkv/gen/raftkv/v1"
)

// Transport defines inter-node communication for Raft consensus RPCs.
type Transport interface {
	SendRequestVote(ctx context.Context, target string, args *raftkvv1.RequestVoteArgs) (*raftkvv1.RequestVoteReply, error)
	SendAppendEntries(ctx context.Context, target string, args *raftkvv1.AppendEntriesArgs) (*raftkvv1.AppendEntriesReply, error)
	SendInstallSnapshot(ctx context.Context, target string, args *raftkvv1.InstallSnapshotArgs) (*raftkvv1.InstallSnapshotReply, error)
	Close() error
}
