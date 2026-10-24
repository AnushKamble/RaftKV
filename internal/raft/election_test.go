package raft

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	raftkvv1 "raftkv/gen/raftkv/v1"
	"raftkv/internal/protocol"
	"raftkv/internal/simulator"
	"raftkv/internal/storage"
)

type simulatedCluster struct {
	nodes   map[protocol.NodeID]*Raft
	stores  map[protocol.NodeID]storage.Storage
	net     *simulator.NetworkSimulator
	clock   *simulator.VirtualClock
	trace   *simulator.TraceLogger
	tempDir string
}

func newSimulatedCluster(t *testing.T, seed int64, nodeIDs []protocol.NodeID) *simulatedCluster {
	tempDir, err := os.MkdirTemp("", "raftkv-sim-cluster-*")
	if err != nil {
		t.Fatal(err)
	}

	trace := simulator.NewTraceLogger()
	clock := simulator.NewVirtualClock(trace)
	net := simulator.NewNetworkSimulator(seed, clock, trace)

	c := &simulatedCluster{
		nodes:   make(map[protocol.NodeID]*Raft),
		stores:  make(map[protocol.NodeID]storage.Storage),
		net:     net,
		clock:   clock,
		trace:   trace,
		tempDir: tempDir,
	}

	for _, id := range nodeIDs {
		nodeDir := filepath.Join(tempDir, string(id))
		s, err := storage.OpenDiskStorage(nodeDir)
		if err != nil {
			t.Fatalf("failed to open storage for %s: %v", id, err)
		}
		c.stores[id] = s

		var peers []protocol.NodeID
		for _, other := range nodeIDs {
			if other != id {
				peers = append(peers, other)
			}
		}

		// Configure slightly staggered default election timeouts
		minTimeout := 150 * time.Millisecond
		maxTimeout := 300 * time.Millisecond

		transportAdapter := net.RegisterNode(string(id), nil)

		cfg := Config{
			ID:                 id,
			Peers:              peers,
			Storage:            s,
			Transport:          transportAdapter,
			Clock:              clock,
			MinElectionTimeout: minTimeout,
			MaxElectionTimeout: maxTimeout,
			HeartbeatInterval:  50 * time.Millisecond,
		}

		// Register node handlers on simulated network
		node, err := NewRaft(cfg)
		if err != nil {
			t.Fatalf("failed to create raft node %s: %v", id, err)
		}
		c.nodes[id] = node
		net.RegisterHandlers(string(id), node)
		node.Start()
	}

	return c
}

func (c *simulatedCluster) close() {
	for _, n := range c.nodes {
		n.Stop()
	}
	for _, s := range c.stores {
		_ = s.Close()
	}
	_ = os.RemoveAll(c.tempDir)
}

func electLeader(t *testing.T, c *simulatedCluster) (*Raft, protocol.NodeID) {
	for i := 0; i < 60; i++ {
		c.net.AdvanceAndDeliver(20 * time.Millisecond)
		time.Sleep(5 * time.Millisecond)
		for id, n := range c.nodes {
			st, err := n.GetStatus(context.Background())
			if err == nil && st.Role == protocol.RoleLeader {
				return n, id
			}
		}
	}
	t.Fatalf("no leader elected after time advance. Trace:\n%s", c.trace.String())
	return nil, ""
}

func TestElection_SingleLeaderElected(t *testing.T) {
	nodeIDs := []protocol.NodeID{"node-1", "node-2", "node-3"}
	c := newSimulatedCluster(t, 42, nodeIDs)
	defer c.close()

	leader, leaderID := electLeader(t, c)
	st, err := leader.GetStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if st.Role != protocol.RoleLeader {
		t.Fatalf("expected leader role, got %s", st.Role)
	}

	// Verify only 1 leader exists across all nodes
	leaders := 0
	for _, n := range c.nodes {
		s, _ := n.GetStatus(context.Background())
		if s.Role == protocol.RoleLeader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", leaders)
	}

	if st.Term < 1 {
		t.Fatalf("expected leader term >= 1, got %d", st.Term)
	}

	t.Logf("Leader successfully elected: %s at term %d", leaderID, st.Term)
}

func TestElection_StaleTermRejected(t *testing.T) {
	nodeIDs := []protocol.NodeID{"node-1"}
	c := newSimulatedCluster(t, 1, nodeIDs)
	defer c.close()

	n := c.nodes["node-1"]
	// Manually set term to 5
	_ = c.stores["node-1"].SaveHardState(5, "")
	n.currentTerm = 5

	reply, err := n.RequestVote(context.Background(), &raftkvv1.RequestVoteArgs{
		Term:         3, // Stale term
		CandidateId:  "candidate-x",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	if reply.VoteGranted {
		t.Fatal("expected vote to be denied for stale term 3 < 5")
	}
	if reply.Term != 5 {
		t.Fatalf("expected reply term 5, got %d", reply.Term)
	}
}

func TestElection_NoDoubleVoteAcrossRestart(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "raftkv-vote-persist-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	trace := simulator.NewTraceLogger()
	clk := simulator.NewVirtualClock(trace)
	net := simulator.NewNetworkSimulator(1, clk, trace)

	// Step 1: Start node, grant vote to candidate-A for term 2
	s1, err := storage.OpenDiskStorage(tempDir)
	if err != nil {
		t.Fatal(err)
	}

	cfg := Config{
		ID:        "node-1",
		Storage:   s1,
		Clock:     clk,
		Transport: net.RegisterNode("node-1", nil),
	}
	r1, err := NewRaft(cfg)
	if err != nil {
		t.Fatal(err)
	}
	r1.Start()

	reply, err := r1.RequestVote(context.Background(), &raftkvv1.RequestVoteArgs{
		Term:         2,
		CandidateId:  "candidate-A",
		LastLogIndex: 1,
		LastLogTerm:  1,
	})
	if err != nil || !reply.VoteGranted {
		t.Fatalf("expected vote granted to candidate-A: err=%v, reply=%+v", err, reply)
	}

	// Try voting for candidate-B in same term: MUST BE REJECTED
	reply2, err := r1.RequestVote(context.Background(), &raftkvv1.RequestVoteArgs{
		Term:         2,
		CandidateId:  "candidate-B",
		LastLogIndex: 1,
		LastLogTerm:  1,
	})
	if err != nil || reply2.VoteGranted {
		t.Fatalf("expected candidate-B to be rejected, got voteGranted=true")
	}

	r1.Stop()
	_ = s1.Close()

	// Step 2: Restart node from same directory, verify it still cannot vote for candidate-B in term 2
	s2, err := storage.OpenDiskStorage(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Storage = s2
	r2, err := NewRaft(cfg)
	if err != nil {
		t.Fatal(err)
	}
	r2.Start()
	defer r2.Stop()
	defer s2.Close()

	replyRestart, err := r2.RequestVote(context.Background(), &raftkvv1.RequestVoteArgs{
		Term:         2,
		CandidateId:  "candidate-B",
		LastLogIndex: 1,
		LastLogTerm:  1,
	})
	if err != nil || replyRestart.VoteGranted {
		t.Fatalf("expected vote to candidate-B to be DENIED across restart in term 2")
	}
}

func TestElection_StaleLogCandidateRejected(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "raftkv-stale-log-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	s, err := storage.OpenDiskStorage(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Local node has entries up to index 5, term 3
	_ = s.AppendEntries([]protocol.LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
		{Index: 4, Term: 3},
		{Index: 5, Term: 3},
	})

	clk := simulator.NewVirtualClock(nil)
	net := simulator.NewNetworkSimulator(1, clk, nil)

	r, err := NewRaft(Config{
		ID:        "node-1",
		Storage:   s,
		Clock:     clk,
		Transport: net.RegisterNode("node-1", nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	r.Start()
	defer r.Stop()

	// Case 1: Candidate with lower last log term (term 2 vs local term 3)
	reply1, err := r.RequestVote(context.Background(), &raftkvv1.RequestVoteArgs{
		Term:         4,
		CandidateId:  "candidate-stale-term",
		LastLogIndex: 10,
		LastLogTerm:  2,
	})
	if err != nil || reply1.VoteGranted {
		t.Fatalf("expected rejection for candidate with lower lastLogTerm (2 vs 3)")
	}

	// Case 2: Candidate with same term 3, but shorter log (index 4 vs local index 5)
	reply2, err := r.RequestVote(context.Background(), &raftkvv1.RequestVoteArgs{
		Term:         4,
		CandidateId:  "candidate-shorter-log",
		LastLogIndex: 4,
		LastLogTerm:  3,
	})
	if err != nil || reply2.VoteGranted {
		t.Fatalf("expected rejection for candidate with shorter log index (4 vs 5)")
	}

	// Case 3: Candidate with same term 3 and equal or greater index (index 5)
	reply3, err := r.RequestVote(context.Background(), &raftkvv1.RequestVoteArgs{
		Term:         4,
		CandidateId:  "candidate-fresh",
		LastLogIndex: 5,
		LastLogTerm:  3,
	})
	if err != nil || !reply3.VoteGranted {
		t.Fatalf("expected vote granted to candidate with matching freshness")
	}
}

func TestElection_HigherTermForcesStepDown(t *testing.T) {
	nodeIDs := []protocol.NodeID{"node-1", "node-2", "node-3"}
	c := newSimulatedCluster(t, 101, nodeIDs)
	defer c.close()

	leader, _ := electLeader(t, c)

	leaderStatus, _ := leader.GetStatus(context.Background())
	higherTerm := leaderStatus.Term + 5

	// Send an AppendEntries or RequestVote with higherTerm to the leader
	reply, err := leader.RequestVote(context.Background(), &raftkvv1.RequestVoteArgs{
		Term:         uint64(higherTerm),
		CandidateId:  "node-other",
		LastLogIndex: 100,
		LastLogTerm:  uint64(higherTerm),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reply.VoteGranted {
		t.Fatal("expected vote granted to higher term candidate")
	}

	// Verify leader stepped down to Follower
	newStatus, _ := leader.GetStatus(context.Background())
	if newStatus.Role != protocol.RoleFollower {
		t.Fatalf("expected leader to step down to Follower, got %s", newStatus.Role)
	}
	if newStatus.Term != higherTerm {
		t.Fatalf("expected term updated to %d, got %d", higherTerm, newStatus.Term)
	}
}
