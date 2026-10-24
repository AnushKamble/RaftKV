package chaos

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"raftkv/internal/protocol"
	"raftkv/internal/raft"
	"raftkv/internal/simulator"
	"raftkv/internal/statemachine"
	"raftkv/internal/storage"
)

type chaosCluster struct {
	nodes   map[protocol.NodeID]*raft.Raft
	sms     map[protocol.NodeID]*statemachine.KVStateMachine
	stores  map[protocol.NodeID]storage.Storage
	net     *simulator.NetworkSimulator
	clock   *simulator.VirtualClock
	trace   *simulator.TraceLogger
	tempDir string
}

func newChaosCluster(t *testing.T, seed int64, dropRate, dupRate float64) *chaosCluster {
	tempDir, err := os.MkdirTemp("", "raftkv-chaos-*")
	if err != nil {
		t.Fatal(err)
	}

	trace := simulator.NewTraceLogger()
	clock := simulator.NewVirtualClock(trace)
	net := simulator.NewNetworkSimulator(seed, clock, trace)
	net.SetFaults(dropRate, dupRate, 10*time.Millisecond, 40*time.Millisecond)

	nodeIDs := []protocol.NodeID{"node-1", "node-2", "node-3"}
	c := &chaosCluster{
		nodes:   make(map[protocol.NodeID]*raft.Raft),
		sms:     make(map[protocol.NodeID]*statemachine.KVStateMachine),
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
			t.Fatal(err)
		}
		c.stores[id] = s

		sm := statemachine.NewKVStateMachine()
		c.sms[id] = sm

		var peers []protocol.NodeID
		for _, o := range nodeIDs {
			if o != id {
				peers = append(peers, o)
			}
		}

		applyChan := make(chan protocol.LogEntry, 100)
		cfg := raft.Config{
			ID:                 id,
			Peers:              peers,
			Storage:            s,
			Clock:              clock,
			StateMachine:       sm,
			MinElectionTimeout: 150 * time.Millisecond,
			MaxElectionTimeout: 300 * time.Millisecond,
			HeartbeatInterval:  50 * time.Millisecond,
			ApplyChan:          applyChan,
		}

		trans := net.RegisterNode(string(id), nil)
		cfg.Transport = trans

		r, err := raft.NewRaft(cfg)
		if err != nil {
			t.Fatal(err)
		}
		c.nodes[id] = r
		net.RegisterHandlers(string(id), r)

		// Start apply loop
		go func(rNode *raft.Raft, machine *statemachine.KVStateMachine, ch chan protocol.LogEntry) {
			for entry := range ch {
				machine.Apply(entry)
			}
		}(r, sm, applyChan)

		r.Start()
	}

	return c
}

func (c *chaosCluster) close() {
	for _, n := range c.nodes {
		n.Stop()
	}
	for _, s := range c.stores {
		_ = s.Close()
	}
	_ = os.RemoveAll(c.tempDir)
}

func (c *chaosCluster) assertInvariants(t *testing.T, seed int64) {
	// Invariant 1: At most one leader in any given term
	leadersByTerm := make(map[protocol.Term]protocol.NodeID)
	for id, n := range c.nodes {
		st, err := n.GetStatus(context.Background())
		if err == nil && st.Role == protocol.RoleLeader {
			if existing, found := leadersByTerm[st.Term]; found && existing != id {
				t.Fatalf("[SEED %d] INVARIANT 1 VIOLATION: Two leaders in term %d (%s and %s)",
					seed, st.Term, existing, id)
			}
			leadersByTerm[st.Term] = id
		}

		// Invariant 2: lastApplied <= commitIndex always
		if st.LastApplied > st.CommitIndex {
			t.Fatalf("[SEED %d] INVARIANT 2 VIOLATION: on %s lastApplied (%d) > commitIndex (%d)",
				seed, id, st.LastApplied, st.CommitIndex)
		}
	}

	// Invariant 3: Log matching property
	for id1, s1 := range c.stores {
		for id2, s2 := range c.stores {
			if id1 >= id2 {
				continue
			}
			last1, _, _ := s1.LastIndexAndTerm()
			last2, _, _ := s2.LastIndexAndTerm()
			minLast := last1
			if last2 < minLast {
				minLast = last2
			}
			first1, _ := s1.FirstIndex()
			first2, _ := s2.FirstIndex()
			maxFirst := first1
			if first2 > maxFirst {
				maxFirst = first2
			}

			for idx := maxFirst; idx <= minLast; idx++ {
				t1, err1 := s1.Term(idx)
				t2, err2 := s2.Term(idx)
				if err1 == nil && err2 == nil && t1 == t2 {
					e1, err1 := s1.Entry(idx)
					e2, err2 := s2.Entry(idx)
					if err1 == nil && err2 == nil {
						if e1.Command.Key != e2.Command.Key || e1.Command.Value != e2.Command.Value {
							t.Fatalf("[SEED %d] LOG MATCHING VIOLATION at index %d term %d between %s and %s: %+v vs %+v",
								seed, idx, t1, id1, id2, e1, e2)
						}
					}
				}
			}
		}
	}
}

func TestChaos_SeededFaultCampaign(t *testing.T) {
	seeds := []int64{101, 202, 303, 404, 505}

	for _, seed := range seeds {
		t.Run(fmt.Sprintf("Seed_%d", seed), func(t *testing.T) {
			// 15% drop rate, 10% duplication rate, 10-40ms delay
			c := newChaosCluster(t, seed, 0.15, 0.10)
			defer c.close()

			// Round 1: Run 500ms of virtual time to elect initial leader
			for i := 0; i < 25; i++ {
				c.net.AdvanceAndDeliver(20 * time.Millisecond)
				time.Sleep(2 * time.Millisecond)
				c.assertInvariants(t, seed)
			}

			// Propose commands
			for _, n := range c.nodes {
				st, _ := n.GetStatus(context.Background())
				if st.Role == protocol.RoleLeader {
					_, _, _ = n.Propose(context.Background(), protocol.Command{
						Op:    protocol.OpPut,
						Key:   fmt.Sprintf("key-%d", seed),
						Value: fmt.Sprintf("val-%d", seed),
					})
					break
				}
			}

			// Round 2: Injected partition at virtual time 520ms
			c.net.Partition(map[string]int{
				"node-1": 1,
				"node-2": 2,
				"node-3": 2,
			})

			for i := 0; i < 20; i++ {
				c.net.AdvanceAndDeliver(20 * time.Millisecond)
				time.Sleep(2 * time.Millisecond)
				c.assertInvariants(t, seed)
			}

			// Round 3: Heal partition
			c.net.Heal()

			for i := 0; i < 25; i++ {
				c.net.AdvanceAndDeliver(20 * time.Millisecond)
				time.Sleep(2 * time.Millisecond)
				c.assertInvariants(t, seed)
			}

			t.Logf("Seed %d completed cleanly with zero invariant violations.", seed)
		})
	}
}
