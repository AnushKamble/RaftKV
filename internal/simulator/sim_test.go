package simulator

import (
	"context"
	"fmt"
	"testing"
	"time"

	raftkvv1 "raftkv/gen/raftkv/v1"
)

type dummyNode struct {
	voteCount   int
	appendCount int
}

func (d *dummyNode) HandleRequestVote(ctx context.Context, args *raftkvv1.RequestVoteArgs) (*raftkvv1.RequestVoteReply, error) {
	d.voteCount++
	return &raftkvv1.RequestVoteReply{Term: args.Term, VoteGranted: true}, nil
}

func (d *dummyNode) HandleAppendEntries(ctx context.Context, args *raftkvv1.AppendEntriesArgs) (*raftkvv1.AppendEntriesReply, error) {
	d.appendCount++
	return &raftkvv1.AppendEntriesReply{Term: args.Term, Success: true}, nil
}

func (d *dummyNode) HandleInstallSnapshot(ctx context.Context, args *raftkvv1.InstallSnapshotArgs) (*raftkvv1.InstallSnapshotReply, error) {
	return &raftkvv1.InstallSnapshotReply{Term: args.Term}, nil
}

func TestVirtualClock_DeterministicTimers(t *testing.T) {
	trace := NewTraceLogger()
	clk := NewVirtualClock(trace)

	t1 := clk.NewTimer(100 * time.Millisecond)
	t2 := clk.NewTimer(50 * time.Millisecond)

	// Advance 40ms: neither timer fires
	clk.Advance(40 * time.Millisecond)
	select {
	case <-t1.C():
		t.Fatal("t1 fired prematurely")
	case <-t2.C():
		t.Fatal("t2 fired prematurely")
	default:
	}

	// Advance 20ms (total 60ms): t2 should fire, t1 should not
	clk.Advance(20 * time.Millisecond)
	select {
	case <-t2.C():
		// PASS
	default:
		t.Fatal("t2 should have fired at 50ms")
	}
	select {
	case <-t1.C():
		t.Fatal("t1 fired prematurely")
	default:
	}

	// Advance 50ms (total 110ms): t1 should fire
	clk.Advance(50 * time.Millisecond)
	select {
	case <-t1.C():
		// PASS
	default:
		t.Fatal("t1 should have fired at 100ms")
	}
}

func TestNetworkSimulator_DeterministicDelivery(t *testing.T) {
	trace := NewTraceLogger()
	clk := NewVirtualClock(trace)
	net := NewNetworkSimulator(12345, clk, trace)

	node1 := &dummyNode{}
	node2 := &dummyNode{}

	t1 := net.RegisterNode("node-1", node1)
	net.RegisterNode("node-2", node2)

	// Send an RPC from node-1 to node-2 in a goroutine
	doneCh := make(chan *raftkvv1.RequestVoteReply, 1)
	go func() {
		reply, err := t1.SendRequestVote(context.Background(), "node-2", &raftkvv1.RequestVoteArgs{
			Term:        1,
			CandidateId: "node-1",
		})
		if err != nil {
			t.Errorf("send failed: %v", err)
		}
		doneCh <- reply
	}()

	// Wait briefly for send to be queued (in-memory)
	time.Sleep(10 * time.Millisecond)
	if net.PendingCount() != 1 {
		t.Fatalf("expected 1 pending packet, got %d", net.PendingCount())
	}

	// Before advancing time, packet is not delivered
	if node2.voteCount != 0 {
		t.Fatalf("expected 0 votes delivered before time advance, got %d", node2.voteCount)
	}

	// Advance time past max delay (50ms)
	net.AdvanceAndDeliver(100 * time.Millisecond)

	select {
	case reply := <-doneCh:
		if !reply.VoteGranted || reply.Term != 1 {
			t.Fatalf("unexpected reply: %+v", reply)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for reply")
	}

	if node2.voteCount != 1 {
		t.Fatalf("expected 1 vote delivered to node2, got %d", node2.voteCount)
	}
}

func TestNetworkSimulator_PartitionAndHeal(t *testing.T) {
	trace := NewTraceLogger()
	clk := NewVirtualClock(trace)
	net := NewNetworkSimulator(999, clk, trace)

	n1 := &dummyNode{}
	n2 := &dummyNode{}
	t1 := net.RegisterNode("node-1", n1)
	net.RegisterNode("node-2", n2)

	// Partition node-1 (partition 1) and node-2 (partition 2)
	net.Partition(map[string]int{
		"node-1": 1,
		"node-2": 2,
	})

	_, err := t1.SendRequestVote(context.Background(), "node-2", &raftkvv1.RequestVoteArgs{Term: 1})
	if err == nil {
		t.Fatal("expected send to fail across partition, got nil")
	}

	// Heal partition
	net.Heal()

	doneCh := make(chan error, 1)
	go func() {
		_, err := t1.SendRequestVote(context.Background(), "node-2", &raftkvv1.RequestVoteArgs{Term: 1})
		doneCh <- err
	}()

	time.Sleep(10 * time.Millisecond)
	net.AdvanceAndDeliver(100 * time.Millisecond)

	select {
	case err := <-doneCh:
		if err != nil {
			t.Fatalf("expected send to succeed after heal, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out after heal")
	}
}

func TestNetworkSimulator_SeedDeterminism(t *testing.T) {
	runWithSeed := func(seed int64) string {
		trace := NewTraceLogger()
		clk := NewVirtualClock(trace)
		net := NewNetworkSimulator(seed, clk, trace)
		net.SetFaults(0.3, 0.2, 10*time.Millisecond, 50*time.Millisecond)

		n1 := &dummyNode{}
		n2 := &dummyNode{}
		t1 := net.RegisterNode("n1", n1)
		net.RegisterNode("n2", n2)

		for i := 0; i < 10; i++ {
			go func(idx int) {
				_, _ = t1.SendRequestVote(context.Background(), "n2", &raftkvv1.RequestVoteArgs{Term: uint64(idx)})
			}(i)
		}

		time.Sleep(20 * time.Millisecond)
		net.AdvanceAndDeliver(200 * time.Millisecond)
		time.Sleep(20 * time.Millisecond)
		return trace.String()
	}

	traceA := runWithSeed(42)
	traceB := runWithSeed(42)

	if traceA != traceB {
		fmt.Println("Trace A:\n", traceA)
		fmt.Println("Trace B:\n", traceB)
		t.Fatal("traces differed with identical seed!")
	}
}
