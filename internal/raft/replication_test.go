package raft

import (
	"context"
	"testing"
	"time"

	raftkvv1 "raftkv/gen/raftkv/v1"
	"raftkv/internal/protocol"
	"raftkv/internal/statemachine"
)

func TestReplication_FollowerCatchUp(t *testing.T) {
	nodeIDs := []protocol.NodeID{"node-1", "node-2", "node-3"}
	c := newSimulatedCluster(t, 200, nodeIDs)
	defer c.close()

	leader, leaderID := electLeader(t, c)
	t.Logf("Leader elected: %s", leaderID)

	// Propose 3 entries on the leader
	for i := 1; i <= 3; i++ {
		cmd := protocol.Command{
			Op:    protocol.OpPut,
			Key:   "k",
			Value: string(rune('0' + i)),
		}
		idx, _, err := leader.Propose(context.Background(), cmd)
		if err != nil {
			t.Fatalf("propose entry %d failed: %v", i, err)
		}
		if idx != protocol.Index(i) {
			t.Fatalf("expected index %d, got %d", i, idx)
		}
	}

	// Advance virtual time to replicate entries to followers
	for i := 0; i < 20; i++ {
		c.net.AdvanceAndDeliver(20 * time.Millisecond)
		time.Sleep(5 * time.Millisecond)
	}

	// Verify all nodes committed up to index 3
	for id, n := range c.nodes {
		st, err := n.GetStatus(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if st.CommitIndex < 3 {
			t.Fatalf("node %s did not catch up: commitIndex=%d, want 3", id, st.CommitIndex)
		}
		if st.LastLogIndex < 3 {
			t.Fatalf("node %s missing log entries: lastLogIndex=%d", id, st.LastLogIndex)
		}
	}
}

func TestReplication_ConflictRepairAfterPartition(t *testing.T) {
	nodeIDs := []protocol.NodeID{"node-1", "node-2", "node-3"}
	c := newSimulatedCluster(t, 301, nodeIDs)
	defer c.close()

	leader, leaderID := electLeader(t, c)

	// Propose entry 1 and commit it across all nodes
	_, _, err := leader.Propose(context.Background(), protocol.Command{Op: protocol.OpPut, Key: "init", Value: "val"})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		c.net.AdvanceAndDeliver(20 * time.Millisecond)
		time.Sleep(5 * time.Millisecond)
	}

	// Find a follower to isolate and the other follower
	var isolatedFollower *Raft
	var isolatedID protocol.NodeID
	var otherFollowerID protocol.NodeID
	for id, n := range c.nodes {
		if id != leaderID {
			if isolatedFollower == nil {
				isolatedFollower = n
				isolatedID = id
			} else {
				otherFollowerID = id
			}
		}
	}

	// Partition isolatedFollower into partition 1, while leader and other follower are in partition 2
	c.net.Partition(map[string]int{
		string(isolatedID):      1,
		string(leaderID):        2,
		string(otherFollowerID): 2,
	})

	// Pause isolatedFollower election timer during partition so it acts as an isolated lagging follower
	isolatedFollower.electionTimer.Stop()

	// Advance leader to term 2 and propose entry 2 and 3 in term 2
	leader.currentTerm = 2
	_ = leader.storage.SaveHardState(leader.currentTerm, string(leader.id))

	// Manually inject uncommitted conflicting entries from term 1 into isolatedFollower's storage
	_ = isolatedFollower.storage.AppendEntries([]protocol.LogEntry{
		{Index: 2, Term: 1, Command: protocol.Command{Op: protocol.OpPut, Key: "conflict", Value: "bad"}},
		{Index: 3, Term: 1, Command: protocol.Command{Op: protocol.OpPut, Key: "conflict2", Value: "bad2"}},
	})

	// Leader and other follower commit entries 2 and 3 in term 2
	_, _, err = leader.Propose(context.Background(), protocol.Command{Op: protocol.OpPut, Key: "good2", Value: "v2"})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = leader.Propose(context.Background(), protocol.Command{Op: protocol.OpPut, Key: "good3", Value: "v3"})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		c.net.AdvanceAndDeliver(20 * time.Millisecond)
		time.Sleep(5 * time.Millisecond)
	}

	// Heal partition: isolatedFollower rejoins and leader synchronizes
	c.net.Heal()
	isolatedFollower.resetElectionTimeout()
	leader.broadcastAppendEntries()

	for i := 0; i < 30; i++ {
		c.net.AdvanceAndDeliver(20 * time.Millisecond)
		time.Sleep(5 * time.Millisecond)
	}

	// Verify isolatedFollower truncated its conflicting suffix and adopted leader's log
	st, err := isolatedFollower.GetStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	e2, err := isolatedFollower.storage.Entry(2)
	if err != nil || e2.Command.Key != "good2" {
		t.Fatalf("conflict repair failed on %s: entry 2 = %+v, err=%v", isolatedID, e2, err)
	}
	e3, err := isolatedFollower.storage.Entry(3)
	if err != nil || e3.Command.Key != "good3" {
		t.Fatalf("conflict repair failed on %s: entry 3 = %+v, err=%v", isolatedID, e3, err)
	}
	if st.CommitIndex < 3 {
		t.Fatalf("expected follower commitIndex >= 3 after repair, got %d", st.CommitIndex)
	}
}

func TestReplication_CurrentTermOnlyCommit_Figure8(t *testing.T) {
	nodeIDs := []protocol.NodeID{"node-1", "node-2", "node-3"}
	c := newSimulatedCluster(t, 401, nodeIDs)
	defer c.close()

	leader, leaderID := electLeader(t, c)
	leaderStatus, _ := leader.GetStatus(context.Background())
	currentTerm := leaderStatus.Term

	// Manually inject an entry from a PREVIOUS term (currentTerm - 1) at index 1
	// on leader and node-2 (majority: 2 of 3)
	prevTerm := currentTerm - 1
	if prevTerm < 1 {
		prevTerm = 1
		currentTerm = 2
		leader.currentTerm = 2
	}

	entryPrevTerm := protocol.LogEntry{
		Index:   1,
		Term:    prevTerm,
		Command: protocol.Command{Op: protocol.OpPut, Key: "prevKey", Value: "prevVal"},
	}
	_ = leader.storage.AppendEntries([]protocol.LogEntry{entryPrevTerm})

	var peerID protocol.NodeID
	for id := range c.nodes {
		if id != leaderID {
			peerID = id
			break
		}
	}
	_ = c.nodes[peerID].storage.AppendEntries([]protocol.LogEntry{entryPrevTerm})
	leader.matchIndex[string(peerID)] = 1

	// In Figure 8, even though index 1 is replicated on a majority (leader + peerID),
	// its term (prevTerm) is != currentTerm!
	// Therefore, leader MUST NOT advance commitIndex to 1!
	leader.maybeAdvanceCommitIndex()

	st, _ := leader.GetStatus(context.Background())
	if st.CommitIndex >= 1 {
		t.Fatalf("INVARIANT VIOLATION: Leader committed entry from previous term directly! commitIndex=%d", st.CommitIndex)
	}

	// Now propose an entry in the CURRENT term (index 2)
	idx2, _, err := leader.Propose(context.Background(), protocol.Command{Op: protocol.OpPut, Key: "currKey", Value: "currVal"})
	if err != nil {
		t.Fatal(err)
	}

	// Replicate index 2 to majority
	leader.matchIndex[string(peerID)] = idx2
	leader.maybeAdvanceCommitIndex()

	// Now that an entry from currentTerm committed, index 1 is also safely committed!
	stAfter, _ := leader.GetStatus(context.Background())
	if stAfter.CommitIndex < idx2 {
		t.Fatalf("expected commitIndex >= %d after current term entry committed, got %d", idx2, stAfter.CommitIndex)
	}
}

func TestReplication_StaleReplyDoesNotRegressMatchIndex(t *testing.T) {
	nodeIDs := []protocol.NodeID{"node-1"}
	c := newSimulatedCluster(t, 501, nodeIDs)
	defer c.close()

	leader := c.nodes["node-1"]
	leader.role = protocol.RoleLeader
	leader.currentTerm = 2
	leader.matchIndex["peer-A"] = 10
	leader.nextIndex["peer-A"] = 11

	// Simulate an old delayed AppendEntries reply arriving with matchIndex 5
	leader.handleAppendEntriesReply(appendEntriesReplyEvent{
		from:       "peer-A",
		term:       2,
		success:    true,
		matchIndex: 5, // smaller than current 10
	})

	if leader.matchIndex["peer-A"] != 10 {
		t.Fatalf("matchIndex rolled backwards! expected 10, got %d", leader.matchIndex["peer-A"])
	}
}

func TestReplication_MinorityCannotCommit(t *testing.T) {
	nodeIDs := []protocol.NodeID{"node-1", "node-2", "node-3"}
	c := newSimulatedCluster(t, 601, nodeIDs)
	defer c.close()

	leader, leaderID := electLeader(t, c)

	// Isolate leader into a minority partition (node-1 isolated, node-2 and node-3 together)
	var otherPeers []protocol.NodeID
	for id := range c.nodes {
		if id != leaderID {
			otherPeers = append(otherPeers, id)
		}
	}

	c.net.Partition(map[string]int{
		string(leaderID):      1,
		string(otherPeers[0]): 2,
		string(otherPeers[1]): 2,
	})

	// Leader in minority proposes a write
	idx, _, err := leader.Propose(context.Background(), protocol.Command{Op: protocol.OpPut, Key: "minority", Value: "val"})
	if err != nil {
		t.Fatal(err)
	}

	// Advance time for replication attempts
	for i := 0; i < 15; i++ {
		c.net.AdvanceAndDeliver(20 * time.Millisecond)
		time.Sleep(5 * time.Millisecond)
	}

	// Leader cannot commit because it cannot reach a majority
	st, _ := leader.GetStatus(context.Background())
	if st.CommitIndex >= idx {
		t.Fatalf("Minority leader committed entry %d without majority!", idx)
	}

	// Meanwhile, majority partition (nodes 2 and 3) should elect a new leader and make progress
	var majorityLeader *Raft
	for i := 0; i < 50; i++ {
		c.net.AdvanceAndDeliver(20 * time.Millisecond)
		time.Sleep(5 * time.Millisecond)
		for _, id := range otherPeers {
			st, _ := c.nodes[id].GetStatus(context.Background())
			if st.Role == protocol.RoleLeader {
				majorityLeader = c.nodes[id]
				break
			}
		}
		if majorityLeader != nil {
			break
		}
	}

	if majorityLeader == nil {
		t.Fatalf("majority partition failed to elect a new leader. Trace:\n%s", c.trace.String())
	}

	// Majority leader CAN propose and commit!
	majIdx, _, err := majorityLeader.Propose(context.Background(), protocol.Command{Op: protocol.OpPut, Key: "majKey", Value: "majVal"})
	if err != nil {
		t.Fatalf("majority leader propose failed: %v", err)
	}

	for i := 0; i < 20; i++ {
		c.net.AdvanceAndDeliver(20 * time.Millisecond)
		time.Sleep(5 * time.Millisecond)
	}

	majStatus, _ := majorityLeader.GetStatus(context.Background())
	if majStatus.CommitIndex < majIdx {
		t.Fatalf("majority partition failed to commit: commitIndex=%d, want %d", majStatus.CommitIndex, majIdx)
	}
}

func TestReplication_CorruptedInstallSnapshotRejected(t *testing.T) {
	nodeIDs := []protocol.NodeID{"node-1"}
	c := newSimulatedCluster(t, 701, nodeIDs)
	defer c.close()

	node := c.nodes["node-1"]
	node.stateMachine = statemachine.NewKVStateMachine()
	node.currentTerm = 2
	node.commitIndex = 5
	node.lastApplied = 5

	// Send an InstallSnapshot with corrupted/invalid data payload
	corruptedData := []byte("this is definitely not valid json snapshot data")
	reply := node.handleInstallSnapshot(&raftkvv1.InstallSnapshotArgs{
		Term:              2,
		LeaderId:          "leader-node",
		LastIncludedIndex: 10,
		LastIncludedTerm:  2,
		Data:              corruptedData,
		Done:              true,
	})

	_ = reply

	// Verify that commitIndex and lastApplied did NOT advance
	st, err := node.GetStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.CommitIndex != 5 || st.LastApplied != 5 {
		t.Fatalf("corrupted snapshot advanced indices! commitIndex=%d, lastApplied=%d (expected 5)", st.CommitIndex, st.LastApplied)
	}
}
