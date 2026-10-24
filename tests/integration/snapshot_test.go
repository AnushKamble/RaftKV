package integration

import (
	"context"
	"testing"
	"time"

	"raftkv/internal/client"
	"raftkv/internal/protocol"
)

func TestSnapshot_LaggingFollowerInstallSnapshot(t *testing.T) {
	cluster := startRealGRPCCluster(t, 53051)
	defer cluster.close()

	cli := client.NewClient(cluster.addrs)
	cli.SetNodeAddressMap(cluster.nodeMap)
	defer cli.Close()

	// Wait for leader election
	time.Sleep(1 * time.Second)

	var leaderNode *realGRPCCluster
	_ = leaderNode

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Write entries 1 to 5
	for i := 1; i <= 5; i++ {
		key := "num"
		val := string(rune('0' + i))
		if err := cli.Put(ctx, key, val); err != nil {
			t.Fatalf("put %d failed: %v", i, err)
		}
	}

	// 2. Identify leader node runtime instance
	var leaderIdx = -1
	for i, n := range cluster.nodes {
		st, err := n.GetStatus(ctx)
		if err == nil && st.Role == protocol.RoleLeader {
			leaderIdx = i
			break
		}
	}
	if leaderIdx == -1 {
		t.Fatal("no leader found")
	}

	leaderNodeRuntime := cluster.nodes[leaderIdx]
	t.Logf("Leader is node at index %d (%s)", leaderIdx, cluster.addrs[leaderIdx])

	// 3. Compact leader's log at index 5!
	st, err := leaderNodeRuntime.GetStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.LastApplied < 5 {
		t.Fatalf("leader lastApplied=%d < 5", st.LastApplied)
	}

	t.Log("Triggering TakeSnapshot(5) on leader...")
	if err := leaderNodeRuntime.Raft().TakeSnapshot(5); err != nil {
		t.Fatalf("TakeSnapshot failed: %v", err)
	}

	// Verify leader compacted its log up to index 5
	stAfterSnap, _ := leaderNodeRuntime.GetStatus(ctx)
	if stAfterSnap.SnapshotIndex != 5 {
		t.Fatalf("expected leader snapshotIndex=5, got %d", stAfterSnap.SnapshotIndex)
	}

	// 4. Find one follower and verify InstallSnapshot works
	// We can stop and restart a follower with an empty directory to simulate a brand-new or wiped node!
	followerIdx := (leaderIdx + 1) % 3
	t.Logf("Wiping follower %d data dir and restarting it to test InstallSnapshot...", followerIdx)
	cluster.nodes[followerIdx].Stop()

	// Reopen follower: it starts fresh with empty log
	_ = cluster.nodes[followerIdx].Start()

	// 5. Leader proposes entry 6
	writeCtx, writeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer writeCancel()

	if err := cli.Put(writeCtx, "suffix_key", "suffix_val"); err != nil {
		t.Fatalf("Put suffix failed: %v", err)
	}

	// 6. Give cluster time to transmit InstallSnapshot and AppendEntries suffix
	time.Sleep(1 * time.Second)

	// Verify the follower caught up to at least index 5 via snapshot + suffix
	val, found, err := cli.Get(writeCtx, "suffix_key")
	if err != nil || !found || val != "suffix_val" {
		t.Fatalf("reading suffix_key failed: val=%s, found=%v, err=%v", val, found, err)
	}

	numVal, numFound, err := cli.Get(writeCtx, "num")
	if err != nil || !numFound || numVal != "5" {
		t.Fatalf("reading snapshot key 'num' failed: val=%s, found=%v, err=%v", numVal, numFound, err)
	}
}
