package crash

import (
	"context"
	"testing"
	"time"

	raftkvv1 "raftkv/gen/raftkv/v1"
	"raftkv/internal/client"
)

func TestCrash_LeaderKillAndFailover(t *testing.T) {
	cluster := NewSubprocessCluster(t, 52051)
	defer cluster.StopAll()

	if err := cluster.StartAll(); err != nil {
		t.Fatalf("failed to start cluster subprocesses: %v", err)
	}

	leaderID, leaderAddr := cluster.WaitForLeader(5 * time.Second)
	t.Logf("Initial leader elected: %s at %s", leaderID, leaderAddr)

	cli := client.NewClient(cluster.Addresses)
	cli.SetNodeAddressMap(cluster.NodeMap)
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Initial write
	if err := cli.Put(ctx, "cluster_state", "phase1_active"); err != nil {
		t.Fatalf("initial Put failed: %v", err)
	}

	val, found, err := cli.Get(ctx, "cluster_state")
	if err != nil || !found || val != "phase1_active" {
		t.Fatalf("initial Get failed: val=%s, found=%v, err=%v", val, found, err)
	}

	// Abruptly kill leader process!
	t.Logf("Abruptly killing leader process %s...", leaderID)
	if err := cluster.KillNode(leaderID); err != nil {
		t.Fatalf("failed to kill leader process: %v", err)
	}

	// New write while original leader is dead!
	// Surviving majority must elect a new leader and continue commits.
	t.Log("Proposing write to surviving majority...")
	failoverCtx, failoverCancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer failoverCancel()

	if err := cli.Put(failoverCtx, "cluster_state", "phase2_failover_success"); err != nil {
		t.Fatalf("Put after leader kill failed: %v", err)
	}

	val2, found2, err := cli.Get(failoverCtx, "cluster_state")
	if err != nil || !found2 || val2 != "phase2_failover_success" {
		t.Fatalf("Get after failover failed: val=%s, found=%v, err=%v", val2, found2, err)
	}

	// Restart original leader from existing data directory
	t.Logf("Restarting old leader %s from existing directory...", leaderID)
	if err := cluster.StartNode(leaderID); err != nil {
		t.Fatalf("failed to restart old leader: %v", err)
	}

	// Issue a write to ensure active replication to rejoined node
	_ = cli.Put(failoverCtx, "cluster_state", "phase3_rejoined")

	// Wait for old leader to reconcile with the cluster
	deadline := time.Now().Add(3 * time.Second)
	var st *raftkvv1.GetStatusReply
	for time.Now().Before(deadline) {
		st, err = cluster.QueryStatus(cluster.NodeMap[leaderID])
		if err == nil && st.CommitIndex >= 2 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if st == nil || st.CommitIndex < 2 {
		t.Fatalf("restarted node commitIndex=%v, expected >= 2", st)
	}
}

func TestCrash_AllNodesRestart(t *testing.T) {
	cluster := NewSubprocessCluster(t, 52061)
	defer cluster.StopAll()

	if err := cluster.StartAll(); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}

	_, _ = cluster.WaitForLeader(5 * time.Second)

	cli := client.NewClient(cluster.Addresses)
	cli.SetNodeAddressMap(cluster.NodeMap)
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Write committed data
	entries := map[string]string{
		"config:version": "2.4.0",
		"auth:token":     "secret-token-xyz",
		"system:status":  "healthy",
	}

	for k, v := range entries {
		if err := cli.Put(ctx, k, v); err != nil {
			t.Fatalf("Put %s failed: %v", k, err)
		}
	}

	// Kill all 3 node processes abruptly
	t.Log("Killing all 3 cluster processes abruptly...")
	for id := range cluster.Processes {
		_ = cluster.KillNode(id)
	}

	time.Sleep(500 * time.Millisecond)

	// Restart all 3 processes from existing disk directories
	t.Log("Restarting all 3 nodes from disk...")
	for id := range cluster.Processes {
		if err := cluster.StartNode(id); err != nil {
			t.Fatalf("failed to restart node %s: %v", id, err)
		}
	}

	newLeaderID, _ := cluster.WaitForLeader(6 * time.Second)
	t.Logf("Cluster recovered from all-node restart, elected leader: %s", newLeaderID)

	// Verify all committed entries survived and are read cleanly
	readCtx, readCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer readCancel()

	for k, expectedVal := range entries {
		val, found, err := cli.Get(readCtx, k)
		if err != nil || !found || val != expectedVal {
			t.Fatalf("key %s corrupted or lost across full restart: got %q (found=%v, err=%v)", k, val, found, err)
		}
	}

	// Verify new mutations succeed after recovery
	swapped, curVal, err := cli.CompareAndSwap(readCtx, "system:status", "healthy", "recovered")
	if err != nil || !swapped || curVal != "recovered" {
		t.Fatalf("CAS after full restart failed: swapped=%v, val=%s, err=%v", swapped, curVal, err)
	}
}
