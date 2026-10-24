package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"raftkv/internal/client"
	"raftkv/internal/config"
	"raftkv/internal/runtime"
)

type realGRPCCluster struct {
	nodes   []*runtime.Node
	dirs    []string
	addrs   []string
	nodeMap map[string]string
	baseDir string
}

func startRealGRPCCluster(t *testing.T, basePort int) *realGRPCCluster {
	baseDir, err := os.MkdirTemp("", "raftkv-grpc-cluster-*")
	if err != nil {
		t.Fatal(err)
	}

	nodeIDs := []string{"node-1", "node-2", "node-3"}
	addrs := make([]string, 3)
	dirs := make([]string, 3)
	nodeMap := make(map[string]string)

	for i := 0; i < 3; i++ {
		addrs[i] = fmt.Sprintf("127.0.0.1:%d", basePort+i)
		dirs[i] = filepath.Join(baseDir, nodeIDs[i])
		nodeMap[nodeIDs[i]] = addrs[i]
	}

	var nodes []*runtime.Node
	for i := 0; i < 3; i++ {
		peers := make(map[string]string)
		for j := 0; j < 3; j++ {
			if i != j {
				peers[nodeIDs[j]] = addrs[j]
			}
		}

		cfg := &config.ParsedNodeConfig{
			ID:                 nodeIDs[i],
			DataDir:            dirs[i],
			Address:            addrs[i],
			Peers:              peers,
			MinElectionTimeout: 150 * time.Millisecond,
			MaxElectionTimeout: 300 * time.Millisecond,
			HeartbeatInterval:  50 * time.Millisecond,
		}

		n, err := runtime.NewNode(cfg)
		if err != nil {
			t.Fatalf("failed to create node %s: %v", nodeIDs[i], err)
		}
		if err := n.Start(); err != nil {
			t.Fatalf("failed to start node %s: %v", nodeIDs[i], err)
		}
		nodes = append(nodes, n)
	}

	return &realGRPCCluster{
		nodes:   nodes,
		dirs:    dirs,
		addrs:   addrs,
		nodeMap: nodeMap,
		baseDir: baseDir,
	}
}

func (c *realGRPCCluster) close() {
	for _, n := range c.nodes {
		n.Stop()
	}
	_ = os.RemoveAll(c.baseDir)
}

func TestClusterGRPC_EndToEndOperations(t *testing.T) {
	cluster := startRealGRPCCluster(t, 51051)
	defer cluster.close()

	cli := client.NewClient(cluster.addrs)
	cli.SetNodeAddressMap(cluster.nodeMap)
	defer cli.Close()

	// 1. Wait for leader election over real TCP network
	var leaderAddr string
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for cluster leader election")
		default:
		}

		for _, addr := range cluster.addrs {
			st, err := cli.GetStatus(ctx, addr)
			if err == nil && st.Role == "Leader" {
				leaderAddr = addr
				break
			}
		}
		if leaderAddr != "" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Logf("Real gRPC cluster elected leader at %s", leaderAddr)

	// 2. Client Put
	opCtx, opCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer opCancel()

	if err := cli.Put(opCtx, "greeting", "hello-raft"); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 3. Client Get
	val, found, err := cli.Get(opCtx, "greeting")
	if err != nil || !found || val != "hello-raft" {
		t.Fatalf("Get returned unexpected value: val=%q, found=%v, err=%v", val, found, err)
	}

	// 4. Client CompareAndSwap
	swapped, curVal, err := cli.CompareAndSwap(opCtx, "greeting", "hello-raft", "hello-world")
	if err != nil || !swapped || curVal != "hello-world" {
		t.Fatalf("CAS failed: swapped=%v, val=%q, err=%v", swapped, curVal, err)
	}

	// 5. Client Delete
	found, err = cli.Delete(opCtx, "greeting")
	if err != nil || !found {
		t.Fatalf("Delete failed: found=%v, err=%v", found, err)
	}

	// 6. Verify Delete
	_, foundAfter, err := cli.Get(opCtx, "greeting")
	if err != nil || foundAfter {
		t.Fatalf("expected key to be gone after delete, found=%v", foundAfter)
	}
}

func TestClusterGRPC_NonLeaderHintAndRetry(t *testing.T) {
	cluster := startRealGRPCCluster(t, 51061)
	defer cluster.close()

	// Wait for leader
	time.Sleep(1 * time.Second)

	// Target a follower explicitly
	var followerAddr string
	cli := client.NewClient(cluster.addrs)
	cli.SetNodeAddressMap(cluster.nodeMap)
	defer cli.Close()

	for _, addr := range cluster.addrs {
		st, err := cli.GetStatus(context.Background(), addr)
		if err == nil && st.Role == "Follower" {
			followerAddr = addr
			break
		}
	}
	if followerAddr == "" {
		t.Fatal("could not find follower node")
	}

	// Create client with ONLY the follower address initially
	followerOnlyCli := client.NewClient([]string{followerAddr})
	followerOnlyCli.SetNodeAddressMap(cluster.nodeMap)
	defer followerOnlyCli.Close()

	// Client Put should receive NOT_LEADER, redirect via leader hint, and succeed!
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := followerOnlyCli.Put(ctx, "redirect_key", "redirect_value")
	if err != nil {
		t.Fatalf("expected client to follow leader hint and succeed, got err: %v", err)
	}

	// Read back through follower-only client
	val, found, err := followerOnlyCli.Get(ctx, "redirect_key")
	if err != nil || !found || val != "redirect_value" {
		t.Fatalf("Get via redirect failed: val=%s, found=%v, err=%v", val, found, err)
	}
}
