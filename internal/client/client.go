package client

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	raftkvv1 "raftkv/gen/raftkv/v1"
)

// Client provides a robust key-value client that automatically handles leader routing and retries.
type Client struct {
	mu         sync.Mutex
	clientID   string
	servers    []string
	serverMap  map[string]string // nodeID -> addr
	leaderAddr string
	nextReqID  uint64
	conns      map[string]*grpc.ClientConn
}

// NewClient initializes a client with the provided cluster endpoint addresses.
func NewClient(servers []string) *Client {
	return &Client{
		clientID:   fmt.Sprintf("client-%d-%d", time.Now().UnixNano(), rand.Intn(10000)),
		servers:    servers,
		serverMap:  make(map[string]string),
		conns:      make(map[string]*grpc.ClientConn),
		leaderAddr: servers[0],
	}
}

// SetNodeAddressMap registers known nodeID to address mappings for hint redirects.
func (c *Client) SetNodeAddressMap(nodeMap map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.serverMap = make(map[string]string)
	for k, v := range nodeMap {
		c.serverMap[k] = v
	}
}

func (c *Client) getKVClient(addr string) (raftkvv1.KVServiceClient, error) {
	c.mu.Lock()
	conn, exists := c.conns[addr]
	if exists {
		c.mu.Unlock()
		return raftkvv1.NewKVServiceClient(conn), nil
	}
	c.mu.Unlock()

	newConn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %w", addr, err)
	}

	c.mu.Lock()
	c.conns[addr] = newConn
	c.mu.Unlock()

	return raftkvv1.NewKVServiceClient(newConn), nil
}

// Put writes key and value.
func (c *Client) Put(ctx context.Context, key, value string) error {
	reqID := atomic.AddUint64(&c.nextReqID, 1)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		c.mu.Lock()
		target := c.leaderAddr
		c.mu.Unlock()

		cli, err := c.getKVClient(target)
		if err == nil {
			callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			reply, err := cli.Put(callCtx, &raftkvv1.PutRequest{
				ClientId:  c.clientID,
				RequestId: reqID,
				Key:       key,
				Value:     value,
			})
			cancel()

			if err == nil {
				if reply.Success {
					return nil
				}
				if reply.ErrorCode == "NOT_LEADER" {
					c.updateLeader(reply.LeaderHint)
					time.Sleep(20 * time.Millisecond)
					continue
				}
				return fmt.Errorf("operation error: %s", reply.ErrorCode)
			}
		}

		c.rotateLeader()
		time.Sleep(30 * time.Millisecond)
	}
}

// Get reads the value for key.
func (c *Client) Get(ctx context.Context, key string) (string, bool, error) {
	reqID := atomic.AddUint64(&c.nextReqID, 1)

	for {
		select {
		case <-ctx.Done():
			return "", false, ctx.Err()
		default:
		}

		c.mu.Lock()
		target := c.leaderAddr
		c.mu.Unlock()

		cli, err := c.getKVClient(target)
		if err == nil {
			callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			reply, err := cli.Get(callCtx, &raftkvv1.GetRequest{
				ClientId:  c.clientID,
				RequestId: reqID,
				Key:       key,
			})
			cancel()

			if err == nil {
				if reply.Success {
					return reply.Value, reply.Found, nil
				}
				if reply.ErrorCode == "NOT_LEADER" {
					c.updateLeader(reply.LeaderHint)
					time.Sleep(20 * time.Millisecond)
					continue
				}
				return "", false, fmt.Errorf("operation error: %s", reply.ErrorCode)
			}
		}

		c.rotateLeader()
		time.Sleep(30 * time.Millisecond)
	}
}

// Delete removes key.
func (c *Client) Delete(ctx context.Context, key string) (bool, error) {
	reqID := atomic.AddUint64(&c.nextReqID, 1)

	for {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
		}

		c.mu.Lock()
		target := c.leaderAddr
		c.mu.Unlock()

		cli, err := c.getKVClient(target)
		if err == nil {
			callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			reply, err := cli.Delete(callCtx, &raftkvv1.DeleteRequest{
				ClientId:  c.clientID,
				RequestId: reqID,
				Key:       key,
			})
			cancel()

			if err == nil {
				if reply.Success {
					return reply.Found, nil
				}
				if reply.ErrorCode == "NOT_LEADER" {
					c.updateLeader(reply.LeaderHint)
					time.Sleep(20 * time.Millisecond)
					continue
				}
				return false, fmt.Errorf("operation error: %s", reply.ErrorCode)
			}
		}

		c.rotateLeader()
		time.Sleep(30 * time.Millisecond)
	}
}

// CompareAndSwap conditionally replaces key's value.
func (c *Client) CompareAndSwap(ctx context.Context, key, expected, newValue string) (bool, string, error) {
	reqID := atomic.AddUint64(&c.nextReqID, 1)

	for {
		select {
		case <-ctx.Done():
			return false, "", ctx.Err()
		default:
		}

		c.mu.Lock()
		target := c.leaderAddr
		c.mu.Unlock()

		cli, err := c.getKVClient(target)
		if err == nil {
			callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			reply, err := cli.CompareAndSwap(callCtx, &raftkvv1.CompareAndSwapRequest{
				ClientId:      c.clientID,
				RequestId:     reqID,
				Key:           key,
				ExpectedValue: expected,
				NewValue:      newValue,
			})
			cancel()

			if err == nil {
				if reply.Success {
					return reply.Swapped, reply.Value, nil
				}
				if reply.ErrorCode == "NOT_LEADER" {
					c.updateLeader(reply.LeaderHint)
					time.Sleep(20 * time.Millisecond)
					continue
				}
				return false, "", fmt.Errorf("operation error: %s", reply.ErrorCode)
			}
		}

		c.rotateLeader()
		time.Sleep(30 * time.Millisecond)
	}
}

// GetStatus queries the status of a specific node address.
func (c *Client) GetStatus(ctx context.Context, addr string) (*raftkvv1.GetStatusReply, error) {
	cli, err := c.getKVClient(addr)
	if err != nil {
		return nil, err
	}
	return cli.GetStatus(ctx, &raftkvv1.GetStatusRequest{})
}

func (c *Client) updateLeader(leaderHint string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if leaderHint == "" {
		c.rotateLeaderLocked()
		return
	}

	if addr, ok := c.serverMap[leaderHint]; ok {
		c.leaderAddr = addr
		return
	}

	c.rotateLeaderLocked()
}

func (c *Client) rotateLeader() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rotateLeaderLocked()
}

func (c *Client) rotateLeaderLocked() {
	if len(c.servers) <= 1 {
		return
	}
	for i, s := range c.servers {
		if s == c.leaderAddr {
			c.leaderAddr = c.servers[(i+1)%len(c.servers)]
			return
		}
	}
	c.leaderAddr = c.servers[0]
}

// Close closes all open gRPC client connections.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, conn := range c.conns {
		_ = conn.Close()
	}
	c.conns = make(map[string]*grpc.ClientConn)
	return nil
}
