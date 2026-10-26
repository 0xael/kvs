// Package coordinator routes KV requests to the correct node in the cluster.
// In Phase 2, it forwards requests to the single owner identified by the
// hash ring. In Phase 3, this will be extended with fan-out to N replicas
// and quorum logic.
package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	pb "github.com/alyelalwany/ghostkv/gen/ghostkv/v1"
	"github.com/alyelalwany/ghostkv/internal/config"
	"github.com/alyelalwany/ghostkv/internal/membership"
	"github.com/alyelalwany/ghostkv/internal/store"
)

// Ring is the interface the coordinator uses to look up key ownership.
// The real implementation lives in internal/ring.
type Ring interface {
	GetNode(key string) string
	GetNodes(key string, n int) []string
}

// Coordinator routes KV operations to the appropriate node based on the
// consistent hash ring. For Phase 2, it routes to a single owner.
type Coordinator struct {
	nodeID     string
	ring       Ring
	store      store.Store
	membership *membership.Membership
	cfg        *config.ClusterSettings
	logger     *slog.Logger
}

// New creates a Coordinator that routes requests using the given ring and
// forwards to remote nodes via the membership connections.
func New(nodeID string, ring Ring, s store.Store, m *membership.Membership, cfg *config.ClusterSettings) *Coordinator {
	return &Coordinator{
		nodeID:     nodeID,
		ring:       ring,
		store:      s,
		membership: m,
		cfg:        cfg,
		logger:     slog.Default().With("component", "coordinator", "node", nodeID),
	}
}

// Put stores a key-value pair. If this node is the owner, it writes locally.
// Otherwise it forwards the PutRequest to the owning node via KVService.
func (c *Coordinator) Put(ctx context.Context, key string, value []byte) (int32, error) {
	owner := c.ring.GetNode(key)

	if owner == c.nodeID {
		rec := &store.Record{
			Value:     value,
			Timestamp: time.Now().UnixNano(),
		}
		if err := c.store.Put(key, rec); err != nil {
			return 0, fmt.Errorf("coordinator: local put: %w", err)
		}
		c.logger.Debug("put local", "key", key)
		return 1, nil
	}

	// Forward to the owner node.
	conn, err := c.membership.GetConn(owner)
	if err != nil {
		return 0, fmt.Errorf("coordinator: get conn for %s: %w", owner, err)
	}

	client := pb.NewKVServiceClient(conn)
	resp, err := client.Put(ctx, &pb.PutRequest{
		Key:   key,
		Value: value,
	})
	if err != nil {
		return 0, fmt.Errorf("coordinator: forward put to %s: %w", owner, err)
	}

	c.logger.Debug("put forwarded", "key", key, "owner", owner)
	return resp.GetAcks(), nil
}

// Get retrieves a value by key. If this node is the owner, it reads locally.
// Otherwise it forwards the GetRequest to the owning node via KVService.
func (c *Coordinator) Get(ctx context.Context, key string) (*store.Record, error) {
	owner := c.ring.GetNode(key)

	if owner == c.nodeID {
		rec, err := c.store.Get(key)
		if err != nil {
			return nil, fmt.Errorf("coordinator: local get: %w", err)
		}
		c.logger.Debug("get local", "key", key)
		return rec, nil
	}

	// Forward to the owner node.
	conn, err := c.membership.GetConn(owner)
	if err != nil {
		return nil, fmt.Errorf("coordinator: get conn for %s: %w", owner, err)
	}

	client := pb.NewKVServiceClient(conn)
	resp, err := client.Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		return nil, fmt.Errorf("coordinator: forward get to %s: %w", owner, err)
	}

	c.logger.Debug("get forwarded", "key", key, "owner", owner)

	if !resp.GetFound() {
		return nil, nil
	}

	return &store.Record{
		Value:     resp.GetValue(),
		Timestamp: resp.GetTimestamp(),
	}, nil
}

// Delete removes a key. If this node is the owner, it deletes locally
// (tombstone write). Otherwise it forwards the DeleteRequest to the owner.
func (c *Coordinator) Delete(ctx context.Context, key string) (bool, error) {
	owner := c.ring.GetNode(key)

	if owner == c.nodeID {
		ts := time.Now().UnixNano()
		if err := c.store.Delete(key, ts); err != nil {
			return false, fmt.Errorf("coordinator: local delete: %w", err)
		}
		c.logger.Debug("delete local", "key", key)
		return true, nil
	}

	// Forward to the owner node.
	conn, err := c.membership.GetConn(owner)
	if err != nil {
		return false, fmt.Errorf("coordinator: get conn for %s: %w", owner, err)
	}

	client := pb.NewKVServiceClient(conn)
	resp, err := client.Delete(ctx, &pb.DeleteRequest{Key: key})
	if err != nil {
		return false, fmt.Errorf("coordinator: forward delete to %s: %w", owner, err)
	}

	c.logger.Debug("delete forwarded", "key", key, "owner", owner)
	return resp.GetDeleted(), nil
}
