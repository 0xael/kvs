// Package coordinator routes KV requests to the correct nodes in the cluster.
// Phase 3: fan-out to N replicas with W/R quorum and read repair (LWW).
// Phase 4: hinted handoff — failed writes are stored as hints and replayed
// when the target node recovers.
package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	pb "github.com/alyelalwany/ghostkv/gen/ghostkv/v1"
	"github.com/alyelalwany/ghostkv/internal/config"
	"github.com/alyelalwany/ghostkv/internal/hintedhandoff"
	"github.com/alyelalwany/ghostkv/internal/membership"
	"github.com/alyelalwany/ghostkv/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Ring is the interface the coordinator uses to look up key ownership.
// The real implementation lives in internal/ring.
type Ring interface {
	GetNode(key string) string
	GetNodes(key string, n int) []string
}

// HintStore is the interface for storing hinted-handoff hints.
// Using an interface allows tests to substitute a mock.
type HintStore interface {
	AddHint(hint *hintedhandoff.Hint) error
}

// Coordinator routes KV operations to the appropriate nodes based on the
// consistent hash ring. Phase 3: fan-out to N replicas with quorum.
// Phase 4: hinted handoff for failed writes.
type Coordinator struct {
	nodeID     string
	ring       Ring
	store      store.Store
	membership *membership.Membership
	cfg        *config.ClusterSettings
	logger     *slog.Logger
	hintStore  HintStore // nil = no hinted handoff (backward compatible)
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

// SetHintStore sets the hint store for hinted handoff. Pass nil to disable.
func (c *Coordinator) SetHintStore(hs HintStore) {
	c.hintStore = hs
}

// writeResult captures the outcome of a single write (put/delete) to one node.
type writeResult struct {
	nodeID string
	err    error
}

// readResult captures the outcome of a single read from one node.
type readResult struct {
	nodeID string
	rec    *store.Record // nil means not found
	err    error
}

// Put stores a key-value pair. It fans out writes to all N replicas in the
// preference list and waits for W acknowledgements (write quorum).
func (c *Coordinator) Put(ctx context.Context, key string, value []byte) (int32, error) {
	ts := time.Now().UnixNano()
	nodes := c.ring.GetNodes(key, c.cfg.ReplicationFactor)

	if len(nodes) == 0 {
		return 0, status.Error(codes.Unavailable, "coordinator: no nodes in ring")
	}

	results := make(chan writeResult, len(nodes))
	var wg sync.WaitGroup

	for _, node := range nodes {
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()
			var err error
			if nodeID == c.nodeID {
				err = c.localPut(key, value, ts)
			} else {
				err = c.remotePut(ctx, nodeID, key, value, ts, false)
			}
			results <- writeResult{nodeID: nodeID, err: err}
		}(node)
	}

	// Close results channel once all goroutines finish.
	go func() {
		wg.Wait()
		close(results)
	}()

	acks := int32(0)
	var firstErr error
	for res := range results {
		if res.err == nil {
			acks++
		} else {
			c.logger.Warn("put replica failed", "key", key, "node", res.nodeID, "error", res.err)
			if firstErr == nil {
				firstErr = res.err
			}
			// Store a hint for the failed remote node so the write can be
			// replayed when the node recovers.
			if res.nodeID != c.nodeID && c.hintStore != nil {
				c.storeHint(res.nodeID, key, value, ts, false)
			}
		}
	}

	if int(acks) >= c.cfg.WriteQuorum {
		c.logger.Debug("put quorum met", "key", key, "acks", acks, "quorum", c.cfg.WriteQuorum)
		return acks, nil
	}

	return acks, status.Errorf(codes.Unavailable,
		"coordinator: put quorum not met: got %d acks, need %d: %v", acks, c.cfg.WriteQuorum, firstErr)
}

// Get retrieves a value by key. It fans out reads to all N replicas and
// waits for R responses (read quorum). Uses LWW for conflict resolution
// and triggers read repair for stale replicas.
func (c *Coordinator) Get(ctx context.Context, key string) (*store.Record, error) {
	nodes := c.ring.GetNodes(key, c.cfg.ReplicationFactor)

	if len(nodes) == 0 {
		return nil, status.Error(codes.Unavailable, "coordinator: no nodes in ring")
	}

	results := make(chan readResult, len(nodes))
	var wg sync.WaitGroup

	for _, node := range nodes {
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()
			var rec *store.Record
			var err error
			if nodeID == c.nodeID {
				rec, err = c.store.Get(key)
			} else {
				rec, err = c.remoteGet(ctx, nodeID, key)
			}
			results <- readResult{nodeID: nodeID, rec: rec, err: err}
		}(node)
	}

	// Close results channel once all goroutines finish.
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect responses until we have R successful ones.
	var responses []readResult
	var firstErr error
	for res := range results {
		if res.err == nil {
			responses = append(responses, res)
		} else {
			c.logger.Warn("get replica failed", "key", key, "node", res.nodeID, "error", res.err)
			if firstErr == nil {
				firstErr = res.err
			}
		}
	}

	if len(responses) < c.cfg.ReadQuorum {
		return nil, status.Errorf(codes.Unavailable,
			"coordinator: read quorum not met: got %d responses, need %d: %v",
			len(responses), c.cfg.ReadQuorum, firstErr)
	}

	// LWW resolution: pick the record with the highest timestamp.
	winner := lwwResolve(responses)

	// Read repair: asynchronously send the winning version to stale replicas.
	if winner.rec != nil {
		c.readRepair(key, winner, responses)
	}

	c.logger.Debug("get quorum met", "key", key, "responses", len(responses), "quorum", c.cfg.ReadQuorum)

	// Return nil if winner is a tombstone.
	if winner.rec != nil && winner.rec.IsDelete {
		return nil, nil
	}

	return winner.rec, nil
}

// Delete removes a key by writing a tombstone. It fans out to all N replicas
// and waits for W acknowledgements (write quorum).
func (c *Coordinator) Delete(ctx context.Context, key string) (bool, error) {
	ts := time.Now().UnixNano()
	nodes := c.ring.GetNodes(key, c.cfg.ReplicationFactor)

	if len(nodes) == 0 {
		return false, status.Error(codes.Unavailable, "coordinator: no nodes in ring")
	}

	results := make(chan writeResult, len(nodes))
	var wg sync.WaitGroup

	for _, node := range nodes {
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()
			var err error
			if nodeID == c.nodeID {
				err = c.store.Delete(key, ts)
			} else {
				err = c.remotePut(ctx, nodeID, key, nil, ts, true)
			}
			results <- writeResult{nodeID: nodeID, err: err}
		}(node)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	acks := int32(0)
	var firstErr error
	for res := range results {
		if res.err == nil {
			acks++
		} else {
			c.logger.Warn("delete replica failed", "key", key, "node", res.nodeID, "error", res.err)
			if firstErr == nil {
				firstErr = res.err
			}
			// Store a hint for the failed remote node (delete = tombstone).
			if res.nodeID != c.nodeID && c.hintStore != nil {
				c.storeHint(res.nodeID, key, nil, ts, true)
			}
		}
	}

	if int(acks) >= c.cfg.WriteQuorum {
		c.logger.Debug("delete quorum met", "key", key, "acks", acks, "quorum", c.cfg.WriteQuorum)
		return true, nil
	}

	return false, status.Errorf(codes.Unavailable,
		"coordinator: delete quorum not met: got %d acks, need %d: %v", acks, c.cfg.WriteQuorum, firstErr)
}

// --- internal helpers ---

// storeHint persists a hint for a failed write to the hint store.
// Errors are logged but not propagated (hints are best-effort).
func (c *Coordinator) storeHint(targetNodeID, key string, value []byte, ts int64, isDelete bool) {
	hint := &hintedhandoff.Hint{
		TargetNodeID: targetNodeID,
		Key:          key,
		Value:        value,
		Timestamp:    ts,
		IsDelete:     isDelete,
		CreatedAt:    time.Now().UnixNano(),
	}
	if err := c.hintStore.AddHint(hint); err != nil {
		c.logger.Error("failed to store hint",
			"target", targetNodeID,
			"key", key,
			"error", err,
		)
	} else {
		c.logger.Info("hint stored for offline node",
			"target", targetNodeID,
			"key", key,
			"is_delete", isDelete,
		)
	}
}

// localPut writes a record to the local store.
func (c *Coordinator) localPut(key string, value []byte, ts int64) error {
	rec := &store.Record{
		Value:     value,
		Timestamp: ts,
	}
	return c.store.Put(key, rec)
}

// remotePut sends a Replicate RPC to a remote node.
func (c *Coordinator) remotePut(ctx context.Context, nodeID, key string, value []byte, ts int64, isDelete bool) error {
	conn, err := c.membership.GetConn(nodeID)
	if err != nil {
		return fmt.Errorf("get conn for %s: %w", nodeID, err)
	}

	client := pb.NewInternalServiceClient(conn)
	_, err = client.Replicate(ctx, &pb.ReplicateRequest{
		Key:       key,
		Value:     value,
		Timestamp: ts,
		IsDelete:  isDelete,
	})
	if err != nil {
		return fmt.Errorf("replicate to %s: %w", nodeID, err)
	}

	return nil
}

// remoteGet reads a key from a remote node via KVService.Get with the
// x-ghostkv-local-only metadata header to prevent recursive coordinator
// fan-out on the remote node.
func (c *Coordinator) remoteGet(ctx context.Context, nodeID, key string) (*store.Record, error) {
	conn, err := c.membership.GetConn(nodeID)
	if err != nil {
		return nil, fmt.Errorf("get conn for %s: %w", nodeID, err)
	}

	// Attach local-only header to prevent recursive coordinator fan-out.
	ctx = metadata.AppendToOutgoingContext(ctx, "x-ghostkv-local-only", "true")

	client := pb.NewKVServiceClient(conn)
	resp, err := client.Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		return nil, fmt.Errorf("get from %s: %w", nodeID, err)
	}

	if !resp.GetFound() {
		return nil, nil
	}

	return &store.Record{
		Value:     resp.GetValue(),
		Timestamp: resp.GetTimestamp(),
	}, nil
}

// lwwResolve picks the response with the highest timestamp (last-writer-wins).
// A non-nil record always wins over nil (not-found). Among non-nil records,
// the highest timestamp wins.
func lwwResolve(responses []readResult) readResult {
	best := responses[0]
	for _, r := range responses[1:] {
		if best.rec == nil && r.rec != nil {
			best = r
		} else if best.rec != nil && r.rec != nil && r.rec.Timestamp > best.rec.Timestamp {
			best = r
		}
	}
	return best
}

// readRepair sends the winning record to any replica that had a stale or
// missing version. It fires Replicate RPCs asynchronously (fire-and-forget).
func (c *Coordinator) readRepair(key string, winner readResult, responses []readResult) {
	for _, r := range responses {
		if r.nodeID == winner.nodeID {
			continue
		}
		// Stale if: the replica had no record but the winner does, or its
		// timestamp is older than the winner's.
		stale := r.rec == nil || r.rec.Timestamp < winner.rec.Timestamp
		if !stale {
			continue
		}

		go func(nodeID string) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			var err error
			if nodeID == c.nodeID {
				err = c.store.Put(key, winner.rec)
			} else {
				err = c.remotePut(ctx, nodeID, key, winner.rec.Value, winner.rec.Timestamp, winner.rec.IsDelete)
			}
			if err != nil {
				c.logger.Warn("read repair failed", "key", key, "node", nodeID, "error", err)
			} else {
				c.logger.Debug("read repair sent", "key", key, "node", nodeID, "ts", winner.rec.Timestamp)
			}
		}(r.nodeID)
	}
}
