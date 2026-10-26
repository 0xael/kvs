// Package hintedhandoff implements the hint storage and replay mechanism
// for GhostKV's hinted handoff protocol. When a replica is unreachable,
// the coordinator stores a "hint" locally. A background loop replays
// hints to recovered nodes.
package hintedhandoff

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	pb "github.com/alyelalwany/ghostkv/gen/ghostkv/v1"
	"github.com/alyelalwany/ghostkv/internal/membership"
	bolt "go.etcd.io/bbolt"
)

var hintsBucket = []byte("hints")

// Hint represents a write that could not be delivered to its target node.
type Hint struct {
	TargetNodeID string `json:"target_node_id"`
	Key          string `json:"key"`
	Value        []byte `json:"value"`
	Timestamp    int64  `json:"timestamp"`
	IsDelete     bool   `json:"is_delete"`
	CreatedAt    int64  `json:"created_at"`
}

// HintStore persists hints in a separate bbolt database and replays
// them to recovered nodes.
type HintStore struct {
	db         *bolt.DB
	membership *membership.Membership
	logger     *slog.Logger
	maxAge     time.Duration // hints older than this are garbage collected
}

// NewHintStore opens (or creates) the bbolt database at path for hint storage.
func NewHintStore(path string, m *membership.Membership) (*HintStore, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("hintstore: open db %s: %w", path, err)
	}

	// Ensure the hints bucket exists.
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(hintsBucket)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("hintstore: create bucket: %w", err)
	}

	return &HintStore{
		db:         db,
		membership: m,
		logger:     slog.Default().With("component", "hintstore"),
		maxAge:     3 * time.Hour,
	}, nil
}

// AddHint persists a hint for later replay.
func (hs *HintStore) AddHint(hint *Hint) error {
	if hint.CreatedAt == 0 {
		hint.CreatedAt = time.Now().UnixNano()
	}

	data, err := json.Marshal(hint)
	if err != nil {
		return fmt.Errorf("hintstore: marshal hint: %w", err)
	}

	// Key format: {targetNodeID}:{createdAt}:{key}
	// This groups hints by target and orders them chronologically.
	boltKey := fmt.Sprintf("%s:%020d:%s", hint.TargetNodeID, hint.CreatedAt, hint.Key)

	return hs.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(hintsBucket).Put([]byte(boltKey), data)
	})
}

// Run starts the background replay and GC loop. It blocks until ctx is cancelled.
func (hs *HintStore) Run(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	hs.logger.Info("hint replay loop started")

	for {
		select {
		case <-ctx.Done():
			hs.logger.Info("hint replay loop stopped")
			return
		case <-ticker.C:
			hs.replayAll(ctx)
			hs.gc()
		}
	}
}

// replayAll attempts to deliver hints to any target nodes that have recovered.
func (hs *HintStore) replayAll(ctx context.Context) {
	// Collect unique target node IDs that have pending hints.
	targets, err := hs.pendingTargets()
	if err != nil {
		hs.logger.Error("failed to scan hint targets", "error", err)
		return
	}

	for _, target := range targets {
		if !hs.membership.IsHealthy(target) {
			continue
		}
		hs.replayForTarget(ctx, target)
	}
}

// pendingTargets returns unique target node IDs found in the hints bucket.
func (hs *HintStore) pendingTargets() ([]string, error) {
	seen := make(map[string]struct{})
	err := hs.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(hintsBucket)
		return b.ForEach(func(k, _ []byte) error {
			parts := strings.SplitN(string(k), ":", 3)
			if len(parts) >= 1 {
				seen[parts[0]] = struct{}{}
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	targets := make([]string, 0, len(seen))
	for t := range seen {
		targets = append(targets, t)
	}
	return targets, nil
}

// replayForTarget delivers all hints for a specific target node.
// Stops on first delivery failure (retry next cycle).
func (hs *HintStore) replayForTarget(ctx context.Context, targetNodeID string) {
	conn, err := hs.membership.GetConn(targetNodeID)
	if err != nil {
		hs.logger.Warn("cannot get conn for hint replay", "target", targetNodeID, "error", err)
		return
	}
	client := pb.NewInternalServiceClient(conn)

	prefix := targetNodeID + ":"

	// Collect keys to replay in order.
	type entry struct {
		boltKey []byte
		hint    Hint
	}
	var entries []entry

	if err := hs.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(hintsBucket)
		c := b.Cursor()
		for k, v := c.Seek([]byte(prefix)); k != nil && strings.HasPrefix(string(k), prefix); k, v = c.Next() {
			var h Hint
			if err := json.Unmarshal(v, &h); err != nil {
				hs.logger.Warn("skip corrupt hint", "key", string(k), "error", err)
				continue
			}
			keyCopy := make([]byte, len(k))
			copy(keyCopy, k)
			entries = append(entries, entry{boltKey: keyCopy, hint: h})
		}
		return nil
	}); err != nil {
		hs.logger.Error("failed to read hints for replay", "target", targetNodeID, "error", err)
		return
	}

	replayed := 0
	for _, e := range entries {
		deliverCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := client.DeliverHint(deliverCtx, &pb.DeliverHintRequest{
			Key:       e.hint.Key,
			Value:     e.hint.Value,
			Timestamp: e.hint.Timestamp,
			IsDelete:  e.hint.IsDelete,
		})
		cancel()

		if err != nil {
			hs.logger.Warn("hint delivery failed, will retry",
				"target", targetNodeID,
				"key", e.hint.Key,
				"error", err,
			)
			break // stop replaying for this target; retry next cycle
		}

		// Delete the delivered hint.
		if err := hs.db.Update(func(tx *bolt.Tx) error {
			return tx.Bucket(hintsBucket).Delete(e.boltKey)
		}); err != nil {
			hs.logger.Error("failed to delete replayed hint", "key", string(e.boltKey), "error", err)
		}
		replayed++
	}

	if replayed > 0 {
		hs.logger.Info("replayed hints", "target", targetNodeID, "count", replayed)
	}
}

// gc removes hints older than maxAge.
func (hs *HintStore) gc() {
	cutoff := time.Now().Add(-hs.maxAge).UnixNano()

	var staleKeys [][]byte

	if err := hs.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(hintsBucket)
		return b.ForEach(func(k, v []byte) error {
			var h Hint
			if err := json.Unmarshal(v, &h); err != nil {
				// Corrupt hint — collect for deletion.
				keyCopy := make([]byte, len(k))
				copy(keyCopy, k)
				staleKeys = append(staleKeys, keyCopy)
				return nil
			}
			if h.CreatedAt < cutoff {
				keyCopy := make([]byte, len(k))
				copy(keyCopy, k)
				staleKeys = append(staleKeys, keyCopy)
			}
			return nil
		})
	}); err != nil {
		hs.logger.Error("hint gc scan failed", "error", err)
		return
	}

	if len(staleKeys) == 0 {
		return
	}

	if err := hs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(hintsBucket)
		for _, k := range staleKeys {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		hs.logger.Error("hint gc delete failed", "error", err)
		return
	}

	hs.logger.Info("hint gc cleaned", "count", len(staleKeys))
}

// PendingCount returns the total number of pending hints.
func (hs *HintStore) PendingCount() (int64, error) {
	var count int64
	err := hs.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(hintsBucket)
		count = int64(b.Stats().KeyN)
		return nil
	})
	return count, err
}

// Close closes the underlying bbolt database.
func (hs *HintStore) Close() error {
	return hs.db.Close()
}
