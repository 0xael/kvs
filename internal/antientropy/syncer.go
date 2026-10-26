package antientropy

import (
	"context"
	"log/slog"
	"time"

	pb "github.com/alyelalwany/ghostkv/gen/ghostkv/v1"
	"github.com/alyelalwany/ghostkv/internal/store"
	"google.golang.org/grpc"
)

// Ring abstracts the consistent-hash ring for the Syncer.
type Ring interface {
	GetNodes(key string, n int) []string
	Members() []string
}

// MembershipChecker abstracts the membership/health subsystem.
type MembershipChecker interface {
	IsHealthy(nodeID string) bool
	GetConn(nodeID string) (*grpc.ClientConn, error)
}

// Syncer runs the periodic anti-entropy sync loop.
type Syncer struct {
	nodeID     string
	ring       Ring
	store      store.Store
	membership MembershipChecker
	interval   time.Duration
	depth      int
	logger     *slog.Logger
}

// NewSyncer creates a new anti-entropy Syncer.
func NewSyncer(nodeID string, ring Ring, s store.Store, m MembershipChecker, interval time.Duration) *Syncer {
	return &Syncer{
		nodeID:     nodeID,
		ring:       ring,
		store:      s,
		membership: m,
		interval:   interval,
		depth:      10,
		logger:     slog.Default().With("component", "antientropy"),
	}
}

// Run starts the sync loop.  It blocks until ctx is cancelled.
func (sy *Syncer) Run(ctx context.Context) {
	sy.logger.Info("anti-entropy syncer started", "interval", sy.interval, "depth", sy.depth)

	// Run once immediately, then on the ticker.
	sy.syncOnce(ctx)

	ticker := time.NewTicker(sy.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			sy.logger.Info("anti-entropy syncer stopped")
			return
		case <-ticker.C:
			sy.syncOnce(ctx)
		}
	}
}

// syncOnce performs one full round of anti-entropy with all healthy peers.
func (sy *Syncer) syncOnce(ctx context.Context) {
	members := sy.ring.Members()
	for _, peerID := range members {
		if peerID == sy.nodeID {
			continue
		}
		if !sy.membership.IsHealthy(peerID) {
			sy.logger.Debug("skipping unhealthy peer", "peer", peerID)
			continue
		}

		if err := sy.syncWithPeer(ctx, peerID); err != nil {
			sy.logger.Warn("sync with peer failed", "peer", peerID, "error", err)
		}
	}
}

// syncWithPeer runs a Merkle-tree comparison against a single peer and
// repairs any divergent ranges.
func (sy *Syncer) syncWithPeer(ctx context.Context, peerID string) error {
	// 1. Build the local Merkle tree.
	localTree, err := BuildTree(sy.store, "", "", sy.depth)
	if err != nil {
		return err
	}

	// 2. Fetch the remote Merkle tree via gRPC.
	conn, err := sy.membership.GetConn(peerID)
	if err != nil {
		return err
	}
	client := pb.NewInternalServiceClient(conn)

	resp, err := client.GetMerkleTree(ctx, &pb.MerkleTreeRequest{
		KeyRangeStart: "",
		KeyRangeEnd:   "",
		Depth:         int32(sy.depth),
	})
	if err != nil {
		return err
	}

	remoteRoot := FromProto(resp.GetRoot())

	// 3. Diff the two trees.
	divergent := DiffRanges(localTree.Root, remoteRoot)
	if len(divergent) == 0 {
		sy.logger.Debug("no divergence", "peer", peerID)
		return nil
	}

	sy.logger.Info("found divergent ranges",
		"peer", peerID,
		"ranges", len(divergent),
	)

	// 4. For each divergent range, fetch remote keys and reconcile.
	var syncedKeys int
	for _, kr := range divergent {
		n, err := sy.repairRange(ctx, client, peerID, kr)
		if err != nil {
			sy.logger.Warn("repair range failed",
				"peer", peerID,
				"range_start", kr.Start,
				"range_end", kr.End,
				"error", err,
			)
			continue
		}
		syncedKeys += n
	}

	sy.logger.Info("sync complete",
		"peer", peerID,
		"divergent_ranges", len(divergent),
		"synced_keys", syncedKeys,
	)
	return nil
}

// repairRange fetches all keys in the given range from the remote peer,
// applies LWW reconciliation, and pushes newer local keys back.
func (sy *Syncer) repairRange(ctx context.Context, client pb.InternalServiceClient, peerID string, kr KeyRange) (int, error) {
	resp, err := client.SyncKeys(ctx, &pb.SyncKeysRequest{
		KeyRangeStart: kr.Start,
		KeyRangeEnd:   kr.End,
	})
	if err != nil {
		return 0, err
	}

	var synced int
	for _, entry := range resp.GetEntries() {
		localRec, err := sy.store.Get(entry.GetKey())
		if err != nil {
			sy.logger.Warn("get local key failed", "key", entry.GetKey(), "error", err)
			continue
		}

		if localRec == nil || entry.GetTimestamp() > localRec.Timestamp {
			// Remote is newer (or local doesn't have this key) — apply remote.
			rec := &store.Record{
				Value:     entry.GetValue(),
				Timestamp: entry.GetTimestamp(),
				IsDelete:  entry.GetIsDelete(),
			}
			if err := sy.store.Put(entry.GetKey(), rec); err != nil {
				sy.logger.Warn("put synced key failed", "key", entry.GetKey(), "error", err)
				continue
			}
			synced++
		} else if localRec.Timestamp > entry.GetTimestamp() {
			// Local is newer — push to remote via Replicate.
			_, err := client.Replicate(ctx, &pb.ReplicateRequest{
				Key:       entry.GetKey(),
				Value:     localRec.Value,
				Timestamp: localRec.Timestamp,
				IsDelete:  localRec.IsDelete,
			})
			if err != nil {
				sy.logger.Warn("replicate to peer failed",
					"peer", peerID,
					"key", entry.GetKey(),
					"error", err,
				)
			}
			synced++
		}
		// If timestamps are equal, data is consistent — skip.
	}

	return synced, nil
}
