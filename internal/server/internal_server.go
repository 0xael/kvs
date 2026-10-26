package server

import (
	"context"
	"log/slog"
	"time"

	pb "github.com/alyelalwany/ghostkv/gen/ghostkv/v1"
	"github.com/alyelalwany/ghostkv/internal/antientropy"
	"github.com/alyelalwany/ghostkv/internal/hintedhandoff"
	"github.com/alyelalwany/ghostkv/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// HintAdder is the interface for adding hinted-handoff hints.
type HintAdder interface {
	AddHint(hint *hintedhandoff.Hint) error
}

// InternalServer implements the InternalService gRPC interface.
// Replicate, DeliverHint, and HintedReplicate are implemented;
// the remaining methods use the embedded unimplemented stubs.
type InternalServer struct {
	pb.UnimplementedInternalServiceServer
	store     store.Store
	hintStore HintAdder // nil = HintedReplicate returns Unimplemented
}

// NewInternalServer creates a new InternalServer.
func NewInternalServer(s store.Store) *InternalServer {
	return &InternalServer{store: s}
}

// SetHintStore sets the hint store for HintedReplicate support.
func (s *InternalServer) SetHintStore(hs HintAdder) {
	s.hintStore = hs
}

// Replicate writes a replicated record directly to the local store.
func (s *InternalServer) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	rec := &store.Record{
		Value:     req.GetValue(),
		Timestamp: req.GetTimestamp(),
		IsDelete:  req.GetIsDelete(),
	}

	if err := s.store.Put(req.GetKey(), rec); err != nil {
		slog.Error("replicate failed", "key", req.GetKey(), "error", err)
		return nil, status.Errorf(codes.Internal, "replicate failed: %v", err)
	}

	slog.Debug("replicate", "key", req.GetKey(), "is_delete", req.GetIsDelete())
	return &pb.ReplicateResponse{Success: true}, nil
}

// DeliverHint writes a hinted-handoff record directly to the local store.
func (s *InternalServer) DeliverHint(ctx context.Context, req *pb.DeliverHintRequest) (*pb.DeliverHintResponse, error) {
	rec := &store.Record{
		Value:     req.GetValue(),
		Timestamp: req.GetTimestamp(),
		IsDelete:  req.GetIsDelete(),
	}

	if err := s.store.Put(req.GetKey(), rec); err != nil {
		slog.Error("deliver hint failed", "key", req.GetKey(), "error", err)
		return nil, status.Errorf(codes.Internal, "deliver hint failed: %v", err)
	}

	slog.Debug("deliver_hint", "key", req.GetKey(), "is_delete", req.GetIsDelete())
	return &pb.DeliverHintResponse{Success: true}, nil
}

// HintedReplicate accepts a hinted write from another node and stores it
// locally for later delivery to the target node.
func (s *InternalServer) HintedReplicate(ctx context.Context, req *pb.HintedReplicateRequest) (*pb.HintedReplicateResponse, error) {
	if s.hintStore == nil {
		return nil, status.Error(codes.Unimplemented, "hinted handoff not configured")
	}

	hint := &hintedhandoff.Hint{
		TargetNodeID: req.GetTargetNodeId(),
		Key:          req.GetKey(),
		Value:        req.GetValue(),
		Timestamp:    req.GetTimestamp(),
		IsDelete:     req.GetIsDelete(),
		CreatedAt:    time.Now().UnixNano(),
	}

	if err := s.hintStore.AddHint(hint); err != nil {
		slog.Error("hinted replicate failed", "target", req.GetTargetNodeId(), "key", req.GetKey(), "error", err)
		return nil, status.Errorf(codes.Internal, "hinted replicate failed: %v", err)
	}

	slog.Debug("hinted_replicate", "target", req.GetTargetNodeId(), "key", req.GetKey(), "is_delete", req.GetIsDelete())
	return &pb.HintedReplicateResponse{Success: true}, nil
}

// GetMerkleTree builds a Merkle tree from the local store for the
// requested key range and depth, and returns it as a proto.
func (s *InternalServer) GetMerkleTree(ctx context.Context, req *pb.MerkleTreeRequest) (*pb.MerkleTreeResponse, error) {
	depth := int(req.GetDepth())
	if depth <= 0 {
		depth = 10 // default
	}

	tree, err := antientropy.BuildTree(s.store, req.GetKeyRangeStart(), req.GetKeyRangeEnd(), depth)
	if err != nil {
		slog.Error("build merkle tree failed", "error", err)
		return nil, status.Errorf(codes.Internal, "build merkle tree: %v", err)
	}

	return &pb.MerkleTreeResponse{Root: tree.ToProto()}, nil
}

// SyncKeys returns all key-value entries in the requested range from
// the local store.
func (s *InternalServer) SyncKeys(ctx context.Context, req *pb.SyncKeysRequest) (*pb.SyncKeysResponse, error) {
	var entries []*pb.KeyValueEntry

	err := s.store.ForEachInRange(req.GetKeyRangeStart(), req.GetKeyRangeEnd(), func(key string, rec *store.Record) error {
		entries = append(entries, &pb.KeyValueEntry{
			Key:       key,
			Value:     rec.Value,
			Timestamp: rec.Timestamp,
			IsDelete:  rec.IsDelete,
		})
		return nil
	})
	if err != nil {
		slog.Error("sync keys failed", "error", err)
		return nil, status.Errorf(codes.Internal, "sync keys: %v", err)
	}

	return &pb.SyncKeysResponse{Entries: entries}, nil
}
