package server

import (
	"context"
	"log/slog"

	pb "github.com/alyelalwany/ghostkv/gen/ghostkv/v1"
	"github.com/alyelalwany/ghostkv/internal/antientropy"
	"github.com/alyelalwany/ghostkv/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// InternalServer implements the InternalService gRPC interface.
// For Phase 1, only Replicate and DeliverHint are implemented;
// the remaining methods use the embedded unimplemented stubs.
type InternalServer struct {
	pb.UnimplementedInternalServiceServer
	store store.Store
}

// NewInternalServer creates a new InternalServer.
func NewInternalServer(s store.Store) *InternalServer {
	return &InternalServer{store: s}
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
