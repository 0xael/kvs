// Package server implements the gRPC service handlers for GhostKV.
package server

import (
	"context"
	"log/slog"

	pb "github.com/alyelalwany/ghostkv/gen/ghostkv/v1"
	"github.com/alyelalwany/ghostkv/internal/config"
	"github.com/alyelalwany/ghostkv/internal/coordinator"
	"github.com/alyelalwany/ghostkv/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// KVServer implements the KVService gRPC interface.
// For Phase 2, it delegates to the coordinator for request routing.
type KVServer struct {
	pb.UnimplementedKVServiceServer
	coord  *coordinator.Coordinator
	store  store.Store // still needed for Status (key count)
	cfg    *config.ClusterConfig
	nodeID string
}

// NewKVServer creates a new KVServer that delegates to the given coordinator.
func NewKVServer(coord *coordinator.Coordinator, s store.Store, cfg *config.ClusterConfig, nodeID string) *KVServer {
	return &KVServer{
		coord:  coord,
		store:  s,
		cfg:    cfg,
		nodeID: nodeID,
	}
}

// Put stores a key-value pair, routing to the correct node via the coordinator.
func (s *KVServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	acks, err := s.coord.Put(ctx, req.GetKey(), req.GetValue())
	if err != nil {
		slog.Error("put failed", "key", req.GetKey(), "error", err)
		return nil, status.Errorf(codes.Internal, "put failed: %v", err)
	}

	slog.Debug("put", "key", req.GetKey(), "size", len(req.GetValue()), "acks", acks)
	return &pb.PutResponse{Acks: acks}, nil
}

// Get retrieves a value by key, routing to the correct node via the coordinator.
func (s *KVServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	rec, err := s.coord.Get(ctx, req.GetKey())
	if err != nil {
		slog.Error("get failed", "key", req.GetKey(), "error", err)
		return nil, status.Errorf(codes.Internal, "get failed: %v", err)
	}

	if rec == nil || rec.IsDelete {
		return &pb.GetResponse{Found: false}, nil
	}

	slog.Debug("get", "key", req.GetKey(), "found", true)
	return &pb.GetResponse{
		Value:     rec.Value,
		Found:     true,
		Timestamp: rec.Timestamp,
	}, nil
}

// Delete removes a key, routing to the correct node via the coordinator.
func (s *KVServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	deleted, err := s.coord.Delete(ctx, req.GetKey())
	if err != nil {
		slog.Error("delete failed", "key", req.GetKey(), "error", err)
		return nil, status.Errorf(codes.Internal, "delete failed: %v", err)
	}

	slog.Debug("delete", "key", req.GetKey(), "deleted", deleted)
	return &pb.DeleteResponse{Deleted: deleted}, nil
}

// Status returns cluster status information.
func (s *KVServer) Status(ctx context.Context, _ *pb.StatusRequest) (*pb.StatusResponse, error) {
	keyCount, err := s.store.KeyCount()
	if err != nil {
		slog.Error("key count failed", "error", err)
		return nil, status.Errorf(codes.Internal, "key count failed: %v", err)
	}

	nodeCfg, err := s.cfg.FindNode(s.nodeID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "node config lookup failed: %v", err)
	}

	node := &pb.NodeInfo{
		NodeId:       s.nodeID,
		Address:      nodeCfg.Address,
		Healthy:      true,
		VirtualNodes: int32(s.cfg.Cluster.VirtualNodes),
		KeyCount:     keyCount,
	}

	return &pb.StatusResponse{
		Nodes:             []*pb.NodeInfo{node},
		ReplicationFactor: int32(s.cfg.Cluster.ReplicationFactor),
		WriteQuorum:       int32(s.cfg.Cluster.WriteQuorum),
		ReadQuorum:        int32(s.cfg.Cluster.ReadQuorum),
	}, nil
}
