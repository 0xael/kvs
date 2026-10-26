// Command ghostkv-server starts a GhostKV node.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	pb "github.com/alyelalwany/ghostkv/gen/ghostkv/v1"
	"github.com/alyelalwany/ghostkv/internal/config"
	"github.com/alyelalwany/ghostkv/internal/server"
	"github.com/alyelalwany/ghostkv/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	nodeID := flag.String("node-id", "", "node ID (required, must match a nodes[].id in config)")
	configPath := flag.String("config", "config/cluster.yaml", "path to cluster config file")
	port := flag.Int("port", 0, "port override (0 = use address from config)")
	flag.Parse()

	if *nodeID == "" {
		fmt.Fprintln(os.Stderr, "error: --node-id is required")
		flag.Usage()
		os.Exit(1)
	}

	// Structured logging.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))

	// Load cluster configuration.
	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("failed to load config", "path", *configPath, "error", err)
		os.Exit(1)
	}

	nodeCfg, err := cfg.FindNode(*nodeID)
	if err != nil {
		slog.Error("node not found in config", "node_id", *nodeID, "error", err)
		os.Exit(1)
	}

	// Determine listen address.
	listenAddr := nodeCfg.Address
	if *port != 0 {
		listenAddr = fmt.Sprintf(":%d", *port)
	}

	// Open the store.
	// The bbolt implementation will be provided by another agent.
	// For now, we declare the variable so the code compiles.
	var kvStore store.Store
	_ = nodeCfg.DataDir // will be used to open bbolt in the real implementation

	// This is a compile-time placeholder; the real store.Open call will go here.
	// TODO: kvStore, err = boltstore.Open(nodeCfg.DataDir)
	if kvStore == nil {
		slog.Error("store not initialized: bbolt implementation not yet available")
		os.Exit(1)
	}
	defer func() {
		if err := kvStore.Close(); err != nil {
			slog.Error("failed to close store", "error", err)
		}
	}()

	// Create gRPC server.
	grpcServer := grpc.NewServer()

	// Register services.
	kvSrv := server.NewKVServer(kvStore, cfg, *nodeID)
	pb.RegisterKVServiceServer(grpcServer, kvSrv)

	intSrv := server.NewInternalServer(kvStore)
	pb.RegisterInternalServiceServer(grpcServer, intSrv)

	// Register gRPC health service.
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthSrv)
	healthSrv.SetServingStatus("ghostkv.v1.KVService", healthpb.HealthCheckResponse_SERVING)
	healthSrv.SetServingStatus("ghostkv.v1.InternalService", healthpb.HealthCheckResponse_SERVING)

	// Listen.
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		slog.Error("failed to listen", "address", listenAddr, "error", err)
		os.Exit(1)
	}

	slog.Info("ghostkv-server starting",
		"node_id", *nodeID,
		"address", listenAddr,
		"cluster", cfg.Cluster.Name,
	)

	// Graceful shutdown on SIGINT/SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		slog.Info("shutting down gracefully")
		healthSrv.SetServingStatus("ghostkv.v1.KVService", healthpb.HealthCheckResponse_NOT_SERVING)
		healthSrv.SetServingStatus("ghostkv.v1.InternalService", healthpb.HealthCheckResponse_NOT_SERVING)
		grpcServer.GracefulStop()
	}()

	if err := grpcServer.Serve(lis); err != nil {
		slog.Error("server stopped with error", "error", err)
		os.Exit(1)
	}

	slog.Info("server stopped")
}
