//go:build integration

package internal_test

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/alyelalwany/ghostkv/gen/ghostkv/v1"
	"github.com/alyelalwany/ghostkv/internal/config"
	"github.com/alyelalwany/ghostkv/internal/coordinator"
	"github.com/alyelalwany/ghostkv/internal/hintedhandoff"
	"github.com/alyelalwany/ghostkv/internal/membership"
	"github.com/alyelalwany/ghostkv/internal/ring"
	"github.com/alyelalwany/ghostkv/internal/server"
	"github.com/alyelalwany/ghostkv/internal/store"
	"github.com/alyelalwany/ghostkv/pkg/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// portBlock atomically allocates a block of 3 consecutive ports so that
// parallel tests never collide. Starting at 17001.
var portBlock atomic.Int32

func init() { portBlock.Store(17000) }

func nextPorts(n int) []int {
	base := int(portBlock.Add(int32(n))) - n + 1
	ports := make([]int, n)
	for i := range ports {
		ports[i] = base + i
	}
	return ports
}

// testNode holds the state for a single in-process GhostKV node.
type testNode struct {
	id         string
	store      store.Store
	membership *membership.Membership
	hintStore  *hintedhandoff.HintStore
	grpcServer *grpc.Server
	healthSrv  *health.Server
	listener   net.Listener
	cancelFn   context.CancelFunc
}

// testCluster manages a 3-node in-process GhostKV cluster.
type testCluster struct {
	t       *testing.T
	cfg     *config.ClusterConfig
	nodes   map[string]*testNode
	clients map[string]*client.Client
	mu      sync.Mutex
}

// newTestCluster builds the cluster config and initialises the cluster struct.
// It does not start any nodes yet — call start() for that.
func newTestCluster(t *testing.T) *testCluster {
	t.Helper()

	tmpDir1 := t.TempDir()
	tmpDir2 := t.TempDir()
	tmpDir3 := t.TempDir()

	ports := nextPorts(3)

	cfg := &config.ClusterConfig{
		Cluster: config.ClusterSettings{
			Name:              "test",
			ReplicationFactor: 3,
			WriteQuorum:       2,
			ReadQuorum:        2,
			VirtualNodes:      128,
		},
		Nodes: []config.NodeConfig{
			{ID: "node-1", Address: fmt.Sprintf("127.0.0.1:%d", ports[0]), DataDir: tmpDir1},
			{ID: "node-2", Address: fmt.Sprintf("127.0.0.1:%d", ports[1]), DataDir: tmpDir2},
			{ID: "node-3", Address: fmt.Sprintf("127.0.0.1:%d", ports[2]), DataDir: tmpDir3},
		},
	}

	return &testCluster{
		t:       t,
		cfg:     cfg,
		nodes:   make(map[string]*testNode),
		clients: make(map[string]*client.Client),
	}
}

// start boots all 3 nodes in the cluster.
func (tc *testCluster) start() {
	tc.t.Helper()
	for _, n := range tc.cfg.Nodes {
		tc.startNode(n.ID)
	}
	// Give the failure detector and gRPC connections a moment to settle.
	time.Sleep(1 * time.Second)
}

// startNode starts a single node by ID.
func (tc *testCluster) startNode(id string) {
	tc.t.Helper()
	tc.mu.Lock()
	defer tc.mu.Unlock()

	nodeCfg, err := tc.cfg.FindNode(id)
	if err != nil {
		tc.t.Fatalf("startNode: find node %s: %v", id, err)
	}

	// Open the bbolt store.
	dbPath := filepath.Join(nodeCfg.DataDir, "ghostkv.db")
	kvStore, err := store.NewBoltStore(dbPath)
	if err != nil {
		tc.t.Fatalf("startNode: open store for %s: %v", id, err)
	}

	// Build the consistent hash ring with all configured nodes.
	hashRing := ring.New(tc.cfg.Cluster.VirtualNodes)
	for _, n := range tc.cfg.Nodes {
		hashRing.AddNode(n.ID)
	}

	// Create membership tracker.
	mem, err := membership.New(id, tc.cfg)
	if err != nil {
		kvStore.Close()
		tc.t.Fatalf("startNode: create membership for %s: %v", id, err)
	}

	// Background context for this node's goroutines.
	ctx, cancel := context.WithCancel(context.Background())

	// Start failure detector with short intervals for fast tests.
	detector := membership.NewDetector(mem, 500*time.Millisecond, 300*time.Millisecond)
	go detector.Run(ctx)

	// Create the hinted handoff store.
	hintDBPath := filepath.Join(nodeCfg.DataDir, "hints.db")
	hintStore, err := hintedhandoff.NewHintStore(hintDBPath, mem)
	if err != nil {
		cancel()
		mem.Close()
		kvStore.Close()
		tc.t.Fatalf("startNode: open hint store for %s: %v", id, err)
	}
	go hintStore.Run(ctx)

	// Create coordinator and wire hints.
	coord := coordinator.New(id, hashRing, kvStore, mem, &tc.cfg.Cluster)
	coord.SetHintStore(hintStore)

	// Create gRPC server and register all services.
	grpcServer := grpc.NewServer()

	kvSrv := server.NewKVServer(coord, kvStore, tc.cfg, id)
	pb.RegisterKVServiceServer(grpcServer, kvSrv)

	intSrv := server.NewInternalServer(kvStore)
	intSrv.SetHintStore(hintStore)
	pb.RegisterInternalServiceServer(grpcServer, intSrv)

	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthSrv)
	healthSrv.SetServingStatus("ghostkv.v1.KVService", healthpb.HealthCheckResponse_SERVING)
	healthSrv.SetServingStatus("ghostkv.v1.InternalService", healthpb.HealthCheckResponse_SERVING)

	// Listen on the configured address.
	lis, err := net.Listen("tcp", nodeCfg.Address)
	if err != nil {
		cancel()
		hintStore.Close()
		mem.Close()
		kvStore.Close()
		tc.t.Fatalf("startNode: listen for %s on %s: %v", id, nodeCfg.Address, err)
	}

	// Serve in the background.
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			// GracefulStop or Stop will cause Serve to return; that is expected.
		}
	}()

	tc.nodes[id] = &testNode{
		id:         id,
		store:      kvStore,
		membership: mem,
		hintStore:  hintStore,
		grpcServer: grpcServer,
		healthSrv:  healthSrv,
		listener:   lis,
		cancelFn:   cancel,
	}
}

// stopNode stops a single node by ID.
func (tc *testCluster) stopNode(id string) {
	tc.t.Helper()
	tc.mu.Lock()
	defer tc.mu.Unlock()

	n, ok := tc.nodes[id]
	if !ok {
		return
	}

	// Mark services as not serving before stopping.
	n.healthSrv.SetServingStatus("ghostkv.v1.KVService", healthpb.HealthCheckResponse_NOT_SERVING)
	n.healthSrv.SetServingStatus("ghostkv.v1.InternalService", healthpb.HealthCheckResponse_NOT_SERVING)

	n.grpcServer.Stop()
	n.cancelFn()
	n.hintStore.Close()
	n.membership.Close()
	n.store.Close()
	delete(tc.nodes, id)
}

// stop shuts down all nodes and closes all clients.
func (tc *testCluster) stop() {
	tc.t.Helper()

	for id := range tc.clients {
		tc.clients[id].Close()
	}
	tc.clients = make(map[string]*client.Client)

	// Collect node IDs first so we don't modify the map while iterating.
	var ids []string
	for id := range tc.nodes {
		ids = append(ids, id)
	}
	for _, id := range ids {
		tc.stopNode(id)
	}
}

// getClient returns (and caches) a gRPC client connected to the given node.
func (tc *testCluster) getClient(id string) *client.Client {
	tc.t.Helper()
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if c, ok := tc.clients[id]; ok {
		return c
	}

	nodeCfg, err := tc.cfg.FindNode(id)
	if err != nil {
		tc.t.Fatalf("getClient: find node %s: %v", id, err)
	}

	c, err := client.New(nodeCfg.Address)
	if err != nil {
		tc.t.Fatalf("getClient: connect to %s: %v", id, err)
	}
	tc.clients[id] = c
	return c
}

// closeClient disconnects and removes a cached client. This should be
// called before restarting a node so the next getClient call creates a
// fresh connection.
func (tc *testCluster) closeClient(id string) {
	tc.t.Helper()
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if c, ok := tc.clients[id]; ok {
		c.Close()
		delete(tc.clients, id)
	}
}

// --------------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------------

func TestClusterBasicCRUD(t *testing.T) {
	t.Parallel()

	tc := newTestCluster(t)
	tc.start()
	defer tc.stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c1 := tc.getClient("node-1")

	// Put 10 keys via node-1.
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%02d", i)
		val := fmt.Sprintf("value-%02d", i)
		acks, err := c1.Put(ctx, key, []byte(val))
		if err != nil {
			t.Fatalf("put %s via node-1: %v", key, err)
		}
		if acks < 2 {
			t.Fatalf("put %s: expected >= 2 acks, got %d", key, acks)
		}
	}

	// Get each key from all 3 nodes.
	for _, nodeID := range []string{"node-1", "node-2", "node-3"} {
		c := tc.getClient(nodeID)
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key-%02d", i)
			expected := fmt.Sprintf("value-%02d", i)

			val, found, err := c.Get(ctx, key)
			if err != nil {
				t.Fatalf("get %s from %s: %v", key, nodeID, err)
			}
			if !found {
				t.Fatalf("get %s from %s: expected found=true", key, nodeID)
			}
			if string(val) != expected {
				t.Fatalf("get %s from %s: expected %q, got %q", key, nodeID, expected, string(val))
			}
		}
	}

	// Delete 3 keys.
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("key-%02d", i)
		deleted, err := c1.Delete(ctx, key)
		if err != nil {
			t.Fatalf("delete %s: %v", key, err)
		}
		if !deleted {
			t.Fatalf("delete %s: expected deleted=true", key)
		}
	}

	// Verify deleted keys return not-found from all nodes.
	for _, nodeID := range []string{"node-1", "node-2", "node-3"} {
		c := tc.getClient(nodeID)
		for i := 0; i < 3; i++ {
			key := fmt.Sprintf("key-%02d", i)
			_, found, err := c.Get(ctx, key)
			if err != nil {
				t.Fatalf("get deleted %s from %s: %v", key, nodeID, err)
			}
			if found {
				t.Fatalf("get deleted %s from %s: expected found=false", key, nodeID)
			}
		}
	}

	// Verify remaining keys still exist.
	for _, nodeID := range []string{"node-1", "node-2", "node-3"} {
		c := tc.getClient(nodeID)
		for i := 3; i < 10; i++ {
			key := fmt.Sprintf("key-%02d", i)
			expected := fmt.Sprintf("value-%02d", i)

			val, found, err := c.Get(ctx, key)
			if err != nil {
				t.Fatalf("get remaining %s from %s: %v", key, nodeID, err)
			}
			if !found {
				t.Fatalf("get remaining %s from %s: expected found=true", key, nodeID)
			}
			if string(val) != expected {
				t.Fatalf("get remaining %s from %s: expected %q, got %q", key, nodeID, expected, string(val))
			}
		}
	}
}

func TestClusterQuorumWithOneNodeDown(t *testing.T) {
	t.Parallel()

	tc := newTestCluster(t)
	tc.start()
	defer tc.stop()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Kill node-3.
	tc.closeClient("node-3")
	tc.stopNode("node-3")

	// Wait for the failure detector to notice node-3 is down.
	time.Sleep(2 * time.Second)

	c1 := tc.getClient("node-1")

	// Put 5 keys — should succeed with 2 acks (quorum met from node-1 + node-2).
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("quorum-key-%02d", i)
		val := fmt.Sprintf("quorum-val-%02d", i)
		acks, err := c1.Put(ctx, key, []byte(val))
		if err != nil {
			t.Fatalf("put %s with node-3 down: %v", key, err)
		}
		if acks < 2 {
			t.Fatalf("put %s: expected >= 2 acks, got %d", key, acks)
		}
	}

	// Get those keys — should succeed from nodes 1 and 2.
	for _, nodeID := range []string{"node-1", "node-2"} {
		c := tc.getClient(nodeID)
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("quorum-key-%02d", i)
			expected := fmt.Sprintf("quorum-val-%02d", i)

			val, found, err := c.Get(ctx, key)
			if err != nil {
				t.Fatalf("get %s from %s: %v", key, nodeID, err)
			}
			if !found {
				t.Fatalf("get %s from %s: expected found=true", key, nodeID)
			}
			if string(val) != expected {
				t.Fatalf("get %s from %s: expected %q, got %q", key, nodeID, expected, string(val))
			}
		}
	}

	// Restart node-3.
	tc.startNode("node-3")

	// Wait for hint replay and/or read repair to propagate data.
	time.Sleep(15 * time.Second)

	c3 := tc.getClient("node-3")

	// Verify data is eventually reachable from node-3.
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("quorum-key-%02d", i)
		expected := fmt.Sprintf("quorum-val-%02d", i)

		val, found, err := c3.Get(ctx, key)
		if err != nil {
			t.Fatalf("get %s from node-3 after restart: %v", key, err)
		}
		if !found {
			t.Fatalf("get %s from node-3 after restart: expected found=true", key)
		}
		if string(val) != expected {
			t.Fatalf("get %s from node-3 after restart: expected %q, got %q", key, expected, string(val))
		}
	}
}

func TestClusterHintedHandoff(t *testing.T) {
	t.Parallel()

	tc := newTestCluster(t)
	tc.start()
	defer tc.stop()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Kill node-2.
	tc.closeClient("node-2")
	tc.stopNode("node-2")

	// Wait for the failure detector to notice node-2 is down.
	time.Sleep(2 * time.Second)

	// Put key "hinty" via node-1.
	c1 := tc.getClient("node-1")
	acks, err := c1.Put(ctx, "hinty", []byte("hinted-value"))
	if err != nil {
		t.Fatalf("put hinty with node-2 down: %v", err)
	}
	if acks < 2 {
		t.Logf("put hinty: got %d acks (node-2 is down, quorum from node-1 + node-3)", acks)
	}

	// Restart node-2.
	tc.startNode("node-2")

	// Wait for hint replay. The hint store replays every 10 seconds, plus
	// the failure detector needs to mark node-2 healthy again first.
	time.Sleep(15 * time.Second)

	// Get "hinty" directly from node-2 — it should have arrived via hint replay.
	c2 := tc.getClient("node-2")
	val, found, err := c2.Get(ctx, "hinty")
	if err != nil {
		t.Fatalf("get hinty from node-2 after restart: %v", err)
	}
	if !found {
		t.Fatalf("get hinty from node-2 after restart: expected found=true (hint replay should have delivered it)")
	}
	if string(val) != "hinted-value" {
		t.Fatalf("get hinty from node-2: expected %q, got %q", "hinted-value", string(val))
	}
}

func TestClusterStatus(t *testing.T) {
	t.Parallel()

	tc := newTestCluster(t)
	tc.start()
	defer tc.stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c1 := tc.getClient("node-1")
	resp, err := c1.Status(ctx)
	if err != nil {
		t.Fatalf("status call failed: %v", err)
	}

	if resp.GetReplicationFactor() != 3 {
		t.Fatalf("expected replication_factor=3, got %d", resp.GetReplicationFactor())
	}
	if resp.GetWriteQuorum() != 2 {
		t.Fatalf("expected write_quorum=2, got %d", resp.GetWriteQuorum())
	}
	if resp.GetReadQuorum() != 2 {
		t.Fatalf("expected read_quorum=2, got %d", resp.GetReadQuorum())
	}

	if len(resp.GetNodes()) == 0 {
		t.Fatal("expected at least one node in status response")
	}

	node := resp.GetNodes()[0]
	if node.GetNodeId() != "node-1" {
		t.Fatalf("expected node_id=node-1, got %s", node.GetNodeId())
	}
	if !node.GetHealthy() {
		t.Fatal("expected node to be healthy")
	}
}
