package membership

import (
	"testing"

	"github.com/alyelalwany/ghostkv/internal/config"
)

func testConfig() *config.ClusterConfig {
	return &config.ClusterConfig{
		Cluster: config.ClusterSettings{
			Name:              "test-cluster",
			ReplicationFactor: 3,
			WriteQuorum:       2,
			ReadQuorum:        2,
			VirtualNodes:      128,
		},
		Nodes: []config.NodeConfig{
			{ID: "node-1", Address: "127.0.0.1:7001", DataDir: "/tmp/test/node-1"},
			{ID: "node-2", Address: "127.0.0.1:7002", DataDir: "/tmp/test/node-2"},
			{ID: "node-3", Address: "127.0.0.1:7003", DataDir: "/tmp/test/node-3"},
		},
	}
}

func TestNew(t *testing.T) {
	cfg := testConfig()

	m, err := New("node-1", cfg)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer m.Close()

	// Should have 2 peers (not self).
	if len(m.peers) != 2 {
		t.Errorf("expected 2 peers, got %d", len(m.peers))
	}

	if _, ok := m.peers["node-1"]; ok {
		t.Error("self (node-1) should not be in peers map")
	}

	if _, ok := m.peers["node-2"]; !ok {
		t.Error("node-2 should be in peers map")
	}

	if _, ok := m.peers["node-3"]; !ok {
		t.Error("node-3 should be in peers map")
	}
}

func TestHealthy(t *testing.T) {
	cfg := testConfig()

	m, err := New("node-1", cfg)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer m.Close()

	// All peers start healthy (optimistic).
	if !m.IsHealthy("node-2") {
		t.Error("node-2 should be healthy initially")
	}

	// Mark as unhealthy.
	m.SetHealthy("node-2", false)
	if m.IsHealthy("node-2") {
		t.Error("node-2 should be unhealthy after SetHealthy(false)")
	}

	// Mark healthy again.
	m.SetHealthy("node-2", true)
	if !m.IsHealthy("node-2") {
		t.Error("node-2 should be healthy after SetHealthy(true)")
	}

	// Unknown node returns false.
	if m.IsHealthy("node-unknown") {
		t.Error("unknown node should not be healthy")
	}
}

func TestGetConn(t *testing.T) {
	cfg := testConfig()

	m, err := New("node-1", cfg)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer m.Close()

	conn, err := m.GetConn("node-2")
	if err != nil {
		t.Fatalf("GetConn(node-2) error: %v", err)
	}
	if conn == nil {
		t.Error("GetConn(node-2) should return a non-nil connection")
	}

	// Unknown node returns error.
	_, err = m.GetConn("node-unknown")
	if err == nil {
		t.Error("GetConn(node-unknown) should return an error")
	}
}

func TestGetAddress(t *testing.T) {
	cfg := testConfig()

	m, err := New("node-1", cfg)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer m.Close()

	addr := m.GetAddress("node-2")
	if addr != "127.0.0.1:7002" {
		t.Errorf("GetAddress(node-2) = %q, want %q", addr, "127.0.0.1:7002")
	}

	addr = m.GetAddress("node-unknown")
	if addr != "" {
		t.Errorf("GetAddress(node-unknown) = %q, want %q", addr, "")
	}
}

func TestHealthyPeers(t *testing.T) {
	cfg := testConfig()

	m, err := New("node-1", cfg)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer m.Close()

	peers := m.HealthyPeers()
	if len(peers) != 2 {
		t.Fatalf("expected 2 healthy peers, got %d", len(peers))
	}

	// Mark one unhealthy.
	m.SetHealthy("node-2", false)
	peers = m.HealthyPeers()
	if len(peers) != 1 {
		t.Fatalf("expected 1 healthy peer, got %d", len(peers))
	}
	if peers[0] != "node-3" {
		t.Errorf("healthy peer = %q, want %q", peers[0], "node-3")
	}
}

func TestClose(t *testing.T) {
	cfg := testConfig()

	m, err := New("node-1", cfg)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	if err := m.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	// Connections should be nil after close.
	for id, ps := range m.peers {
		if ps.Conn != nil {
			t.Errorf("peer %s connection should be nil after Close()", id)
		}
	}
}
