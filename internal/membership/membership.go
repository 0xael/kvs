// Package membership tracks peer state and manages gRPC connections
// to other nodes in the GhostKV cluster.
package membership

import (
	"fmt"
	"sync"

	"github.com/alyelalwany/ghostkv/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// PeerState holds the connection state for a single remote peer.
type PeerState struct {
	NodeID  string
	Address string
	Healthy bool
	Conn    *grpc.ClientConn
}

// Membership tracks the set of peers in the cluster and manages their gRPC
// connections. It excludes the local node (self).
type Membership struct {
	mu    sync.RWMutex
	self  string
	peers map[string]*PeerState // nodeID -> state (excludes self)
}

// New creates a Membership, eagerly establishing gRPC connections to every
// peer in the cluster config that is not the local node (self).
// Connections start with Healthy: true (optimistic assumption).
func New(self string, cfg *config.ClusterConfig) (*Membership, error) {
	m := &Membership{
		self:  self,
		peers: make(map[string]*PeerState, len(cfg.Nodes)-1),
	}

	for _, n := range cfg.Nodes {
		if n.ID == self {
			continue
		}

		conn, err := grpc.NewClient(
			n.Address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			// Clean up any connections we already opened.
			_ = m.Close()
			return nil, fmt.Errorf("membership: connect to %s (%s): %w", n.ID, n.Address, err)
		}

		m.peers[n.ID] = &PeerState{
			NodeID:  n.ID,
			Address: n.Address,
			Healthy: true,
			Conn:    conn,
		}
	}

	return m, nil
}

// IsHealthy reports whether the given peer is considered healthy.
// Returns false for unknown nodes.
func (m *Membership) IsHealthy(nodeID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ps, ok := m.peers[nodeID]
	if !ok {
		return false
	}
	return ps.Healthy
}

// SetHealthy updates the health status for the given peer.
func (m *Membership) SetHealthy(nodeID string, healthy bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ps, ok := m.peers[nodeID]; ok {
		ps.Healthy = healthy
	}
}

// GetConn returns the gRPC client connection for the given peer.
func (m *Membership) GetConn(nodeID string) (*grpc.ClientConn, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ps, ok := m.peers[nodeID]
	if !ok {
		return nil, fmt.Errorf("membership: unknown peer %q", nodeID)
	}
	return ps.Conn, nil
}

// GetAddress returns the address for the given peer, or "" if unknown.
func (m *Membership) GetAddress(nodeID string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if ps, ok := m.peers[nodeID]; ok {
		return ps.Address
	}
	return ""
}

// HealthyPeers returns the node IDs of all peers currently marked healthy.
func (m *Membership) HealthyPeers() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var ids []string
	for id, ps := range m.peers {
		if ps.Healthy {
			ids = append(ids, id)
		}
	}
	return ids
}

// PeerIDs returns the node IDs of all known peers (regardless of health).
func (m *Membership) PeerIDs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ids := make([]string, 0, len(m.peers))
	for id := range m.peers {
		ids = append(ids, id)
	}
	return ids
}

// Close shuts down all gRPC connections to peers.
func (m *Membership) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var firstErr error
	for _, ps := range m.peers {
		if ps.Conn != nil {
			if err := ps.Conn.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
			ps.Conn = nil
		}
	}
	return firstErr
}
