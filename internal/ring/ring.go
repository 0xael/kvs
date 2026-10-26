// Package ring provides a consistent hash ring for distributing keys
// across cluster nodes. This is a minimal stub that will be replaced
// by the full implementation from another agent.
package ring

import (
	"hash/fnv"
	"sort"
	"sync"
)

// Ring is a consistent hash ring that maps keys to nodes.
type Ring struct {
	mu     sync.RWMutex
	nodes  []string
	vnodes int
}

// New creates a new Ring with the given number of virtual nodes per real node.
func New(vnodes int) *Ring {
	if vnodes < 1 {
		vnodes = 128
	}
	return &Ring{vnodes: vnodes}
}

// AddNode adds a node to the ring.
func (r *Ring) AddNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Avoid duplicates.
	for _, n := range r.nodes {
		if n == nodeID {
			return
		}
	}
	r.nodes = append(r.nodes, nodeID)
	sort.Strings(r.nodes)
}

// GetNode returns the single node responsible for the given key.
// Uses FNV-1a hash modulo the number of nodes as a simple placeholder.
func (r *Ring) GetNode(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.nodes) == 0 {
		return ""
	}

	h := fnv.New32a()
	h.Write([]byte(key))
	idx := int(h.Sum32()) % len(r.nodes)
	if idx < 0 {
		idx = -idx
	}
	return r.nodes[idx]
}

// GetNodes returns up to n distinct nodes for the given key, starting from
// the primary owner. This stub returns nodes in a deterministic order.
func (r *Ring) GetNodes(key string, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.nodes) == 0 {
		return nil
	}
	if n > len(r.nodes) {
		n = len(r.nodes)
	}

	h := fnv.New32a()
	h.Write([]byte(key))
	start := int(h.Sum32()) % len(r.nodes)
	if start < 0 {
		start = -start
	}

	result := make([]string, n)
	for i := range n {
		result[i] = r.nodes[(start+i)%len(r.nodes)]
	}
	return result
}

// Members returns all nodes currently in the ring.
func (r *Ring) Members() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]string, len(r.nodes))
	copy(out, r.nodes)
	return out
}
