package ring

import (
	"fmt"
	"sort"
	"sync"

	"github.com/cespare/xxhash/v2"
)

// Ring implements a consistent hash ring with virtual nodes.
// Each physical node is mapped to multiple positions on the ring
// to ensure even key distribution.
type Ring struct {
	mu           sync.RWMutex
	vnodes       int               // virtual nodes per physical node
	sortedHashes []uint64          // sorted ring positions
	hashMap      map[uint64]string // ring position -> physical node ID
	nodes        map[string]struct{}
}

// New creates a new consistent hash ring. vnodes controls how many
// virtual nodes each physical node gets (e.g. 128).
func New(vnodes int) *Ring {
	return &Ring{
		vnodes:  vnodes,
		hashMap: make(map[uint64]string),
		nodes:   make(map[string]struct{}),
	}
}

// AddNode inserts a physical node into the ring, creating vnodes
// virtual positions for it.
func (r *Ring) AddNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodes[nodeID]; exists {
		return
	}
	r.nodes[nodeID] = struct{}{}

	for i := 0; i < r.vnodes; i++ {
		h := xxhash.Sum64String(fmt.Sprintf("%s#%d", nodeID, i))
		r.sortedHashes = append(r.sortedHashes, h)
		r.hashMap[h] = nodeID
	}

	sort.Slice(r.sortedHashes, func(i, j int) bool {
		return r.sortedHashes[i] < r.sortedHashes[j]
	})
}

// RemoveNode removes a physical node and all its virtual positions
// from the ring.
func (r *Ring) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodes[nodeID]; !exists {
		return
	}
	delete(r.nodes, nodeID)

	for i := 0; i < r.vnodes; i++ {
		h := xxhash.Sum64String(fmt.Sprintf("%s#%d", nodeID, i))
		delete(r.hashMap, h)
	}

	// Rebuild sortedHashes without the removed node's positions.
	newSorted := make([]uint64, 0, len(r.sortedHashes)-r.vnodes)
	for _, h := range r.sortedHashes {
		if _, ok := r.hashMap[h]; ok {
			newSorted = append(newSorted, h)
		}
	}
	r.sortedHashes = newSorted
}

// GetNode returns the single physical node responsible for the given key.
// Returns "" if the ring is empty.
func (r *Ring) GetNode(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.sortedHashes) == 0 {
		return ""
	}

	h := xxhash.Sum64String(key)
	idx := sort.Search(len(r.sortedHashes), func(i int) bool {
		return r.sortedHashes[i] >= h
	})

	// Wrap around to the beginning of the ring.
	if idx >= len(r.sortedHashes) {
		idx = 0
	}

	return r.hashMap[r.sortedHashes[idx]]
}

// GetNodes returns a preference list of up to n distinct physical nodes,
// walking clockwise from the key's position on the ring.
// The first node is the coordinator; the rest are replicas.
func (r *Ring) GetNodes(key string, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.sortedHashes) == 0 {
		return nil
	}

	// Can't return more distinct nodes than we have.
	if n > len(r.nodes) {
		n = len(r.nodes)
	}

	h := xxhash.Sum64String(key)
	idx := sort.Search(len(r.sortedHashes), func(i int) bool {
		return r.sortedHashes[i] >= h
	})
	if idx >= len(r.sortedHashes) {
		idx = 0
	}

	seen := make(map[string]struct{}, n)
	result := make([]string, 0, n)

	for i := 0; i < len(r.sortedHashes) && len(result) < n; i++ {
		pos := (idx + i) % len(r.sortedHashes)
		nodeID := r.hashMap[r.sortedHashes[pos]]
		if _, ok := seen[nodeID]; !ok {
			seen[nodeID] = struct{}{}
			result = append(result, nodeID)
		}
	}

	return result
}

// Members returns all physical node IDs currently in the ring.
func (r *Ring) Members() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	members := make([]string, 0, len(r.nodes))
	for id := range r.nodes {
		members = append(members, id)
	}
	sort.Strings(members)
	return members
}
