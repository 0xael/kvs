// Package antientropy implements Merkle-tree-based divergence detection
// and background repair for GhostKV replicas.
package antientropy

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"

	pb "github.com/alyelalwany/ghostkv/gen/ghostkv/v1"
	"github.com/alyelalwany/ghostkv/internal/store"
)

// MerkleTree is a hash tree over a key range from the local store.
type MerkleTree struct {
	Root  *MerkleNode
	Depth int
}

// MerkleNode represents one node of a Merkle tree.
type MerkleNode struct {
	Hash       []byte
	RangeStart string
	RangeEnd   string
	Children   []*MerkleNode
	IsLeaf     bool
}

// KeyRange describes a contiguous key range that diverges between two trees.
type KeyRange struct {
	Start string
	End   string
}

// keyRecord is an intermediate pair collected from the store.
type keyRecord struct {
	key string
	rec *store.Record
}

// BuildTree constructs a Merkle tree of the given depth over keys in
// [start, end) from the store.  Leaf buckets are formed by splitting
// the collected keys evenly into 2^depth buckets.
func BuildTree(s store.Store, start, end string, depth int) (*MerkleTree, error) {
	// Collect all keys in range (already sorted by bbolt/BoltStore).
	var entries []keyRecord
	if err := s.ForEachInRange(start, end, func(key string, rec *store.Record) error {
		entries = append(entries, keyRecord{key: key, rec: rec})
		return nil
	}); err != nil {
		return nil, fmt.Errorf("antientropy: collect keys: %w", err)
	}

	numLeaves := 1 << depth // 2^depth

	root := buildNode(entries, start, end, depth, 0, numLeaves, 0)
	return &MerkleTree{Root: root, Depth: depth}, nil
}

// buildNode recursively constructs the tree.  bucketStart..bucketEnd is the
// slice of the leaf-bucket index space assigned to this node.
func buildNode(entries []keyRecord, rangeStart, rangeEnd string, maxDepth, currentDepth, numLeaves, bucketOffset int) *MerkleNode {
	node := &MerkleNode{
		RangeStart: rangeStart,
		RangeEnd:   rangeEnd,
	}

	if currentDepth == maxDepth {
		// Leaf node — hash the entries that fall into this bucket.
		node.IsLeaf = true
		node.Hash = hashEntries(entries)
		return node
	}

	// Internal node: split into two children.
	mid := len(entries) / 2

	// Determine the boundary key for the split.
	var midKey string
	if mid > 0 && mid < len(entries) {
		midKey = entries[mid].key
	} else if mid == 0 && len(entries) > 0 {
		midKey = entries[0].key
	} else {
		// No entries — pick a synthetic midpoint (empty string means
		// the children share the parent's range boundaries).
		midKey = midpointString(rangeStart, rangeEnd)
	}

	leftEntries := entries[:mid]
	rightEntries := entries[mid:]

	halfBuckets := (numLeaves - bucketOffset) / 2

	left := buildNode(leftEntries, rangeStart, midKey, maxDepth, currentDepth+1, numLeaves, bucketOffset)
	right := buildNode(rightEntries, midKey, rangeEnd, maxDepth, currentDepth+1, numLeaves, bucketOffset+halfBuckets)

	node.Children = []*MerkleNode{left, right}

	// Internal hash = SHA-256(left.Hash || right.Hash)
	h := sha256.New()
	h.Write(left.Hash)
	h.Write(right.Hash)
	node.Hash = h.Sum(nil)

	return node
}

// hashEntries computes SHA-256 over key|value|timestamp for each entry
// (sorted by key).  Returns SHA-256("") for an empty slice.
func hashEntries(entries []keyRecord) []byte {
	// Entries are already in sorted key order from the store scan.
	h := sha256.New()
	for _, e := range entries {
		h.Write([]byte(e.key))
		h.Write([]byte("|"))
		h.Write(e.rec.Value)
		h.Write([]byte("|"))
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(e.rec.Timestamp))
		h.Write(buf[:])
	}
	return h.Sum(nil)
}

// midpointString returns a rough lexicographic midpoint between two strings.
// This is only used when a subtree has no entries — it just needs to be
// deterministic and between start and end.
func midpointString(a, b string) string {
	if a == "" && b == "" {
		return "m" // arbitrary midpoint of the entire key space
	}
	if a == "" {
		return b
	}
	if b == "" {
		return a + "\x00"
	}
	// Pad to equal length, then average byte-by-byte.
	maxLen := len(a)
	if len(b) > maxLen {
		maxLen = len(b)
	}
	padded := func(s string, n int) []byte {
		out := make([]byte, n)
		copy(out, s)
		return out
	}
	ab := padded(a, maxLen+1)
	bb := padded(b, maxLen+1)

	mid := make([]byte, maxLen+1)
	var carry int
	for i := maxLen; i >= 0; i-- {
		sum := int(ab[i]) + int(bb[i]) + carry
		mid[i] = byte(sum / 2)
		carry = (sum % 2) << 8
	}
	// Trim trailing zero bytes.
	end := len(mid)
	for end > 0 && mid[end-1] == 0 {
		end--
	}
	if end == 0 {
		return a
	}
	return string(mid[:end])
}

// ToProto converts the in-memory MerkleNode tree to its protobuf form.
func (t *MerkleTree) ToProto() *pb.MerkleNode {
	if t == nil || t.Root == nil {
		return nil
	}
	return nodeToProto(t.Root)
}

func nodeToProto(n *MerkleNode) *pb.MerkleNode {
	if n == nil {
		return nil
	}
	pn := &pb.MerkleNode{
		Hash:       n.Hash,
		RangeStart: n.RangeStart,
		RangeEnd:   n.RangeEnd,
	}
	for _, child := range n.Children {
		pn.Children = append(pn.Children, nodeToProto(child))
	}
	return pn
}

// FromProto converts a protobuf MerkleNode tree back to the in-memory form.
func FromProto(pn *pb.MerkleNode) *MerkleNode {
	if pn == nil {
		return nil
	}
	n := &MerkleNode{
		Hash:       pn.Hash,
		RangeStart: pn.RangeStart,
		RangeEnd:   pn.RangeEnd,
		IsLeaf:     len(pn.Children) == 0,
	}
	for _, child := range pn.Children {
		n.Children = append(n.Children, FromProto(child))
	}
	return n
}

// DiffRanges compares two MerkleNodes (local and remote) top-down and
// returns the leaf-level key ranges where hashes diverge.
func DiffRanges(local, remote *MerkleNode) []KeyRange {
	if local == nil || remote == nil {
		// If either tree is nil, treat the entire range as divergent.
		if local != nil {
			return []KeyRange{{Start: local.RangeStart, End: local.RangeEnd}}
		}
		if remote != nil {
			return []KeyRange{{Start: remote.RangeStart, End: remote.RangeEnd}}
		}
		return nil
	}

	// Hashes match — subtree is identical.
	if bytes.Equal(local.Hash, remote.Hash) {
		return nil
	}

	// Both have children — recurse into each pair.
	if len(local.Children) > 0 && len(remote.Children) > 0 {
		var ranges []KeyRange
		// Walk matching children (trees are built with the same depth,
		// so children should be aligned).
		n := len(local.Children)
		if len(remote.Children) < n {
			n = len(remote.Children)
		}
		for i := 0; i < n; i++ {
			ranges = append(ranges, DiffRanges(local.Children[i], remote.Children[i])...)
		}
		// Any extra children on either side are divergent.
		for i := n; i < len(local.Children); i++ {
			ranges = append(ranges, KeyRange{
				Start: local.Children[i].RangeStart,
				End:   local.Children[i].RangeEnd,
			})
		}
		for i := n; i < len(remote.Children); i++ {
			ranges = append(ranges, KeyRange{
				Start: remote.Children[i].RangeStart,
				End:   remote.Children[i].RangeEnd,
			})
		}
		// Sort the ranges for deterministic output.
		sort.Slice(ranges, func(i, j int) bool {
			return ranges[i].Start < ranges[j].Start
		})
		return ranges
	}

	// At least one is a leaf and hashes differ — return this range.
	// Use the union of both ranges.
	start := local.RangeStart
	if remote.RangeStart < start || start == "" {
		start = remote.RangeStart
	}
	end := local.RangeEnd
	if remote.RangeEnd > end || end == "" {
		end = remote.RangeEnd
	}
	return []KeyRange{{Start: start, End: end}}
}
