package antientropy

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/alyelalwany/ghostkv/internal/store"
)

// memStore is a minimal in-memory Store for testing.
type memStore struct {
	records map[string]*store.Record
	keys    []string // insertion-ordered for determinism
}

func newMemStore() *memStore {
	return &memStore{records: make(map[string]*store.Record)}
}

func (m *memStore) Get(key string) (*store.Record, error) {
	rec, ok := m.records[key]
	if !ok {
		return nil, nil
	}
	return rec, nil
}

func (m *memStore) Put(key string, rec *store.Record) error {
	if _, exists := m.records[key]; !exists {
		m.keys = append(m.keys, key)
	}
	m.records[key] = rec
	return nil
}

func (m *memStore) Delete(key string, timestamp int64) error {
	return m.Put(key, &store.Record{IsDelete: true, Timestamp: timestamp})
}

func (m *memStore) ForEachInRange(start, end string, fn func(string, *store.Record) error) error {
	// Sort the keys and iterate in order, matching the bbolt cursor behavior.
	sorted := make([]string, len(m.keys))
	copy(sorted, m.keys)
	// Simple insertion sort for test data.
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && sorted[j] < sorted[j-1]; j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}

	for _, k := range sorted {
		if start != "" && k < start {
			continue
		}
		if end != "" && k >= end {
			break
		}
		if err := fn(k, m.records[k]); err != nil {
			return err
		}
	}
	return nil
}

func (m *memStore) KeyCount() (int64, error) {
	var n int64
	for _, rec := range m.records {
		if !rec.IsDelete {
			n++
		}
	}
	return n, nil
}

func (m *memStore) Close() error { return nil }

// ----- Tests -----

func TestBuildTreeEmpty(t *testing.T) {
	s := newMemStore()
	tree, err := BuildTree(s, "", "", 3)
	if err != nil {
		t.Fatalf("BuildTree on empty store: %v", err)
	}
	if tree.Root == nil {
		t.Fatal("expected non-nil root")
	}
	if tree.Root.Hash == nil {
		t.Fatal("expected non-nil root hash")
	}
	if len(tree.Root.Hash) != 32 {
		t.Errorf("expected SHA-256 hash (32 bytes), got %d bytes", len(tree.Root.Hash))
	}
}

func TestBuildTreeDeterministic(t *testing.T) {
	build := func() *MerkleTree {
		s := newMemStore()
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("key-%03d", i)
			_ = s.Put(key, &store.Record{
				Value:     []byte(fmt.Sprintf("value-%d", i)),
				Timestamp: int64(1000 + i),
			})
		}
		tree, err := BuildTree(s, "", "", 4)
		if err != nil {
			t.Fatalf("BuildTree: %v", err)
		}
		return tree
	}

	t1 := build()
	t2 := build()

	if !bytes.Equal(t1.Root.Hash, t2.Root.Hash) {
		t.Error("two trees from the same data should have the same root hash")
	}
}

func TestDiffRangesIdentical(t *testing.T) {
	s := newMemStore()
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%03d", i)
		_ = s.Put(key, &store.Record{
			Value:     []byte(fmt.Sprintf("val-%d", i)),
			Timestamp: int64(100 + i),
		})
	}

	tree1, err := BuildTree(s, "", "", 3)
	if err != nil {
		t.Fatal(err)
	}
	tree2, err := BuildTree(s, "", "", 3)
	if err != nil {
		t.Fatal(err)
	}

	ranges := DiffRanges(tree1.Root, tree2.Root)
	if len(ranges) != 0 {
		t.Errorf("expected 0 divergent ranges for identical trees, got %d: %v", len(ranges), ranges)
	}
}

func TestDiffRangesDivergent(t *testing.T) {
	// Build two stores with overlapping but different data.
	s1 := newMemStore()
	s2 := newMemStore()

	// Shared keys.
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%03d", i)
		rec := &store.Record{
			Value:     []byte(fmt.Sprintf("val-%d", i)),
			Timestamp: int64(100 + i),
		}
		_ = s1.Put(key, rec)
		_ = s2.Put(key, rec)
	}

	// Divergent: different value for key-005 in s2.
	_ = s2.Put("key-005", &store.Record{
		Value:     []byte("different-val"),
		Timestamp: 200,
	})

	tree1, err := BuildTree(s1, "", "", 3)
	if err != nil {
		t.Fatal(err)
	}
	tree2, err := BuildTree(s2, "", "", 3)
	if err != nil {
		t.Fatal(err)
	}

	ranges := DiffRanges(tree1.Root, tree2.Root)
	if len(ranges) == 0 {
		t.Fatal("expected at least 1 divergent range")
	}

	// The divergent range should include "key-005".
	found := false
	for _, kr := range ranges {
		// key-005 should fall within [kr.Start, kr.End).
		if (kr.Start == "" || kr.Start <= "key-005") && (kr.End == "" || "key-005" < kr.End) {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("divergent ranges %v do not cover key-005", ranges)
	}
}

func TestToFromProto(t *testing.T) {
	s := newMemStore()
	for i := 0; i < 15; i++ {
		key := fmt.Sprintf("k%02d", i)
		_ = s.Put(key, &store.Record{
			Value:     []byte(fmt.Sprintf("v%d", i)),
			Timestamp: int64(i * 10),
		})
	}

	tree, err := BuildTree(s, "", "", 3)
	if err != nil {
		t.Fatal(err)
	}

	// Convert to proto and back.
	protoNode := tree.ToProto()
	if protoNode == nil {
		t.Fatal("ToProto returned nil")
	}

	roundTripped := FromProto(protoNode)
	if roundTripped == nil {
		t.Fatal("FromProto returned nil")
	}

	// Root hashes must match.
	if !bytes.Equal(tree.Root.Hash, roundTripped.Hash) {
		t.Error("round-tripped hash does not match original")
	}

	// The round-tripped tree should produce the same diff (none) against the original.
	ranges := DiffRanges(tree.Root, roundTripped)
	if len(ranges) != 0 {
		t.Errorf("expected 0 divergent ranges after round-trip, got %d", len(ranges))
	}
}

func TestDiffRangesNilTrees(t *testing.T) {
	// Both nil → no ranges.
	ranges := DiffRanges(nil, nil)
	if len(ranges) != 0 {
		t.Errorf("nil vs nil: expected 0 ranges, got %d", len(ranges))
	}

	// One nil → the other's range.
	node := &MerkleNode{
		Hash:       []byte{1, 2, 3},
		RangeStart: "a",
		RangeEnd:   "z",
		IsLeaf:     true,
	}
	ranges = DiffRanges(node, nil)
	if len(ranges) != 1 || ranges[0].Start != "a" || ranges[0].End != "z" {
		t.Errorf("node vs nil: expected [{a z}], got %v", ranges)
	}

	ranges = DiffRanges(nil, node)
	if len(ranges) != 1 || ranges[0].Start != "a" || ranges[0].End != "z" {
		t.Errorf("nil vs node: expected [{a z}], got %v", ranges)
	}
}
