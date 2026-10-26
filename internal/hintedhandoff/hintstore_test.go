package hintedhandoff

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alyelalwany/ghostkv/internal/config"
	"github.com/alyelalwany/ghostkv/internal/membership"
)

// newTestHintStore creates a HintStore backed by a temp directory.
// The caller should defer os.RemoveAll on the returned dir.
func newTestHintStore(t *testing.T) (*HintStore, string) {
	t.Helper()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "hints.db")

	// Create a minimal membership (no real peers needed for storage tests).
	cfg := &config.ClusterConfig{
		Nodes: []config.NodeConfig{
			{ID: "self", Address: ":0"},
		},
	}
	mem, err := membership.New("self", cfg)
	if err != nil {
		t.Fatalf("membership.New: %v", err)
	}
	t.Cleanup(func() { _ = mem.Close() })

	hs, err := NewHintStore(dbPath, mem)
	if err != nil {
		t.Fatalf("NewHintStore: %v", err)
	}
	t.Cleanup(func() { _ = hs.Close() })

	return hs, dir
}

func TestAddAndPendingCount(t *testing.T) {
	hs, _ := newTestHintStore(t)

	// Initially no hints.
	count, err := hs.PendingCount()
	if err != nil {
		t.Fatalf("PendingCount: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 pending hints, got %d", count)
	}

	// Add a few hints.
	hints := []*Hint{
		{TargetNodeID: "node-2", Key: "foo", Value: []byte("bar"), Timestamp: 100, CreatedAt: 1},
		{TargetNodeID: "node-2", Key: "baz", Value: []byte("qux"), Timestamp: 200, CreatedAt: 2},
		{TargetNodeID: "node-3", Key: "abc", Value: nil, Timestamp: 300, IsDelete: true, CreatedAt: 3},
	}
	for _, h := range hints {
		if err := hs.AddHint(h); err != nil {
			t.Fatalf("AddHint: %v", err)
		}
	}

	count, err = hs.PendingCount()
	if err != nil {
		t.Fatalf("PendingCount: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected 3 pending hints, got %d", count)
	}
}

func TestAddHintAutoCreatedAt(t *testing.T) {
	hs, _ := newTestHintStore(t)

	before := time.Now().UnixNano()
	h := &Hint{
		TargetNodeID: "node-2",
		Key:          "auto-ts",
		Value:        []byte("val"),
		Timestamp:    1000,
	}
	if err := hs.AddHint(h); err != nil {
		t.Fatalf("AddHint: %v", err)
	}
	after := time.Now().UnixNano()

	if h.CreatedAt < before || h.CreatedAt > after {
		t.Fatalf("CreatedAt %d not between %d and %d", h.CreatedAt, before, after)
	}

	count, err := hs.PendingCount()
	if err != nil {
		t.Fatalf("PendingCount: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 pending hint, got %d", count)
	}
}

func TestHintGC(t *testing.T) {
	hs, _ := newTestHintStore(t)

	// Override maxAge to something tiny for the test.
	hs.maxAge = 1 * time.Millisecond

	oldHint := &Hint{
		TargetNodeID: "node-2",
		Key:          "old-key",
		Value:        []byte("old-val"),
		Timestamp:    100,
		CreatedAt:    time.Now().Add(-1 * time.Hour).UnixNano(), // well in the past
	}
	newHint := &Hint{
		TargetNodeID: "node-2",
		Key:          "new-key",
		Value:        []byte("new-val"),
		Timestamp:    200,
		CreatedAt:    time.Now().UnixNano(),
	}

	if err := hs.AddHint(oldHint); err != nil {
		t.Fatalf("AddHint(old): %v", err)
	}
	if err := hs.AddHint(newHint); err != nil {
		t.Fatalf("AddHint(new): %v", err)
	}

	count, _ := hs.PendingCount()
	if count != 2 {
		t.Fatalf("expected 2 pending hints before gc, got %d", count)
	}

	// Wait for the maxAge to pass.
	time.Sleep(5 * time.Millisecond)

	// Run GC — should remove the old hint and the new one (both older than 1ms by now).
	hs.gc()

	count, _ = hs.PendingCount()
	// Both should be gone because maxAge is 1ms and we slept 5ms.
	if count != 0 {
		t.Fatalf("expected 0 pending hints after gc, got %d", count)
	}
}

func TestHintGCPreservesRecent(t *testing.T) {
	hs, _ := newTestHintStore(t)

	// Set maxAge to a generous duration so recent hints survive.
	hs.maxAge = 1 * time.Hour

	oldHint := &Hint{
		TargetNodeID: "node-2",
		Key:          "old-key",
		Value:        []byte("old-val"),
		Timestamp:    100,
		CreatedAt:    time.Now().Add(-2 * time.Hour).UnixNano(),
	}
	recentHint := &Hint{
		TargetNodeID: "node-2",
		Key:          "recent-key",
		Value:        []byte("recent-val"),
		Timestamp:    200,
		CreatedAt:    time.Now().UnixNano(),
	}

	if err := hs.AddHint(oldHint); err != nil {
		t.Fatalf("AddHint(old): %v", err)
	}
	if err := hs.AddHint(recentHint); err != nil {
		t.Fatalf("AddHint(recent): %v", err)
	}

	hs.gc()

	count, _ := hs.PendingCount()
	if count != 1 {
		t.Fatalf("expected 1 remaining hint after gc, got %d", count)
	}
}

func TestPendingTargets(t *testing.T) {
	hs, _ := newTestHintStore(t)

	hints := []*Hint{
		{TargetNodeID: "node-2", Key: "a", Value: []byte("v"), Timestamp: 1, CreatedAt: 1},
		{TargetNodeID: "node-2", Key: "b", Value: []byte("v"), Timestamp: 2, CreatedAt: 2},
		{TargetNodeID: "node-3", Key: "c", Value: []byte("v"), Timestamp: 3, CreatedAt: 3},
	}
	for _, h := range hints {
		if err := hs.AddHint(h); err != nil {
			t.Fatalf("AddHint: %v", err)
		}
	}

	targets, err := hs.pendingTargets()
	if err != nil {
		t.Fatalf("pendingTargets: %v", err)
	}

	targetSet := make(map[string]bool)
	for _, tgt := range targets {
		targetSet[tgt] = true
	}

	if !targetSet["node-2"] || !targetSet["node-3"] {
		t.Fatalf("expected targets node-2 and node-3, got %v", targets)
	}
	if len(targets) != 2 {
		t.Fatalf("expected 2 unique targets, got %d", len(targets))
	}
}

func TestNewHintStoreCreatesFile(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "hints.db")

	cfg := &config.ClusterConfig{
		Nodes: []config.NodeConfig{
			{ID: "self", Address: ":0"},
		},
	}
	mem, err := membership.New("self", cfg)
	if err != nil {
		t.Fatalf("membership.New: %v", err)
	}
	defer mem.Close()

	hs, err := NewHintStore(dbPath, mem)
	if err != nil {
		t.Fatalf("NewHintStore: %v", err)
	}
	defer hs.Close()

	// Verify the DB file was created.
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatal("hints.db was not created")
	}
}
