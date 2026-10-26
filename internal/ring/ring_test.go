package ring

import (
	"fmt"
	"testing"
)

func TestAddRemoveNode(t *testing.T) {
	r := New(128)
	r.AddNode("node-1")
	r.AddNode("node-2")
	r.AddNode("node-3")

	members := r.Members()
	if len(members) != 3 {
		t.Fatalf("expected 3 members, got %d: %v", len(members), members)
	}

	r.RemoveNode("node-2")
	members = r.Members()
	if len(members) != 2 {
		t.Fatalf("expected 2 members after removal, got %d: %v", len(members), members)
	}
	for _, m := range members {
		if m == "node-2" {
			t.Fatal("node-2 should have been removed")
		}
	}
}

func TestGetNode(t *testing.T) {
	r := New(128)
	r.AddNode("node-1")
	r.AddNode("node-2")
	r.AddNode("node-3")

	valid := map[string]bool{"node-1": true, "node-2": true, "node-3": true}

	keys := []string{"user:1001", "order:abc", "session:xyz", "key-foo", "key-bar"}
	for _, key := range keys {
		node := r.GetNode(key)
		if !valid[node] {
			t.Errorf("GetNode(%q) returned invalid node %q", key, node)
		}
	}
}

func TestGetNodesReturnsDistinct(t *testing.T) {
	r := New(128)
	r.AddNode("node-1")
	r.AddNode("node-2")
	r.AddNode("node-3")

	nodes := r.GetNodes("some-key", 3)
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d: %v", len(nodes), nodes)
	}

	seen := make(map[string]bool)
	for _, n := range nodes {
		if seen[n] {
			t.Fatalf("duplicate node in preference list: %v", nodes)
		}
		seen[n] = true
	}
}

func TestGetNodesWhenFewerNodes(t *testing.T) {
	r := New(128)
	r.AddNode("node-1")
	r.AddNode("node-2")

	nodes := r.GetNodes("some-key", 3)
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes (only 2 physical), got %d: %v", len(nodes), nodes)
	}
}

func TestDistribution(t *testing.T) {
	r := New(128)
	r.AddNode("node-1")
	r.AddNode("node-2")
	r.AddNode("node-3")

	counts := make(map[string]int)
	total := 10000
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key-%d", i)
		node := r.GetNode(key)
		counts[node]++
	}

	for node, count := range counts {
		pct := float64(count) / float64(total) * 100
		t.Logf("%s: %d keys (%.1f%%)", node, count, pct)
		if pct < 20.0 {
			t.Errorf("node %s got only %.1f%% of keys, expected at least 20%%", node, pct)
		}
	}
}

func TestDeterministic(t *testing.T) {
	r := New(128)
	r.AddNode("node-1")
	r.AddNode("node-2")
	r.AddNode("node-3")

	key := "deterministic-test-key"
	first := r.GetNode(key)

	for i := 0; i < 100; i++ {
		got := r.GetNode(key)
		if got != first {
			t.Fatalf("iteration %d: GetNode(%q) = %q, want %q", i, key, got, first)
		}
	}
}

func TestEmptyRing(t *testing.T) {
	r := New(128)
	if node := r.GetNode("key"); node != "" {
		t.Errorf("expected empty string for empty ring, got %q", node)
	}
	if nodes := r.GetNodes("key", 3); nodes != nil {
		t.Errorf("expected nil for empty ring, got %v", nodes)
	}
}

func TestAddDuplicateNode(t *testing.T) {
	r := New(128)
	r.AddNode("node-1")
	r.AddNode("node-1") // duplicate

	members := r.Members()
	if len(members) != 1 {
		t.Fatalf("expected 1 member after duplicate add, got %d", len(members))
	}
	// Verify we still have exactly 128 virtual nodes, not 256.
	if len(r.sortedHashes) != 128 {
		t.Fatalf("expected 128 virtual nodes, got %d", len(r.sortedHashes))
	}
}
