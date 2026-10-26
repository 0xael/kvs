package coordinator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alyelalwany/ghostkv/internal/config"
	"github.com/alyelalwany/ghostkv/internal/membership"
	"github.com/alyelalwany/ghostkv/internal/store"
)

// --- mocks ---

// mockRing returns a configurable preference list for any key.
type mockRing struct {
	nodes []string
}

func (r *mockRing) GetNode(_ string) string {
	if len(r.nodes) == 0 {
		return ""
	}
	return r.nodes[0]
}

func (r *mockRing) GetNodes(_ string, n int) []string {
	if n > len(r.nodes) {
		n = len(r.nodes)
	}
	return r.nodes[:n]
}

// mockStore is a thread-safe in-memory store for testing.
type mockStore struct {
	mu   sync.Mutex
	data map[string]*store.Record
}

func newMockStore() *mockStore {
	return &mockStore{data: make(map[string]*store.Record)}
}

func (s *mockStore) Get(key string) (*store.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.data[key]
	if !ok {
		return nil, nil
	}
	return rec, nil
}

func (s *mockStore) Put(key string, rec *store.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = rec
	return nil
}

func (s *mockStore) Delete(key string, timestamp int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = &store.Record{IsDelete: true, Timestamp: timestamp}
	return nil
}

func (s *mockStore) ForEachInRange(_, _ string, fn func(string, *store.Record) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range s.data {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

func (s *mockStore) KeyCount() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var count int64
	for _, v := range s.data {
		if !v.IsDelete {
			count++
		}
	}
	return count, nil
}

func (s *mockStore) Close() error { return nil }

// newMembershipSelfOnly creates a *membership.Membership where self is the
// only known node. GetConn for any other nodeID will return an error.
func newMembershipSelfOnly(selfID string) *membership.Membership {
	minCfg := &config.ClusterConfig{
		Cluster: config.ClusterSettings{},
		Nodes:   []config.NodeConfig{{ID: selfID, Address: "localhost:0"}},
	}
	mem, _ := membership.New(selfID, minCfg)
	return mem
}

// --- local-only tests (single node in preference list) ---

func TestPutLocalOnly(t *testing.T) {
	ms := newMockStore()
	ring := &mockRing{nodes: []string{"node-1"}}
	cfg := &config.ClusterSettings{
		ReplicationFactor: 1,
		WriteQuorum:       1,
		ReadQuorum:        1,
	}

	c := New("node-1", ring, ms, nil, cfg)

	acks, err := c.Put(context.Background(), "hello", []byte("world"))
	if err != nil {
		t.Fatalf("Put() error: %v", err)
	}
	if acks != 1 {
		t.Errorf("Put() acks = %d, want 1", acks)
	}

	rec, err := ms.Get("hello")
	if err != nil {
		t.Fatalf("store.Get() error: %v", err)
	}
	if rec == nil {
		t.Fatal("expected record in store, got nil")
	}
	if string(rec.Value) != "world" {
		t.Errorf("stored value = %q, want %q", string(rec.Value), "world")
	}
	if rec.Timestamp == 0 {
		t.Error("stored timestamp should be non-zero")
	}
}

func TestGetLocalOnly(t *testing.T) {
	ms := newMockStore()
	ring := &mockRing{nodes: []string{"node-1"}}
	cfg := &config.ClusterSettings{
		ReplicationFactor: 1,
		WriteQuorum:       1,
		ReadQuorum:        1,
	}

	c := New("node-1", ring, ms, nil, cfg)

	_, err := c.Put(context.Background(), "hello", []byte("world"))
	if err != nil {
		t.Fatalf("Put() error: %v", err)
	}

	rec, err := c.Get(context.Background(), "hello")
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if rec == nil {
		t.Fatal("Get() returned nil, expected record")
	}
	if string(rec.Value) != "world" {
		t.Errorf("Get() value = %q, want %q", string(rec.Value), "world")
	}
}

func TestGetNotFoundLocalOnly(t *testing.T) {
	ms := newMockStore()
	ring := &mockRing{nodes: []string{"node-1"}}
	cfg := &config.ClusterSettings{
		ReplicationFactor: 1,
		WriteQuorum:       1,
		ReadQuorum:        1,
	}

	c := New("node-1", ring, ms, nil, cfg)

	rec, err := c.Get(context.Background(), "missing")
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if rec != nil {
		t.Errorf("Get() returned record for missing key: %+v", rec)
	}
}

func TestDeleteLocalOnly(t *testing.T) {
	ms := newMockStore()
	ring := &mockRing{nodes: []string{"node-1"}}
	cfg := &config.ClusterSettings{
		ReplicationFactor: 1,
		WriteQuorum:       1,
		ReadQuorum:        1,
	}

	c := New("node-1", ring, ms, nil, cfg)

	_, err := c.Put(context.Background(), "hello", []byte("world"))
	if err != nil {
		t.Fatalf("Put() error: %v", err)
	}

	deleted, err := c.Delete(context.Background(), "hello")
	if err != nil {
		t.Fatalf("Delete() error: %v", err)
	}
	if !deleted {
		t.Error("Delete() returned false, want true")
	}

	rec, err := ms.Get("hello")
	if err != nil {
		t.Fatalf("store.Get() error: %v", err)
	}
	if rec == nil || !rec.IsDelete {
		t.Error("expected tombstone record after delete")
	}
}

// --- empty ring tests ---

func TestPutEmptyRing(t *testing.T) {
	ms := newMockStore()
	ring := &mockRing{nodes: nil}
	cfg := &config.ClusterSettings{
		ReplicationFactor: 3,
		WriteQuorum:       2,
		ReadQuorum:        2,
	}

	c := New("node-1", ring, ms, nil, cfg)

	_, err := c.Put(context.Background(), "key", []byte("val"))
	if err == nil {
		t.Fatal("Put() with empty ring should fail")
	}
}

func TestGetEmptyRing(t *testing.T) {
	ms := newMockStore()
	ring := &mockRing{nodes: nil}
	cfg := &config.ClusterSettings{
		ReplicationFactor: 3,
		WriteQuorum:       2,
		ReadQuorum:        2,
	}

	c := New("node-1", ring, ms, nil, cfg)

	_, err := c.Get(context.Background(), "key")
	if err == nil {
		t.Fatal("Get() with empty ring should fail")
	}
}

func TestDeleteEmptyRing(t *testing.T) {
	ms := newMockStore()
	ring := &mockRing{nodes: nil}
	cfg := &config.ClusterSettings{
		ReplicationFactor: 3,
		WriteQuorum:       2,
		ReadQuorum:        2,
	}

	c := New("node-1", ring, ms, nil, cfg)

	_, err := c.Delete(context.Background(), "key")
	if err == nil {
		t.Fatal("Delete() with empty ring should fail")
	}
}

// --- LWW resolution tests ---

func TestLWWResolve_PicksHighestTimestamp(t *testing.T) {
	responses := []readResult{
		{nodeID: "node-1", rec: &store.Record{Value: []byte("old"), Timestamp: 100}},
		{nodeID: "node-2", rec: &store.Record{Value: []byte("new"), Timestamp: 200}},
		{nodeID: "node-3", rec: &store.Record{Value: []byte("mid"), Timestamp: 150}},
	}

	winner := lwwResolve(responses)
	if winner.nodeID != "node-2" {
		t.Errorf("lwwResolve winner = %q, want node-2", winner.nodeID)
	}
	if string(winner.rec.Value) != "new" {
		t.Errorf("lwwResolve value = %q, want %q", string(winner.rec.Value), "new")
	}
	if winner.rec.Timestamp != 200 {
		t.Errorf("lwwResolve timestamp = %d, want 200", winner.rec.Timestamp)
	}
}

func TestLWWResolve_NonNilWinsOverNil(t *testing.T) {
	responses := []readResult{
		{nodeID: "node-1", rec: nil},
		{nodeID: "node-2", rec: &store.Record{Value: []byte("data"), Timestamp: 100}},
		{nodeID: "node-3", rec: nil},
	}

	winner := lwwResolve(responses)
	if winner.nodeID != "node-2" {
		t.Errorf("lwwResolve winner = %q, want node-2", winner.nodeID)
	}
	if winner.rec == nil {
		t.Fatal("lwwResolve returned nil record, expected non-nil")
	}
}

func TestLWWResolve_AllNil(t *testing.T) {
	responses := []readResult{
		{nodeID: "node-1", rec: nil},
		{nodeID: "node-2", rec: nil},
	}

	winner := lwwResolve(responses)
	if winner.rec != nil {
		t.Errorf("lwwResolve should return nil record when all are nil, got %+v", winner.rec)
	}
}

func TestLWWResolve_TombstoneWithHigherTimestamp(t *testing.T) {
	responses := []readResult{
		{nodeID: "node-1", rec: &store.Record{Value: []byte("val"), Timestamp: 100}},
		{nodeID: "node-2", rec: &store.Record{IsDelete: true, Timestamp: 200}},
	}

	winner := lwwResolve(responses)
	if winner.nodeID != "node-2" {
		t.Errorf("lwwResolve winner = %q, want node-2 (tombstone with higher ts)", winner.nodeID)
	}
	if !winner.rec.IsDelete {
		t.Error("winner should be the tombstone")
	}
}

// --- read repair tests ---

func TestReadRepair_RepairsStaleLocal(t *testing.T) {
	ms := newMockStore()
	ring := &mockRing{nodes: []string{"node-1", "node-2", "node-3"}}
	cfg := &config.ClusterSettings{
		ReplicationFactor: 3,
		WriteQuorum:       2,
		ReadQuorum:        2,
	}

	// Use a real membership (self-only) so remotePut gets a clean error
	// instead of a nil pointer panic.
	mem := newMembershipSelfOnly("node-1")
	defer mem.Close()
	c := New("node-1", ring, ms, mem, cfg)

	winner := readResult{
		nodeID: "node-2",
		rec:    &store.Record{Value: []byte("latest"), Timestamp: 200},
	}
	responses := []readResult{
		{nodeID: "node-1", rec: &store.Record{Value: []byte("old"), Timestamp: 100}},
		winner,
		{nodeID: "node-3", rec: nil},
	}

	// readRepair should write to node-1 (self, stale) and skip node-3
	// (remote, we can't reach it without real connections but the goroutine
	// will log the error).
	c.readRepair("testkey", winner, responses)

	// Give goroutines time to complete.
	time.Sleep(100 * time.Millisecond)

	// Verify local store got the repair write.
	rec, err := ms.Get("testkey")
	if err != nil {
		t.Fatalf("store.Get() error: %v", err)
	}
	if rec == nil {
		t.Fatal("expected read-repaired record in local store")
	}
	if string(rec.Value) != "latest" {
		t.Errorf("repaired value = %q, want %q", string(rec.Value), "latest")
	}
	if rec.Timestamp != 200 {
		t.Errorf("repaired timestamp = %d, want 200", rec.Timestamp)
	}
}

func TestReadRepair_NoRepairWhenAllCurrent(t *testing.T) {
	ms := newMockStore()
	ring := &mockRing{nodes: []string{"node-1", "node-2"}}
	cfg := &config.ClusterSettings{
		ReplicationFactor: 2,
		WriteQuorum:       2,
		ReadQuorum:        2,
	}

	mem := newMembershipSelfOnly("node-1")
	defer mem.Close()
	c := New("node-1", ring, ms, mem, cfg)

	rec := &store.Record{Value: []byte("same"), Timestamp: 100}
	winner := readResult{nodeID: "node-1", rec: rec}
	responses := []readResult{
		winner,
		{nodeID: "node-2", rec: &store.Record{Value: []byte("same"), Timestamp: 100}},
	}

	c.readRepair("testkey", winner, responses)

	time.Sleep(50 * time.Millisecond)

	// Local store should not have the key since no repair was triggered
	// (node-1 is the winner, and node-2 has same timestamp).
	stored, _ := ms.Get("testkey")
	if stored != nil {
		t.Error("no read repair should have been triggered for node-1 (winner)")
	}
}

// --- quorum failure tests ---

func TestPutQuorumFailure(t *testing.T) {
	ms := newMockStore()
	ring := &mockRing{nodes: []string{"node-1", "node-2", "node-3"}}
	cfg := &config.ClusterSettings{
		ReplicationFactor: 3,
		WriteQuorum:       2,
		ReadQuorum:        2,
	}

	// Only self exists in membership; remote calls will fail.
	mem := newMembershipSelfOnly("node-1")
	defer mem.Close()
	c := New("node-1", ring, ms, mem, cfg)

	_, err := c.Put(context.Background(), "key", []byte("val"))
	if err == nil {
		t.Fatal("Put() should fail: only self acks (1 < W=2)")
	}

	// But the local store should still have the value (self succeeded).
	rec, _ := ms.Get("key")
	if rec == nil {
		t.Fatal("local store should have the value even though quorum failed")
	}
}

func TestDeleteQuorumFailure(t *testing.T) {
	ms := newMockStore()
	ring := &mockRing{nodes: []string{"node-1", "node-2", "node-3"}}
	cfg := &config.ClusterSettings{
		ReplicationFactor: 3,
		WriteQuorum:       2,
		ReadQuorum:        2,
	}

	mem := newMembershipSelfOnly("node-1")
	defer mem.Close()
	c := New("node-1", ring, ms, mem, cfg)

	_, err := c.Delete(context.Background(), "key")
	if err == nil {
		t.Fatal("Delete() should fail: only self acks (1 < W=2)")
	}
}

func TestGetQuorumFailure(t *testing.T) {
	ms := newMockStore()
	ring := &mockRing{nodes: []string{"node-1", "node-2", "node-3"}}
	cfg := &config.ClusterSettings{
		ReplicationFactor: 3,
		WriteQuorum:       2,
		ReadQuorum:        2,
	}

	_ = ms.Put("key", &store.Record{Value: []byte("val"), Timestamp: 100})

	mem := newMembershipSelfOnly("node-1")
	defer mem.Close()
	c := New("node-1", ring, ms, mem, cfg)

	_, err := c.Get(context.Background(), "key")
	if err == nil {
		t.Fatal("Get() should fail: only 1 response < R=2")
	}
}

// --- tombstone handling tests ---

func TestGetReturnNilForTombstone(t *testing.T) {
	ms := newMockStore()
	ring := &mockRing{nodes: []string{"node-1"}}
	cfg := &config.ClusterSettings{
		ReplicationFactor: 1,
		WriteQuorum:       1,
		ReadQuorum:        1,
	}

	_ = ms.Put("dead", &store.Record{IsDelete: true, Timestamp: 100})

	c := New("node-1", ring, ms, nil, cfg)

	rec, err := c.Get(context.Background(), "dead")
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if rec != nil {
		t.Errorf("Get() should return nil for tombstone, got %+v", rec)
	}
}

// --- single-node full lifecycle ---

func TestSingleNodeClusterPutGetDelete(t *testing.T) {
	ms := newMockStore()
	ring := &mockRing{nodes: []string{"node-1"}}
	cfg := &config.ClusterSettings{
		ReplicationFactor: 1,
		WriteQuorum:       1,
		ReadQuorum:        1,
	}

	c := New("node-1", ring, ms, nil, cfg)

	// Put
	acks, err := c.Put(context.Background(), "k", []byte("v"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if acks != 1 {
		t.Errorf("acks=%d want 1", acks)
	}

	// Get
	rec, err := c.Get(context.Background(), "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if rec == nil || string(rec.Value) != "v" {
		t.Fatalf("Get: unexpected %+v", rec)
	}

	// Delete
	deleted, err := c.Delete(context.Background(), "k")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !deleted {
		t.Error("Delete: want true")
	}

	// Get after delete returns nil
	rec, err = c.Get(context.Background(), "k")
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if rec != nil {
		t.Errorf("Get after delete: expected nil, got %+v", rec)
	}
}
