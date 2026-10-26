package coordinator

import (
	"context"
	"testing"

	"github.com/alyelalwany/ghostkv/internal/config"
	"github.com/alyelalwany/ghostkv/internal/store"
)

// mockRing always returns the configured owner for any key.
type mockRing struct {
	owner string
}

func (r *mockRing) GetNode(_ string) string          { return r.owner }
func (r *mockRing) GetNodes(_ string, n int) []string { return []string{r.owner} }

// mockStore is an in-memory store for testing.
type mockStore struct {
	data map[string]*store.Record
}

func newMockStore() *mockStore {
	return &mockStore{data: make(map[string]*store.Record)}
}

func (s *mockStore) Get(key string) (*store.Record, error) {
	rec, ok := s.data[key]
	if !ok {
		return nil, nil
	}
	return rec, nil
}

func (s *mockStore) Put(key string, rec *store.Record) error {
	s.data[key] = rec
	return nil
}

func (s *mockStore) Delete(key string, timestamp int64) error {
	s.data[key] = &store.Record{IsDelete: true, Timestamp: timestamp}
	return nil
}

func (s *mockStore) ForEachInRange(_, _ string, fn func(string, *store.Record) error) error {
	for k, v := range s.data {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

func (s *mockStore) KeyCount() (int64, error) {
	var count int64
	for _, v := range s.data {
		if !v.IsDelete {
			count++
		}
	}
	return count, nil
}

func (s *mockStore) Close() error { return nil }

func TestLocalPut(t *testing.T) {
	ms := newMockStore()
	ring := &mockRing{owner: "node-1"}
	cfg := &config.ClusterSettings{
		ReplicationFactor: 3,
		WriteQuorum:       2,
		ReadQuorum:        2,
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

func TestLocalGet(t *testing.T) {
	ms := newMockStore()
	ring := &mockRing{owner: "node-1"}
	cfg := &config.ClusterSettings{}

	c := New("node-1", ring, ms, nil, cfg)

	// Put first.
	_, err := c.Put(context.Background(), "hello", []byte("world"))
	if err != nil {
		t.Fatalf("Put() error: %v", err)
	}

	// Get.
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

func TestLocalGetNotFound(t *testing.T) {
	ms := newMockStore()
	ring := &mockRing{owner: "node-1"}
	cfg := &config.ClusterSettings{}

	c := New("node-1", ring, ms, nil, cfg)

	rec, err := c.Get(context.Background(), "missing")
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if rec != nil {
		t.Errorf("Get() returned record for missing key: %+v", rec)
	}
}

func TestLocalDelete(t *testing.T) {
	ms := newMockStore()
	ring := &mockRing{owner: "node-1"}
	cfg := &config.ClusterSettings{}

	c := New("node-1", ring, ms, nil, cfg)

	// Put first.
	_, err := c.Put(context.Background(), "hello", []byte("world"))
	if err != nil {
		t.Fatalf("Put() error: %v", err)
	}

	// Delete.
	deleted, err := c.Delete(context.Background(), "hello")
	if err != nil {
		t.Fatalf("Delete() error: %v", err)
	}
	if !deleted {
		t.Error("Delete() returned false, want true")
	}

	// Verify tombstone.
	rec, err := ms.Get("hello")
	if err != nil {
		t.Fatalf("store.Get() error: %v", err)
	}
	if rec == nil || !rec.IsDelete {
		t.Error("expected tombstone record after delete")
	}
}
