package store

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *BoltStore {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := NewBoltStore(path)
	if err != nil {
		t.Fatalf("NewBoltStore: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestPutAndGet(t *testing.T) {
	s := newTestStore(t)

	ts := time.Now().UnixNano()
	if err := s.Put("hello", &Record{
		Value:     []byte("world"),
		Timestamp: ts,
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	rec, err := s.Get("hello")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if rec == nil {
		t.Fatal("expected record, got nil")
	}
	if string(rec.Value) != "world" {
		t.Errorf("Value = %q, want %q", rec.Value, "world")
	}
	if rec.Timestamp != ts {
		t.Errorf("Timestamp = %d, want %d", rec.Timestamp, ts)
	}
	if rec.IsDelete {
		t.Error("IsDelete should be false")
	}
}

func TestGetMissing(t *testing.T) {
	s := newTestStore(t)

	rec, err := s.Get("nonexistent")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if rec != nil {
		t.Fatalf("expected nil record, got %+v", rec)
	}
}

func TestDeleteWritesTombstone(t *testing.T) {
	s := newTestStore(t)

	ts := time.Now().UnixNano()
	if err := s.Put("key1", &Record{
		Value:     []byte("val1"),
		Timestamp: ts,
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	delTS := ts + 1
	if err := s.Delete("key1", delTS); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	rec, err := s.Get("key1")
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if rec == nil {
		t.Fatal("expected tombstone record, got nil")
	}
	if !rec.IsDelete {
		t.Error("expected IsDelete = true")
	}
	if rec.Timestamp != delTS {
		t.Errorf("Timestamp = %d, want %d", rec.Timestamp, delTS)
	}
}

func TestKeyCountExcludesTombstones(t *testing.T) {
	s := newTestStore(t)

	ts := time.Now().UnixNano()
	for i := 0; i < 5; i++ {
		if err := s.Put(fmt.Sprintf("key%d", i), &Record{
			Value:     []byte(fmt.Sprintf("val%d", i)),
			Timestamp: ts,
		}); err != nil {
			t.Fatalf("Put key%d: %v", i, err)
		}
	}

	// Delete two keys.
	if err := s.Delete("key1", ts+1); err != nil {
		t.Fatalf("Delete key1: %v", err)
	}
	if err := s.Delete("key3", ts+1); err != nil {
		t.Fatalf("Delete key3: %v", err)
	}

	count, err := s.KeyCount()
	if err != nil {
		t.Fatalf("KeyCount: %v", err)
	}
	if count != 3 {
		t.Errorf("KeyCount = %d, want 3", count)
	}
}

func TestForEachInRange(t *testing.T) {
	s := newTestStore(t)

	ts := time.Now().UnixNano()
	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		if err := s.Put(k, &Record{
			Value:     []byte("v-" + k),
			Timestamp: ts,
		}); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}

	// Range [b, d) should yield b and c.
	var got []string
	err := s.ForEachInRange("b", "d", func(key string, rec *Record) error {
		got = append(got, key)
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachInRange: %v", err)
	}
	if len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Errorf("got %v, want [b c]", got)
	}
}

func TestForEachInRangeOpenStart(t *testing.T) {
	s := newTestStore(t)

	ts := time.Now().UnixNano()
	for _, k := range []string{"a", "b", "c"} {
		if err := s.Put(k, &Record{Value: []byte(k), Timestamp: ts}); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	// Empty start means from beginning, end "b" means up to (but not including) b.
	var got []string
	err := s.ForEachInRange("", "b", func(key string, _ *Record) error {
		got = append(got, key)
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachInRange: %v", err)
	}
	if len(got) != 1 || got[0] != "a" {
		t.Errorf("got %v, want [a]", got)
	}
}

func TestForEachInRangeOpenEnd(t *testing.T) {
	s := newTestStore(t)

	ts := time.Now().UnixNano()
	for _, k := range []string{"a", "b", "c"} {
		if err := s.Put(k, &Record{Value: []byte(k), Timestamp: ts}); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	// Start "b", empty end means to the end.
	var got []string
	err := s.ForEachInRange("b", "", func(key string, _ *Record) error {
		got = append(got, key)
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachInRange: %v", err)
	}
	if len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Errorf("got %v, want [b c]", got)
	}
}

func TestForEachInRangeFullScan(t *testing.T) {
	s := newTestStore(t)

	ts := time.Now().UnixNano()
	for _, k := range []string{"x", "y", "z"} {
		if err := s.Put(k, &Record{Value: []byte(k), Timestamp: ts}); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	var got []string
	err := s.ForEachInRange("", "", func(key string, _ *Record) error {
		got = append(got, key)
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachInRange: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("got %d keys, want 3", len(got))
	}
}

func TestForEachInRangeEarlyStop(t *testing.T) {
	s := newTestStore(t)

	ts := time.Now().UnixNano()
	for _, k := range []string{"a", "b", "c", "d"} {
		if err := s.Put(k, &Record{Value: []byte(k), Timestamp: ts}); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	stopErr := fmt.Errorf("stop")
	var got []string
	err := s.ForEachInRange("", "", func(key string, _ *Record) error {
		got = append(got, key)
		if key == "b" {
			return stopErr
		}
		return nil
	})
	if err != stopErr {
		t.Fatalf("expected stopErr, got %v", err)
	}
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("got %v, want [a b]", got)
	}
}

// Verify BoltStore satisfies the Store interface at compile time.
var _ Store = (*BoltStore)(nil)
