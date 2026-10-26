package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	bolt "go.etcd.io/bbolt"
)

var dataBucket = []byte("data")

// BoltStore implements Store backed by a single bbolt database file.
type BoltStore struct {
	db *bolt.DB
}

// NewBoltStore opens (or creates) a bbolt database at path.
func NewBoltStore(path string) (*BoltStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create db directory: %w", err)
	}

	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("open bbolt: %w", err)
	}

	// Ensure the data bucket exists.
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(dataBucket)
		return err
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("create data bucket: %w", err)
	}

	return &BoltStore{db: db}, nil
}

// Get retrieves the record for key. Returns (nil, nil) if the key is absent.
func (s *BoltStore) Get(key string) (*Record, error) {
	var rec *Record
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dataBucket)
		v := b.Get([]byte(key))
		if v == nil {
			return nil
		}
		rec = &Record{}
		return json.Unmarshal(v, rec)
	})
	return rec, err
}

// Put stores a record under key.
func (s *BoltStore) Put(key string, rec *Record) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(dataBucket)
		data, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("marshal record: %w", err)
		}
		return b.Put([]byte(key), data)
	})
}

// Delete writes a tombstone for key with the given timestamp.
func (s *BoltStore) Delete(key string, timestamp int64) error {
	rec := &Record{
		IsDelete:  true,
		Timestamp: timestamp,
	}
	return s.Put(key, rec)
}

// ForEachInRange iterates keys in [start, end). Empty start/end means
// "from the beginning" / "to the end" respectively.
func (s *BoltStore) ForEachInRange(start, end string, fn func(string, *Record) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(dataBucket).Cursor()

		var k, v []byte
		if start == "" {
			k, v = c.First()
		} else {
			k, v = c.Seek([]byte(start))
		}

		for ; k != nil; k, v = c.Next() {
			if end != "" && string(k) >= end {
				break
			}
			var rec Record
			if err := json.Unmarshal(v, &rec); err != nil {
				return fmt.Errorf("unmarshal key %q: %w", string(k), err)
			}
			if err := fn(string(k), &rec); err != nil {
				return err
			}
		}

		return nil
	})
}

// KeyCount returns the number of live (non-tombstone) keys.
func (s *BoltStore) KeyCount() (int64, error) {
	var count int64
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(dataBucket).ForEach(func(k, v []byte) error {
			var rec Record
			if err := json.Unmarshal(v, &rec); err != nil {
				return err
			}
			if !rec.IsDelete {
				count++
			}
			return nil
		})
	})
	return count, err
}

// Close closes the underlying bbolt database.
func (s *BoltStore) Close() error {
	return s.db.Close()
}
