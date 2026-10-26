// Package store defines the storage interface for GhostKV.
package store

// Record represents a single key-value entry in the store.
type Record struct {
	Value     []byte
	Timestamp int64
	IsDelete  bool
}

// Store is the interface that all storage backends must implement.
type Store interface {
	Get(key string) (*Record, error)
	Put(key string, rec *Record) error
	Delete(key string, timestamp int64) error
	ForEachInRange(start, end string, fn func(key string, rec *Record) error) error
	KeyCount() (int64, error)
	Close() error
}
