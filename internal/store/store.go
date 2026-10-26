package store

// Record is the unit of storage in GhostKV.
// Every key maps to a Record that carries the value, a timestamp for
// last-writer-wins (LWW) conflict resolution, and a tombstone flag.
type Record struct {
	Value     []byte
	Timestamp int64 // Unix nanoseconds
	IsDelete  bool  // tombstone marker
}

// Store is the storage interface for a single GhostKV node.
type Store interface {
	// Get retrieves the record for the given key.
	// Returns (nil, nil) when the key does not exist.
	Get(key string) (*Record, error)

	// Put writes a record for the given key.
	Put(key string, rec *Record) error

	// Delete writes a tombstone record for the given key.
	Delete(key string, timestamp int64) error

	// ForEachInRange iterates over keys in the half-open range [start, end).
	// An empty start means "from the beginning"; an empty end means "to the end".
	// The callback can return a non-nil error to stop iteration early.
	ForEachInRange(start, end string, fn func(key string, rec *Record) error) error

	// KeyCount returns the number of live (non-tombstone) keys.
	KeyCount() (int64, error)

	// Close releases all resources held by the store.
	Close() error
}
