package search

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// LockInfo contains the metadata for a distributed lock.
type LockInfo struct {
	Owner     string        `json:"owner"`
	TTL       time.Duration `json:"ttl"`
	Heartbeat time.Time     `json:"heartbeat"`
}

// IsExpired returns true if the lock's heartbeat + TTL has passed.
func (l *LockInfo) IsExpired(now time.Time) bool {
	return now.After(l.Heartbeat.Add(l.TTL))
}

// LockBackend abstracts conditional storage operations for distributed locking.
// Production implementations use ETag-based conditional writes on S3/GCS/Azure.
// Test implementations use an in-memory mutex-guarded map.
type LockBackend interface {
	// Create atomically creates a lock if it does not exist.
	// Returns ErrLockHeld if lock already exists and is not expired.
	Create(ctx context.Context, key string, info LockInfo) error

	// Update atomically updates an existing lock, verifying ownership.
	// Returns error if lock does not exist or is owned by a different owner.
	Update(ctx context.Context, key string, info LockInfo) error

	// Delete atomically deletes a lock, verifying ownership.
	// Returns error if lock does not exist or is owned by a different owner.
	Delete(ctx context.Context, key string, owner string) error

	// Read returns the current lock info, or ErrLockNotFound if no lock exists.
	Read(ctx context.Context, key string) (*LockInfo, error)
}

// ErrLockHeld is returned when a lock cannot be acquired because it is held by another owner.
var ErrLockHeld = fmt.Errorf("lock is held by another owner")

// ErrLockNotFound is returned when a lock operation targets a non-existent lock.
var ErrLockNotFound = fmt.Errorf("lock not found")

// ObjectStorageLock provides distributed locking via conditional object storage writes.
type ObjectStorageLock struct {
	backend           LockBackend
	key               string
	owner             string
	ttl               time.Duration
	heartbeatInterval time.Duration

	mu       sync.Mutex
	held     bool
	cancelHB context.CancelFunc
	hbDone   chan struct{}

	// LostCh is closed if the lock is lost due to a heartbeat failure.
	// Callers holding the lock should select on this channel to detect lock loss.
	LostCh chan struct{}
}

// ObjectStorageLockConfig holds configuration for creating an ObjectStorageLock.
type ObjectStorageLockConfig struct {
	Backend           LockBackend
	Key               string
	Owner             string
	TTL               time.Duration // Default: 180s
	HeartbeatInterval time.Duration // Default: 60s
}

// NewObjectStorageLock creates a new distributed lock.
func NewObjectStorageLock(cfg ObjectStorageLockConfig) *ObjectStorageLock {
	if cfg.TTL == 0 {
		cfg.TTL = 180 * time.Second
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 60 * time.Second
	}
	return &ObjectStorageLock{
		backend:           cfg.Backend,
		key:               cfg.Key,
		owner:             cfg.Owner,
		ttl:               cfg.TTL,
		heartbeatInterval: cfg.HeartbeatInterval,
		LostCh:            make(chan struct{}),
	}
}

// Acquire attempts to acquire the distributed lock.
// If the lock is held by another owner and not expired, returns ErrLockHeld.
// Starts a background heartbeat goroutine on success.
func (l *ObjectStorageLock) Acquire(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.held {
		return fmt.Errorf("lock already held by this instance")
	}

	info := LockInfo{
		Owner:     l.owner,
		TTL:       l.ttl,
		Heartbeat: time.Now(),
	}

	if err := l.backend.Create(ctx, l.key, info); err != nil {
		return err
	}

	l.held = true
	l.startHeartbeat()
	return nil
}

// Release releases the distributed lock and stops the heartbeat goroutine.
func (l *ObjectStorageLock) Release(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.held {
		return nil
	}

	l.stopHeartbeat()

	err := l.backend.Delete(ctx, l.key, l.owner)
	l.held = false
	l.LostCh = make(chan struct{})
	return err
}

func (l *ObjectStorageLock) startHeartbeat() {
	ctx, cancel := context.WithCancel(context.Background())
	l.cancelHB = cancel
	l.hbDone = make(chan struct{})

	go func() {
		defer close(l.hbDone)
		ticker := time.NewTicker(l.heartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				info := LockInfo{
					Owner:     l.owner,
					TTL:       l.ttl,
					Heartbeat: time.Now(),
				}
				if err := l.backend.Update(context.Background(), l.key, info); err != nil {
					l.mu.Lock()
					l.held = false
					close(l.LostCh)
					l.mu.Unlock()
					return // Lock lost
				}
			}
		}
	}()
}

func (l *ObjectStorageLock) stopHeartbeat() {
	if l.cancelHB != nil {
		l.cancelHB()
		<-l.hbDone
		l.cancelHB = nil
		l.hbDone = nil
	}
}

