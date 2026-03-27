package search

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// inMemoryLockBackend is a test implementation of LockBackend
// that simulates ETag-based atomicity using a mutex-guarded map.
type inMemoryLockBackend struct {
	mu    sync.Mutex
	locks map[string]*LockInfo
}

func newInMemoryLockBackend() *inMemoryLockBackend {
	return &inMemoryLockBackend{
		locks: make(map[string]*LockInfo),
	}
}

func (b *inMemoryLockBackend) Create(_ context.Context, key string, info LockInfo) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if existing, ok := b.locks[key]; ok {
		if !existing.IsExpired(time.Now()) {
			return ErrLockHeld
		}
	}
	copied := info
	b.locks[key] = &copied
	return nil
}

func (b *inMemoryLockBackend) Update(_ context.Context, key string, info LockInfo) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	existing, ok := b.locks[key]
	if !ok {
		return ErrLockNotFound
	}
	if existing.Owner != info.Owner {
		return ErrLockHeld
	}
	copied := info
	b.locks[key] = &copied
	return nil
}

func (b *inMemoryLockBackend) Delete(_ context.Context, key string, owner string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	existing, ok := b.locks[key]
	if !ok {
		return ErrLockNotFound
	}
	if existing.Owner != owner {
		return ErrLockHeld
	}
	delete(b.locks, key)
	return nil
}

func (b *inMemoryLockBackend) Read(_ context.Context, key string) (*LockInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	info, ok := b.locks[key]
	if !ok {
		return nil, ErrLockNotFound
	}
	copied := *info
	return &copied, nil
}

func TestObjectStorageLock_AcquireRelease(t *testing.T) {
	backend := newInMemoryLockBackend()
	lock := NewObjectStorageLock(ObjectStorageLockConfig{
		Backend:           backend,
		Key:               "test-lock",
		Owner:             "instance-1",
		TTL:               5 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	})

	ctx := context.Background()

	// Acquire
	err := lock.Acquire(ctx)
	require.NoError(t, err)

	// Verify lock is held
	info, err := backend.Read(ctx, "test-lock")
	require.NoError(t, err)
	require.Equal(t, "instance-1", info.Owner)

	// Release
	err = lock.Release(ctx)
	require.NoError(t, err)

	// Verify lock is gone
	_, err = backend.Read(ctx, "test-lock")
	require.ErrorIs(t, err, ErrLockNotFound)
}

func TestObjectStorageLock_Contention(t *testing.T) {
	backend := newInMemoryLockBackend()

	lock1 := NewObjectStorageLock(ObjectStorageLockConfig{
		Backend:           backend,
		Key:               "test-lock",
		Owner:             "instance-1",
		TTL:               5 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	})
	lock2 := NewObjectStorageLock(ObjectStorageLockConfig{
		Backend:           backend,
		Key:               "test-lock",
		Owner:             "instance-2",
		TTL:               5 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	})

	ctx := context.Background()

	// Lock1 acquires
	require.NoError(t, lock1.Acquire(ctx))

	// Lock2 cannot acquire
	err := lock2.Acquire(ctx)
	require.ErrorIs(t, err, ErrLockHeld)

	// Release lock1
	require.NoError(t, lock1.Release(ctx))

	// Now lock2 can acquire
	require.NoError(t, lock2.Acquire(ctx))
	require.NoError(t, lock2.Release(ctx))
}

func TestObjectStorageLock_StaleTakeover(t *testing.T) {
	backend := newInMemoryLockBackend()

	lock2 := NewObjectStorageLock(ObjectStorageLockConfig{
		Backend:           backend,
		Key:               "test-lock",
		Owner:             "instance-2",
		TTL:               5 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	})

	ctx := context.Background()

	// Simulate a crashed instance holding an expired lock
	backend.mu.Lock()
	backend.locks["test-lock"] = &LockInfo{
		Owner:     "instance-1",
		TTL:       100 * time.Millisecond,
		Heartbeat: time.Now().Add(-200 * time.Millisecond), // already expired
	}
	backend.mu.Unlock()

	// Lock2 can take over expired lock
	require.NoError(t, lock2.Acquire(ctx))
	require.NoError(t, lock2.Release(ctx))
}

func TestObjectStorageLock_Heartbeat(t *testing.T) {
	backend := newInMemoryLockBackend()
	lock := NewObjectStorageLock(ObjectStorageLockConfig{
		Backend:           backend,
		Key:               "test-lock",
		Owner:             "instance-1",
		TTL:               5 * time.Second,
		HeartbeatInterval: 50 * time.Millisecond,
	})

	ctx := context.Background()
	require.NoError(t, lock.Acquire(ctx))

	// Get initial heartbeat
	info1, err := backend.Read(ctx, "test-lock")
	require.NoError(t, err)

	// Wait for heartbeat to fire
	time.Sleep(100 * time.Millisecond)

	// Heartbeat should have updated
	info2, err := backend.Read(ctx, "test-lock")
	require.NoError(t, err)
	require.True(t, info2.Heartbeat.After(info1.Heartbeat), "heartbeat should advance")

	require.NoError(t, lock.Release(ctx))
}
