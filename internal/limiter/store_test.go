package limiter

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func frozenClock() time.Time { return start }

func (s *Store) size() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.buckets)
}

func TestStoreKeysAreIndependent(t *testing.T) {
	s := NewStore(Config{Capacity: 1, RefillRate: 1, IdleTTL: time.Minute, Clock: frozenClock})

	if ok, _ := s.Allow("a"); !ok {
		t.Fatal("first request for a was denied")
	}
	if ok, _ := s.Allow("a"); ok {
		t.Fatal("second request for a was allowed")
	}
	if ok, _ := s.Allow("b"); !ok {
		t.Fatal("b was denied; it should have its own bucket")
	}
}

func TestStoreConcurrentAllow(t *testing.T) {
	const (
		capacity = 50
		keys     = 10
		perKey   = 200
	)
	s := NewStore(Config{Capacity: capacity, RefillRate: 1, IdleTTL: time.Minute, Clock: frozenClock})

	var allowed [keys]atomic.Int64
	var wg sync.WaitGroup
	for k := range keys {
		key := fmt.Sprintf("client-%d", k)
		for range perKey {
			wg.Go(func() {
				if ok, _ := s.Allow(key); ok {
					allowed[k].Add(1)
				}
			})
		}
	}
	wg.Wait()

	for k := range keys {
		if got := allowed[k].Load(); got != capacity {
			t.Errorf("client-%d: allowed %d requests, want %d", k, got, capacity)
		}
	}
}

func TestStoreCleanup(t *testing.T) {
	now := start
	s := NewStore(Config{
		Capacity: 1, RefillRate: 1, IdleTTL: time.Minute,
		Clock: func() time.Time { return now },
	})

	s.Allow("old")
	now = now.Add(45 * time.Second)
	s.Allow("recent")
	now = now.Add(30 * time.Second) // old idle 75s, recent idle 30s

	if removed := s.Cleanup(); removed != 1 {
		t.Errorf("Cleanup() removed %d buckets, want 1", removed)
	}
	if _, ok := s.buckets["old"]; ok {
		t.Error("idle bucket was not removed")
	}
	if _, ok := s.buckets["recent"]; !ok {
		t.Error("active bucket was removed")
	}
}

func TestRunCleanup(t *testing.T) {
	now := start
	s := NewStore(Config{
		Capacity: 1, RefillRate: 1, IdleTTL: time.Minute,
		Clock: func() time.Time { return now },
	})
	s.Allow("a")
	now = now.Add(time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.RunCleanup(ctx, time.Millisecond)
		close(done)
	}()

	deadline := time.After(time.Second)
	for s.size() > 0 {
		select {
		case <-deadline:
			t.Fatal("background cleanup never removed the idle bucket")
		case <-time.After(time.Millisecond):
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunCleanup did not return after cancel")
	}
}
