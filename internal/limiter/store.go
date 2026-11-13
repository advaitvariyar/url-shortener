package limiter

import (
	"context"
	"sync"
	"time"
)

type Config struct {
	Capacity   float64
	RefillRate float64 // tokens per second

	// IdleTTL is how long a bucket can go unused before Cleanup removes it.
	// If it is at least Capacity/RefillRate seconds, removed buckets would
	// have been full anyway, so removing them changes nothing for clients.
	IdleTTL time.Duration

	// Clock returns the current time. Defaults to time.Now.
	Clock func() time.Time
}

// Store holds one Bucket per client key. It is safe for concurrent use.
type Store struct {
	mu      sync.Mutex
	buckets map[string]*Bucket
	cfg     Config
}

func NewStore(cfg Config) *Store {
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	return &Store{
		buckets: make(map[string]*Bucket),
		cfg:     cfg,
	}
}

// Allow checks the bucket for key, creating it if needed.
func (s *Store) Allow(key string) (bool, time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.cfg.Clock()
	b, ok := s.buckets[key]
	if !ok {
		b = NewBucket(s.cfg.Capacity, s.cfg.RefillRate, now)
		s.buckets[key] = b
	}
	return b.Allow(now)
}

// Cleanup removes buckets idle for longer than IdleTTL and returns how many
// it removed.
func (s *Store) Cleanup() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.cfg.Clock()
	removed := 0
	for key, b := range s.buckets {
		if now.Sub(b.lastRefill) > s.cfg.IdleTTL {
			delete(s.buckets, key)
			removed++
		}
	}
	return removed
}

// RunCleanup calls Cleanup every interval until ctx is cancelled.
// Run it in its own goroutine.
func (s *Store) RunCleanup(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.Cleanup()
		}
	}
}
