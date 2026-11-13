// Package limiter implements a token bucket rate limiter.
package limiter

import "time"

// Bucket is a single token bucket. It is not safe for concurrent use;
// callers must synchronize access to it.
type Bucket struct {
	capacity   float64
	refillRate float64 // tokens per second
	tokens     float64
	lastRefill time.Time
}

// NewBucket returns a full bucket.
func NewBucket(capacity, refillRate float64, now time.Time) *Bucket {
	return &Bucket{
		capacity:   capacity,
		refillRate: refillRate,
		tokens:     capacity,
		lastRefill: now,
	}
}

// Allow reports whether a request at time now may proceed, consuming a token
// if so. When denied, it also returns how long until a token is available.
func (b *Bucket) Allow(now time.Time) (bool, time.Duration) {
	b.refill(now)

	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}

	missing := 1 - b.tokens
	wait := time.Duration(missing / b.refillRate * float64(time.Second))
	return false, wait
}

func (b *Bucket) refill(now time.Time) {
	elapsed := now.Sub(b.lastRefill)
	if elapsed <= 0 {
		return
	}
	b.tokens = min(b.capacity, b.tokens+elapsed.Seconds()*b.refillRate)
	b.lastRefill = now
}
