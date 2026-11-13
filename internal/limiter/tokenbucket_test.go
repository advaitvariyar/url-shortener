package limiter

import (
	"testing"
	"time"
)

var start = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestBucketAllow(t *testing.T) {
	type step struct {
		at   time.Duration // offset from start
		want bool
	}

	tests := []struct {
		name     string
		capacity float64
		rate     float64
		steps    []step
	}{
		{
			name:     "allows up to capacity then denies",
			capacity: 3, rate: 1,
			steps: []step{{0, true}, {0, true}, {0, true}, {0, false}},
		},
		{
			name:     "refills over time",
			capacity: 2, rate: 2,
			steps: []step{
				{0, true}, {0, true}, {0, false},
				{500 * time.Millisecond, true},
				{500 * time.Millisecond, false},
				{time.Second, true},
			},
		},
		{
			name:     "never exceeds capacity after long idle",
			capacity: 2, rate: 10,
			steps: []step{
				{0, true}, {0, true},
				{10 * time.Second, true}, {10 * time.Second, true}, {10 * time.Second, false},
			},
		},
		{
			name:     "partial token is not enough",
			capacity: 1, rate: 1,
			steps: []step{{0, true}, {500 * time.Millisecond, false}, {time.Second, true}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewBucket(tt.capacity, tt.rate, start)
			for i, s := range tt.steps {
				got, _ := b.Allow(start.Add(s.at))
				if got != s.want {
					t.Errorf("step %d (t=%v): Allow() = %v, want %v", i, s.at, got, s.want)
				}
			}
		})
	}
}

func TestBucketRetryAfter(t *testing.T) {
	b := NewBucket(1, 2, start)
	b.Allow(start)

	tests := []struct {
		at   time.Duration
		want time.Duration
	}{
		{0, 500 * time.Millisecond},
		{250 * time.Millisecond, 250 * time.Millisecond},
	}

	for _, tt := range tests {
		ok, wait := b.Allow(start.Add(tt.at))
		if ok {
			t.Fatalf("t=%v: expected denial", tt.at)
		}
		if wait != tt.want {
			t.Errorf("t=%v: retry after = %v, want %v", tt.at, wait, tt.want)
		}
	}
}
