package bouncer

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MemoryStore is an in-process Store: bucket state lives in a map guarded by a
// mutex. It has no external dependencies and is ideal for a single process. For
// a multi-instance fleet use a distributed Store (e.g. redisstore) so the limit
// is shared.
//
// Note: MemoryStore keeps one bucket per key for the life of the process; with
// high-cardinality keys (e.g. per-IP across the whole internet) the map grows
// unbounded. For such workloads prefer a distributed store with key TTLs, or
// request an eviction policy here.
type MemoryStore struct {
	clk Clock

	mu      sync.Mutex
	buckets map[string]*bucket
}

// bucket holds the state for one key. val is interpreted per algorithm: tokens
// available (token bucket), current water level (leaky bucket), or the count of
// events in the current window (fixed window). window is the active window's
// index, used only by the fixed window algorithm.
type bucket struct {
	val    float64
	last   time.Time
	window int64
	init   bool
}

// MemoryOption customizes a MemoryStore.
type MemoryOption func(*MemoryStore)

// WithClock overrides the time source, primarily for deterministic tests.
func WithClock(clk Clock) MemoryOption {
	return func(s *MemoryStore) {
		if clk != nil {
			s.clk = clk
		}
	}
}

// NewMemoryStore returns an in-memory Store.
func NewMemoryStore(opts ...MemoryOption) *MemoryStore {
	s := &MemoryStore{clk: SystemClock, buckets: make(map[string]*bucket)}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Supports reports the algorithms MemoryStore implements.
func (s *MemoryStore) Supports(a Algorithm) bool {
	return a == TokenBucket || a == LeakyBucket || a == FixedWindow
}

// Allow applies p to key for n events. See Store for the contract.
func (s *MemoryStore) Allow(_ context.Context, key string, p Policy, n int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	b := s.buckets[key]
	if b == nil {
		b = &bucket{}
		s.buckets[key] = b
	}
	now := s.clk.Now()

	switch p.Algorithm {
	case TokenBucket:
		return tokenBucketStep(b, p, n, now), nil
	case LeakyBucket:
		return leakyBucketStep(b, p, n, now), nil
	case FixedWindow:
		return fixedWindowStep(b, p, n, now), nil
	default:
		return false, fmt.Errorf("%w: %q", ErrUnsupportedAlgorithm, p.Algorithm)
	}
}

// tokenBucketStep advances a token bucket and decides admission. Tokens accrue
// at p.Rate up to p.Burst; a fresh bucket starts full.
func tokenBucketStep(b *bucket, p Policy, n int, now time.Time) bool {
	capacity := float64(p.Burst)
	if !b.init {
		b.val, b.last, b.init = capacity, now, true
	}
	if elapsed := now.Sub(b.last); elapsed > 0 {
		b.val += elapsed.Seconds() * p.Rate
		if b.val > capacity {
			b.val = capacity
		}
	}
	b.last = now

	need := float64(n)
	if b.val >= need {
		b.val -= need
		return true
	}
	return false
}

// leakyBucketStep advances a leaky bucket (meter) and decides admission. The
// level drains at p.Rate; a fresh bucket starts empty.
func leakyBucketStep(b *bucket, p Policy, n int, now time.Time) bool {
	capacity := float64(p.Burst)
	if !b.init {
		b.val, b.last, b.init = 0, now, true
	}
	if elapsed := now.Sub(b.last); elapsed > 0 {
		b.val -= elapsed.Seconds() * p.Rate
		if b.val < 0 {
			b.val = 0
		}
	}
	b.last = now

	add := float64(n)
	if b.val+add <= capacity {
		b.val += add
		return true
	}
	return false
}

// fixedWindowStep advances a fixed window counter and decides admission. Up to
// p.Burst events are allowed per window of length p.Burst/p.Rate seconds; the
// count resets when the window boundary is crossed. Windows are aligned to the
// Unix epoch so all keys share boundaries.
func fixedWindowStep(b *bucket, p Policy, n int, now time.Time) bool {
	limit := float64(p.Burst)
	windowSec := float64(p.Burst) / p.Rate
	nowSec := float64(now.UnixNano()) / float64(time.Second)
	// Integer division floors for non-negative values, giving the window index.
	windowID := int64(nowSec / windowSec)

	if !b.init || b.window != windowID {
		b.val, b.window, b.init = 0, windowID, true
	}

	if b.val+float64(n) <= limit {
		b.val += float64(n)
		return true
	}
	return false
}
