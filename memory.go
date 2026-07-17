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

// bucket holds the state for one key. Fields are interpreted per algorithm:
//   - val: tokens available (token bucket), water level (leaky bucket), or the
//     count in the current window (fixed / sliding-counter).
//   - prev: the previous window's count (sliding window counter only).
//   - last: last update time (token / leaky bucket).
//   - window: active window index (fixed / sliding-counter).
//   - stamps: per-event timestamps within the trailing window (sliding window).
type bucket struct {
	val    float64
	prev   float64
	last   time.Time
	window int64
	stamps []time.Time
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
	switch a {
	case TokenBucket, LeakyBucket, FixedWindow, SlidingWindow, SlidingWindowCounter, GCRA:
		return true
	default:
		return false
	}
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
	case SlidingWindow:
		return slidingWindowStep(b, p, n, now), nil
	case SlidingWindowCounter:
		return slidingWindowCounterStep(b, p, n, now), nil
	case GCRA:
		return gcraStep(b, p, n, now), nil
	default:
		return false, fmt.Errorf("%w: %q", ErrUnsupportedAlgorithm, p.Algorithm)
	}
}

// windowDuration returns the window length for the window-based algorithms:
// Burst/Rate seconds, expressed as a time.Duration.
func windowDuration(p Policy) time.Duration {
	return time.Duration(float64(p.Burst) / p.Rate * float64(time.Second))
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

// slidingWindowStep advances an exact sliding window log and decides admission.
// It keeps a timestamp per admitted event, drops any older than the trailing
// window (Burst/Rate seconds), and allows if the remaining count plus n stays
// within Burst.
func slidingWindowStep(b *bucket, p Policy, n int, now time.Time) bool {
	b.init = true
	cutoff := now.Add(-windowDuration(p))

	// stamps is kept in ascending order, so drop the leading run at/before the
	// cutoff (i.e. events no longer inside the trailing window).
	drop := 0
	for drop < len(b.stamps) && !b.stamps[drop].After(cutoff) {
		drop++
	}
	if drop > 0 {
		b.stamps = b.stamps[drop:]
	}

	if len(b.stamps)+n <= p.Burst {
		for i := 0; i < n; i++ {
			b.stamps = append(b.stamps, now)
		}
		return true
	}
	return false
}

// slidingWindowCounterStep advances the approximate sliding window counter and
// decides admission. It estimates the trailing count as
// prev*(1-frac) + cur, where frac is how far the current window has advanced,
// then admits if that estimate plus n stays within Burst.
func slidingWindowCounterStep(b *bucket, p Policy, n int, now time.Time) bool {
	limit := float64(p.Burst)
	windowSec := float64(p.Burst) / p.Rate
	pos := float64(now.UnixNano()) / float64(time.Second) / windowSec
	windowID := int64(pos) // floor for non-negative values
	frac := pos - float64(windowID)

	switch {
	case !b.init:
		b.val, b.prev, b.window, b.init = 0, 0, windowID, true
	case windowID == b.window+1:
		// Advanced exactly one window: the current count becomes previous.
		b.prev, b.val, b.window = b.val, 0, windowID
	case windowID > b.window:
		// Skipped one or more windows: no recent history remains.
		b.prev, b.val, b.window = 0, 0, windowID
	}

	estimated := b.prev*(1-frac) + b.val
	if estimated+float64(n) <= limit {
		b.val += float64(n)
		return true
	}
	return false
}

// gcraStep advances the Generic Cell Rate Algorithm and decides admission.
//
// State is a single "theoretical arrival time" (TAT), stored in b.last: the
// instant at which the flow would be exactly on schedule. Each admitted event
// pushes the TAT forward by the emission interval (1/Rate); a request is allowed
// as long as the TAT does not run more than the burst tolerance (Burst/Rate)
// ahead of now.
func gcraStep(b *bucket, p Policy, n int, now time.Time) bool {
	emission := time.Duration(float64(time.Second) / p.Rate) // 1/Rate
	tolerance := time.Duration(p.Burst) * emission           // Burst emissions
	increment := time.Duration(n) * emission

	// tat = max(storedTAT, now): a TAT in the past means the flow is idle, so
	// it resets to now.
	tat := now
	if b.init && b.last.After(now) {
		tat = b.last
	}

	newTAT := tat.Add(increment)
	allowAt := newTAT.Add(-tolerance)
	if now.Before(allowAt) {
		return false // too far ahead of schedule
	}

	b.last, b.init = newTAT, true
	return true
}
