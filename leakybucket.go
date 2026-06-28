package bouncer

import (
	"sync"
	"time"
)

func init() {
	Register(LeakyBucket, newLeakyBucket)
}

// leakyBucket implements the leaky bucket algorithm (as a meter).
//
// Conceptually the bucket holds queued events ("water"). Each admitted event
// adds one unit; the bucket drains continuously at leakRate. An event is
// admitted only if it fits below capacity. Unlike the token bucket, the leaky
// bucket models a fixed-size queue draining at a steady rate, so it smooths
// traffic toward a constant outflow rather than permitting the whole burst to
// fire at once and then refill.
type leakyBucket struct {
	capacity float64
	leakRate float64 // events drained per second
	clk      Clock

	mu    sync.Mutex
	level float64 // current amount of water in the bucket
	last  time.Time
}

func newLeakyBucket(cfg Config, clk Clock) (Limiter, error) {
	return &leakyBucket{
		capacity: float64(cfg.Burst),
		leakRate: cfg.Rate,
		clk:      clk,
		level:    0, // start empty so an initial burst up to capacity is allowed
		last:     clk.Now(),
	}, nil
}

func (b *leakyBucket) Allow() bool { return b.AllowN(1) }

func (b *leakyBucket) AllowN(n int) bool {
	if n == 0 {
		return true
	}
	if n < 0 {
		return false
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.leak()

	add := float64(n)
	if b.level+add <= b.capacity {
		b.level += add
		return true
	}
	return false
}

// leak drains water for the time elapsed since the last update, clamped at
// empty. The caller must hold b.mu.
func (b *leakyBucket) leak() {
	now := b.clk.Now()
	elapsed := now.Sub(b.last)
	if elapsed <= 0 {
		b.last = now
		return
	}
	b.level -= elapsed.Seconds() * b.leakRate
	if b.level < 0 {
		b.level = 0
	}
	b.last = now
}
