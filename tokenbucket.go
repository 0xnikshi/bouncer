package bouncer

import (
	"sync"
	"time"
)

func init() {
	Register(TokenBucket, newTokenBucket)
}

// tokenBucket implements the token bucket algorithm.
//
// Tokens accrue continuously at refillRate up to a ceiling of capacity. Each
// admitted event removes tokens. Because a full bucket can be drained in one
// go, the token bucket permits bursts up to capacity while bounding the
// long-run average to refillRate.
type tokenBucket struct {
	capacity   float64
	refillRate float64 // tokens per second
	clk        Clock

	mu     sync.Mutex
	tokens float64
	last   time.Time
}

func newTokenBucket(cfg Config, clk Clock) (Limiter, error) {
	return &tokenBucket{
		capacity:   float64(cfg.Burst),
		refillRate: cfg.Rate,
		clk:        clk,
		tokens:     float64(cfg.Burst), // start full so an initial burst is allowed
		last:       clk.Now(),
	}, nil
}

func (b *tokenBucket) Allow() bool { return b.AllowN(1) }

func (b *tokenBucket) AllowN(n int) bool {
	if n == 0 {
		return true
	}
	if n < 0 {
		return false
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.refill()

	need := float64(n)
	if b.tokens >= need {
		b.tokens -= need
		return true
	}
	return false
}

// refill adds tokens for the time elapsed since the last update, capped at
// capacity. The caller must hold b.mu.
func (b *tokenBucket) refill() {
	now := b.clk.Now()
	elapsed := now.Sub(b.last)
	if elapsed <= 0 {
		// Clock did not advance (or went backwards); nothing to add. Still
		// move the marker forward to avoid a future negative elapsed.
		b.last = now
		return
	}
	b.tokens += elapsed.Seconds() * b.refillRate
	if b.tokens > b.capacity {
		b.tokens = b.capacity
	}
	b.last = now
}
