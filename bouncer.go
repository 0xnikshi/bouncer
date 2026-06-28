// Package bouncer is a pluggable rate limiter.
//
// A rate policy (algorithm + rate + burst) is enforced by a Limiter, but the
// state behind that policy lives in a pluggable Store — the "broker". The
// in-memory store (this package) needs no dependencies and suits a single
// process; the Redis store (subpackage redisstore) shares state across a fleet.
// Any backend — Memcached, SQL, DynamoDB — is just another Store, so callers
// swap brokers without changing how they call the limiter.
//
//	// single process:
//	lim, _ := bouncer.NewMemory(bouncer.Policy{Algorithm: bouncer.TokenBucket, Rate: 100, Burst: 20})
//	ok, _ := lim.Allow(ctx, "user:42")
//
//	// distributed (see subpackage redisstore):
//	store, _ := redisstore.New(client)
//	lim, _ := bouncer.New(store, bouncer.Policy{Algorithm: bouncer.TokenBucket, Rate: 100, Burst: 20})
//	ok, _ := lim.Allow(ctx, "user:42")
package bouncer

import (
	"context"
	"errors"
	"fmt"
)

// Algorithm names a rate limiting strategy. A Store decides which algorithms it
// supports; the built-in stores support both values below.
type Algorithm string

// Built-in algorithms.
const (
	TokenBucket Algorithm = "token_bucket"
	LeakyBucket Algorithm = "leaky_bucket"
)

// Errors returned by New and by Store implementations.
var (
	ErrNilStore             = errors.New("bouncer: store is nil")
	ErrInvalidRate          = errors.New("bouncer: rate must be > 0")
	ErrInvalidBurst         = errors.New("bouncer: burst must be >= 0")
	ErrUnsupportedAlgorithm = errors.New("bouncer: algorithm not supported by store")
)

// Policy is a broker-agnostic rate limit: the same Policy can be enforced by any
// Store. It carries no storage or connection concerns (those belong to the
// Store), so a policy translates directly between in-memory and distributed use.
type Policy struct {
	// Algorithm selects the strategy. Required; must be supported by the Store.
	Algorithm Algorithm
	// Rate is the steady-state events permitted per second, per key. Must be > 0.
	Rate float64
	// Burst is the maximum events permitted in an instantaneous burst, per key.
	// If zero, it defaults to ceil(Rate) so at least one event is admissible.
	Burst int
}

// validate returns a copy of p with defaults applied, or an error.
func (p Policy) validate() (Policy, error) {
	if p.Rate <= 0 {
		return Policy{}, fmt.Errorf("%w: got %v", ErrInvalidRate, p.Rate)
	}
	if p.Burst < 0 {
		return Policy{}, fmt.Errorf("%w: got %d", ErrInvalidBurst, p.Burst)
	}
	if p.Burst == 0 {
		p.Burst = ceil(p.Rate)
	}
	return p, nil
}

// Store holds rate limiter state behind a chosen backend ("broker"). It is the
// extension point: implement Store to support a new backend.
//
// Implementations must be safe for concurrent use. Each call applies the given
// Policy to key for n events and reports whether all n are admitted, consuming
// capacity only if so. The whole read-modify-write MUST be atomic per key — for
// distributed backends that means performing it server-side (e.g. a Redis Lua
// script), not as separate get/set round-trips, or concurrent callers will
// over-admit. n is always >= 1 (the Limiter handles the n == 0 and n < 0 cases).
type Store interface {
	Allow(ctx context.Context, key string, p Policy, n int) (bool, error)
}

// AlgorithmChecker is an optional Store interface. If a Store implements it, New
// validates the policy's algorithm up front so misconfiguration fails fast
// instead of on the first Allow call. The built-in stores implement it.
type AlgorithmChecker interface {
	Supports(Algorithm) bool
}

// Limiter enforces a Policy against a Store, per key. It is safe for concurrent
// use (its safety derives from the Store's).
type Limiter struct {
	store  Store
	policy Policy
}

// New builds a Limiter that enforces p using store. It validates p (applying the
// Burst default) and, if the store reports its supported algorithms, checks that
// it supports p.Algorithm.
func New(store Store, p Policy) (*Limiter, error) {
	if store == nil {
		return nil, ErrNilStore
	}
	vp, err := p.validate()
	if err != nil {
		return nil, err
	}
	if c, ok := store.(AlgorithmChecker); ok && !c.Supports(vp.Algorithm) {
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedAlgorithm, vp.Algorithm)
	}
	return &Limiter{store: store, policy: vp}, nil
}

// NewMemory is a convenience constructor for the common single-process case. It
// is shorthand for New(NewMemoryStore(opts...), p).
func NewMemory(p Policy, opts ...MemoryOption) (*Limiter, error) {
	return New(NewMemoryStore(opts...), p)
}

// Allow reports whether a single event for key may proceed right now, consuming
// capacity if so. Shorthand for AllowN(ctx, key, 1).
func (l *Limiter) Allow(ctx context.Context, key string) (bool, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN reports whether n events for key may proceed right now. If there is
// capacity for all n they are consumed atomically and it returns true; otherwise
// nothing is consumed and it returns false. AllowN(0) is always true and
// consumes nothing; a negative n returns false. A non-nil error indicates a
// backend failure; the accompanying bool reflects the Store's failure mode.
func (l *Limiter) AllowN(ctx context.Context, key string, n int) (bool, error) {
	if n == 0 {
		return true, nil
	}
	if n < 0 {
		return false, nil
	}
	return l.store.Allow(ctx, key, l.policy, n)
}

// ceil returns the smallest int >= f.
func ceil(f float64) int {
	i := int(f)
	if f > float64(i) {
		return i + 1
	}
	return i
}
