// Package redisstore is a Redis-backed bouncer.Store for distributed rate
// limiting.
//
// State lives in Redis and is shared by every process pointed at the same
// server, so a policy like "100/sec" holds across a whole fleet rather than per
// instance. The decision runs atomically inside Redis via a Lua script, so
// concurrent callers cannot over-admit, and time comes from the Redis server so
// clock skew between app instances is a non-issue.
//
// Plug it into a Limiter like any other store:
//
//	store, _ := redisstore.New(client, redisstore.WithKeyPrefix("rl:"))
//	lim, _ := bouncer.New(store, bouncer.Policy{Algorithm: bouncer.TokenBucket, Rate: 100, Burst: 20})
//	ok, _ := lim.Allow(ctx, "user:42")
//
// This package depends on github.com/redis/go-redis/v9. The core bouncer package
// stays dependency-free; import this only when you need distributed limiting.
package redisstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/0xnikshi/bouncer"
	"github.com/redis/go-redis/v9"
)

// ErrNilClient is returned by New when the Redis client is nil.
var ErrNilClient = errors.New("redisstore: redis client is nil")

// Store is a Redis-backed bouncer.Store.
type Store struct {
	client     redis.UniversalClient
	keyPrefix  string
	failClosed bool
}

// scripts maps each supported algorithm to its Lua implementation.
var scripts = map[bouncer.Algorithm]*redis.Script{
	bouncer.TokenBucket: redis.NewScript(tokenBucketScript),
	bouncer.LeakyBucket: redis.NewScript(leakyBucketScript),
}

// Option customizes a Store.
type Option func(*Store)

// WithKeyPrefix namespaces every key this store writes to Redis (e.g. "rl:login:"),
// so multiple limiters can share one Redis without colliding.
func WithKeyPrefix(prefix string) Option {
	return func(s *Store) { s.keyPrefix = prefix }
}

// WithFailClosed sets the behavior when Redis is unreachable. By default the
// store fails open (Allow returns true plus the error) so a Redis outage does
// not take down the application. Pass true to fail closed (deny on error),
// protecting a fragile downstream during an outage.
func WithFailClosed(failClosed bool) Option {
	return func(s *Store) { s.failClosed = failClosed }
}

// New builds a Redis-backed store. client is a connected go-redis client
// (*redis.Client or *redis.ClusterClient — both satisfy redis.UniversalClient).
// New does not contact Redis; the connection is exercised lazily on first use.
func New(client redis.UniversalClient, opts ...Option) (*Store, error) {
	if client == nil {
		return nil, ErrNilClient
	}
	s := &Store{client: client}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// Supports reports the algorithms this store implements.
func (s *Store) Supports(a bouncer.Algorithm) bool {
	_, ok := scripts[a]
	return ok
}

// Allow applies p to key for n events atomically in Redis. See bouncer.Store for
// the contract. On a Redis failure it returns the configured failure-mode bool
// plus a non-nil error; an unsupported algorithm is a configuration error and is
// returned regardless of failure mode.
func (s *Store) Allow(ctx context.Context, key string, p bouncer.Policy, n int) (bool, error) {
	script, ok := scripts[p.Algorithm]
	if !ok {
		return false, fmt.Errorf("%w: %q", bouncer.ErrUnsupportedAlgorithm, p.Algorithm)
	}

	res, err := script.Run(ctx, s.client,
		[]string{s.keyPrefix + key},
		p.Rate, p.Burst, n,
	).Int64()
	if err != nil {
		return !s.failClosed, fmt.Errorf("redisstore: redis call failed: %w", err)
	}
	return res == 1, nil
}
