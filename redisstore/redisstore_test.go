package redisstore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/0xnikshi/bouncer"
	"github.com/0xnikshi/bouncer/redisstore"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newTestLimiter spins up an in-memory Redis (miniredis) and a Limiter backed by
// a redisstore. The returned miniredis lets tests control time and simulate
// outages.
func newTestLimiter(t *testing.T, p bouncer.Policy, opts ...redisstore.Option) (*bouncer.Limiter, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	mr.SetTime(time.Unix(1000, 0)) // freeze the clock for determinism

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	store, err := redisstore.New(client, opts...)
	if err != nil {
		t.Fatalf("redisstore.New() error = %v", err)
	}
	lim, err := bouncer.New(store, p)
	if err != nil {
		t.Fatalf("bouncer.New() error = %v", err)
	}
	return lim, mr
}

func TestNewNilClient(t *testing.T) {
	_, err := redisstore.New(nil)
	if !errors.Is(err, redisstore.ErrNilClient) {
		t.Fatalf("New(nil) error = %v, want ErrNilClient", err)
	}
}

func TestUnsupportedAlgorithmFailsFast(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	store, _ := redisstore.New(client)
	// New should reject an unsupported algorithm up front via Supports.
	_, err := bouncer.New(store, bouncer.Policy{Algorithm: "nope", Rate: 1})
	if !errors.Is(err, bouncer.ErrUnsupportedAlgorithm) {
		t.Fatalf("New() error = %v, want ErrUnsupportedAlgorithm", err)
	}
}

func TestTokenBucketBurstThenRefill(t *testing.T) {
	ctx := context.Background()
	lim, mr := newTestLimiter(t, bouncer.Policy{Algorithm: bouncer.TokenBucket, Rate: 10, Burst: 5}, redisstore.WithKeyPrefix("t:"))

	for i := 0; i < 5; i++ {
		if ok, err := lim.Allow(ctx, "user"); err != nil || !ok {
			t.Fatalf("event %d: ok=%v err=%v, want allowed", i, ok, err)
		}
	}
	if ok, _ := lim.Allow(ctx, "user"); ok {
		t.Fatal("6th event should be denied")
	}

	mr.SetTime(time.Unix(1000, 0).Add(100 * time.Millisecond)) // one token at 10/sec
	if ok, err := lim.Allow(ctx, "user"); err != nil || !ok {
		t.Fatalf("after refill: ok=%v err=%v, want allowed", ok, err)
	}
	if ok, _ := lim.Allow(ctx, "user"); ok {
		t.Fatal("only one token should have refilled")
	}
}

func TestLeakyBucketBurstThenLeak(t *testing.T) {
	ctx := context.Background()
	lim, mr := newTestLimiter(t, bouncer.Policy{Algorithm: bouncer.LeakyBucket, Rate: 10, Burst: 5}, redisstore.WithKeyPrefix("l:"))

	for i := 0; i < 5; i++ {
		if ok, err := lim.Allow(ctx, "user"); err != nil || !ok {
			t.Fatalf("event %d: ok=%v err=%v, want allowed", i, ok, err)
		}
	}
	if ok, _ := lim.Allow(ctx, "user"); ok {
		t.Fatal("6th event should overflow")
	}

	mr.SetTime(time.Unix(1000, 0).Add(100 * time.Millisecond)) // one unit leaks at 10/sec
	if ok, err := lim.Allow(ctx, "user"); err != nil || !ok {
		t.Fatalf("after leak: ok=%v err=%v, want allowed", ok, err)
	}
	if ok, _ := lim.Allow(ctx, "user"); ok {
		t.Fatal("only one unit should have leaked")
	}
}

func TestPerKeyIsolation(t *testing.T) {
	ctx := context.Background()
	lim, _ := newTestLimiter(t, bouncer.Policy{Algorithm: bouncer.TokenBucket, Rate: 1, Burst: 2}, redisstore.WithKeyPrefix("k:"))

	lim.Allow(ctx, "alice")
	lim.Allow(ctx, "alice")
	if ok, _ := lim.Allow(ctx, "alice"); ok {
		t.Fatal("alice should be exhausted")
	}
	if ok, err := lim.Allow(ctx, "bob"); err != nil || !ok {
		t.Fatalf("bob: ok=%v err=%v, want allowed", ok, err)
	}
}

func TestFailOpen(t *testing.T) {
	ctx := context.Background()
	lim, mr := newTestLimiter(t, bouncer.Policy{Algorithm: bouncer.TokenBucket, Rate: 1, Burst: 1})

	mr.Close() // simulate Redis outage

	ok, err := lim.Allow(ctx, "user")
	if err == nil {
		t.Fatal("expected an error when Redis is down")
	}
	if !ok {
		t.Fatal("fail-open: event should be allowed despite the error")
	}
}

func TestFailClosed(t *testing.T) {
	ctx := context.Background()
	lim, mr := newTestLimiter(t, bouncer.Policy{Algorithm: bouncer.TokenBucket, Rate: 1, Burst: 1}, redisstore.WithFailClosed(true))

	mr.Close() // simulate Redis outage

	ok, err := lim.Allow(ctx, "user")
	if err == nil {
		t.Fatal("expected an error when Redis is down")
	}
	if ok {
		t.Fatal("fail-closed: event should be denied on error")
	}
}
