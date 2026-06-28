# bouncer

A small, pluggable rate limiter for Go.

A **policy** (algorithm + rate + burst) is enforced by a `Limiter`, but the state
behind that policy lives in a pluggable **`Store`** — the *broker*. Swap the
broker and the same limiter goes from single-process to fleet-wide without
changing how you call it. Two algorithms ship in the box — **token bucket** and
**leaky bucket** — and any backend (in-memory, Redis, and beyond) is just a
`Store`.

## Install

```sh
go get github.com/0xnikshi/bouncer
```

## Usage (single process)

```go
lim, err := bouncer.NewMemory(bouncer.Policy{
    Algorithm: bouncer.TokenBucket,
    Rate:      100, // sustained events per second, per key
    Burst:     10,  // max events in an instantaneous burst, per key
})
if err != nil {
    log.Fatal(err)
}

ok, _ := lim.Allow(ctx, "user:42")
if !ok {
    http.Error(w, "rate limited", http.StatusTooManyRequests)
    return
}
```

`NewMemory` is a convenience for `New(NewMemoryStore(), policy)`. Limits are
applied **per key**, so `"user:42"` and `"user:99"` get independent buckets.

## The model

```go
// A Policy is broker-agnostic — algorithm, rate, burst. No storage concerns.
type Policy struct {
    Algorithm Algorithm
    Rate      float64
    Burst     int
}

// A Store is where bucket state lives. Implement it to add a backend.
type Store interface {
    Allow(ctx context.Context, key string, p Policy, n int) (bool, error)
}

// A Limiter enforces a Policy against a Store.
func New(store Store, p Policy) (*Limiter, error)

func (l *Limiter) Allow(ctx context.Context, key string) (bool, error)
func (l *Limiter) AllowN(ctx context.Context, key string, n int) (bool, error)
```

The call site is identical regardless of broker — only the `Store` you pass to
`New` changes.

## Algorithms

| Algorithm             | Behavior                                                                  |
| --------------------- | ------------------------------------------------------------------------- |
| `bouncer.TokenBucket` | Tokens accrue at `Rate` up to `Burst`. Allows bursts, caps the average.    |
| `bouncer.LeakyBucket` | A queue of depth `Burst` draining at `Rate`. Smooths toward steady output. |

Both bound the long-run rate to `Rate`. The difference is shape: the token
bucket lets a full bucket fire all at once, while the leaky bucket meters events
toward a constant outflow.

## Distributed limiting with Redis (production)

The in-memory store keeps state in one process — run several copies of your app
and each gets its own bucket, so a "100/sec" limit silently becomes
"100/sec × instances". For a fleet, use the `redisstore` broker: the bucket
lives in Redis and is shared by every instance, and the decision runs atomically
inside Redis via a Lua script so concurrent callers can't over-admit.

```go
import (
    "github.com/0xnikshi/bouncer"
    "github.com/0xnikshi/bouncer/redisstore"
    "github.com/redis/go-redis/v9"
)

client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

store, err := redisstore.New(client, redisstore.WithKeyPrefix("ratelimit:login:"))
if err != nil {
    log.Fatal(err)
}

// Same Policy, same Limiter API — only the store changed.
lim, err := bouncer.New(store, bouncer.Policy{
    Algorithm: bouncer.TokenBucket,
    Rate:      100,
    Burst:     20,
})
if err != nil {
    log.Fatal(err)
}

ok, err := lim.Allow(ctx, "user:42")
if err != nil {
    log.Printf("rate limiter degraded: %v", err) // fail-open: ok is true here
}
if !ok {
    http.Error(w, "rate limited", http.StatusTooManyRequests)
    return
}
```

Notes:

- **Failure mode** — on a Redis outage the store **fails open** by default
  (allows the event, returns the error so you can alert). Pass
  `redisstore.WithFailClosed(true)` to deny during an outage instead.
- **Clock skew is a non-issue** — bucket time comes from the Redis server, not
  each app instance, and idle keys auto-expire via a TTL.
- **Dependency isolation** — the core `bouncer` package has no third-party
  dependencies; go-redis is pulled in only if you import `redisstore`.

## Adding a broker

Any backend — Memcached, SQL, DynamoDB — is just a `Store`. Implement one method:

```go
type Store interface {
    Allow(ctx context.Context, key string, p Policy, n int) (bool, error)
}
```

The one rule: the whole read-modify-write **must be atomic per key**. In-memory
uses a mutex; Redis runs the logic server-side in a Lua script; SQL would use a
transaction. A naive get-then-set store races under concurrency and over-admits.
Optionally implement `Supports(Algorithm) bool` so `New` rejects unsupported
algorithms up front.

## Running the demos

```sh
go run ./cmd/demo        # in-memory store, no setup needed

# distributed store — needs Redis:
docker run --rm -p 6379:6379 redis:7
go run ./cmd/redisdemo
```

## Development

```sh
go test -race ./...
```

## Known limitations

- `MemoryStore` keeps one bucket per key for the life of the process; with very
  high-cardinality keys the map grows unbounded. For such workloads use
  `redisstore` (keys carry a TTL) — or open an issue and an eviction option can
  be added.
