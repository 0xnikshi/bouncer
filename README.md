# bouncer

A small, pluggable rate limiter for Go.

Algorithms are selected by configuration, not by import, so you can swap
strategies without reshaping your code. Two ship in the box — **token bucket**
and **leaky bucket** — and you can register your own.

## Install

```sh
go get github.com/0xnikshi/bouncer
```

## Usage

```go
lim, err := bouncer.New(bouncer.Config{
    Algorithm: bouncer.TokenBucket,
    Rate:      100, // sustained events per second
    Burst:     10,  // max events in an instantaneous burst
})
if err != nil {
    log.Fatal(err)
}

if lim.Allow() {
    // handle the request
}
```

Every limiter satisfies the `Limiter` interface and is safe for concurrent use:

```go
type Limiter interface {
    Allow() bool        // permit one event, consuming capacity
    AllowN(n int) bool  // permit n events atomically (all or nothing)
}
```

## Algorithms

| Algorithm             | Behavior                                                                  |
| --------------------- | ------------------------------------------------------------------------- |
| `bouncer.TokenBucket` | Tokens accrue at `Rate` up to `Burst`. Allows bursts, caps the average.    |
| `bouncer.LeakyBucket` | A queue of depth `Burst` draining at `Rate`. Smooths toward steady output. |

Both bound the long-run rate to `Rate`. The difference is shape: the token
bucket lets a full bucket fire all at once, while the leaky bucket meters events
toward a constant outflow.

## Adding an algorithm

Implement `Limiter`, then register a constructor under a name. Once registered,
the algorithm is selectable through `Config.Algorithm` like any built-in:

```go
const SlidingWindow bouncer.Algorithm = "sliding_window"

func init() {
    bouncer.Register(SlidingWindow, func(cfg bouncer.Config, clk bouncer.Clock) (bouncer.Limiter, error) {
        return newSlidingWindow(cfg, clk), nil
    })
}
```

The injected `bouncer.Clock` is the time source — use it instead of `time.Now`
so your limiter stays unit-testable.

## Development

```sh
go test -race ./...
```
