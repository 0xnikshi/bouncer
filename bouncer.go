// Package bouncer is a pluggable rate limiter.
//
// A rate limiting algorithm is exposed through the Limiter interface. Concrete
// algorithms register themselves under a name (an Algorithm) and are built via
// New, so callers select an algorithm by configuration rather than by importing
// a specific type. Two algorithms ship in the box: token bucket and leaky
// bucket. Additional algorithms can be plugged in with Register.
package bouncer

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Limiter decides whether events are permitted to proceed under a rate policy.
//
// Implementations must be safe for concurrent use.
type Limiter interface {
	// Allow reports whether a single event may proceed right now, consuming
	// capacity if so. It is shorthand for AllowN(1).
	Allow() bool

	// AllowN reports whether n events may proceed right now. If the limiter
	// has capacity for all n events they are consumed atomically and AllowN
	// returns true; otherwise nothing is consumed and it returns false.
	// AllowN(0) is always true and consumes nothing. A negative n returns
	// false.
	AllowN(n int) bool
}

// Algorithm names a registered rate limiting strategy.
type Algorithm string

// Built-in algorithms.
const (
	TokenBucket Algorithm = "token_bucket"
	LeakyBucket Algorithm = "leaky_bucket"
)

// Config describes a rate limiter to build. The same fields apply across
// algorithms so callers can swap strategies without reshaping their config.
type Config struct {
	// Algorithm selects which registered strategy to build. Required.
	Algorithm Algorithm

	// Rate is the steady-state number of events permitted per second.
	// Must be > 0.
	Rate float64

	// Burst is the maximum number of events that may be permitted in an
	// instantaneous burst. For the token bucket this is the bucket size; for
	// the leaky bucket it is the queue depth. If zero, it defaults to
	// ceil(Rate) so the limiter can always admit at least one event.
	Burst int
}

// Errors returned by New and by Constructors.
var (
	ErrUnknownAlgorithm = errors.New("bouncer: unknown algorithm")
	ErrInvalidRate      = errors.New("bouncer: rate must be > 0")
	ErrInvalidBurst     = errors.New("bouncer: burst must be >= 0")
)

// Constructor builds a Limiter from a validated Config using the given Clock.
// Algorithm implementations supply a Constructor to Register.
type Constructor func(cfg Config, clk Clock) (Limiter, error)

var (
	registryMu sync.RWMutex
	registry   = map[Algorithm]Constructor{}
)

// Register makes a rate limiting algorithm available to New under the given
// name. It panics if name is empty or if c is nil, and overwrites any existing
// registration for the same name. Register is intended to be called from an
// init function, typically in the package that implements the algorithm.
func Register(name Algorithm, c Constructor) {
	if name == "" {
		panic("bouncer: Register called with empty algorithm name")
	}
	if c == nil {
		panic("bouncer: Register called with nil constructor")
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	registry[name] = c
}

// Algorithms returns the names of all registered algorithms, sorted.
func Algorithms() []Algorithm {
	registryMu.RLock()
	defer registryMu.RUnlock()
	names := make([]Algorithm, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool { return names[i] < names[j] })
	return names
}

// Option customizes limiter construction.
type Option func(*options)

type options struct {
	clock Clock
}

// WithClock overrides the time source used by the limiter. It exists primarily
// for deterministic testing; production callers can rely on the default
// SystemClock.
func WithClock(clk Clock) Option {
	return func(o *options) {
		if clk != nil {
			o.clock = clk
		}
	}
}

// New builds a Limiter for the algorithm named in cfg.
//
// It validates cfg, applies defaults (see Config.Burst), and dispatches to the
// registered Constructor. It returns ErrUnknownAlgorithm if the algorithm is
// not registered, or a validation error if cfg is malformed.
func New(cfg Config, opts ...Option) (Limiter, error) {
	o := options{clock: SystemClock}
	for _, opt := range opts {
		opt(&o)
	}

	if cfg.Rate <= 0 {
		return nil, fmt.Errorf("%w: got %v", ErrInvalidRate, cfg.Rate)
	}
	if cfg.Burst < 0 {
		return nil, fmt.Errorf("%w: got %d", ErrInvalidBurst, cfg.Burst)
	}
	if cfg.Burst == 0 {
		// Round up so a limiter is always able to admit at least one event,
		// even for fractional rates (e.g. Rate=0.5 -> Burst=1).
		cfg.Burst = int(ceil(cfg.Rate))
	}

	registryMu.RLock()
	c, ok := registry[cfg.Algorithm]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownAlgorithm, cfg.Algorithm)
	}

	return c(cfg, o.clock)
}

// ceil returns the smallest integer >= f without importing math for a single
// call site (and avoids surprises for already-integral values).
func ceil(f float64) float64 {
	i := float64(int64(f))
	if f > i {
		return i + 1
	}
	return i
}

// perEventInterval returns the time it takes to accrue one event of capacity at
// the given rate. Shared by the built-in algorithms.
func perEventInterval(ratePerSec float64) time.Duration {
	return time.Duration(float64(time.Second) / ratePerSec)
}
