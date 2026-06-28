package bouncer

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeClock is a manually-advanced Clock for deterministic tests.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(0, 0)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func TestNewValidation(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want error
	}{
		{"unknown algorithm", Config{Algorithm: "nope", Rate: 1}, ErrUnknownAlgorithm},
		{"zero rate", Config{Algorithm: TokenBucket, Rate: 0}, ErrInvalidRate},
		{"negative rate", Config{Algorithm: TokenBucket, Rate: -1}, ErrInvalidRate},
		{"negative burst", Config{Algorithm: TokenBucket, Rate: 1, Burst: -1}, ErrInvalidBurst},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.cfg)
			if !errors.Is(err, tt.want) {
				t.Fatalf("New() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestNewBurstDefault(t *testing.T) {
	// Fractional rate should still yield a usable limiter (Burst defaults to
	// ceil(Rate) = 1), admitting one event up front.
	lim, err := New(Config{Algorithm: TokenBucket, Rate: 0.5})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if !lim.Allow() {
		t.Fatal("expected first event to be allowed with default burst")
	}
}

func TestAlgorithmsRegistered(t *testing.T) {
	got := Algorithms()
	want := map[Algorithm]bool{TokenBucket: true, LeakyBucket: true}
	for _, a := range got {
		delete(want, a)
	}
	if len(want) != 0 {
		t.Fatalf("missing built-in algorithms: %v (got %v)", want, got)
	}
}

func TestRegisterPanics(t *testing.T) {
	t.Run("empty name", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic on empty name")
			}
		}()
		Register("", func(Config, Clock) (Limiter, error) { return nil, nil })
	})
	t.Run("nil constructor", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic on nil constructor")
			}
		}()
		Register("x", nil)
	})
}

// commonLimiterContract exercises behavior every Limiter must honor regardless
// of algorithm.
func commonLimiterContract(t *testing.T, algo Algorithm) {
	t.Helper()
	clk := newFakeClock()
	lim, err := New(Config{Algorithm: algo, Rate: 10, Burst: 5}, WithClock(clk))
	if err != nil {
		t.Fatalf("New(%s) error = %v", algo, err)
	}

	if !lim.AllowN(0) {
		t.Error("AllowN(0) should always be true")
	}
	if lim.AllowN(-1) {
		t.Error("AllowN(-1) should be false")
	}
	// A request larger than capacity can never be admitted.
	if lim.AllowN(6) {
		t.Error("AllowN(burst+1) should be false")
	}
}

func TestCommonContract(t *testing.T) {
	for _, algo := range []Algorithm{TokenBucket, LeakyBucket} {
		t.Run(string(algo), func(t *testing.T) {
			commonLimiterContract(t, algo)
		})
	}
}

// TestConcurrentAccess ensures the limiters are race-free under -race. It does
// not assert an exact count, only that exactly Burst events are admitted from a
// full/empty bucket with no time advancing.
func TestConcurrentAccess(t *testing.T) {
	for _, algo := range []Algorithm{TokenBucket, LeakyBucket} {
		t.Run(string(algo), func(t *testing.T) {
			clk := newFakeClock()
			const burst = 100
			lim, err := New(Config{Algorithm: algo, Rate: 1, Burst: burst}, WithClock(clk))
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}

			var (
				wg      sync.WaitGroup
				mu      sync.Mutex
				allowed int
			)
			for i := 0; i < burst*4; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if lim.Allow() {
						mu.Lock()
						allowed++
						mu.Unlock()
					}
				}()
			}
			wg.Wait()

			if allowed != burst {
				t.Fatalf("admitted %d events, want %d", allowed, burst)
			}
		})
	}
}
