package bouncer_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/0xnikshi/bouncer"
)

// fakeClock is a manually-advanced Clock for deterministic tests.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.Unix(0, 0)} }

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
	store := bouncer.NewMemoryStore()
	tests := []struct {
		name  string
		store bouncer.Store
		p     bouncer.Policy
		want  error
	}{
		{"nil store", nil, bouncer.Policy{Algorithm: bouncer.TokenBucket, Rate: 1}, bouncer.ErrNilStore},
		{"zero rate", store, bouncer.Policy{Algorithm: bouncer.TokenBucket, Rate: 0}, bouncer.ErrInvalidRate},
		{"negative rate", store, bouncer.Policy{Algorithm: bouncer.TokenBucket, Rate: -1}, bouncer.ErrInvalidRate},
		{"negative burst", store, bouncer.Policy{Algorithm: bouncer.TokenBucket, Rate: 1, Burst: -1}, bouncer.ErrInvalidBurst},
		{"unsupported algorithm", store, bouncer.Policy{Algorithm: "nope", Rate: 1}, bouncer.ErrUnsupportedAlgorithm},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := bouncer.New(tt.store, tt.p)
			if !errors.Is(err, tt.want) {
				t.Fatalf("New() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestBurstDefault(t *testing.T) {
	// Fractional rate -> Burst defaults to ceil(Rate)=1, so one event passes.
	lim, err := bouncer.NewMemory(bouncer.Policy{Algorithm: bouncer.TokenBucket, Rate: 0.5})
	if err != nil {
		t.Fatalf("NewMemory() error = %v", err)
	}
	if ok, _ := lim.Allow(context.Background(), "k"); !ok {
		t.Fatal("expected first event allowed with default burst")
	}
}

func TestAllowNEdgeCases(t *testing.T) {
	ctx := context.Background()
	lim, _ := bouncer.NewMemory(bouncer.Policy{Algorithm: bouncer.TokenBucket, Rate: 10, Burst: 5})

	if ok, _ := lim.AllowN(ctx, "k", 0); !ok {
		t.Error("AllowN(0) should be true")
	}
	if ok, _ := lim.AllowN(ctx, "k", -1); ok {
		t.Error("AllowN(-1) should be false")
	}
	if ok, _ := lim.AllowN(ctx, "k", 6); ok {
		t.Error("AllowN(burst+1) should be false")
	}
}

// memoryContract exercises behavior both built-in algorithms must honor.
func memoryContract(t *testing.T, algo bouncer.Algorithm) {
	t.Helper()
	ctx := context.Background()
	clk := newFakeClock()
	lim, err := bouncer.NewMemory(
		bouncer.Policy{Algorithm: algo, Rate: 10, Burst: 5},
		bouncer.WithClock(clk),
	)
	if err != nil {
		t.Fatalf("NewMemory(%s) error = %v", algo, err)
	}

	// Burst of 5 from a fresh key, then denied.
	for i := 0; i < 5; i++ {
		if ok, _ := lim.Allow(ctx, "user"); !ok {
			t.Fatalf("%s: event %d should be allowed", algo, i)
		}
	}
	if ok, _ := lim.Allow(ctx, "user"); ok {
		t.Fatalf("%s: 6th event should be denied", algo)
	}

	// At 10/sec, 100ms restores capacity for exactly one event.
	clk.Advance(100 * time.Millisecond)
	if ok, _ := lim.Allow(ctx, "user"); !ok {
		t.Fatalf("%s: event should be allowed after 100ms", algo)
	}
	if ok, _ := lim.Allow(ctx, "user"); ok {
		t.Fatalf("%s: only one event of capacity should have recovered", algo)
	}
}

func TestMemoryContract(t *testing.T) {
	for _, algo := range []bouncer.Algorithm{bouncer.TokenBucket, bouncer.LeakyBucket} {
		t.Run(string(algo), func(t *testing.T) { memoryContract(t, algo) })
	}
}

func TestFixedWindow(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock() // starts at Unix(0,0)
	// Rate 10, Burst 5 -> window = 5/10 = 0.5s, limit 5 per window.
	lim, _ := bouncer.NewMemory(
		bouncer.Policy{Algorithm: bouncer.FixedWindow, Rate: 10, Burst: 5},
		bouncer.WithClock(clk),
	)

	// Fill the first window.
	for i := 0; i < 5; i++ {
		if ok, _ := lim.Allow(ctx, "u"); !ok {
			t.Fatalf("event %d should be allowed in the first window", i)
		}
	}
	if ok, _ := lim.Allow(ctx, "u"); ok {
		t.Fatal("6th event in the same window should be denied")
	}

	// Still the same window after 400ms (< 500ms): the count does not reset.
	clk.Advance(400 * time.Millisecond)
	if ok, _ := lim.Allow(ctx, "u"); ok {
		t.Fatal("still within the window, should be denied")
	}

	// Crossing into the next window at 500ms resets the count.
	clk.Advance(100 * time.Millisecond)
	if ok, _ := lim.Allow(ctx, "u"); !ok {
		t.Fatal("new window should allow again")
	}
}

func TestPerKeyIsolation(t *testing.T) {
	ctx := context.Background()
	lim, _ := bouncer.NewMemory(bouncer.Policy{Algorithm: bouncer.TokenBucket, Rate: 1, Burst: 2})

	lim.Allow(ctx, "alice")
	lim.Allow(ctx, "alice")
	if ok, _ := lim.Allow(ctx, "alice"); ok {
		t.Fatal("alice should be exhausted")
	}
	if ok, _ := lim.Allow(ctx, "bob"); !ok {
		t.Fatal("bob should be unaffected by alice")
	}
}

// TestConcurrentAccess runs under -race and asserts exactly Burst events are
// admitted for one key when time does not advance.
func TestConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	const burst = 100
	lim, _ := bouncer.NewMemory(
		bouncer.Policy{Algorithm: bouncer.TokenBucket, Rate: 1, Burst: burst},
		bouncer.WithClock(clk),
	)

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		allowed int
	)
	for i := 0; i < burst*4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ok, _ := lim.Allow(ctx, "user"); ok {
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
}
