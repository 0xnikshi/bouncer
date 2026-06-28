package bouncer

import (
	"testing"
	"time"
)

func TestLeakyBucketFillThenLeak(t *testing.T) {
	clk := newFakeClock()
	lim, err := New(Config{Algorithm: LeakyBucket, Rate: 10, Burst: 5}, WithClock(clk))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Bucket starts empty: it can absorb a burst up to capacity, then overflows.
	for i := 0; i < 5; i++ {
		if !lim.Allow() {
			t.Fatalf("event %d should fit in an empty bucket", i)
		}
	}
	if lim.Allow() {
		t.Fatal("6th event should overflow the full bucket")
	}

	// At 10/sec the bucket drains one unit per 100ms, making room for one event.
	clk.Advance(100 * time.Millisecond)
	if !lim.Allow() {
		t.Fatal("event should fit after one unit leaked")
	}
	if lim.Allow() {
		t.Fatal("only one unit should have leaked")
	}
}

func TestLeakyBucketDrainCapsAtEmpty(t *testing.T) {
	clk := newFakeClock()
	lim, err := New(Config{Algorithm: LeakyBucket, Rate: 10, Burst: 5}, WithClock(clk))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Fill, then idle far longer than needed to drain. Level must clamp at 0,
	// so afterward exactly Burst events fit again (no "negative" credit).
	for i := 0; i < 5; i++ {
		lim.Allow()
	}
	clk.Advance(time.Hour)

	allowed := 0
	for i := 0; i < 100; i++ {
		if lim.Allow() {
			allowed++
		}
	}
	if allowed != 5 {
		t.Fatalf("after long idle, admitted %d events, want capacity 5", allowed)
	}
}

func TestLeakyBucketSteadyRate(t *testing.T) {
	clk := newFakeClock()
	lim, err := New(Config{Algorithm: LeakyBucket, Rate: 5, Burst: 1}, WithClock(clk))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// With Burst=1 the bucket admits at most one event per drain interval
	// (1/5s = 200ms), enforcing a steady output rate.
	if !lim.Allow() {
		t.Fatal("first event should be allowed")
	}
	if lim.Allow() {
		t.Fatal("second immediate event should be denied")
	}
	clk.Advance(200 * time.Millisecond)
	if !lim.Allow() {
		t.Fatal("event should be allowed after a full drain interval")
	}
}
