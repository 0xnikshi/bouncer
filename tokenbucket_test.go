package bouncer

import (
	"testing"
	"time"
)

func TestTokenBucketBurstThenRefill(t *testing.T) {
	clk := newFakeClock()
	lim, err := New(Config{Algorithm: TokenBucket, Rate: 10, Burst: 5}, WithClock(clk))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Bucket starts full: the first 5 events burst through, the 6th is denied.
	for i := 0; i < 5; i++ {
		if !lim.Allow() {
			t.Fatalf("event %d should be allowed from a full bucket", i)
		}
	}
	if lim.Allow() {
		t.Fatal("6th event should be denied: bucket is empty")
	}

	// At 10 tokens/sec, 100ms accrues exactly one token.
	clk.Advance(100 * time.Millisecond)
	if !lim.Allow() {
		t.Fatal("event should be allowed after one token refilled")
	}
	if lim.Allow() {
		t.Fatal("only one token should have refilled")
	}
}

func TestTokenBucketRefillCapsAtCapacity(t *testing.T) {
	clk := newFakeClock()
	lim, err := New(Config{Algorithm: TokenBucket, Rate: 10, Burst: 5}, WithClock(clk))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Drain, then idle far longer than it takes to refill. Tokens must not
	// exceed capacity, so only Burst events should burst through afterward.
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

func TestTokenBucketAllowN(t *testing.T) {
	clk := newFakeClock()
	lim, err := New(Config{Algorithm: TokenBucket, Rate: 10, Burst: 5}, WithClock(clk))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if !lim.AllowN(3) {
		t.Fatal("AllowN(3) from full bucket should succeed")
	}
	if !lim.AllowN(2) {
		t.Fatal("AllowN(2) should succeed: 2 tokens remain")
	}
	if lim.AllowN(1) {
		t.Fatal("AllowN(1) should fail: bucket is empty")
	}
}
