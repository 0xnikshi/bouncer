package bouncer_test

import (
	"context"
	"fmt"

	"github.com/0xnikshi/bouncer"
)

// ExampleNewMemory shows the common single-process path: a memory-backed limiter
// gating events per key.
func ExampleNewMemory() {
	lim, err := bouncer.NewMemory(bouncer.Policy{
		Algorithm: bouncer.TokenBucket,
		Rate:      100, // 100 events/sec sustained, per key
		Burst:     10,  // up to 10 in an instantaneous burst
	})
	if err != nil {
		panic(err)
	}

	ok, _ := lim.Allow(context.Background(), "user:42")
	if ok {
		fmt.Println("request permitted")
	}
	// Output: request permitted
}

// closedStore is a trivial third-party Store demonstrating the broker plugin
// seam: implement Store and any backend works behind the same Limiter.
type closedStore struct{}

func (closedStore) Allow(context.Context, string, bouncer.Policy, int) (bool, error) {
	return false, nil
}

// ExampleNew shows plugging in a custom Store. Once it satisfies bouncer.Store,
// it drives a Limiter exactly like the built-in stores.
func ExampleNew() {
	lim, err := bouncer.New(closedStore{}, bouncer.Policy{
		Algorithm: bouncer.TokenBucket,
		Rate:      1,
	})
	if err != nil {
		panic(err)
	}

	ok, _ := lim.Allow(context.Background(), "anything")
	fmt.Println(ok)
	// Output: false
}
