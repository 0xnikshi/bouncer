// Command demo is a tiny runnable program that shows the in-memory limiter.
// Run it with:
//
//	go run ./cmd/demo
package main

import (
	"context"
	"fmt"

	"github.com/0xnikshi/bouncer"
)

func main() {
	ctx := context.Background()

	// Memory-backed token bucket: 2 events/sec sustained, burst up to 5, per key.
	lim, err := bouncer.NewMemory(bouncer.Policy{
		Algorithm: bouncer.TokenBucket,
		Rate:      2,
		Burst:     5,
	})
	if err != nil {
		panic(err)
	}

	const key = "user:42"
	fmt.Printf("Firing 8 requests for %q at a 5-burst limiter:\n", key)
	for i := 1; i <= 8; i++ {
		ok, _ := lim.Allow(ctx, key)
		if ok {
			fmt.Printf("  request %d: ALLOWED\n", i)
		} else {
			fmt.Printf("  request %d: denied (bucket empty)\n", i)
		}
	}
}
