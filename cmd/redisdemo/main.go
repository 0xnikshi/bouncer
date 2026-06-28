// Command redisdemo shows the Redis-backed distributed limiter.
//
// It needs a running Redis. The address defaults to localhost:6379 and can be
// overridden with the REDIS_ADDR environment variable:
//
//	# start a throwaway Redis with Docker, then:
//	docker run --rm -p 6379:6379 redis:7
//	go run ./cmd/redisdemo
//
// Because the bucket state lives in Redis, running this program twice
// concurrently shares one limit across both processes — that is the whole point
// of the distributed store.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/0xnikshi/bouncer"
	"github.com/0xnikshi/bouncer/redisstore"
	"github.com/redis/go-redis/v9"
)

func main() {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()

	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		fmt.Printf("cannot reach Redis at %s: %v\n", addr, err)
		fmt.Println("start one with:  docker run --rm -p 6379:6379 redis:7")
		os.Exit(1)
	}

	// Same Policy as the in-memory demo — only the store (broker) differs.
	store, err := redisstore.New(client, redisstore.WithKeyPrefix("demo:"))
	if err != nil {
		panic(err)
	}
	lim, err := bouncer.New(store, bouncer.Policy{
		Algorithm: bouncer.TokenBucket,
		Rate:      2,
		Burst:     5,
	})
	if err != nil {
		panic(err)
	}

	const key = "user:42"
	fmt.Printf("Firing 8 requests for %q at a 5-burst limiter (state in Redis at %s):\n", key, addr)
	for i := 1; i <= 8; i++ {
		ok, err := lim.Allow(ctx, key)
		switch {
		case err != nil:
			fmt.Printf("  request %d: ERROR %v\n", i, err)
		case ok:
			fmt.Printf("  request %d: ALLOWED\n", i)
		default:
			fmt.Printf("  request %d: denied\n", i)
		}
	}
	fmt.Println("\nRun this program again within a second — the limit carries over,")
	fmt.Println("because the bucket lives in Redis, not in this process.")
}
