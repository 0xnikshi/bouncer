package bouncer_test

import (
	"fmt"

	"github.com/0xnikshi/bouncer"
)

// ExampleNew shows the common path: build a limiter by configuration and gate
// events with Allow.
func ExampleNew() {
	lim, err := bouncer.New(bouncer.Config{
		Algorithm: bouncer.TokenBucket,
		Rate:      100, // 100 events/sec sustained
		Burst:     10,  // up to 10 in an instantaneous burst
	})
	if err != nil {
		panic(err)
	}

	if lim.Allow() {
		fmt.Println("request permitted")
	}
	// Output: request permitted
}

// alwaysDeny is a trivial third-party algorithm demonstrating the plugin seam.
type alwaysDeny struct{}

func (alwaysDeny) Allow() bool       { return false }
func (alwaysDeny) AllowN(n int) bool { return false }

// ExampleRegister shows how to plug in a custom algorithm. Once registered, it
// is selectable through Config.Algorithm just like the built-ins.
func ExampleRegister() {
	const Closed bouncer.Algorithm = "always_deny"

	bouncer.Register(Closed, func(bouncer.Config, bouncer.Clock) (bouncer.Limiter, error) {
		return alwaysDeny{}, nil
	})

	lim, err := bouncer.New(bouncer.Config{Algorithm: Closed, Rate: 1})
	if err != nil {
		panic(err)
	}

	fmt.Println(lim.Allow())
	// Output: false
}
