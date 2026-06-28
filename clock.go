package bouncer

import "time"

// Clock abstracts the source of time so that limiters can be tested
// deterministically. Production code uses the real wall clock; tests
// inject a controllable fake.
type Clock interface {
	Now() time.Time
}

// realClock reports the actual wall-clock time.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// SystemClock is the default Clock backed by time.Now.
var SystemClock Clock = realClock{}
