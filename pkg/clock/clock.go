package clock

import "time"

// Clock abstracts wall-clock time so tests are deterministic.
type Clock interface {
	Now() time.Time
}

// RealClock uses the host wall clock.
type RealClock struct{}

// FixedClock always returns the same instant (used by tests / replay).
type FixedClock struct {
	T time.Time
}

func (RealClock) Now() time.Time { return time.Now() }

func (c *FixedClock) Now() time.Time { return c.T }
