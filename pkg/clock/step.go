package clock

import (
	"sync"
	"time"
)

// StepClock returns T plus Step for every Now() call and advances T, so a
// serial event stream gets monotonically increasing receipt times. Safe for
// concurrent use (callers that need deterministic per-event receipt ordering
// should serialize ingestion anyway).
type StepClock struct {
	mu   sync.Mutex
	T    time.Time
	Step time.Duration
}

// Now returns the current time and advances by Step afterward.
func (c *StepClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.T
	c.T = c.T.Add(c.Step)
	return t
}
