package window

import "time"

// Window is a half-open event-time interval [Start, End).
type Window struct {
	Start time.Time
	End   time.Time
	Index int64 // sequence number, Start == Epoch + Index*Size
}

// Spec describes a tumbling window grid.
type Spec struct {
	Size  time.Duration
	Epoch time.Time // grid origin (defaults to unix epoch when zero)
}

// Assign returns the window containing event time ts.
func (s Spec) Assign(ts time.Time) Window {
	return Window{}
}

// Next returns the window immediately following w on the same grid.
func (s Spec) Next(w Window) Window {
	return Window{}
}

// Contains reports whether ts falls inside w.
func (w Window) Contains(ts time.Time) bool {
	return false
}
