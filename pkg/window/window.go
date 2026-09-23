package window

import "time"

// Window is a half-open event-time interval [Start, End).
type Window struct {
	Start time.Time
	End   time.Time
	Index int64 // sequence number: Start == Epoch + Index*Size
}

// Spec describes a tumbling window grid.
type Spec struct {
	Size  time.Duration
	Epoch time.Time // grid origin (zero value means the Unix epoch)
}

var unixEpoch = time.Unix(0, 0)

// Validate checks the grid configuration.
func (s Spec) Validate() error {
	if s.Size <= 0 {
		return ErrInvalidSize
	}
	return nil
}

// epoch returns the effective grid origin.
func (s Spec) epoch() time.Time {
	if s.Epoch.IsZero() {
		return unixEpoch
	}
	return s.Epoch
}

// Assign returns the window containing event time ts.
func (s Spec) Assign(ts time.Time) Window {
	if err := s.Validate(); err != nil {
		panic(err)
	}
	epoch := s.epoch()
	idx := floorDiv(ts.Sub(epoch).Nanoseconds(), int64(s.Size))
	start := epoch.Add(time.Duration(idx) * s.Size)
	return Window{Start: start, End: start.Add(s.Size), Index: idx}
}

// Next returns the window immediately following w on the same grid.
func (s Spec) Next(w Window) Window {
	if err := s.Validate(); err != nil {
		panic(err)
	}
	return Window{
		Start: w.Start.Add(s.Size),
		End:   w.End.Add(s.Size),
		Index: w.Index + 1,
	}
}

// Contains reports whether ts falls inside the half-open window w.
func (w Window) Contains(ts time.Time) bool {
	return !ts.Before(w.Start) && ts.Before(w.End)
}

// floorDiv divides toward negative infinity (unlike Go's truncated integer
// division), which the event-time grid needs for times before the epoch.
func floorDiv(a, b int64) int64 {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}
