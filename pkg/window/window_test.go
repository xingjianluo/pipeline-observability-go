package window

import (
	"testing"
	"time"
)

func TestSpecAssign(t *testing.T) {
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	spec := Spec{Size: time.Minute, Epoch: base}

	cases := []struct {
		offset time.Duration
		idx    int64
	}{
		{0, 0},
		{30 * time.Second, 0},
		{59*time.Second + 999999999, 0},
		{time.Minute, 1},
		{-time.Second, -1},
		{-time.Minute, -1},
	}
	for _, tc := range cases {
		ts := base.Add(tc.offset)
		w := spec.Assign(ts)
		if w.Index != tc.idx {
			t.Fatalf("Assign(%s) index=%d want %d", ts.Format(time.RFC3339Nano), w.Index, tc.idx)
		}
		if !w.Contains(ts) {
			t.Fatalf("window %v does not contain its source time %s", w, ts)
		}
		if w.End.Sub(w.Start) != time.Minute {
			t.Fatalf("window length = %s want 1m", w.End.Sub(w.Start))
		}
	}
	// Boundary: [start, end) half-open.
	w0 := spec.Assign(base)
	if w0.Contains(base.Add(time.Minute)) {
		t.Fatal("end instant must belong to the next window")
	}
	n := spec.Next(w0)
	if n.Index != 1 || !n.Start.Equal(w0.End) {
		t.Fatalf("Next mismatch: %+v", n)
	}
}

func TestSpecDefaultEpochAndValidation(t *testing.T) {
	spec := Spec{Size: time.Hour}
	w := spec.Assign(time.Unix(3600, 0))
	if w.Index != 1 {
		t.Fatalf("default epoch index=%d want 1", w.Index)
	}
	if (Spec{Size: 0}).Validate() == nil {
		t.Fatal("zero size must be invalid")
	}
}
