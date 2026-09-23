package ingest

import (
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/pipeline-observability-go/internal/testhelp"
	"github.com/xingjianluo/pipeline-observability-go/pkg/clock"
	"github.com/xingjianluo/pipeline-observability-go/pkg/model"
)

func newTestIngestor(t *testing.T, c clock.Clock, allowed time.Duration) *Ingestor {
	t.Helper()
	return newTestIngestorAt(t, c, allowed, time.Time{})
}

func newTestIngestorAt(t *testing.T, c clock.Clock, allowed time.Duration, epoch time.Time) *Ingestor {
	t.Helper()
	g, err := New(c, Config{WindowSize: time.Minute, AllowedLateness: allowed, WindowEpoch: epoch})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return g
}

// At 10:00:30 a watermark advance to 10:02:00 closes window 0 (end 10:01:00,
// cutoff with 30s lateness = 10:01:30) but not window 1 (cutoff 10:02:30).
func TestOutOfOrderWithinLatenessIsAccepted(t *testing.T) {
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	clk := &clock.FixedClock{T: base.Add(10 * time.Second)}
	g := newTestIngestor(t, clk, 30*time.Second)
	key := model.StreamKey{JobID: "job1", Dataset: "d1"}

	evs := []model.RunEvent{
		testhelp.Ev("e1", "job1", "d1", base.Add(10*time.Second), base.Add(10*time.Second), 100),
		// later arrival, older event time, still inside window 0
		testhelp.Ev("e2", "job1", "d1", base.Add(5*time.Second), base.Add(5*time.Second), 50),
		// push watermark to 10:02:00 -> window 0 confirms
		testhelp.Ev("e3", "job1", "d1", base.Add(2*time.Minute), base.Add(2*time.Minute), 0),
	}
	var rs []model.IngestResult
	for _, e := range evs {
		rs = append(rs, g.Ingest(e))
	}
	if rs[0].Status != model.StatusAccepted || rs[1].Status != model.StatusAccepted {
		testhelp.Fatalf(t, evs, "out-of-order events must be ACCEPTED:\n%s", testhelp.DumpResults(rs))
	}
	if n := len(rs[2].Confirmed); n != 1 {
		testhelp.Fatalf(t, evs, "third ingest should confirm exactly window 0, got %d:\n%s", n, testhelp.DumpResults(rs))
	}
	out := rs[2].Confirmed[0]
	if out.Summary.EventCount != 2 || out.Summary.Records != 150 {
		testhelp.Fatalf(t, evs, "window 0 must contain e1,e2: %s", out.Summary.String())
	}
	if got := g.Watermark(key); !got.Equal(base.Add(2 * time.Minute)) {
		t.Fatalf("watermark=%s want 10:02", got)
	}
}

// After a window is confirmed, a same-window event past the lateness cutoff is
// rejected with a distinguishable reason; no silent drop, no mutation.
func TestLateBeyondLatenessRejected(t *testing.T) {
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	clk := &clock.FixedClock{T: base.Add(10 * time.Second)}
	g := newTestIngestor(t, clk, 30*time.Second)

	evs := []model.RunEvent{
		testhelp.Ev("e1", "job1", "d1", base.Add(10*time.Second), base.Add(10*time.Second), 100),
		// watermark 10:03:00 closes windows 0 and 1
		testhelp.Ev("wm", "job1", "d1", base.Add(3*time.Minute), base.Add(3*time.Minute), 0),
		// window 0 cutoff was 10:01:30 -> reject
		testhelp.Ev("late0", "job1", "d1", base.Add(20*time.Second), base.Add(20*time.Second), 7),
		// window 1 cutoff was 10:02:30 -> reject
		testhelp.Ev("late1", "job1", "d1", base.Add(65*time.Second), base.Add(65*time.Second), 7),
	}
	var rs []model.IngestResult
	for _, e := range evs {
		rs = append(rs, g.Ingest(e))
	}
	for i := 2; i < 4; i++ {
		if rs[i].Status != model.StatusRejectedLate || rs[i].Reason == "" {
			testhelp.Fatalf(t, evs, "event %s must be REJECTED_LATE with reason:\n%s",
				evs[i].EventID, testhelp.DumpResults(rs))
		}
	}
	// Confirmed window 0 stays exactly as first decided.
	out := rs[1].Confirmed[0]
	if out.Summary.EventCount != 1 || out.Summary.Records != 100 {
		testhelp.Fatalf(t, evs, "confirmed window must not change: %s", out.Summary.String())
	}
}

// Zero allowed lateness: window closes the moment watermark passes its end.
func TestZeroLatenessBoundary(t *testing.T) {
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	clk := &clock.FixedClock{T: base}
	g := newTestIngestor(t, clk, 0)
	r1 := g.Ingest(testhelp.Ev("a", "j", "d", base, base, 1))
	if len(r1.Confirmed) != 0 {
		t.Fatalf("event at window start must not confirm it immediately: %s", r1)
	}
	r2 := g.Ingest(testhelp.Ev("b", "j", "d", base.Add(time.Minute), base.Add(time.Minute), 1))
	if len(r2.Confirmed) != 1 {
		t.Fatalf("watermark == window end must close window 0 with zero lateness: %s", r2)
	}
	r3 := g.Ingest(testhelp.Ev("late", "j", "d", base.Add(30*time.Second), base.Add(30*time.Second), 1))
	if r3.Status != model.StatusRejectedLate {
		t.Fatalf("same-window event after close must be rejected, got %s", r3)
	}
}

func TestInvalidEventRejectedWithReason(t *testing.T) {
	clk := &clock.FixedClock{T: time.Now()}
	g := newTestIngestor(t, clk, time.Minute)
	r := g.Ingest(model.RunEvent{JobID: "j"}) // missing dataset, id, time
	if r.Status != model.StatusRejectedInvalid || r.Reason == "" {
		t.Fatalf("invalid event must be REJECTED_INVALID with reason, got %s", r)
	}
}

func TestDuplicateDoesNotDoubleCount(t *testing.T) {
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	clk := &clock.FixedClock{T: base.Add(time.Minute)}
	g := newTestIngestor(t, clk, 0)

	evs := []model.RunEvent{
		testhelp.Ev("dup1", "j", "d", base, base, 100),
		testhelp.Ev("dup1", "j", "d", base, base, 100), // exact redelivery
		testhelp.Ev("close", "j", "d", base.Add(time.Minute), base.Add(time.Minute), 0),
	}
	var rs []model.IngestResult
	for _, e := range evs {
		rs = append(rs, g.Ingest(e))
	}
	if rs[1].Status != model.StatusDuplicate {
		testhelp.Fatalf(t, evs, "second delivery must be DUPLICATE:\n%s", testhelp.DumpResults(rs))
	}
	if rs[1].DuplicateOfTime.IsZero() {
		t.Fatal("duplicate result must cite first ingest time")
	}
	out := rs[2].Confirmed[0]
	if out.Summary.EventCount != 1 || out.Summary.Records != 100 {
		testhelp.Fatalf(t, evs, "duplicate must not double count: %s", out.Summary.String())
	}
	if len(out.EventIDs) != 1 || out.EventIDs[0] != "dup1" {
		t.Fatalf("evidence ids = %v", out.EventIDs)
	}

	// Replay the duplicate again after the window closed: it stays DUPLICATE,
	// and the already confirmed conclusion is untouched.
	r4 := g.Ingest(evs[1])
	if r4.Status != model.StatusDuplicate {
		t.Fatalf("post-close redelivery must still be DUPLICATE, got %s", r4)
	}
}

func TestStreamsDoNotPolluteEachOther(t *testing.T) {
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	clk := &clock.FixedClock{T: base.Add(time.Minute)}
	g := newTestIngestor(t, clk, 0)

	j1 := model.StreamKey{JobID: "job1", Dataset: "d"}
	j2 := model.StreamKey{JobID: "job2", Dataset: "d"}
	// job2 races its watermark far ahead; job1 must not see windows close.
	g.Ingest(testhelp.Ev("a", "job1", "d", base, base, 10))
	// job2's first observation: windows before it are unknown and not
	// backfilled, so nothing is confirmed by this single event.
	r := g.Ingest(testhelp.Ev("far", "job2", "d", base.Add(10*time.Minute), base.Add(10*time.Minute), 1))
	if len(r.Confirmed) != 0 {
		t.Fatalf("first observation must not backfill ancient windows, got %d", len(r.Confirmed))
	}
	// Advancing job2 by two minutes confirms its gap window 10:10..10:11 and
	// the event's window only when it closes; job1 remains untouched.
	r = g.Ingest(testhelp.Ev("far2", "job2", "d", base.Add(12*time.Minute), base.Add(12*time.Minute), 0))
	if len(r.Confirmed) != 2 {
		t.Fatalf("job2 should confirm windows 10:10 and 10:11, got %d", len(r.Confirmed))
	}
	if got := g.Watermark(j1); !got.Equal(base) {
		t.Fatalf("job1 watermark leaked: %s", got)
	}
	if got := g.Watermark(j2); !got.Equal(base.Add(12 * time.Minute)) {
		t.Fatalf("job2 watermark wrong: %s", got)
	}
}

func TestEmptyGapWindowsAreConfirmed(t *testing.T) {
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	clk := &clock.FixedClock{T: base.Add(time.Minute)}
	g := newTestIngestorAt(t, clk, 0, base)
	r := g.Ingest(testhelp.Ev("only", "j", "d", base, base, 60))
	if len(r.Confirmed) != 0 {
		t.Fatalf("first event at its window start confirms nothing, got %d", len(r.Confirmed))
	}
	r = g.Ingest(testhelp.Ev("jump", "j", "d", base.Add(3*time.Minute), base.Add(3*time.Minute), 0))
	// windows 0,1,2 all close; 1 and 2 are explicit empty gap windows.
	if len(r.Confirmed) != 3 {
		t.Fatalf("expected 3 confirmed windows (incl. empty gaps), got %d", len(r.Confirmed))
	}
	for i, out := range r.Confirmed {
		if out.Window.Index != int64(i) || out.Key.JobID != "j" {
			t.Fatalf("confirmed[%d] wrong: idx=%d key=%s", i, out.Window.Index, out.Key)
		}
	}
	gap := r.Confirmed[1].Summary
	if gap.EventCount != 0 || gap.Records != 0 || gap.ThroughputPerSec != 0 {
		t.Fatalf("empty window must have zero metrics: %s", gap.String())
	}
	first := r.Confirmed[0].Summary
	if first.ThroughputPerSec != 1.0 { // 60 records / 60s
		t.Fatalf("throughput = %f want 1.0", first.ThroughputPerSec)
	}
}

func TestConcurrentIngestIsConsistent(t *testing.T) {
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	clk := &clock.FixedClock{T: base.Add(time.Minute)}
	g := newTestIngestor(t, clk, 0)

	const n = 200
	var wg sync.WaitGroup
	dups := make(chan model.IngestResult, n)
	accepts := make(chan model.IngestResult, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ts := base.Add(time.Duration(i) * 100 * time.Millisecond)
			r := g.Ingest(testhelp.Ev("e", "j", "d", ts, ts, 1)) // same id!
			if r.Status == model.StatusDuplicate {
				dups <- r
			} else {
				accepts <- r
			}
		}(i)
	}
	wg.Wait()
	close(dups)
	close(accepts)
	if len(accepts) != 1 || len(dups) != n-1 {
		t.Fatalf("exactly one of %d same-id concurrent events must be accepted, got accepted=%d duplicate=%d",
			n, len(accepts), len(dups))
	}
}
