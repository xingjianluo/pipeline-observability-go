package observer

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/pipeline-observability-go/internal/testhelp"
	"github.com/xingjianluo/pipeline-observability-go/pkg/alert"
	"github.com/xingjianluo/pipeline-observability-go/pkg/clock"
	"github.com/xingjianluo/pipeline-observability-go/pkg/model"
)

var t0 = time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)

func newTestObserver(t *testing.T, c clock.Clock, rules []alert.Rule) *Observer {
	t.Helper()
	o, err := New(c, Config{
		WindowSize:      time.Minute,
		WindowEpoch:     t0,
		AllowedLateness: 30 * time.Second,
		Rules:           rules,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return o
}

// Metrics are frozen from the accepted event set and are explainable.
func TestEndToEndMetrics(t *testing.T) {
	clk := &clock.FixedClock{T: t0.Add(30 * time.Second)}
	o := newTestObserver(t, clk, nil)
	key := model.StreamKey{JobID: "job1", Dataset: "d1"}

	evs := []model.RunEvent{
		// event at 10:00:10, watermark 10:00:20, received 10:00:30
		testhelp.Ev("a", "job1", "d1", t0.Add(10*time.Second), t0.Add(20*time.Second), 600),
	}
	r := o.Ingest(evs[0])
	if r.Ingest.Status != model.StatusAccepted {
		testhelp.Fatalf(t, evs, "want accepted: %s", r.Ingest)
	}
	// Move clock forward so the late-but-in-order report has a larger latency.
	clk.T = t0.Add(40 * time.Second)
	evs = append(evs, testhelp.Ev("b", "job1", "d1", t0.Add(5*time.Second), t0.Add(30*time.Second), 300))
	o.Ingest(evs[1])
	// Close window 0: cutoff = 10:01:30.
	closeEv := testhelp.Ev("close", "job1", "d1", t0.Add(90*time.Second), t0.Add(90*time.Second), 0)
	evs = append(evs, closeEv)
	r = o.Ingest(closeEv)
	if len(r.Ingest.Confirmed) != 1 {
		testhelp.Fatalf(t, evs, "window 0 should confirm:\n%s", testhelp.DumpResults([]model.IngestResult{r.Ingest}))
	}
	sum, ok := o.GetWindow(key, 0)
	if !ok {
		t.Fatal("window 0 not stored")
	}
	if sum.EventCount != 2 || sum.Records != 900 {
		t.Fatalf("counts wrong: %s", sum.String())
	}
	if sum.ThroughputPerSec != 15 { // 900 records / 60s
		t.Fatalf("throughput=%v want 15 rec/s: %s", sum.ThroughputPerSec, sum.String())
	}
	// max watermark seen in the window is 10:00:30 -> lag 30s.
	if sum.FreshnessLag != 30*time.Second {
		t.Fatalf("freshness lag=%s want 30s: %s", sum.FreshnessLag, sum.String())
	}
	// max report latency: b received at 10:00:40 for event at 10:00:05 = 35s.
	if sum.MaxReportLatency != 35*time.Second {
		t.Fatalf("max report latency=%s want 35s: %s", sum.MaxReportLatency, sum.String())
	}
}

// The same events in different arrival orders yield identical confirmed metrics.
func TestArrivalOrderDeterminism(t *testing.T) {
	mkEvents := func() []model.RunEvent {
		return []model.RunEvent{
			testhelp.Ev("a", "j", "d", t0.Add(10*time.Second), t0.Add(10*time.Second), 100),
			testhelp.Ev("b", "j", "d", t0.Add(50*time.Second), t0.Add(50*time.Second), 200),
			testhelp.Ev("c", "j", "d", t0.Add(5*time.Second), t0.Add(5*time.Second), 50),
			testhelp.WM("close", "j", "d", t0.Add(91*time.Second)),
		}
	}
	run := func(order []int) []model.WindowSummary {
		o := newTestObserver(t, &clock.FixedClock{T: t0.Add(2 * time.Minute)}, nil)
		evs := mkEvents()
		for _, i := range order {
			o.Ingest(evs[i])
		}
		return o.Windows(model.StreamKey{JobID: "j", Dataset: "d"})
	}
	normal := run([]int{0, 1, 2, 3})
	shuffled := run([]int{2, 0, 1, 3})
	if len(normal) != len(shuffled) {
		t.Fatalf("window count differs: %d vs %d", len(normal), len(shuffled))
	}
	for i := range normal {
		if fmt.Sprintf("%v", normal[i]) != fmt.Sprintf("%v", shuffled[i]) {
			t.Fatalf("metric differs by arrival order at window %d:\n%v\nvs\n%v", i, normal[i], shuffled[i])
		}
		if w := normal[i]; w.EventCount != 3 || w.Records != 350 {
			t.Fatalf("window %d metrics wrong: %s", i, w.String())
		}
	}
}

// Replaying the entire event stream after it ran once must not double count,
// regress metrics or re-fire alerts.
func TestReplayIdempotence(t *testing.T) {
	lag := 20 * time.Second
	rules := []alert.Rule{{JobID: "j", Dataset: "d", MaxFreshnessLag: &lag}}
	evs := []model.RunEvent{
		testhelp.Ev("a", "j", "d", t0.Add(10*time.Second), t0.Add(10*time.Second), 10), // wm 10:00:10 -> lag 50s
		testhelp.WM("close", "j", "d", t0.Add(91*time.Second)),
	}
	o := newTestObserver(t, &clock.FixedClock{T: t0.Add(2 * time.Minute)}, rules)
	var firstAlerts int
	for _, e := range evs {
		firstAlerts += len(o.Ingest(e).Alerts)
	}
	if firstAlerts != 1 {
		t.Fatalf("expected exactly one initial alert, got %d", firstAlerts)
	}
	sumBefore := o.Windows(model.StreamKey{JobID: "j", Dataset: "d"})

	// Full replay: every event becomes a duplicate, nothing changes.
	for _, e := range evs {
		r := o.Ingest(e)
		if r.Ingest.Status != model.StatusDuplicate {
			testhelp.Fatalf(t, evs, "replay must be duplicate, got %s", r.Ingest)
		}
		if len(r.Alerts) != 0 {
			t.Fatalf("replay must not re-fire alerts: %+v", r.Alerts)
		}
	}
	sumAfter := o.Windows(model.StreamKey{JobID: "j", Dataset: "d"})
	if fmt.Sprintf("%v", sumBefore) != fmt.Sprintf("%v", sumAfter) {
		t.Fatalf("metrics changed after replay:\nbefore=%v\nafter=%v", sumBefore, sumAfter)
	}
	if len(o.ActiveAlerts()) != 1 {
		t.Fatalf("still exactly one active episode, got %d", len(o.ActiveAlerts()))
	}
}

// End-to-end: continuous failure fires once, recovery closes it, recurrence
// re-fires; late events after close are rejected with distinct reasons.
func TestEndToEndAlertLifecycleAndLateReject(t *testing.T) {
	lat := 30 * time.Second
	rules := []alert.Rule{{JobID: "j", Dataset: "d", MaxFreshnessLag: &lat}}
	o := newTestObserver(t, &clock.FixedClock{T: t0.Add(10 * time.Minute)}, rules)

	// window 0 bad (wm 10:00:10 -> freshness lag 50s); control at 10:01:31
	// advances the stream watermark past window 0 cutoff 10:01:30.
	o.Ingest(testhelp.Ev("w0bad", "j", "d", t0.Add(10*time.Second), t0.Add(10*time.Second), 0))
	r := o.Ingest(testhelp.WM("c0", "j", "d", t0.Add(91*time.Second)))
	if len(r.Alerts) != 1 || r.Alerts[0].Reason != alert.ReasonFreshness {
		t.Fatalf("window 0 breach must fire once: %+v", r.Alerts)
	}
	id1 := r.Alerts[0].EpisodeID

	// window 1 also bad (wm 10:01:10); control at 10:02:31 confirms it.
	o.Ingest(testhelp.Ev("w1bad", "j", "d", t0.Add(70*time.Second), t0.Add(70*time.Second), 0))
	r = o.Ingest(testhelp.WM("c1", "j", "d", t0.Add(151*time.Second)))
	if len(r.Alerts) != 0 {
		t.Fatalf("continuous breach must be suppressed: %+v", r.Alerts)
	}

	// window 2 healthy: data watermark reaches its end 10:03:00; control at
	// 10:03:31 confirms the healthy window and recovers the episode.
	o.Ingest(testhelp.Ev("w2ok", "j", "d", t0.Add(179*time.Second), t0.Add(180*time.Second), 0))
	r = o.Ingest(testhelp.WM("c2", "j", "d", t0.Add(211*time.Second)))
	if len(r.Alerts) != 0 || len(o.ActiveAlerts()) != 0 {
		t.Fatalf("healthy window must recover the episode without firing; alerts=%+v active=%+v",
			r.Alerts, o.ActiveAlerts())
	}

	// window 3 bad again (wm 10:03:10); control at 10:04:31 confirms it.
	o.Ingest(testhelp.Ev("w3bad", "j", "d", t0.Add(190*time.Second), t0.Add(190*time.Second), 0))
	r = o.Ingest(testhelp.WM("c3", "j", "d", t0.Add(271*time.Second)))
	if len(r.Alerts) != 1 || r.Alerts[0].EpisodeID == id1 {
		t.Fatalf("recurring breach must fire a new episode: %+v", r.Alerts)
	}

	// A straggler for long-closed window 0 is rejected late, never silent.
	rr := o.Ingest(testhelp.Ev("straggler", "j", "d", t0.Add(20*time.Second), t0.Add(20*time.Second), 0))
	if rr.Ingest.Status != model.StatusRejectedLate || rr.Ingest.Reason == "" {
		t.Fatalf("straggler must be REJECTED_LATE with reason: %s", rr.Ingest)
	}
}

// Concurrent ingestion across multiple streams: totals must be exact and
// streams must stay isolated.
func TestEndToEndConcurrent(t *testing.T) {
	o := newTestObserver(t, &clock.FixedClock{T: t0.Add(10 * time.Minute)}, nil)
	const perStream = 100
	var wg sync.WaitGroup
	var mu sync.Mutex
	var accepted, duplicate int
	for s := 0; s < 4; s++ {
		job := fmt.Sprintf("job%d", s)
		for i := 0; i < perStream; i++ {
			wg.Add(1)
			go func(job string, i int) {
				defer wg.Done()
				e := testhelp.Ev(fmt.Sprintf("e%d", i), job, "d",
					t0.Add(time.Duration(i)*100*time.Millisecond),
					t0.Add(time.Duration(i)*100*time.Millisecond), 1)
				r := o.Ingest(e)
				mu.Lock()
				switch r.Ingest.Status {
				case model.StatusAccepted:
					accepted++
				case model.StatusDuplicate:
					duplicate++
				}
				mu.Unlock()
			}(job, i)
		}
	}
	wg.Wait()
	if accepted != 4*perStream || duplicate != 0 {
		t.Fatalf("accepted=%d want %d, duplicate=%d want 0", accepted, 4*perStream, duplicate)
	}
	// Advance each stream watermark past window 0 (end 10:01 + 30s lateness)
	// so totals can be read from confirmed, immutable windows.
	for s := 0; s < 4; s++ {
		job := fmt.Sprintf("job%d", s)
		r := o.Ingest(testhelp.WM("close", job, "d", t0.Add(91*time.Second)))
		if len(r.Ingest.Confirmed) != 1 {
			t.Fatalf("%s: expected window 0 to confirm, got %d", job, len(r.Ingest.Confirmed))
		}
	}
	for s := 0; s < 4; s++ {
		k := model.StreamKey{JobID: fmt.Sprintf("job%d", s), Dataset: "d"}
		got := o.Windows(k)
		var events, records int64
		for _, w := range got {
			events += w.EventCount
			records += w.Records
		}
		if events != perStream || records != perStream {
			t.Fatalf("%s totals events=%d records=%d want %d each", k, events, records, perStream)
		}
	}
}
