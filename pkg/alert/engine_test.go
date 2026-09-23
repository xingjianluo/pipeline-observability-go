package alert

import (
	"testing"
	"time"

	"github.com/xingjianluo/pipeline-observability-go/pkg/model"
	"github.com/xingjianluo/pipeline-observability-go/pkg/window"
)

var base = time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)

func outAt(idx int64, lag, lat time.Duration, tput float64, ids ...string) model.WindowOutput {
	w := window.Window{Start: base.Add(time.Duration(idx) * time.Minute),
		End: base.Add(time.Duration(idx+1) * time.Minute), Index: idx}
	return model.WindowOutput{
		Key:       model.StreamKey{JobID: "job1", Dataset: "d1"},
		Window:    w,
		EventIDs:  ids,
		Confirmed: base.Add(time.Duration(idx+2) * time.Minute),
		Summary: model.WindowSummary{
			WindowStart:      w.Start,
			WindowEnd:        w.End,
			FreshnessLag:     lag,
			MaxReportLatency: lat,
			ThroughputPerSec: tput,
		},
	}
}

func dptr(d time.Duration) *time.Duration { return &d }
func fptr(f float64) *float64             { return &f }

// Bad -> suppressed during continuation -> good recovers -> bad fires again.
func TestEpisodeSuppressionAndRecovery(t *testing.T) {
	e := New([]Rule{{
		JobID:               "job1",
		Dataset:             "d1",
		MaxFreshnessLag:     dptr(30 * time.Second),
		MaxReportLatency:    dptr(60 * time.Second),
		MinThroughputPerSec: fptr(10),
	}})

	good := func(idx int64) model.WindowOutput { return outAt(idx, 10*time.Second, 10*time.Second, 100, "g") }
	bad := func(idx int64) model.WindowOutput { return outAt(idx, time.Minute, 2*time.Minute, 1, "b") }

	// window 0 bad: all three reasons fire once.
	fired := e.Evaluate(bad(0))
	if len(fired) != 3 {
		t.Fatalf("first bad window: want 3 alerts (one per reason), got %d: %+v", len(fired), fired)
	}
	firstIDs := map[Reason]string{}
	for _, a := range fired {
		firstIDs[a.Reason] = a.EpisodeID
		if len(a.EventIDs) != 1 || a.EventIDs[0] != "b" {
			t.Fatalf("alert must carry source events for reproduction: %+v", a)
		}
	}
	// window 1 continues the failure: suppressed.
	if f := e.Evaluate(bad(1)); len(f) != 0 {
		t.Fatalf("continuing failure must be suppressed, got %d alerts", len(f))
	}
	if len(e.ActiveAlerts()) != 3 {
		t.Fatalf("3 episodes should stay active, got %d", len(e.ActiveAlerts()))
	}
	// window 2 healthy: all episodes recover.
	if f := e.Evaluate(good(2)); len(f) != 0 {
		t.Fatalf("healthy window fires no alerts, got %d", len(f))
	}
	if len(e.ActiveAlerts()) != 0 {
		t.Fatalf("episodes must recover after one good window, got %d active", len(e.ActiveAlerts()))
	}
	eps := e.Episodes()
	if len(eps) != 3 {
		t.Fatalf("want 3 closed episodes, got %d", len(eps))
	}
	for _, ep := range eps {
		if ep.RecoveredAt.IsZero() {
			t.Fatalf("episode %s should carry recovery time", ep.EpisodeID)
		}
	}
	// window 3 bad again: new episodes with fresh ids must fire.
	fired = e.Evaluate(bad(3))
	if len(fired) != 3 {
		t.Fatalf("recurring failure must re-fire, got %d alerts", len(fired))
	}
	for _, a := range fired {
		if a.EpisodeID == firstIDs[a.Reason] {
			t.Fatalf("reason %s reused old episode id %s after recovery", a.Reason, a.EpisodeID)
		}
	}
}

func TestThresholdBoundaryAndDisabledChecks(t *testing.T) {
	// Exactly at threshold is healthy (breach is strictly over/under).
	e := New([]Rule{{JobID: "job1", Dataset: "d1", MaxFreshnessLag: dptr(30 * time.Second)}})
	if f := e.Evaluate(outAt(0, 30*time.Second, 0, 0)); len(f) != 0 {
		t.Fatalf("lag exactly at threshold must not alert, got %d", len(f))
	}
	if f := e.Evaluate(outAt(1, 30*time.Second+1, 0, 0)); len(f) != 1 {
		t.Fatalf("lag just over threshold must alert, got %d", len(f))
	}

	// Another stream is not governed by the rule.
	e2 := New([]Rule{{JobID: "other", Dataset: "d1", MaxFreshnessLag: dptr(time.Second)}})
	o := outAt(0, time.Hour, 0, 0)
	o.Key = model.StreamKey{JobID: "job1", Dataset: "d1"}
	if f := e2.Evaluate(o); len(f) != 0 {
		t.Fatalf("rule for other job must not match, got %d", len(f))
	}
}

func TestDifferentReasonsAreIndependentEpisodes(t *testing.T) {
	e := New([]Rule{{
		JobID:            "job1",
		Dataset:          "d1",
		MaxFreshnessLag:  dptr(30 * time.Second),
		MaxReportLatency: dptr(30 * time.Second),
	}})
	// freshness bad, latency healthy -> only freshness fires.
	f := e.Evaluate(outAt(0, time.Minute, time.Second, 0))
	if len(f) != 1 || f[0].Reason != ReasonFreshness {
		t.Fatalf("want one freshness alert, got %+v", f)
	}
	// freshness recovers while latency starts failing: no freshness re-fire,
	// latency opens its own first episode.
	f = e.Evaluate(outAt(1, time.Second, time.Minute, 0))
	if len(f) != 1 || f[0].Reason != ReasonLatency {
		t.Fatalf("want one latency alert (freshness recovered), got %+v", f)
	}
	eps := e.Episodes()
	if len(eps) != 2 {
		t.Fatalf("want one closed + one active episode, got %+v", eps)
	}
	if eps[0].Reason != ReasonFreshness || eps[0].RecoveredAt.IsZero() {
		t.Fatalf("first episode must be recovered freshness: %+v", eps[0])
	}
	if eps[1].Reason != ReasonLatency || !eps[1].Active() {
		t.Fatalf("second episode must be active latency: %+v", eps[1])
	}
}
