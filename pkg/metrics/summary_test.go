package metrics

import (
	"testing"
	"time"

	"github.com/xingjianluo/pipeline-observability-go/internal/testhelp"
	"github.com/xingjianluo/pipeline-observability-go/pkg/model"
	"github.com/xingjianluo/pipeline-observability-go/pkg/window"
)

func TestSummarizeDeterminismAndSemantics(t *testing.T) {
	t0 := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	w := window.Window{Start: t0, End: t0.Add(time.Minute), Index: 0}
	mk := func() []EventPoint {
		return []EventPoint{
			{Event: testhelp.Ev("a", "j", "d", t0.Add(10*time.Second), t0.Add(50*time.Second), 100),
				IngestTime: t0.Add(30 * time.Second)}, // report latency 20s
			{Event: testhelp.Ev("b", "j", "d", t0.Add(20*time.Second), t0.Add(40*time.Second), 200),
				IngestTime: t0.Add(70 * time.Second)}, // report latency 50s
		}
	}
	out1 := Summarize(model.StreamKey{JobID: "j", Dataset: "d"}, w, mk(), t0.Add(2*time.Minute))
	// Shuffle input order: identical output.
	pts := mk()
	pts[0], pts[1] = pts[1], pts[0]
	out2 := Summarize(model.StreamKey{JobID: "j", Dataset: "d"}, w, pts, t0.Add(2*time.Minute))
	if out1.Summary.String() != out2.Summary.String() {
		t.Fatalf("summary depends on point order:\n%s\nvs\n%s", out1.Summary, out2.Summary)
	}
	s := out1.Summary
	if s.EventCount != 2 || s.Records != 300 {
		t.Fatalf("counts wrong: %s", s.String())
	}
	if s.MaxReportLatency != 50*time.Second {
		t.Fatalf("max report latency=%s want 50s", s.MaxReportLatency)
	}
	if !s.MaxWatermark.Equal(t0.Add(50 * time.Second)) {
		t.Fatalf("max watermark=%s", s.MaxWatermark)
	}
	if s.FreshnessLag != 10*time.Second { // end 10:01:00 - wm 10:00:50
		t.Fatalf("freshness lag=%s want 10s", s.FreshnessLag)
	}
	if s.ThroughputPerSec != 5 { // 300 / 60
		t.Fatalf("throughput=%v want 5", s.ThroughputPerSec)
	}
	if len(out1.EventIDs) != 2 {
		t.Fatalf("evidence ids=%v", out1.EventIDs)
	}

	// Empty window: zero freshness lag, zero throughput (no bogus max lag).
	empty := Summarize(model.StreamKey{JobID: "j", Dataset: "d"}, w, nil, t0.Add(2*time.Minute))
	if empty.Summary.FreshnessLag != 0 || empty.Summary.ThroughputPerSec != 0 ||
		empty.Summary.MaxReportLatency != 0 || len(empty.EventIDs) != 0 {
		t.Fatalf("empty window semantics wrong: %s", empty.Summary.String())
	}

	// Watermark past window end: freshness lag is clamped at zero, never neg.
	future := []EventPoint{{
		Event:      testhelp.Ev("c", "j", "d", t0.Add(5*time.Second), t0.Add(2*time.Minute), 1),
		IngestTime: t0.Add(10 * time.Second),
	}}
	if got := Summarize(model.StreamKey{JobID: "j", Dataset: "d"}, w, future, t0).Summary.FreshnessLag; got != 0 {
		t.Fatalf("future watermark freshness lag must clamp at 0, got %s", got)
	}
}
