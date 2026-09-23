package metrics

import (
	"time"

	"github.com/xingjianluo/pipeline-observability-go/pkg/model"
	"github.com/xingjianluo/pipeline-observability-go/pkg/window"
)

// EventPoint is one accepted event paired with its deterministic receipt time.
type EventPoint struct {
	Event      model.RunEvent
	IngestTime time.Time
}

// Summarize freezes one window's metrics from exactly the accepted events it
// contains. Because the input is the immutable set of window events, repeated
// calls with the same input always yield byte-identical output: metrics never
// regress and replay cannot double count.
//
// Metrics:
//   - EventCount / Records: sums over accepted events (duplicates excluded
//     upstream, so they never reach here).
//   - MaxReportLatency: max(ingestTime - eventTime), how delayed a report was.
//   - FreshnessLag: max(0, windowEnd - max watermark observed), how far behind
//     the data had caught up by the end of the window.
//   - ThroughputPerSec: records / window length in seconds.
func Summarize(key model.StreamKey, w window.Window, points []EventPoint, confirmedAt time.Time) model.WindowOutput {
	out := model.WindowOutput{
		Key:       key,
		Window:    w,
		Confirmed: confirmedAt,
		EventIDs:  make([]string, 0, len(points)),
	}
	sum := model.WindowSummary{
		WindowStart: w.Start,
		WindowEnd:   w.End,
	}
	var maxLatency time.Duration
	var maxWM time.Time
	for _, p := range points {
		sum.EventCount++
		sum.Records += p.Event.Records
		if lat := p.IngestTime.Sub(p.Event.EventTime); lat > maxLatency {
			maxLatency = lat
		}
		if p.Event.WatermarkTs.After(maxWM) {
			maxWM = p.Event.WatermarkTs
		}
		out.EventIDs = append(out.EventIDs, p.Event.EventID)
	}
	sum.MaxReportLatency = maxLatency
	sum.MaxWatermark = maxWM
	if lag := w.End.Sub(maxWM); lag > 0 {
		sum.FreshnessLag = lag
	}
	secs := w.End.Sub(w.Start).Seconds()
	if secs > 0 {
		sum.ThroughputPerSec = float64(sum.Records) / secs
	}
	out.Summary = sum
	return out
}
