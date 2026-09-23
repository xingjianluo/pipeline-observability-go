// Package testhelp shares test diagnostics across the component packages.
package testhelp

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xingjianluo/pipeline-observability-go/pkg/model"
)

// Ev builds a run event concisely.
func Ev(id, job, dataset string, eventTime, watermark time.Time, records int64) model.RunEvent {
	return model.RunEvent{
		EventID:     id,
		JobID:       job,
		Dataset:     dataset,
		EventTime:   eventTime,
		WatermarkTs: watermark,
		Records:     records,
	}
}

// DumpEvents renders the input event list for failure messages.
func DumpEvents(evs []model.RunEvent) string {
	var b strings.Builder
	for i, e := range evs {
		fmt.Fprintf(&b, "\n  input[%d] id=%s stream=%s eventTime=%s watermark=%s records=%d",
			i, e.EventID, e.Key(), e.EventTime.Format(time.RFC3339Nano),
			e.WatermarkTs.Format(time.RFC3339Nano), e.Records)
	}
	return b.String()
}

// DumpResults renders every ingest decision for failure messages.
func DumpResults(rs []model.IngestResult) string {
	var b strings.Builder
	for i, r := range rs {
		fmt.Fprintf(&b, "\n  decision[%d] %s", i, r.String())
	}
	return b.String()
}

// Fatalf prints inputs plus the failing assertion basis.
func Fatalf(t *testing.T, evs []model.RunEvent, format string, args ...any) {
	t.Helper()
	msg := fmt.Sprintf(format, args...)
	t.Fatalf("%s\ninputs:%s", msg, DumpEvents(evs))
}

// WM builds a watermark-only control event that advances the watermark
// without contributing to any window's metrics.
func WM(id, job, dataset string, at time.Time) model.RunEvent {
	return model.RunEvent{
		EventID:       id,
		JobID:         job,
		Dataset:       dataset,
		EventTime:     at,
		WatermarkTs:   at,
		WatermarkOnly: true,
	}
}
