package model

import (
	"fmt"
	"time"

	"github.com/xingjianluo/pipeline-observability-go/pkg/window"
)

// StreamKey identifies one (job, dataset) event stream.
type StreamKey struct {
	JobID   string
	Dataset string
}

func (k StreamKey) String() string {
	return k.JobID + "/" + k.Dataset
}

// RunEvent is one reported run record for a job/dataset.
type RunEvent struct {
	EventID     string // dedup identity (must be unique per stream)
	JobID       string
	Dataset     string
	EventTime   time.Time // when the run actually happened (business time)
	WatermarkTs time.Time // data watermark carried by the producer at report time
	Records     int64     // number of records produced by the run
	// WatermarkOnly marks a control event that only advances the watermark.
	// It is buffered per stream for ordering but excluded from window metrics
	// (counts, records, latency, freshness watermark), so watermark progress
	// can never make a data window look healthier or busier.
	WatermarkOnly bool
}

// Key returns the stream the event belongs to.
func (e RunEvent) Key() StreamKey { return StreamKey{JobID: e.JobID, Dataset: e.Dataset} }

// Validate checks mandatory fields and invariants.
func (e RunEvent) Validate() error {
	switch {
	case e.JobID == "":
		return fmt.Errorf("event %q: job id is required", e.EventID)
	case e.Dataset == "":
		return fmt.Errorf("event %q: dataset is required", e.EventID)
	case e.EventID == "":
		return fmt.Errorf("event for %s/%s: event id is required", e.JobID, e.Dataset)
	case e.EventTime.IsZero():
		return fmt.Errorf("event %q: event time is required", e.EventID)
	case e.Records < 0:
		return fmt.Errorf("event %q: records must be non-negative, got %d", e.EventID, e.Records)
	case e.WatermarkOnly && e.Records != 0:
		return fmt.Errorf("event %q: watermark-only control event must carry zero records", e.EventID)
	}
	return nil
}

// Status is the classification result of one ingest attempt.
type Status int

const (
	StatusAccepted        Status = iota // appended to an open/pending window
	StatusDuplicate                     // identical event already seen; state unchanged
	StatusRejectedLate                  // event time is older than the allowed lateness cutoff
	StatusRejectedInvalid               // failed validation; never entered any window
)

func (s Status) String() string {
	switch s {
	case StatusAccepted:
		return "ACCEPTED"
	case StatusDuplicate:
		return "DUPLICATE"
	case StatusRejectedLate:
		return "REJECTED_LATE"
	case StatusRejectedInvalid:
		return "REJECTED_INVALID"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int(s))
	}
}

// IngestResult is the outcome of Ingestor.Ingest.
type IngestResult struct {
	Status          Status
	Reason          string         // human-readable explanation, never empty on non-accept
	Event           RunEvent       // the original event
	IngestTime      time.Time      // wall-clock receipt time
	TargetWindow    window.Window  // window the event belongs to (zero when rejected)
	Watermark       time.Time      // stream watermark after ingest (zero when rejected)
	Confirmed       []WindowOutput // windows closed and confirmed by this ingest
	DuplicateOfTime time.Time      // first ingest time when duplicate
}

// String prints the full decision basis (input + classification), which test
// failures can surface directly.
func (r IngestResult) String() string {
	switch r.Status {
	case StatusAccepted:
		return fmt.Sprintf(
			"ACCEPTED event=%s stream=%s eventTime=%s records=%d -> window[%d %s..%s) watermark=%s confirmed=%d",
			r.Event.EventID, r.Event.Key(), r.Event.EventTime.Format(time.RFC3339Nano),
			r.Event.Records, r.TargetWindow.Index,
			r.TargetWindow.Start.Format(time.RFC3339), r.TargetWindow.End.Format(time.RFC3339),
			fmtWatermark(r.Watermark), len(r.Confirmed))
	case StatusDuplicate:
		return fmt.Sprintf(
			"DUPLICATE event=%s stream=%s eventTime=%s: identical event first ingested at %s",
			r.Event.EventID, r.Event.Key(), r.Event.EventTime.Format(time.RFC3339Nano),
			r.DuplicateOfTime.Format(time.RFC3339Nano))
	case StatusRejectedLate:
		return fmt.Sprintf(
			"REJECTED_LATE event=%s stream=%s eventTime=%s: %s",
			r.Event.EventID, r.Event.Key(), r.Event.EventTime.Format(time.RFC3339Nano), r.Reason)
	default:
		return fmt.Sprintf(
			"REJECTED_INVALID event=%s stream=%s eventTime=%s: %s",
			r.Event.EventID, r.Event.Key(), r.Event.EventTime.Format(time.RFC3339Nano), r.Reason)
	}
}

func fmtWatermark(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Format(time.RFC3339Nano)
}

// WindowOutput is an immutable, confirmed result for one window of one stream.
type WindowOutput struct {
	Key       StreamKey
	Window    window.Window
	Summary   WindowSummary
	EventIDs  []string  // accepted event ids in ingest order (reproduction evidence)
	Confirmed time.Time // when the window was finalized
}

// WindowSummary is the frozen metric view of a confirmed window.
type WindowSummary struct {
	WindowStart time.Time
	WindowEnd   time.Time
	EventCount  int64
	Records     int64
	// MaxReportLatency = max(ingestTime - eventTime) over window events.
	MaxReportLatency time.Duration
	// FreshnessLag = max(0, windowEnd - max watermark observed in the window).
	FreshnessLag time.Duration
	MaxWatermark time.Time // largest watermark observed in the window
	// ThroughputPerSec = records / window length in seconds.
	ThroughputPerSec float64
}

// String renders the summary for assertions and diagnostics.
func (s WindowSummary) String() string {
	return fmt.Sprintf(
		"window[%s..%s) events=%d records=%d maxReportLatency=%s maxWatermark=%s freshnessLag=%s throughputPerSec=%.6f",
		s.WindowStart.Format(time.RFC3339), s.WindowEnd.Format(time.RFC3339),
		s.EventCount, s.Records, s.MaxReportLatency,
		fmtWatermark(s.MaxWatermark), s.FreshnessLag, s.ThroughputPerSec)
}
