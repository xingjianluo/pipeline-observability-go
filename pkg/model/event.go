package model

import (
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
}

// Key returns the stream the event belongs to.
func (e RunEvent) Key() StreamKey { return StreamKey{JobID: e.JobID, Dataset: e.Dataset} }

// Validate checks mandatory fields.
func (e RunEvent) Validate() error {
	return nil
}

// Status is the classification result of one ingest attempt.
type Status int

const (
	StatusAccepted     Status = iota // appended to an open/pending window
	StatusDuplicate                  // identical event already seen; state unchanged
	StatusRejectedLate               // event time is older than the allowed lateness cutoff
)

func (s Status) String() string {
	return ""
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

func (r IngestResult) String() string {
	return ""
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
	// ReportLatency = ingestTime - eventTime per event.
	MaxReportLatency time.Duration
	// FreshnessLag = windowEnd - max(watermarkTs seen in window); never negative.
	FreshnessLag time.Duration
	MaxWatermark time.Time // largest watermark observed in the window
	// Throughput = records / window size.
	ThroughputPerSec float64
}
