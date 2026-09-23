package observer

import (
	"time"

	"github.com/xingjianluo/pipeline-observability-go/pkg/alert"
	"github.com/xingjianluo/pipeline-observability-go/pkg/clock"
	"github.com/xingjianluo/pipeline-observability-go/pkg/ingest"
	"github.com/xingjianluo/pipeline-observability-go/pkg/metrics"
	"github.com/xingjianluo/pipeline-observability-go/pkg/model"
)

// Config builds an Observer.
type Config struct {
	WindowSize      time.Duration
	WindowEpoch     time.Time
	AllowedLateness time.Duration
	Rules           []alert.Rule
}

// Observer is the facade tying ingestor, metrics store and alert engine
// together; all operations are safe for concurrent use.
type Observer struct {
	clock    clock.Clock
	ingestor *ingest.Ingestor
	store    *metrics.Store
	alerts   *alert.Engine
}

// New constructs an Observer.
func New(c clock.Clock, cfg Config) *Observer { return nil }

// Ingest feeds one event and returns its classification plus any alerts that
// fired because windows got confirmed by this ingest.
func (o *Observer) Ingest(e model.RunEvent) Result { return Result{} }

// Result bundles one ingest's outcome.
type Result struct {
	Ingest model.IngestResult
	Alerts []alert.Alert
}

// Windows returns confirmed summaries for a stream in order.
func (o *Observer) Windows(k model.StreamKey) []model.WindowSummary { return nil }

// Watermark returns the current watermark for a stream.
func (o *Observer) Watermark(k model.StreamKey) time.Time { return time.Time{} }

// ActiveAlerts / Episodes delegate to the alert engine.
func (o *Observer) ActiveAlerts() []alert.Alert { return nil }
func (o *Observer) Episodes() []alert.Alert     { return nil }
