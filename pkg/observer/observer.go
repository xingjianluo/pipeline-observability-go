package observer

import (
	"sync"
	"time"

	"github.com/xingjianluo/pipeline-observability-go/pkg/alert"
	"github.com/xingjianluo/pipeline-observability-go/pkg/clock"
	"github.com/xingjianluo/pipeline-observability-go/pkg/ingest"
	"github.com/xingjianluo/pipeline-observability-go/pkg/metrics"
	"github.com/xingjianluo/pipeline-observability-go/pkg/model"
	"github.com/xingjianluo/pipeline-observability-go/pkg/window"
)

// Config builds an Observer.
type Config struct {
	WindowSize      time.Duration
	WindowEpoch     time.Time
	AllowedLateness time.Duration
	Rules           []alert.Rule
}

// Observer is the facade tying ingestor, metrics store and alert engine
// together. Confirmed windows are stored exactly once and evaluated exactly
// once, so replay and concurrency cannot double count or re-fire alerts.
type Observer struct {
	spec     window.Spec
	ingestor *ingest.Ingestor
	store    *metrics.Store
	alerts   *alert.Engine

	// ingestMu serializes whole Ingest calls so the per-call alert capture
	// cannot interleave between concurrent callers.
	ingestMu sync.Mutex
	// firedMu guards capture, which is only non-nil during an Ingest call.
	firedMu sync.Mutex
	capture []alert.Alert
}

// New constructs an Observer.
func New(c clock.Clock, cfg Config) (*Observer, error) {
	spec := window.Spec{Size: cfg.WindowSize, Epoch: cfg.WindowEpoch}
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	store := metrics.New(spec)
	engine := alert.New(cfg.Rules)
	o := &Observer{spec: spec, store: store, alerts: engine}
	ing, err := ingest.New(c, ingest.Config{
		WindowSize:      cfg.WindowSize,
		WindowEpoch:     cfg.WindowEpoch,
		AllowedLateness: cfg.AllowedLateness,
		OnConfirm: func(out model.WindowOutput) {
			// Each confirmed window is stored and evaluated at most once.
			_ = store.Finalize(out)
			for _, a := range engine.Evaluate(out) {
				o.collect(a)
			}
		},
	})
	if err != nil {
		return nil, err
	}
	o.ingestor = ing
	return o, nil
}

func (o *Observer) collect(a alert.Alert) {
	o.firedMu.Lock()
	defer o.firedMu.Unlock()
	if o.capture != nil {
		o.capture = append(o.capture, a)
	}
}

// Ingest feeds one event and returns its classification plus any alerts that
// fired because windows got confirmed by this ingest.
func (o *Observer) Ingest(e model.RunEvent) Result {
	o.ingestMu.Lock()
	defer o.ingestMu.Unlock()

	o.firedMu.Lock()
	o.capture = []alert.Alert{}
	o.firedMu.Unlock()

	res := o.ingestor.Ingest(e)

	o.firedMu.Lock()
	fired := o.capture
	o.capture = nil
	o.firedMu.Unlock()

	return Result{Ingest: res, Alerts: fired}
}

// Result bundles one ingest's outcome.
type Result struct {
	Ingest model.IngestResult
	Alerts []alert.Alert
}

// Windows returns confirmed summaries for a stream in window order.
func (o *Observer) Windows(k model.StreamKey) []model.WindowSummary {
	return o.store.List(k)
}

// GetWindow returns one confirmed summary by window index.
func (o *Observer) GetWindow(k model.StreamKey, idx int64) (model.WindowSummary, bool) {
	return o.store.Get(k, idx)
}

// Watermark returns the current watermark for a stream.
func (o *Observer) Watermark(k model.StreamKey) time.Time {
	return o.ingestor.Watermark(k)
}

// ActiveAlerts delegates to the alert engine.
func (o *Observer) ActiveAlerts() []alert.Alert { return o.alerts.ActiveAlerts() }

// Episodes delegates to the alert engine.
func (o *Observer) Episodes() []alert.Alert { return o.alerts.Episodes() }
