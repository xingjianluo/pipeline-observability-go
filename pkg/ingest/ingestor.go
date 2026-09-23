package ingest

import (
	"sync"
	"time"

	"github.com/xingjianluo/pipeline-observability-go/pkg/clock"
	"github.com/xingjianluo/pipeline-observability-go/pkg/model"
	"github.com/xingjianluo/pipeline-observability-go/pkg/window"
)

// Ingestor classifies incoming events per stream, advances per-stream
// watermarks and confirms windows once they leave the lateness horizon.
type Ingestor struct {
	mu        sync.Mutex
	clock     clock.Clock
	spec      window.Spec
	allowed   time.Duration
	streams   map[model.StreamKey]*streamState
	onConfirm func(model.WindowOutput)
}

// Config configures an Ingestor.
type Config struct {
	WindowSize      time.Duration
	WindowEpoch     time.Time
	AllowedLateness time.Duration
	OnConfirm       func(model.WindowOutput)
}

// New constructs an Ingestor.
func New(c clock.Clock, cfg Config) *Ingestor { return nil }

// Ingest validates and classifies one event, then advances the watermark.
func (g *Ingestor) Ingest(e model.RunEvent) model.IngestResult {
	return model.IngestResult{}
}

// Watermark returns the current event-time watermark of a stream.
func (g *Ingestor) Watermark(k model.StreamKey) time.Time { return time.Time{} }

// streamState is the per-(job,dataset) bookkeeping.
type streamState struct {
	watermark time.Time
	seen      map[string]time.Time // eventID -> first ingest time
	// open accumulates accepted events per window index until confirmation.
	open map[int64]*pendingWindow
	// confirmedUpTo is the highest window index already finalized.
	confirmedUpTo int64
}

type pendingEvent struct {
	event      model.RunEvent
	ingestTime time.Time
}

type pendingWindow struct {
	w      window.Window
	events []pendingEvent
}
