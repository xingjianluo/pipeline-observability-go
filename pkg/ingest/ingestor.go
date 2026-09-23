package ingest

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xingjianluo/pipeline-observability-go/pkg/clock"
	"github.com/xingjianluo/pipeline-observability-go/pkg/metrics"
	"github.com/xingjianluo/pipeline-observability-go/pkg/model"
	"github.com/xingjianluo/pipeline-observability-go/pkg/window"
)

// ErrInvalidConfig is returned by New for a bad window configuration.
var ErrInvalidConfig = errors.New("ingest: invalid configuration")

// Ingestor classifies incoming events per stream, advances per-stream
// watermarks and confirms windows once they leave the lateness horizon.
//
// Ordering rules (all timestamps are event time, not wall-clock time):
//   - watermark(k, n) = max over accepted events of max(EventTime, WatermarkTs)
//   - window i is confirmed once watermark >= E(i).End + AllowedLateness
//   - an event whose window end + AllowedLateness <= watermark is rejected as
//     REJECTED_LATE: it could never change an already decided conclusion
//   - events with identical EventID are DUPLICATE (first receipt wins, state
//     untouched); rejected/late events are not recorded as duplicates
//
// All state lives per StreamKey, so different jobs/datasets cannot pollute
// each other, and a single mutex makes any arrival order produce the same
// result as the equivalent serial replay.
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

// New constructs an Ingestor. AllowedLateness must be non-negative.
func New(c clock.Clock, cfg Config) (*Ingestor, error) {
	spec := window.Spec{Size: cfg.WindowSize, Epoch: cfg.WindowEpoch}
	if err := spec.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	if cfg.AllowedLateness < 0 {
		return nil, fmt.Errorf("%w: allowed lateness must be non-negative", ErrInvalidConfig)
	}
	if c == nil {
		c = clock.RealClock{}
	}
	return &Ingestor{
		clock:     c,
		spec:      spec,
		allowed:   cfg.AllowedLateness,
		streams:   make(map[model.StreamKey]*streamState),
		onConfirm: cfg.OnConfirm,
	}, nil
}

// Ingest validates and classifies one event, then advances the watermark and
// confirms every window that the advance closes.
func (g *Ingestor) Ingest(e model.RunEvent) model.IngestResult {
	now := g.clock.Now()
	res := model.IngestResult{Event: e, IngestTime: now}

	if err := e.Validate(); err != nil {
		res.Status = model.StatusRejectedInvalid
		res.Reason = err.Error()
		return res
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	st := g.stateFor(e.Key())
	win := g.spec.Assign(e.EventTime)

	// Duplicate check first: replays must be observable as duplicates even if
	// the re-sent copy is already late relative to the advanced watermark.
	if first, ok := st.seen[e.EventID]; ok {
		res.Status = model.StatusDuplicate
		res.DuplicateOfTime = first
		res.Watermark = st.watermark
		return res
	}

	// Lateness decision relative to the current watermark. A confirmed window
	// is immutable; admitting such an event would change a frozen conclusion.
	if !st.watermark.IsZero() && !win.End.Add(g.allowed).After(st.watermark) {
		res.Status = model.StatusRejectedLate
		res.Reason = fmt.Sprintf(
			"event time %s belongs to window ending %s; stream watermark is %s and allowed lateness is %s (cutoff %s)",
			e.EventTime.Format(time.RFC3339Nano), win.End.Format(time.RFC3339Nano),
			st.watermark.Format(time.RFC3339Nano), g.allowed,
			win.End.Add(g.allowed).Format(time.RFC3339Nano))
		res.Watermark = st.watermark
		return res
	}

	// Accept: record identity before any state change.
	st.seen[e.EventID] = now

	// First observation seeds the grid: windows strictly before the stream's
	// first event are unknown to it and are not backfilled as empty windows.
	if !st.started {
		st.started = true
		st.confirmedUpTo = win.Index - 1
	}

	// Data events are buffered into their window; watermark-only controls
	// advance the watermark but never enter a window's metric input.
	if !e.WatermarkOnly {
		pw := st.open[win.Index]
		if pw == nil {
			pw = &pendingWindow{w: win}
			st.open[win.Index] = pw
		}
		pw.events = append(pw.events, pendingEvent{event: e, ingestTime: now})
	}

	g.advanceWatermark(st, e)
	res.Status = model.StatusAccepted
	res.TargetWindow = win
	res.Watermark = st.watermark
	res.Confirmed = g.confirmClosed(st)
	return res
}

// Watermark returns the current event-time watermark of a stream.
func (g *Ingestor) Watermark(k model.StreamKey) time.Time {
	g.mu.Lock()
	defer g.mu.Unlock()
	if st, ok := g.streams[k]; ok {
		return st.watermark
	}
	return time.Time{}
}

// Spec returns the window grid used by the ingestor.
func (g *Ingestor) Spec() window.Spec { return g.spec }

func (g *Ingestor) stateFor(k model.StreamKey) *streamState {
	st := g.streams[k]
	if st == nil {
		st = &streamState{
			key:           k,
			seen:          make(map[string]time.Time),
			open:          make(map[int64]*pendingWindow),
			confirmedUpTo: 0, // meaningless until started
		}
		g.streams[k] = st
	}
	return st
}

// advanceWatermark folds one accepted event's timestamps into the watermark.
func (g *Ingestor) advanceWatermark(st *streamState, e model.RunEvent) {
	candidate := e.EventTime
	if e.WatermarkTs.After(candidate) {
		candidate = e.WatermarkTs
	}
	if candidate.After(st.watermark) {
		st.watermark = candidate
	}
}

// confirmClosed finalizes every window whose end + allowed lateness is no
// longer after the watermark. Buffers are released after confirmation.
// Called with g.mu held; callbacks run outside the critical section.
func (g *Ingestor) confirmClosed(st *streamState) []model.WindowOutput {
	var outputs []model.WindowOutput
	for {
		idx := st.confirmedUpTo + 1
		win := g.windowAt(idx)
		if win.End.Add(g.allowed).After(st.watermark) {
			break
		}
		pw := st.open[idx]
		var points []metrics.EventPoint
		if pw != nil {
			points = make([]metrics.EventPoint, 0, len(pw.events))
			for _, pe := range pw.events {
				points = append(points, metrics.EventPoint{Event: pe.event, IngestTime: pe.ingestTime})
			}
		}
		out := metrics.Summarize(st.key, win, points, g.clock.Now())
		st.confirmedUpTo = idx
		delete(st.open, idx)
		outputs = append(outputs, out)
	}
	if g.onConfirm != nil {
		for _, out := range outputs {
			g.onConfirm(out)
		}
	}
	return outputs
}

// windowAt returns the window for an arbitrary index, even when the stream
// never observed it (empty windows still get confirmed so gaps are explicit).
func (g *Ingestor) windowAt(idx int64) window.Window {
	start := g.spec.Epoch
	if start.IsZero() {
		start = time.Unix(0, 0)
	}
	start = start.Add(time.Duration(idx) * g.spec.Size)
	return window.Window{Start: start, End: start.Add(g.spec.Size), Index: idx}
}

// streamState is the per-(job,dataset) bookkeeping.
type streamState struct {
	key           model.StreamKey
	watermark     time.Time
	seen          map[string]time.Time // eventID -> first ingest time
	open          map[int64]*pendingWindow
	confirmedUpTo int64
	started       bool
}

type pendingEvent struct {
	event      model.RunEvent
	ingestTime time.Time
}

type pendingWindow struct {
	w      window.Window
	events []pendingEvent
}
