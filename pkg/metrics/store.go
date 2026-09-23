package metrics

import (
	"sync"
	"time"

	"github.com/xingjianluo/pipeline-observability-go/pkg/model"
	"github.com/xingjianluo/pipeline-observability-go/pkg/window"
)

// Store keeps confirmed window summaries; values are append-only and never
// change once stored, so replay cannot rewrite history or regress numbers.
type Store struct {
	mu      sync.Mutex
	windows map[model.StreamKey]map[int64]model.WindowSummary
	order   map[model.StreamKey][]int64
	spec    window.Spec
}

// New constructs a metrics Store for the given window spec.
func New(spec window.Spec) *Store { return nil }

// Finalize records an already confirmed window output. Duplicate finalization
// of the same window returns ErrAlreadyFinalized and changes nothing.
func (s *Store) Finalize(out model.WindowOutput) error { return nil }

// Get returns the confirmed summary for a stream/window index.
func (s *Store) Get(k model.StreamKey, idx int64) (model.WindowSummary, bool) {
	return model.WindowSummary{}, false
}

// List returns confirmed summaries of a stream in window-index order.
func (s *Store) List(k model.StreamKey) []model.WindowSummary { return nil }

// Streams returns all known stream keys.
func (s *Store) Streams() []model.StreamKey { return nil }

// LatestWatermark... (kept minimal: summaries carry their own times)
var _ = time.Time{}
