package metrics

import (
	"errors"
	"sort"
	"sync"

	"github.com/xingjianluo/pipeline-observability-go/pkg/model"
	"github.com/xingjianluo/pipeline-observability-go/pkg/window"
)

// ErrAlreadyFinalized is returned when a confirmed window is finalized again;
// the store keeps the first value and never overwrites history.
var ErrAlreadyFinalized = errors.New("metrics: window already finalized")

// Store keeps confirmed window summaries; values are append-only and never
// change once stored, so replay cannot rewrite history or regress numbers.
type Store struct {
	mu      sync.Mutex
	windows map[model.StreamKey]map[int64]model.WindowSummary
	spec    window.Spec
}

// New constructs a metrics Store for the given window spec.
func New(spec window.Spec) *Store {
	return &Store{
		windows: make(map[model.StreamKey]map[int64]model.WindowSummary),
		spec:    spec,
	}
}

// Finalize records an already confirmed window output. A second finalization
// of the same window returns ErrAlreadyFinalized and changes nothing.
func (s *Store) Finalize(out model.WindowOutput) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	byIdx, ok := s.windows[out.Key]
	if !ok {
		byIdx = make(map[int64]model.WindowSummary)
		s.windows[out.Key] = byIdx
	}
	if _, exists := byIdx[out.Window.Index]; exists {
		return ErrAlreadyFinalized
	}
	byIdx[out.Window.Index] = out.Summary
	return nil
}

// Get returns the confirmed summary for a stream/window index.
func (s *Store) Get(k model.StreamKey, idx int64) (model.WindowSummary, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if byIdx, ok := s.windows[k]; ok {
		v, found := byIdx[idx]
		return v, found
	}
	return model.WindowSummary{}, false
}

// List returns confirmed summaries of a stream in window-index order.
func (s *Store) List(k model.StreamKey) []model.WindowSummary {
	s.mu.Lock()
	defer s.mu.Unlock()
	byIdx := s.windows[k]
	if len(byIdx) == 0 {
		return nil
	}
	idxs := make([]int64, 0, len(byIdx))
	for idx := range byIdx {
		idxs = append(idxs, idx)
	}
	sort.Slice(idxs, func(i, j int) bool { return idxs[i] < idxs[j] })
	out := make([]model.WindowSummary, 0, len(idxs))
	for _, idx := range idxs {
		out = append(out, byIdx[idx])
	}
	return out
}

// Streams returns all known stream keys in stable (job, dataset) order.
func (s *Store) Streams() []model.StreamKey {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]model.StreamKey, 0, len(s.windows))
	for k := range s.windows {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].JobID != keys[j].JobID {
			return keys[i].JobID < keys[j].JobID
		}
		return keys[i].Dataset < keys[j].Dataset
	})
	return keys
}
