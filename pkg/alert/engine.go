package alert

import (
	"sync"
	"time"

	"github.com/xingjianluo/pipeline-observability-go/pkg/model"
)

// Reason distinguishes alert failure causes.
type Reason string

const (
	ReasonFreshness  Reason = "FRESHNESS_BREACH" // freshness lag above threshold
	ReasonLatency    Reason = "REPORT_LATENCY_BREACH"
	ReasonThroughput Reason = "THROUGHPUT_BELOW_THRESHOLD"
)

// Rule is one threshold rule for one stream. nil pointers disable that check.
type Rule struct {
	JobID               string
	Dataset             string
	MaxFreshnessLag     *time.Duration
	MaxReportLatency    *time.Duration
	MinThroughputPerSec *float64
}

// Matches reports whether this rule governs the given stream.
func (r Rule) Matches(k model.StreamKey) bool { return false }

// Alert is one firing conclusion, fully reproducible from the cited window.
type Alert struct {
	EpisodeID   string // identifies one continuous failure episode
	Reason      Reason
	JobID       string
	Dataset     string
	WindowStart time.Time
	WindowEnd   time.Time
	Observed    float64 // measured value (seconds / records-per-second)
	Threshold   float64 // configured threshold
	Message     string
	EventIDs    []string // source events that produced the window summary
	FiredAt     time.Time
	RecoveredAt time.Time
}

// Engine evaluates confirmed windows against rules and suppresses repeat
// alerts within a continuous failure episode per (rule, reason).
type Engine struct {
	mu     sync.Mutex
	rules  []Rule
	active map[episodeKey]*episode
}

type episodeKey struct {
	jobID   string
	dataset string
	reason  Reason
}

type episode struct {
	id          string
	alert       Alert
	lastBadIdx  int64
	consecutive int
}

// New constructs an Engine with the given rules.
func New(rules []Rule) *Engine { return nil }

// Evaluate runs all matching rules against one confirmed window.
// A new alert is emitted only when a failure episode starts; windows that
// continue the same episode return nothing; recovery closes the episode.
func (e *Engine) Evaluate(out model.WindowOutput) []Alert { return nil }

// ActiveAlerts returns episodes still firing (not recovered).
func (e *Engine) ActiveAlerts() []Alert { return nil }

// Episodes returns the full episode history (fired + recovered) in order.
func (e *Engine) Episodes() []Alert { return nil }
