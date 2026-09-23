package alert

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/xingjianluo/pipeline-observability-go/pkg/model"
)

// Reason distinguishes alert failure causes.
type Reason string

const (
	ReasonFreshness  Reason = "FRESHNESS_BREACH"           // freshness lag above threshold
	ReasonLatency    Reason = "REPORT_LATENCY_BREACH"      // max report latency above threshold
	ReasonThroughput Reason = "THROUGHPUT_BELOW_THRESHOLD" // throughput below threshold
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
func (r Rule) Matches(k model.StreamKey) bool {
	return r.JobID == k.JobID && r.Dataset == k.Dataset
}

func (r Rule) key() streamRuleKey { return streamRuleKey{jobID: r.JobID, dataset: r.Dataset} }

type streamRuleKey struct {
	jobID   string
	dataset string
}

// Alert is one firing conclusion, fully reproducible from the cited window.
type Alert struct {
	EpisodeID   string // identifies one continuous failure episode
	Reason      Reason
	JobID       string
	Dataset     string
	WindowStart time.Time
	WindowEnd   time.Time
	Observed    float64 // measured value in seconds, or records-per-second
	Threshold   float64 // configured threshold in the same unit
	Message     string
	EventIDs    []string // source events that produced the window summary
	FiredAt     time.Time
	RecoveredAt time.Time // zero while the episode is still active
}

// Active reports whether the episode is still firing.
func (a Alert) Active() bool { return a.RecoveredAt.IsZero() }

func (a Alert) String() string {
	return fmt.Sprintf(
		"ALERT episode=%s reason=%s stream=%s/%s window[%s..%s) observed=%.6f threshold=%.6f events=%v",
		a.EpisodeID, a.Reason, a.JobID, a.Dataset,
		a.WindowStart.Format(time.RFC3339), a.WindowEnd.Format(time.RFC3339),
		a.Observed, a.Threshold, a.EventIDs)
}

// Engine evaluates confirmed windows against rules and suppresses repeat
// alerts within a continuous failure episode per (rule, reason).
type Engine struct {
	mu      sync.Mutex
	rules   []Rule
	active  map[episodeKey]*episode // currently open episodes
	history []Alert                 // fired alerts in firing order (mutated copies added on recovery)
	seq     int
}

type episodeKey struct {
	jobID   string
	dataset string
	reason  Reason
}

type episode struct {
	alert      Alert
	lastBadIdx int64
}

// New constructs an Engine with the given rules.
func New(rules []Rule) *Engine {
	cp := make([]Rule, len(rules))
	copy(cp, rules)
	return &Engine{
		rules:  cp,
		active: make(map[episodeKey]*episode),
	}
}

// Evaluate runs all matching rules against one confirmed window.
//
// Confirmed windows arrive in strictly increasing index order per stream.
// For every enabled check:
//   - bad window with no open episode          -> a new Alert fires
//   - bad window continuing the open episode   -> suppressed (nil)
//   - good window while an episode is open     -> episode recovers (closes)
//
// A later bad window after recovery opens a new episode and fires again.
func (e *Engine) Evaluate(out model.WindowOutput) []Alert {
	e.mu.Lock()
	defer e.mu.Unlock()
	var fired []Alert
	idx := out.Window.Index
	for _, rule := range e.rulesFor(out.Key) {
		checks := []check{
			{
				reason:    ReasonFreshness,
				enabled:   rule.MaxFreshnessLag != nil,
				observed:  out.Summary.FreshnessLag.Seconds(),
				threshold: thresholdSeconds(rule.MaxFreshnessLag),
				breach:    rule.MaxFreshnessLag != nil && out.Summary.FreshnessLag > *rule.MaxFreshnessLag,
				unit:      "s",
				describe:  "freshness lag",
			},
			{
				reason:    ReasonLatency,
				enabled:   rule.MaxReportLatency != nil,
				observed:  out.Summary.MaxReportLatency.Seconds(),
				threshold: thresholdSeconds(rule.MaxReportLatency),
				breach:    rule.MaxReportLatency != nil && out.Summary.MaxReportLatency > *rule.MaxReportLatency,
				unit:      "s",
				describe:  "max report latency",
			},
			{
				reason:    ReasonThroughput,
				enabled:   rule.MinThroughputPerSec != nil,
				observed:  out.Summary.ThroughputPerSec,
				threshold: derefFloat(rule.MinThroughputPerSec),
				breach:    rule.MinThroughputPerSec != nil && out.Summary.ThroughputPerSec < *rule.MinThroughputPerSec,
				unit:      "rec/s",
				describe:  "throughput",
			},
		}
		for _, ch := range checks {
			if !ch.enabled {
				continue
			}
			k := episodeKey{jobID: out.Key.JobID, dataset: out.Key.Dataset, reason: ch.reason}
			ep := e.active[k]
			switch {
			case ch.breach && ep == nil:
				a := e.newAlert(k, idx, out, ch)
				e.active[k] = &episode{alert: a, lastBadIdx: idx}
				fired = append(fired, a)
			case ch.breach && ep != nil:
				// Same continuous failure: confirm adjacency and suppress.
				ep.lastBadIdx = idx
			case !ch.breach && ep != nil:
				// First healthy window after the breach closes the episode.
				recovered := ep.alert
				recovered.RecoveredAt = out.Confirmed
				delete(e.active, k)
				e.appendRecovered(recovered)
			}
		}
	}
	return fired
}

type check struct {
	reason    Reason
	enabled   bool
	observed  float64
	threshold float64
	breach    bool
	unit      string
	describe  string
}

// ActiveAlerts returns episodes still firing (not recovered).
func (e *Engine) ActiveAlerts() []Alert {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Alert, 0, len(e.active))
	for _, ep := range e.active {
		out = append(out, ep.alert)
	}
	return out
}

// Episodes returns every episode (recovered and still active) in a
// deterministic order: by firing time, then reason. Recovered episodes carry
// their recovery timestamp; active ones have a zero RecoveredAt.
func (e *Engine) Episodes() []Alert {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Alert, 0, len(e.history)+len(e.active))
	out = append(out, e.history...)
	for _, ep := range e.active {
		out = append(out, ep.alert)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].FiredAt.Equal(out[j].FiredAt) {
			return out[i].FiredAt.Before(out[j].FiredAt)
		}
		if out[i].WindowStart.Equal(out[j].WindowStart) {
			return out[i].Reason < out[j].Reason
		}
		return out[i].WindowStart.Before(out[j].WindowStart)
	})
	return out
}

func (e *Engine) rulesFor(k model.StreamKey) []Rule {
	var out []Rule
	for _, r := range e.rules {
		if r.Matches(k) {
			out = append(out, r)
		}
	}
	return out
}

func (e *Engine) newAlert(k episodeKey, idx int64, out model.WindowOutput, ch check) Alert {
	e.seq++
	ids := append([]string(nil), out.EventIDs...)
	a := Alert{
		EpisodeID:   fmt.Sprintf("%s/%s#%s-%d", k.jobID, k.dataset, k.reason, e.seq),
		Reason:      ch.reason,
		JobID:       k.jobID,
		Dataset:     k.dataset,
		WindowStart: out.Window.Start,
		WindowEnd:   out.Window.End,
		Observed:    ch.observed,
		Threshold:   ch.threshold,
		EventIDs:    ids,
		FiredAt:     out.Confirmed,
		Message: fmt.Sprintf("%s %.6f%s breaches threshold %.6f%s in window [%s,%s); reproduce from events %v",
			ch.describe, ch.observed, ch.unit, ch.threshold, ch.unit,
			out.Window.Start.Format(time.RFC3339), out.Window.End.Format(time.RFC3339), ids),
	}
	return a
}

func (e *Engine) appendRecovered(a Alert) { e.history = append(e.history, a) }

func thresholdSeconds(d *time.Duration) float64 {
	if d == nil {
		return 0
	}
	return d.Seconds()
}

func derefFloat(f *float64) float64 {
	if f == nil {
		return 0
	}
	return *f
}
