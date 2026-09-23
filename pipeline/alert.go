package pipeline

import (
	"fmt"
	"strings"
	"time"
)

// AlertKind 告警类别。
type AlertKind string

const (
	// AlertFreshness 新鲜度告警：窗口产出滞后超过阈值。
	AlertFreshness AlertKind = "freshness_breach"
	// AlertLatency 处理延迟告警：窗口最大处理延迟超过阈值。
	AlertLatency AlertKind = "latency_breach"
	// AlertThroughput 吞吐告警：窗口吞吐低于阈值。
	AlertThroughput AlertKind = "throughput_breach"
	// AlertFailure 作业失败率告警：窗口失败率超过阈值。
	AlertFailure AlertKind = "failure_rate_breach"
)

// AlertRule 定义单个 (Job, Dataset) 维度上的阈值规则；零值字段表示不启用该类检查。
type AlertRule struct {
	Job                  string
	Dataset              string
	MaxFreshnessLag      time.Duration // >0 启用：新鲜度上限
	MaxProcessingLatency time.Duration // >0 启用：处理延迟上限
	MinThroughputPerSec  float64       // >0 启用：吞吐下限
	MaxFailureRate       float64       // >0 启用：失败率上限（0..1）
}

func (r AlertRule) matches(job, dataset string) bool {
	return r.Job == job && r.Dataset == dataset
}

// Alert 是一条可由原始事件复现的告警结论。
type Alert struct {
	Kind      AlertKind
	Job       string
	Dataset   string
	Window    WindowID
	Observed  float64  // 观测值（统一用数值表示，含义随 Kind；时长类单位为秒）
	Threshold float64  // 触发阈值（同上）
	Reason    string   // 人类可读的判定依据（含窗口、指标值、触发条件）
	EventIDs  []string // 参与该窗口计算的去重事件 ID，用于从原始事件复现
	FiredAt   time.Time
}

// alertIncident 跟踪同一异常的“一次持续故障区间”，用于去重抑制。
// 连续确认窗口中：首次越限触发告警并置 active；持续越限只抑制不重复触发；
// 任一窗口恢复（不再越限）即清除 active，之后再次越限可重新触发。
type alertIncident struct {
	kind      AlertKind
	active    bool
	lastAlert Alert
}

// breach 描述一次阈值越限的判定细节。
type breach struct {
	kind             AlertKind
	observed         float64
	threshold        float64
	observedDisplay  string
	thresholdDisplay string
	condition        string
}

// evaluateWindow 对单个窗口、单条规则计算全部越限项；未启用的检查不产生越限。
func evaluateWindow(w *Window, rule AlertRule) []breach {
	m := w.Metrics
	failureRate := 0.0
	if m.EventCount > 0 {
		failureRate = float64(m.FailedCount) / float64(m.EventCount)
	}
	var out []breach

	if rule.MaxFreshnessLag > 0 && m.FreshnessLag > rule.MaxFreshnessLag {
		out = append(out, breach{
			kind:             AlertFreshness,
			observed:         m.FreshnessLag.Seconds(),
			threshold:        rule.MaxFreshnessLag.Seconds(),
			observedDisplay:  m.FreshnessLag.String(),
			thresholdDisplay: rule.MaxFreshnessLag.String(),
			condition:        "freshness_lag > max_freshness_lag",
		})
	}
	if rule.MaxProcessingLatency > 0 && m.MaxProcessingLatency > rule.MaxProcessingLatency {
		out = append(out, breach{
			kind:             AlertLatency,
			observed:         m.MaxProcessingLatency.Seconds(),
			threshold:        rule.MaxProcessingLatency.Seconds(),
			observedDisplay:  m.MaxProcessingLatency.String(),
			thresholdDisplay: rule.MaxProcessingLatency.String(),
			condition:        "max_processing_latency > max_processing_latency",
		})
	}
	if rule.MinThroughputPerSec > 0 && m.ThroughputPerSec < rule.MinThroughputPerSec {
		out = append(out, breach{
			kind:             AlertThroughput,
			observed:         m.ThroughputPerSec,
			threshold:        rule.MinThroughputPerSec,
			observedDisplay:  fmt.Sprintf("%.4f/s", m.ThroughputPerSec),
			thresholdDisplay: fmt.Sprintf("%.4f/s", rule.MinThroughputPerSec),
			condition:        "throughput_per_sec < min_throughput_per_sec",
		})
	}
	if rule.MaxFailureRate > 0 && failureRate > rule.MaxFailureRate {
		out = append(out, breach{
			kind:             AlertFailure,
			observed:         failureRate,
			threshold:        rule.MaxFailureRate,
			observedDisplay:  fmt.Sprintf("%.4f", failureRate),
			thresholdDisplay: fmt.Sprintf("%.4f", rule.MaxFailureRate),
			condition:        "failure_rate > max_failure_rate",
		})
	}
	return out
}

// evaluateAlerts 在窗口确认时对匹配的规则做一次性阈值判定，并进行去重抑制。
// 调用方须持有 Engine 锁。
func evaluateAlerts(st *streamState, w *Window, rules []AlertRule, at time.Time) {
	var rule *AlertRule
	for i := range rules {
		if rules[i].matches(w.ID.Job, w.ID.Dataset) {
			rule = &rules[i]
			break
		}
	}

	breached := make(map[AlertKind]breach)
	if rule != nil {
		for _, b := range evaluateWindow(w, *rule) {
			breached[b.kind] = b
		}
	}

	// 对规则涉及的每一类告警更新状态：未越限即视为恢复（清除持续故障区间）。
	kinds := []AlertKind{AlertFreshness, AlertLatency, AlertThroughput, AlertFailure}
	for _, kind := range kinds {
		inc, ok := st.incidents[kind]
		if !ok {
			inc = &alertIncident{kind: kind}
			st.incidents[kind] = inc
		}
		b, bad := breached[kind]
		if !bad {
			// 恢复：下一次越限将重新触发。
			inc.active = false
			continue
		}
		if inc.active {
			// 同一持续故障区间：去重抑制，不重复触发。
			continue
		}
		ids := w.eventIDs()
		alert := Alert{
			Kind:      kind,
			Job:       w.ID.Job,
			Dataset:   w.ID.Dataset,
			Window:    w.ID,
			Observed:  b.observed,
			Threshold: b.threshold,
			Reason: fmt.Sprintf(
				"%s on %s/%s window [%s,%s): %s; observed=%s threshold=%s; events=%d (ids: %s)",
				kind, w.ID.Job, w.ID.Dataset,
				w.ID.Start.Format(time.RFC3339), w.ID.End.Format(time.RFC3339),
				b.condition, b.observedDisplay, b.thresholdDisplay,
				len(ids), strings.Join(ids, ",")),
			EventIDs: ids,
			FiredAt:  at,
		}
		inc.active = true
		inc.lastAlert = alert
		st.alerts = append(st.alerts, alert)
	}
}
