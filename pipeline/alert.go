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
//
// 同一 (Job, Dataset) 可注册多条规则（不同维度、或同一维度不同级别），
// 它们在同一窗口上各自独立判定、各自产生告警、各自维护持续故障区间，
// 互不影响。RuleID 用于在告警结论中区分是哪条规则触发的。
type AlertRule struct {
	// RuleID 规则标识，同一引擎内应唯一；为空时由引擎按注册顺序分配稳定兜底标识。
	RuleID               string
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

// ruleEntry 是引擎内部登记的规则：附带稳定唯一的判定键，
// 用于把持续故障区间归属到具体规则。
type ruleEntry struct {
	rule AlertRule
	key  string
}

// Alert 是一条可由原始事件复现的告警结论。
type Alert struct {
	Kind AlertKind
	// RuleID 触发该告警的规则标识（多规则并存时据此互相区分）。
	RuleID    string
	Job       string
	Dataset   string
	Window    WindowID
	Observed  float64  // 观测值（统一用数值表示，含义随 Kind；时长类单位为秒）
	Threshold float64  // 触发阈值（同上）
	Reason    string   // 人类可读的判定依据（含窗口、指标值、触发条件）
	EventIDs  []string // 参与该窗口计算的去重事件 ID，用于从原始事件复现
	FiredAt   time.Time
}

// incidentKey 标识一条独立的持续故障区间：按 (规则, 告警类别) 互相隔离。
type incidentKey struct {
	rule string
	kind AlertKind
}

// alertIncident 跟踪同一规则同一告警类别的“一次持续故障区间”，用于去重抑制。
// 连续确认窗口中：首次越限触发告警并置 active；持续越限只抑制不重复触发；
// 任一窗口恢复（不再越限，含空窗口）即清除 active，之后再次越限可重新触发。
// 不同规则、不同告警类别的 incident 互不干扰。
type alertIncident struct {
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

// alertKindOrder 固定告警类别的判定顺序，保证同一批输入下告警产出顺序稳定。
var alertKindOrder = []AlertKind{AlertFreshness, AlertLatency, AlertThroughput, AlertFailure}

// evaluateAlerts 在窗口确认时对全部匹配的规则做一次性阈值判定，并按规则独立去重抑制。
// 同一窗口内多条规则同时越限时，每条规则各自产出可区分的告警；
// 告警产出顺序固定为：窗口确认顺序 × 规则注册顺序 × 告警类别固定顺序，
// 因此同一批输入重放会得到完全一致的告警序列。
// 调用方须持有 Engine 锁。
func evaluateAlerts(st *streamState, w *Window, rules []ruleEntry, at time.Time) {
	// 空窗口（窗口期内无任何事件）：不评估任何越限——空闲本身不凭空产生告警；
	// 但空闲是明确的“无异常”信号，作为恢复点打断所有进行中的持续故障区间，
	// 使一次持续异常不能悄悄跨过空闲期继续抑制。
	if w.Empty {
		for _, inc := range st.incidents {
			inc.active = false
		}
		return
	}

	for _, re := range rules {
		if !re.rule.matches(w.ID.Job, w.ID.Dataset) {
			continue
		}
		breached := make(map[AlertKind]breach)
		for _, b := range evaluateWindow(w, re.rule) {
			breached[b.kind] = b
		}

		// 对该规则涉及的每一类告警更新状态：未越限即视为恢复（清除持续故障区间）。
		for _, kind := range alertKindOrder {
			key := incidentKey{rule: re.key, kind: kind}
			inc, ok := st.incidents[key]
			if !ok {
				inc = &alertIncident{}
				st.incidents[key] = inc
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
				RuleID:    re.key,
				Job:       w.ID.Job,
				Dataset:   w.ID.Dataset,
				Window:    w.ID,
				Observed:  b.observed,
				Threshold: b.threshold,
				Reason: fmt.Sprintf(
					"%s on %s/%s window [%s,%s) rule=%s: %s; observed=%s threshold=%s; events=%d (ids: %s)",
					kind, w.ID.Job, w.ID.Dataset,
					w.ID.Start.Format(time.RFC3339), w.ID.End.Format(time.RFC3339),
					re.key,
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
}
