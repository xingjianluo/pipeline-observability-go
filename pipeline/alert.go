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
// 同一个 (Job, Dataset) 流可以同时挂多条规则（含同一维度的不同档位）；
// RuleID 在该流范围内唯一标识一条规则，用于区分同一窗口上不同规则的告警结论，
// 并让每条规则的持续抑制/恢复状态彼此独立。RuleID 为空时由引擎在注册时自动分配。
type AlertRule struct {
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

// Alert 是一条可由原始事件复现的告警结论。
type Alert struct {
	Kind      AlertKind
	RuleID    string // 命中的规则；与 (Kind) 共同区分同一窗口上多条规则的结论
	Job       string
	Dataset   string
	Window    WindowID
	Observed  float64  // 观测值（统一用数值表示，含义随 Kind；时长类单位为秒）
	Threshold float64  // 触发阈值（同上）
	Reason    string   // 人类可读的判定依据（含规则、窗口、指标值、触发条件）
	EventIDs  []string // 参与该窗口计算的去重事件 ID，用于从原始事件复现
	FiredAt   time.Time
}

// ruleKey 标识一个流内的一条规则。
type ruleKey struct {
	job     string
	dataset string
	ruleID  string
}

// incidentKey 标识一个“规则 × 告警维度”的独立持续故障区间。
// 多条规则（即使 AlertKind 相同）的抑制与恢复状态互不干扰。
type incidentKey struct {
	ruleID string
	kind   AlertKind
}

// alertIncident 跟踪同一规则、同一告警维度的“一次持续故障区间”，用于去重抑制。
// 连续确认窗口中：首次越限触发告警并置 active；持续越限只抑制不重复触发；
// 任一已确认窗口不再越限（含空闲空窗）即清除 active，之后再次越限可重新触发。
// 不同规则、不同 AlertKind 各自持有独立 incident，互不干扰。
type alertIncident struct {
	active    bool
	lastAlert Alert
}

// alertKinds 固定评估顺序，保证同批输入下告警结论的产出顺序稳定、可重放。
var alertKinds = []AlertKind{AlertFreshness, AlertLatency, AlertThroughput, AlertFailure}

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

// evaluateAlerts 在窗口确认时对该流挂载的全部规则做一次性阈值判定，并按
// “规则 × 告警维度”独立进行去重抑制。调用方须持有 Engine 锁。
//
// 调用方传入的 rules 必须已经是与该窗口 (Job, Dataset) 匹配的规则（按注册顺序）。
//
// 规则：
//  1. 多条规则按注册顺序评估；同一规则内按 alertKinds 固定顺序评估，
//     因而同一批输入产出的告警顺序确定、可由原始事件重放复现；
//  2. 同一窗口可同时产出多条、多维度告警，结论用 (RuleID, AlertKind) 互相区分；
//  3. 每条规则、每个维度的持续抑制/恢复状态独立，互不影响；
//  4. 空闲空窗（w.Idle）不会凭空产生越限告警，但它是一个已确认窗口：
//     所有规则的所有维度在此视为恢复点，打断进行中的故障区间。
func evaluateAlerts(st *streamState, w *Window, rules []AlertRule, at time.Time) {
	ids := w.eventIDs()

	for ri := range rules {
		rule := rules[ri]
		// 空闲窗口：无数据，不产生任何越限，但作为恢复点重置该规则的所有维度。
		breached := make(map[AlertKind]breach)
		if !w.Idle {
			for _, b := range evaluateWindow(w, rule) {
				breached[b.kind] = b
			}
		}

		for _, kind := range alertKinds {
			key := incidentKey{ruleID: rule.RuleID, kind: kind}
			inc, ok := st.incidents[key]
			if !ok {
				inc = &alertIncident{}
				st.incidents[key] = inc
			}

			b, bad := breached[kind]
			if !bad {
				// 恢复（未启用该维度、本窗口未越限，或空闲空窗）：下一次越限将重新触发。
				inc.active = false
				continue
			}
			if inc.active {
				// 同一持续故障区间：去重抑制，不重复触发。
				continue
			}
			alert := Alert{
				Kind:      kind,
				RuleID:    rule.RuleID,
				Job:       w.ID.Job,
				Dataset:   w.ID.Dataset,
				Window:    w.ID,
				Observed:  b.observed,
				Threshold: b.threshold,
				Reason: fmt.Sprintf(
					"%s on %s/%s window [%s,%s) rule=%s: %s; observed=%s threshold=%s; events=%d (ids: %s)",
					kind, w.ID.Job, w.ID.Dataset,
					w.ID.Start.Format(time.RFC3339), w.ID.End.Format(time.RFC3339),
					rule.RuleID, b.condition, b.observedDisplay, b.thresholdDisplay,
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
