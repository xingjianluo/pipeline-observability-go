package pipeline

import "time"

// WindowKind 标识窗口类型。
type WindowKind string

const (
	WindowTumbling WindowKind = "tumbling"
)

// WindowID 唯一标识一个已归属的时间窗口。
type WindowID struct {
	Job     string
	Dataset string
	Kind    WindowKind
	Start   time.Time // 含
	End     time.Time // 不含
}

// WindowState 标识窗口生命周期。
type WindowState string

const (
	// WindowOpen 窗口开放中，仍可接收事件（含迟到但在容忍范围内的事件）。
	WindowOpen WindowState = "open"
	// WindowClosed 窗口已确认（关闭），指标与告警结论冻结，永不再变。
	WindowClosed WindowState = "closed"
)

// Metrics 是单个窗口上的观测指标快照。
//
// 指标含义：
//   - EventCount：窗口内去重后的事件数。
//   - SuccessCount / FailedCount：按状态分类计数。
//   - Rows：去重事件的行数总和（吞吐）。
//   - ThroughputPerSec：Rows / 窗口秒长。
//   - FreshnessLag：窗口应产出时间（Window.End）到“最晚成功事件摄入时间”的延迟；
//     没有成功事件时取窗口内最晚摄入时间；早于应产出时间记为 0。
//   - MaxProcessingLatency：窗口内 max(IngestTime - EventTime)，反映处理延迟，最小为 0。
//   - SuccessRate：成功事件占比（0..1）。
type Metrics struct {
	EventCount           int64
	SuccessCount         int64
	FailedCount          int64
	Rows                 int64
	ThroughputPerSec     float64
	FreshnessLag         time.Duration
	MaxProcessingLatency time.Duration
	SuccessRate          float64
}

// Window 持有单个窗口的聚合状态。非线程安全，由 Engine 加锁保护。
type Window struct {
	ID      WindowID
	State   WindowState
	Metrics Metrics

	events []Event
	seen   map[string]struct{}
}

// WindowView 是窗口状态的只读视图（引擎对外返回，避免外部修改内部状态）。
type WindowView struct {
	ID      WindowID
	State   WindowState
	Metrics Metrics
}

func newWindow(id WindowID) *Window {
	return &Window{
		ID:    id,
		State: WindowOpen,
		seen:  make(map[string]struct{}),
	}
}

func (w *Window) view() WindowView { return WindowView{ID: w.ID, State: w.State, Metrics: w.Metrics} }

// add 将事件纳入窗口聚合。返回 false 表示重复事件（EventID 已见过），被幂等忽略。
// 只允许在 WindowOpen 状态下调用。
func (w *Window) add(ev Event) bool {
	if _, ok := w.seen[ev.EventID]; ok {
		return false
	}
	w.seen[ev.EventID] = struct{}{}
	w.events = append(w.events, ev)
	return true
}

// contains 报告窗口是否已接收过指定 EventID（即使窗口已关闭也可查，用于重复投递判定）。
func (w *Window) contains(eventID string) bool {
	_, ok := w.seen[eventID]
	return ok
}

// eventIDs 返回参与本窗口计算的去重事件 ID（按摄入顺序），用于告警复现。
func (w *Window) eventIDs() []string {
	ids := make([]string, 0, len(w.events))
	for _, ev := range w.events {
		ids = append(ids, ev.EventID)
	}
	return ids
}

// finalize 确认窗口：从去重后的事件一次性计算全部指标并冻结状态。
// 幂等：重复调用不会改变结果。
func (w *Window) finalize() {
	if w.State == WindowClosed {
		return
	}
	m := Metrics{EventCount: int64(len(w.events))}

	var latestSuccessIngest time.Time
	var latestIngest time.Time
	for _, ev := range w.events {
		switch ev.Status {
		case StatusSuccess:
			m.SuccessCount++
			if ev.IngestTime.After(latestSuccessIngest) {
				latestSuccessIngest = ev.IngestTime
			}
		case StatusFailed:
			m.FailedCount++
		}
		m.Rows += ev.Rows
		if lag := ev.IngestTime.Sub(ev.EventTime); lag > m.MaxProcessingLatency {
			m.MaxProcessingLatency = lag
		}
		if ev.IngestTime.After(latestIngest) {
			latestIngest = ev.IngestTime
		}
	}

	// 新鲜度：优先以最晚成功产出的摄入时刻为准；无成功事件则退化为最晚摄入时刻。
	freshnessAnchor := latestSuccessIngest
	if freshnessAnchor.IsZero() {
		freshnessAnchor = latestIngest
	}
	if !freshnessAnchor.IsZero() {
		if lag := freshnessAnchor.Sub(w.ID.End); lag > 0 {
			m.FreshnessLag = lag
		}
	}

	if secs := w.ID.End.Sub(w.ID.Start).Seconds(); secs > 0 {
		m.ThroughputPerSec = float64(m.Rows) / secs
	}
	if m.EventCount > 0 {
		m.SuccessRate = float64(m.SuccessCount) / float64(m.EventCount)
	}

	w.Metrics = m
	w.State = WindowClosed
}
