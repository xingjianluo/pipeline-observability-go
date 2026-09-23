package pipeline

import (
	"sort"
	"sync"
	"time"
)

// Config 控制窗口划分、水位推进与迟到容忍。
type Config struct {
	// WindowSize 滚动窗口长度，必须 > 0；为 0 时默认 1 分钟。
	WindowSize time.Duration
	// AllowedLateness 水位越过窗口结束时间后，仍允许乱序/迟到事件补入的额外宽限。
	// 事件时间 < watermark - AllowedLateness 时拒绝；
	// 某窗口在 watermark >= 窗口结束时间 + AllowedLateness 时确认（指标与告警结论冻结）。
	AllowedLateness time.Duration
	// Now 用于填充事件缺失的 IngestTime 以及作为窗口确认时间；默认 time.Now。
	Now func() time.Time
}

// RejectedEvent 记录被明确拒绝/单独归类的事件，保证不静默丢弃。
type RejectedEvent struct {
	Event      Event
	Reason     RejectReason
	Detail     string
	RejectedAt time.Time
}

type streamKey struct {
	job     string
	dataset string
}

// streamState 是单个 (Job, Dataset) 事件流的全部内部状态。
type streamState struct {
	watermark time.Time
	windows   []*Window // 按 Start 升序，包含开放与已确认窗口
	byStart   map[time.Time]*Window
	incidents map[AlertKind]*alertIncident
	alerts    []Alert
}

func newStreamState() *streamState {
	return &streamState{
		byStart:   make(map[time.Time]*Window),
		incidents: make(map[AlertKind]*alertIncident),
	}
}

// Engine 是线程安全的流水线可观测性引擎。
// 所有摄入、窗口查询、告警查询方法均可被多 goroutine 并发调用；
// 内部以单一互斥锁串行化状态变更，保证结论与到达顺序无关、可复现。
type Engine struct {
	cfg   Config
	rules []AlertRule
	mu    sync.Mutex

	streams  map[streamKey]*streamState
	rejected []RejectedEvent
}

// NewEngine 创建引擎。
func NewEngine(cfg Config) *Engine {
	if cfg.WindowSize <= 0 {
		cfg.WindowSize = time.Minute
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Engine{
		cfg:     cfg,
		streams: make(map[streamKey]*streamState),
	}
}

func (e *Engine) now() time.Time { return e.cfg.Now() }

// AddRule 注册一条阈值告警规则。
func (e *Engine) AddRule(rule AlertRule) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rules = append(e.rules, rule)
}

// stream 调用方须持锁。
func (e *Engine) stream(job, dataset string) *streamState {
	k := streamKey{job, dataset}
	st, ok := e.streams[k]
	if !ok {
		st = newStreamState()
		e.streams[k] = st
	}
	return st
}

// reject 调用方须持锁。
func (e *Engine) reject(ev Event, reason RejectReason, detail string) *IngestResult {
	e.rejected = append(e.rejected, RejectedEvent{
		Event: ev, Reason: reason, Detail: detail, RejectedAt: e.now(),
	})
	return &IngestResult{
		Accepted: false,
		Rejected: true,
		Reason:   reason,
		Detail:   detail,
	}
}

func validateEvent(ev Event) string {
	switch {
	case ev.EventID == "":
		return "empty EventID"
	case ev.Job == "":
		return "empty Job"
	case ev.Dataset == "":
		return "empty Dataset"
	case ev.EventTime.IsZero():
		return "zero EventTime"
	case ev.Status != StatusSuccess && ev.Status != StatusFailed:
		return "invalid Status: " + string(ev.Status)
	case ev.Rows < 0:
		return "negative Rows"
	default:
		return ""
	}
}

// Ingest 摄入一条事件，返回其明确去向（接受 / 重复 / 拒绝）。
func (e *Engine) Ingest(ev Event) *IngestResult {
	if bad := validateEvent(ev); bad != "" {
		e.mu.Lock()
		res := e.reject(ev, RejectInvalid, "event rejected ("+bad+")")
		e.mu.Unlock()
		return res
	}
	if ev.IngestTime.IsZero() {
		ev.IngestTime = e.now()
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	st := e.stream(ev.Job, ev.Dataset)

	// 推进事件时间水位：观测到的事件时间、事件携带的 WatermarkHint、
	// 显式 AdvanceWatermark 三者取最大。
	if ev.EventTime.After(st.watermark) {
		st.watermark = ev.EventTime
	}
	if ev.WatermarkHint.After(st.watermark) {
		st.watermark = ev.WatermarkHint
	}

	start := ev.EventTime.Truncate(e.cfg.WindowSize)
	end := start.Add(e.cfg.WindowSize)
	threshold := st.watermark.Add(-e.cfg.AllowedLateness)

	w, exists := st.byStart[start]
	if exists {
		// 重复投递优先判定：即使窗口已确认，重复事件也只能幂等忽略，绝不改写结论。
		if w.contains(ev.EventID) {
			return &IngestResult{Accepted: true, Duplicate: true,
				Detail: "duplicate event already ingested into window [" + start.Format(time.RFC3339) + "," + end.Format(time.RFC3339) + ")"}
		}
		if w.State == WindowClosed {
			detail := "event " + ev.EventID + " eventTime=" + ev.EventTime.Format(time.RFC3339Nano) +
				" is older than the watermark tolerance bound; window [" + start.Format(time.RFC3339) + "," +
				end.Format(time.RFC3339) + ") already closed at watermark " +
				st.watermark.Format(time.RFC3339Nano) + " (threshold eventTime < " +
				threshold.Format(time.RFC3339Nano) + ")"
			return e.reject(ev, RejectLateBeyondTolerance, detail)
		}
	} else {
		// 全新窗口：若事件时间已经晚于容忍下界，则它属于一个本应早已确认的窗口，拒绝。
		if ev.EventTime.Before(threshold) {
			detail := "event " + ev.EventID + " eventTime=" + ev.EventTime.Format(time.RFC3339Nano) +
				" falls beyond allowed lateness for an unobserved window [" +
				start.Format(time.RFC3339) + "," + end.Format(time.RFC3339) +
				"); watermark=" + st.watermark.Format(time.RFC3339Nano) +
				", threshold eventTime < " + threshold.Format(time.RFC3339Nano)
			return e.reject(ev, RejectLateBeyondTolerance, detail)
		}
		w = newWindow(WindowID{
			Job: ev.Job, Dataset: ev.Dataset, Kind: WindowTumbling,
			Start: start, End: end,
		})
		st.byStart[start] = w
		st.windows = append(st.windows, w)
		sort.Slice(st.windows, func(i, j int) bool { return st.windows[i].ID.Start.Before(st.windows[j].ID.Start) })
	}

	if !w.add(ev) {
		// 理论不可达（contains 已判定），保守幂等处理。
		return &IngestResult{Accepted: true, Duplicate: true, Detail: "duplicate event"}
	}

	closed := e.closeDueWindows(st, e.now())
	return &IngestResult{Accepted: true, ClosedWindows: closed}
}

// closeDueWindows 确认所有 watermark 已越过其结束时间+迟到宽限的开放窗口。
// 调用方须持锁。返回新确认窗口数；指标与告警结论在此一次性冻结。
func (e *Engine) closeDueWindows(st *streamState, at time.Time) int {
	closed := 0
	for _, w := range st.windows {
		if w.State == WindowOpen && !st.watermark.Before(w.ID.End.Add(e.cfg.AllowedLateness)) {
			w.finalize()
			evaluateAlerts(st, w, e.rules, at)
			closed++
		}
	}
	return closed
}

// AdvanceWatermark 显式推进某作业（其下所有数据集）的水位，
// 返回本次推进新确认窗口的只读视图。
func (e *Engine) AdvanceWatermark(job string, t time.Time) []WindowView {
	e.mu.Lock()
	defer e.mu.Unlock()
	at := e.now()
	var out []WindowView
	for k, st := range e.streams {
		if k.job != job {
			continue
		}
		if !t.After(st.watermark) {
			continue
		}
		st.watermark = t
		for _, w := range st.windows {
			if w.State == WindowOpen && !t.Before(w.ID.End.Add(e.cfg.AllowedLateness)) {
				w.finalize()
				evaluateAlerts(st, w, e.rules, at)
				out = append(out, w.view())
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ID.Dataset != out[j].ID.Dataset {
			return out[i].ID.Dataset < out[j].ID.Dataset
		}
		return out[i].ID.Start.Before(out[j].ID.Start)
	})
	return out
}

// Windows 返回某作业+数据集下所有窗口（含开放与已确认）的只读视图，按窗口起点升序。
func (e *Engine) Windows(job, dataset string) []WindowView {
	e.mu.Lock()
	defer e.mu.Unlock()
	st, ok := e.streams[streamKey{job, dataset}]
	if !ok {
		return nil
	}
	views := make([]WindowView, 0, len(st.windows))
	for _, w := range st.windows {
		views = append(views, w.view())
	}
	return views
}

// Window 按 ID 查询单个窗口。
func (e *Engine) Window(id WindowID) (WindowView, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	st, ok := e.streams[streamKey{id.Job, id.Dataset}]
	if !ok {
		return WindowView{}, false
	}
	w, ok := st.byStart[id.Start]
	if !ok || w.ID.End != id.End {
		return WindowView{}, false
	}
	return w.view(), true
}

// Watermark 返回某作业+数据集流的当前水位；未见过的流返回零时间。
func (e *Engine) Watermark(job string) time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	var wm time.Time
	for k, st := range e.streams {
		if k.job == job && st.watermark.After(wm) {
			wm = st.watermark
		}
	}
	return wm
}

// RejectedEvents 返回所有被拒绝/单独归类事件的副本。
func (e *Engine) RejectedEvents() []RejectedEvent {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]RejectedEvent, len(e.rejected))
	copy(out, e.rejected)
	return out
}

// Alerts 返回某作业+数据集已触发告警的副本（按触发时间升序，同时间按窗口起点）。
func (e *Engine) Alerts(job, dataset string) []Alert {
	e.mu.Lock()
	defer e.mu.Unlock()
	st, ok := e.streams[streamKey{job, dataset}]
	if !ok {
		return nil
	}
	out := make([]Alert, len(st.alerts))
	copy(out, st.alerts)
	return out
}
