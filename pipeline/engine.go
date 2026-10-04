package pipeline

import (
	"fmt"
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
	// Retention 限制每个 (Job, Dataset) 流最多保留多少个已确认窗口；<=0 表示不限制。
	// 超出的最旧已确认窗口被回收（回收前其指标与告警结论早已冻结）：
	// 回收后属于这些窗口的迟到事件以 window_reclaimed 明确拒绝、不回写，
	// 进行中的告警区间状态不受回收影响。回收数量可通过 Engine.RetentionStats 查询。
	Retention int
}

// RetentionStats 报告窗口保留与回收情况（可解释的容量管理）。
type RetentionStats struct {
	// ReclaimedWindows 自引擎创建以来因超出保留容量被回收的窗口总数（跨所有流）。
	ReclaimedWindows int64
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
	// 已观测到的最早窗口起点：空闲填充只会补齐首个观测窗口之后的空窗，
	// 不会凭空捏造流开始之前的历史。
	firstObservedStart time.Time
	hasObserved        bool
	// fillCursor 是下一个待尝试空闲填充的窗口起点；每个起点在引擎生命周期内至多访问一次。
	fillCursor time.Time
	// closedRetained 是当前仍保留（未回收）的已确认窗口数；已确认窗口恒为 windows 前缀。
	closedRetained int
	// reclaimCutoff：已回收窗口的最晚结束时间（不含）；事件时间早于它即属已回收区间。
	reclaimCutoff time.Time
	reclaimed     int64 // 本流已回收窗口数
	incidents     map[incidentKey]*alertIncident
	alerts        []Alert
}

func newStreamState() *streamState {
	return &streamState{
		byStart:   make(map[time.Time]*Window),
		incidents: make(map[incidentKey]*alertIncident),
	}
}

// Engine 是线程安全的流水线可观测性引擎。
// 所有摄入、窗口查询、告警查询方法均可被多 goroutine 并发调用；
// 内部以单一互斥锁串行化状态变更，保证结论与到达顺序无关、可复现。
type Engine struct {
	cfg   Config
	rules []AlertRule
	// ruleIndex 按流索引规则在 rules 中的下标，数据量与规则数增大时避免全表扫描。
	ruleIndex map[streamKey][]int
	mu        sync.Mutex

	streams          map[streamKey]*streamState
	rejected         []RejectedEvent
	reclaimedWindows int64 // 跨所有流的回收窗口总数
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
		cfg:       cfg,
		rules:     nil,
		ruleIndex: make(map[streamKey][]int),
		streams:   make(map[streamKey]*streamState),
	}
}

func (e *Engine) now() time.Time { return e.cfg.Now() }

// AddRule 注册一条阈值告警规则。同一个 (Job, Dataset) 可挂多条规则。
// RuleID 为空时自动分配（形如 rule-1，全局唯一）；同一流内 RuleID 重复时
// 后注册的规则替换先注册的规则（保持原有注册位置，评估顺序稳定）。
func (e *Engine) AddRule(rule AlertRule) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if rule.RuleID == "" {
		rule.RuleID = fmt.Sprintf("rule-%d", len(e.rules)+1)
	}
	k := streamKey{job: rule.Job, dataset: rule.Dataset}
	idxs := e.ruleIndex[k]
	for pos, idx := range idxs {
		if e.rules[idx].RuleID == rule.RuleID {
			e.rules[idx] = rule
			return
		}
		_ = pos
	}
	e.ruleIndex[k] = append(idxs, len(e.rules))
	e.rules = append(e.rules, rule)
}

// matchingRules 调用方须持锁。返回与指定流匹配的规则（按注册顺序）。
func (e *Engine) matchingRules(job, dataset string) []AlertRule {
	idxs := e.ruleIndex[streamKey{job: job, dataset: dataset}]
	out := make([]AlertRule, 0, len(idxs))
	for _, idx := range idxs {
		out = append(out, e.rules[idx])
	}
	return out
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
		// 归属窗口已不在保留集合：先判定为“已回收”，给出可解释原因，绝不回写。
		if !st.reclaimCutoff.IsZero() && ev.EventTime.Before(st.reclaimCutoff) {
			detail := "event " + ev.EventID + " eventTime=" + ev.EventTime.Format(time.RFC3339Nano) +
				" belongs to window [" + start.Format(time.RFC3339) + "," + end.Format(time.RFC3339) +
				") which was already confirmed and later reclaimed by retention (cutoff=" +
				st.reclaimCutoff.Format(time.RFC3339Nano) + "); reclaimed windows never accept late events"
			return e.reject(ev, RejectWindowReclaimed, detail)
		}
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
		if !st.hasObserved {
			// 首个观测窗口之前不捏造历史；空闲填充从首个观测窗口自身开始。
			st.firstObservedStart = start
			st.fillCursor = start
			st.hasObserved = true
		} else if start.Before(st.firstObservedStart) {
			// 容忍内迟到事件带来更早的观测窗口：它此刻尚未到确认点，
			// 其与旧首窗之间的空档也尚未被填充，回退游标保证它们到期时被补齐。
			st.firstObservedStart = start
			st.fillCursor = start
		}
		st.byStart[start] = w
		pos := sort.Search(len(st.windows), func(i int) bool {
			return !st.windows[i].ID.Start.Before(start)
		})
		st.windows = append(st.windows, nil)
		copy(st.windows[pos+1:], st.windows[pos:])
		st.windows[pos] = w
	}

	if !w.add(ev) {
		// 理论不可达（contains 已判定），保守幂等处理。
		return &IngestResult{Accepted: true, Duplicate: true, Detail: "duplicate event"}
	}

	closed := e.closeDueWindows(ev.Job, ev.Dataset, st, e.now())
	return &IngestResult{Accepted: true, ClosedWindows: closed}
}

// materializeIdleWindows 把首个观测窗口之后、确认点之前“真实经过但没有任何
// 事件上报”的时间格补齐为空闲空窗。调用方须持锁。返回新补出的窗口数。
//
// 确认点：start + WindowSize + AllowedLateness <= watermark。
// 每个起点在引擎生命周期内至多被填充一次（fillCursor 单调推进），总成本均摊 O(窗口数)。
func (e *Engine) materializeIdleWindows(job, dataset string, st *streamState) int {
	if !st.hasObserved {
		return 0
	}
	created := 0
	size := e.cfg.WindowSize
	dueEnd := st.watermark.Add(-e.cfg.AllowedLateness) // 起点 end <= dueEnd 的窗口已到期
	cursor := st.fillCursor
	for !cursor.Add(size).After(dueEnd) {
		if _, exists := st.byStart[cursor]; !exists {
			idle := newWindow(WindowID{
				Job: job, Dataset: dataset, Kind: WindowTumbling,
				Start: cursor,
				End:   cursor.Add(size),
			})
			idle.Idle = true
			st.byStart[cursor] = idle
			pos := sort.Search(len(st.windows), func(i int) bool {
				return !st.windows[i].ID.Start.Before(cursor)
			})
			st.windows = append(st.windows, nil)
			copy(st.windows[pos+1:], st.windows[pos:])
			st.windows[pos] = idle
			created++
		}
		cursor = cursor.Add(size)
	}
	st.fillCursor = cursor
	return created
}

// closeCycle 执行一次完整的确认周期：空闲填充 → 确认到期窗口 → 回收超容量旧窗口。
// 调用方须持锁。返回本次新确认窗口（按起点升序，含空闲空窗）。
func (e *Engine) closeCycle(job, dataset string, st *streamState, at time.Time) []*Window {
	e.materializeIdleWindows(job, dataset, st)

	var newlyClosed []*Window
	dueEnd := st.watermark.Add(-e.cfg.AllowedLateness)
	for _, w := range st.windows {
		if w.State == WindowOpen && !w.ID.End.After(dueEnd) {
			w.finalize()
			st.closedRetained++
			evaluateAlerts(st, w, e.matchingRules(job, dataset), at)
			newlyClosed = append(newlyClosed, w)
		}
	}
	e.reclaimExcessWindows(st)
	return newlyClosed
}

// reclaimExcessWindows 回收超出 Retention 的最旧已确认窗口。调用方须持锁。
//
// 已确认窗口恒为 st.windows 的前缀；回收只删除前缀，不触碰开放窗口与告警区间状态。
func (e *Engine) reclaimExcessWindows(st *streamState) {
	if e.cfg.Retention <= 0 {
		return
	}
	for st.closedRetained > e.cfg.Retention {
		w := st.windows[0]
		st.windows = st.windows[1:]
		delete(st.byStart, w.ID.Start)
		st.closedRetained--
		st.reclaimed++
		e.reclaimedWindows++
		// cutoff 为被回收区间的最晚结束时间：事件时间早于它即落在已回收窗口内。
		st.reclaimCutoff = w.ID.End
	}
}

// closeDueWindows 确认所有 watermark 已越过其结束时间+迟到宽限的开放窗口。
// 调用方须持锁。返回新确认窗口数；指标与告警结论在此一次性冻结。
func (e *Engine) closeDueWindows(job, dataset string, st *streamState, at time.Time) int {
	return len(e.closeCycle(job, dataset, st, at))
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
		// 统一确认周期：先补齐空闲空窗，再按起点升序确认（空窗同样打断进行中的告警区间）。
		for _, w := range e.closeCycle(k.job, k.dataset, st, at) {
			out = append(out, w.view())
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

// RetentionStats 返回窗口保留/回收统计（跨所有流的累计回收窗口数）。
func (e *Engine) RetentionStats() RetentionStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	return RetentionStats{ReclaimedWindows: e.reclaimedWindows}
}

// ReclaimedWindows 返回指定流因超出保留容量被回收的窗口数；未见过的流返回 0。
func (e *Engine) ReclaimedWindows(job, dataset string) int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	st, ok := e.streams[streamKey{job, dataset}]
	if !ok {
		return 0
	}
	return st.reclaimed
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
