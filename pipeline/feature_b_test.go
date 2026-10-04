package pipeline

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// alertSig 给出告警序列的可读签名，用于断言与失败时打印判定依据。
func alertSig(alerts []Alert) string {
	s := ""
	for i, a := range alerts {
		s += fmt.Sprintf("  #%d rule=%s kind=%s window=[%s,%s) observed=%.4f threshold=%.4f\n",
			i, a.RuleID, a.Kind,
			a.Window.Start.Format("15:04:05"), a.Window.End.Format("15:04:05"),
			a.Observed, a.Threshold)
	}
	return s
}

// minuteEvent 构造 minute 偏移分钟处的一条成功事件。
func minuteEvent(id, job, dataset string, base time.Time, minute int, rows int64) Event {
	return Event{
		EventID:   id,
		Job:       job,
		Dataset:   dataset,
		Status:    StatusSuccess,
		EventTime: base.Add(time.Duration(minute)*time.Minute + 5*time.Second),
		Rows:      rows,
	}
}

// TestMultipleRulesFireSimultaneously：同一窗口多条规则同时越限时，
// 每条规则各自产出可区分（RuleID 不同）的告警；同一批输入重放产出完全一致的告警序列。
func TestMultipleRulesFireSimultaneously(t *testing.T) {
	base := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	evs := []Event{
		{EventID: "f1", Job: "j", Dataset: "d", Status: StatusFailed,
			EventTime: base.Add(2 * time.Second), Rows: 40},
		{EventID: "f2", Job: "j", Dataset: "d", Status: StatusFailed,
			EventTime: base.Add(3 * time.Second), Rows: 40},
	}
	// 窗口指标：吞吐 80/60≈1.33/s，失败率 1.0。
	// 三条规则全部命中：tp-warn(1.33<10)、fr(1.0>0.5)、tp-crit(1.33<1000)。
	rules := []AlertRule{
		{RuleID: "tp-warn", Job: "j", Dataset: "d", MinThroughputPerSec: 10},
		{RuleID: "fr", Job: "j", Dataset: "d", MaxFailureRate: 0.5},
		{RuleID: "tp-crit", Job: "j", Dataset: "d", MinThroughputPerSec: 1000},
	}
	run := func() []Alert {
		now, _ := testClock()
		eng := NewEngine(Config{WindowSize: time.Minute, AllowedLateness: 0, Now: now})
		for _, r := range rules {
			eng.AddRule(r)
		}
		for _, ev := range evs {
			eng.Ingest(ev)
		}
		eng.AdvanceWatermark("j", base.Add(time.Minute))
		return eng.Alerts("j", "d")
	}

	alerts := run()
	if len(alerts) != 3 {
		failureMsg(t, fmt.Sprintf("expected 3 distinguishable alerts from 3 rules, got %d", len(alerts)),
			evs, alertSig(alerts))
	}
	// 产出顺序固定：规则注册顺序 × 告警类别固定顺序。
	want := []struct {
		rule string
		kind AlertKind
	}{
		{"tp-warn", AlertThroughput},
		{"fr", AlertFailure},
		{"tp-crit", AlertThroughput},
	}
	for i, w := range want {
		if alerts[i].RuleID != w.rule || alerts[i].Kind != w.kind {
			failureMsg(t, fmt.Sprintf("alert #%d should be rule=%s kind=%s", i, w.rule, w.kind),
				evs, alertSig(alerts))
		}
	}
	// 同一窗口、同一类别的两条告警（tp-warn / tp-crit）必须互相可区分。
	if alerts[0].RuleID == alerts[2].RuleID || alerts[0].Threshold == alerts[2].Threshold {
		failureMsg(t, "two throughput tiers must be distinguishable by rule id and threshold",
			evs, alertSig(alerts))
	}
	// 重放：同一批输入得到完全一致的告警序列。
	replay := run()
	if len(replay) != len(alerts) {
		failureMsg(t, "replay produced different alert count", evs, alertSig(replay))
	}
	for i := range alerts {
		if replay[i].RuleID != alerts[i].RuleID || replay[i].Kind != alerts[i].Kind ||
			!replay[i].Window.Start.Equal(alerts[i].Window.Start) ||
			replay[i].Observed != alerts[i].Observed {
			failureMsg(t, fmt.Sprintf("replay diverged at alert #%d", i), evs,
				"first:\n"+alertSig(alerts)+"replay:\n"+alertSig(replay))
		}
	}
}

// TestRuleIncidentsIndependent：同维度两档阈值（warn/crit）各自的持续抑制与恢复
// 互不干扰：warn 恢复不能清除 crit 的持续区间，反之亦然。
func TestRuleIncidentsIndependent(t *testing.T) {
	now, _ := testClock()
	eng := NewEngine(Config{WindowSize: time.Minute, AllowedLateness: 0, Now: now})
	eng.AddRule(AlertRule{RuleID: "warn", Job: "j", Dataset: "d", MinThroughputPerSec: 10})
	eng.AddRule(AlertRule{RuleID: "crit", Job: "j", Dataset: "d", MinThroughputPerSec: 100})

	base := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	// 吞吐：w0=1/s（双越限），w1=50/s（warn 恢复、crit 持续），w2=10/s（warn 不越限、crit 持续），
	// w3=1000/s（crit 恢复），w4=1/s（双双重新触发）。
	plans := []int64{60, 3000, 600, 60000, 60}
	var evs []Event
	for m, rows := range plans {
		ev := minuteEvent(fmt.Sprintf("m%d", m), "j", "d", base, m, rows)
		evs = append(evs, ev)
		eng.Ingest(ev)
		eng.AdvanceWatermark("j", base.Add(time.Duration(m+1)*time.Minute))
	}

	alerts := eng.Alerts("j", "d")
	type sig struct {
		rule   string
		minute int
	}
	var got []sig
	for _, a := range alerts {
		got = append(got, sig{a.RuleID, int(a.Window.Start.Sub(base) / time.Minute)})
	}
	want := []sig{{"warn", 0}, {"crit", 0}, {"warn", 4}, {"crit", 4}}
	if len(got) != len(want) {
		failureMsg(t, fmt.Sprintf("expected %d alerts, got %d", len(want), len(got)),
			evs, alertSig(alerts))
	}
	for i := range want {
		if got[i] != want[i] {
			failureMsg(t, fmt.Sprintf("alert #%d should be rule=%s at minute %d (suppression/recovery leaked across rules?)",
				i, want[i].rule, want[i].minute), evs, alertSig(alerts))
		}
	}
}

// TestIdleWindowBreaksIncident：空闲时间段被确认为空窗口（零值+Empty 标识），
// 空窗口本身不产生越限告警，但作为恢复点打断持续故障区间：空闲之后再次越限重新触发。
func TestIdleWindowBreaksIncident(t *testing.T) {
	now, _ := testClock()
	eng := NewEngine(Config{WindowSize: time.Minute, AllowedLateness: 0, Now: now})
	eng.AddRule(AlertRule{RuleID: "tp", Job: "j", Dataset: "d", MinThroughputPerSec: 10})

	base := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	var evs []Event
	put := func(minute int, rows int64) {
		ev := minuteEvent(fmt.Sprintf("m%d", minute), "j", "d", base, minute, rows)
		evs = append(evs, ev)
		eng.Ingest(ev)
	}
	// w0、w1 低吞吐（同一持续区间，只触发一次）；w2、w3 无任何事件（空闲）；
	// w4 再次低吞吐，必须重新触发。
	put(0, 100)
	eng.AdvanceWatermark("j", base.Add(1*time.Minute))
	put(1, 100)
	eng.AdvanceWatermark("j", base.Add(2*time.Minute))
	put(4, 100) // 摄入 w4 事件时水位自动越过 w2、w3，空闲窗口在此被确认
	eng.AdvanceWatermark("j", base.Add(5*time.Minute))

	views := eng.Windows("j", "d")
	if len(views) != 5 {
		failureMsg(t, fmt.Sprintf("expected 5 confirmed windows (incl. 2 idle), got %d", len(views)),
			evs, fmt.Sprintf("%+v", views))
	}
	// 空闲窗口：Empty=true、指标全零、已确认。
	for _, m := range []int{2, 3} {
		w := views[m]
		if w.State != WindowClosed || !w.Empty || w.Metrics.EventCount != 0 || w.Metrics.Rows != 0 {
			failureMsg(t, fmt.Sprintf("minute %d should be a confirmed empty zero-valued window", m),
				evs, fmt.Sprintf("%+v", w))
		}
	}
	// 有数据窗口不得被标记为空。
	for _, m := range []int{0, 1, 4} {
		if views[m].Empty {
			failureMsg(t, fmt.Sprintf("minute %d has data and must not be marked empty", m),
				evs, fmt.Sprintf("%+v", views[m]))
		}
	}

	alerts := eng.Alerts("j", "d")
	// 空窗口自身不产生告警（否则会有 w2/w3 的吞吐告警）；空闲打断区间 => w4 重新触发。
	if len(alerts) != 2 {
		failureMsg(t, fmt.Sprintf("expected 2 alerts (w0 fire, idle breaks, w4 refire), got %d", len(alerts)),
			evs, alertSig(alerts))
	}
	if len(alerts) == 2 {
		if !alerts[0].Window.Start.Equal(base) || !alerts[1].Window.Start.Equal(base.Add(4*time.Minute)) {
			failureMsg(t, "alerts should fire on minute 0 and minute 4 only", evs, alertSig(alerts))
		}
	}
}

// TestEmptyWindowVsZeroValueWindow：空窗口（无数据）与“有数据但数值为零”的窗口
// 明确区分：后者 Empty=false 且吞吐为 0 属真实越限，前者 Empty=true 不产生告警且作恢复点。
func TestEmptyWindowVsZeroValueWindow(t *testing.T) {
	now, _ := testClock()
	eng := NewEngine(Config{WindowSize: time.Minute, AllowedLateness: 0, Now: now})
	eng.AddRule(AlertRule{RuleID: "tp", Job: "j", Dataset: "d", MinThroughputPerSec: 1})

	base := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	var evs []Event
	put := func(minute int, rows int64) {
		ev := minuteEvent(fmt.Sprintf("m%d", minute), "j", "d", base, minute, rows)
		evs = append(evs, ev)
		eng.Ingest(ev)
	}
	// w0：有事件但 Rows=0 => 吞吐 0，真实越限，触发。
	// w1：空闲 => 空窗口，不告警，且打断持续区间。
	// w2：再次 Rows=0 => 重新触发。
	put(0, 0)
	eng.AdvanceWatermark("j", base.Add(1*time.Minute))
	put(2, 0)
	eng.AdvanceWatermark("j", base.Add(3*time.Minute))

	views := eng.Windows("j", "d")
	if len(views) != 3 {
		failureMsg(t, "expected 3 windows", evs, fmt.Sprintf("%+v", views))
	}
	if views[0].Empty || views[0].Metrics.EventCount != 1 || views[0].Metrics.Rows != 0 {
		failureMsg(t, "minute 0: data-with-zero-values window misclassified", evs, fmt.Sprintf("%+v", views[0]))
	}
	if !views[1].Empty || views[1].Metrics.EventCount != 0 {
		failureMsg(t, "minute 1: idle window must be empty with zero metrics", evs, fmt.Sprintf("%+v", views[1]))
	}
	alerts := eng.Alerts("j", "d")
	if len(alerts) != 2 ||
		!alerts[0].Window.Start.Equal(base) || !alerts[1].Window.Start.Equal(base.Add(2*time.Minute)) {
		failureMsg(t, "zero-throughput data windows should fire at minute 0 and 2; idle minute 1 must not fire",
			evs, alertSig(alerts))
	}
}

// TestRetentionEviction：已确认窗口保留规模可控；回收后迟到事件仍被拒绝（且原因可解释）、
// 进行中的告警持续区间与恢复/复发判断不受回收影响。
func TestRetentionEviction(t *testing.T) {
	now, _ := testClock()
	eng := NewEngine(Config{WindowSize: time.Minute, AllowedLateness: 0, MaxClosedWindows: 3, Now: now})
	eng.AddRule(AlertRule{RuleID: "tp", Job: "j", Dataset: "d", MinThroughputPerSec: 10})

	base := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	var evs []Event
	put := func(minute int, rows int64) *IngestResult {
		ev := minuteEvent(fmt.Sprintf("m%d", minute), "j", "d", base, minute, rows)
		evs = append(evs, ev)
		r := eng.Ingest(ev)
		eng.AdvanceWatermark("j", base.Add(time.Duration(minute+1)*time.Minute))
		return r
	}
	// w0..w7 持续低吞吐：同一持续故障区间，只在 w0 触发一次；
	// 期间最旧窗口不断被回收，不得影响抑制状态。
	for m := 0; m < 8; m++ {
		put(m, 100)
	}
	if got := eng.EvictedWindows("j", "d"); got != 5 {
		failureMsg(t, fmt.Sprintf("expected 5 evicted windows (8 closed, keep 3), got %d", got),
			evs, "")
	}
	if views := eng.Windows("j", "d"); len(views) != 3 {
		failureMsg(t, fmt.Sprintf("retention should keep exactly 3 closed windows, got %d", len(views)),
			evs, fmt.Sprintf("%+v", views))
	}
	if alerts := eng.Alerts("j", "d"); len(alerts) != 1 {
		failureMsg(t, "eviction must not disturb the ongoing incident (still suppressed)",
			evs, alertSig(alerts))
	}

	// 迟到事件落入已回收窗口：仍按 late_beyond_tolerance 拒绝，且详情可解释（提及回收）。
	stale := minuteEvent("stale-m0", "j", "d", base, 0, 999)
	evs = append(evs, stale)
	r := eng.Ingest(stale)
	if !r.Rejected || r.Reason != RejectLateBeyondTolerance {
		failureMsg(t, "late event into evicted window must be rejected", evs, fmt.Sprintf("%+v", r))
	}
	if got := r.Detail; !strings.Contains(got, "evicted") {
		failureMsg(t, "rejection detail should explain the window was evicted due to retention",
			evs, got)
	}
	// 回收簿记不被迟到事件改变。
	if got := eng.EvictedWindows("j", "d"); got != 5 {
		failureMsg(t, "evicted count changed after rejected late event", evs, fmt.Sprintf("%d", got))
	}

	// w8 高吞吐恢复，w9 再次低吞吐：恢复与复发判断在回收之后仍然正确。
	put(8, 60000)
	put(9, 100)
	alerts := eng.Alerts("j", "d")
	if len(alerts) != 2 || !alerts[1].Window.Start.Equal(base.Add(9*time.Minute)) {
		failureMsg(t, "recovery and re-fire after eviction broken", evs, alertSig(alerts))
	}
}

// TestScaleSteadyState：数据量与规则数上来之后，摄入/确认/回收在规模下保持正确：
// 保留窗口数恒定、回收数精确、告警序列可复现。
func TestScaleSteadyState(t *testing.T) {
	base := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	const windows = 2000
	const rules = 20

	build := func() *Engine {
		now, _ := testClock()
		eng := NewEngine(Config{WindowSize: time.Minute, AllowedLateness: 0, MaxClosedWindows: 64, Now: now})
		for i := 0; i < rules; i++ {
			eng.AddRule(AlertRule{
				RuleID:              fmt.Sprintf("r%02d", i),
				Job:                 "j",
				Dataset:             "d",
				MinThroughputPerSec: float64(10 + i), // 低吞吐窗口全部命中
			})
		}
		return eng
	}
	var evs []Event
	for m := 0; m < windows; m++ {
		evs = append(evs, minuteEvent(fmt.Sprintf("m%d", m), "j", "d", base, m, 60)) // 1/s，全部越限
	}
	run := func() []Alert {
		eng := build()
		for _, ev := range evs {
			eng.Ingest(ev)
		}
		eng.AdvanceWatermark("j", base.Add(time.Duration(windows)*time.Minute))
		if got := eng.EvictedWindows("j", "d"); got != windows-64 {
			failureMsg(t, fmt.Sprintf("expected %d evicted windows, got %d", windows-64, got), evs[:3], "")
		}
		if views := eng.Windows("j", "d"); len(views) != 64 {
			failureMsg(t, fmt.Sprintf("retention should keep 64 windows, got %d", len(views)), evs[:3], "")
		}
		return eng.Alerts("j", "d")
	}

	first := run()
	// 每条规则只在首个窗口触发一次（持续区间一直抑制到结束）。
	if len(first) != rules {
		failureMsg(t, fmt.Sprintf("expected %d alerts (one per rule), got %d", rules, len(first)),
			evs[:3], alertSig(first))
	}
	second := run()
	if len(first) != len(second) {
		failureMsg(t, "replay at scale diverged", evs[:3], "")
	}
	for i := range first {
		if first[i].RuleID != second[i].RuleID || !first[i].Window.Start.Equal(second[i].Window.Start) {
			failureMsg(t, fmt.Sprintf("replay diverged at alert #%d", i), evs[:3],
				"first:\n"+alertSig(first)+"\nsecond:\n"+alertSig(second))
		}
	}
}
