package pipeline

import (
	"fmt"
	"testing"
	"time"
)

// findView 按窗口起点查视图。
func findView(eng *Engine, job, dataset string, start time.Time) (WindowView, bool) {
	for _, v := range eng.Windows(job, dataset) {
		if v.ID.Start.Equal(start) {
			return v, true
		}
	}
	return WindowView{}, false
}

// TestIdleWindowMaterializedAsClosedEmpty：没有事件上报的时间段在水位越过确认点后，
// 必须成为状态为 closed 的空窗口，指标全为零，并以 Idle=true 与“有数据但零值”窗口区分。
func TestIdleWindowMaterializedAsClosedEmpty(t *testing.T) {
	now, _ := testClock()
	eng := NewEngine(Config{WindowSize: time.Minute, AllowedLateness: 0, Now: now})
	base := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)

	// m0：有事件但 Rows=0 => 吞吐为零的“数据零值窗口”（Idle=false）。
	m0 := Event{EventID: "m0", Job: "j", Dataset: "d", Status: StatusSuccess,
		EventTime: base.Add(5 * time.Second), Rows: 0}
	// m1：完全没有事件（空闲）。
	// m2：有事件。
	m2 := Event{EventID: "m2", Job: "j", Dataset: "d", Status: StatusSuccess,
		EventTime: base.Add(2*time.Minute + 5*time.Second), Rows: 600}
	var all []Event
	for _, ev := range []Event{m0, m2} {
		all = append(all, ev)
		if r := eng.Ingest(ev); r.Rejected {
			failureMsg(t, "unexpected rejection: "+r.Detail, all, "")
		}
	}
	// 水位推进到 m3 起点：m0、m1、m2 全部到确认点。
	eng.AdvanceWatermark("j", base.Add(3*time.Minute))

	views := eng.Windows("j", "d")
	if len(views) != 3 {
		failureMsg(t, fmt.Sprintf("expected 3 windows (data, idle, data), got %d", len(views)),
			all, fmt.Sprintf("%+v", views))
	}
	v0, ok0 := findView(eng, "j", "d", base)
	v1, ok1 := findView(eng, "j", "d", base.Add(time.Minute))
	v2, ok2 := findView(eng, "j", "d", base.Add(2*time.Minute))
	if !ok0 || !ok1 || !ok2 {
		failureMsg(t, "missing expected windows", all, fmt.Sprintf("%+v", views))
	}
	if v0.State != WindowClosed || v1.State != WindowClosed || v2.State != WindowClosed {
		failureMsg(t, "all three windows must be confirmed closed", all, fmt.Sprintf("%+v %+v %+v", v0, v1, v2))
	}
	if !v1.Idle {
		failureMsg(t, "window with no events must be marked Idle", all, fmt.Sprintf("%+v", v1))
	}
	if v1.Metrics.EventCount != 0 || v1.Metrics.Rows != 0 || v1.Metrics.ThroughputPerSec != 0 {
		failureMsg(t, "idle window metrics must be zero", all, fmt.Sprintf("%+v", v1.Metrics))
	}
	if v0.Idle {
		failureMsg(t, "window containing a zero-rows event is data, not idle (zero-value must be distinguishable)",
			all, fmt.Sprintf("%+v", v0))
	}
	if v0.Metrics.EventCount != 1 || v0.Metrics.ThroughputPerSec != 0 {
		failureMsg(t, "zero-rows data window: count=1 but value zero", all, fmt.Sprintf("%+v", v0.Metrics))
	}
	if v2.Idle || v2.Metrics.ThroughputPerSec != 10.0 {
		failureMsg(t, "m2 must be a normal data window", all, fmt.Sprintf("%+v", v2))
	}
}

// TestIdleWindowBreaksOngoingIncident：空闲窗口是恢复点——
// 持续越限不能跨过空闲期继续抑制；空闲后再越限必须重新触发；空闲本身不凭空造告警。
func TestIdleWindowBreaksOngoingIncident(t *testing.T) {
	now, _ := testClock()
	eng := NewEngine(Config{WindowSize: time.Minute, AllowedLateness: 0, Now: now})
	eng.AddRule(AlertRule{RuleID: "tp", Job: "j", Dataset: "d", MinThroughputPerSec: 10})
	base := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)

	var all []Event
	// m0：低吞吐（100 行/分 ≈ 1.67/s）越限，触发。
	ingestMinute(t, eng, 0, 100, &all, base)
	// m1：无任何事件上报。仅推进水位越过 m1 结束点，补确认空闲空窗 => 恢复点。
	eng.AdvanceWatermark("j", base.Add(2*time.Minute))
	// m2：再次低吞吐，必须重新触发（而不是被当作同一持续区间抑制掉）。
	ingestMinute(t, eng, 2, 100, &all, base)

	alerts := eng.Alerts("j", "d")
	if len(alerts) != 2 {
		failureMsg(t, fmt.Sprintf("idle gap must break incident: expected 2 alerts, got %d", len(alerts)),
			all, fmt.Sprintf("%+v", alerts))
	}
	if len(alerts) == 2 {
		if alerts[0].Window.Start != base || alerts[1].Window.Start != base.Add(2*time.Minute) {
			failureMsg(t, "alerts must fire on m0 and m2", all, fmt.Sprintf("%+v", alerts))
		}
		for _, a := range alerts {
			if a.Window.Start.Equal(base.Add(time.Minute)) {
				failureMsg(t, "idle window must not fabricate an alert", all, fmt.Sprintf("%+v", a))
			}
		}
	}
	v1, ok := findView(eng, "j", "d", base.Add(time.Minute))
	if !ok || !v1.Idle || v1.State != WindowClosed {
		failureMsg(t, "m1 must be a confirmed idle window acting as recovery point", all,
			fmt.Sprintf("ok=%v view=%+v", ok, v1))
	}
}

// TestNoIdleWindowsBeforeFirstObserved：空闲填充不得捏造首个观测窗口之前的历史。
func TestNoIdleWindowsBeforeFirstObserved(t *testing.T) {
	now, _ := testClock()
	eng := NewEngine(Config{WindowSize: time.Minute, AllowedLateness: 0, Now: now})
	base := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	// 第一个事件直接落在 m5，水位推到 m7：只能确认 m5、m6，不能凭空造出 m0..m4。
	var all []Event
	ingestMinute(t, eng, 5, 6000, &all, base)
	eng.AdvanceWatermark("j", base.Add(7*time.Minute))
	views := eng.Windows("j", "d")
	if len(views) != 2 {
		failureMsg(t, fmt.Sprintf("expected exactly 2 windows (m5 data, m6 idle), got %d", len(views)),
			all, fmt.Sprintf("%+v", views))
	}
	if !views[0].ID.Start.Equal(base.Add(5*time.Minute)) || views[0].Idle {
		failureMsg(t, "first window must be the observed m5 data window", all, fmt.Sprintf("%+v", views[0]))
	}
	if !views[1].ID.Start.Equal(base.Add(6*time.Minute)) || !views[1].Idle {
		failureMsg(t, "m6 must be idle", all, fmt.Sprintf("%+v", views[1]))
	}
}
