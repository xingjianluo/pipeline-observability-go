package pipeline

import (
	"fmt"
	"testing"
	"time"
)

// TestAlertFiresOncePerIncidentAndRearmsAfterRecovery：
// 连续多个窗口越限只告警一次；恢复窗口清除持续故障区间；之后再次越限能重新触发。
func TestAlertFiresOncePerIncidentAndRearmsAfterRecovery(t *testing.T) {
	now, _ := testClock()
	eng := NewEngine(Config{WindowSize: time.Minute, AllowedLateness: 0, Now: now})
	eng.AddRule(AlertRule{Job: "j", Dataset: "d", MinThroughputPerSec: 10})

	base := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	// 窗口 0、1：吞吐低（每分钟 100 行 => 1.67/s < 10），属同一持续故障区间。
	// 窗口 2：吞吐高（60000 行 => 1000/s），恢复。
	// 窗口 3：再次低吞吐，须重新触发。
	plans := []struct {
		minute int
		rows   int64
	}{
		{0, 100}, {1, 100}, {2, 60000}, {3, 100},
	}
	var allEvs []Event
	for _, p := range plans {
		ev := Event{
			EventID:   fmt.Sprintf("m%d", p.minute),
			Job:       "j",
			Dataset:   "d",
			Status:    StatusSuccess,
			EventTime: base.Add(time.Duration(p.minute) * time.Minute).Add(5 * time.Second),
			Rows:      p.rows,
		}
		allEvs = append(allEvs, ev)
		eng.Ingest(ev)
		// 水位越过下一分钟边界 => 确认当前分钟窗口（AllowedLateness=0）。
		eng.AdvanceWatermark("j", base.Add(time.Duration(p.minute+1)*time.Minute))
	}

	alerts := eng.Alerts("j", "d")
	if len(alerts) != 2 {
		failureMsg(t, fmt.Sprintf("expected exactly 2 alerts (fire, suppress, suppress..., recover, refire), got %d", len(alerts)),
			allEvs, fmt.Sprintf("%+v", alerts))
	}
	if len(alerts) == 2 {
		if alerts[0].Kind != AlertThroughput || alerts[1].Kind != AlertThroughput {
			failureMsg(t, "unexpected alert kinds", allEvs, fmt.Sprintf("%+v", alerts))
		}
		if !alerts[0].Window.Start.Equal(base) || !alerts[1].Window.Start.Equal(base.Add(3*time.Minute)) {
			failureMsg(t, "alerts should fire on minute 0 and minute 3", allEvs,
				fmt.Sprintf("w0=%s w1=%s", alerts[0].Window.Start, alerts[1].Window.Start))
		}
		// 复现性：告警必须携带构成该窗口的原始事件 ID。
		if len(alerts[0].EventIDs) != 1 || alerts[0].EventIDs[0] != "m0" {
			failureMsg(t, "first alert not reproducible from source events", allEvs,
				fmt.Sprintf("%+v", alerts[0].EventIDs))
		}
		if len(alerts[1].EventIDs) != 1 || alerts[1].EventIDs[0] != "m3" {
			failureMsg(t, "second alert not reproducible from source events", allEvs,
				fmt.Sprintf("%+v", alerts[1].EventIDs))
		}
	}
}

// TestAlertKindsDistinct：同一窗口不同失败维度产生不同 AlertKind，原因彼此可区分。
func TestAlertKindsDistinct(t *testing.T) {
	now, _ := testClock()
	eng := NewEngine(Config{WindowSize: time.Minute, AllowedLateness: 0, Now: now})
	eng.AddRule(AlertRule{
		Job:                  "j",
		Dataset:              "d",
		MaxFreshnessLag:      5 * time.Second,
		MaxProcessingLatency: 10 * time.Second,
		MinThroughputPerSec:  100,
		MaxFailureRate:       0.0, // 0 表示不启用失败率检查
	})
	base := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	// 失败事件：处理延迟 30s；吞吐 1 行 => 低；最晚成功摄入在窗口结束后 60s => 新鲜度越限。
	evs := []Event{
		{EventID: "f", Job: "j", Dataset: "d", Status: StatusFailed,
			EventTime: base.Add(2 * time.Second), IngestTime: base.Add(32 * time.Second), Rows: 0},
		{EventID: "s", Job: "j", Dataset: "d", Status: StatusSuccess,
			EventTime: base.Add(3 * time.Second), IngestTime: base.Add(2 * time.Minute), Rows: 1},
	}
	eng.Ingest(evs[0])
	eng.Ingest(evs[1])
	eng.AdvanceWatermark("j", base.Add(time.Minute))

	alerts := eng.Alerts("j", "d")
	kinds := map[AlertKind]bool{}
	for _, a := range alerts {
		kinds[a.Kind] = true
		if a.Reason == "" || len(a.EventIDs) != 2 {
			failureMsg(t, "alert must carry reason and source event ids", evs,
				fmt.Sprintf("%+v", a))
		}
	}
	for _, want := range []AlertKind{AlertFreshness, AlertLatency, AlertThroughput} {
		if !kinds[want] {
			failureMsg(t, fmt.Sprintf("missing alert %s", want), evs, fmt.Sprintf("got=%+v", alerts))
		}
	}
	if kinds[AlertFailure] {
		failureMsg(t, "failure-rate check disabled (threshold 0) must not fire", evs,
			fmt.Sprintf("got=%+v", alerts))
	}
}

// TestFailureRateAlertRecovery：纯失败率维度的触发与恢复。
func TestFailureRateAlertRecovery(t *testing.T) {
	now, _ := testClock()
	eng := NewEngine(Config{WindowSize: time.Minute, AllowedLateness: 0, Now: now})
	// 阈值 0.5：全失败窗口失败率 1.0 越限，全成功窗口 0.0 恢复；阈值零值表示不启用该检查。
	eng.AddRule(AlertRule{Job: "j", Dataset: "d", MaxFailureRate: 0.5})
	base := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)

	ingest := func(minute int, status EventStatus) {
		ev := Event{EventID: fmt.Sprintf("e-%d", minute), Job: "j", Dataset: "d",
			Status: status, EventTime: base.Add(time.Duration(minute)*time.Minute + 5*time.Second),
			IngestTime: base.Add(time.Duration(minute)*time.Minute + 6*time.Second), Rows: 6000}
		eng.Ingest(ev)
		eng.AdvanceWatermark("j", base.Add(time.Duration(minute+1)*time.Minute))
	}
	// 阈值 0.5：全失败窗口越限，全成功窗口恢复。
	ingest(0, StatusFailed)  // 触发
	ingest(1, StatusFailed)  // 抑制
	ingest(2, StatusSuccess) // 恢复
	ingest(3, StatusFailed)  // 重新触发
	if n := len(eng.Alerts("j", "d")); n != 2 {
		t.Fatalf("expected 2 failure alerts, got %d: %+v", n, eng.Alerts("j", "d"))
	}
}

// TestAlertReproducibleFromRawEvents：根据告警携带的 EventIDs，从原始输入中重放窗口，
// 能重新算出相同的观测值与阈值判定。
func TestAlertReproducibleFromRawEvents(t *testing.T) {
	now, _ := testClock()
	eng := NewEngine(Config{WindowSize: time.Minute, AllowedLateness: 0, Now: now})
	rule := AlertRule{Job: "j", Dataset: "d", MinThroughputPerSec: 10}
	eng.AddRule(rule)
	base := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	evs := []Event{
		{EventID: "a", Job: "j", Dataset: "d", Status: StatusSuccess, EventTime: base.Add(2 * time.Second), Rows: 5},
		{EventID: "b", Job: "j", Dataset: "d", Status: StatusSuccess, EventTime: base.Add(3 * time.Second), Rows: 7},
	}
	for _, ev := range evs {
		eng.Ingest(ev)
	}
	eng.AdvanceWatermark("j", base.Add(time.Minute))
	alerts := eng.Alerts("j", "d")
	if len(alerts) != 1 {
		failureMsg(t, "expected 1 alert", evs, fmt.Sprintf("%+v", alerts))
	}
	a := alerts[0]

	// 用告警中的 EventIDs 从原始事件重建并复算窗口吞吐。
	byID := map[string]Event{}
	for _, ev := range evs {
		byID[ev.EventID] = ev
	}
	var totalRows int64
	for _, id := range a.EventIDs {
		ev, ok := byID[id]
		if !ok {
			failureMsg(t, "alert references unknown event id: "+id, evs, fmt.Sprintf("%+v", a))
		}
		totalRows += ev.Rows
	}
	recomputed := float64(totalRows) / 60.0
	if recomputed != a.Observed {
		failureMsg(t, "observed value not reproducible", evs,
			fmt.Sprintf("recomputed=%v alert.Observed=%v", recomputed, a.Observed))
	}
	if !(recomputed < rule.MinThroughputPerSec && a.Threshold == rule.MinThroughputPerSec) {
		failureMsg(t, "threshold verdict not reproducible", evs,
			fmt.Sprintf("recomputed=%v threshold=%v", recomputed, a.Threshold))
	}
}
