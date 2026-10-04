package pipeline

import (
	"fmt"
	"testing"
	"time"
)

// TestRetentionEvictsOldestWindows：超出保留容量的最旧已确认窗口被回收，
// 计数可查；查询只返回仍保留的窗口；开放窗口永远不被回收。
func TestRetentionEvictsOldestWindows(t *testing.T) {
	now, _ := testClock()
	eng := NewEngine(Config{WindowSize: time.Minute, AllowedLateness: 0, Now: now, Retention: 2})
	base := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	var all []Event
	for m := 0; m < 4; m++ {
		ingestMinute(t, eng, m, int64(6000*(m%2)+100), &all, base)
	}
	views := eng.Windows("j", "d")
	if len(views) != 2 {
		failureMsg(t, fmt.Sprintf("retention=2 must keep only 2 windows, got %d", len(views)),
			all, fmt.Sprintf("%+v", views))
	}
	if !views[0].ID.Start.Equal(base.Add(2*time.Minute)) ||
		!views[1].ID.Start.Equal(base.Add(3*time.Minute)) {
		failureMsg(t, "newest 2 windows must be retained", all, fmt.Sprintf("%+v", views))
	}
	if got := eng.ReclaimedWindows("j", "d"); got != 2 {
		failureMsg(t, fmt.Sprintf("per-stream reclaimed count=2, got %d", got), all, "")
	}
	if got := eng.RetentionStats().ReclaimedWindows; got != 2 {
		failureMsg(t, fmt.Sprintf("global reclaimed count=2, got %d", got), all, "")
	}
	// 精确查询已回收窗口：明确不存在。
	if _, ok := eng.Window(WindowID{Job: "j", Dataset: "d", Kind: WindowTumbling,
		Start: base, End: base.Add(time.Minute)}); ok {
		failureMsg(t, "reclaimed window must not be queryable", all, "")
	}
}

// TestReclaimedWindowRejectsLateEventsAndFreezesAlerts：回收后：
//  1. 迟到事件（含原始事件的重复投递）都以 window_reclaimed 明确拒绝、绝不回写；
//  2. 进行中的告警区间不受回收影响，恢复/复发判断仍然正确；
//  3. 拒绝事件进入台账，不静默丢弃。
func TestReclaimedWindowRejectsLateEventsAndFreezesAlerts(t *testing.T) {
	now, _ := testClock()
	eng := NewEngine(Config{WindowSize: time.Minute, AllowedLateness: 0, Now: now, Retention: 2})
	eng.AddRule(AlertRule{RuleID: "tp", Job: "j", Dataset: "d", MinThroughputPerSec: 10})
	base := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)

	var all []Event
	// m0 低吞吐越限 => 触发告警；m1 高吞吐 => 恢复；m2、m3 持续低吞吐 => m2 重新触发、m3 抑制。
	ingestMinute(t, eng, 0, 100, &all, base) // 触发
	ingestMinute(t, eng, 1, 60000, &all, base)
	ingestMinute(t, eng, 2, 100, &all, base) // 重新触发
	ingestMinute(t, eng, 3, 100, &all, base) // 同一区间抑制；此刻 m0、m1 已被回收

	if got := eng.ReclaimedWindows("j", "d"); got != 2 {
		failureMsg(t, fmt.Sprintf("expected 2 reclaimed, got %d", got), all, "")
	}
	alerts := eng.Alerts("j", "d")
	if len(alerts) != 2 ||
		alerts[0].Window.Start != base ||
		alerts[1].Window.Start != base.Add(2*time.Minute) {
		failureMsg(t, "ongoing incident state must survive reclamation", all,
			fmt.Sprintf("%+v", alerts))
	}

	// 迟到事件落入已回收窗口 m0：明确拒绝为 window_reclaimed。
	late := Event{EventID: "late-m0", Job: "j", Dataset: "d", Status: StatusSuccess,
		EventTime: base.Add(10 * time.Second), Rows: 99999}
	r := eng.Ingest(late)
	if !r.Rejected || r.Reason != RejectWindowReclaimed {
		failureMsg(t, "late event into reclaimed window must be rejected as window_reclaimed",
			append(all, late), fmt.Sprintf("%+v", r))
	}
	// 原始事件的重复投递（窗口已回收）也不能再被当成普通重复静默接受，更不能回写。
	redelivery := Event{EventID: "m0", Job: "j", Dataset: "d", Status: StatusSuccess,
		EventTime: base.Add(5 * time.Second), Rows: 100}
	r2 := eng.Ingest(redelivery)
	if !r2.Rejected || r2.Reason != RejectWindowReclaimed {
		failureMsg(t, "redelivery of event from a reclaimed window must be rejected window_reclaimed",
			append(all, redelivery), fmt.Sprintf("%+v", r2))
	}
	// 回收不影响新窗口的继续判定：m4 低吞吐仍在 m2..m4 的同一区间内 => 抑制，无新告警。
	ingestMinute(t, eng, 4, 100, &all, base)
	if n := len(eng.Alerts("j", "d")); n != 2 {
		failureMsg(t, "suppression across reclaimed boundary must remain intact", all,
			fmt.Sprintf("alerts=%d", n))
	}
	// m5 高吞吐恢复后 m6 再越限 => 重新触发，证明恢复/复发判断不受回收影响。
	ingestMinute(t, eng, 5, 60000, &all, base)
	ingestMinute(t, eng, 6, 100, &all, base)
	alerts = eng.Alerts("j", "d")
	if len(alerts) != 3 || alerts[2].Window.Start != base.Add(6*time.Minute) {
		failureMsg(t, "re-fire after recovery must work post-reclamation", all,
			fmt.Sprintf("%+v", alerts))
	}
	// 被拒事件全部进入台账。
	var reclaimedRejections int
	for _, rej := range eng.RejectedEvents() {
		if rej.Reason == RejectWindowReclaimed {
			reclaimedRejections++
		}
	}
	if reclaimedRejections != 2 {
		failureMsg(t, "reclaimed-window rejections must be recorded in ledger", all,
			fmt.Sprintf("got=%d", reclaimedRejections))
	}
}

// TestRetentionDoesNotMutateFrozenConclusions：回收只删除可查询窗口，
// 已触发告警携带的窗口依据与事件 ID 仍然完整可复现。
func TestRetentionKeepsAlertEvidence(t *testing.T) {
	now, _ := testClock()
	eng := NewEngine(Config{WindowSize: time.Minute, AllowedLateness: 0, Now: now, Retention: 1})
	eng.AddRule(AlertRule{Job: "j", Dataset: "d", MinThroughputPerSec: 10})
	base := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	var all []Event
	ingestMinute(t, eng, 0, 100, &all, base)
	ingestMinute(t, eng, 1, 100, &all, base)
	ingestMinute(t, eng, 2, 100, &all, base)
	a0 := eng.Alerts("j", "d")[0]
	if len(a0.EventIDs) != 1 || a0.EventIDs[0] != "m0" || a0.Window.Start != base {
		failureMsg(t, "alert evidence must remain intact after its window was reclaimed",
			all, fmt.Sprintf("%+v", a0))
	}
}
