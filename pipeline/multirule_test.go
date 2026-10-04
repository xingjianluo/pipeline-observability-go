package pipeline

import (
	"fmt"
	"testing"
	"time"
)

// ingestMinute 摄入一条落在第 minute 个窗口内的成功事件，随后推进水位确认该窗口。
func ingestMinute(t *testing.T, eng *Engine, minute int, rows int64, all *[]Event, base time.Time) {
	t.Helper()
	ev := Event{
		EventID:   fmt.Sprintf("m%d", minute),
		Job:       "j",
		Dataset:   "d",
		Status:    StatusSuccess,
		EventTime: base.Add(time.Duration(minute) * time.Minute).Add(5 * time.Second),
		Rows:      rows,
	}
	*all = append(*all, ev)
	if r := eng.Ingest(ev); r.Rejected {
		failureMsg(t, "unexpected rejection: "+r.Detail, []Event{ev}, "")
	}
	eng.AdvanceWatermark("j", base.Add(time.Duration(minute+1)*time.Minute))
}

// alertFingerprint 提取 (RuleID, Kind, 窗口起点) 作为可区分结论的指纹。
func alertFingerprint(a Alert) string {
	return fmt.Sprintf("rule=%s/%s@%s", a.RuleID, a.Kind, a.Window.Start.Format("15:04:05"))
}

// TestMultipleRulesFireTogether：同一流挂多条规则（含同维度两档阈值），
// 同一窗口同时踩中时必须给出互相可区分的全部告警结论。
func TestMultipleRulesFireTogether(t *testing.T) {
	now, _ := testClock()
	eng := NewEngine(Config{WindowSize: time.Minute, AllowedLateness: 0, Now: now})
	// 同一吞吐维度两档：严格档 100/s、宽松档 10/s；低吞吐窗口（1.67/s）同时踩中两条。
	eng.AddRule(AlertRule{RuleID: "tp-strict", Job: "j", Dataset: "d", MinThroughputPerSec: 100})
	eng.AddRule(AlertRule{RuleID: "tp-loose", Job: "j", Dataset: "d", MinThroughputPerSec: 10})
	// 另一维度的规则：失败率上限。
	eng.AddRule(AlertRule{RuleID: "fr", Job: "j", Dataset: "d", MaxFailureRate: 0.5})

	base := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	var all []Event
	// 低吞吐 + 全失败窗口：三条规则全部越限。
	ev := Event{EventID: "bad", Job: "j", Dataset: "d", Status: StatusFailed,
		EventTime: base.Add(5 * time.Second), Rows: 100}
	all = append(all, ev)
	if r := eng.Ingest(ev); r.Rejected {
		failureMsg(t, "unexpected rejection: "+r.Detail, all, "")
	}
	eng.AdvanceWatermark("j", base.Add(time.Minute))

	alerts := eng.Alerts("j", "d")
	got := map[string]bool{}
	var order []string
	for _, a := range alerts {
		got[alertFingerprint(a)] = true
		order = append(order, alertFingerprint(a))
		if a.RuleID == "" {
			failureMsg(t, "alert must carry RuleID", all, fmt.Sprintf("%+v", a))
		}
		if len(a.EventIDs) != 1 || a.EventIDs[0] != "bad" {
			failureMsg(t, "alert must be reproducible from source events", all, fmt.Sprintf("%+v", a))
		}
	}
	want := []string{
		"rule=tp-strict/throughput_breach@12:00:00",
		"rule=tp-loose/throughput_breach@12:00:00",
		"rule=fr/failure_rate_breach@12:00:00",
	}
	if len(alerts) != len(want) {
		failureMsg(t, fmt.Sprintf("expected %d distinguishable alerts, got %d", len(want), len(alerts)),
			all, fmt.Sprintf("order=%v", order))
	}
	for _, w := range want {
		if !got[w] {
			failureMsg(t, "missing alert "+w, all, fmt.Sprintf("order=%v", order))
		}
	}
	// 顺序稳定：按规则注册顺序、规则内按固定维度顺序。
	for i, w := range want {
		if order[i] != w {
			failureMsg(t, "alert order not deterministic", all,
				fmt.Sprintf("got=%v want=%v", order, want))
		}
	}
}

// TestRuleIncidentsIndependent：多条规则各自的持续抑制与恢复互不干扰。
// 严格档持续越限只触发一次；宽松档在“恢复→再越限”时独立重新触发，
// 且宽松档的恢复/复发不能影响严格档仍在持续的区间。
func TestRuleIncidentsIndependent(t *testing.T) {
	now, _ := testClock()
	eng := NewEngine(Config{WindowSize: time.Minute, AllowedLateness: 0, Now: now})
	eng.AddRule(AlertRule{RuleID: "strict", Job: "j", Dataset: "d", MinThroughputPerSec: 100})
	eng.AddRule(AlertRule{RuleID: "loose", Job: "j", Dataset: "d", MinThroughputPerSec: 10})
	base := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)

	var all []Event
	// 吞吐（行/分钟 → /秒）：
	// m0: 100 行 ≈1.67/s  两档都越限，各自触发
	// m1: 3000 行 =50/s   严格档仍越限（抑制）；宽松档恢复
	// m2: 100 行 ≈1.67/s  严格档仍在同一区间（继续抑制）；宽松档重新触发
	ingestMinute(t, eng, 0, 100, &all, base)
	ingestMinute(t, eng, 1, 3000, &all, base)
	ingestMinute(t, eng, 2, 100, &all, base)

	alerts := eng.Alerts("j", "d")
	var strict, loose []Alert
	for _, a := range alerts {
		switch a.RuleID {
		case "strict":
			strict = append(strict, a)
		case "loose":
			loose = append(loose, a)
		}
	}
	if len(strict) != 1 || strict[0].Window.Start != base {
		failureMsg(t, "strict rule must fire once for the whole ongoing incident", all,
			fmt.Sprintf("strict=%+v", strict))
	}
	if len(loose) != 2 ||
		loose[0].Window.Start != base ||
		loose[1].Window.Start != base.Add(2*time.Minute) {
		failureMsg(t, "loose rule must recover on m1 and re-fire on m2 independently", all,
			fmt.Sprintf("loose=%+v", loose))
	}
	// 严格档在宽松档“恢复又复发”期间不产生新告警，证明状态互不串扰。
}

// TestMultiRuleReplayStable：同一批输入完整重放到全新引擎，告警指纹序列完全一致。
func TestMultiRuleReplayStable(t *testing.T) {
	base := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	run := func() []string {
		now, _ := testClock()
		eng := NewEngine(Config{WindowSize: time.Minute, AllowedLateness: 0, Now: now})
		eng.AddRule(AlertRule{RuleID: "a", Job: "j", Dataset: "d", MinThroughputPerSec: 100})
		eng.AddRule(AlertRule{RuleID: "b", Job: "j", Dataset: "d", MinThroughputPerSec: 10})
		var all []Event
		ingestMinute(t, eng, 0, 100, &all, base)
		ingestMinute(t, eng, 1, 60000, &all, base)
		ingestMinute(t, eng, 2, 100, &all, base)
		var fps []string
		for _, a := range eng.Alerts("j", "d") {
			fps = append(fps, alertFingerprint(a))
		}
		return fps
	}
	first, second := run(), run()
	if len(first) != len(second) {
		t.Fatalf("replay alert count differs: %v vs %v", first, second)
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("replay alert sequence differs: %v vs %v", first, second)
		}
	}
}
