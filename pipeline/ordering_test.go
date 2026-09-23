package pipeline

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// describeEvent 给出事件的可读描述，断言失败时作为输入与判定依据打印。
func describeEvent(ev Event) string {
	return fmt.Sprintf("Event{id=%s job=%s dataset=%s status=%s eventTime=%s ingestTime=%s rows=%d hint=%s}",
		ev.EventID, ev.Job, ev.Dataset, ev.Status,
		ev.EventTime.Format(time.RFC3339Nano), ev.IngestTime.Format(time.RFC3339Nano),
		ev.Rows, ev.WatermarkHint.Format(time.RFC3339Nano))
}

func failureMsg(t *testing.T, prefix string, evs []Event, extra string) {
	t.Helper()
	var b strings.Builder
	b.WriteString(prefix + "\n  input events:\n")
	for _, ev := range evs {
		b.WriteString("    " + describeEvent(ev) + "\n")
	}
	if extra != "" {
		b.WriteString("  evidence: " + extra + "\n")
	}
	t.Fatal(b.String())
}

func testClock() (func() time.Time, *time.Time) {
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	cur := base
	return func() time.Time { return cur }, &cur
}

// TestOutOfOrderWithinTolerance：乱序事件在窗口确认前补入，指标与有序到达完全一致。
func TestOutOfOrderWithinTolerance(t *testing.T) {
	now, _ := testClock()
	eng := NewEngine(Config{WindowSize: time.Minute, AllowedLateness: 30 * time.Second, Now: now})

	t0 := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	// 同一分钟窗口，故意乱序摄入：先 [40s]，再 [05s]，再 [20s]。
	evs := []Event{
		{EventID: "e2", Job: "j1", Dataset: "d1", Status: StatusSuccess, EventTime: t0.Add(40 * time.Second), IngestTime: t0.Add(42 * time.Second), Rows: 200},
		{EventID: "e1", Job: "j1", Dataset: "d1", Status: StatusSuccess, EventTime: t0.Add(5 * time.Second), IngestTime: t0.Add(10 * time.Second), Rows: 100},
		{EventID: "e3", Job: "j1", Dataset: "d1", Status: StatusFailed, EventTime: t0.Add(20 * time.Second), IngestTime: t0.Add(25 * time.Second), Rows: 0},
	}
	for _, ev := range evs {
		if r := eng.Ingest(ev); r.Rejected {
			failureMsg(t, "unexpected rejection: "+r.Detail, []Event{ev}, "")
		}
	}
	// 水位推进到窗口结束 + 迟到宽限之后，窗口确认。
	closed := eng.AdvanceWatermark("j1", t0.Add(91*time.Second))
	if len(closed) != 1 {
		failureMsg(t, fmt.Sprintf("expected 1 closed window, got %d", len(closed)), evs, "")
	}
	views := eng.Windows("j1", "d1")
	if len(views) != 1 {
		failureMsg(t, fmt.Sprintf("expected 1 window, got %d", len(views)), evs, "")
	}
	m := views[0].Metrics
	if m.EventCount != 3 || m.SuccessCount != 2 || m.FailedCount != 1 || m.Rows != 300 {
		failureMsg(t, "aggregation mismatch", evs, fmt.Sprintf("metrics=%+v want count=3 succ=2 fail=1 rows=300", m))
	}
	// 吞吐 = 300 行 / 60 秒 = 5。
	if m.ThroughputPerSec != 5.0 {
		failureMsg(t, "throughput mismatch", evs, fmt.Sprintf("metrics=%+v", m))
	}
	// 新鲜度锚点 = 最晚成功摄入 = 42s；窗口结束 = 60s => 0（提前产出，不记负值）。
	if m.FreshnessLag != 0 {
		failureMsg(t, "freshness mismatch", evs, fmt.Sprintf("metrics=%+v", m))
	}
}

// TestLateEventBeyondToleranceRejected：超出允许迟到范围的事件必须被明确拒绝并归类原因。
func TestLateEventBeyondToleranceRejected(t *testing.T) {
	now, _ := testClock()
	eng := NewEngine(Config{WindowSize: time.Minute, AllowedLateness: 10 * time.Second, Now: now})

	t0 := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	first := Event{EventID: "a1", Job: "j", Dataset: "d", Status: StatusSuccess,
		EventTime: t0.Add(5 * time.Second), IngestTime: t0.Add(6 * time.Second), Rows: 100}
	// 一个 03:00 的事件把水位推到 03:00，旧窗口 [02:00,03:00) 早已过结束+宽限。
	future := Event{EventID: "late-marker", Job: "j", Dataset: "d", Status: StatusSuccess,
		EventTime: t0.Add(3 * time.Minute), IngestTime: t0.Add(3 * time.Minute), Rows: 1}
	stale := Event{EventID: "stale1", Job: "j", Dataset: "d", Status: StatusSuccess,
		EventTime: t0.Add(15 * time.Second), IngestTime: t0.Add(3 * time.Minute), Rows: 50}

	if r := eng.Ingest(first); r.Rejected {
		failureMsg(t, "first event rejected: "+r.Detail, []Event{first}, "")
	}
	eng.Ingest(future)

	r := eng.Ingest(stale)
	if !r.Rejected || r.Reason != RejectLateBeyondTolerance {
		failureMsg(t, fmt.Sprintf("stale event must be rejected as %s, got %+v", RejectLateBeyondTolerance, r),
			[]Event{first, future, stale}, r.Detail)
	}
	rejected := eng.RejectedEvents()
	if len(rejected) != 1 || rejected[0].Event.EventID != "stale1" {
		failureMsg(t, "rejected ledger mismatch", []Event{first, future, stale},
			fmt.Sprintf("%+v", rejected))
	}
	// 已确认窗口的结论不得因迟到事件改变。
	views := eng.Windows("j", "d")
	var w0 WindowView
	for _, v := range views {
		if v.ID.Start.Equal(t0) {
			w0 = v
		}
	}
	if w0.State != WindowClosed {
		failureMsg(t, "old window should be closed/frozen", []Event{first, future, stale},
			fmt.Sprintf("state=%s views=%+v", w0.State, views))
	}
	if w0.Metrics.Rows != 100 || w0.Metrics.EventCount != 1 {
		failureMsg(t, "frozen window mutated by late event", []Event{first, future, stale},
			fmt.Sprintf("%+v", w0.Metrics))
	}
}

// TestDuplicateDeliveryIdempotent：重复投递（含窗口确认后的重复）不得重复计数或改变结论。
func TestDuplicateDeliveryIdempotent(t *testing.T) {
	now, _ := testClock()
	eng := NewEngine(Config{WindowSize: time.Minute, AllowedLateness: 10 * time.Second, Now: now})
	t0 := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)

	ev := Event{EventID: "dup1", Job: "j", Dataset: "d", Status: StatusSuccess,
		EventTime: t0.Add(5 * time.Second), IngestTime: t0.Add(6 * time.Second), Rows: 100}
	r1 := eng.Ingest(ev)
	if r1.Duplicate || r1.Rejected {
		failureMsg(t, "first ingest should be accepted", []Event{ev}, fmt.Sprintf("%+v", r1))
	}
	r2 := eng.Ingest(ev)
	if !r2.Duplicate {
		failureMsg(t, "identical redelivery must be reported as duplicate",
			[]Event{ev, ev}, fmt.Sprintf("r2=%+v", r2))
	}

	eng.AdvanceWatermark("j", t0.Add(71*time.Second))
	// 窗口确认后再投递一次：仍必须识别为重复，而不是拒绝或重复计数。
	r3 := eng.Ingest(ev)
	if !r3.Duplicate || r3.Rejected {
		failureMsg(t, "post-close redelivery must remain duplicate, not rejected",
			[]Event{ev, ev, ev}, fmt.Sprintf("r3=%+v", r3))
	}
	views := eng.Windows("j", "d")
	if views[0].Metrics.EventCount != 1 || views[0].Metrics.Rows != 100 {
		failureMsg(t, "duplicate caused double counting", []Event{ev},
			fmt.Sprintf("%+v", views[0].Metrics))
	}
	if n := len(eng.RejectedEvents()); n != 0 {
		failureMsg(t, "duplicates must not enter rejection ledger", []Event{ev},
			fmt.Sprintf("rejected=%d", n))
	}
}

// TestInvalidEventsClassified：非法事件须被拒绝并标注 RejectInvalid，不静默丢弃。
func TestInvalidEventsClassified(t *testing.T) {
	now, _ := testClock()
	eng := NewEngine(Config{WindowSize: time.Minute, Now: now})
	t0 := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	bad := []Event{
		{EventID: "", Job: "j", Dataset: "d", Status: StatusSuccess, EventTime: t0},
		{EventID: "x", Job: "", Dataset: "d", Status: StatusSuccess, EventTime: t0},
		{EventID: "x", Job: "j", Dataset: "", Status: StatusSuccess, EventTime: t0},
		{EventID: "x", Job: "j", Dataset: "d", Status: "weird", EventTime: t0},
		{EventID: "x", Job: "j", Dataset: "d", Status: StatusSuccess, EventTime: time.Time{}},
	}
	for _, ev := range bad {
		r := eng.Ingest(ev)
		if !r.Rejected || r.Reason != RejectInvalid {
			failureMsg(t, "invalid event must be classified", []Event{ev}, fmt.Sprintf("%+v", r))
		}
	}
	if len(eng.RejectedEvents()) != len(bad) {
		failureMsg(t, "all invalid events must land in ledger", bad, "")
	}
}
