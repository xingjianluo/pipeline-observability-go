package pipeline

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// closedMetrics 摄入事件、推进水位并返回首个（也是唯一）已确认窗口的指标。
func closedMetrics(t *testing.T, eng *Engine, job, dataset string, evs []Event, wm time.Time) Metrics {
	t.Helper()
	for _, ev := range evs {
		if r := eng.Ingest(ev); r.Rejected {
			failureMsg(t, "unexpected rejection: "+r.Detail, []Event{ev}, "")
		}
	}
	eng.AdvanceWatermark(job, wm)
	for _, v := range eng.Windows(job, dataset) {
		if v.State == WindowClosed {
			return v.Metrics
		}
	}
	failureMsg(t, "no closed window", evs, "")
	return Metrics{}
}

// TestMetricsStableUnderDifferentOrders：相同事件集以不同顺序（含乱序）到达，
// 确认窗口的指标必须完全一致；且确认后再来事件不回退、不改变数值。
func TestMetricsStableUnderDifferentOrders(t *testing.T) {
	t0 := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	evs := []Event{
		{EventID: "1", Job: "j", Dataset: "d", Status: StatusSuccess, EventTime: t0.Add(2 * time.Second), IngestTime: t0.Add(10 * time.Second), Rows: 100},
		{EventID: "2", Job: "j", Dataset: "d", Status: StatusSuccess, EventTime: t0.Add(30 * time.Second), IngestTime: t0.Add(70 * time.Second), Rows: 300},
		{EventID: "3", Job: "j", Dataset: "d", Status: StatusFailed, EventTime: t0.Add(50 * time.Second), IngestTime: t0.Add(55 * time.Second), Rows: 0},
	}
	orders := [][]int{{0, 1, 2}, {2, 1, 0}, {1, 0, 2}, {2, 0, 1}}

	var want Metrics
	first := true
	for oi, order := range orders {
		now, _ := testClock()
		eng := NewEngine(Config{WindowSize: time.Minute, AllowedLateness: 2 * time.Minute, Now: now})
		var ordered []Event
		for _, i := range order {
			ordered = append(ordered, evs[i])
		}
		m := closedMetrics(t, eng, "j", "d", ordered, t0.Add(3*time.Minute))
		if first {
			want = m
			first = false
		} else if m != want {
			failureMsg(t, fmt.Sprintf("order %d produced different metrics", oi), ordered,
				fmt.Sprintf("got=%+v want=%+v", m, want))
		}
	}

	// 新鲜度：最晚成功摄入 = 70s，窗口结束 60s => 10s；
	// 最大处理延迟：max(10-2=8s, 70-30=40s, 55-50=5s) = 40s。
	if want.FreshnessLag != 10*time.Second || want.MaxProcessingLatency != 40*time.Second {
		failureMsg(t, "freshness/latency mismatch", evs, fmt.Sprintf("%+v", want))
	}
	if want.ThroughputPerSec != float64(400)/60.0 {
		failureMsg(t, "throughput mismatch", evs, fmt.Sprintf("%+v", want))
	}
}

// TestMetricsFreezeAfterClose：窗口确认后，指标不随后续事件/查询发生任何变化（单调稳定、不回退）。
func TestMetricsFreezeAfterClose(t *testing.T) {
	now, _ := testClock()
	eng := NewEngine(Config{WindowSize: time.Minute, AllowedLateness: 0, Now: now})
	t0 := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	ev := Event{EventID: "1", Job: "j", Dataset: "d", Status: StatusSuccess,
		EventTime: t0.Add(2 * time.Second), IngestTime: t0.Add(3 * time.Second), Rows: 100}
	eng.Ingest(ev)
	eng.AdvanceWatermark("j", t0.Add(time.Minute))

	v1, ok := eng.Window(WindowID{Job: "j", Dataset: "d", Kind: WindowTumbling, Start: t0, End: t0.Add(time.Minute)})
	if !ok || v1.State != WindowClosed {
		t.Fatalf("window not closed: %+v", v1)
	}
	// 重复投递 + 推进更晚水位，指标必须原样冻结。
	eng.Ingest(ev)
	eng.AdvanceWatermark("j", t0.Add(10*time.Minute))
	v2, _ := eng.Window(v1.ID)
	if v2.Metrics != v1.Metrics {
		failureMsg(t, "metrics changed after close", []Event{ev},
			fmt.Sprintf("before=%+v after=%+v", v1.Metrics, v2.Metrics))
	}
}

// TestJobDatasetIsolation：不同作业/数据集互不污染，水位也互不影响。
func TestJobDatasetIsolation(t *testing.T) {
	now, _ := testClock()
	eng := NewEngine(Config{WindowSize: time.Minute, AllowedLateness: 30 * time.Second, Now: now})
	t0 := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)

	j1d1 := Event{EventID: "a", Job: "j1", Dataset: "d1", Status: StatusSuccess, EventTime: t0.Add(2 * time.Second), IngestTime: t0.Add(3 * time.Second), Rows: 100}
	j1d2 := Event{EventID: "b", Job: "j1", Dataset: "d2", Status: StatusFailed, EventTime: t0.Add(2 * time.Second), IngestTime: t0.Add(3 * time.Second), Rows: 5}
	j2d1 := Event{EventID: "c", Job: "j2", Dataset: "d1", Status: StatusSuccess, EventTime: t0.Add(2 * time.Second), IngestTime: t0.Add(3 * time.Second), Rows: 999}
	for _, ev := range []Event{j1d1, j1d2, j2d1} {
		eng.Ingest(ev)
	}
	// 只推进 j2 水位：j1 的窗口不得被确认。
	eng.AdvanceWatermark("j2", t0.Add(2*time.Minute))
	j1views := eng.Windows("j1", "d1")
	if len(j1views) != 1 || j1views[0].State != WindowOpen {
		failureMsg(t, "j1/d1 window must remain open when only j2 advanced",
			[]Event{j1d1, j1d2, j2d1}, fmt.Sprintf("%+v", j1views))
	}
	eng.AdvanceWatermark("j1", t0.Add(2*time.Minute))
	cases := []struct {
		job, dataset string
		rows         int64
		failed       int64
	}{
		{"j1", "d1", 100, 0},
		{"j1", "d2", 5, 1},
		{"j2", "d1", 999, 0},
	}
	for _, c := range cases {
		vs := eng.Windows(c.job, c.dataset)
		if len(vs) != 1 || vs[0].Metrics.Rows != c.rows || vs[0].Metrics.FailedCount != c.failed {
			t.Fatalf("isolation broken for %s/%s: %+v", c.job, c.dataset, vs)
		}
	}
}

// TestConcurrentIngestion：并发摄入大量事件（含重复），结果与串行去重后的期望一致，无重复计数。
func TestConcurrentIngestion(t *testing.T) {
	now, _ := testClock()
	eng := NewEngine(Config{WindowSize: time.Minute, AllowedLateness: time.Hour, Now: now})
	t0 := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)

	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		ev := Event{
			EventID:   fmt.Sprintf("ev-%03d", i),
			Job:       "job",
			Dataset:   "ds",
			Status:    StatusSuccess,
			EventTime: t0.Add(time.Duration(i%50) * time.Second),
			Rows:      1,
		}
		// 偶数事件额外重复投递一次（不同 goroutine 制造竞态）。
		wg.Add(1)
		go func(ev Event) {
			defer wg.Done()
			eng.Ingest(ev)
		}(ev)
		if i%2 == 0 {
			wg.Add(1)
			go func(ev Event) {
				defer wg.Done()
				eng.Ingest(ev)
			}(ev)
		}
	}
	wg.Wait()

	eng.AdvanceWatermark("job", t0.Add(3*time.Hour))
	var totalEvents, totalRows int64
	var closedWindows int
	for _, v := range eng.Windows("job", "ds") {
		if v.State != WindowClosed {
			t.Fatalf("all windows should be closed: %+v", v)
		}
		closedWindows++
		totalEvents += v.Metrics.EventCount
		totalRows += v.Metrics.Rows
	}
	if totalEvents != n || totalRows != n {
		t.Fatalf("concurrent dedup broken: events=%d rows=%d want=%d (windows=%d, rejected=%d)",
			totalEvents, totalRows, n, closedWindows, len(eng.RejectedEvents()))
	}
	if len(eng.RejectedEvents()) != 0 {
		t.Fatalf("no event should be rejected: %+v", eng.RejectedEvents())
	}
}

// TestReplayConsistency：把同一批事件完整重放到新引擎，结论必须与首轮一致。
func TestReplayConsistency(t *testing.T) {
	t0 := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	evs := []Event{
		{EventID: "1", Job: "j", Dataset: "d", Status: StatusSuccess, EventTime: t0.Add(2 * time.Second), IngestTime: t0.Add(3 * time.Second), Rows: 100},
		{EventID: "2", Job: "j", Dataset: "d", Status: StatusSuccess, EventTime: t0.Add(61 * time.Second), IngestTime: t0.Add(90 * time.Second), Rows: 200},
	}
	run := func() []WindowView {
		now, _ := testClock()
		eng := NewEngine(Config{WindowSize: time.Minute, AllowedLateness: time.Minute, Now: now})
		for _, ev := range evs {
			eng.Ingest(ev)
		}
		eng.AdvanceWatermark("j", t0.Add(10*time.Minute))
		return eng.Windows("j", "d")
	}
	first := run()
	second := run()
	if len(first) != len(second) {
		failureMsg(t, "replay window count differs", evs, fmt.Sprintf("%v vs %v", first, second))
	}
	for i := range first {
		if first[i].Metrics != second[i].Metrics || first[i].State != second[i].State {
			failureMsg(t, "replay metrics differ", evs,
				fmt.Sprintf("first=%+v second=%+v", first[i], second[i]))
		}
	}
}
