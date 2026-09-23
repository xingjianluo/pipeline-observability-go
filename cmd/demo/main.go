// Command demo 在本地用确定性事件流模拟流水线运行，
// 演示乱序、迟到（容忍内/超范围）、重复投递下的窗口指标，
// 以及阈值告警的触发、持续区间抑制与恢复后重新触发。
//
// 运行：go run ./cmd/demo
package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/xingjianluo/pipeline-observability-go/pipeline"
)

func main() {
	base := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	clock := base
	now := func() time.Time { return clock }

	eng := pipeline.NewEngine(pipeline.Config{
		WindowSize:      time.Minute,
		AllowedLateness: 10 * time.Second,
		Now:             now,
	})
	eng.AddRule(pipeline.AlertRule{
		Job:                  "orders-etl",
		Dataset:              "orders_daily",
		MaxFreshnessLag:      5 * time.Second,
		MaxProcessingLatency: 15 * time.Second,
		MinThroughputPerSec:  50,
		MaxFailureRate:       0, // 0 表示不启用失败率检查
	})

	const job, dataset = "orders-etl", "orders_daily"
	ev := func(id string, status pipeline.EventStatus, off, ingestOff time.Duration, rows int64) pipeline.Event {
		return pipeline.Event{
			EventID:    id,
			Job:        job,
			Dataset:    dataset,
			Status:     status,
			EventTime:  base.Add(off),
			IngestTime: base.Add(ingestOff),
			Rows:       rows,
		}
	}

	fmt.Println("=== 本地事件流模拟（窗口=1m，允许迟到=10s）===")

	// 1) 窗口 [12:00,12:01)：乱序到达（先 40s 后 05s），吞吐偏低 + 新鲜度/延迟越限。
	ingest(eng, &clock, ev("w0-late-arrival", pipeline.StatusSuccess, 40*time.Second, 90*time.Second, 1000))
	ingest(eng, &clock, ev("w0-early-arrival", pipeline.StatusSuccess, 5*time.Second, 8*time.Second, 2000))
	// 2) 同一条事件重复投递两次（含一次在窗口确认后）。
	dup := ev("w0-dup", pipeline.StatusSuccess, 10*time.Second, 12*time.Second, 500)
	ingest(eng, &clock, dup)
	ingest(eng, &clock, dup)
	// 3) 窗口 [12:01,12:02)：正常高吞吐窗口（恢复）。
	ingest(eng, &clock, ev("w1-ok", pipeline.StatusSuccess, 65*time.Second, 66*time.Second, 6000))
	// 4) 先确认窗口 0，再补一条容忍范围内的迟到事件是不允许的（已冻结）；
	//    这里推进水位确认窗口 0。
	advance(eng, &clock, base.Add(70*time.Second)) // watermark=12:01:10 => 关闭窗口 0
	ingest(eng, &clock, dup)                       // 窗口确认后重复投递：仍幂等
	// 5) 窗口 1 的迟到事件（eventTime=12:01:01，watermark=12:01:10，阈值=12:01:00 => 接受）。
	ingest(eng, &clock, ev("w1-late-ok", pipeline.StatusSuccess, 61*time.Second, 71*time.Second, 6000))
	// 6) 推进水位到 12:02:10，确认恢复窗口 w1（高吞吐，不越限 => 各故障区间恢复）。
	advance(eng, &clock, base.Add(2*time.Minute+10*time.Second))
	// 7) 窗口 2：再次低吞吐，应在恢复后重新触发告警。
	ingest(eng, &clock, ev("w2-bad", pipeline.StatusSuccess, 125*time.Second, 130*time.Second, 100))
	// 8) 超出允许迟到范围的迟到事件：必须被明确拒绝并归类。
	stale := ev("stale-w0", pipeline.StatusSuccess, 15*time.Second, 3*time.Minute, 999)
	ingest(eng, &clock, stale)
	advance(eng, &clock, base.Add(3*time.Minute+10*time.Second))

	fmt.Println("\n=== 已确认窗口指标（随事件推进冻结，不回退）===")
	for _, w := range eng.Windows(job, dataset) {
		fmt.Printf("窗口 [%s,%s) state=%s\n", w.ID.Start.Format("15:04:05"), w.ID.End.Format("15:04:05"), w.State)
		fmt.Printf("  事件=%d 成功=%d 失败=%d 行数=%d 吞吐=%.2f/s 成功%%=%.2f\n",
			w.Metrics.EventCount, w.Metrics.SuccessCount, w.Metrics.FailedCount,
			w.Metrics.Rows, w.Metrics.ThroughputPerSec, w.Metrics.SuccessRate*100)
		fmt.Printf("  新鲜度滞后=%s 最大处理延迟=%s\n", w.Metrics.FreshnessLag, w.Metrics.MaxProcessingLatency)
	}

	fmt.Println("\n=== 告警（同一持续故障区间只触发一次，恢复后可重新触发）===")
	for _, a := range eng.Alerts(job, dataset) {
		fmt.Printf("- [%s] 窗口[%s,%s) observed=%.4f threshold=%.4f\n    依据: %s\n",
			a.Kind, a.Window.Start.Format("15:04:05"), a.Window.End.Format("15:04:05"),
			a.Observed, a.Threshold, indent(a.Reason))
	}

	fmt.Println("\n=== 被拒绝/单独归类事件（不静默丢弃）===")
	for _, r := range eng.RejectedEvents() {
		fmt.Printf("- id=%s reason=%s\n    %s\n", r.Event.EventID, r.Reason, indent(r.Detail))
	}
}

// ingest 摄入事件并打印其明确去向；同时把演示时钟推进到事件摄入时刻。
func ingest(eng *pipeline.Engine, clock *time.Time, ev pipeline.Event) {
	if ev.IngestTime.After(*clock) {
		*clock = ev.IngestTime
	}
	r := eng.Ingest(ev)
	switch {
	case r.Rejected:
		fmt.Printf("拒绝  id=%-16s eventTime=%s reason=%s (%s)\n",
			ev.EventID, ev.EventTime.Format("15:04:05"), r.Reason, r.Detail)
	case r.Duplicate:
		fmt.Printf("重复  id=%-16s eventTime=%s => 幂等忽略（%s）\n",
			ev.EventID, ev.EventTime.Format("15:04:05"), r.Detail)
	default:
		fmt.Printf("接受  id=%-16s eventTime=%s rows=%d，本次关闭窗口数=%d\n",
			ev.EventID, ev.EventTime.Format("15:04:05"), ev.Rows, r.ClosedWindows)
	}
}

func advance(eng *pipeline.Engine, clock *time.Time, t time.Time) {
	*clock = t
	closed := eng.AdvanceWatermark("orders-etl", t)
	fmt.Printf("水位推进到 %s，新确认窗口 %d 个\n", t.Format("15:04:05"), len(closed))
}

func indent(s string) string { return strings.ReplaceAll(s, "\n", "\n    ") }
