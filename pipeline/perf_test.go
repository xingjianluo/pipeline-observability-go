package pipeline

import (
	"fmt"
	"testing"
	"time"
)

// BenchmarkIngestScalesWithRulesAndWindows：大量窗口 + 大量规则下的摄入耗时。
// 规则按流索引（O(匹配规则数) 而非全表扫描），窗口维护为对有序切片的二分插入与
// 均摊 O(1) 的空闲填充/回收，单条摄入不应随窗口总数线性退化。
//
// 运行：go test -run XXX -bench BenchmarkIngest -benchtime=200x ./pipeline
func BenchmarkIngestScalesWithRulesAndWindows(b *testing.B) {
	base := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	const rulesN = 50
	const windowsN = 2000
	now, _ := testClock()
	eng := NewEngine(Config{WindowSize: time.Minute, AllowedLateness: 0, Now: now, Retention: 128})
	// 匹配规则 + 大量不相关流的规则（验证索引避免全表扫描）。
	for i := 0; i < rulesN; i++ {
		eng.AddRule(AlertRule{RuleID: fmt.Sprintf("r-%d", i), Job: "j", Dataset: "d", MinThroughputPerSec: 1e-9})
		eng.AddRule(AlertRule{RuleID: fmt.Sprintf("x-%d", i), Job: "other", Dataset: "d", MinThroughputPerSec: 1e-9})
	}
	evs := make([]Event, windowsN)
	for m := 0; m < windowsN; m++ {
		evs[m] = Event{EventID: fmt.Sprintf("m-%d", m), Job: "j", Dataset: "d",
			Status: StatusSuccess, EventTime: base.Add(time.Duration(m) * time.Minute).Add(5 * time.Second), Rows: 6000}
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		m := n % windowsN
		eng.Ingest(evs[m])
		eng.AdvanceWatermark("j", base.Add(time.Duration(m+1)*time.Minute))
	}
}

// TestIngestionDoesNotDegrade：小规模冒烟护栏，确认摄入路径不随窗口数明显退化。
// （真正的趋势对比用上面的 Benchmark；此处只保证大数据量流程能在线性预算内完成。）
func TestIngestionDoesNotDegrade(t *testing.T) {
	base := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	const windowsN = 3000
	now := func() time.Time { return base }
	eng := NewEngine(Config{WindowSize: time.Minute, AllowedLateness: 0, Now: now, Retention: 64})
	for i := 0; i < 40; i++ {
		eng.AddRule(AlertRule{RuleID: fmt.Sprintf("r-%d", i), Job: "j", Dataset: "d", MinThroughputPerSec: 1e-9})
	}
	deadline := time.Now().Add(3 * time.Second)
	for m := 0; m < windowsN; m++ {
		ev := Event{EventID: fmt.Sprintf("m-%d", m), Job: "j", Dataset: "d",
			Status: StatusSuccess, EventTime: base.Add(time.Duration(m) * time.Minute).Add(5 * time.Second), Rows: 6000}
		if r := eng.Ingest(ev); r.Rejected {
			t.Fatalf("unexpected rejection at m=%d: %s", m, r.Detail)
		}
		eng.AdvanceWatermark("j", base.Add(time.Duration(m+1)*time.Minute))
	}
	if time.Now().After(deadline) {
		t.Fatalf("ingesting %d windows exceeded 3s budget — performance regressed", windowsN)
	}
	if got := eng.RetentionStats().ReclaimedWindows; got != int64(windowsN-64) {
		t.Fatalf("reclaimed=%d want=%d", got, windowsN-64)
	}
	if n := len(eng.Windows("j", "d")); n != 64 {
		t.Fatalf("retained windows=%d want 64", n)
	}
}
