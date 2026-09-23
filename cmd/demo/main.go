// Command demo simulates a local event stream for one job/dataset and prints
// ingestion decisions, confirmed window metrics and alert episodes. It needs
// no external dependencies: run it with `go run ./cmd/demo`.
package main

import (
	"fmt"
	"time"

	"github.com/xingjianluo/pipeline-observability-go/pkg/alert"
	"github.com/xingjianluo/pipeline-observability-go/pkg/clock"
	"github.com/xingjianluo/pipeline-observability-go/pkg/model"
	"github.com/xingjianluo/pipeline-observability-go/pkg/observer"
)

func main() {
	t0 := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	maxLag := 30 * time.Second
	maxLatency := 40 * time.Second
	minTput := 5.0

	// Receipt times run a few seconds after each event's own time, giving
	// realistic (small) report latencies instead of an artificial fixed clock.
	o, err := observer.New(&clock.StepClock{T: t0.Add(5 * time.Second), Step: 3 * time.Second}, observer.Config{
		WindowSize:      time.Minute,
		WindowEpoch:     t0,
		AllowedLateness: 30 * time.Second,
		Rules: []alert.Rule{{
			JobID:               "job-etl",
			Dataset:             "events",
			MaxFreshnessLag:     &maxLag,
			MaxReportLatency:    &maxLatency,
			MinThroughputPerSec: &minTput,
		}},
	})
	if err != nil {
		panic(err)
	}

	// off/wm offsets are seconds after t0. Records are the run output count.
	ev := func(id string, off, wm int64, rec int64) model.RunEvent {
		return model.RunEvent{
			EventID: id, JobID: "job-etl", Dataset: "events",
			EventTime:   t0.Add(time.Duration(off) * time.Second),
			WatermarkTs: t0.Add(time.Duration(wm) * time.Second),
			Records:     rec,
		}
	}
	wm := func(id string, at int64) model.RunEvent {
		return model.RunEvent{
			EventID: id, JobID: "job-etl", Dataset: "events",
			EventTime:     t0.Add(time.Duration(at) * time.Second),
			WatermarkTs:   t0.Add(time.Duration(at) * time.Second),
			WatermarkOnly: true,
		}
	}

	// Scripted arrival stream: out-of-order, duplicate, late, healthy, broken.
	stream := []model.RunEvent{
		ev("e1", 10, 10, 600),   // window 0, wm 10:00:10 -> lag 50s
		ev("e2", 50, 50, 100),   // later in window 0
		ev("e1", 10, 10, 600),   // duplicate redelivery
		ev("e3", 5, 5, 100),     // out of order, still within window 0
		wm("c0", 91),            // confirm window 0 -> freshness/throughput alerts
		ev("e4", 70, 70, 100),   // window 1, still bad freshness
		wm("c1", 151),           // confirm window 1 -> suppressed
		ev("e5", 179, 180, 600), // window 2 healthy: wm at end, tput 10/s
		wm("c2", 211),           // confirm window 2 -> recover episodes
		ev("old", 20, 20, 5),    // too late: window 0 long confirmed -> rejected
		ev("e6", 190, 190, 0),   // window 3 bad again -> new episode
		wm("c3", 271),           // confirm window 3 -> re-fires
	}

	for _, e := range stream {
		r := o.Ingest(e)
		fmt.Println("•", r.Ingest)
		for _, a := range r.Alerts {
			fmt.Println("   ⚠ FIRED  ", a)
		}
	}

	fmt.Println("\n=== confirmed window metrics ===")
	key := model.StreamKey{JobID: "job-etl", Dataset: "events"}
	for _, s := range o.Windows(key) {
		fmt.Println("  ", s)
	}

	fmt.Println("\n=== alert episodes (fired -> recovered) ===")
	for _, a := range o.Episodes() {
		state := "ACTIVE"
		if !a.Active() {
			state = "recovered at " + a.RecoveredAt.Format(time.RFC3339)
		}
		fmt.Printf("   %s [%s] %s\n      evidence=%v\n", a.EpisodeID, state, a.Message, a.EventIDs)
	}
}
