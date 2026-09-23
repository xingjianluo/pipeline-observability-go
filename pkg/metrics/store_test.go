package metrics

import (
	"errors"
	"testing"
	"time"

	"github.com/xingjianluo/pipeline-observability-go/internal/testhelp"
	"github.com/xingjianluo/pipeline-observability-go/pkg/model"
	"github.com/xingjianluo/pipeline-observability-go/pkg/window"
)

func TestFinalizeIsAppendOnly(t *testing.T) {
	t0 := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	spec := window.Spec{Size: time.Minute, Epoch: t0}
	st := New(spec)
	w := spec.Assign(t0)
	key := model.StreamKey{JobID: "j", Dataset: "d"}
	out := Summarize(key, w, []EventPoint{
		{Event: testhelp.Ev("a", "j", "d", t0.Add(5*time.Second), t0.Add(5*time.Second), 10),
			IngestTime: t0.Add(6 * time.Second)},
	}, t0.Add(2*time.Minute))
	if err := st.Finalize(out); err != nil {
		t.Fatalf("first finalize: %v", err)
	}
	if err := st.Finalize(out); !errors.Is(err, ErrAlreadyFinalized) {
		t.Fatalf("duplicate finalize must return ErrAlreadyFinalized, got %v", err)
	}
	// Value stays the first one.
	got, ok := st.Get(key, 0)
	if !ok || got.Records != 10 {
		t.Fatalf("stored value mutated/rejected: ok=%v %s", ok, got.String())
	}
	// Second window lists in order.
	w1 := spec.Assign(t0.Add(time.Minute))
	out1 := Summarize(key, w1, nil, t0.Add(3*time.Minute))
	if err := st.Finalize(out1); err != nil {
		t.Fatalf("finalize window 1: %v", err)
	}
	list := st.List(key)
	if len(list) != 2 || list[0].WindowStart != w.Start || list[1].WindowStart != w1.Start {
		t.Fatalf("list order wrong: %d entries", len(list))
	}
}
