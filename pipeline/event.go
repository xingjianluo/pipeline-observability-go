package pipeline

import "time"

// EventStatus 表示一次作业运行/数据集产出的状态。
type EventStatus string

const (
	StatusSuccess EventStatus = "success"
	StatusFailed  EventStatus = "failed"
)

// Event 是流水线运行事件的最小语义单元。
//
// 字段语义：
//   - EventID：事件唯一标识，用于重复投递去重（同一 (Job, Dataset, EventID) 视为同一条事件）。
//   - Job / Dataset：作业名与数据集名，指标与告警均按 (Job, Dataset) 维度独立计算。
//   - EventTime：业务时间（事件实际发生时间），窗口归属以此为准。
//   - IngestTime：摄入时间（到达观测组件的墙上时间），由调用方提供，0 表示由引擎填充。
//   - WatermarkHint：可选，告知引擎“该作业已知不会再有早于此时间的事件”，用于推进水位。
//   - Rows：本批次产出行数，用于吞吐统计。
//   - Seq：单调序号（可选），同一作业内严格递增；为 0 表示不提供。
type Event struct {
	EventID       string
	Job           string
	Dataset       string
	Status        EventStatus
	EventTime     time.Time
	IngestTime    time.Time
	WatermarkHint time.Time
	Rows          int64
	Seq           int64
}

// RejectReason 描述事件被拒绝/单独归类的原因。
type RejectReason string

const (
	// RejectLateBeyondTolerance 事件事件时间早于“已确认窗口下界 - 允许迟到”，
	// 其归属窗口已关闭且结论已冻结，不能再改变任何指标。
	RejectLateBeyondTolerance RejectReason = "late_beyond_tolerance"
	// RejectInvalid 事件缺少必要字段或字段自相矛盾，无法参与计算。
	RejectInvalid RejectReason = "invalid_event"
	// RejectWindowReclaimed 事件归属窗口早已确认，并因保留容量被回收，
	// 迟到/重复事件都不能再写回（回收过程可解释、不静默吞掉）。
	RejectWindowReclaimed RejectReason = "window_reclaimed"
)

// IngestResult 是单条事件摄入的判定结果，保证每条输入都有明确去向。
type IngestResult struct {
	Accepted bool
	// Duplicate 为 true 表示该事件此前已处理过，本次为重复投递，被幂等忽略。
	Duplicate bool
	// Rejected 为 true 表示事件被明确拒绝（不静默丢弃），Reason 给出可区分原因。
	Rejected bool
	Reason   RejectReason
	// Detail 给出人类可读的判定依据（涉及的水位、窗口边界等）。
	Detail string
	// ClosedWindows 本次摄入触发确认（关闭）的窗口数量。
	ClosedWindows int
}
