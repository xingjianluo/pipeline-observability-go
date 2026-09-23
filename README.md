# 流水线可观测性组件（本地可运行）

用本地确定性事件流模拟作业运行数据，计算按**作业 × 数据集 × 时间窗口**维度的
新鲜度（freshness）、处理延迟（latency）、吞吐（throughput）与失败率指标，
并在窗口确认时做阈值告警。所有结论满足：

- 与事件到达顺序无关（乱序、迟到在容忍范围内补入，结果与有序到达一致）；
- 对重复投递幂等（同一事件不会重复计数）；
- 已确认窗口的指标与告警**永久冻结、不回退**；
- 超出迟到容忍的事件**不静默丢弃**：被明确拒绝并记录可区分原因；
- 同一持续故障区间告警只触发一次，恢复后再次发生可重新触发；
- 每条告警都携带构成该窗口的原始事件 ID，可由原始事件复现判定。

## 目录结构

```
pipeline/
  event.go     事件、摄入结果、拒绝原因
  window.go    滚动窗口聚合与指标
  alert.go     阈值规则、告警、去重抑制/恢复
  engine.go    线程安全引擎：摄入、水位推进、窗口确认
  *_test.go    乱序/迟到/重复/并发/告警抑制与恢复等用例
cmd/demo/      本地确定性事件流演示
```

## 快速开始

```bash
go run ./cmd/demo        # 本地事件流演示
go test ./...            # 全部用例
go test -race ./...      # 含并发竞态检测
go test -v ./...         # 查看每个用例
```

## 事件语义

`pipeline.Event`：

| 字段 | 含义 |
| --- | --- |
| `EventID` | 事件唯一标识。同一 `(Job, Dataset, EventID)` 视为同一条事件，用于去重。 |
| `Job` / `Dataset` | 作业名与数据集名。指标、水位、告警均按该二元组独立计算，互不污染。 |
| `EventTime` | **业务时间**（事件实际发生时间）。窗口归属以此为准。 |
| `IngestTime` | **摄入时间**（到达观测组件的时间）。为零值时由引擎时钟填充。 |
| `Status` | `success` / `failed`。 |
| `Rows` | 本批次产出行数，计入吞吐；不可为负。 |
| `WatermarkHint` | 可选，告知引擎“该流不会再有早于此时间的事件”，用于推进水位。 |

非法事件（缺字段、未知状态、负行数等）以原因 `invalid_event` 拒绝并记入拒绝台账。

每条输入事件都有明确去向，`Ingest` 返回的 `IngestResult` 三者恰居其一：

- **接受**（`Accepted=true`）；
- **重复**（`Duplicate=true`）：此前已摄入，幂等忽略——即使窗口已确认，
  重复事件仍然识别为重复，而**不是**按迟到拒绝，也不重复计数；
- **拒绝**（`Rejected=true`，`Reason` 区分原因，`Detail` 给出水位与边界等判定依据）：
  - `late_beyond_tolerance`：超出允许迟到范围；
  - `invalid_event`：非法事件。

被拒绝事件可用 `Engine.RejectedEvents()` 取回，绝不静默丢弃。

## 窗口、水位推进与迟到容忍

- 窗口为**滚动窗口**，长度 `Config.WindowSize`（默认 1 分钟），
  归属规则：`start = EventTime.Truncate(WindowSize)`，区间 `[start, start+size)`。
- 每个 `(Job, Dataset)` 流维护一个**事件时间水位 watermark**，取以下三者最大值：
  1. 已观测到的最大 `EventTime`；
  2. 事件携带的 `WatermarkHint`；
  3. `Engine.AdvanceWatermark(job, t)` 显式推进（作用于该作业下所有数据集）。
- 迟到容忍 `Config.AllowedLateness`：
  - 接受条件：事件不重复，且其归属窗口仍开放，或
    `EventTime >= watermark - AllowedLateness`（容忍范围内的迟到/乱序可补入）；
  - 拒绝条件：`EventTime < watermark - AllowedLateness` 且归属窗口已确认
    （或属于一个本应早已确认的未观测窗口）→ `late_beyond_tolerance`。
- **窗口确认（关闭）**：当 `watermark >= 窗口结束时间 + AllowedLateness` 时，
  窗口在下次摄入/水位推进时被确认。确认时从去重后的事件**一次性**计算指标、
  评估告警，随后状态与数值**冻结**：之后任何迟到事件、重复事件、水位推进都
  不会再改变该窗口（迟到超界者拒绝，重复者幂等忽略）。

> 因为 watermark 会随观测到的最大事件时间自动前进，若希望人为制造“容忍内迟到”，
> 应在更晚事件到达之前先摄入迟到事件，或调大 `AllowedLateness`。

## 指标定义（窗口确认时一次性计算）

| 指标 | 定义 |
| --- | --- |
| `EventCount` / `SuccessCount` / `FailedCount` | 去重后的事件数及按状态分类计数。 |
| `Rows` | 去重事件行数总和。 |
| `ThroughputPerSec` | `Rows / 窗口秒长`。 |
| `MaxProcessingLatency` | `max(IngestTime - EventTime)`（不为负）。 |
| `FreshnessLag` | 窗口应产出时间 `Window.End` 到**最晚成功事件摄入时间**的滞后；无成功事件时退化为最晚摄入时间；提前产出记 `0`。 |
| `SuccessRate` | `SuccessCount / EventCount`。 |

指标只在窗口确认时由完整去重事件集算出，因此乱序到达与有序到达结果完全相同；
窗口一旦确认数值冻结，天然单调稳定、不回退。重复回放同一批事件到全新引擎，
会得到逐窗口一致的结果。

## 告警判定规则

`AlertRule` 按 `(Job, Dataset)` 匹配，零值阈值表示**不启用**该类检查：

| 字段 | 触发条件 | `AlertKind` |
| --- | --- | --- |
| `MaxFreshnessLag > 0` | `FreshnessLag > MaxFreshnessLag` | `freshness_breach` |
| `MaxProcessingLatency > 0` | `MaxProcessingLatency > MaxProcessingLatency` | `latency_breach` |
| `MinThroughputPerSec > 0` | `ThroughputPerSec < MinThroughputPerSec` | `throughput_breach` |
| `MaxFailureRate > 0` | `FailedCount/EventCount > MaxFailureRate` | `failure_rate_breach` |

判定时机与依据：

1. 告警只在**窗口确认时**评估一次，依据是冻结后的窗口指标；
2. **去重抑制**：同一 `(Job, Dataset, AlertKind)` 连续窗口持续越限，
   只在首次越限窗口触发一条告警，后续窗口抑制，不重复通知；
3. **恢复后重新触发**：任一确认窗口不再越限即视为恢复，清除持续故障区间；
   之后再次越限会重新触发；
4. 不同失败维度产生不同 `AlertKind`，`Reason` 文本包含窗口、指标观测值、
   阈值与触发条件，`EventIDs` 包含参与该窗口计算的全部去重事件 ID——
   据此可从原始事件重放窗口并复算出 `Observed`，完整复现判定。

告警通过 `Engine.Alerts(job, dataset)` 读取（按触发顺序返回副本）。

## 并发模型

引擎内部以单一互斥锁串行化所有状态变更，摄入/推进/查询方法均可并发调用。
`Config.Now` 可注入确定性时钟，便于测试与本地演示复现。

## 测试覆盖

- 乱序事件在容忍范围内补入，指标与有序到达一致；
- 迟到事件：容忍范围内接受、超范围拒绝（`late_beyond_tolerance`）且已确认窗口不被改写；
- 重复投递：窗口开放/确认后的重复均幂等，拒绝台账不包含重复事件；
- 非法事件分类拒绝；
- 指标在不同到达顺序下一致、确认后冻结不回退、整批重放一致；
- 作业/数据集隔离（含水位互不影响）；
- 并发摄入 + 重复投递的去重正确性（`-race`）；
- 告警：持续区间只触发一次、恢复后重新触发、四种维度可区分、由原始事件可复现。

所有断言失败时会打印输入事件列表与判定依据（指标、水位、拒绝/告警详情）。
