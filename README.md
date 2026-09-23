# Pipeline Observability（本地流水线可观测性组件）

用本地事件流模拟作业运行数据，对**新鲜度（freshness）**、**报告延迟（report latency）**与
**吞吐（throughput）**进行基于事件时间窗口的判定，并产生可去重、可恢复、可由原始事件复现的告警。

核心目标：无论事件以何种**到达顺序**到达、是否**迟到**或**重复投递**、是否**并发摄入**，
已确认时间窗口的结论都保持**一致、单调、可解释**，且不同作业/数据集之间互不污染。

- 语言：Go 1.25，仅用标准库，无外部依赖。
- 本地演示：`go run ./cmd/demo`

---

## 1. 事件语义

每个摄入项是一条 `model.RunEvent`（见 `pkg/model/event.go`）：

| 字段 | 含义 |
| --- | --- |
| `EventID` | 流内唯一标识，用于重复投递判定（必填） |
| `JobID` / `Dataset` | 作业与数据集，共同构成一条独立的流（必填） |
| `EventTime` | 事件实际发生的**业务时间**，窗口归属与水位线都以它为准（必填） |
| `WatermarkTs` | 生产者上报时携带的**数据水位线**，表示“该时刻之前的数据已基本到齐” |
| `Records` | 本次运行产出的记录数（非负），参与吞吐统计 |
| `WatermarkOnly` | 控制事件标记：只推进流水位线，不进入任何窗口的指标计算 |

### 事件分类（绝不静默丢弃）

每次摄入返回 `model.IngestResult`，状态四选一，且非接受结果都带可区分的 `Reason`：

1. `ACCEPTED`：进入其所属窗口（可能是乱序但仍在容忍范围内的事件）。
2. `DUPLICATE`：同一流内 `EventID` 已见过。第一次投递生效，后续投递不改变任何状态，
   结果中回传首次接收时间 `DuplicateOfTime` 作为依据。
   即使重复副本送达时窗口已确认，它仍然归类为 `DUPLICATE`，而不是迟到。
3. `REJECTED_LATE`：事件所属窗口已越过迟到截止线，接收它会改写已经冻结的结论，
   因此明确拒绝；原因串包含窗口结束时间、当前水位线、允许迟到时长与计算出的截止线。
4. `REJECTED_INVALID`：字段校验失败（缺必填、负记录数、控制事件携带记录等），不进入任何窗口。

> 被拒绝/重复的事件都通过返回值显式上报，调用方可以落审计日志或 DLQ；组件内部不做静默丢弃。

### 数据事件 vs 水位线控制事件

普通数据事件既推进水位线（取 `max(EventTime, WatermarkTs)`），也进入窗口指标；
`WatermarkOnly` 控制事件**只推进水位线**，不计入事件数、记录数、延迟或新鲜度水位线，
因此“关闭窗口”这个动作本身不会让一个数据窗口看起来更健康或更繁忙。

---

## 2. 水位线、迟到容忍与窗口推进规则

实现见 `pkg/ingest/ingestor.go`，窗口网格见 `pkg/window`。

- **窗口**：固定大小的滚动（tumbling）窗口，半开区间 `[Start, End)`，由
  `WindowSize` 与可选 `WindowEpoch` 决定网格。索引为负数时使用向下取整（floor），
  保证 epoch 之前的时间也落在正确窗口。
- **每流独立水位线**：对某个 `(JobID, Dataset)`，
  `watermark = max`（所有已接受事件的 `max(EventTime, WatermarkTs)`）。
  水位线状态按流隔离，作业之间不会相互推进或关闭窗口。
- **窗口确认（finalize）条件**：
  当 `watermark >= window.End + AllowedLateness` 时，窗口被确认并冻结。
  - `AllowedLateness = 0` 表示水位线一过窗口上沿立即确认。
  - 确认循环按窗口索引**逐个、连续**推进；两个有数据窗口之间没有事件的“空档”
    会被确认为**显式空窗口**（零事件、零记录、零吞吐），缺口不会被隐藏。
  - 每个流**第一次观测之前**的远古窗口不回填（该流此前不存在）。
- **迟到判定**：事件到达时，若其窗口的 `End + AllowedLateness <= watermark`，
  即 `End + AllowedLateness` 已不在当前水位线之后，则判为 `REJECTED_LATE`。
  换言之：只有仍可能影响“尚未冻结”窗口的事件才会被接受。
- **冻结不可变**：窗口一经确认，其事件集合与 `WindowSummary` 即固定；
  `metrics.Store.Finalize` 对同一窗口的第二次写入返回 `ErrAlreadyFinalized`，绝不覆盖。

### 三个指标的口径（`pkg/metrics/summary.go`）

只基于“该窗口已接受的数据事件集合”做纯函数计算，相同输入永远得到相同输出：

- **事件数 / 记录数**：窗口内去重后的数据事件计数与 `Records` 求和。
- **最大报告延迟** `MaxReportLatency = max(ingestTime - eventTime)`，
  衡量上报本身的延迟（接收时间来自可注入的 `Clock`，测试用固定/步进时钟保证确定性）。
- **新鲜度滞后** `FreshnessLag = max(0, window.End - max(窗口内观测到的 WatermarkTs))`。
  - 空窗口（无任何数据水位线观测）新鲜度滞后记为 0：无产出由“吞吐=0”表达，
    不会用零时间算出荒谬的巨大滞后。
  - 水位线越过窗口末尾时滞后钳制为 0，不出现负值。
- **吞吐** `ThroughputPerSec = Records / 窗口秒数`。

指标只增不改、按窗口索引有序存放；重复回放与并发摄入不会造成重复计数或数值回退。

---

## 3. 告警判定与去重抑制规则

实现见 `pkg/alert/engine.go`。规则 `alert.Rule` 绑定一个 `(JobID, Dataset)`，
阈值指针为 `nil` 的检查项表示禁用：

| 原因 `Reason` | 触发条件（严格比较，相等不算违约） |
| --- | --- |
| `FRESHNESS_BREACH` | `FreshnessLag > MaxFreshnessLag` |
| `REPORT_LATENCY_BREACH` | `MaxReportLatency > MaxReportLatency 阈值` |
| `THROUGHPUT_BELOW_THRESHOLD` | `ThroughputPerSec < MinThroughputPerSec` |

确认窗口按索引单调到达，引擎按 **(规则, 原因)** 维护“连续故障区间（episode）”：

1. 窗口坏且当前没有该原因的活跃区间 → **触发一次**新告警，分配单调 `EpisodeID`。
2. 后续连续坏窗口属于同一持续故障 → **抑制**，不再触发。
3. 出现第一个健康窗口 → 区间**恢复（recover）**，记录 `RecoveredAt`。
4. 恢复之后再次变坏 → 开启**新的区间并重新触发**（新 `EpisodeID`）。
5. 不同原因相互独立：例如新鲜度恢复的同一窗口里延迟开始恶化，不会互相牵连。

每条告警都携带可复现证据：窗口起止时间、观测值与阈值、以及产生该窗口的
**原始事件 ID 列表** `EventIDs` 与人类可读 `Message`。按事件 ID 重放这些原始事件，
即可重新算出同一个窗口摘要与同一条告警结论。

并发安全：摄入器、指标存储、告警引擎、Observer 均内部加锁；
Observer 对一次完整 `Ingest`（含窗口确认、落库、告警评估）串行化，
保证同一次调用产生的告警归属不会在并发调用间串扰。

---

## 4. 包结构

```
pkg/clock     Clock 抽象：RealClock / FixedClock / StepClock（确定性测试与回放）
pkg/window    滚动窗口网格：分配、后继、半开包含、floor 取整
pkg/model     RunEvent、StreamKey、摄入状态/结果、WindowSummary 等类型与诊断串
pkg/ingest    乱序/重复/迟到归类、每流水位线推进、窗口确认
pkg/metrics   纯函数窗口汇总 Summarize + 只增不覆盖的 Store
pkg/alert     阈值规则、连续故障区间、去重抑制与恢复
pkg/observer  门面：串联摄入→确认→落库→告警，一次摄入返回分类与新告警
cmd/demo      本地可运行的事件流演示
internal/testhelp 测试辅助：构造事件，失败时打印输入与判定依据
```

---

## 5. 验证方法

```bash
# 编译与静态检查
go build ./...
go vet ./...

# 全部单元/端到端测试（含竞态检测）
go test ./... -race -count=1

# 只看某个场景（示例）
go test ./pkg/ingest/ -race -v
go test ./pkg/observer/ -race -v -run TestArrivalOrderDeterminism
go test ./pkg/alert/  -v -run TestEpisodeSuppressionAndRecovery

# 本地事件流演示
go run ./cmd/demo
```

### 测试覆盖的场景

- **乱序**：同一窗口内先到晚事件、后到早事件，窗口结论一致
  （`TestOutOfOrderWithinLatenessIsAccepted`、`TestArrivalOrderDeterminism`）。
- **迟到容忍/拒绝**：容忍范围内接受并纳入窗口；超过截止线 `REJECTED_LATE`
  并附原因；零容忍边界；拒绝后已确认窗口不改变
  （`TestLateBeyondLatenessRejected`、`TestZeroLatenessBoundary`、端到端 straggler 用例）。
- **重复投递**：并发/串行重复返回 `DUPLICATE`、不双计数；整段重放幂等、指标不回退、
  告警不重触发（`TestDuplicateDoesNotDoubleCount`、`TestConcurrentIngestIsConsistent`、
  `TestReplayIdempotence`）。
- **并发摄入**：同 ID 并发仅 1 个接受；多作业并发后逐流总量精确、无相互污染
  （`TestConcurrentIngestIsConsistent`、`TestEndToEndConcurrent`，均以 `-race` 运行）。
- **告警抑制与恢复**：持续故障只触发一次、健康窗口恢复、复发重新触发、
  不同原因独立、阈值边界严格（`pkg/alert` 全部用例、`TestEndToEndAlertLifecycleAndLateReject`）。
- **空窗口缺口**：水位线跳跃产生显式空窗口，零指标且归属正确
  （`TestEmptyGapWindowsAreConfirmed`）。

断言失败时，测试辅助 `internal/testhelp` 会打印**全部输入事件**（时间/水位线/记录数）
与**每次摄入的判定依据**（状态、目标窗口、水位线、确认窗口），便于复盘。
