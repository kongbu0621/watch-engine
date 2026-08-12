# watch-engine

[English](README.md) | 简体中文

`watch-engine` 是一个可复用的 Python 3.11+ 条件监视运行时。它负责调度观测、维护可信状态、
调用领域代码解释状态转换、持久化由此产生的事件，并通过事务型 Outbox 投递这些事件。

它解决轮询式和事件驱动式监视器共有的可靠性问题。它不负责抓取网站，不理解 `AVAILABLE`
之类的领域状态，不向特定服务商发送消息，也不提供分布式调度器或 Web 管理界面。

## 核心概念

- `WatchDefinition` 以稳定的 `watch_id` 组合一个 `Trigger`、`Observer`、
  `TransitionPolicy` 和 Observer 重试策略。
- `Observation` 表示一份观测证据，其状态为 `VALID`、`DEGRADED` 或 `FAILED`。
- Authority（authoritative state，权威状态）是最新的 `VALID` Observation。`DEGRADED` 或
  `FAILED` Observation 会保留用于诊断，但绝不会取代 Authority。
- 只有 `TransitionPolicy` 理解领域状态并返回 `EventDraft`。它应当是纯粹且快速的领域决策逻辑：
  不访问网络或外部服务，不执行阻塞 I/O，不发送通知，不修改 SQLite 数据库之外的状态，也不产生
  其他不可逆副作用。
- `WatchEvent` 是由引擎管理、持久保存的 v1 事件信封。
- `EventSink` 成功返回 `None`、失败抛出异常，并且必须将 `event_id` 作为重试时的幂等键；其他
  返回值按失败处理。

这一区分至关重要：观测失败表示引擎无法可靠地观测目标，并不表示目标发生了状态变化。无论经历
多少次非 `VALID` Observation，下一次 `VALID` Observation 始终与最后一个 authoritative
state 进行比较。

## 最小 Watch 示例

```python
from datetime import datetime, timezone

from watch_engine import (
    EventDraft,
    IntervalTrigger,
    Observation,
    OutboxDispatcher,
    SQLiteStore,
    WatchDefinition,
    WatchRuntime,
)


class HealthObserver:
    def observe(self) -> Observation:
        # Domain I/O and parsing live here, outside watch-engine.
        return Observation.valid(
            {"healthy": True},
            observed_at=datetime.now(timezone.utc),
            evidence={"source": "local-example"},
        )


class HealthTransitions:
    def evaluate(self, previous, current):
        if previous is None or previous.state == current.state:
            return []
        return [
            EventDraft(
                event_type="health.changed",
                severity="warning",
                dedupe_key=f"health:{previous.state!r}:{current.state!r}",
                subject={"service": "example"},
                payload={"previous": previous.state, "current": current.state},
            )
        ]


class SummarySink:
    def __init__(self):
        self.seen = set()

    def deliver(self, event):
        if event.event_id in self.seen:
            return
        self.seen.add(event.event_id)
        # 不记录 subject/payload，避免调用方放入的敏感数据进入日志。
        print({"event_id": event.event_id, "event_type": event.event_type})


store = SQLiteStore("watch-engine.db")
definition = WatchDefinition(
    watch_id="example-health",
    trigger=IntervalTrigger(90, 150),
    observer=HealthObserver(),
    transition_policy=HealthTransitions(),
)

# One observation, independent of scheduling:
result = WatchRuntime(store).run_once(definition)

# Deliver all currently due events. Run this repeatedly in a worker/process loop.
delivery_results = OutboxDispatcher(store, SummarySink()).dispatch_ready()
```

如需调度执行，`await WatchRunner(runtime).run_next(definition)` 会等待一次 Trigger 并执行一次
运行。`ManualTrigger.fire()` 会显式放行一个正在等待的运行。`CronTrigger` 使用常规 cron
表达式，并要求显式指定时区。

## 持久化与投递

`SQLiteStore` 要求使用文件数据库，并会自动初始化版本 1 schema。它保存 Watch 运行元数据、每一份
Observation、authoritative Observation、事件、Outbox 行以及每一次投递尝试。
在 POSIX 系统上，数据库、WAL 和 SHM 文件会被强制设为仅所有者可读写（`0600`），并拒绝
符号链接或具有多个硬链接的数据库路径；部署时还应将父目录设为仅所有者可访问（`0700`）。

持久化过程刻意划分为两个事务边界：

1. 每个已完成的 Observation 首先作为证据提交，同时提交用于诊断的 Watch 运行元数据。
2. 对于未过时的 `VALID` Observation，另一个 `BEGIN IMMEDIATE` 事务负责执行 Policy、
   替换 Authority，并将每个 WatchEvent 及其 Outbox 行一并插入。

如果 Policy 求值或提升过程失败，第二个事务会同时回滚 Authority、Event 和 Outbox，而此前已提交的
Observation 仍可用于诊断。EventSink 的投递失败发生在之后，因此无法回滚或破坏 Authority。

`TransitionPolicy.evaluate(previous, current)` 有意在该 SQLite 写事务中运行，使 Previous
Authority、Policy 决策、Event/Outbox 创建和 New Authority 共同构成一次原子的提升决策。
因此，Policy 应尽可能保持确定性并快速返回。它不得发起网络请求、调用外部服务、发送通知、阻塞
I/O、修改 SQLite 之外的状态或产生无法回滚的副作用。这些操作属于 Outbox 事务提交之后的
`EventSink`。

每次事件发生都会获得新的 `event_id`，包括相同状态转换后来再次发生的情况。`dedupe_key` 是领域
关联上下文，因此 events 表刻意不对它设置唯一约束。投递重试复用已存储的事件和稳定的
`event_id`，绝不会创建第二条事件记录。投递语义明确为 **at-least-once**（至少一次）。进程可能
在 EventSink 接受事件之后、SQLite 记录成功之前退出；下一个进程会再次发送具有同一
`event_id` 的已存储事件。EventSink 必须按 `event_id` 去重。失败的投递尝试使用可配置且有上限的
指数退避，并能在重启后继续。重试耗尽的事件会以 `DEAD` 状态保留，供诊断使用。
v0.1 的 SQLite 后端只支持一个本机拥有进程和一个活跃 Dispatcher。确认时会校验已完成尝试次数，
但它不是租约或唯一 claim token；旧 Dispatcher 与替代 Dispatcher 不得重叠运行。

Observer 抛出的异常会在一次运行内按照该 Watch 的有界重试策略重试。尝试次数耗尽后，运行时会
持久化一份 `FAILED` Observation，其中只包含固定内建异常类别和尝试次数；异常消息与下游自定义
异常类名会被主动丢弃。若 Observer 主动返回
`DEGRADED` 或 `FAILED`，说明它已经对证据完成分类，因此该结果会立即持久化，不会被隐式重试。

所有时间戳都包含时区信息并统一为 UTC。JSON 采用确定性的键排序方式存储。跨项目集成契约是
[`schemas/watch-event-v1.json`](schemas/watch-event-v1.json)；使用方应依据该契约，而不是导入
内部数据库模型。

每个 JSON 字段编码后上限为 1 MiB，标识符/事件元数据标量和错误字段上限为 2,048 字符。数据保留由调用方明确控制：`purge_before(cutoff)` 只删除
旧的终态（`DELIVERED`/`DEAD`）事件历史和非 Authority 观测，`delete_watch()` 默认拒绝删除仍有
未投递事件的 Watch，只有实际布尔值 `True` 才能覆盖该保护；`compact_storage()` 应在其他数据库
使用者停止后执行 checkpoint 和 vacuum。
执行破坏性 Watch 删除或压缩前，必须先停止 Runner 与 Dispatcher。

SQLite 文件是引擎独占的存储边界，不是与业务共用的数据库。重新打开时会在改动文件前校验 v1
全部用户定义 Schema 对象、列、Foreign Key、状态 CHECK、Outbox 事件唯一约束以及规范化后的完整
建表/建索引 SQL；其中包括索引排序与 Collation、`AUTOINCREMENT` 和完整表约束集合。不得向该文件
增加采用方的表、View、Trigger 或 Index；业务数据和个人信息必须使用独立存储。单次投递领取上限
为 500 个事件，默认值为 100。

`get_watch_status(watch_id)` 返回 typed、只读的最新运行诊断快照，下游无需读取 SQLite 内部表。
模型构造时会复制调用方 JSON，frozen dataclass 也禁止字段重新绑定，但模型暴露的嵌套 JSON 容器
不是深只读对象。应把它们当作快照，不要修改或跨并发任务共享；对这些内存容器的修改不会回写已经
持久化的 Observation、Authority 或 Event。

生产使用前请阅读[安全策略](SECURITY.zh-CN.md)与[数据治理策略](DATA-GOVERNANCE.zh-CN.md)。任何引擎字段都不得
包含凭据、个人信息或其他敏感数据。
库自身日志不会输出调用方可控的 Watch/Event 标识或载荷字段。

## 设计与采用文档

- [模块需求说明](docs/module-requirements.zh-CN.md)：定义独立可复用模块的目标、边界、需求和验收标准。
- [架构设计](docs/architecture.zh-CN.md)：说明运行时分层、事务、并发、失败恢复和对外契约。
- [实际落地技术方案](docs/implementation.zh-CN.md)：把当前版本映射到代码、SQLite、配置、测试、CI 和发布流程。
- [下游采用指南](docs/adoption-guide.zh-CN.md)：指导其他模块、其他工程和其他开发者完成真实接入。

## 开发

```bash
python -m pip install -e ".[dev]"
python -m pytest
python -m ruff check .
python -m mypy src
python -m pip_audit . --progress-spinner=off
```

测试套件完全在本地运行，不需要网络或第三方服务。

## v0.1 边界

运行时有意限定为单节点，并在 Observer/EventSink 边界采用同步调用。SQLite 负责协调本地事务，
但它不是分布式锁。

当前部署基线是一个 SQLite 数据库只运行一个本机监控进程，Runner 与 Dispatcher 都由该进程拥有；
增加监控目标时优先在同一进程内串行执行，不盲目增加进程。

同一 `watch_id` 的多个运行可以在 Observer 阶段重叠。Authority 提升由 SQLite 串行化，并按
`Observation.observed_at` 排序。时间戳早于或等于当前 Authority 的 `VALID` Observation 只会
作为证据保留：它不会传给 TransitionPolicy，不会替换 Authority，也不能创建事件。因此，
Observer 必须提供包含时区信息的时间戳，准确表示证据的取得时间。如果调用方并发调用同一个
Observer，该 Observer 必须自行保证线程安全。时间戳相等时采用保守的先到者胜出规则：已经提升的
Authority 继续保持权威，之后完成的 Observation 仅作为证据。时间戳必须包含时区信息，并具有足够
精度，以便对 Observer 实际取得的证据进行排序。

Trigger 适配器是异步的，但 v0.1 不包含 daemon CLI、进程监督器、分布式调度器、动态插件加载器、
PostgreSQL、Redis 或消息代理。

已实现的 Trigger 包括带抖动的时间间隔、cron 调度和进程内手动请求。以后可以增加外部事件、文件
变更和 webhook 等适配器，而无需改变 Observation/Authority/Event 流水线。

## 许可证

本项目采用 Apache License 2.0。参见 [`LICENSE`](LICENSE)。
