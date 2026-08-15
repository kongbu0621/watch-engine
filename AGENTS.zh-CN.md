# 仓库协作指南

[English](AGENTS.md) | 简体中文

## 目标与边界

`watch-engine` 是可复用、领域无关的 Condition Watch Runtime（条件监视运行时）。不得加入与特定
产品、零售商、通知 Provider、Agent 或业务状态绑定的逻辑。领域代码只能通过公开的 `Observer`、
`TransitionPolicy`、`Trigger` 和 `EventSink` Protocol 接入。

当前 0.x 边界为 Python 3.11+、SQLite、进程内调度 Adapter、同步 `Observer`/`EventSink`，以及
异步 `Trigger` Adapter。不要在没有明确需求时加入 Redis、PostgreSQL、Broker、Distributed Lock、
Service Discovery、Kubernetes、Plugin Loader 或 Web UI。

## 不变量（Invariants）

- `Observation`（观测证据）与 Authority（权威状态）必须分离。每一份 Observation 都要持久化，
  但只有 `VALID` 可以替换 Authority。
- 不得把 `DEGRADED` 或 `FAILED` 偷偷解释为领域状态转换。下一份有效状态必须与最后一个有效
  Authority 比较。
- 必须先提交 Observation 证据，再运行 `TransitionPolicy` 或 Authority Promotion（权威提升）
  逻辑。后续失败不得抹掉已经取得的 Observation。
- 有效状态更新、每个新 `WatchEvent` 及其 Outbox 行必须共享同一个 SQLite Transaction。
- `TransitionPolicy.evaluate()` 在该 SQLite Write Transaction 内运行。它应尽可能快速、纯粹且
  确定（deterministic），不得访问网络或外部服务、发送通知、阻塞 I/O、修改数据库之外的状态，
  或产生不可逆 Side Effect。外部 Side Effect 必须放在 Outbox 之后的 `EventSink` 中。
- 多次运行可以在 Observation 阶段重叠。Authority Promotion 必须由 SQLite 串行化；满足
  `observed_at <= authority.observed_at` 的 `VALID` Observation 只能作为证据保存，不能执行 Policy、
  不能提升 Authority，也不能产生 Event。时间戳相等时采用保守的 first-wins。`Observer` 负责保证
  `observed_at` 正确、包含 Timezone，并具有足够精度。
- 每一次真实状态转换都必须获得新的 `event_id`。不得让 `(watch_id, dedupe_key)` 永久唯一，因为
  状态循环可能合法地再次发生同一语义转换。
- 投递语义是 at-least-once（至少一次）。重试必须复用已持久化 Event 的稳定 `event_id`；
  `EventSink` 负责按 `event_id` 实现下游 Idempotency（幂等）。`dedupe_key` 是领域关联上下文，
  不是 Event 的永久身份。
- 投递失败不得回滚或修改 Authority。
- 当前 0.x 每个 SQLite 数据库只允许一个活跃的 `OutboxDispatcher` Owner。新 Dispatcher 会恢复
  所有 `DELIVERING` 行，因此必须等旧 Owner 完全退出后才能启动。
- `WatchRunner.serve()` 采用运行之间的 Cooperative Stop；它不会唤醒正在等待的 Trigger，也不会
  终止已经交给 `asyncio.to_thread` 的同步工作。
- Retry Attempt 次数与 Delay 上限必须显式、可注入并经过测试。
- 使用 timezone-aware UTC `datetime` 和确定性的 JSON Serialization。

## 公开契约（Public Contracts）

`schemas/watch-event-v1.json` 是跨工程 Integration Contract。兼容性变更必须保持 Backward
Compatibility。任何破坏性 Schema 变更都必须创建新的 Schema Version 和新文件；不得静默改写
Event v1 语义。内部 Python Model 不是跨项目契约。

数据库演进从 `SQLiteStore.SCHEMA_VERSION` 开始。当前 0.x 没有 Migration Framework，但 Schema
变化必须识别并拒绝不支持的版本，不能重新解释已有数据。

## 文档基线

这是一个程序仓库，以下三层文档必须保持完整和同步：

1. 产品/模块需求：为什么需要、面向谁、必须提供的行为、边界和验收；
2. 技术架构：Component、Dependency、Data Flow、Invariant 和 Design Rationale；
3. 实际落地技术方案：文件、Class、Schema、Config、Test、CI、Deployment、Release 的映射与状态。

因为 `watch-engine` 是独立可复用模块，还必须维护下游 Adoption Guide（采用指南）。代码、Schema、
Packaging、Runtime、Deployment、Release 或 Public Contract 变化时，必须在同一变更中同步受影响的
文档层。不得把未来计划写成已经实现，也不得把已经发布的 Release 写成待发布。

仓库任意源码目录中，由本仓库维护且面向人的英文 Markdown 文档，都必须有同目录的 `.zh-CN.md`
中文版本，并互相提供语言入口。生成的 Build Output、工具 Cache 和第三方 Metadata 不属于仓库文档。
`.zh-CN.md` 必须包含有实际说明价值的中文正文，不能只把英文副本改成中文文件名。中文版本必须保留
Public API 名、Protocol 名、状态值、Command、File Path 和关键英文工程术语，使读者能够把说明直接
映射回代码和外部资料。两种语言必须在同一变更中更新。

## 必须执行的检查

提交变更前运行：

```bash
python -m pytest
python -m ruff check .
python -m mypy src
python -m build
python -m twine check dist/*
python scripts/verify_sdist_bilingual.py dist/*.tar.gz
python -m pip_audit --local --progress-spinner=off
```

测试不得访问外部网络或真实第三方服务。涉及 Authority、Transaction Boundary、Dedupe、Retry、
Recovery 或 Event Contract 的变更必须增加针对性测试。
