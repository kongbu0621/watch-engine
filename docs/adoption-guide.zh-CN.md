# watch-engine 下游采用指南

## 1. 适用范围

本文面向准备在独立工程或功能模块中采用 `watch-engine` 的开发者。模块目标见 [模块需求说明](module-requirements.zh-CN.md)，总体结构见 [架构设计](architecture.zh-CN.md)，当前版本的内部实现见 [实际落地技术方案](implementation.zh-CN.md)。

采用者不需要修改引擎核心，只需：

1. 实现 Observer；
2. 实现 TransitionPolicy；
3. 实现 EventSink；
4. 创建 WatchDefinition；
5. 组合 Runtime、Trigger、Store 和 Dispatcher；
6. 自行负责进程部署、领域配置和秘密管理。

## 2. 什么时候适合采用

适合：

- 需要反复或按信号观测某个目标；
- 需要保存证据并区分有效、降级、失败；
- 需要维护最后可信状态；
- 只有可信状态变化时才产生事件；
- 需要事件持久化、失败重试和重启恢复；
- 可以接受单节点 SQLite 和 at-least-once 投递。

不适合：

- 只执行一次、无需保存历史的一次性脚本；
- 要求多节点分布式协调；
- 要求 exactly-once 外部副作用，但下游无法按 `event_id` 幂等；
- 需要把业务抓取器或通知供应商直接写入引擎核心；
- 需要实时流处理平台或高吞吐消息代理。

## 3. 安装

开发阶段可以从固定 Git Tag 安装：

```bash
python -m pip install "watch-engine @ git+https://github.com/kongbu0621/watch-engine.git@v0.1.0"
```

仓库本地开发：

```bash
python -m pip install -e ".[dev]"
python -m pytest
python -m ruff check .
python -m mypy src
```

下游生产环境应固定明确版本，不直接跟随 `main`。

在维护者完成 PyPI 名称保留并发布可验证产物前，不要执行无来源约束的
`pip install watch-engine`。应使用受信任仓库的精确 Tag/Commit，并在生产构建中校验提交或制品摘要，
避免同名包抢注造成 dependency-confusion 风险。

## 4. 接入职责表

| 能力 | watch-engine | 下游程序 |
|---|---:|---:|
| 调度触发 | ✓ | 选择和配置 |
| 获取领域数据 |  | ✓ |
| 证据解析与质量分类 |  | ✓ |
| 保存 Observation | ✓ |  |
| 维护 Authority | ✓ |  |
| 解释领域状态变化 |  | ✓ |
| 原子创建 Event / Outbox | ✓ |  |
| 事件重试与恢复 | ✓ |  |
| 发送到具体通知/业务系统 |  | ✓ |
| EventSink 幂等 |  | ✓ |
| 业务配置与秘密管理 |  | ✓ |
| 进程监督与部署 |  | ✓ |

## 5. 第一步：实现 Observer

Observer 负责所有领域 I/O 和解析，并返回 Observation。

```python
from datetime import datetime, timezone

from watch_engine import Observation


class InventoryObserver:
    def observe(self) -> Observation:
        # 访问/解析异常直接抛给 Runtime，由引擎按有界策略重试；重试耗尽后
        # 只持久化固定内建异常类别，不保存异常消息或下游自定义类名。
        evidence = fetch_and_parse_inventory()

        if evidence.is_incomplete:
            return Observation.degraded(
                state={"sku": evidence.sku},
                observed_at=datetime.now(timezone.utc),
                evidence={"reason": "required-field-missing"},
                error="required field missing",
            )

        return Observation.valid(
            {
                "sku": evidence.sku,
                "available": evidence.available,
                "price": evidence.price,
            },
            observed_at=datetime.now(timezone.utc),
            evidence={"source": evidence.source_url},
        )
```

要求：

- `observed_at` 必须包含时区，推荐 UTC；
- state 应当可 JSON 序列化；
- `FAILED` 表示无法可靠判断，不等价于业务“不可用”；
- `DEGRADED` 不会覆盖 Authority；
- 证据中不要保存密码、Token 或完整敏感响应；
- Observer 若可能被并发调用，需要自行保证线程安全。

如果希望引擎对暂时错误执行重试，Observer 可以抛出异常；如果 Observer 已经完成分类，应明确返回 `FAILED` 或 `DEGRADED`。

## 6. 第二步：实现 TransitionPolicy

Policy 只解释 Previous Authority 与 Current Observation，不访问网络和外部服务。

```python
from watch_engine import EventDraft


class InventoryTransitions:
    def evaluate(self, previous, current):
        if previous is None:
            return []

        was_available = bool(previous.state["available"])
        is_available = bool(current.state["available"])

        if was_available == is_available:
            return []

        return [
            EventDraft(
                event_type="inventory.availability.changed",
                severity="notice" if is_available else "warning",
                dedupe_key=(
                    f"inventory:{current.state['sku']}:"
                    f"{was_available}:{is_available}"
                ),
                subject={"sku": current.state["sku"]},
                payload={
                    "previous_available": was_available,
                    "available": is_available,
                    "price": current.state.get("price"),
                },
            )
        ]
```

Policy 必须：

- 快速返回；
- 对相同输入给出相同结果；
- 不进行网络、文件或阻塞 I/O；
- 不发送通知；
- 不修改 SQLite 之外的状态；
- 不产生无法随事务回滚的副作用。

Policy 可以决定首次有效 Observation 是否产生事件。建议默认不把初始基线当作“状态变化”，除非业务明确需要。

## 7. 第三步：实现幂等 EventSink

EventSink 在数据库事务提交后执行。投递是 at-least-once，同一 `event_id` 可能被调用多次。
成功必须返回 `None`，失败必须抛出异常；返回 `False`、响应对象或其他非 `None` 值会被引擎按失败
处理，不能作为隐式成功/失败信号。

```python
class NotificationSink:
    def __init__(self, client):
        self.client = client

    def deliver(self, event):
        self.client.send(
            idempotency_key=event.event_id,
            event=event.to_dict(),
        )
```

正确做法：

- 把 `event_id` 传给支持幂等键的下游服务；或
- 在 Sink 自己的持久存储中记录已接受的 `event_id`；或
- 让通知网关按 `event_id` 去重。

仅使用进程内 `set` 只能作为示例，不能跨重启保证幂等。

## 8. 第四步：组合 WatchDefinition

```python
from watch_engine import (
    IntervalTrigger,
    WatchDefinition,
)

definition = WatchDefinition(
    watch_id="inventory-cn-target",
    trigger=IntervalTrigger(90, 150),
    observer=InventoryObserver(),
    transition_policy=InventoryTransitions(),
)
```

`watch_id`：

- 在数据库生命周期内保持稳定；
- 表达逻辑监视目标，而不是一次运行；
- 不包含秘密；
- 不因部署重启而变化；
- 如果语义完全改变，应使用新 `watch_id` 或明确迁移。

## 9. 第五步：执行观测

单次运行：

```python
from watch_engine import SQLiteStore, WatchRuntime

store = SQLiteStore("watch-engine.db")
runtime = WatchRuntime(store)

result = runtime.run_once(definition)
```

等待 Trigger 后运行一次：

```python
import asyncio

from watch_engine import WatchRunner

runner = WatchRunner(runtime)
result = asyncio.run(runner.run_next(definition))
```

下游可以在自己的应用循环中反复调用，但不得假设引擎 v0.1 自带 daemon 或 systemd 配置。
如果代码本来已经运行在 asyncio Event Loop 中，应在 `async def` 内直接 `await`，不要再次调用
`asyncio.run()`。

## 10. 第六步：运行 Outbox Dispatcher

```python
from watch_engine import OutboxDispatcher

dispatcher = OutboxDispatcher(store, NotificationSink(client))
delivery_results = dispatcher.dispatch_ready()
```

v0.1 的部署基线是：一个 SQLite 数据库只由一个本机监控进程拥有，Runtime 与 Dispatcher 都在
该进程内运行。增加监控目标时，优先在同一进程内串行执行，不通过增加进程提高吞吐。只有未来出现
大量独立目标或分布式部署等明确需求时，才重新设计租约、并发和存储架构。新进程只能在旧进程完全
退出后启动，因为新 Dispatcher 会把遗留 `DELIVERING` 视为上一个投递进程已经中断。
`attempts` 只是已完成投递次数，不是唯一 claim token；不得用它把两个重叠 Dispatcher 误认为安全。

`WatchRunner.serve()` 只在两次运行之间检查 stop event。若它正在等待长 Interval/Cron，设置 stop
不会立即唤醒等待；要求及时停机时，下游应取消外层 asyncio Task 或实现自己的信号/超时编排，并
等待已经进入 `run_once()` 的同步工作和 SQLite 事务安全结束。

## 11. 完整最小结构

推荐的下游目录：

```text
your-monitor/
├── pyproject.toml
├── src/your_monitor/
│   ├── __init__.py
│   ├── observer.py
│   ├── transitions.py
│   ├── sink.py
│   ├── config.py
│   └── main.py
└── tests/
    ├── test_observer.py
    ├── test_transitions.py
    ├── test_sink.py
    └── test_integration.py
```

不要把这些领域实现提交到 `watch-engine/src/watch_engine/`。

## 12. 下游测试要求

采用者至少应测试：

### Observer

- 正常响应产生 `VALID`；
- 缺失或可疑证据产生 `DEGRADED`；
- 访问/解析失败不会伪造成领域状态；
- 时间戳包含时区；
- state 与 evidence 不包含秘密。
- 不修改或跨线程共享已经返回的 Observation 嵌套 JSON；模型会隔离原始输入，但嵌套容器不是深只读。

### TransitionPolicy

- 初始基线行为；
- 无变化不产生事件；
- 每种有效变化产生预期事件；
- 相同输入行为确定；
- 不执行外部 I/O。

### EventSink

- 相同 `event_id` 重复调用不会产生重复外部副作用；
- 成功返回 `None`，失败抛出异常，非 `None` 返回值不会被静默确认；
- 暂时失败可安全重试；
- 永久失败可诊断；
- 发送内容符合 Watch Event v1。

### 端到端

至少验证：

1. 首次 `VALID` 建立 Authority；
2. `FAILED` / `DEGRADED` 不改变 Authority；
3. 后续 `VALID` 与上次 Authority 比较；
4. 有效变化创建 Event 和 Outbox；
5. Sink 失败后重试；
6. 重启后继续投递；
7. 同一事件重复投递被 Sink 幂等处理。
8. `get_watch_status()` 能在成功和错误运行后返回 typed 诊断，业务日志不得直接输出含调用方字段的
   整个诊断对象。

## 13. 跨工程事件消费

当事件离开 Python 进程或被其他模块消费时，应按固定版本的 Schema 验证。`v0.1.1` 起可通过
`load_watch_event_schema()` 读取 wheel 内置副本，也可从同版本 Release Tag 获取并随消费者固定保存。

验证，而不是：

- import 引擎内部数据库模型；
- 直接读取 SQLite 表；
- 根据 README 示例猜测字段；
- 每次运行时读取会变化的 `main` 分支 Schema；
- 使用 `dedupe_key` 替代 `event_id`。

消费者应保留未知的可选字段，并明确记录自己支持的 Schema 版本。

## 14. 配置与秘密

推荐：

- 普通业务配置放下游配置文件或环境变量；
- Token、密码和证书使用部署环境的秘密管理方式；
- SQLite 文件放在明确的持久数据目录；
- 日志中对 URL 参数、Header 和响应内容脱敏；
- `watch_id`、state、evidence、event payload 均不保存秘密。

`watch-engine` 不负责替下游加载或保管秘密。

## 15. 数据保留与删除

采用者必须根据业务用途确定保留期限，并由自己的调度或运维系统定期执行清理。引擎不会静默删除
数据，也不会替业务选择法律保留期限。

```python
from datetime import UTC, datetime, timedelta

cutoff = datetime.now(UTC) - timedelta(days=30)
result = store.purge_before(cutoff)
print(
    {
        "observations_deleted": result.observations_deleted,
        "events_deleted": result.events_deleted,
        "outbox_rows_deleted": result.outbox_rows_deleted,
        "delivery_attempts_deleted": result.delivery_attempts_deleted,
    }
)
```

`purge_before()` 只清理 `DELIVERED/DEAD` 事件历史和非 Authority Observation；`PENDING`、
`RETRY`、`DELIVERING` 以及当前 Authority 不会被删除。可通过 `watch_id=` 只清理一个 Watch。

彻底删除 Watch 前先停止对应 Runner 和 Dispatcher。`delete_watch()` 默认拒绝尚有未投递事件的
Watch；只有明确接受事件丢失时才使用 `allow_undelivered=True`。停止所有数据库所有者后，可以调用
`compact_storage()` 做 WAL checkpoint 与 `VACUUM`。数据库外的备份、快照和日志必须单独清理。

## 16. 版本升级

下游应：

1. 固定精确 Tag 或受控版本范围；
2. 阅读 Release Notes；
3. 在测试环境升级；
4. 运行自己的端到端采用测试；
5. 检查 Public API 与 Watch Event Schema；
6. 备份持久数据库后再执行涉及 Schema migration 的升级。

watch-engine SQLite 文件必须保持引擎独占。不要在其中添加业务表、View、Trigger 或自定义 Index；
这些对象会使下次启动的只读身份校验失败。已有表或 Index 的 DDL 语义变化（包括排序、Collation、
`AUTOINCREMENT` 或约束集合）同样会被拒绝；业务数据和个人信息应放在采用方自己的独立存储中。

Semantic Versioning 解释：

- Patch：兼容性修复；
- Minor：向后兼容能力；
- Major：Public API 的破坏性变化。

事件 Schema 独立带版本，不能只依据 Python 包版本推断消息格式。

## 17. 采用完成标准

一个下游只有满足以下条件，才算真正完成采用：

- 没有修改 `watch-engine` 核心源码；
- Observer、Policy、Sink 位于下游仓库；
- 只依赖 Public API 和公开 Schema；
- 明确处理 `VALID`、`DEGRADED`、`FAILED`；
- Sink 按 `event_id` 幂等；
- 端到端测试覆盖失败和重启；
- 固定引擎版本；
- 部署配置、秘密和进程监督由下游管理；
- 真实运行产生的通用缺口与领域需求被分别记录。

## 18. 反馈通用缺口

发现问题时先判断：

应反馈到 `watch-engine`：

- 所有下游都可能遇到的 Authority、事务、重试或契约问题；
- Public API 无法表达通用采用需求；
- 文档与实现不一致；
- 独立测试可复现的核心缺陷。

应留在下游：

- 特定厂商页面结构、产品编号、价格和库存语义；
- 某个服务的认证与速率限制；
- 微信、邮件等通知格式；
- 下游进程部署策略；
- 只属于单一业务的状态和规则。

只有经过这种边界判断，`watch-engine` 才能保持为真正可复用的独立功能模块，而不是逐渐变成某个应用的公共杂物箱。
