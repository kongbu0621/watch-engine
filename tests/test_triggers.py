from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path

from tests.helpers import NoEventsPolicy, SequenceObserver
from watch_engine import (
    CronTrigger,
    IntervalTrigger,
    ManualTrigger,
    Observation,
    SQLiteStore,
    WatchDefinition,
    WatchRunner,
    WatchRuntime,
)


def test_interval_jitter_stays_in_configured_range() -> None:
    values = iter([90.0, 117.5, 150.0])
    trigger = IntervalTrigger(90, 150, random_uniform=lambda _low, _high: next(values))
    assert [trigger.next_delay() for _ in range(3)] == [90.0, 117.5, 150.0]


def test_exponential_backoff_has_explicit_cap() -> None:
    from watch_engine import RetryPolicy

    retry = RetryPolicy(
        max_attempts=10, base_delay_seconds=2, maximum_delay_seconds=10, multiplier=2
    )
    assert [retry.delay_for_failure(number) for number in range(1, 7)] == [2, 4, 8, 10, 10, 10]


def test_cron_trigger_calculates_next_regular_schedule() -> None:
    trigger = CronTrigger("0 9 * * *", timezone=UTC)
    after = datetime(2025, 1, 1, 8, 30, tzinfo=UTC)
    assert trigger.next_fire_time(after) == datetime(2025, 1, 1, 9, 0, tzinfo=UTC)


def test_manual_trigger_releases_one_watch_execution(tmp_path: Path) -> None:
    async def scenario() -> None:
        now = datetime(2025, 1, 1, tzinfo=UTC)
        trigger = ManualTrigger()
        store = SQLiteStore(tmp_path / "watch.db", clock=lambda: now)
        definition = WatchDefinition(
            watch_id="manual-watch",
            trigger=trigger,
            observer=SequenceObserver([Observation.valid("ready", observed_at=now)]),
            transition_policy=NoEventsPolicy(),
        )
        runtime = WatchRuntime(store, clock=lambda: now)
        task = asyncio.create_task(WatchRunner(runtime).run_next(definition))
        await asyncio.sleep(0)
        assert not task.done()
        trigger.fire()
        result = await asyncio.wait_for(task, timeout=2)
        assert result.observation.state == "ready"

    asyncio.run(scenario())
