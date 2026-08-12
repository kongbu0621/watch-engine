from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from math import inf, nan
from pathlib import Path

import pytest

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


@pytest.mark.parametrize("value", [nan, inf, -inf])
def test_interval_rejects_non_finite_configuration(value: float) -> None:
    with pytest.raises(ValueError, match="finite"):
        IntervalTrigger(value, 10)


def test_interval_rejects_integer_too_large_for_finite_float() -> None:
    with pytest.raises(ValueError, match="finite"):
        IntervalTrigger(10**10_000, 10**10_000)


@pytest.mark.parametrize(
    ("keyword", "value"), [("random_uniform", None), ("sleep", 0)]
)
def test_interval_trigger_rejects_non_callable_dependencies(
    keyword: str, value: object
) -> None:
    with pytest.raises(TypeError, match=f"{keyword} must be callable"):
        IntervalTrigger(1, 2, **{keyword: value})  # type: ignore[arg-type]


@pytest.mark.parametrize("value", [True, "1", nan, inf])
def test_interval_rejects_invalid_custom_random_result(value: object) -> None:
    trigger = IntervalTrigger(1, 2, random_uniform=lambda _low, _high: value)  # type: ignore[arg-type,return-value]
    expected = TypeError if isinstance(value, (bool, str)) else ValueError
    with pytest.raises(expected):
        trigger.next_delay()


def test_exponential_backoff_has_explicit_cap() -> None:
    from watch_engine import RetryPolicy

    retry = RetryPolicy(
        max_attempts=10, base_delay_seconds=2, maximum_delay_seconds=10, multiplier=2
    )
    assert [retry.delay_for_failure(number) for number in range(1, 7)] == [2, 4, 8, 10, 10, 10]


@pytest.mark.parametrize("value", [nan, inf, -inf])
def test_retry_policy_rejects_non_finite_configuration(value: float) -> None:
    from watch_engine import RetryPolicy

    with pytest.raises(ValueError, match="finite"):
        RetryPolicy(base_delay_seconds=value)


def test_retry_policy_rejects_integer_too_large_for_finite_float() -> None:
    from watch_engine import RetryPolicy

    with pytest.raises(ValueError, match="finite"):
        RetryPolicy(maximum_delay_seconds=10**10_000)


def test_retry_policy_caps_extreme_failure_number_without_overflow() -> None:
    from watch_engine import RetryPolicy

    policy = RetryPolicy(
        base_delay_seconds=1,
        maximum_delay_seconds=60,
        multiplier=2,
    )

    assert policy.delay_for_failure(10**100) == 60


def test_cron_trigger_calculates_next_regular_schedule() -> None:
    trigger = CronTrigger("0 9 * * *", timezone=UTC)
    after = datetime(2025, 1, 1, 8, 30, tzinfo=UTC)
    assert trigger.next_fire_time(after) == datetime(2025, 1, 1, 9, 0, tzinfo=UTC)


def test_cron_trigger_rejects_missing_explicit_timezone() -> None:
    with pytest.raises(ValueError, match="timezone must be explicit"):
        CronTrigger("0 9 * * *", timezone=None)  # type: ignore[arg-type]


def test_cron_trigger_rejects_wrong_runtime_types() -> None:
    with pytest.raises(TypeError, match="expression must be a string"):
        CronTrigger(123, timezone=UTC)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="timezone must be a tzinfo"):
        CronTrigger("0 9 * * *", timezone=object())  # type: ignore[arg-type]


@pytest.mark.parametrize(("keyword", "value"), [("clock", None), ("sleep", 0)])
def test_cron_trigger_rejects_non_callable_dependencies(
    keyword: str, value: object
) -> None:
    with pytest.raises(TypeError, match=f"{keyword} must be callable"):
        CronTrigger("0 9 * * *", timezone=UTC, **{keyword: value})  # type: ignore[arg-type]


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
