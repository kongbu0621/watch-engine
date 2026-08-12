from __future__ import annotations

import asyncio
import math
import random
from collections.abc import Awaitable, Callable
from datetime import datetime, tzinfo

from croniter import croniter

from watch_engine._time import require_aware, utc_now

AsyncSleep = Callable[[float], Awaitable[None]]


class IntervalTrigger:
    def __init__(
        self,
        minimum_interval: float,
        maximum_interval: float,
        *,
        random_uniform: Callable[[float, float], float] = random.uniform,
        sleep: AsyncSleep = asyncio.sleep,
    ) -> None:
        if not callable(random_uniform):
            raise TypeError("random_uniform must be callable")
        if not callable(sleep):
            raise TypeError("sleep must be callable")
        for field_name, value in (
            ("minimum_interval", minimum_interval),
            ("maximum_interval", maximum_interval),
        ):
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise TypeError(f"{field_name} must be a number")
            try:
                normalized = float(value)
            except OverflowError:
                raise ValueError(f"{field_name} must be finite") from None
            if not math.isfinite(normalized):
                raise ValueError(f"{field_name} must be finite")
        if minimum_interval <= 0:
            raise ValueError("minimum_interval must be positive")
        if maximum_interval < minimum_interval:
            raise ValueError("maximum_interval must be >= minimum_interval")
        self.minimum_interval = float(minimum_interval)
        self.maximum_interval = float(maximum_interval)
        self._random_uniform = random_uniform
        self._sleep = sleep

    def next_delay(self) -> float:
        delay = self._random_uniform(self.minimum_interval, self.maximum_interval)
        if isinstance(delay, bool) or not isinstance(delay, (int, float)):
            raise TypeError("random source must return a number")
        try:
            delay = float(delay)
        except OverflowError:
            raise ValueError("random source must return a finite number") from None
        if not math.isfinite(delay):
            raise ValueError("random source must return a finite number")
        if not self.minimum_interval <= delay <= self.maximum_interval:
            raise ValueError("random source returned a value outside the configured interval")
        return delay

    async def wait_next(self) -> None:
        await self._sleep(self.next_delay())


class CronTrigger:
    def __init__(
        self,
        expression: str,
        *,
        timezone: tzinfo,
        clock: Callable[[], datetime] = utc_now,
        sleep: AsyncSleep = asyncio.sleep,
    ) -> None:
        if timezone is None:
            raise ValueError("timezone must be explicit")
        if not isinstance(timezone, tzinfo):
            raise TypeError("timezone must be a tzinfo")
        if not isinstance(expression, str):
            raise TypeError("expression must be a string")
        if not callable(clock):
            raise TypeError("clock must be callable")
        if not callable(sleep):
            raise TypeError("sleep must be callable")
        if not croniter.is_valid(expression):
            raise ValueError(f"invalid cron expression: {expression}")
        self.expression = expression
        self.timezone = timezone
        self._clock = clock
        self._sleep = sleep

    def next_fire_time(self, after: datetime) -> datetime:
        base = require_aware(after, field="after").astimezone(self.timezone)
        result: datetime = croniter(self.expression, base).get_next(datetime)
        return require_aware(result, field="cron result")

    async def wait_next(self) -> None:
        now = require_aware(self._clock(), field="clock result")
        fire_at = self.next_fire_time(now)
        await self._sleep(max(0.0, (fire_at - now).total_seconds()))


class ManualTrigger:
    """An in-process trigger. Each call to ``fire`` releases exactly one execution."""

    def __init__(self) -> None:
        self._requests: asyncio.Queue[None] = asyncio.Queue()

    def fire(self) -> None:
        self._requests.put_nowait(None)

    async def wait_next(self) -> None:
        await self._requests.get()
