from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime

from watch_engine._errors import safe_exception_text
from watch_engine._time import utc_now
from watch_engine.interfaces import Observer, TransitionPolicy, Trigger
from watch_engine.models import Observation, RetryPolicy, RunResult
from watch_engine.storage import SQLiteStore

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class WatchDefinition:
    watch_id: str
    trigger: Trigger
    observer: Observer
    transition_policy: TransitionPolicy
    observer_retry: RetryPolicy = field(default_factory=RetryPolicy)

    def __post_init__(self) -> None:
        if not self.watch_id:
            raise ValueError("watch_id must not be empty")


class WatchRuntime:
    def __init__(
        self,
        store: SQLiteStore,
        *,
        clock: Callable[[], datetime] = utc_now,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.store = store
        self._clock = clock
        self._sleep = sleep

    def run_once(self, definition: WatchDefinition) -> RunResult:
        self.store.mark_run_started(definition.watch_id, now=self._clock())
        observation = self._observe(definition)
        try:
            events = self.store.record_observation(
                definition.watch_id,
                observation,
                definition.transition_policy,
                now=self._clock(),
            )
        except Exception as exc:
            self.store.mark_run_error(
                definition.watch_id, safe_exception_text(exc), now=self._clock()
            )
            logger.error(
                "observation persistence or promotion failed",
                extra={
                    "watch_id": definition.watch_id,
                    "exception_type": type(exc).__name__,
                },
            )
            raise
        return RunResult(observation=observation, events=events)

    def _observe(self, definition: WatchDefinition) -> Observation:
        retry = definition.observer_retry
        for attempt in range(1, retry.max_attempts + 1):
            try:
                return definition.observer.observe()
            except Exception as exc:
                logger.warning(
                    "observer attempt failed",
                    extra={
                        "watch_id": definition.watch_id,
                        "attempt": attempt,
                        "max_attempts": retry.max_attempts,
                        "exception_type": type(exc).__name__,
                    },
                )
                if attempt == retry.max_attempts:
                    return Observation.failed(
                        observed_at=self._clock(),
                        error=safe_exception_text(exc),
                        evidence={"exception_type": type(exc).__name__, "attempts": attempt},
                    )
                self._sleep(retry.delay_for_failure(attempt))
        raise AssertionError("unreachable")


class WatchRunner:
    """Small trigger adapter; scheduling remains separate from watch semantics."""

    def __init__(self, runtime: WatchRuntime) -> None:
        self.runtime = runtime

    async def run_next(self, definition: WatchDefinition) -> RunResult:
        await definition.trigger.wait_next()
        return await asyncio.to_thread(self.runtime.run_once, definition)

    async def serve(self, definition: WatchDefinition, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await self.run_next(definition)
