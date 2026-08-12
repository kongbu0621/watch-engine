from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime

from watch_engine._errors import safe_exception_text, safe_exception_type
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
        if not isinstance(self.watch_id, str):
            raise TypeError("watch_id must be a string")
        if not self.watch_id:
            raise ValueError("watch_id must not be empty")
        for field_name, value, method_name in (
            ("trigger", self.trigger, "wait_next"),
            ("observer", self.observer, "observe"),
            ("transition_policy", self.transition_policy, "evaluate"),
        ):
            if not callable(getattr(value, method_name, None)):
                raise TypeError(f"{field_name} must provide callable {method_name}()")
        if not isinstance(self.observer_retry, RetryPolicy):
            raise TypeError("observer_retry must be a RetryPolicy")


class WatchRuntime:
    def __init__(
        self,
        store: SQLiteStore,
        *,
        clock: Callable[[], datetime] = utc_now,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not isinstance(store, SQLiteStore):
            raise TypeError("store must be a SQLiteStore")
        if not callable(clock):
            raise TypeError("clock must be callable")
        if not callable(sleep):
            raise TypeError("sleep must be callable")
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
                extra={"exception_type": safe_exception_type(exc)},
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
                        "attempt": attempt,
                        "max_attempts": retry.max_attempts,
                        "exception_type": safe_exception_type(exc),
                    },
                )
                if attempt == retry.max_attempts:
                    return Observation.failed(
                        observed_at=self._clock(),
                        error=safe_exception_text(exc),
                        evidence={"exception_type": safe_exception_type(exc), "attempts": attempt},
                    )
                self._sleep(retry.delay_for_failure(attempt))
        raise AssertionError("unreachable")


class WatchRunner:
    """Small trigger adapter; scheduling remains separate from watch semantics."""

    def __init__(self, runtime: WatchRuntime) -> None:
        if not isinstance(runtime, WatchRuntime):
            raise TypeError("runtime must be a WatchRuntime")
        self.runtime = runtime

    async def run_next(self, definition: WatchDefinition) -> RunResult:
        await definition.trigger.wait_next()
        return await asyncio.to_thread(self.runtime.run_once, definition)

    async def serve(self, definition: WatchDefinition, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await self.run_next(definition)
