from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from watch_engine.models import EventDraft, Observation, WatchEvent


class Observer(Protocol):
    def observe(self) -> Observation:
        """Return one observation or raise a transient/unexpected error."""
        ...


class TransitionPolicy(Protocol):
    def evaluate(
        self, previous: Observation | None, current: Observation
    ) -> Sequence[EventDraft]:
        """Interpret two authoritative domain states and emit zero or more events."""
        ...


class EventSink(Protocol):
    def deliver(self, event: WatchEvent) -> None:
        """Deliver an event; implementations must deduplicate retries by event_id."""
        ...


class Trigger(Protocol):
    async def wait_next(self) -> None:
        """Wait until one watch execution should begin."""
        ...
