from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from watch_engine.models import EventDraft, Observation, WatchEvent


class Observer(Protocol):
    def observe(self) -> Observation:
        """Return one observation or raise a transient/unexpected error."""
        ...


class TransitionPolicy(Protocol):
    """Pure domain decision logic executed inside a SQLite write transaction.

    Implementations must return quickly and should be deterministic for the same
    inputs. They must not perform network calls, access external services, send
    notifications, execute blocking I/O, modify state outside the promotion
    transaction, or produce any other irreversible side effect. Delivery and
    external effects belong in EventSink implementations behind the Outbox.
    """

    def evaluate(
        self, previous: Observation | None, current: Observation
    ) -> Sequence[EventDraft]:
        """Interpret two authoritative domain states and return event drafts."""
        ...


class EventSink(Protocol):
    def deliver(self, event: WatchEvent) -> None:
        """Deliver and return None, or raise; deduplicate retries by event_id."""
        ...


class Trigger(Protocol):
    async def wait_next(self) -> None:
        """Wait until one watch execution should begin."""
        ...
