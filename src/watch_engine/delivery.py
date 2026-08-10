from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime, timedelta

from watch_engine._time import utc_now
from watch_engine.interfaces import EventSink
from watch_engine.models import DeliveryConfig, DeliveryResult
from watch_engine.storage import SQLiteStore

logger = logging.getLogger(__name__)


class OutboxDispatcher:
    """Deliver durable events at least once, preserving stable event identifiers."""

    def __init__(
        self,
        store: SQLiteStore,
        sink: EventSink,
        *,
        config: DeliveryConfig | None = None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.store = store
        self.sink = sink
        self.config = config or DeliveryConfig()
        self._clock = clock
        self._recovered = False

    def dispatch_ready(self) -> tuple[DeliveryResult, ...]:
        now = self._clock()
        if not self._recovered:
            recovered = self.store.recover_in_flight(now=now)
            if recovered:
                logger.info("recovered interrupted outbox deliveries", extra={"count": recovered})
            self._recovered = True
        claimed_events = self.store.claim_due(now=now, limit=self.config.batch_size)
        results: list[DeliveryResult] = []
        for claimed in claimed_events:
            try:
                self.sink.deliver(claimed.event)
            except Exception as exc:
                error = str(exc)
                failure_number = claimed.attempts + 1
                retry_at = None
                if failure_number < self.config.retry.max_attempts:
                    delay = self.config.retry.delay_for_failure(failure_number)
                    retry_at = now + timedelta(seconds=delay)
                self.store.record_delivery_failure(
                    claimed, error, retry_at=retry_at, now=now
                )
                logger.warning(
                    "event delivery failed",
                    extra={
                        "event_id": claimed.event.event_id,
                        "watch_id": claimed.event.watch_id,
                        "attempt": failure_number,
                        "will_retry": retry_at is not None,
                    },
                    exc_info=True,
                )
                results.append(
                    DeliveryResult(event_id=claimed.event.event_id, delivered=False, error=error)
                )
            else:
                self.store.record_delivery_success(claimed, now=now)
                results.append(DeliveryResult(event_id=claimed.event.event_id, delivered=True))
        return tuple(results)
