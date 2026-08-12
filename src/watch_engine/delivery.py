from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import cast

from watch_engine._errors import safe_exception_text, safe_exception_type
from watch_engine._time import utc_now
from watch_engine.interfaces import EventSink
from watch_engine.models import DeliveryConfig, DeliveryResult, WatchEvent
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
        if not isinstance(store, SQLiteStore):
            raise TypeError("store must be a SQLiteStore")
        if not callable(getattr(sink, "deliver", None)):
            raise TypeError("sink must provide callable deliver()")
        if config is not None and not isinstance(config, DeliveryConfig):
            raise TypeError("config must be a DeliveryConfig or None")
        if not callable(clock):
            raise TypeError("clock must be callable")
        self.store = store
        self.sink = sink
        self.config = config if config is not None else DeliveryConfig()
        self._clock = clock
        self._recovered = False

    def dispatch_ready(self) -> tuple[DeliveryResult, ...]:
        claim_time = self._clock()
        completed = False
        try:
            if not self._recovered:
                recovered = self.store.recover_in_flight(now=claim_time)
                if recovered:
                    logger.info(
                        "recovered interrupted outbox deliveries", extra={"count": recovered}
                    )
                self._recovered = True
            claimed_events = self.store.claim_due(
                now=claim_time, limit=self.config.batch_size
            )
            results: list[DeliveryResult] = []
            for claimed in claimed_events:
                try:
                    sink_result = cast(
                        Callable[[WatchEvent], object], self.sink.deliver
                    )(claimed.event)
                    if sink_result is not None:
                        raise TypeError("event sink deliver() must return None or raise")
                except Exception as exc:
                    completed_at = self._clock()
                    error = safe_exception_text(exc)
                    failure_number = claimed.attempts + 1
                    retry_at = None
                    if failure_number < self.config.retry.max_attempts:
                        delay = self.config.retry.delay_for_failure(failure_number)
                        retry_at = completed_at + timedelta(seconds=delay)
                    self.store.record_delivery_failure(
                        claimed, error, retry_at=retry_at, now=completed_at
                    )
                    logger.warning(
                        "event delivery failed",
                        extra={
                            "attempt": failure_number,
                            "will_retry": retry_at is not None,
                            "exception_type": safe_exception_type(exc),
                        },
                    )
                    results.append(
                        DeliveryResult(
                            event_id=claimed.event.event_id,
                            delivered=False,
                            error=error,
                        )
                    )
                else:
                    self.store.record_delivery_success(claimed, now=self._clock())
                    results.append(
                        DeliveryResult(event_id=claimed.event.event_id, delivered=True)
                    )
            completed = True
            return tuple(results)
        finally:
            # A claim may already have committed before a clock, row decoding, or
            # acknowledgement failure or process-level interruption. The same
            # Dispatcher must recover those rows on its next invocation instead of
            # leaving them stuck in DELIVERING.
            if not completed:
                self._recovered = False
