from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from tests.helpers import SequenceObserver, StateChangePolicy
from watch_engine import (
    DeliveryConfig,
    ManualTrigger,
    Observation,
    OutboxDispatcher,
    RetryPolicy,
    SQLiteStore,
    WatchDefinition,
    WatchEvent,
    WatchRuntime,
)

NOW = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)


class RecordingSink:
    def __init__(self, *, failures: int = 0) -> None:
        self.failures = failures
        self.calls: list[str] = []
        self.processed: list[str] = []
        self._seen: set[str] = set()

    def deliver(self, event: WatchEvent) -> None:
        self.calls.append(event.event_id)
        if self.failures:
            self.failures -= 1
            raise RuntimeError("sink unavailable")
        if event.event_id not in self._seen:
            self._seen.add(event.event_id)
            self.processed.append(event.event_id)


def make_pending_event(store: SQLiteStore) -> WatchEvent:
    definition = WatchDefinition(
        watch_id="delivery-watch",
        trigger=ManualTrigger(),
        observer=SequenceObserver(
            [
                Observation.valid("A", observed_at=NOW),
                Observation.valid("B", observed_at=NOW.replace(second=1)),
            ]
        ),
        transition_policy=StateChangePolicy(),
    )
    runtime = WatchRuntime(store, clock=lambda: NOW)
    runtime.run_once(definition)
    result = runtime.run_once(definition)
    return result.events[0]


def test_delivery_failure_survives_dispatcher_restart(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    event = make_pending_event(store)
    retry = RetryPolicy(max_attempts=3, base_delay_seconds=5, maximum_delay_seconds=20)
    first_sink = RecordingSink(failures=1)

    first = OutboxDispatcher(
        store,
        first_sink,
        config=DeliveryConfig(retry=retry),
        clock=lambda: NOW,
    ).dispatch_ready()
    assert first[0].delivered is False
    assert store.outbox_rows()[0]["status"] == "RETRY"

    second_sink = RecordingSink()
    second = OutboxDispatcher(
        store,
        second_sink,
        config=DeliveryConfig(retry=retry),
        clock=lambda: NOW + timedelta(seconds=5),
    ).dispatch_ready()

    assert second[0].delivered is True
    assert second_sink.processed == [event.event_id]
    assert store.outbox_rows()[0]["status"] == "DELIVERED"
    assert [row["status"] for row in store.delivery_attempt_rows()] == ["FAILED", "SUCCEEDED"]


def test_stable_event_id_supports_downstream_dedupe_after_crash(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    event = make_pending_event(store)
    sink = RecordingSink()

    claimed = store.claim_due(now=NOW)
    sink.deliver(claimed[0].event)
    # Simulate process death after the sink accepted the event but before acknowledgement.
    dispatcher = OutboxDispatcher(store, sink, clock=lambda: NOW)
    result = dispatcher.dispatch_ready()

    assert result[0].delivered is True
    assert sink.calls == [event.event_id, event.event_id]
    assert sink.processed == [event.event_id]


def test_state_cycle_can_create_same_transition_again(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    definition = WatchDefinition(
        watch_id="dedupe-watch",
        trigger=ManualTrigger(),
        observer=SequenceObserver(
            [
                Observation.valid("A", observed_at=NOW),
                Observation.valid("B", observed_at=NOW.replace(second=1)),
                Observation.valid("A", observed_at=NOW.replace(second=2)),
                Observation.valid("B", observed_at=NOW.replace(second=3)),
            ]
        ),
        transition_policy=StateChangePolicy(),
    )
    runtime = WatchRuntime(store, clock=lambda: NOW)

    runtime.run_once(definition)
    first = runtime.run_once(definition)
    second = runtime.run_once(definition)
    third = runtime.run_once(definition)

    assert len(first.events) == 1
    assert len(second.events) == 1
    assert len(third.events) == 1
    assert first.events[0].dedupe_key == third.events[0].dedupe_key
    assert first.events[0].event_id != third.events[0].event_id
    assert len(store.list_events("dedupe-watch")) == 3
    assert len(store.outbox_rows()) == 3


def test_delivery_stops_at_explicit_attempt_limit(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    make_pending_event(store)
    sink = RecordingSink(failures=10)
    retry = RetryPolicy(max_attempts=2, base_delay_seconds=1, maximum_delay_seconds=1)

    first = OutboxDispatcher(
        store, sink, config=DeliveryConfig(retry=retry), clock=lambda: NOW
    )
    assert first.dispatch_ready()[0].delivered is False
    second = OutboxDispatcher(
        store,
        sink,
        config=DeliveryConfig(retry=retry),
        clock=lambda: NOW + timedelta(seconds=1),
    )
    assert second.dispatch_ready()[0].delivered is False

    assert store.outbox_rows()[0]["status"] == "DEAD"
    assert len(store.delivery_attempt_rows()) == 2
    assert second.dispatch_ready() == ()
