from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

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
from watch_engine.storage import ClaimedEvent

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


@pytest.mark.parametrize("limit", [0, -1])
def test_claim_due_rejects_non_positive_limit(tmp_path: Path, limit: int) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    with pytest.raises(ValueError, match="at least 1"):
        store.claim_due(now=NOW, limit=limit)


def test_delivery_acknowledgement_must_match_claimed_event(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    event = make_pending_event(store)
    claimed = store.claim_due(now=NOW)[0]
    forged_event = WatchEvent(
        schema_version=event.schema_version,
        event_id="different-event",
        watch_id=event.watch_id,
        event_type=event.event_type,
        severity=event.severity,
        occurred_at=event.occurred_at,
        dedupe_key=event.dedupe_key,
        subject=event.subject,
        payload=event.payload,
    )

    with pytest.raises(RuntimeError, match="no longer active"):
        store.record_delivery_failure(
            ClaimedEvent(claimed.outbox_id, claimed.attempts, forged_event),
            "failed",
            retry_at=None,
            now=NOW,
        )
    with pytest.raises(RuntimeError, match="no longer active"):
        store.record_delivery_success(
            ClaimedEvent(claimed.outbox_id, claimed.attempts + 1, claimed.event), now=NOW
        )

    assert store.outbox_rows()[0]["status"] == "DELIVERING"


def test_state_cycle_can_create_same_transition_again(tmp_path: Path) -> None:
    database = tmp_path / "watch.db"
    store = SQLiteStore(database, clock=lambda: NOW)
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

    assert len(first.events) == 1
    assert len(second.events) == 1
    sink = RecordingSink()
    delivered = OutboxDispatcher(store, sink, clock=lambda: NOW).dispatch_ready()
    assert [result.event_id for result in delivered] == [
        first.events[0].event_id,
        second.events[0].event_id,
    ]

    third = runtime.run_once(definition)
    assert len(third.events) == 1
    assert first.events[0].dedupe_key == third.events[0].dedupe_key
    assert first.events[0].event_id != third.events[0].event_id

    claimed = store.claim_due(now=NOW)
    assert [item.event.event_id for item in claimed] == [third.events[0].event_id]
    sink.deliver(claimed[0].event)
    # Crash after E3 reaches the sink but before its outbox acknowledgement.
    del runtime, store

    reopened_store = SQLiteStore(database, clock=lambda: NOW)
    retried = OutboxDispatcher(reopened_store, sink, clock=lambda: NOW).dispatch_ready()
    assert [result.event_id for result in retried] == [third.events[0].event_id]
    assert sink.calls == [
        first.events[0].event_id,
        second.events[0].event_id,
        third.events[0].event_id,
        third.events[0].event_id,
    ]
    assert sink.processed == [
        first.events[0].event_id,
        second.events[0].event_id,
        third.events[0].event_id,
    ]
    assert len(reopened_store.list_events("dedupe-watch")) == 3
    assert [row["status"] for row in reopened_store.outbox_rows()] == [
        "DELIVERED",
        "DELIVERED",
        "DELIVERED",
    ]


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
