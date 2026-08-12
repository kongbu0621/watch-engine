from __future__ import annotations

import sqlite3
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


class FalseReturningSink:
    def deliver(self, event: WatchEvent) -> bool:
        return False


class InterruptingSink(RecordingSink):
    def __init__(self) -> None:
        super().__init__()
        self.interrupt_next = True

    def deliver(self, event: WatchEvent) -> None:
        self.calls.append(event.event_id)
        if self.interrupt_next:
            self.interrupt_next = False
            raise KeyboardInterrupt
        if event.event_id not in self._seen:
            self._seen.add(event.event_id)
            self.processed.append(event.event_id)


class FailingAcknowledgementStore(SQLiteStore):
    def __init__(self, path: str | Path, **kwargs: object) -> None:
        super().__init__(path, **kwargs)  # type: ignore[arg-type]
        self.fail_next_acknowledgement = True

    def record_delivery_success(
        self, claimed: ClaimedEvent, *, now: datetime | None = None
    ) -> None:
        if self.fail_next_acknowledgement:
            self.fail_next_acknowledgement = False
            raise OSError("simulated acknowledgement failure")
        super().record_delivery_success(claimed, now=now)


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


def test_same_dispatcher_recovers_claim_after_acknowledgement_failure(
    tmp_path: Path,
) -> None:
    store = FailingAcknowledgementStore(tmp_path / "watch.db", clock=lambda: NOW)
    event = make_pending_event(store)
    sink = RecordingSink()
    dispatcher = OutboxDispatcher(store, sink, clock=lambda: NOW)

    with pytest.raises(OSError, match="acknowledgement failure"):
        dispatcher.dispatch_ready()

    assert store.outbox_rows()[0]["status"] == "DELIVERING"
    result = dispatcher.dispatch_ready()

    assert result[0].delivered is True
    assert sink.calls == [event.event_id, event.event_id]
    assert sink.processed == [event.event_id]
    assert store.outbox_rows()[0]["status"] == "DELIVERED"


def test_same_dispatcher_recovers_claim_after_process_level_interruption(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    event = make_pending_event(store)
    sink = InterruptingSink()
    dispatcher = OutboxDispatcher(store, sink, clock=lambda: NOW)

    with pytest.raises(KeyboardInterrupt):
        dispatcher.dispatch_ready()

    assert store.outbox_rows()[0]["status"] == "DELIVERING"
    assert dispatcher.dispatch_ready()[0].delivered is True
    assert sink.calls == [event.event_id, event.event_id]
    assert sink.processed == [event.event_id]


def test_delivery_timestamps_use_actual_completion_time(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    make_pending_event(store)
    completed_at = NOW + timedelta(seconds=30)
    times = iter((NOW, completed_at))

    OutboxDispatcher(store, RecordingSink(), clock=lambda: next(times)).dispatch_ready()

    assert store.outbox_rows()[0]["delivered_at"] == "2025-01-01T12:00:30Z"


def test_retry_delay_starts_when_failed_delivery_completes(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    make_pending_event(store)
    completed_at = NOW + timedelta(seconds=30)
    times = iter((NOW, completed_at))
    retry = RetryPolicy(max_attempts=2, base_delay_seconds=5, maximum_delay_seconds=5)

    OutboxDispatcher(
        store,
        RecordingSink(failures=1),
        config=DeliveryConfig(retry=retry),
        clock=lambda: next(times),
    ).dispatch_ready()

    row = store.outbox_rows()[0]
    assert row["updated_at"] == "2025-01-01T12:00:30Z"
    assert row["next_attempt_at"] == "2025-01-01T12:00:35Z"


@pytest.mark.parametrize("limit", [0, -1])
def test_claim_due_rejects_non_positive_limit(tmp_path: Path, limit: int) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    with pytest.raises(ValueError, match="at least 1"):
        store.claim_due(now=NOW, limit=limit)


def test_delivery_batch_size_and_claim_limit_are_bounded(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="batch_size must be at most 500"):
        DeliveryConfig(batch_size=501)

    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    with pytest.raises(ValueError, match="limit must be at most 500"):
        store.claim_due(now=NOW, limit=501)


def test_maximum_claim_batch_remains_within_sqlite_variable_limit(
    tmp_path: Path,
) -> None:
    database = tmp_path / "watch.db"
    store = SQLiteStore(database, clock=lambda: NOW)
    timestamp = "2025-01-01T12:00:00Z"
    with sqlite3.connect(database) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(
            """
            INSERT INTO watches(
                watch_id, execution_count, run_status, created_at, updated_at
            ) VALUES (?, 0, 'IDLE', ?, ?)
            """,
            ("batch-watch", timestamp, timestamp),
        )
        events = [
            (
                f"event-{number}",
                "batch-watch",
                "1.0",
                "batch.test",
                "info",
                timestamp,
                f"batch-{number}",
                "{}",
                "{}",
                timestamp,
            )
            for number in range(500)
        ]
        connection.executemany(
            """
            INSERT INTO events(
                event_id, watch_id, schema_version, event_type, severity,
                occurred_at, dedupe_key, subject_json, payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            events,
        )
        connection.executemany(
            """
            INSERT INTO outbox(
                event_id, status, attempts, next_attempt_at, created_at, updated_at
            ) VALUES (?, 'PENDING', 0, ?, ?, ?)
            """,
            ((event[0], timestamp, timestamp, timestamp) for event in events),
        )

    claimed = store.claim_due(now=NOW, limit=500)

    assert len(claimed) == 500


def test_falsey_wrong_typed_time_is_not_replaced_by_clock(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    with pytest.raises(TypeError, match="datetime"):
        store.claim_due(now=0)  # type: ignore[arg-type]


def test_falsey_wrong_typed_retry_time_rolls_back_claim(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    make_pending_event(store)
    claimed = store.claim_due(now=NOW)[0]

    with pytest.raises(TypeError, match="datetime"):
        store.record_delivery_failure(
            claimed,
            "failed",
            retry_at=0,  # type: ignore[arg-type]
            now=NOW,
        )

    assert store.outbox_rows()[0]["status"] == "DELIVERING"
    assert store.delivery_attempt_rows() == []


def test_dispatcher_rejects_falsey_wrong_typed_config(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db")
    with pytest.raises(TypeError, match="DeliveryConfig or None"):
        OutboxDispatcher(store, RecordingSink(), config={})  # type: ignore[arg-type]


def test_dispatcher_rejects_sink_without_callable_delivery(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db")
    with pytest.raises(TypeError, match="callable deliver"):
        OutboxDispatcher(store, object())  # type: ignore[arg-type]


def test_non_none_sink_return_is_a_failed_attempt_not_silent_success(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    make_pending_event(store)

    result = OutboxDispatcher(
        store, FalseReturningSink(), clock=lambda: NOW  # type: ignore[arg-type]
    ).dispatch_ready()

    assert result[0].delivered is False
    assert result[0].error == "TypeError: operation failed"
    assert store.outbox_rows()[0]["status"] == "RETRY"
    assert store.delivery_attempt_rows()[0]["status"] == "FAILED"


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
