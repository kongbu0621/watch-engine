from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from tests.helpers import MultiEventPolicy, SequenceObserver, StateChangePolicy
from watch_engine import (
    DeliveryConfig,
    ManualTrigger,
    Observation,
    ObservationStatus,
    OutboxDispatcher,
    RetryPolicy,
    SQLiteStore,
    WatchDefinition,
    WatchEvent,
    WatchRuntime,
)

NOW = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)


class FailOnceSink:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self._failed = False

    def deliver(self, event: WatchEvent) -> None:
        self.calls.append(event.event_id)
        if not self._failed:
            self._failed = True
            raise RuntimeError("simulated sink outage")


class RecordingSink:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def deliver(self, event: WatchEvent) -> None:
        self.calls.append(event.event_id)


class FailSelectedOnceSink:
    def __init__(self, event_id: str) -> None:
        self.event_id = event_id
        self.calls: list[str] = []
        self._failed = False

    def deliver(self, event: WatchEvent) -> None:
        self.calls.append(event.event_id)
        if event.event_id == self.event_id and not self._failed:
            self._failed = True
            raise RuntimeError("selected event failed")


def definition(
    watch_id: str,
    observations: list[Observation],
    policy: StateChangePolicy | MultiEventPolicy,
) -> WatchDefinition:
    return WatchDefinition(
        watch_id=watch_id,
        trigger=ManualTrigger(),
        observer=SequenceObserver(observations),
        transition_policy=policy,
    )


def test_full_lifecycle_survives_restarts_and_delivery_retry(tmp_path: Path) -> None:
    database = tmp_path / "lifecycle.db"
    watch_id = "lifecycle"
    initial = [
        Observation.valid("A", observed_at=NOW),
        Observation.degraded(
            observed_at=NOW + timedelta(seconds=1), error="partial evidence"
        ),
        Observation.failed(
            observed_at=NOW + timedelta(seconds=2), error="source unavailable"
        ),
    ]
    first_store = SQLiteStore(database, clock=lambda: NOW)
    first_runtime = WatchRuntime(first_store, clock=lambda: NOW)
    first_definition = definition(watch_id, initial, StateChangePolicy())
    for _ in initial:
        assert first_runtime.run_once(first_definition).events == ()

    first_authority = first_store.get_authoritative_observation(watch_id)
    assert first_authority is not None and first_authority.state == "A"
    assert [item.status for item in first_store.list_observations(watch_id)] == [
        ObservationStatus.VALID,
        ObservationStatus.DEGRADED,
        ObservationStatus.FAILED,
    ]
    del first_runtime, first_store

    second_store = SQLiteStore(database, clock=lambda: NOW)
    restored_authority = second_store.get_authoritative_observation(watch_id)
    assert restored_authority is not None and restored_authority.state == "A"
    second_runtime = WatchRuntime(second_store, clock=lambda: NOW)
    transition = second_runtime.run_once(
        definition(
            watch_id,
            [Observation.valid("B", observed_at=NOW + timedelta(seconds=3))],
            StateChangePolicy(),
        )
    )
    assert len(transition.events) == 1
    event_id = transition.events[0].event_id
    assert transition.events[0].payload == {"previous": "A", "current": "B"}

    failed_sink = FailOnceSink()
    retry = RetryPolicy(max_attempts=3, base_delay_seconds=5, maximum_delay_seconds=5)
    first_delivery = OutboxDispatcher(
        second_store,
        failed_sink,
        config=DeliveryConfig(retry=retry),
        clock=lambda: NOW,
    ).dispatch_ready()
    assert [(item.event_id, item.delivered) for item in first_delivery] == [(event_id, False)]
    assert second_store.outbox_rows()[0]["status"] == "RETRY"
    del second_runtime, second_store

    third_store = SQLiteStore(database, clock=lambda: NOW + timedelta(seconds=5))
    success_sink = RecordingSink()
    retry_delivery = OutboxDispatcher(
        third_store,
        success_sink,
        config=DeliveryConfig(retry=retry),
        clock=lambda: NOW + timedelta(seconds=5),
    ).dispatch_ready()
    assert [(item.event_id, item.delivered) for item in retry_delivery] == [(event_id, True)]
    assert success_sink.calls == [event_id]

    repeated = WatchRuntime(third_store, clock=lambda: NOW).run_once(
        definition(
            watch_id,
            [Observation.valid("B", observed_at=NOW + timedelta(seconds=4))],
            StateChangePolicy(),
        )
    )
    assert repeated.events == ()
    assert [item.status for item in third_store.list_observations(watch_id)] == [
        ObservationStatus.VALID,
        ObservationStatus.DEGRADED,
        ObservationStatus.FAILED,
        ObservationStatus.VALID,
        ObservationStatus.VALID,
    ]
    final_authority = third_store.get_authoritative_observation(watch_id)
    assert final_authority is not None and final_authority.state == "B"
    assert [event.event_id for event in third_store.list_events(watch_id)] == [event_id]
    assert [row["status"] for row in third_store.outbox_rows()] == ["DELIVERED"]
    assert [row["status"] for row in third_store.delivery_attempt_rows()] == [
        "FAILED",
        "SUCCEEDED",
    ]


def test_mixed_delivery_batch_retries_only_failed_event(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "mixed-delivery.db", clock=lambda: NOW)
    runtime = WatchRuntime(store, clock=lambda: NOW)
    watch = definition(
        "mixed-delivery",
        [
            Observation.valid("A", observed_at=NOW),
            Observation.valid("B", observed_at=NOW + timedelta(seconds=1)),
        ],
        MultiEventPolicy(),
    )
    runtime.run_once(watch)
    transition = runtime.run_once(watch)
    event_ids = [event.event_id for event in transition.events]
    assert len(event_ids) == 3

    sink = FailSelectedOnceSink(event_ids[1])
    retry = RetryPolicy(max_attempts=3, base_delay_seconds=5, maximum_delay_seconds=5)
    first_batch = OutboxDispatcher(
        store,
        sink,
        config=DeliveryConfig(retry=retry),
        clock=lambda: NOW,
    ).dispatch_ready()

    assert [(item.event_id, item.delivered) for item in first_batch] == [
        (event_ids[0], True),
        (event_ids[1], False),
        (event_ids[2], True),
    ]
    assert sink.calls == event_ids
    assert [row["status"] for row in store.outbox_rows()] == [
        "DELIVERED",
        "RETRY",
        "DELIVERED",
    ]
    assert [row["status"] for row in store.delivery_attempt_rows()] == [
        "SUCCEEDED",
        "FAILED",
        "SUCCEEDED",
    ]

    retry_batch = OutboxDispatcher(
        store,
        sink,
        config=DeliveryConfig(retry=retry),
        clock=lambda: NOW + timedelta(seconds=5),
    ).dispatch_ready()
    assert [(item.event_id, item.delivered) for item in retry_batch] == [
        (event_ids[1], True)
    ]
    assert sink.calls == [*event_ids, event_ids[1]]
    assert [row["status"] for row in store.outbox_rows()] == [
        "DELIVERED",
        "DELIVERED",
        "DELIVERED",
    ]
    assert [row["status"] for row in store.delivery_attempt_rows()] == [
        "SUCCEEDED",
        "FAILED",
        "SUCCEEDED",
        "SUCCEEDED",
    ]
