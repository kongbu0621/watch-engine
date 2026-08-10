from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from tests.helpers import NoEventsPolicy, SequenceObserver, StateChangePolicy
from watch_engine import (
    ManualTrigger,
    Observation,
    ObservationStatus,
    RetryPolicy,
    SQLiteStore,
    WatchDefinition,
    WatchRuntime,
)

NOW = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)


def make_definition(observer: SequenceObserver, policy: object | None = None) -> WatchDefinition:
    return WatchDefinition(
        watch_id="example",
        trigger=ManualTrigger(),
        observer=observer,
        transition_policy=policy or StateChangePolicy(),  # type: ignore[arg-type]
        observer_retry=RetryPolicy(
            max_attempts=3, base_delay_seconds=1, maximum_delay_seconds=2
        ),
    )


def test_first_valid_observation_becomes_authoritative(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    observation = Observation.valid({"value": "A"}, observed_at=NOW)

    result = WatchRuntime(store, clock=lambda: NOW).run_once(
        make_definition(SequenceObserver([observation]))
    )

    assert result.events == ()
    assert store.get_authoritative_observation("example") == observation


@pytest.mark.parametrize("status", [ObservationStatus.DEGRADED, ObservationStatus.FAILED])
def test_non_valid_observation_never_replaces_authority(
    tmp_path: Path, status: ObservationStatus
) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    valid = Observation.valid({"value": "A"}, observed_at=NOW)
    non_valid = Observation(
        status=status,
        observed_at=NOW,
        state={"value": "B"},
        error="untrusted result",
    )
    runtime = WatchRuntime(store, clock=lambda: NOW)
    definition = make_definition(SequenceObserver([valid, non_valid]))

    runtime.run_once(definition)
    runtime.run_once(definition)

    assert store.get_authoritative_observation("example") == valid
    assert store.list_observations("example") == [valid, non_valid]
    assert store.list_events("example") == []


def test_next_valid_compares_against_last_authoritative_valid(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    valid_a = Observation.valid({"value": "A"}, observed_at=NOW)
    degraded_b = Observation.degraded(
        observed_at=NOW, state={"value": "B"}, error="parser confidence low"
    )
    valid_b = Observation.valid({"value": "B"}, observed_at=NOW)
    runtime = WatchRuntime(store, clock=lambda: NOW)
    definition = make_definition(SequenceObserver([valid_a, degraded_b, valid_b]))

    runtime.run_once(definition)
    runtime.run_once(definition)
    result = runtime.run_once(definition)

    assert len(result.events) == 1
    assert result.events[0].payload == {
        "previous": {"value": "A"},
        "current": {"value": "B"},
    }
    assert store.get_authoritative_observation("example") == valid_b


def test_same_state_produces_no_duplicate_event(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    values = [
        Observation.valid("A", observed_at=NOW),
        Observation.valid("B", observed_at=NOW),
        Observation.valid("B", observed_at=NOW),
    ]
    runtime = WatchRuntime(store, clock=lambda: NOW)
    definition = make_definition(SequenceObserver(values))

    runtime.run_once(definition)
    runtime.run_once(definition)
    result = runtime.run_once(definition)

    assert result.events == ()
    assert len(store.list_events("example")) == 1
    assert len(store.outbox_rows()) == 1


class FaultyOutboxStore(SQLiteStore):
    def _insert_outbox(
        self, connection: sqlite3.Connection, event_id: str, now: datetime
    ) -> None:
        raise sqlite3.OperationalError("simulated outbox failure")


def test_event_outbox_and_authority_update_are_atomic(tmp_path: Path) -> None:
    store = FaultyOutboxStore(tmp_path / "watch.db", clock=lambda: NOW)
    runtime = WatchRuntime(store, clock=lambda: NOW)
    definition = make_definition(
        SequenceObserver(
            [
                Observation.valid("A", observed_at=NOW),
                Observation.valid("B", observed_at=NOW),
            ]
        )
    )
    runtime.run_once(definition)

    with pytest.raises(sqlite3.OperationalError, match="simulated outbox failure"):
        runtime.run_once(definition)

    authoritative = store.get_authoritative_observation("example")
    assert authoritative is not None and authoritative.state == "A"
    assert len(store.list_observations("example")) == 1
    assert store.list_events("example") == []
    assert store.outbox_rows() == []


def test_observer_exceptions_use_bounded_retry_then_persist_failed(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    sleeps: list[float] = []
    runtime = WatchRuntime(store, clock=lambda: NOW, sleep=sleeps.append)
    definition = make_definition(
        SequenceObserver([RuntimeError("network down")] * 3), NoEventsPolicy()
    )

    result = runtime.run_once(definition)

    assert result.observation.status is ObservationStatus.FAILED
    assert result.observation.error == "network down"
    assert sleeps == [1, 2]
    assert store.get_authoritative_observation("example") is None
