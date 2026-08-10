from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from threading import Event

import pytest

from tests.helpers import NoEventsPolicy, SequenceObserver, StateChangePolicy
from watch_engine import (
    EventDraft,
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
    valid_b = Observation.valid({"value": "B"}, observed_at=NOW.replace(second=1))
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
        Observation.valid("B", observed_at=NOW.replace(second=1)),
        Observation.valid("B", observed_at=NOW.replace(second=2)),
    ]
    runtime = WatchRuntime(store, clock=lambda: NOW)
    definition = make_definition(SequenceObserver(values))

    runtime.run_once(definition)
    runtime.run_once(definition)
    result = runtime.run_once(definition)

    assert result.events == ()
    assert len(store.list_events("example")) == 1
    assert len(store.outbox_rows()) == 1


def test_equal_timestamp_valid_observation_is_evidence_only(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    runtime = WatchRuntime(store, clock=lambda: NOW)
    definition = make_definition(
        SequenceObserver(
            [
                Observation.valid("FIRST", observed_at=NOW),
                Observation.valid("AMBIGUOUS", observed_at=NOW),
            ]
        )
    )

    runtime.run_once(definition)
    result = runtime.run_once(definition)

    assert result.events == ()
    assert [item.state for item in store.list_observations("example")] == [
        "FIRST",
        "AMBIGUOUS",
    ]
    authoritative = store.get_authoritative_observation("example")
    assert authoritative is not None and authoritative.state == "FIRST"


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
                Observation.valid("B", observed_at=NOW.replace(second=1)),
            ]
        )
    )
    runtime.run_once(definition)

    with pytest.raises(sqlite3.OperationalError, match="simulated outbox failure"):
        runtime.run_once(definition)

    authoritative = store.get_authoritative_observation("example")
    assert authoritative is not None and authoritative.state == "A"
    assert [item.state for item in store.list_observations("example")] == ["A", "B"]
    assert store.list_events("example") == []
    assert store.outbox_rows() == []


class ExplodingTransitionPolicy:
    def evaluate(
        self, previous: Observation | None, current: Observation
    ) -> list[EventDraft]:
        if previous is not None:
            raise RuntimeError("policy defect")
        return []


def test_policy_failure_preserves_observation_but_not_promotion(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    runtime = WatchRuntime(store, clock=lambda: NOW)
    definition = make_definition(
        SequenceObserver(
            [
                Observation.valid("A", observed_at=NOW),
                Observation.valid("B", observed_at=NOW.replace(second=1)),
            ]
        ),
        ExplodingTransitionPolicy(),
    )
    runtime.run_once(definition)

    with pytest.raises(RuntimeError, match="policy defect"):
        runtime.run_once(definition)

    assert [item.state for item in store.list_observations("example")] == ["A", "B"]
    authoritative = store.get_authoritative_observation("example")
    assert authoritative is not None and authoritative.state == "A"
    assert store.list_events("example") == []
    assert store.outbox_rows() == []


class BlockingObserver:
    def __init__(self, observation: Observation, started: Event, release: Event) -> None:
        self.observation = observation
        self.started = started
        self.release = release

    def observe(self) -> Observation:
        self.started.set()
        if not self.release.wait(timeout=2):
            raise TimeoutError("test did not release observer")
        return self.observation


def test_older_overlapping_run_is_evidence_only(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    runtime = WatchRuntime(store, clock=lambda: NOW)
    runtime.run_once(
        make_definition(SequenceObserver([Observation.valid("BASE", observed_at=NOW)]))
    )

    old_started = Event()
    release_old = Event()
    old_definition = WatchDefinition(
        watch_id="example",
        trigger=ManualTrigger(),
        observer=BlockingObserver(
            Observation.valid("OLD", observed_at=NOW.replace(second=1)),
            old_started,
            release_old,
        ),
        transition_policy=StateChangePolicy(),
    )
    new_definition = make_definition(
        SequenceObserver(
            [Observation.valid("NEW", observed_at=NOW.replace(second=2))]
        )
    )

    with ThreadPoolExecutor(max_workers=1) as executor:
        older_future = executor.submit(runtime.run_once, old_definition)
        assert old_started.wait(timeout=2)
        newer_result = runtime.run_once(new_definition)
        release_old.set()
        older_result = older_future.result(timeout=2)

    assert len(newer_result.events) == 1
    assert newer_result.events[0].payload == {"previous": "BASE", "current": "NEW"}
    assert older_result.events == ()
    authoritative = store.get_authoritative_observation("example")
    assert authoritative is not None and authoritative.state == "NEW"
    assert [item.state for item in store.list_observations("example")] == [
        "BASE",
        "NEW",
        "OLD",
    ]
    assert [event.payload for event in store.list_events("example")] == [
        {"previous": "BASE", "current": "NEW"}
    ]


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
