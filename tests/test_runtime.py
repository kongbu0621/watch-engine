from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from threading import Event

import pytest

from tests.helpers import MultiEventPolicy, NoEventsPolicy, SequenceObserver, StateChangePolicy
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


@pytest.mark.parametrize(("keyword", "value"), [("clock", None), ("sleep", 0)])
def test_runtime_rejects_non_callable_dependencies(
    tmp_path: Path, keyword: str, value: object
) -> None:
    store = SQLiteStore(tmp_path / "watch.db")
    with pytest.raises(TypeError, match=f"{keyword} must be callable"):
        WatchRuntime(store, **{keyword: value})  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("keyword", "message"),
    [
        ("trigger", "callable wait_next"),
        ("observer", "callable observe"),
        ("transition_policy", "callable evaluate"),
    ],
)
def test_watch_definition_rejects_invalid_collaborators(
    keyword: str, message: str
) -> None:
    values: dict[str, object] = {
        "trigger": ManualTrigger(),
        "observer": SequenceObserver([]),
        "transition_policy": NoEventsPolicy(),
    }
    values[keyword] = object()
    with pytest.raises(TypeError, match=message):
        WatchDefinition(watch_id="watch", **values)  # type: ignore[arg-type]


def test_watch_definition_rejects_oversized_watch_id() -> None:
    with pytest.raises(ValueError, match="must not exceed 2048"):
        WatchDefinition(
            watch_id="x" * 2_049,
            trigger=ManualTrigger(),
            observer=SequenceObserver([]),
            transition_policy=NoEventsPolicy(),
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
    outbox_insert_count = 0

    def _insert_outbox(
        self, connection: sqlite3.Connection, event_id: str, now: datetime
    ) -> None:
        self.outbox_insert_count += 1
        if self.outbox_insert_count == 2:
            raise sqlite3.OperationalError("simulated second outbox failure")
        super()._insert_outbox(connection, event_id, now)


def test_multi_event_promotion_is_all_or_nothing(tmp_path: Path) -> None:
    normal_store = SQLiteStore(tmp_path / "normal.db", clock=lambda: NOW)
    normal_runtime = WatchRuntime(normal_store, clock=lambda: NOW)
    normal_definition = make_definition(
        SequenceObserver(
            [
                Observation.valid("A", observed_at=NOW),
                Observation.valid("B", observed_at=NOW.replace(second=1)),
            ]
        ),
        MultiEventPolicy(),
    )
    normal_runtime.run_once(normal_definition)
    normal_result = normal_runtime.run_once(normal_definition)

    assert len(normal_result.events) == 3
    assert len(normal_store.list_events("example")) == 3
    assert len(normal_store.outbox_rows()) == 3
    normal_authority = normal_store.get_authoritative_observation("example")
    assert normal_authority is not None and normal_authority.state == "B"

    store = FaultyOutboxStore(tmp_path / "fault.db", clock=lambda: NOW)
    runtime = WatchRuntime(store, clock=lambda: NOW)
    definition = make_definition(
        SequenceObserver(
            [
                Observation.valid("A", observed_at=NOW),
                Observation.valid("B", observed_at=NOW.replace(second=1)),
            ]
        ),
        MultiEventPolicy(),
    )
    runtime.run_once(definition)

    with pytest.raises(sqlite3.OperationalError, match="simulated second outbox failure"):
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


class MutatingTransitionPolicy:
    def evaluate(
        self, previous: Observation | None, current: Observation
    ) -> list[EventDraft]:
        assert isinstance(current.state, dict)
        current.state["policy_mutation"] = True
        return []


class InvalidTransitionPolicy:
    def __init__(self, result: object) -> None:
        self.result = result

    def evaluate(self, previous: Observation | None, current: Observation) -> object:
        return self.result


def test_policy_cannot_change_persisted_authority_projection(tmp_path: Path) -> None:
    database = tmp_path / "watch.db"
    store = SQLiteStore(database, clock=lambda: NOW)
    observation = Observation.valid({"original": True}, observed_at=NOW)

    store.record_observation("example", observation, MutatingTransitionPolicy(), now=NOW)

    with sqlite3.connect(database) as connection:
        observation_state = connection.execute(
            "SELECT state_json FROM observations"
        ).fetchone()[0]
        authority_state = connection.execute(
            "SELECT state_json FROM authoritative_states"
        ).fetchone()[0]
    assert observation_state == '{"original":true}'
    assert authority_state == observation_state


def test_policy_failure_preserves_observation_but_not_promotion(tmp_path: Path) -> None:
    database = tmp_path / "watch.db"
    store = SQLiteStore(database, clock=lambda: NOW)
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

    del runtime, store
    reopened_store = SQLiteStore(database, clock=lambda: NOW)
    recovered_result = WatchRuntime(reopened_store, clock=lambda: NOW).run_once(
        make_definition(
            SequenceObserver(
                [Observation.valid("B", observed_at=NOW.replace(second=2))]
            ),
            StateChangePolicy(),
        )
    )

    assert len(recovered_result.events) == 1
    assert recovered_result.events[0].payload == {"previous": "A", "current": "B"}
    assert [item.state for item in reopened_store.list_observations("example")] == [
        "A",
        "B",
        "B",
    ]
    recovered_authority = reopened_store.get_authoritative_observation("example")
    assert recovered_authority is not None and recovered_authority.state == "B"
    assert len(reopened_store.outbox_rows()) == 1


@pytest.mark.parametrize("invalid_result", [None, "not-drafts", [object()]])
def test_invalid_policy_result_preserves_evidence_and_rolls_back_promotion(
    tmp_path: Path, invalid_result: object
) -> None:
    database = tmp_path / "watch.db"
    store = SQLiteStore(database, clock=lambda: NOW)
    runtime = WatchRuntime(store, clock=lambda: NOW)
    definition = make_definition(
        SequenceObserver([Observation.valid("A", observed_at=NOW)]),
        InvalidTransitionPolicy(invalid_result),
    )

    with pytest.raises(TypeError, match="sequence of EventDraft"):
        runtime.run_once(definition)

    assert [item.state for item in store.list_observations("example")] == ["A"]
    assert store.get_authoritative_observation("example") is None
    assert store.list_events("example") == []
    assert store.outbox_rows() == []
    assert read_run_state(database) == ("ERROR", "TypeError: operation failed")


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


class RecordingStateChangePolicy(StateChangePolicy):
    def __init__(self) -> None:
        self.calls: list[tuple[object, object]] = []

    def evaluate(
        self, previous: Observation | None, current: Observation
    ) -> list[EventDraft]:
        self.calls.append((previous.state if previous else None, current.state))
        return super().evaluate(previous, current)


def test_older_overlapping_run_is_evidence_only(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    runtime = WatchRuntime(store, clock=lambda: NOW)
    policy = RecordingStateChangePolicy()
    runtime.run_once(
        make_definition(
            SequenceObserver([Observation.valid("BASE", observed_at=NOW)]), policy
        )
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
        transition_policy=policy,
    )
    new_definition = make_definition(
        SequenceObserver(
            [Observation.valid("NEW", observed_at=NOW.replace(second=2))]
        ),
        policy,
    )
    degraded_definition = make_definition(
        SequenceObserver(
            [
                Observation.degraded(
                    observed_at=NOW.replace(second=3), error="temporary ambiguity"
                )
            ]
        ),
        policy,
    )

    with ThreadPoolExecutor(max_workers=1) as executor:
        older_future = executor.submit(runtime.run_once, old_definition)
        assert old_started.wait(timeout=2)
        newer_result = runtime.run_once(new_definition)
        degraded_result = runtime.run_once(degraded_definition)
        release_old.set()
        older_result = older_future.result(timeout=2)

    assert len(newer_result.events) == 1
    assert degraded_result.events == ()
    assert newer_result.events[0].payload == {"previous": "BASE", "current": "NEW"}
    assert older_result.events == ()
    authoritative = store.get_authoritative_observation("example")
    assert authoritative is not None and authoritative.state == "NEW"
    observations = store.list_observations("example")
    assert [item.status for item in observations] == [
        ObservationStatus.VALID,
        ObservationStatus.VALID,
        ObservationStatus.DEGRADED,
        ObservationStatus.VALID,
    ]
    assert [item.state for item in observations] == ["BASE", "NEW", None, "OLD"]
    assert policy.calls == [(None, "BASE"), ("BASE", "NEW")]
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
    assert result.observation.error == "RuntimeError: operation failed"
    assert sleeps == [1, 2]
    assert store.get_authoritative_observation("example") is None


class InvalidObserver:
    def observe(self) -> object:
        return None


def read_run_state(database: Path) -> tuple[str, str | None]:
    with sqlite3.connect(database) as connection:
        row = connection.execute(
            "SELECT run_status, last_error FROM watches WHERE watch_id = 'example'"
        ).fetchone()
    assert row is not None
    return str(row[0]), row[1]


def test_invalid_observer_result_marks_started_run_as_error(tmp_path: Path) -> None:
    database = tmp_path / "watch.db"
    store = SQLiteStore(database, clock=lambda: NOW)
    definition = WatchDefinition(
        watch_id="example",
        trigger=ManualTrigger(),
        observer=InvalidObserver(),  # type: ignore[arg-type]
        transition_policy=NoEventsPolicy(),
    )

    with pytest.raises(TypeError, match="must return an Observation"):
        WatchRuntime(store, clock=lambda: NOW).run_once(definition)

    assert read_run_state(database) == ("ERROR", "TypeError: operation failed")


def test_retry_sleep_failure_marks_started_run_as_error(tmp_path: Path) -> None:
    database = tmp_path / "watch.db"
    store = SQLiteStore(database, clock=lambda: NOW)

    def broken_sleep(_: float) -> None:
        raise RuntimeError("scheduler failed")

    definition = make_definition(
        SequenceObserver([RuntimeError("observer failed")]), NoEventsPolicy()
    )
    with pytest.raises(RuntimeError, match="scheduler failed"):
        WatchRuntime(store, clock=lambda: NOW, sleep=broken_sleep).run_once(definition)

    assert read_run_state(database) == ("ERROR", "RuntimeError: operation failed")


def test_later_clock_failure_uses_start_time_for_error_state(tmp_path: Path) -> None:
    database = tmp_path / "watch.db"
    store = SQLiteStore(database, clock=lambda: NOW)
    calls = 0

    def failing_clock() -> datetime:
        nonlocal calls
        calls += 1
        if calls == 1:
            return NOW
        raise RuntimeError("clock failed")

    definition = make_definition(
        SequenceObserver([Observation.valid("state", observed_at=NOW)]),
        NoEventsPolicy(),
    )
    with pytest.raises(RuntimeError, match="clock failed"):
        WatchRuntime(store, clock=failing_clock).run_once(definition)

    assert read_run_state(database) == ("ERROR", "RuntimeError: operation failed")
