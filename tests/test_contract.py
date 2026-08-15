from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError

import watch_engine
from watch_engine import (
    DeliveryAttemptDiagnostic,
    DeliveryAttemptStatus,
    DeliveryResult,
    EventDraft,
    Observation,
    ObservationStatus,
    OutboxDiagnostic,
    OutboxStatus,
    PurgeResult,
    RunResult,
    RunStatus,
    SQLiteStore,
    WatchEvent,
    WatchStatus,
    load_watch_event_schema,
)

EXPECTED_PUBLIC_API = {
    "CronTrigger",
    "DeliveryConfig",
    "DeliveryAttemptDiagnostic",
    "DeliveryAttemptStatus",
    "DeliveryResult",
    "EventDraft",
    "EventSink",
    "IntervalTrigger",
    "ManualTrigger",
    "Observation",
    "ObservationStatus",
    "Observer",
    "OutboxDispatcher",
    "OutboxDiagnostic",
    "OutboxStatus",
    "PurgeResult",
    "RetryPolicy",
    "RunStatus",
    "RunResult",
    "SQLiteStore",
    "TransitionPolicy",
    "Trigger",
    "WatchDefinition",
    "WatchEvent",
    "WatchRunner",
    "WatchStatus",
    "WatchRuntime",
    "load_watch_event_schema",
}


def test_public_api_exports_are_explicit_and_importable() -> None:
    assert set(watch_engine.__all__) == EXPECTED_PUBLIC_API
    for name in EXPECTED_PUBLIC_API:
        assert getattr(watch_engine, name) is not None


def test_watch_event_v1_validates_against_published_contract() -> None:
    schema_path = Path(__file__).parents[1] / "schemas" / "watch-event-v1.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    event = WatchEvent(
        schema_version="1.0",
        event_id="evt-1",
        watch_id="watch-1",
        event_type="state.changed",
        severity="info",
        occurred_at=datetime(2025, 1, 1, tzinfo=UTC),
        dedupe_key="resource-1:A:B",
        subject={"resource_id": "resource-1"},
        payload={"previous": "A", "current": "B"},
    ).to_dict()

    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    validator.validate(event)


def test_packaged_schema_matches_repository_contract() -> None:
    schema_path = Path(__file__).parents[1] / "schemas" / "watch-event-v1.json"
    assert load_watch_event_schema() == json.loads(schema_path.read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    "mutation",
    [
        lambda event: event.pop("event_id"),
        lambda event: event.update({"unexpected": True}),
        lambda event: event.update({"occurred_at": "not-a-date"}),
    ],
)
def test_watch_event_v1_schema_rejects_invalid_instances(mutation) -> None:  # type: ignore[no-untyped-def]
    schema_path = Path(__file__).parents[1] / "schemas" / "watch-event-v1.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    event = WatchEvent(
        schema_version="1.0",
        event_id="evt-1",
        watch_id="watch-1",
        event_type="state.changed",
        severity="info",
        occurred_at=datetime(2025, 1, 1, tzinfo=UTC),
        dedupe_key="resource-1:A:B",
        subject={"resource_id": "resource-1"},
        payload={"previous": "A", "current": "B"},
    ).to_dict()
    mutation(event)

    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    with pytest.raises(ValidationError):
        validator.validate(event)


def test_runtime_metadata_limit_does_not_narrow_released_event_v1_contract() -> None:
    too_long = "x" * 2_049
    with pytest.raises(ValueError, match="must not exceed 2048"):
        EventDraft(
            event_type=too_long,
            severity="info",
            dedupe_key="key",
            subject={},
            payload={},
        )

    schema = load_watch_event_schema()
    event = WatchEvent(
        schema_version="1.0",
        event_id="evt-1",
        watch_id="watch-1",
        event_type="state.changed",
        severity="info",
        occurred_at=datetime(2025, 1, 1, tzinfo=UTC),
        dedupe_key="key",
        subject={},
        payload={},
    ).to_dict()
    event["event_id"] = too_long
    Draft202012Validator(schema).validate(event)


def test_event_v1_accepts_every_released_identifier_length() -> None:
    """v0.1.0 allowed every non-empty length; Event v1 must remain compatible."""

    schema = load_watch_event_schema()
    event = {
        "schema_version": "1.0",
        "event_id": "e" * 2_049,
        "watch_id": "w" * 2_049,
        "event_type": "t" * 2_049,
        "severity": "s" * 2_049,
        "occurred_at": "2025-01-01T00:00:00Z",
        "dedupe_key": "d" * 2_049,
        "subject": {},
        "payload": {},
    }

    Draft202012Validator(schema, format_checker=FormatChecker()).validate(event)


def test_naive_event_datetime_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        WatchEvent(
            schema_version="1.0",
            event_id="evt-1",
            watch_id="watch-1",
            event_type="state.changed",
            severity="info",
            occurred_at=datetime(2025, 1, 1),
            dedupe_key="key",
            subject={},
            payload={},
        )


def test_non_datetime_event_timestamp_is_rejected_explicitly() -> None:
    with pytest.raises(TypeError, match="occurred_at must be a datetime"):
        WatchEvent(
            schema_version="1.0",
            event_id="evt-1",
            watch_id="watch-1",
            event_type="state.changed",
            severity="info",
            occurred_at="2025-01-01T00:00:00Z",  # type: ignore[arg-type]
            dedupe_key="key",
            subject={},
            payload={},
        )


def test_watch_event_rejects_unsupported_schema_version_at_runtime() -> None:
    with pytest.raises(ValueError, match="schema_version"):
        WatchEvent(
            schema_version="2.0",  # type: ignore[arg-type]
            event_id="evt-1",
            watch_id="watch-1",
            event_type="state.changed",
            severity="info",
            occurred_at=datetime(2025, 1, 1, tzinfo=UTC),
            dedupe_key="key",
            subject={},
            payload={},
        )


@pytest.mark.parametrize(
    "invalid",
    [
        ("tuple",),
        {1: "non-string-key"},
    ],
)
def test_models_reject_values_that_json_would_silently_coerce(invalid: object) -> None:
    with pytest.raises(TypeError):
        Observation.valid(invalid, observed_at=datetime(2025, 1, 1, tzinfo=UTC))  # type: ignore[arg-type]


def test_models_reject_cyclic_json_before_serialization() -> None:
    cyclic: list[object] = []
    cyclic.append(cyclic)
    with pytest.raises(ValueError, match="reference cycle"):
        Observation.valid(cyclic, observed_at=datetime(2025, 1, 1, tzinfo=UTC))  # type: ignore[arg-type]


def test_models_reject_excessively_nested_json_before_recursion_failure() -> None:
    nested: object = "leaf"
    for _ in range(101):
        nested = [nested]

    with pytest.raises(ValueError, match="JSON nesting exceeds 100"):
        Observation.valid(nested, observed_at=datetime(2025, 1, 1, tzinfo=UTC))  # type: ignore[arg-type]


def test_event_draft_requires_json_objects_for_subject_and_payload() -> None:
    with pytest.raises(TypeError, match="JSON objects"):
        EventDraft(
            event_type="state.changed",
            severity="info",
            dedupe_key="key",
            subject=[],  # type: ignore[arg-type]
            payload={},
        )


def test_empty_wrong_typed_evidence_is_not_silently_replaced() -> None:
    with pytest.raises(TypeError, match="evidence must be a JSON object"):
        Observation.valid(
            "state",
            observed_at=datetime(2025, 1, 1, tzinfo=UTC),
            evidence=[],  # type: ignore[arg-type]
        )


def test_public_result_models_reject_semantically_invalid_runtime_values() -> None:
    observation = Observation.valid(
        "state", observed_at=datetime(2025, 1, 1, tzinfo=UTC)
    )
    with pytest.raises(TypeError, match="delivered must be a bool"):
        DeliveryResult(event_id="event", delivered="yes")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="must not contain an error"):
        DeliveryResult(event_id="event", delivered=True, error="contradiction")
    with pytest.raises(ValueError, match="must not be negative"):
        PurgeResult(events_deleted=-1)
    with pytest.raises(TypeError, match="tuple of WatchEvent"):
        RunResult(observation=observation, events=[])  # type: ignore[arg-type]


def test_watch_status_rejects_invalid_runtime_values() -> None:
    with pytest.raises(TypeError, match="observation_count must be an integer"):
        WatchStatus("watch", True, RunStatus.IDLE)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="observation_count must not be negative"):
        WatchStatus("watch", -1, RunStatus.IDLE)
    with pytest.raises(TypeError, match="run_status must be a RunStatus"):
        WatchStatus("watch", 0, "IDLE")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="last_observation_status"):
        WatchStatus(
            "watch",
            0,
            RunStatus.IDLE,
            last_observation_status="VALID",  # type: ignore[arg-type]
        )

    status = WatchStatus(
        "watch",
        1,
        RunStatus.IDLE,
        last_started_at=datetime(2025, 1, 1, tzinfo=UTC),
        last_observation_status=ObservationStatus.VALID,
    )
    with pytest.raises(AttributeError):
        status.observation_count = 2  # type: ignore[misc]


def test_typed_delivery_diagnostics_reject_invalid_runtime_values() -> None:
    timestamp = datetime(2025, 1, 1, tzinfo=UTC)
    with pytest.raises(TypeError, match="status must be an OutboxStatus"):
        OutboxDiagnostic(
            outbox_id=1,
            event_id="event",
            watch_id="watch",
            status="PENDING",  # type: ignore[arg-type]
            attempts=0,
            created_at=timestamp,
            updated_at=timestamp,
        )
    with pytest.raises(ValueError, match="outbox_id must be positive"):
        OutboxDiagnostic(
            outbox_id=0,
            event_id="event",
            watch_id="watch",
            status=OutboxStatus.PENDING,
            attempts=0,
            created_at=timestamp,
            updated_at=timestamp,
        )
    with pytest.raises(TypeError, match="status must be a DeliveryAttemptStatus"):
        DeliveryAttemptDiagnostic(
            attempt_id=1,
            outbox_id=1,
            event_id="event",
            watch_id="watch",
            attempt_number=1,
            attempted_at=timestamp,
            status="FAILED",  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="attempt_number must be positive"):
        DeliveryAttemptDiagnostic(
            attempt_id=1,
            outbox_id=1,
            event_id="event",
            watch_id="watch",
            attempt_number=0,
            attempted_at=timestamp,
            status=DeliveryAttemptStatus.FAILED,
        )


def test_models_detach_caller_owned_json_and_event_dict_output() -> None:
    state = {"nested": ["original"]}
    subject = {"resource": {"id": "original"}}
    event = WatchEvent(
        schema_version="1.0",
        event_id="evt-1",
        watch_id="watch-1",
        event_type="state.changed",
        severity="info",
        occurred_at=datetime(2025, 1, 1, tzinfo=UTC),
        dedupe_key="key",
        subject=subject,
        payload={},
    )
    observation = Observation.valid(state, observed_at=datetime(2025, 1, 1, tzinfo=UTC))

    state["nested"].append("caller-mutation")
    subject["resource"]["id"] = "caller-mutation"  # type: ignore[index]
    serialized = event.to_dict()
    serialized_subject = serialized["subject"]
    assert isinstance(serialized_subject, dict)
    serialized_subject["resource"] = {"id": "output-mutation"}

    assert observation.state == {"nested": ["original"]}
    assert event.subject == {"resource": {"id": "original"}}


def test_unsupported_sqlite_schema_version_fails_fast(tmp_path: Path) -> None:
    database = tmp_path / "watch.db"
    SQLiteStore(database)
    with sqlite3.connect(database) as connection:
        connection.execute("UPDATE schema_meta SET version = 2")

    with pytest.raises(RuntimeError, match="unsupported database schema 2; expected 1"):
        SQLiteStore(database)


def test_ambiguous_sqlite_schema_metadata_fails_fast(tmp_path: Path) -> None:
    database = tmp_path / "watch.db"
    SQLiteStore(database)
    with sqlite3.connect(database) as connection:
        connection.execute("INSERT INTO schema_meta(version) VALUES (1)")

    with pytest.raises(RuntimeError, match="schema_meta must contain exactly one row"):
        SQLiteStore(database)


def test_non_integer_sqlite_schema_version_fails_fast(tmp_path: Path) -> None:
    database = tmp_path / "watch.db"
    SQLiteStore(database)
    with sqlite3.connect(database) as connection:
        connection.execute("UPDATE schema_meta SET version = 1.5")

    with pytest.raises(RuntimeError, match="schema_meta contains an invalid version"):
        SQLiteStore(database)


def test_persisted_event_schema_version_is_not_silently_masked(tmp_path: Path) -> None:
    database = tmp_path / "watch.db"
    store = SQLiteStore(database)
    now = datetime(2025, 1, 1, tzinfo=UTC).isoformat().replace("+00:00", "Z")
    with sqlite3.connect(database) as connection:
        connection.execute(
            "INSERT INTO watches(watch_id, created_at, updated_at) VALUES ('watch-1', ?, ?)",
            (now, now),
        )
        connection.execute(
            """
            INSERT INTO events(
                event_id, watch_id, schema_version, event_type, severity, occurred_at,
                dedupe_key, subject_json, payload_json, created_at
            ) VALUES ('evt-1', 'watch-1', '2.0', 'changed', 'info', ?, 'key', '{}', '{}', ?)
            """,
            (now, now),
        )

    with pytest.raises(ValueError, match="schema_version"):
        store.list_events("watch-1")
