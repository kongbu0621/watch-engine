from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError

import watch_engine
from watch_engine import SQLiteStore, WatchEvent, load_watch_event_schema

EXPECTED_PUBLIC_API = {
    "CronTrigger",
    "DeliveryConfig",
    "DeliveryResult",
    "EventDraft",
    "EventSink",
    "IntervalTrigger",
    "ManualTrigger",
    "Observation",
    "ObservationStatus",
    "Observer",
    "OutboxDispatcher",
    "PurgeResult",
    "RetryPolicy",
    "RunResult",
    "SQLiteStore",
    "TransitionPolicy",
    "Trigger",
    "WatchDefinition",
    "WatchEvent",
    "WatchRunner",
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


def test_unsupported_sqlite_schema_version_fails_fast(tmp_path: Path) -> None:
    database = tmp_path / "watch.db"
    SQLiteStore(database)
    with sqlite3.connect(database) as connection:
        connection.execute("UPDATE schema_meta SET version = 2")

    with pytest.raises(RuntimeError, match="unsupported database schema 2; expected 1"):
        SQLiteStore(database)
