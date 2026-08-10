from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError

from watch_engine import WatchEvent


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
