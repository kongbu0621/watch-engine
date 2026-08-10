from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from watch_engine import WatchEvent


def test_watch_event_v1_matches_published_contract_shape() -> None:
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

    assert schema["properties"]["schema_version"]["const"] == "1.0"
    assert set(event) == set(schema["required"])
    assert event["occurred_at"] == "2025-01-01T00:00:00Z"


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
