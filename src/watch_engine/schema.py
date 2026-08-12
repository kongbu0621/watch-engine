from __future__ import annotations

import json
from importlib.resources import files
from typing import cast

from watch_engine._json import JsonObject


def load_watch_event_schema() -> JsonObject:
    """Load the packaged WatchEvent v1 JSON Schema."""
    resource = files("watch_engine").joinpath("schemas/watch-event-v1.json")
    value = json.loads(resource.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError("packaged WatchEvent schema is not an object")
    return cast(JsonObject, value)
