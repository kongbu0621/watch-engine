from __future__ import annotations

import json
from typing import TypeAlias

JsonScalar: TypeAlias = None | bool | int | float | str
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
JsonObject: TypeAlias = dict[str, JsonValue]

MAX_JSON_BYTES = 1024 * 1024


def encode_json(value: JsonValue) -> str:
    """Encode JSON deterministically for persistence and comparisons."""
    encoded = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    size = len(encoded.encode("utf-8"))
    if size > MAX_JSON_BYTES:
        raise ValueError(f"JSON value exceeds {MAX_JSON_BYTES} encoded bytes")
    return encoded


def decode_json(value: str) -> JsonValue:
    decoded: JsonValue = json.loads(value)
    return decoded


def validate_json(value: JsonValue) -> None:
    """Fail early if a caller supplied a non-JSON-compatible runtime value."""
    encode_json(value)
