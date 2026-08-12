from __future__ import annotations

import json
import math
from typing import TypeAlias

JsonScalar: TypeAlias = None | bool | int | float | str
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
JsonObject: TypeAlias = dict[str, JsonValue]

MAX_JSON_BYTES = 1024 * 1024
MAX_JSON_NESTING = 100


def encode_json(value: JsonValue) -> str:
    """Encode JSON deterministically for persistence and comparisons."""
    _validate_json_shape(value, active_containers=set(), depth=0)
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
    if len(value.encode("utf-8")) > MAX_JSON_BYTES:
        raise ValueError(f"JSON value exceeds {MAX_JSON_BYTES} encoded bytes")
    try:
        decoded: JsonValue = json.loads(value)
    except RecursionError:
        raise ValueError(
            f"JSON nesting exceeds {MAX_JSON_NESTING} container levels"
        ) from None
    _validate_json_shape(decoded, active_containers=set(), depth=0)
    return decoded


def copy_json(value: JsonValue) -> JsonValue:
    """Return a detached canonical JSON value after full validation."""

    return decode_json(encode_json(value))


def validate_json(value: JsonValue) -> None:
    """Fail early if a caller supplied a non-JSON-compatible runtime value."""
    encode_json(value)


def _validate_json_shape(
    value: object, *, active_containers: set[int], depth: int
) -> None:
    if value is None or isinstance(value, (bool, str, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("JSON numbers must be finite")
        return
    if isinstance(value, list):
        if depth >= MAX_JSON_NESTING:
            raise ValueError(
                f"JSON nesting exceeds {MAX_JSON_NESTING} container levels"
            )
        marker = id(value)
        if marker in active_containers:
            raise ValueError("JSON value must not contain a reference cycle")
        active_containers.add(marker)
        try:
            for item in value:
                _validate_json_shape(
                    item, active_containers=active_containers, depth=depth + 1
                )
        finally:
            active_containers.remove(marker)
        return
    if isinstance(value, dict):
        if depth >= MAX_JSON_NESTING:
            raise ValueError(
                f"JSON nesting exceeds {MAX_JSON_NESTING} container levels"
            )
        marker = id(value)
        if marker in active_containers:
            raise ValueError("JSON value must not contain a reference cycle")
        active_containers.add(marker)
        try:
            for key, item in value.items():
                if not isinstance(key, str):
                    raise TypeError("JSON object keys must be strings")
                _validate_json_shape(
                    item, active_containers=active_containers, depth=depth + 1
                )
        finally:
            active_containers.remove(marker)
        return
    raise TypeError(f"unsupported JSON value type: {type(value).__name__}")
