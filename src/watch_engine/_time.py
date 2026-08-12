from __future__ import annotations

from datetime import UTC, datetime


def require_aware(value: datetime, *, field: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def utc_now() -> datetime:
    return datetime.now(UTC)


def to_iso(value: datetime) -> str:
    return require_aware(value, field="datetime").isoformat().replace("+00:00", "Z")


def from_iso(value: str) -> datetime:
    return require_aware(datetime.fromisoformat(value.replace("Z", "+00:00")), field="datetime")
