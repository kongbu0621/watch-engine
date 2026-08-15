from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from math import isfinite
from typing import Literal

from watch_engine._errors import bounded_error_text
from watch_engine._json import JsonObject, JsonValue, copy_json
from watch_engine._time import require_aware, to_iso

MAX_METADATA_CHARACTERS = 2_048
_MAX_DELIVERY_BATCH_SIZE = 500


class ObservationStatus(StrEnum):
    VALID = "VALID"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"


class RunStatus(StrEnum):
    IDLE = "IDLE"
    RUNNING = "RUNNING"
    ERROR = "ERROR"


class OutboxStatus(StrEnum):
    PENDING = "PENDING"
    DELIVERING = "DELIVERING"
    RETRY = "RETRY"
    DELIVERED = "DELIVERED"
    DEAD = "DEAD"


class DeliveryAttemptStatus(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class Observation:
    """One observed fact; only VALID observations may become authoritative."""

    status: ObservationStatus
    observed_at: datetime
    state: JsonValue = None
    evidence: JsonObject = field(default_factory=dict)
    error: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.status, ObservationStatus):
            raise TypeError("status must be an ObservationStatus")
        if not isinstance(self.evidence, dict):
            raise TypeError("evidence must be a JSON object")
        object.__setattr__(
            self, "observed_at", require_aware(self.observed_at, field="observed_at")
        )
        object.__setattr__(self, "state", copy_json(self.state))
        evidence = copy_json(self.evidence)
        if not isinstance(evidence, dict):
            raise AssertionError("validated evidence did not remain a JSON object")
        object.__setattr__(self, "evidence", evidence)
        if self.error is not None:
            if not isinstance(self.error, str):
                raise TypeError("error must be a string or None")
            object.__setattr__(self, "error", bounded_error_text(self.error))

    @classmethod
    def valid(
        cls,
        state: JsonValue,
        *,
        observed_at: datetime,
        evidence: JsonObject | None = None,
    ) -> Observation:
        return cls(
            status=ObservationStatus.VALID,
            observed_at=observed_at,
            state=state,
            evidence=evidence if evidence is not None else {},
        )

    @classmethod
    def degraded(
        cls,
        *,
        observed_at: datetime,
        evidence: JsonObject | None = None,
        error: str | None = None,
        state: JsonValue = None,
    ) -> Observation:
        return cls(
            status=ObservationStatus.DEGRADED,
            observed_at=observed_at,
            state=state,
            evidence=evidence if evidence is not None else {},
            error=error,
        )

    @classmethod
    def failed(
        cls,
        *,
        observed_at: datetime,
        error: str,
        evidence: JsonObject | None = None,
    ) -> Observation:
        return cls(
            status=ObservationStatus.FAILED,
            observed_at=observed_at,
            evidence=evidence if evidence is not None else {},
            error=error,
        )


@dataclass(frozen=True, slots=True)
class EventDraft:
    """Domain policy output; dedupe_key is context, not global event uniqueness."""

    event_type: str
    severity: str
    dedupe_key: str
    subject: JsonObject
    payload: JsonObject

    def __post_init__(self) -> None:
        for field_name in ("event_type", "severity", "dedupe_key"):
            _require_non_empty_string(getattr(self, field_name), field=field_name)
        if not isinstance(self.subject, dict) or not isinstance(self.payload, dict):
            raise TypeError("subject and payload must be JSON objects")
        subject = copy_json(self.subject)
        payload = copy_json(self.payload)
        if not isinstance(subject, dict) or not isinstance(payload, dict):
            raise AssertionError("validated event draft fields did not remain JSON objects")
        object.__setattr__(self, "subject", subject)
        object.__setattr__(self, "payload", payload)


@dataclass(frozen=True, slots=True)
class WatchEvent:
    schema_version: Literal["1.0"]
    event_id: str
    watch_id: str
    event_type: str
    severity: str
    occurred_at: datetime
    dedupe_key: str
    subject: JsonObject
    payload: JsonObject

    def __post_init__(self) -> None:
        if self.schema_version != "1.0":
            raise ValueError("schema_version must be '1.0'")
        object.__setattr__(
            self, "occurred_at", require_aware(self.occurred_at, field="occurred_at")
        )
        for field_name in ("event_id", "watch_id", "event_type", "severity", "dedupe_key"):
            _require_non_empty_string(getattr(self, field_name), field=field_name)
        if not isinstance(self.subject, dict) or not isinstance(self.payload, dict):
            raise TypeError("subject and payload must be JSON objects")
        subject = copy_json(self.subject)
        payload = copy_json(self.payload)
        if not isinstance(subject, dict) or not isinstance(payload, dict):
            raise AssertionError("validated event fields did not remain JSON objects")
        object.__setattr__(self, "subject", subject)
        object.__setattr__(self, "payload", payload)

    def to_dict(self) -> JsonObject:
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "watch_id": self.watch_id,
            "event_type": self.event_type,
            "severity": self.severity,
            "occurred_at": to_iso(self.occurred_at),
            "dedupe_key": self.dedupe_key,
            "subject": copy_json(self.subject),
            "payload": copy_json(self.payload),
        }


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay_seconds: float = 1.0
    maximum_delay_seconds: float = 60.0
    multiplier: float = 2.0

    def __post_init__(self) -> None:
        if isinstance(self.max_attempts, bool) or not isinstance(self.max_attempts, int):
            raise TypeError("max_attempts must be an integer")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        for field_name in ("base_delay_seconds", "maximum_delay_seconds", "multiplier"):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise TypeError(f"{field_name} must be a number")
            try:
                normalized = float(value)
            except OverflowError:
                raise ValueError(f"{field_name} must be finite") from None
            if not isfinite(normalized):
                raise ValueError(f"{field_name} must be finite")
            object.__setattr__(self, field_name, normalized)
        if self.base_delay_seconds <= 0:
            raise ValueError("base_delay_seconds must be positive")
        if self.maximum_delay_seconds < self.base_delay_seconds:
            raise ValueError("maximum_delay_seconds must be >= base_delay_seconds")
        if self.multiplier < 1:
            raise ValueError("multiplier must be >= 1")

    def delay_for_failure(self, failure_number: int) -> float:
        if isinstance(failure_number, bool) or not isinstance(failure_number, int):
            raise TypeError("failure_number must be an integer")
        if failure_number < 1:
            raise ValueError("failure_number must be at least 1")
        try:
            delay = self.base_delay_seconds * self.multiplier ** (failure_number - 1)
        except OverflowError:
            return self.maximum_delay_seconds
        return min(delay, self.maximum_delay_seconds)


@dataclass(frozen=True, slots=True)
class DeliveryConfig:
    retry: RetryPolicy = field(default_factory=lambda: RetryPolicy(max_attempts=5))
    batch_size: int = 100

    def __post_init__(self) -> None:
        if not isinstance(self.retry, RetryPolicy):
            raise TypeError("retry must be a RetryPolicy")
        if isinstance(self.batch_size, bool) or not isinstance(self.batch_size, int):
            raise TypeError("batch_size must be an integer")
        if self.batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        if self.batch_size > _MAX_DELIVERY_BATCH_SIZE:
            raise ValueError(
                f"batch_size must be at most {_MAX_DELIVERY_BATCH_SIZE}"
            )


@dataclass(frozen=True, slots=True)
class RunResult:
    observation: Observation
    events: tuple[WatchEvent, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.observation, Observation):
            raise TypeError("observation must be an Observation")
        if not isinstance(self.events, tuple) or not all(
            isinstance(event, WatchEvent) for event in self.events
        ):
            raise TypeError("events must be a tuple of WatchEvent values")


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    event_id: str
    delivered: bool
    error: str | None = None

    def __post_init__(self) -> None:
        _require_non_empty_string(self.event_id, field="event_id")
        if not isinstance(self.delivered, bool):
            raise TypeError("delivered must be a bool")
        if self.error is not None:
            if not isinstance(self.error, str):
                raise TypeError("error must be a string or None")
            object.__setattr__(self, "error", bounded_error_text(self.error))
        if self.delivered and self.error is not None:
            raise ValueError("a delivered result must not contain an error")


@dataclass(frozen=True, slots=True)
class PurgeResult:
    observations_deleted: int = 0
    events_deleted: int = 0
    delivery_attempts_deleted: int = 0
    watches_deleted: int = 0
    outbox_rows_deleted: int = 0

    def __post_init__(self) -> None:
        for field_name in (
            "observations_deleted",
            "events_deleted",
            "delivery_attempts_deleted",
            "watches_deleted",
            "outbox_rows_deleted",
        ):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"{field_name} must be an integer")
            if value < 0:
                raise ValueError(f"{field_name} must not be negative")


@dataclass(frozen=True, slots=True)
class WatchStatus:
    """Read-only diagnostic snapshot for one persisted watch."""

    watch_id: str
    observation_count: int
    run_status: RunStatus
    last_started_at: datetime | None = None
    last_finished_at: datetime | None = None
    last_observation_status: ObservationStatus | None = None
    last_error: str | None = None

    def __post_init__(self) -> None:
        _require_non_empty_string(self.watch_id, field="watch_id")
        if isinstance(self.observation_count, bool) or not isinstance(
            self.observation_count, int
        ):
            raise TypeError("observation_count must be an integer")
        if self.observation_count < 0:
            raise ValueError("observation_count must not be negative")
        if not isinstance(self.run_status, RunStatus):
            raise TypeError("run_status must be a RunStatus")
        for field_name in ("last_started_at", "last_finished_at"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, require_aware(value, field=field_name))
        if self.last_observation_status is not None and not isinstance(
            self.last_observation_status, ObservationStatus
        ):
            raise TypeError(
                "last_observation_status must be an ObservationStatus or None"
            )
        if self.last_error is not None:
            if not isinstance(self.last_error, str):
                raise TypeError("last_error must be a string or None")
            object.__setattr__(self, "last_error", bounded_error_text(self.last_error))


@dataclass(frozen=True, slots=True)
class OutboxDiagnostic:
    """Typed, read-only delivery state detached from the SQLite schema."""

    outbox_id: int
    event_id: str
    watch_id: str
    status: OutboxStatus
    attempts: int
    created_at: datetime
    updated_at: datetime
    next_attempt_at: datetime | None = None
    locked_at: datetime | None = None
    delivered_at: datetime | None = None
    last_error: str | None = None

    def __post_init__(self) -> None:
        _require_positive_integer(self.outbox_id, field="outbox_id")
        _require_non_empty_string(self.event_id, field="event_id")
        _require_non_empty_string(self.watch_id, field="watch_id")
        if not isinstance(self.status, OutboxStatus):
            raise TypeError("status must be an OutboxStatus")
        _require_non_negative_integer(self.attempts, field="attempts")
        for field_name in ("created_at", "updated_at"):
            value = getattr(self, field_name)
            object.__setattr__(self, field_name, require_aware(value, field=field_name))
        for field_name in ("next_attempt_at", "locked_at", "delivered_at"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, require_aware(value, field=field_name))
        if self.last_error is not None:
            if not isinstance(self.last_error, str):
                raise TypeError("last_error must be a string or None")
            object.__setattr__(self, "last_error", bounded_error_text(self.last_error))


@dataclass(frozen=True, slots=True)
class DeliveryAttemptDiagnostic:
    """Typed, read-only snapshot of one completed delivery attempt."""

    attempt_id: int
    outbox_id: int
    event_id: str
    watch_id: str
    attempt_number: int
    attempted_at: datetime
    status: DeliveryAttemptStatus
    error: str | None = None

    def __post_init__(self) -> None:
        _require_positive_integer(self.attempt_id, field="attempt_id")
        _require_positive_integer(self.outbox_id, field="outbox_id")
        _require_non_empty_string(self.event_id, field="event_id")
        _require_non_empty_string(self.watch_id, field="watch_id")
        _require_positive_integer(self.attempt_number, field="attempt_number")
        object.__setattr__(
            self,
            "attempted_at",
            require_aware(self.attempted_at, field="attempted_at"),
        )
        if not isinstance(self.status, DeliveryAttemptStatus):
            raise TypeError("status must be a DeliveryAttemptStatus")
        if self.error is not None:
            if not isinstance(self.error, str):
                raise TypeError("error must be a string or None")
            object.__setattr__(self, "error", bounded_error_text(self.error))


def _require_non_empty_string(value: object, *, field: str) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    if not value:
        raise ValueError(f"{field} must not be empty")
    if len(value) > MAX_METADATA_CHARACTERS:
        raise ValueError(
            f"{field} must not exceed {MAX_METADATA_CHARACTERS} characters"
        )


def _require_non_negative_integer(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field} must be an integer")
    if value < 0:
        raise ValueError(f"{field} must not be negative")
    return value


def _require_positive_integer(value: object, *, field: str) -> None:
    normalized = _require_non_negative_integer(value, field=field)
    if normalized < 1:
        raise ValueError(f"{field} must be positive")
