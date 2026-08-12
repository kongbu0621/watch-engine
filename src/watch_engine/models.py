from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Literal

from watch_engine._errors import bounded_error_text
from watch_engine._json import JsonObject, JsonValue, validate_json
from watch_engine._time import require_aware, to_iso


class ObservationStatus(StrEnum):
    VALID = "VALID"
    DEGRADED = "DEGRADED"
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
        object.__setattr__(
            self, "observed_at", require_aware(self.observed_at, field="observed_at")
        )
        validate_json(self.state)
        validate_json(self.evidence)
        if self.error is not None:
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
            evidence=evidence or {},
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
            evidence=evidence or {},
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
            evidence=evidence or {},
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
            if not getattr(self, field_name):
                raise ValueError(f"{field_name} must not be empty")
        validate_json(self.subject)
        validate_json(self.payload)


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
        object.__setattr__(
            self, "occurred_at", require_aware(self.occurred_at, field="occurred_at")
        )
        for field_name in ("event_id", "watch_id", "event_type", "severity", "dedupe_key"):
            if not getattr(self, field_name):
                raise ValueError(f"{field_name} must not be empty")
        validate_json(self.subject)
        validate_json(self.payload)

    def to_dict(self) -> JsonObject:
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "watch_id": self.watch_id,
            "event_type": self.event_type,
            "severity": self.severity,
            "occurred_at": to_iso(self.occurred_at),
            "dedupe_key": self.dedupe_key,
            "subject": self.subject,
            "payload": self.payload,
        }


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay_seconds: float = 1.0
    maximum_delay_seconds: float = 60.0
    multiplier: float = 2.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.base_delay_seconds <= 0:
            raise ValueError("base_delay_seconds must be positive")
        if self.maximum_delay_seconds < self.base_delay_seconds:
            raise ValueError("maximum_delay_seconds must be >= base_delay_seconds")
        if self.multiplier < 1:
            raise ValueError("multiplier must be >= 1")

    def delay_for_failure(self, failure_number: int) -> float:
        if failure_number < 1:
            raise ValueError("failure_number must be at least 1")
        return min(
            self.base_delay_seconds * self.multiplier ** (failure_number - 1),
            self.maximum_delay_seconds,
        )


@dataclass(frozen=True, slots=True)
class DeliveryConfig:
    retry: RetryPolicy = field(default_factory=lambda: RetryPolicy(max_attempts=5))
    batch_size: int = 100

    def __post_init__(self) -> None:
        if self.batch_size < 1:
            raise ValueError("batch_size must be at least 1")


@dataclass(frozen=True, slots=True)
class RunResult:
    observation: Observation
    events: tuple[WatchEvent, ...]


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    event_id: str
    delivered: bool
    error: str | None = None

    def __post_init__(self) -> None:
        if self.error is not None:
            object.__setattr__(self, "error", bounded_error_text(self.error))


@dataclass(frozen=True, slots=True)
class PurgeResult:
    observations_deleted: int = 0
    events_deleted: int = 0
    delivery_attempts_deleted: int = 0
    watches_deleted: int = 0
