from __future__ import annotations

import sqlite3
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from watch_engine._json import decode_json, encode_json
from watch_engine._time import from_iso, to_iso, utc_now
from watch_engine.interfaces import TransitionPolicy
from watch_engine.models import EventDraft, Observation, ObservationStatus, WatchEvent


@dataclass(frozen=True, slots=True)
class ClaimedEvent:
    outbox_id: int
    attempts: int
    event: WatchEvent


class SQLiteStore:
    """SQLite persistence with an explicit transactional-outbox boundary."""

    SCHEMA_VERSION = 1

    def __init__(
        self,
        path: str | Path,
        *,
        observation_id_factory: Callable[[], str] = lambda: str(uuid4()),
        event_id_factory: Callable[[], str] = lambda: str(uuid4()),
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.path = str(path)
        self._observation_id_factory = observation_id_factory
        self._event_id_factory = event_id_factory
        self._clock = clock
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, isolation_level=None, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA journal_mode = WAL")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS schema_meta (
                    version INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS watches (
                    watch_id TEXT PRIMARY KEY,
                    execution_count INTEGER NOT NULL DEFAULT 0,
                    run_status TEXT NOT NULL DEFAULT 'IDLE',
                    last_started_at TEXT,
                    last_finished_at TEXT,
                    last_observation_status TEXT,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS observations (
                    observation_id TEXT PRIMARY KEY,
                    watch_id TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('VALID', 'DEGRADED', 'FAILED')),
                    observed_at TEXT NOT NULL,
                    state_json TEXT NOT NULL,
                    evidence_json TEXT NOT NULL,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(watch_id) REFERENCES watches(watch_id)
                );
                CREATE INDEX IF NOT EXISTS idx_observations_watch_time
                    ON observations(watch_id, observed_at);

                CREATE TABLE IF NOT EXISTS authoritative_states (
                    watch_id TEXT PRIMARY KEY,
                    observation_id TEXT NOT NULL,
                    state_json TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(watch_id) REFERENCES watches(watch_id),
                    FOREIGN KEY(observation_id) REFERENCES observations(observation_id)
                );

                CREATE TABLE IF NOT EXISTS events (
                    event_id TEXT PRIMARY KEY,
                    watch_id TEXT NOT NULL,
                    schema_version TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    dedupe_key TEXT NOT NULL,
                    subject_json TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(watch_id, dedupe_key),
                    FOREIGN KEY(watch_id) REFERENCES watches(watch_id)
                );

                CREATE TABLE IF NOT EXISTS outbox (
                    outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL CHECK(status IN (
                        'PENDING', 'DELIVERING', 'RETRY', 'DELIVERED', 'DEAD'
                    )),
                    attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at TEXT,
                    locked_at TEXT,
                    delivered_at TEXT,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(event_id) REFERENCES events(event_id)
                );
                CREATE INDEX IF NOT EXISTS idx_outbox_due
                    ON outbox(status, next_attempt_at, outbox_id);

                CREATE TABLE IF NOT EXISTS delivery_attempts (
                    attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    outbox_id INTEGER NOT NULL,
                    event_id TEXT NOT NULL,
                    attempt_number INTEGER NOT NULL,
                    attempted_at TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('SUCCEEDED', 'FAILED')),
                    error TEXT,
                    FOREIGN KEY(outbox_id) REFERENCES outbox(outbox_id),
                    FOREIGN KEY(event_id) REFERENCES events(event_id)
                );
                """
            )
            row = connection.execute("SELECT version FROM schema_meta").fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO schema_meta(version) VALUES (?)", (self.SCHEMA_VERSION,)
                )
            elif int(row["version"]) != self.SCHEMA_VERSION:
                raise RuntimeError(
                    f"unsupported database schema {row['version']}; expected {self.SCHEMA_VERSION}"
                )

    def _ensure_watch(
        self, connection: sqlite3.Connection, watch_id: str, now: datetime
    ) -> None:
        now_text = to_iso(now)
        connection.execute(
            """
            INSERT INTO watches(watch_id, created_at, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(watch_id) DO NOTHING
            """,
            (watch_id, now_text, now_text),
        )

    def mark_run_started(self, watch_id: str, *, now: datetime | None = None) -> None:
        timestamp = now or self._clock()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._ensure_watch(connection, watch_id, timestamp)
            connection.execute(
                """
                UPDATE watches
                SET run_status = 'RUNNING', last_started_at = ?, last_error = NULL,
                    updated_at = ?
                WHERE watch_id = ?
                """,
                (to_iso(timestamp), to_iso(timestamp), watch_id),
            )
            connection.commit()

    def mark_run_error(
        self, watch_id: str, error: str, *, now: datetime | None = None
    ) -> None:
        timestamp = now or self._clock()
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE watches SET run_status = 'ERROR', last_error = ?,
                    last_finished_at = ?, updated_at = ? WHERE watch_id = ?
                """,
                (error, to_iso(timestamp), to_iso(timestamp), watch_id),
            )

    def record_observation(
        self,
        watch_id: str,
        observation: Observation,
        policy: TransitionPolicy,
        *,
        now: datetime | None = None,
    ) -> tuple[WatchEvent, ...]:
        """Atomically persist observation, authority, events, and outbox rows."""
        timestamp = now or self._clock()
        observation_id = self._observation_id_factory()
        created_events: list[WatchEvent] = []
        with self._connect() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._ensure_watch(connection, watch_id, timestamp)
                connection.execute(
                    """
                    INSERT INTO observations(
                        observation_id, watch_id, status, observed_at, state_json,
                        evidence_json, error, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        observation_id,
                        watch_id,
                        observation.status.value,
                        to_iso(observation.observed_at),
                        encode_json(observation.state),
                        encode_json(observation.evidence),
                        observation.error,
                        to_iso(timestamp),
                    ),
                )

                if observation.status is ObservationStatus.VALID:
                    previous = self._read_authoritative(connection, watch_id)
                    drafts = policy.evaluate(previous, observation)
                    created_events.extend(
                        self._insert_events(connection, watch_id, observation, drafts, timestamp)
                    )
                    connection.execute(
                        """
                        INSERT INTO authoritative_states(
                            watch_id, observation_id, state_json, observed_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(watch_id) DO UPDATE SET
                            observation_id = excluded.observation_id,
                            state_json = excluded.state_json,
                            observed_at = excluded.observed_at,
                            updated_at = excluded.updated_at
                        """,
                        (
                            watch_id,
                            observation_id,
                            encode_json(observation.state),
                            to_iso(observation.observed_at),
                            to_iso(timestamp),
                        ),
                    )

                connection.execute(
                    """
                    UPDATE watches
                    SET execution_count = execution_count + 1, run_status = 'IDLE',
                        last_finished_at = ?, last_observation_status = ?, last_error = ?,
                        updated_at = ?
                    WHERE watch_id = ?
                    """,
                    (
                        to_iso(timestamp),
                        observation.status.value,
                        observation.error,
                        to_iso(timestamp),
                        watch_id,
                    ),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return tuple(created_events)

    def _insert_events(
        self,
        connection: sqlite3.Connection,
        watch_id: str,
        observation: Observation,
        drafts: Sequence[EventDraft],
        now: datetime,
    ) -> list[WatchEvent]:
        created: list[WatchEvent] = []
        for draft in drafts:
            event = WatchEvent(
                schema_version="1.0",
                event_id=self._event_id_factory(),
                watch_id=watch_id,
                event_type=draft.event_type,
                severity=draft.severity,
                occurred_at=observation.observed_at,
                dedupe_key=draft.dedupe_key,
                subject=draft.subject,
                payload=draft.payload,
            )
            cursor = connection.execute(
                """
                INSERT INTO events(
                    event_id, watch_id, schema_version, event_type, severity, occurred_at,
                    dedupe_key, subject_json, payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(watch_id, dedupe_key) DO NOTHING
                """,
                (
                    event.event_id,
                    event.watch_id,
                    event.schema_version,
                    event.event_type,
                    event.severity,
                    to_iso(event.occurred_at),
                    event.dedupe_key,
                    encode_json(event.subject),
                    encode_json(event.payload),
                    to_iso(now),
                ),
            )
            if cursor.rowcount == 1:
                self._insert_outbox(connection, event.event_id, now)
                created.append(event)
        return created

    def _insert_outbox(
        self, connection: sqlite3.Connection, event_id: str, now: datetime
    ) -> None:
        timestamp = to_iso(now)
        connection.execute(
            """
            INSERT INTO outbox(
                event_id, status, attempts, next_attempt_at, created_at, updated_at
            ) VALUES (?, 'PENDING', 0, ?, ?, ?)
            """,
            (event_id, timestamp, timestamp, timestamp),
        )

    def _read_authoritative(
        self, connection: sqlite3.Connection, watch_id: str
    ) -> Observation | None:
        row = connection.execute(
            """
            SELECT o.status, o.observed_at, o.state_json, o.evidence_json, o.error
            FROM authoritative_states a
            JOIN observations o ON o.observation_id = a.observation_id
            WHERE a.watch_id = ?
            """,
            (watch_id,),
        ).fetchone()
        return self._observation_from_row(row) if row is not None else None

    @staticmethod
    def _observation_from_row(row: sqlite3.Row) -> Observation:
        evidence = decode_json(row["evidence_json"])
        if not isinstance(evidence, dict):
            raise RuntimeError("persisted observation evidence is not an object")
        return Observation(
            status=ObservationStatus(row["status"]),
            observed_at=from_iso(row["observed_at"]),
            state=decode_json(row["state_json"]),
            evidence=evidence,
            error=row["error"],
        )

    def get_authoritative_observation(self, watch_id: str) -> Observation | None:
        with self._connect() as connection:
            return self._read_authoritative(connection, watch_id)

    def list_observations(self, watch_id: str) -> list[Observation]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT status, observed_at, state_json, evidence_json, error
                FROM observations WHERE watch_id = ? ORDER BY rowid
                """,
                (watch_id,),
            ).fetchall()
        return [self._observation_from_row(row) for row in rows]

    def list_events(self, watch_id: str) -> list[WatchEvent]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM events WHERE watch_id = ? ORDER BY rowid", (watch_id,)
            ).fetchall()
        return [self._event_from_row(row) for row in rows]

    @staticmethod
    def _event_from_row(row: sqlite3.Row) -> WatchEvent:
        subject = decode_json(row["subject_json"])
        payload = decode_json(row["payload_json"])
        if not isinstance(subject, dict) or not isinstance(payload, dict):
            raise RuntimeError("persisted event subject/payload is not an object")
        return WatchEvent(
            schema_version="1.0",
            event_id=row["event_id"],
            watch_id=row["watch_id"],
            event_type=row["event_type"],
            severity=row["severity"],
            occurred_at=from_iso(row["occurred_at"]),
            dedupe_key=row["dedupe_key"],
            subject=subject,
            payload=payload,
        )

    def recover_in_flight(self, *, now: datetime | None = None) -> int:
        timestamp = to_iso(now or self._clock())
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE outbox SET status = 'RETRY', next_attempt_at = ?, locked_at = NULL,
                    updated_at = ?, last_error = COALESCE(last_error, 'delivery interrupted')
                WHERE status = 'DELIVERING'
                """,
                (timestamp, timestamp),
            )
            return cursor.rowcount

    def claim_due(
        self, *, now: datetime | None = None, limit: int = 100
    ) -> list[ClaimedEvent]:
        timestamp = now or self._clock()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                """
                SELECT o.outbox_id, o.attempts, e.*
                FROM outbox o JOIN events e ON e.event_id = o.event_id
                WHERE o.status IN ('PENDING', 'RETRY') AND o.next_attempt_at <= ?
                ORDER BY o.outbox_id LIMIT ?
                """,
                (to_iso(timestamp), limit),
            ).fetchall()
            ids = [int(row["outbox_id"]) for row in rows]
            if ids:
                placeholders = ",".join("?" for _ in ids)
                connection.execute(
                    f"UPDATE outbox SET status = 'DELIVERING', locked_at = ?, updated_at = ? "
                    f"WHERE outbox_id IN ({placeholders})",  # noqa: S608 - placeholders only
                    (to_iso(timestamp), to_iso(timestamp), *ids),
                )
            connection.commit()
        return [
            ClaimedEvent(
                outbox_id=int(row["outbox_id"]),
                attempts=int(row["attempts"]),
                event=self._event_from_row(row),
            )
            for row in rows
        ]

    def record_delivery_success(
        self, claimed: ClaimedEvent, *, now: datetime | None = None
    ) -> None:
        timestamp = now or self._clock()
        attempt_number = claimed.attempts + 1
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                """
                UPDATE outbox SET status = 'DELIVERED', attempts = ?, delivered_at = ?,
                    next_attempt_at = NULL, locked_at = NULL, last_error = NULL, updated_at = ?
                WHERE outbox_id = ? AND status = 'DELIVERING'
                """,
                (attempt_number, to_iso(timestamp), to_iso(timestamp), claimed.outbox_id),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise RuntimeError(f"outbox claim {claimed.outbox_id} is no longer active")
            self._insert_attempt(
                connection, claimed, attempt_number, timestamp, status="SUCCEEDED", error=None
            )
            connection.commit()

    def record_delivery_failure(
        self,
        claimed: ClaimedEvent,
        error: str,
        *,
        retry_at: datetime | None,
        now: datetime | None = None,
    ) -> None:
        timestamp = now or self._clock()
        attempt_number = claimed.attempts + 1
        status = "DEAD" if retry_at is None else "RETRY"
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                """
                UPDATE outbox SET status = ?, attempts = ?, next_attempt_at = ?,
                    locked_at = NULL, last_error = ?, updated_at = ?
                WHERE outbox_id = ? AND status = 'DELIVERING'
                """,
                (
                    status,
                    attempt_number,
                    to_iso(retry_at) if retry_at else None,
                    error,
                    to_iso(timestamp),
                    claimed.outbox_id,
                ),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise RuntimeError(f"outbox claim {claimed.outbox_id} is no longer active")
            self._insert_attempt(
                connection, claimed, attempt_number, timestamp, status="FAILED", error=error
            )
            connection.commit()

    @staticmethod
    def _insert_attempt(
        connection: sqlite3.Connection,
        claimed: ClaimedEvent,
        attempt_number: int,
        now: datetime,
        *,
        status: str,
        error: str | None,
    ) -> None:
        connection.execute(
            """
            INSERT INTO delivery_attempts(
                outbox_id, event_id, attempt_number, attempted_at, status, error
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                claimed.outbox_id,
                claimed.event.event_id,
                attempt_number,
                to_iso(now),
                status,
                error,
            ),
        )

    def outbox_rows(self) -> list[dict[str, Any]]:
        """Return diagnostic outbox snapshots without exposing a live connection."""
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM outbox ORDER BY outbox_id").fetchall()
        return [dict(row) for row in rows]

    def delivery_attempt_rows(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM delivery_attempts ORDER BY attempt_id"
            ).fetchall()
        return [dict(row) for row in rows]
