from __future__ import annotations

import logging
import os
import sqlite3
import stat
from collections.abc import Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from functools import cache
from pathlib import Path
from typing import Any
from uuid import uuid4

from watch_engine._errors import bounded_error_text
from watch_engine._json import decode_json, encode_json
from watch_engine._time import from_iso, require_aware, to_iso, to_sortable_iso, utc_now
from watch_engine.interfaces import TransitionPolicy
from watch_engine.models import (
    _MAX_DELIVERY_BATCH_SIZE,
    EventDraft,
    Observation,
    ObservationStatus,
    PurgeResult,
    RunStatus,
    WatchEvent,
    WatchStatus,
    _require_non_empty_string,
)

logger = logging.getLogger(__name__)

_TIMESTAMP_SQL_COLUMNS = frozenset(
    {"e.created_at", "o.created_at", "o.next_attempt_at"}
)


def _sortable_timestamp_sql(column: str) -> str:
    """Normalize legacy engine timestamps to fixed-width UTC text in SQL.

    ``datetime.isoformat()`` omits the fractional component for exact seconds. The
    resulting ``...00Z`` sorts after ``...00.500000Z`` even though it is earlier.
    Existing v0.1 databases may contain both forms, so comparison queries must
    normalize stored exact-second values instead of only changing new writes.
    """

    if column not in _TIMESTAMP_SQL_COLUMNS:
        raise ValueError("unsupported timestamp SQL column")
    return (
        f"CASE WHEN instr({column}, '.') = 0 "
        f"THEN substr({column}, 1, length({column}) - 1) || '.000000Z' "
        f"ELSE {column} END"
    )

_SCHEMA_SQL = """
BEGIN IMMEDIATE;

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
    FOREIGN KEY(watch_id) REFERENCES watches(watch_id)
);
CREATE INDEX IF NOT EXISTS idx_events_watch_dedupe
    ON events(watch_id, dedupe_key);

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


def _normalize_schema_sql(sql: str) -> tuple[str, ...]:
    tokens: list[str] = []
    position = 0
    while position < len(sql):
        character = sql[position]
        next_character = sql[position + 1] if position + 1 < len(sql) else ""
        if character.isspace():
            position += 1
            continue
        if character == "-" and next_character == "-":
            newline = sql.find("\n", position + 2)
            position = len(sql) if newline == -1 else newline + 1
            continue
        if character == "/" and next_character == "*":
            comment_end = sql.find("*/", position + 2)
            position = len(sql) if comment_end == -1 else comment_end + 2
            continue
        if character in {"'", '"', "`", "["}:
            quote_end = "]" if character == "[" else character
            quoted = [character]
            position += 1
            while position < len(sql):
                quoted_character = sql[position]
                quoted.append(quoted_character)
                position += 1
                if quoted_character != quote_end:
                    continue
                if (
                    quote_end != "]"
                    and position < len(sql)
                    and sql[position] == quote_end
                ):
                    quoted.append(sql[position])
                    position += 1
                    continue
                break
            tokens.append("".join(quoted))
            continue
        if character.isalnum() or character in {"_", "$"}:
            token_start = position
            position += 1
            while position < len(sql):
                token_character = sql[position]
                if not (token_character.isalnum() or token_character in {"_", "$"}):
                    break
                position += 1
            tokens.append(sql[token_start:position].upper())
            continue
        three_character_operator = sql[position : position + 3]
        if three_character_operator == "->>":
            tokens.append(three_character_operator)
            position += 3
            continue
        two_character_operator = sql[position : position + 2]
        if two_character_operator in {
            "||",
            "<<",
            ">>",
            "<=",
            ">=",
            "<>",
            "!=",
            "==",
            "->",
        }:
            tokens.append(two_character_operator)
            position += 2
            continue
        tokens.append(character)
        position += 1
    return tuple(tokens)


@cache
def _expected_schema_sql() -> tuple[tuple[str, str, tuple[str, ...]], ...]:
    with sqlite3.connect(":memory:") as connection:
        connection.row_factory = sqlite3.Row
        connection.executescript(_SCHEMA_SQL)
        return tuple(
            sorted(
                (
                    str(row["type"]),
                    str(row["name"]),
                    _normalize_schema_sql(str(row["sql"])),
                )
                for row in connection.execute(
                    """
                    SELECT type, name, sql FROM sqlite_master
                    WHERE name NOT LIKE 'sqlite_%'
                    """
                ).fetchall()
            )
        )


@dataclass(frozen=True, slots=True)
class ClaimedEvent:
    outbox_id: int
    attempts: int
    event: WatchEvent


class SQLiteStore:
    """SQLite persistence with an explicit transactional-outbox boundary."""

    SCHEMA_VERSION = 1
    REQUIRED_TABLES = frozenset(
        {
            "schema_meta",
            "watches",
            "observations",
            "authoritative_states",
            "events",
            "outbox",
            "delivery_attempts",
        }
    )
    _EXPECTED_SCHEMA_OBJECTS = frozenset(
        {("table", name) for name in REQUIRED_TABLES}
        | {
            ("index", "idx_observations_watch_time"),
            ("index", "idx_events_watch_dedupe"),
            ("index", "idx_outbox_due"),
        }
    )
    _EXPECTED_FOREIGN_KEYS = {
        "schema_meta": frozenset(),
        "watches": frozenset(),
        "observations": frozenset(
            {("watches", "watch_id", "watch_id", "NO ACTION", "NO ACTION", "NONE")}
        ),
        "authoritative_states": frozenset(
            {
                (
                    "observations",
                    "observation_id",
                    "observation_id",
                    "NO ACTION",
                    "NO ACTION",
                    "NONE",
                ),
                ("watches", "watch_id", "watch_id", "NO ACTION", "NO ACTION", "NONE"),
            }
        ),
        "events": frozenset(
            {("watches", "watch_id", "watch_id", "NO ACTION", "NO ACTION", "NONE")}
        ),
        "outbox": frozenset(
            {("events", "event_id", "event_id", "NO ACTION", "NO ACTION", "NONE")}
        ),
        "delivery_attempts": frozenset(
            {
                ("events", "event_id", "event_id", "NO ACTION", "NO ACTION", "NONE"),
                ("outbox", "outbox_id", "outbox_id", "NO ACTION", "NO ACTION", "NONE"),
            }
        ),
    }
    _EXPECTED_CHECK_FRAGMENTS = {
        "observations": _normalize_schema_sql(
            "CHECK(status IN ('VALID', 'DEGRADED', 'FAILED'))"
        ),
        "outbox": _normalize_schema_sql(
            "CHECK(status IN ('PENDING', 'DELIVERING', 'RETRY', 'DELIVERED', 'DEAD'))"
        ),
        "delivery_attempts": _normalize_schema_sql(
            "CHECK(status IN ('SUCCEEDED', 'FAILED'))"
        ),
    }
    _EXPECTED_INDEXES = {
        "idx_observations_watch_time": (
            "observations",
            ("watch_id", "observed_at"),
        ),
        "idx_events_watch_dedupe": ("events", ("watch_id", "dedupe_key")),
        "idx_outbox_due": (
            "outbox",
            ("status", "next_attempt_at", "outbox_id"),
        ),
    }
    _EXPECTED_TABLE_COLUMNS = {
        "schema_meta": (("version", "INTEGER", 1, None, 0),),
        "watches": (
            ("watch_id", "TEXT", 0, None, 1),
            ("execution_count", "INTEGER", 1, "0", 0),
            ("run_status", "TEXT", 1, "'IDLE'", 0),
            ("last_started_at", "TEXT", 0, None, 0),
            ("last_finished_at", "TEXT", 0, None, 0),
            ("last_observation_status", "TEXT", 0, None, 0),
            ("last_error", "TEXT", 0, None, 0),
            ("created_at", "TEXT", 1, None, 0),
            ("updated_at", "TEXT", 1, None, 0),
        ),
        "observations": (
            ("observation_id", "TEXT", 0, None, 1),
            ("watch_id", "TEXT", 1, None, 0),
            ("status", "TEXT", 1, None, 0),
            ("observed_at", "TEXT", 1, None, 0),
            ("state_json", "TEXT", 1, None, 0),
            ("evidence_json", "TEXT", 1, None, 0),
            ("error", "TEXT", 0, None, 0),
            ("created_at", "TEXT", 1, None, 0),
        ),
        "authoritative_states": (
            ("watch_id", "TEXT", 0, None, 1),
            ("observation_id", "TEXT", 1, None, 0),
            ("state_json", "TEXT", 1, None, 0),
            ("observed_at", "TEXT", 1, None, 0),
            ("updated_at", "TEXT", 1, None, 0),
        ),
        "events": (
            ("event_id", "TEXT", 0, None, 1),
            ("watch_id", "TEXT", 1, None, 0),
            ("schema_version", "TEXT", 1, None, 0),
            ("event_type", "TEXT", 1, None, 0),
            ("severity", "TEXT", 1, None, 0),
            ("occurred_at", "TEXT", 1, None, 0),
            ("dedupe_key", "TEXT", 1, None, 0),
            ("subject_json", "TEXT", 1, None, 0),
            ("payload_json", "TEXT", 1, None, 0),
            ("created_at", "TEXT", 1, None, 0),
        ),
        "outbox": (
            ("outbox_id", "INTEGER", 0, None, 1),
            ("event_id", "TEXT", 1, None, 0),
            ("status", "TEXT", 1, None, 0),
            ("attempts", "INTEGER", 1, "0", 0),
            ("next_attempt_at", "TEXT", 0, None, 0),
            ("locked_at", "TEXT", 0, None, 0),
            ("delivered_at", "TEXT", 0, None, 0),
            ("last_error", "TEXT", 0, None, 0),
            ("created_at", "TEXT", 1, None, 0),
            ("updated_at", "TEXT", 1, None, 0),
        ),
        "delivery_attempts": (
            ("attempt_id", "INTEGER", 0, None, 1),
            ("outbox_id", "INTEGER", 1, None, 0),
            ("event_id", "TEXT", 1, None, 0),
            ("attempt_number", "INTEGER", 1, None, 0),
            ("attempted_at", "TEXT", 1, None, 0),
            ("status", "TEXT", 1, None, 0),
            ("error", "TEXT", 0, None, 0),
        ),
    }

    def __init__(
        self,
        path: str | Path,
        *,
        observation_id_factory: Callable[[], str] = lambda: str(uuid4()),
        event_id_factory: Callable[[], str] = lambda: str(uuid4()),
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        if not isinstance(path, (str, Path)):
            raise TypeError("path must be a string or Path")
        if not str(path):
            raise ValueError("path must not be empty")
        for field_name, value in (
            ("observation_id_factory", observation_id_factory),
            ("event_id_factory", event_id_factory),
            ("clock", clock),
        ):
            if not callable(value):
                raise TypeError(f"{field_name} must be callable")
        self.path = str(path)
        self._observation_id_factory = observation_id_factory
        self._event_id_factory = event_id_factory
        self._clock = clock
        database_created = self._prepare_database_file()
        if not database_created:
            self._validate_existing_database_read_only()
        self._secure_database_files()
        self._initialize()

    def _prepare_database_file(self) -> bool:
        if self.path == ":memory:":
            raise ValueError("SQLiteStore requires a file-backed database path")
        database = Path(self.path)
        if database.is_symlink():
            raise ValueError("SQLite database path must not be a symbolic link")
        flags = os.O_CREAT | os.O_EXCL | os.O_RDWR
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(database, flags, 0o600)
        except FileExistsError:
            file_status = database.stat(follow_symlinks=False)
            if (
                database.is_symlink()
                or not stat.S_ISREG(file_status.st_mode)
                or (os.name == "posix" and file_status.st_nlink != 1)
            ):
                raise ValueError(
                    "SQLite database path must be a single-link regular file"
                ) from None
            return False
        else:
            os.close(descriptor)
            return True

    def _validate_existing_database_read_only(self) -> None:
        database_uri = f"{Path(self.path).resolve(strict=True).as_uri()}?mode=ro"
        with sqlite3.connect(database_uri, uri=True) as connection:
            connection.row_factory = sqlite3.Row
            existing_objects = self._read_schema_objects(connection)
            existing_tables = {
                name for object_type, name in existing_objects if object_type == "table"
            }
            if existing_objects and "schema_meta" not in existing_tables:
                raise RuntimeError(
                    "database is not an initialized watch-engine database"
                )
            if "schema_meta" in existing_tables:
                self._validate_table_layout(connection, "schema_meta")
                rows = connection.execute(
                    "SELECT version FROM schema_meta LIMIT 2"
                ).fetchall()
                self._validate_schema_meta(rows)
                self._validate_required_tables(existing_tables)
                self._validate_table_layouts(connection)
                self._validate_schema_objects(existing_objects)
                self._validate_expected_indexes(connection)
                self._validate_schema_sql(connection)

    @staticmethod
    def _read_schema_objects(
        connection: sqlite3.Connection,
    ) -> set[tuple[str, str]]:
        return {
            (str(row["type"]), str(row["name"]))
            for row in connection.execute(
                """
                SELECT type, name FROM sqlite_master
                WHERE name NOT LIKE 'sqlite_%'
                """
            ).fetchall()
        }

    def _secure_database_files(self) -> None:
        if os.name != "posix" or self.path == ":memory:":
            return
        for candidate in (self.path, f"{self.path}-wal", f"{self.path}-shm"):
            descriptor: int | None = None
            with suppress(FileNotFoundError):
                flags = os.O_RDONLY
                if hasattr(os, "O_NOFOLLOW"):
                    flags |= os.O_NOFOLLOW
                descriptor = os.open(candidate, flags)
                try:
                    file_status = os.fstat(descriptor)
                    if (
                        not stat.S_ISREG(file_status.st_mode)
                        or file_status.st_nlink != 1
                    ):
                        raise ValueError(
                            "SQLite database files must be single-link regular files"
                        )
                    os.fchmod(descriptor, 0o600)
                finally:
                    os.close(descriptor)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, isolation_level=None, timeout=5.0)
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA busy_timeout = 5000")
            journal_mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
            if str(journal_mode).lower() != "wal":
                raise RuntimeError("SQLite WAL mode is required")
            secure_delete = connection.execute("PRAGMA secure_delete = ON").fetchone()[0]
            if int(secure_delete) != 1:
                raise RuntimeError("SQLite secure_delete support is required")
            self._secure_database_files()
            return connection
        except Exception:
            connection.close()
            raise

    def _initialize(self) -> None:
        with self._connect() as connection:
            existing_objects = self._read_schema_objects(connection)
            existing_tables = {
                name for object_type, name in existing_objects if object_type == "table"
            }
            if existing_objects and "schema_meta" not in existing_tables:
                raise RuntimeError(
                    "database is not an initialized watch-engine database"
                )
            if "schema_meta" in existing_tables:
                self._validate_table_layout(connection, "schema_meta")
                rows = connection.execute(
                    "SELECT version FROM schema_meta LIMIT 2"
                ).fetchall()
                self._validate_schema_meta(rows)
                self._validate_required_tables(existing_tables)
                self._validate_table_layouts(connection)
                self._validate_schema_objects(existing_objects)
                self._validate_expected_indexes(connection)
                self._validate_schema_sql(connection)
                return

            try:
                connection.executescript(_SCHEMA_SQL)
                connection.execute(
                    "INSERT INTO schema_meta(version) VALUES (?)",
                    (self.SCHEMA_VERSION,),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    def _validate_schema_meta(self, rows: Sequence[sqlite3.Row]) -> None:
        if len(rows) != 1:
            raise RuntimeError("schema_meta must contain exactly one row")
        try:
            raw_version = rows[0]["version"]
        except (IndexError, KeyError):
            raise RuntimeError("schema_meta contains an invalid version") from None
        if isinstance(raw_version, bool) or not isinstance(raw_version, int):
            raise RuntimeError("schema_meta contains an invalid version")
        version = raw_version
        if version != self.SCHEMA_VERSION:
            raise RuntimeError(
                f"unsupported database schema {rows[0]['version']}; "
                f"expected {self.SCHEMA_VERSION}"
            )

    def _validate_required_tables(self, existing_tables: set[str]) -> None:
        missing = self.REQUIRED_TABLES - existing_tables
        if missing:
            raise RuntimeError("watch-engine database schema is incomplete")
        if existing_tables != self.REQUIRED_TABLES:
            raise RuntimeError("watch-engine database contains unexpected tables")

    def _validate_schema_objects(
        self, existing_objects: set[tuple[str, str]]
    ) -> None:
        if existing_objects != self._EXPECTED_SCHEMA_OBJECTS:
            raise RuntimeError("watch-engine database schema objects are incompatible")

    @staticmethod
    def _validate_schema_sql(connection: sqlite3.Connection) -> None:
        actual = tuple(
            sorted(
                (
                    str(row["type"]),
                    str(row["name"]),
                    _normalize_schema_sql(str(row["sql"])),
                )
                for row in connection.execute(
                    """
                    SELECT type, name, sql FROM sqlite_master
                    WHERE name NOT LIKE 'sqlite_%'
                    """
                ).fetchall()
            )
        )
        if actual != _expected_schema_sql():
            raise RuntimeError("watch-engine database schema SQL is incompatible")

    def _validate_expected_indexes(self, connection: sqlite3.Connection) -> None:
        for index_name, (table_name, expected_columns) in self._EXPECTED_INDEXES.items():
            index_rows = connection.execute(
                f"PRAGMA index_list({table_name})"  # noqa: S608 - fixed internal names
            ).fetchall()
            matching = [row for row in index_rows if str(row["name"]) == index_name]
            if len(matching) != 1:
                raise RuntimeError("watch-engine database index definitions are incompatible")
            index = matching[0]
            columns = self._read_index_columns(connection, index_name)
            if (
                int(index["unique"]) != 0
                or str(index["origin"]) != "c"
                or int(index["partial"]) != 0
                or columns != expected_columns
            ):
                raise RuntimeError("watch-engine database index definitions are incompatible")

    def _validate_table_layouts(self, connection: sqlite3.Connection) -> None:
        for table_name in self._EXPECTED_TABLE_COLUMNS:
            self._validate_table_layout(connection, table_name)

    def _validate_table_layout(
        self, connection: sqlite3.Connection, table_name: str
    ) -> None:
        expected = self._EXPECTED_TABLE_COLUMNS[table_name]
        rows = connection.execute(
            f"PRAGMA table_info({table_name})"  # noqa: S608 - fixed internal names
        ).fetchall()
        extended_rows = connection.execute(
            f"PRAGMA table_xinfo({table_name})"  # noqa: S608 - fixed internal names
        ).fetchall()
        actual = tuple(
            (
                str(row["name"]),
                str(row["type"]).upper(),
                int(row["notnull"]),
                str(row["dflt_value"])
                if row["dflt_value"] is not None
                else None,
                int(row["pk"]),
            )
            for row in rows
        )
        if actual != expected:
            raise RuntimeError(
                f"watch-engine database table layout is incompatible: {table_name}"
            )
        if len(extended_rows) != len(rows) or any(
            int(row["hidden"]) != 0 for row in extended_rows
        ):
            raise RuntimeError(
                f"watch-engine database table layout is incompatible: {table_name}"
            )
        self._validate_foreign_keys(connection, table_name)
        self._validate_check_constraint(connection, table_name)
        if table_name == "outbox":
            self._validate_unique_index(connection, table_name, ("event_id",))

    def _validate_foreign_keys(
        self, connection: sqlite3.Connection, table_name: str
    ) -> None:
        actual = frozenset(
            (
                str(row["table"]),
                str(row["from"]),
                str(row["to"]),
                str(row["on_update"]),
                str(row["on_delete"]),
                str(row["match"]),
            )
            for row in connection.execute(
                f"PRAGMA foreign_key_list({table_name})"  # noqa: S608
            ).fetchall()
        )
        if actual != self._EXPECTED_FOREIGN_KEYS[table_name]:
            raise RuntimeError(
                f"watch-engine database foreign keys are incompatible: {table_name}"
            )

    def _validate_check_constraint(
        self, connection: sqlite3.Connection, table_name: str
    ) -> None:
        expected = self._EXPECTED_CHECK_FRAGMENTS.get(table_name)
        if expected is None:
            return
        row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        ).fetchone()
        if row is None or not isinstance(row["sql"], str):
            raise RuntimeError(
                f"watch-engine database constraints are incompatible: {table_name}"
            )
        normalized = _normalize_schema_sql(row["sql"])
        contains_expected = any(
            normalized[position : position + len(expected)] == expected
            for position in range(len(normalized) - len(expected) + 1)
        )
        if not contains_expected:
            raise RuntimeError(
                f"watch-engine database constraints are incompatible: {table_name}"
            )

    @staticmethod
    def _read_index_columns(
        connection: sqlite3.Connection, index_name: str
    ) -> tuple[str, ...]:
        escaped_name = index_name.replace('"', '""')
        return tuple(
            str(column["name"])
            for column in connection.execute(
                f'PRAGMA index_info("{escaped_name}")'  # noqa: S608
            ).fetchall()
        )

    @classmethod
    def _validate_unique_index(
        cls,
        connection: sqlite3.Connection,
        table_name: str,
        expected_columns: tuple[str, ...],
    ) -> None:
        for row in connection.execute(
            f"PRAGMA index_list({table_name})"  # noqa: S608 - fixed internal names
        ).fetchall():
            if int(row["unique"]) != 1 or int(row["partial"]) != 0:
                continue
            columns = cls._read_index_columns(connection, str(row["name"]))
            if columns == expected_columns:
                return
        raise RuntimeError(
            f"watch-engine database unique constraints are incompatible: {table_name}"
        )

    def _ensure_watch(
        self, connection: sqlite3.Connection, watch_id: str, now: datetime
    ) -> None:
        self._validate_watch_id(watch_id)
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
        timestamp = now if now is not None else self._clock()
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
        self._validate_watch_id(watch_id)
        timestamp = now if now is not None else self._clock()
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE watches SET run_status = 'ERROR', last_error = ?,
                    last_finished_at = ?, updated_at = ? WHERE watch_id = ?
                """,
                (bounded_error_text(error), to_iso(timestamp), to_iso(timestamp), watch_id),
            )

    def record_observation(
        self,
        watch_id: str,
        observation: Observation,
        policy: TransitionPolicy,
        *,
        now: datetime | None = None,
    ) -> tuple[WatchEvent, ...]:
        """Persist evidence first, then atomically promote eligible valid evidence.

        Observation evidence survives any later policy or promotion failure. For a
        non-stale VALID observation, authority, events, and outbox rows are one
        independent transaction.
        """
        self._validate_watch_id(watch_id)
        timestamp = now if now is not None else self._clock()
        observation_id = self._observation_id_factory()
        try:
            _require_non_empty_string(observation_id, field="observation_id")
        except (TypeError, ValueError) as exc:
            raise type(exc)(f"observation_id_factory returned invalid data: {exc}") from None
        self._persist_observation(watch_id, observation_id, observation, timestamp)
        if observation.status is not ObservationStatus.VALID:
            return ()
        return self._promote_valid_observation(
            watch_id, observation_id, policy, timestamp
        )

    def _persist_observation(
        self,
        watch_id: str,
        observation_id: str,
        observation: Observation,
        now: datetime,
    ) -> None:
        with self._connect() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._ensure_watch(connection, watch_id, now)
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
                        to_iso(now),
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
                        to_iso(now),
                        observation.status.value,
                        observation.error,
                        to_iso(now),
                        watch_id,
                    ),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    def _promote_valid_observation(
        self,
        watch_id: str,
        observation_id: str,
        policy: TransitionPolicy,
        now: datetime,
    ) -> tuple[WatchEvent, ...]:
        with self._connect() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                current_row = connection.execute(
                    """
                    SELECT status, observed_at, state_json, evidence_json, error
                    FROM observations
                    WHERE observation_id = ? AND watch_id = ?
                    """,
                    (observation_id, watch_id),
                ).fetchone()
                if current_row is None:
                    raise RuntimeError("persisted observation is missing during promotion")
                current = self._observation_from_row(current_row)
                authority_state_json = str(current_row["state_json"])
                previous = self._read_authoritative(connection, watch_id)
                if previous is not None and current.observed_at <= previous.observed_at:
                    connection.commit()
                    logger.info(
                        "stale valid observation retained without authority promotion",
                        extra={
                            "observed_at": to_iso(current.observed_at),
                            "authority_observed_at": to_iso(previous.observed_at),
                        },
                    )
                    return ()

                # Policy is deliberately inside this atomic promotion decision. Its
                # public contract therefore requires fast, pure, side-effect-free work.
                raw_drafts = policy.evaluate(previous, current)
                if isinstance(raw_drafts, (str, bytes)) or not isinstance(
                    raw_drafts, Sequence
                ):
                    raise TypeError(
                        "transition policy evaluate() must return a sequence of EventDraft"
                    )
                drafts = tuple(raw_drafts)
                if not all(isinstance(draft, EventDraft) for draft in drafts):
                    raise TypeError(
                        "transition policy evaluate() must return a sequence of EventDraft"
                    )
                created_events = self._insert_events(
                    connection, watch_id, current, drafts, now
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
                        authority_state_json,
                        to_iso(current.observed_at),
                        to_iso(now),
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
            connection.execute(
                """
                INSERT INTO events(
                    event_id, watch_id, schema_version, event_type, severity, occurred_at,
                    dedupe_key, subject_json, payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        self._validate_watch_id(watch_id)
        with self._connect() as connection:
            return self._read_authoritative(connection, watch_id)

    def get_watch_status(self, watch_id: str) -> WatchStatus | None:
        """Return the latest persisted diagnostic snapshot without exposing tables."""

        self._validate_watch_id(watch_id)
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT watch_id, execution_count, run_status, last_started_at,
                       last_finished_at, last_observation_status, last_error
                FROM watches WHERE watch_id = ?
                """,
                (watch_id,),
            ).fetchone()
        if row is None:
            return None
        last_observation_status = row["last_observation_status"]
        return WatchStatus(
            watch_id=str(row["watch_id"]),
            observation_count=int(row["execution_count"]),
            run_status=RunStatus(row["run_status"]),
            last_started_at=(
                from_iso(row["last_started_at"])
                if row["last_started_at"] is not None
                else None
            ),
            last_finished_at=(
                from_iso(row["last_finished_at"])
                if row["last_finished_at"] is not None
                else None
            ),
            last_observation_status=(
                ObservationStatus(last_observation_status)
                if last_observation_status is not None
                else None
            ),
            last_error=row["last_error"],
        )

    def list_observations(self, watch_id: str) -> list[Observation]:
        self._validate_watch_id(watch_id)
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
        self._validate_watch_id(watch_id)
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
            schema_version=row["schema_version"],
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
        timestamp = to_iso(now if now is not None else self._clock())
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
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise TypeError("limit must be an integer")
        if limit < 1:
            raise ValueError("limit must be at least 1")
        if limit > _MAX_DELIVERY_BATCH_SIZE:
            raise ValueError(f"limit must be at most {_MAX_DELIVERY_BATCH_SIZE}")
        timestamp = now if now is not None else self._clock()
        due_timestamp_sql = _sortable_timestamp_sql("o.next_attempt_at")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                f"""
                SELECT o.outbox_id, o.attempts, e.*
                FROM outbox o JOIN events e ON e.event_id = o.event_id
                WHERE o.status IN ('PENDING', 'RETRY') AND {due_timestamp_sql} <= ?
                ORDER BY o.outbox_id LIMIT ?
                """,  # noqa: S608 - only a validated internal column is composed
                (to_sortable_iso(timestamp), limit),
            ).fetchall()
            ids = [int(row["outbox_id"]) for row in rows]
            if ids:
                placeholders = ",".join("?" for _ in ids)
                claim_sql = (
                    f"UPDATE outbox SET status = 'DELIVERING', locked_at = ?, "  # noqa: S608
                    f"updated_at = ? WHERE outbox_id IN ({placeholders})"
                )
                connection.execute(
                    claim_sql,
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
        timestamp = now if now is not None else self._clock()
        attempt_number = claimed.attempts + 1
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                """
                UPDATE outbox SET status = 'DELIVERED', attempts = ?, delivered_at = ?,
                    next_attempt_at = NULL, locked_at = NULL, last_error = NULL, updated_at = ?
                WHERE outbox_id = ? AND event_id = ? AND attempts = ?
                  AND status = 'DELIVERING'
                """,
                (
                    attempt_number,
                    to_iso(timestamp),
                    to_iso(timestamp),
                    claimed.outbox_id,
                    claimed.event.event_id,
                    claimed.attempts,
                ),
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
        timestamp = now if now is not None else self._clock()
        attempt_number = claimed.attempts + 1
        status = "DEAD" if retry_at is None else "RETRY"
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                """
                UPDATE outbox SET status = ?, attempts = ?, next_attempt_at = ?,
                    locked_at = NULL, last_error = ?, updated_at = ?
                WHERE outbox_id = ? AND event_id = ? AND attempts = ?
                  AND status = 'DELIVERING'
                """,
                (
                    status,
                    attempt_number,
                    to_iso(retry_at) if retry_at is not None else None,
                    bounded_error_text(error),
                    to_iso(timestamp),
                    claimed.outbox_id,
                    claimed.event.event_id,
                    claimed.attempts,
                ),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise RuntimeError(f"outbox claim {claimed.outbox_id} is no longer active")
            self._insert_attempt(
                connection,
                claimed,
                attempt_number,
                timestamp,
                status="FAILED",
                error=bounded_error_text(error),
            )
            connection.commit()

    def purge_before(
        self, cutoff: datetime, *, watch_id: str | None = None
    ) -> PurgeResult:
        """Delete old terminal history while preserving authority and undelivered events."""
        cutoff_text = to_sortable_iso(require_aware(cutoff, field="cutoff"))
        event_filter = f"{_sortable_timestamp_sql('e.created_at')} < ?"
        observation_filter = f"{_sortable_timestamp_sql('o.created_at')} < ?"
        event_parameters: list[str] = [cutoff_text]
        observation_parameters: list[str] = [cutoff_text]
        if watch_id is not None:
            self._validate_watch_id(watch_id)
            event_filter += " AND e.watch_id = ?"
            observation_filter += " AND o.watch_id = ?"
            event_parameters.append(watch_id)
            observation_parameters.append(watch_id)

        with self._connect() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    """
                    CREATE TEMP TABLE purge_targets(
                        outbox_id INTEGER PRIMARY KEY,
                        event_id TEXT NOT NULL UNIQUE
                    )
                    """
                )
                connection.execute(
                    f"""
                    INSERT INTO purge_targets(outbox_id, event_id)
                    SELECT o.outbox_id, e.event_id
                    FROM outbox o JOIN events e ON e.event_id = o.event_id
                    WHERE {event_filter} AND o.status IN ('DELIVERED', 'DEAD')
                    """,  # noqa: S608 - only fixed clauses are composed
                    event_parameters,
                )
                attempts_deleted = connection.execute(
                    """
                    DELETE FROM delivery_attempts
                    WHERE outbox_id IN (SELECT outbox_id FROM purge_targets)
                    """
                ).rowcount
                outbox_deleted = connection.execute(
                    """
                    DELETE FROM outbox
                    WHERE outbox_id IN (SELECT outbox_id FROM purge_targets)
                    """
                ).rowcount
                events_deleted = connection.execute(
                    """
                    DELETE FROM events
                    WHERE event_id IN (SELECT event_id FROM purge_targets)
                    """
                ).rowcount

                observations_deleted = connection.execute(
                    f"""
                    DELETE FROM observations AS o
                    WHERE {observation_filter}
                      AND NOT EXISTS (
                          SELECT 1 FROM authoritative_states a
                          WHERE a.observation_id = o.observation_id
                      )
                    """,  # noqa: S608 - only fixed clauses are composed
                    observation_parameters,
                ).rowcount
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return PurgeResult(
            observations_deleted=observations_deleted,
            events_deleted=events_deleted,
            outbox_rows_deleted=outbox_deleted,
            delivery_attempts_deleted=attempts_deleted,
        )

    def delete_watch(self, watch_id: str, *, allow_undelivered: bool = False) -> PurgeResult:
        """Delete one watch after owners stop, refusing queued delivery by default."""
        self._validate_watch_id(watch_id)
        if not isinstance(allow_undelivered, bool):
            raise TypeError("allow_undelivered must be a bool")
        with self._connect() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                undelivered = connection.execute(
                    """
                    SELECT COUNT(*) FROM outbox o
                    JOIN events e ON e.event_id = o.event_id
                    WHERE e.watch_id = ? AND o.status IN ('PENDING', 'RETRY', 'DELIVERING')
                    """,
                    (watch_id,),
                ).fetchone()[0]
                if undelivered and not allow_undelivered:
                    raise RuntimeError("watch has undelivered outbox events")

                attempts_deleted = connection.execute(
                    """
                    DELETE FROM delivery_attempts WHERE event_id IN (
                        SELECT event_id FROM events WHERE watch_id = ?
                    )
                    """,
                    (watch_id,),
                ).rowcount
                outbox_deleted = connection.execute(
                    """
                    DELETE FROM outbox WHERE event_id IN (
                        SELECT event_id FROM events WHERE watch_id = ?
                    )
                    """,
                    (watch_id,),
                ).rowcount
                events_deleted = connection.execute(
                    "DELETE FROM events WHERE watch_id = ?", (watch_id,)
                ).rowcount
                connection.execute(
                    "DELETE FROM authoritative_states WHERE watch_id = ?", (watch_id,)
                )
                observations_deleted = connection.execute(
                    "DELETE FROM observations WHERE watch_id = ?", (watch_id,)
                ).rowcount
                watches_deleted = connection.execute(
                    "DELETE FROM watches WHERE watch_id = ?", (watch_id,)
                ).rowcount
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return PurgeResult(
            observations_deleted=observations_deleted,
            events_deleted=events_deleted,
            outbox_rows_deleted=outbox_deleted,
            delivery_attempts_deleted=attempts_deleted,
            watches_deleted=watches_deleted,
        )

    def compact_storage(self) -> None:
        """Checkpoint and compact SQLite after all other store owners have stopped."""
        with self._connect() as connection:
            checkpoint = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            if checkpoint is None or int(checkpoint[0]) != 0:
                raise RuntimeError(
                    "SQLite WAL checkpoint is busy; stop all other database owners"
                )
            connection.execute("VACUUM")
        self._secure_database_files()

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

    @staticmethod
    def _validate_watch_id(watch_id: object) -> None:
        _require_non_empty_string(watch_id, field="watch_id")
