from __future__ import annotations

import logging
import os
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tests.helpers import SequenceObserver, StateChangePolicy
from watch_engine import (
    ManualTrigger,
    Observation,
    OutboxDispatcher,
    SQLiteStore,
    WatchDefinition,
    WatchEvent,
    WatchRuntime,
)

NOW = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)


def make_event(store: SQLiteStore) -> WatchEvent:
    definition = WatchDefinition(
        watch_id="lifecycle",
        trigger=ManualTrigger(),
        observer=SequenceObserver(
            [
                Observation.valid("A", observed_at=NOW),
                Observation.valid("B", observed_at=NOW + timedelta(seconds=1)),
            ]
        ),
        transition_policy=StateChangePolicy(),
    )
    runtime = WatchRuntime(store, clock=lambda: NOW)
    runtime.run_once(definition)
    return runtime.run_once(definition).events[0]


class AcceptingSink:
    def deliver(self, event: WatchEvent) -> None:
        return None


class LeakingSink:
    def __init__(self, secret: str) -> None:
        self.secret = secret

    def deliver(self, event: WatchEvent) -> None:
        raise RuntimeError(self.secret)


@pytest.mark.skipif(os.name != "posix", reason="POSIX permission semantics")
def test_database_and_live_sidecars_are_owner_only(tmp_path: Path) -> None:
    previous = os.umask(0o022)
    try:
        store = SQLiteStore(tmp_path / "watch.db")
        with store._connect() as connection:
            connection.execute("CREATE TABLE permission_probe(value INTEGER)")
            connection.execute("INSERT INTO permission_probe VALUES (1)")
            for path in (store.path, f"{store.path}-wal", f"{store.path}-shm"):
                assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    finally:
        os.umask(previous)


@pytest.mark.skipif(os.name != "posix", reason="POSIX permission semantics")
def test_existing_database_permission_is_tightened(tmp_path: Path) -> None:
    database = tmp_path / "existing.db"
    database.touch(mode=0o644)
    SQLiteStore(database)
    assert stat.S_IMODE(database.stat().st_mode) == 0o600


def test_symbolic_link_database_is_rejected(tmp_path: Path) -> None:
    target = tmp_path / "target.db"
    target.touch()
    link = tmp_path / "link.db"
    link.symlink_to(target)
    with pytest.raises(ValueError, match="symbolic link"):
        SQLiteStore(link)


def test_purge_removes_only_terminal_history_and_preserves_authority(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    make_event(store)
    OutboxDispatcher(store, AcceptingSink(), clock=lambda: NOW).dispatch_ready()

    result = store.purge_before(NOW + timedelta(minutes=1))

    assert result.events_deleted == 1
    assert result.delivery_attempts_deleted == 1
    assert result.observations_deleted == 1
    assert store.outbox_rows() == []
    authority = store.get_authoritative_observation("lifecycle")
    assert authority is not None and authority.state == "B"


def test_purge_preserves_undelivered_events(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    make_event(store)

    result = store.purge_before(NOW + timedelta(minutes=1))

    assert result.events_deleted == 0
    assert store.outbox_rows()[0]["status"] == "PENDING"


def test_delete_watch_requires_explicit_undelivered_override(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    make_event(store)
    with pytest.raises(RuntimeError, match="undelivered"):
        store.delete_watch("lifecycle")

    result = store.delete_watch("lifecycle", allow_undelivered=True)
    assert result.watches_deleted == 1
    assert result.events_deleted == 1
    assert result.observations_deleted == 2
    assert store.list_observations("lifecycle") == []


def test_compact_storage_keeps_database_readable(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    make_event(store)
    store.compact_storage()
    assert store.get_authoritative_observation("lifecycle") is not None


def test_exception_message_never_reaches_logs_or_database(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    secret = "private-token-should-not-persist"
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    definition = WatchDefinition(
        watch_id="redaction",
        trigger=ManualTrigger(),
        observer=SequenceObserver([RuntimeError(secret)] * 3),
        transition_policy=StateChangePolicy(),
    )
    with caplog.at_level(logging.WARNING):
        result = WatchRuntime(store, clock=lambda: NOW, sleep=lambda _: None).run_once(definition)

    assert result.observation.error == "RuntimeError: operation failed"
    assert secret not in caplog.text
    assert all(secret.encode() not in path.read_bytes() for path in tmp_path.glob("watch.db*"))


def test_sink_exception_message_never_reaches_logs_or_database(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    secret = "private-sink-token-should-not-persist"
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    make_event(store)
    with caplog.at_level(logging.ERROR):
        result = OutboxDispatcher(store, LeakingSink(secret), clock=lambda: NOW).dispatch_ready()

    assert result[0].error == "RuntimeError: operation failed"
    assert secret not in caplog.text
    assert all(secret.encode() not in path.read_bytes() for path in tmp_path.glob("watch.db*"))


def test_json_fields_have_a_one_mebibyte_encoded_limit() -> None:
    with pytest.raises(ValueError, match="exceeds 1048576"):
        Observation.valid({"value": "x" * 1_048_576}, observed_at=NOW)


def test_caller_supplied_error_is_bounded() -> None:
    observation = Observation.failed(observed_at=NOW, error="x" * 10_000)
    assert observation.error is not None
    assert len(observation.error) == 2_048
    assert observation.error.endswith("...<truncated>")
