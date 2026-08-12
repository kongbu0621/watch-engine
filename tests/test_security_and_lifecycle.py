from __future__ import annotations

import logging
import os
import sqlite3
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tests.helpers import SequenceObserver, StateChangePolicy
from watch_engine import (
    DeliveryConfig,
    EventDraft,
    ManualTrigger,
    Observation,
    OutboxDispatcher,
    PurgeResult,
    RetryPolicy,
    SQLiteStore,
    WatchDefinition,
    WatchEvent,
    WatchRuntime,
)

NOW = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)


def make_event(store: SQLiteStore, *, watch_id: str = "lifecycle") -> WatchEvent:
    definition = WatchDefinition(
        watch_id=watch_id,
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


class LeakingPolicy:
    def __init__(self, secret: str) -> None:
        self.secret = secret

    def evaluate(self, previous: Observation | None, current: Observation) -> list[EventDraft]:
        if previous is not None:
            raise RuntimeError(self.secret)
        return []


class AdapterSpecificSecretError(RuntimeError):
    pass


class BulkEventPolicy:
    def __init__(self, count: int) -> None:
        self.count = count

    def evaluate(self, previous: Observation | None, current: Observation) -> list[EventDraft]:
        return [
            EventDraft(
                event_type="bulk.event",
                severity="info",
                dedupe_key=f"bulk-{index}",
                subject={},
                payload={"index": index},
            )
            for index in range(self.count)
        ]


class FastBusyTimeoutStore(SQLiteStore):
    def _connect(self) -> sqlite3.Connection:
        connection = super()._connect()
        connection.execute("PRAGMA busy_timeout = 1")
        return connection


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


def test_secure_delete_is_enabled_on_every_connection(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db")
    with store._connect() as connection:
        assert connection.execute("PRAGMA secure_delete").fetchone()[0] == 1


def test_symbolic_link_database_is_rejected(tmp_path: Path) -> None:
    target = tmp_path / "target.db"
    target.touch()
    link = tmp_path / "link.db"
    link.symlink_to(target)
    with pytest.raises(ValueError, match="symbolic link"):
        SQLiteStore(link)


def test_memory_database_fails_fast_instead_of_losing_schema_between_connections() -> None:
    with pytest.raises(ValueError, match="file-backed"):
        SQLiteStore(":memory:")


def test_existing_unrelated_sqlite_database_is_not_modified(tmp_path: Path) -> None:
    database = tmp_path / "unrelated.db"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE private_records(value TEXT)")
        connection.execute("INSERT INTO private_records VALUES ('keep-me')")
    if os.name == "posix":
        database.chmod(0o644)

    with pytest.raises(RuntimeError, match="not an initialized watch-engine database"):
        SQLiteStore(database)

    with sqlite3.connect(database) as connection:
        journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
        objects = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        ).fetchall()
        value = connection.execute("SELECT value FROM private_records").fetchone()[0]
    assert journal_mode == "delete"
    assert objects == [("private_records",)]
    assert value == "keep-me"
    assert not Path(f"{database}-wal").exists()
    assert not Path(f"{database}-shm").exists()
    if os.name == "posix":
        assert stat.S_IMODE(database.stat().st_mode) == 0o644


def test_existing_sqlite_database_with_only_a_view_is_not_modified(
    tmp_path: Path,
) -> None:
    database = tmp_path / "unrelated-view.db"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE VIEW private_view AS SELECT 'keep-me' AS value")

    with pytest.raises(RuntimeError, match="not an initialized watch-engine database"):
        SQLiteStore(database)

    with sqlite3.connect(database) as connection:
        objects = connection.execute(
            "SELECT type, name FROM sqlite_master ORDER BY type, name"
        ).fetchall()
        value = connection.execute("SELECT value FROM private_view").fetchone()[0]
    assert objects == [("view", "private_view")]
    assert value == "keep-me"


def test_incompatible_database_is_rejected_before_creating_engine_tables(
    tmp_path: Path,
) -> None:
    database = tmp_path / "future.db"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE schema_meta(version INTEGER NOT NULL)")
        connection.execute("INSERT INTO schema_meta VALUES (999)")
        connection.execute("CREATE TABLE future_private_state(value TEXT)")

    with pytest.raises(RuntimeError, match="unsupported database schema 999"):
        SQLiteStore(database)

    with sqlite3.connect(database) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert tables == {"schema_meta", "future_private_state"}


def test_coincidental_schema_meta_table_does_not_claim_unrelated_database(
    tmp_path: Path,
) -> None:
    database = tmp_path / "coincidental.db"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE schema_meta(version INTEGER NOT NULL)")
        connection.execute("INSERT INTO schema_meta VALUES (1)")
        connection.execute("CREATE TABLE private_state(value TEXT)")

    with pytest.raises(RuntimeError, match="schema is incomplete"):
        SQLiteStore(database)

    with sqlite3.connect(database) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
    assert tables == {"schema_meta", "private_state"}
    assert journal_mode == "delete"


def test_lookalike_table_names_with_wrong_layout_are_rejected_without_mutation(
    tmp_path: Path,
) -> None:
    database = tmp_path / "lookalike.db"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE schema_meta(version INTEGER NOT NULL)")
        connection.execute("INSERT INTO schema_meta VALUES (1)")
        for table_name in SQLiteStore.REQUIRED_TABLES - {"schema_meta"}:
            connection.execute(f'CREATE TABLE "{table_name}"(wrong_column TEXT)')
    if os.name == "posix":
        database.chmod(0o644)

    with pytest.raises(RuntimeError, match="table layout is incompatible"):
        SQLiteStore(database)

    with sqlite3.connect(database) as connection:
        journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert tables == SQLiteStore.REQUIRED_TABLES
    assert journal_mode == "delete"
    assert not Path(f"{database}-wal").exists()
    assert not Path(f"{database}-shm").exists()
    if os.name == "posix":
        assert stat.S_IMODE(database.stat().st_mode) == 0o644


@pytest.mark.parametrize("path", [None, 1, object()])
def test_storage_rejects_wrong_typed_database_paths(path: object) -> None:
    with pytest.raises(TypeError, match="path must be a string or Path"):
        SQLiteStore(path)  # type: ignore[arg-type]


def test_storage_rejects_empty_database_path() -> None:
    with pytest.raises(ValueError, match="path must not be empty"):
        SQLiteStore("")


@pytest.mark.parametrize(
    ("keyword", "value"),
    [
        ("observation_id_factory", None),
        ("event_id_factory", 0),
        ("clock", "clock"),
    ],
)
def test_storage_rejects_non_callable_dependencies(
    tmp_path: Path, keyword: str, value: object
) -> None:
    with pytest.raises(TypeError, match=f"{keyword} must be callable"):
        SQLiteStore(tmp_path / "watch.db", **{keyword: value})  # type: ignore[arg-type]


def test_storage_public_methods_reject_empty_watch_id(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db")
    with pytest.raises(ValueError, match="watch_id must not be empty"):
        store.record_observation(
            "",
            Observation.valid("state", observed_at=NOW),
            StateChangePolicy(),
            now=NOW,
        )
    with pytest.raises(ValueError, match="watch_id must not be empty"):
        store.delete_watch("")


def test_storage_rejects_invalid_observation_id_factory(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db", observation_id_factory=lambda: "")
    with pytest.raises(ValueError, match="observation_id_factory"):
        store.record_observation(
            "watch",
            Observation.valid("state", observed_at=NOW),
            StateChangePolicy(),
            now=NOW,
        )


def test_storage_rejects_oversized_observation_id_factory_value(tmp_path: Path) -> None:
    store = SQLiteStore(
        tmp_path / "watch.db", observation_id_factory=lambda: "x" * 2_049
    )
    with pytest.raises(ValueError, match="observation_id_factory.*2048"):
        store.record_observation(
            "watch",
            Observation.valid("state", observed_at=NOW),
            StateChangePolicy(),
            now=NOW,
        )

    assert store.list_observations("watch") == []


def test_adoption_guide_does_not_teach_exception_detail_persistence() -> None:
    guide = (
        Path(__file__).parents[1] / "docs" / "adoption-guide.zh-CN.md"
    ).read_text(encoding="utf-8")

    assert "error=str(exc)" not in guide
    assert '"error_type": type(exc).__name__' not in guide


def test_purge_result_keeps_existing_positional_field_order() -> None:
    result = PurgeResult(1, 2, 3, 4)
    assert result.observations_deleted == 1
    assert result.events_deleted == 2
    assert result.delivery_attempts_deleted == 3
    assert result.watches_deleted == 4
    assert result.outbox_rows_deleted == 0


def test_purge_removes_only_terminal_history_and_preserves_authority(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    make_event(store)
    OutboxDispatcher(store, AcceptingSink(), clock=lambda: NOW).dispatch_ready()

    result = store.purge_before(NOW + timedelta(minutes=1))

    assert result.events_deleted == 1
    assert result.outbox_rows_deleted == 1
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


def test_purge_preserves_retry_and_delivering_events(tmp_path: Path) -> None:
    retry_store = SQLiteStore(tmp_path / "retry.db", clock=lambda: NOW)
    make_event(retry_store)
    OutboxDispatcher(
        retry_store,
        LeakingSink("temporary failure"),
        config=DeliveryConfig(
            retry=RetryPolicy(
                max_attempts=2, base_delay_seconds=1, maximum_delay_seconds=1
            )
        ),
        clock=lambda: NOW,
    ).dispatch_ready()
    assert retry_store.outbox_rows()[0]["status"] == "RETRY"
    assert retry_store.purge_before(NOW + timedelta(minutes=1)).events_deleted == 0

    delivering_store = SQLiteStore(tmp_path / "delivering.db", clock=lambda: NOW)
    make_event(delivering_store)
    assert len(delivering_store.claim_due(now=NOW)) == 1
    assert delivering_store.outbox_rows()[0]["status"] == "DELIVERING"
    assert delivering_store.purge_before(NOW + timedelta(minutes=1)).events_deleted == 0


def test_purge_removes_dead_history(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    make_event(store)
    OutboxDispatcher(
        store,
        LeakingSink("permanent failure"),
        config=DeliveryConfig(retry=RetryPolicy(max_attempts=1)),
        clock=lambda: NOW,
    ).dispatch_ready()
    assert store.outbox_rows()[0]["status"] == "DEAD"

    result = store.purge_before(NOW + timedelta(minutes=1))
    assert result.events_deleted == 1
    assert result.outbox_rows_deleted == 1
    assert result.delivery_attempts_deleted == 1


def test_purge_can_be_scoped_to_one_watch(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    make_event(store, watch_id="first")
    make_event(store, watch_id="second")
    OutboxDispatcher(store, AcceptingSink(), clock=lambda: NOW).dispatch_ready()

    result = store.purge_before(NOW + timedelta(minutes=1), watch_id="first")

    assert result.events_deleted == 1
    assert store.list_events("first") == []
    assert len(store.list_events("second")) == 1
    assert store.get_authoritative_observation("first") is not None
    assert store.get_authoritative_observation("second") is not None


def test_purge_handles_more_rows_than_legacy_sqlite_parameter_limits(tmp_path: Path) -> None:
    count = 1_200
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    events = store.record_observation(
        "bulk",
        Observation.valid("state", observed_at=NOW),
        BulkEventPolicy(count),
        now=NOW,
    )
    assert len(events) == count
    with store._connect() as connection:
        connection.execute("UPDATE outbox SET status = 'DELIVERED'")

    result = store.purge_before(NOW + timedelta(seconds=1))

    assert result.events_deleted == count
    assert result.outbox_rows_deleted == count
    assert store.list_events("bulk") == []


def test_purge_rejects_naive_cutoff(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db")
    with pytest.raises(ValueError, match="timezone-aware"):
        store.purge_before(datetime(2025, 1, 2))


def test_delete_watch_requires_explicit_undelivered_override(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    make_event(store)
    with pytest.raises(RuntimeError, match="undelivered"):
        store.delete_watch("lifecycle")
    assert len(store.list_observations("lifecycle")) == 2
    assert len(store.list_events("lifecycle")) == 1

    result = store.delete_watch("lifecycle", allow_undelivered=True)
    assert result.watches_deleted == 1
    assert result.events_deleted == 1
    assert result.outbox_rows_deleted == 1
    assert result.observations_deleted == 2
    assert store.list_observations("lifecycle") == []


@pytest.mark.parametrize("invalid_override", ["false", 1, None])
def test_delete_watch_rejects_truthy_non_boolean_override(
    tmp_path: Path, invalid_override: object
) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    make_event(store)

    with pytest.raises(TypeError, match="allow_undelivered must be a bool"):
        store.delete_watch(
            "lifecycle", allow_undelivered=invalid_override  # type: ignore[arg-type]
        )

    assert len(store.list_events("lifecycle")) == 1
    assert store.outbox_rows()[0]["status"] == "PENDING"


def test_compact_storage_keeps_database_readable(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    make_event(store)
    store.compact_storage()
    assert store.get_authoritative_observation("lifecycle") is not None


def test_compact_storage_fails_instead_of_hiding_busy_checkpoint(
    tmp_path: Path,
) -> None:
    database = tmp_path / "watch.db"
    store = FastBusyTimeoutStore(database, clock=lambda: NOW)
    make_event(store)
    reader = sqlite3.connect(database, isolation_level=None)
    try:
        reader.execute("PRAGMA journal_mode = WAL")
        reader.execute("BEGIN")
        reader.execute("SELECT * FROM events").fetchall()
        with store._connect() as writer:
            writer.execute("UPDATE watches SET updated_at = updated_at")

        with pytest.raises(RuntimeError, match="checkpoint is busy"):
            store.compact_storage()
    finally:
        reader.close()

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


def test_custom_exception_class_name_never_reaches_logs_or_database(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    class_name = "customer_private_product_error"
    AdapterSpecificSecretError.__name__ = class_name
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    definition = WatchDefinition(
        watch_id="redaction",
        trigger=ManualTrigger(),
        observer=SequenceObserver([AdapterSpecificSecretError("failure")] * 3),
        transition_policy=StateChangePolicy(),
    )

    with caplog.at_level(logging.WARNING):
        result = WatchRuntime(store, clock=lambda: NOW, sleep=lambda _: None).run_once(definition)

    assert result.observation.error == "RuntimeError: operation failed"
    assert result.observation.evidence["exception_type"] == "RuntimeError"
    assert class_name not in caplog.text
    assert all(class_name.encode() not in path.read_bytes() for path in tmp_path.glob("watch.db*"))


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


def test_policy_exception_message_never_reaches_logs_or_database(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    secret = "private-policy-token-should-not-persist"
    store = SQLiteStore(tmp_path / "watch.db", clock=lambda: NOW)
    definition = WatchDefinition(
        watch_id="policy-redaction",
        trigger=ManualTrigger(),
        observer=SequenceObserver(
            [
                Observation.valid("A", observed_at=NOW),
                Observation.valid("B", observed_at=NOW + timedelta(seconds=1)),
            ]
        ),
        transition_policy=LeakingPolicy(secret),
    )
    runtime = WatchRuntime(store, clock=lambda: NOW)
    runtime.run_once(definition)
    with caplog.at_level(logging.ERROR), pytest.raises(RuntimeError, match=secret):
        runtime.run_once(definition)

    assert secret not in caplog.text
    assert all(secret.encode() not in path.read_bytes() for path in tmp_path.glob("watch.db*"))


def test_library_logs_omit_caller_controlled_identifiers(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    watch_identifier = "caller-controlled-watch-identifier"
    event_identifier = "caller-controlled-event-identifier"
    observer_store = SQLiteStore(tmp_path / "observer.db", clock=lambda: NOW)
    definition = WatchDefinition(
        watch_id=watch_identifier,
        trigger=ManualTrigger(),
        observer=SequenceObserver([RuntimeError("failure")] * 3),
        transition_policy=StateChangePolicy(),
    )
    with caplog.at_level(logging.WARNING):
        WatchRuntime(observer_store, clock=lambda: NOW, sleep=lambda _: None).run_once(definition)

    event_store = SQLiteStore(
        tmp_path / "event.db",
        clock=lambda: NOW,
        event_id_factory=lambda: event_identifier,
    )
    make_event(event_store)
    OutboxDispatcher(
        event_store, LeakingSink("failure"), clock=lambda: NOW
    ).dispatch_ready()

    assert watch_identifier not in caplog.text
    assert event_identifier not in caplog.text


def test_json_fields_have_a_one_mebibyte_encoded_limit() -> None:
    with pytest.raises(ValueError, match="exceeds 1048576"):
        Observation.valid({"value": "x" * 1_048_576}, observed_at=NOW)


def test_caller_supplied_error_is_bounded() -> None:
    observation = Observation.failed(observed_at=NOW, error="x" * 10_000)
    assert observation.error is not None
    assert len(observation.error) == 2_048
    assert observation.error.endswith("...<truncated>")
