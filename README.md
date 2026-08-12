# watch-engine

English | [简体中文](README.zh-CN.md)

`watch-engine` is a reusable Python 3.11+ condition-watch runtime. It schedules observations,
preserves trustworthy state, asks domain code to interpret transitions, persists resulting
events, and delivers them through a transactional outbox.

It solves the reliability mechanics shared by polling and event-driven monitors. It does not
scrape websites, understand domain states such as `AVAILABLE`, send messages to a particular
provider, or provide a distributed scheduler or web administration UI.

## Core concepts

- `WatchDefinition` composes a `Trigger`, `Observer`, `TransitionPolicy`, and observer retry
  policy under a stable `watch_id`.
- `Observation` is one piece of evidence with status `VALID`, `DEGRADED`, or `FAILED`.
- The authoritative state is the newest `VALID` observation. A degraded or failed observation
  is retained for diagnosis but never replaces it.
- `TransitionPolicy` alone understands domain state and returns `EventDraft` values. It is pure,
  fast domain decision logic: no network, external-service access, blocking I/O, notifications,
  database-external mutation, or other irreversible side effects.
- `WatchEvent` is the engine-owned, durable v1 event envelope.
- `EventSink` delivers events and must treat `event_id` as the retry idempotency key.

This distinction is fundamental: an observation failure says the engine could not confidently
observe the subject. It does not say the subject changed state. After any number of non-valid
observations, the next valid observation is compared with the last authoritative valid one.

## Minimal watch

```python
from datetime import datetime, timezone

from watch_engine import (
    EventDraft,
    IntervalTrigger,
    Observation,
    OutboxDispatcher,
    SQLiteStore,
    WatchDefinition,
    WatchRuntime,
)


class HealthObserver:
    def observe(self) -> Observation:
        # Domain I/O and parsing live here, outside watch-engine.
        return Observation.valid(
            {"healthy": True},
            observed_at=datetime.now(timezone.utc),
            evidence={"source": "local-example"},
        )


class HealthTransitions:
    def evaluate(self, previous, current):
        if previous is None or previous.state == current.state:
            return []
        return [
            EventDraft(
                event_type="health.changed",
                severity="warning",
                dedupe_key=f"health:{previous.state!r}:{current.state!r}",
                subject={"service": "example"},
                payload={"previous": previous.state, "current": current.state},
            )
        ]


class SummarySink:
    def __init__(self):
        self.seen = set()

    def deliver(self, event):
        if event.event_id in self.seen:
            return
        self.seen.add(event.event_id)
        # Deliberately avoid logging subject/payload: callers may put sensitive data there.
        print({"event_id": event.event_id, "event_type": event.event_type})


store = SQLiteStore("watch-engine.db")
definition = WatchDefinition(
    watch_id="example-health",
    trigger=IntervalTrigger(90, 150),
    observer=HealthObserver(),
    transition_policy=HealthTransitions(),
)

# One observation, independent of scheduling:
result = WatchRuntime(store).run_once(definition)

# Deliver all currently due events. Run this repeatedly in a worker/process loop.
delivery_results = OutboxDispatcher(store, SummarySink()).dispatch_ready()
```

For scheduled execution, `await WatchRunner(runtime).run_next(definition)` waits for one trigger
and performs one run. `ManualTrigger.fire()` explicitly releases one waiting run. `CronTrigger`
uses ordinary cron expressions and requires an explicit timezone.

## Persistence and delivery

`SQLiteStore` requires a file-backed database and initializes schema version 1 automatically. It
keeps watch run metadata, every observation, the authoritative observation, events, outbox rows,
and every delivery attempt.
On POSIX systems the database, WAL, and SHM files are forced to owner-only mode (`0600`), and
symbolic-link database paths are rejected. Deployments should also use an owner-only (`0700`)
parent directory.

Persistence deliberately has two transaction boundaries:

1. Every completed Observation is first committed as evidence, together with diagnostic watch
   run metadata.
2. For a non-stale VALID Observation, a separate `BEGIN IMMEDIATE` transaction evaluates the
   policy, replaces Authority, and inserts each WatchEvent with its Outbox row.

If policy evaluation or promotion fails, the second transaction rolls back Authority, Event, and
Outbox together while the already committed Observation remains available for diagnosis. Sink
failures happen later and therefore cannot roll back or corrupt authority.

`TransitionPolicy.evaluate(previous, current)` intentionally runs inside that SQLite write
transaction so Previous Authority, the policy decision, Event/Outbox creation, and New Authority
form one atomic promotion decision. A Policy must therefore be deterministic where practical and
return quickly. It must not perform network requests, call external services, send notifications,
block on I/O, modify state outside SQLite, or produce non-rollbackable side effects. Those effects
belong in `EventSink`, after the Outbox transaction commits.

Every event occurrence gets a new `event_id`, including a later repetition of the same transition.
`dedupe_key` is domain correlation context and is intentionally not unique in the events table.
Delivery retry reuses the stored event and stable `event_id`; it never creates a second event row.
Delivery is intentionally **at least once**. A process can die
after a sink accepts an event but before SQLite records success; the next process will send the
same stored `event_id` again. Sinks must deduplicate by `event_id`. Failed attempts use configurable,
bounded exponential backoff and survive restart. Exhausted events remain as `DEAD` diagnostics.
The v0.1 SQLite backend supports one owning local process and one active dispatcher. Its completed
attempt count is checked during acknowledgement but is not a lease or unique claim token; old and
replacement dispatchers must never overlap.

Observer exceptions are retried within a run using the watch's bounded retry policy. When the
attempt budget is exhausted, the runtime persists one `FAILED` observation with a fixed built-in
exception category and the attempt count; exception messages and downstream-defined exception
class names are deliberately discarded. An observer that
deliberately returns `DEGRADED` or `FAILED` has already
classified its evidence, so that result is persisted immediately and is not retried implicitly.

All timestamps are timezone-aware and normalized to UTC. JSON is stored with deterministic key
ordering. The cross-project contract is
[`schemas/watch-event-v1.json`](schemas/watch-event-v1.json); consumers should use that contract,
not import internal database models.

Each encoded JSON field is limited to 1 MiB. Retention is caller-controlled:
`purge_before(cutoff)` deletes only old terminal (`DELIVERED`/`DEAD`) event history and
non-authoritative observations, `delete_watch()` refuses undelivered work unless explicitly
overridden with the actual boolean `True`, and `compact_storage()` checkpoints and vacuums after
other owners stop. Stop the
runner and dispatcher before a destructive watch deletion or compaction.

Before production use, read the [security policy](SECURITY.md) and
[data-governance policy](DATA-GOVERNANCE.md). Engine fields must not contain credentials,
personal information, or other sensitive data.
Library-generated logs omit caller-controlled watch/event identifiers and payload fields.

## Development

```bash
python -m pip install -e ".[dev]"
python -m pytest
python -m ruff check .
python -m mypy src
python -m pip_audit . --progress-spinner=off
```

The test suite is entirely local and requires no network or third-party service.

## v0.1 boundaries

The runtime is intentionally single-node and synchronous at the observer/sink boundary. SQLite
coordinates local transactions; it is not a distributed lock.

The current deployment baseline is one local monitor process per SQLite database. That process
owns both runners and the dispatcher; add targets serially in the same process instead of adding
processes without a demonstrated scaling requirement.

Runs for one `watch_id` may overlap at the Observer stage. Authority promotion is serialized by
SQLite and ordered by `Observation.observed_at`. A VALID observation whose timestamp is older
than or equal to current Authority is retained as evidence but is not passed to TransitionPolicy,
does not replace Authority, and cannot create an event. Observers must therefore assign an aware
timestamp that represents when their evidence was obtained. If callers invoke the same Observer
concurrently, that Observer is responsible for its own thread safety. Equal timestamps use a
conservative first-wins rule: the already promoted Authority remains authoritative and the later
completion is evidence-only. Timestamps must be timezone-aware and precise enough to order the
Observer's real evidence.

The trigger adapter is async, but v0.1 does not include a daemon CLI, process supervisor,
distributed scheduler, dynamic plugin loader, PostgreSQL, Redis, or a message broker.

Implemented triggers are jittered intervals, cron schedules, and in-process manual requests.
Potential later adapters include external events, file changes, and webhooks without changing the
observation/authority/event pipeline.

## License

Apache License 2.0. See [`LICENSE`](LICENSE).
