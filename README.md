# watch-engine

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
- `TransitionPolicy` alone understands domain state and returns `EventDraft` values.
- `WatchEvent` is the engine-owned, durable v1 event envelope.
- `EventSink` delivers events and must treat `event_id` or `dedupe_key` as an idempotency key.

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


class JsonLineSink:
    def __init__(self):
        self.seen = set()

    def deliver(self, event):
        if event.event_id in self.seen:
            return
        self.seen.add(event.event_id)
        print(event.to_dict())


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
delivery_results = OutboxDispatcher(store, JsonLineSink()).dispatch_ready()
```

For scheduled execution, `await WatchRunner(runtime).run_next(definition)` waits for one trigger
and performs one run. `ManualTrigger.fire()` explicitly releases one waiting run. `CronTrigger`
uses ordinary cron expressions and requires an explicit timezone.

## Persistence and delivery

`SQLiteStore` initializes schema version 1 automatically. It keeps watch run metadata, every
observation, the authoritative observation, events, outbox rows, and every delivery attempt.

For a valid observation, policy evaluation and these writes share one `BEGIN IMMEDIATE`
transaction:

1. persist the observation;
2. insert any new event (unique by `watch_id + dedupe_key`);
3. insert its pending outbox row;
4. replace the authoritative pointer and state.

Any failure rolls all four operations back. Sink failures happen later and therefore cannot
roll back or corrupt authority. Delivery is intentionally **at least once**. A process can die
after a sink accepts an event but before SQLite records success; the next process will send the
same stored `event_id` again. Sinks must deduplicate it. Failed attempts use configurable,
bounded exponential backoff and survive restart. Exhausted events remain as `DEAD` diagnostics.

Observer exceptions are retried within a run using the watch's bounded retry policy. When the
attempt budget is exhausted, the runtime persists one `FAILED` observation with the exception
type and attempt count. An observer that deliberately returns `DEGRADED` or `FAILED` has already
classified its evidence, so that result is persisted immediately and is not retried implicitly.

All timestamps are timezone-aware and normalized to UTC. JSON is stored with deterministic key
ordering. The cross-project contract is
[`schemas/watch-event-v1.json`](schemas/watch-event-v1.json); consumers should use that contract,
not import internal database models.

## Development

```bash
python -m pip install -e ".[dev]"
python -m pytest
python -m ruff check .
python -m mypy src
```

The test suite is entirely local and requires no network or third-party service.

## v0.1 boundaries

The runtime is intentionally single-node and synchronous at the observer/sink boundary. SQLite
coordinates local transactions; it is not a distributed lock. The trigger adapter is async, but
v0.1 does not include a daemon CLI, process supervisor, distributed scheduler, dynamic plugin
loader, PostgreSQL, Redis, or a message broker.

Implemented triggers are jittered intervals, cron schedules, and in-process manual requests.
Potential later adapters include external events, file changes, and webhooks without changing the
observation/authority/event pipeline.

## License

Apache License 2.0. See [`LICENSE`](LICENSE).
