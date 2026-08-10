# Repository guidance

## Purpose and boundaries

`watch-engine` is a reusable, domain-neutral Condition Watch Runtime. Never add logic tied to a
particular product, retailer, notification provider, agent, or business state. Domain code enters
only through the public Observer, TransitionPolicy, Trigger, and EventSink protocols.

The v0.1 boundary is Python 3.11+, SQLite, in-process scheduling adapters, synchronous observers
and sinks, and an async trigger adapter. Do not casually add Redis, PostgreSQL, brokers,
distributed locks, service discovery, Kubernetes, plugin loaders, or a web UI.

## Invariants

- Observation and authority are separate. Persist every observation, but only `VALID` may replace
  authoritative state.
- Never silently treat `DEGRADED` or `FAILED` as a domain transition. The next valid state compares
  with the last authoritative valid state.
- A valid state update, each new `WatchEvent`, and its outbox row share one SQLite transaction.
- Delivery is at least once. Preserve stable `event_id` and `dedupe_key`; sinks are responsible
  for idempotent downstream processing.
- Delivery failure must not roll back or modify authoritative state.
- Keep retry attempt counts and delay caps explicit, injectable, and tested.
- Use timezone-aware UTC datetimes and deterministic JSON serialization.

## Public contracts

`schemas/watch-event-v1.json` is the integration contract. Keep compatible changes backward
compatible. Any destructive schema change requires a new schema version and a new schema file;
never silently rewrite v1 semantics. Internal Python models are not the cross-project contract.

Database evolution starts at `SQLiteStore.SCHEMA_VERSION`. v0.1 has no migration framework, but
schema changes must detect unsupported versions rather than reinterpret existing data.

## Required checks

Run all commands before proposing a change:

```bash
python -m pytest
python -m ruff check .
python -m mypy src
```

Tests must not use external networks or real third-party services. Add focused tests for any
change touching authority, transaction boundaries, dedupe, retries, recovery, or event contracts.
