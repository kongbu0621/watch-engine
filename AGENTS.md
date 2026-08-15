# Repository guidance

English | [简体中文](AGENTS.zh-CN.md)

## Purpose and boundaries

`watch-engine` is a reusable, domain-neutral Condition Watch Runtime. Never add logic tied to a
particular product, retailer, notification provider, agent, or business state. Domain code enters
only through the public Observer, TransitionPolicy, Trigger, and EventSink protocols.

The current 0.x boundary is Python 3.11+, SQLite, in-process scheduling adapters, synchronous observers
and sinks, and an async trigger adapter. Do not casually add Redis, PostgreSQL, brokers,
distributed locks, service discovery, Kubernetes, plugin loaders, or a web UI.

## Invariants

- Observation and authority are separate. Persist every observation, but only `VALID` may replace
  authoritative state.
- Never silently treat `DEGRADED` or `FAILED` as a domain transition. The next valid state compares
  with the last authoritative valid state.
- Commit Observation evidence before running TransitionPolicy or promotion logic. A later failure
  may not erase an Observation that was already obtained.
- A valid state update, each new `WatchEvent`, and its outbox row share one SQLite transaction.
- `TransitionPolicy.evaluate()` runs inside that SQLite write transaction. Keep it fast, pure, and
  deterministic where practical. It must not perform network or external-service calls, send
  notifications, block on I/O, mutate database-external state, or cause irreversible side effects.
  External effects belong behind the Outbox in `EventSink`.
- Runs may overlap during observation. Serialize promotion in SQLite and treat a VALID observation
  with `observed_at <= authority.observed_at` as evidence-only: never evaluate it, promote it, or
  emit events from it. Equal timestamps are conservative first-wins. Observers own the correctness,
  timezone awareness, and sufficient precision of `observed_at`.
- Every real transition occurrence gets a new `event_id`. Never make `(watch_id, dedupe_key)`
  permanently unique; state cycles can legitimately repeat a semantic transition.
- Delivery is at least once. Retry the same stored event with its stable `event_id`; sinks are
  responsible for idempotent downstream processing by `event_id`. `dedupe_key` is domain context,
  not lifetime event identity.
- Delivery failure must not roll back or modify authoritative state.
- The current 0.x line permits only one active OutboxDispatcher owner per SQLite database. A new dispatcher
  recovers every `DELIVERING` row and therefore must start only after the old owner has exited.
- `WatchRunner.serve()` stop is cooperative between runs; it does not wake a pending trigger or
  terminate synchronous work already handed to `asyncio.to_thread`.
- Keep retry attempt counts and delay caps explicit, injectable, and tested.
- Use timezone-aware UTC datetimes and deterministic JSON serialization.

## Public contracts

`schemas/watch-event-v1.json` is the integration contract. Keep compatible changes backward
compatible. Any destructive schema change requires a new schema version and a new schema file;
never silently rewrite v1 semantics. Internal Python models are not the cross-project contract.

Database evolution starts at `SQLiteStore.SCHEMA_VERSION`. The current 0.x line has no migration framework, but
schema changes must detect unsupported versions rather than reinterpret existing data.

## Documentation baseline

This is a program repository. Keep all three mandatory document layers current:

1. product/module requirements: why, users, required behavior, boundaries, and acceptance;
2. technical architecture: components, dependencies, data flow, invariants, and design rationale;
3. concrete implementation: file/class/schema/config/test/CI/deployment/release mapping and status.

Because `watch-engine` is independently reusable, also maintain a downstream adoption guide.
Code, schema, packaging, runtime, deployment, release, or public-contract changes must update the
affected document layer in the same change. Never describe a future plan as already implemented or
an already published release as pending.

Every repository-owned, human-facing English Markdown document, in any source directory, must
have a sibling `.zh-CN.md` version and link to it. Generated build output, tool caches, and
third-party metadata are not repository documentation. A `.zh-CN.md` file must contain substantive
Chinese text; an English copy with a Chinese filename is not a Chinese version. The Chinese version
must preserve public API names, protocol names, state values, commands, file paths, and important
English engineering terms so readers can map the explanation back to code and external references.
Update both language versions in the same change.

## Required checks

Run all commands before proposing a change:

```bash
python -m pytest
python -m ruff check .
python -m mypy src
python -m build
python -m twine check dist/*
python scripts/verify_sdist_bilingual.py dist/*.tar.gz
python -m pip_audit --local --progress-spinner=off
```

Tests must not use external networks or real third-party services. Add focused tests for any
change touching authority, transaction boundaries, dedupe, retries, recovery, or event contracts.
