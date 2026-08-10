from __future__ import annotations

from collections.abc import Sequence

from watch_engine import EventDraft, Observation


class SequenceObserver:
    def __init__(self, values: Sequence[Observation | Exception]) -> None:
        self._values = iter(values)

    def observe(self) -> Observation:
        value = next(self._values)
        if isinstance(value, Exception):
            raise value
        return value


class StateChangePolicy:
    def evaluate(
        self, previous: Observation | None, current: Observation
    ) -> list[EventDraft]:
        if previous is None or previous.state == current.state:
            return []
        return [
            EventDraft(
                event_type="state.changed",
                severity="info",
                dedupe_key=f"{previous.state!r}->{current.state!r}",
                subject={"kind": "test-resource"},
                payload={"previous": previous.state, "current": current.state},
            )
        ]


class NoEventsPolicy:
    def evaluate(
        self, previous: Observation | None, current: Observation
    ) -> list[EventDraft]:
        return []
