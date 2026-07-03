"""DuckCore — the transport-free heart of a Duck.

Wires the push-only :class:`~duckstring.engine.worker.WorkerState` to the run ledger and an outgoing
event buffer. It is driven by two inputs — ``begin_run(F)`` from the Catchment and ``ripple_completed``
from the executor — and produces two outputs — the list of Ripple **names to launch** and buffered
**events** to report. Threads, the executor, and HTTP live in :mod:`.executor` / :mod:`.client` /
``__main__`` so this stays deterministically testable.

Resilience: every state change is persisted to the ledger before it is reported, and events are
buffered until the Catchment acknowledges them — so a Pond Run completes (and is recoverable) with no
Catchment involvement.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from ..engine import NEVER, worker
from ..engine import pond as ledger
from ..schema_contract import ContractViolation


@dataclass
class Event:
    """A buffered report to the Catchment. ``kind`` is ``"ripple"`` or ``"run_completed"``; idempotent
    on ``(kind, ripple, f)`` so replay after a reconnect is safe. ``started_at``/``finished_at`` are
    the Ripple's wall-clock execution span (telemetry for the run-history duration)."""

    kind: str
    f: datetime
    ripple: str | None = None
    status: str = "success"
    retry: int = 0  # attempt index for a ripple/failed event (0 = first try); the retry trace
    error: str | None = None  # failure message (for a failed ripple/run), surfaced in the UI + DB
    traceback: str | None = None  # full traceback for the failure, if any
    started_at: datetime | None = None
    finished_at: datetime | None = None
    schema: dict | None = None  # published output schema (run_completed) — captured as the version contract
    changed: bool = True  # did this Pond Run change its output? False = a pass (pond.skip() / empty delta).
                          # Set on the Run-completing ripple event + run_completed; holds changed_f at the
                          # Catchment so downstream skips. See plans/no-change-skip.md.
    source: str | None = None  # missing_source event: the Source + table the Ripple couldn't read (it is
    table: str | None = None   # not published) — the Catchment parks the Pond blocked-with-a-reason.

    def payload(self) -> dict:
        d = {"kind": self.kind, "f": self.f.isoformat(), "status": self.status, "retry": self.retry,
             "changed": self.changed}
        if self.ripple is not None:
            d["ripple"] = self.ripple
        if self.error is not None:
            d["error"] = self.error
        if self.traceback is not None:
            d["traceback"] = self.traceback
        if self.started_at is not None:
            d["started_at"] = self.started_at.isoformat()
        if self.finished_at is not None:
            d["finished_at"] = self.finished_at.isoformat()
        if self.schema is not None:
            d["schema"] = self.schema
        if self.source is not None:
            d["source"] = self.source
        if self.table is not None:
            d["table"] = self.table
        return d


class DuckCore:
    def __init__(self, pond_name: str, con, parents: dict[str, list[str]], optional=None):
        self.pond_name = pond_name
        self.con = con
        self.state = ledger.load_state(con, parents, optional)
        self.events: list[Event] = []  # buffered, awaiting delivery to the Catchment
        self.attempts: dict[str, int] = {}  # ripple name → attempt index of its current in-flight run
        self.last_begin_f: datetime = NEVER  # freshness of the most recently started Pond Run
        self._previous_f: dict[datetime, datetime] = {}  # Pond Run freshness → the prior run's freshness
        self.contract: dict | None = None  # the major line's additive schema contract (gated at publish)
        # No-change tracking (plans/no-change-skip.md), keyed by Pond Run freshness so concurrent Runs
        # don't cross-contaminate. ``_skipped`` holds Runs a Ripple marked no-change via ``pond.skip()``;
        # ``_sources_changed`` is the engine's verdict (exposed to Ripples as ``pond.sources_changed()``).
        self._skipped: set[datetime] = set()
        self._sources_changed: dict[datetime, bool] = {}

    def mark_skipped(self, f: datetime) -> None:
        """A Ripple called ``pond.skip()`` for the Run at ``f`` — report it as a pass (changed=False)."""
        self._skipped.add(f)

    def sources_changed_for(self, f: datetime) -> bool:
        """The engine's "did any Source change" verdict for the Run at ``f`` (default True if unknown,
        e.g. a Run recovered from the ledger with no live begin_run)."""
        return self._sources_changed.get(f, True)

    def begin_run(
        self, f: datetime, now: datetime, retry_immediately: int = 0, force: bool = False,
        previous_f: datetime = NEVER, contract: dict | None = None, sources_changed: bool = True,
    ) -> list[str]:
        """Start a Pond Run at freshness ``f`` (idempotent — completed Ripples are not re-stamped, unless
        ``force``, which recomputes every Ripple). ``retry_immediately`` is the Run's Ripple-retry budget
        (the Catchment's live setting); ``previous_f`` is the prior completed run's freshness (the
        Catchment computes it at dispatch), carried through to the Ripples as ``pond.previous_f``;
        ``contract`` is the major line's additive schema contract, vetted at publish.
        Returns the Ripple names the caller must launch."""
        self.contract = contract
        self.state = worker.begin_run(self.state, f, retry_immediately, force=force)
        self.last_begin_f = max(self.last_begin_f, f)
        self._previous_f[f] = previous_f
        self._sources_changed[f] = sources_changed
        ledger.record_pond_run_start(self.con, f, now)
        return self._advance(now)

    def previous_f_for(self, f: datetime) -> datetime:
        """The prior run's freshness for the in-flight Pond Run at ``f`` (``NEVER`` if unknown — e.g.
        a Run recovered from the ledger after a Duck restart, with no live ``begin_run`` to carry it)."""
        return self._previous_f.get(f, NEVER)

    def pond_failed(self, error: str | None = None, traceback: str | None = None) -> None:
        """Buffer a Pond-level failure (a Duck error not tied to a Ripple — e.g. a failed ledger
        write). Attributed to the most recently started Pond Run; the Catchment fails the whole Pond."""
        self.events.append(Event(
            kind="pond_failed", f=self.last_begin_f, status="failed", error=error, traceback=traceback,
        ))

    def ripple_completed(
        self, name: str, now: datetime, started_at=None, finished_at=None, export=None
    ) -> list[str]:
        """Record a finished Ripple, buffer a report, and (if a Pond Run just completed) export +
        buffer the run completion. ``started_at``/``finished_at`` are the Ripple's wall-clock span
        (run-history duration telemetry). Returns any newly launchable Ripple names."""
        end_f = self.state.states[name].start_f
        ledger.record_ripple_complete(self.con, name, end_f)
        self.state, rc = worker.complete_ripple(self.state, name, now)
        ripple_event = Event(kind="ripple", ripple=name, f=end_f, retry=self.attempts.pop(name, 0),
                             started_at=started_at, finished_at=finished_at)
        self.events.append(ripple_event)
        if rc is not None:
            # The Run completed at this Ripple — decide whether its output changed and stamp it on this
            # (Run-completing) ripple event so the Catchment holds changed_f *before* it triggers
            # downstream, plus on run_completed for the pond_run flag (see plans/no-change-skip.md).
            changed = rc.f not in self._skipped
            self._skipped.discard(rc.f)
            self._sources_changed.pop(rc.f, None)
            ripple_event.changed = changed
            schema = None
            if export is not None:
                try:
                    # Publish stamped with the run freshness. The contract is vetted here, before the
                    # live tables are overwritten — a violation aborts the publish (last-good intact).
                    schema = export(rc.f, self.contract)
                except ContractViolation as exc:
                    self._previous_f.pop(rc.f, None)
                    ledger.record_pond_run_finish(self.con, rc.f, now, status="failed")  # don't re-run on restart
                    self.events.append(Event(kind="contract_failed", f=rc.f, status="failed", error=str(exc)))
                    return self._advance(now)
            ledger.record_pond_run_finish(self.con, rc.f, now)
            self._previous_f.pop(rc.f, None)  # the Run is done; drop its carried previous_f
            self.events.append(Event(kind="run_completed", f=rc.f, schema=schema, changed=changed))
        return self._advance(now)

    def ripple_failed(
        self, name: str, now: datetime, started_at=None, finished_at=None,
        error: str | None = None, traceback: str | None = None,
    ) -> list[str]:
        """A Ripple errored. Spend one of the Pond Run's immediate retries (relaunching the Ripple) if
        any remain; otherwise the Run gives up — record it and buffer a ``failed`` event for the
        Catchment. ``error``/``traceback`` are the failure message + stack (surfaced in the UI).
        Returns any Ripple names to (re)launch."""
        self.state, rf = worker.fail_ripple(self.state, name, now)
        ledger.record_ripple_failed(self.con, name)
        attempt = self.attempts.get(name, 0)
        if rf is None:  # retried within budget — log the failed attempt, the Run continues
            self.events.append(Event(
                kind="ripple", ripple=name, f=self.state.states[name].start_f, status="failed",
                retry=attempt, error=error, traceback=traceback, started_at=started_at, finished_at=finished_at,
            ))
            self.attempts[name] = attempt + 1  # next launch of this Ripple is the next attempt
        else:  # immediate budget exhausted — this Pond Run failed at the Ripple's freshness
            ledger.record_pond_run_finish(self.con, rf.f, now, status="failed")
            self.events.append(Event(
                kind="failed", ripple=name, f=rf.f, status="failed",
                retry=attempt, error=error, traceback=traceback, started_at=started_at, finished_at=finished_at,
            ))
            self.attempts.pop(name, None)  # the Run is done; reset for any future run
        return self._advance(now)

    def ripple_missing_source(
        self, name: str, source: str, table: str, now: datetime, started_at=None, finished_at=None,
    ) -> list[str]:
        """A Ripple read a Source asset that isn't published (:class:`~duckstring.core.MissingSourceAsset`).
        Give up the Run at this Ripple **without** spending immediate retries (the Source won't republish
        mid-Run) and buffer a ``missing_source`` event — the Catchment parks the Pond blocked-with-a-reason,
        not failed. See plans/reset.md."""
        self.state, rf = worker.fail_ripple(self.state, name, now, retry=False)
        ledger.record_ripple_failed(self.con, name)
        f = rf.f if rf is not None else self.state.states[name].start_f
        ledger.record_pond_run_finish(self.con, f, now, status="failed")  # don't re-run on restart
        self.events.append(Event(
            kind="missing_source", ripple=name, f=f, status="failed", source=source, table=table,
            started_at=started_at, finished_at=finished_at,
        ))
        self.attempts.pop(name, None)
        return self._advance(now)

    def _advance(self, now: datetime) -> list[str]:
        self.state, launched = worker.sentinel(now, self.state)
        for name in launched:
            ledger.record_ripple_start(self.con, name, self.state.states[name].start_f)
        return launched

    def idle(self) -> bool:
        """True when no Ripple is running and no target is outstanding — the Pond has quiesced and the
        Duck may shut down (once its event buffer has drained)."""
        return all(not s.is_running and not s.targets for s in self.state.states.values())

    def flush(self, send) -> None:
        """Try to deliver buffered events in order via ``send(payload) -> bool``, **stopping at the
        first failure** so causal order is preserved (ripples before their run completion). A failure
        usually means the Catchment is unreachable, so the rest would fail too. Safe to call
        repeatedly; events are idempotent on the Catchment side."""
        while self.events:
            if not send(self.events[0].payload()):
                return
            self.events.pop(0)
