"""Shared primitives for the orchestration engines.

The freshness/push-token model is split across two deployable engines: the
:mod:`~duckstring.engine.catchment` brain (full ponds + ripples, pull + push) that the Catchment
runs, and the push-only :mod:`~duckstring.engine.worker` that each Duck runs. Both build on the
dataclasses and helpers here so the rules are single-sourced.

Freshness ``F`` is a timezone-aware UTC ``datetime``; the window delay ``D`` and Tide bounds are
``timedelta``. The "never run" freshness is :data:`NEVER` (``datetime.min``), which orders below every
real timestamp.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone

PondId = str
RippleId = str

# Freshness sentinel for "never run". Orders below every real UTC timestamp.
NEVER = datetime.min.replace(tzinfo=timezone.utc)
ZERO = timedelta(0)


# ─── Topology (immutable inputs) ──────────────────────────────────────────────


_DAYS = ("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN")
_UNIT_SECONDS = {"SECOND": 1, "MINUTE": 60, "HOUR": 3600, "DAY": 86400, "WEEK": 604800}


@dataclass(frozen=True)
class Window:
    """A recurring batch-availability window on an Inlet (RFC-5545-flavoured): the first window opens
    at ``start_anchor`` and stays open ("fresh until" the end) for ``duration``; it then recurs every
    ``freq_interval`` × ``freq_unit``. ``valid_days`` (a set of MON..SUN, or None) restricts which
    weekdays an occurrence is kept; ``until`` ends the recurrence. Occurrences are the grid
    ``start_anchor + k·delta`` (k ≥ 0), filtered by ``valid_days``/``until``."""

    start_anchor: datetime
    duration: timedelta
    freq_unit: str  # SECOND | MINUTE | HOUR | DAY | WEEK
    freq_interval: int = 1
    valid_days: frozenset[str] | None = None
    until: datetime | None = None

    def _delta(self) -> timedelta:
        return timedelta(seconds=_UNIT_SECONDS[self.freq_unit] * self.freq_interval)

    def _kept(self, t: datetime) -> bool:
        if self.until is not None and t > self.until:
            return False
        return self.valid_days is None or _DAYS[t.weekday()] in self.valid_days

    def active_end(self, now: datetime) -> datetime | None:
        """If ``now`` falls inside an occurrence, return that window's end ("fresh until"); else None.
        O(1): the most-recent grid point at/before ``now`` (windows are assumed non-overlapping)."""
        delta = self._delta()
        if now < self.start_anchor or delta.total_seconds() <= 0:
            return None
        c = self.start_anchor + ((now - self.start_anchor) // delta) * delta
        if not self._kept(c):
            return None
        end = c + self.duration
        return end if now < end else None

    def next_open(self, now: datetime, cap: int = 10000) -> datetime | None:
        """The next occurrence start strictly after ``now`` (bounded scan for ``valid_days``)."""
        delta = self._delta()
        if delta.total_seconds() <= 0:
            return None
        k = 0 if now < self.start_anchor else (now - self.start_anchor) // delta + 1
        for i in range(cap):
            t = self.start_anchor + (k + i) * delta
            if self.until is not None and t > self.until:
                return None
            if self.valid_days is None or _DAYS[t.weekday()] in self.valid_days:
                return t
        return None

    def next_boundary(self, now: datetime) -> datetime | None:
        """The soonest future instant the window's active-state changes (its close if active, or the
        next open) — used to schedule a tick."""
        cands = []
        end = self.active_end(now)
        if end is not None and end > now:
            cands.append(end)
        nxt = self.next_open(now)
        if nxt is not None and nxt > now:
            cands.append(nxt)
        return min(cands) if cands else None

    def occurrences(self, hstart: datetime, hend: datetime, cap: int = 2000) -> list[tuple[datetime, datetime]]:
        """Expand to explicit (start, end) windows intersecting [hstart, hend). Bounded by ``cap``;
        used only for the overlap check on add, not the per-tick hot path."""
        delta = self._delta()
        if delta.total_seconds() <= 0:
            return []
        if hstart <= self.start_anchor:
            k = 0
        else:
            k = max(0, (hstart - self.start_anchor) // delta - 1)  # back up one to catch a straddling window
        out: list[tuple[datetime, datetime]] = []
        for i in range(cap):
            t = self.start_anchor + (k + i) * delta
            if t >= hend or (self.until is not None and t > self.until):
                break
            if (self.valid_days is None or _DAYS[t.weekday()] in self.valid_days):
                te = t + self.duration
                if te > hstart:
                    out.append((t, te))
        return out


@dataclass
class Pond:
    id: PondId
    name: str
    sources: list[PondId] = field(default_factory=list)
    optional_sources: set[PondId] = field(default_factory=set)
    windows: list[Window] = field(default_factory=list)
    # Fault tolerance budgets (operational config, editable against the live Pond; default 0 = no
    # retries). ``retry_immediately`` is consumed per Pond Run by the Duck (Ripple-level retries);
    # ``retry_on_change`` governs how many failed Pond Runs the Catchment re-attempts on a Source
    # update. See docs/guide/theory.md "Fault Tolerance".
    retry_immediately: int = 0
    retry_on_change: int = 0
    # A side-effecting Pond that must run every time (e.g. a monitoring ping): it is never engine-passed
    # when its Sources are unchanged, so the Duck always runs (the Ripple gates its data work via
    # ``pond.sources_changed()`` / ``pond.skip()``). ORed up from its Ripples' ``always_run`` flags.
    always_run: bool = False
    # A Pond Draw (cross-Catchment): fed by a duct, not executed by a Duck. It has no local sources;
    # its freshness is the upstream freshness mirrored by the poller (PondState.remote_f), and its
    # single ripple performs the data transfer. See plans/cross-catchment-ducts.md.
    is_draw: bool = False
    # A Spout (egress): a terminal node hanging off its Source with a standing Wake — it delivers the
    # Source's output to an external system, is run by the egress worker (not a Duck), and never
    # propagates (a Wake up, terminal down). The egress dual of a Draw. See plans/egress.md.
    is_spout: bool = False
    # At least one declared Source (required OR optional) is absent from the Catchment — not deployed
    # and not drawn over a duct. The Pond cannot run with a missing dependency, so it is hard-blocked
    # until every Source is present (e.g. the Source is deployed, or a duct draws it in).
    has_missing_source: bool = False


@dataclass
class Ripple:
    id: RippleId
    pond_id: PondId
    name: str
    parents: list[RippleId] = field(default_factory=list)
    optional_parents: set[RippleId] = field(default_factory=set)


@dataclass
class Trigger:
    """A standing trigger on a Pond. ``wave`` re-Taps on completion/idle; ``tide`` is a clock that
    emits a push target ``now`` when the last requested freshness ages past ``bound``."""

    pond_id: PondId
    kind: str  # "wave" | "tide"
    bound: timedelta | None = None  # Tide only: the staleness bound.


@dataclass(frozen=True)
class BeginRun:
    """A command from the Catchment to a Duck: start a Pond Run at freshness ``f`` (push every Ripple
    to ``f``). Emitted by the Catchment's ``start_pond_run``; identified/idempotent by ``(pond_id, f)``.
    ``force`` re-runs every Ripple even if already fresh to ``f`` (a Force/recompute)."""

    pond_id: PondId
    f: datetime
    force: bool = False
    refresh: bool = False  # a Refresh: the Duck wipes the registry first → a cold full rebuild
    sources_changed: bool = True  # the engine's no-change verdict, exposed to Ripples as
                                  # pond.sources_changed() (an always_run Pond runs even when False)


# ─── Run state (mutable) ──────────────────────────────────────────────────────


@dataclass
class PondState:
    start_f: datetime = NEVER  # freshness of the most recently started Pond Run
    end_f: datetime = NEVER  # freshness of the most recently completed Pond Run
    changed_f: datetime = NEVER  # freshness at which this Pond's OUTPUT last actually changed
                                 # (<= end_f). A pass advances end_f but holds this. See
                                 # plans/no-change-skip.md.
    d: timedelta = ZERO  # window delay carried by the current freshness
    remote_f: datetime = NEVER  # Pond Draws only: the upstream freshness mirrored by the poller
                                # (transient — repopulated on each poll, not persisted)
    remote_down: bool = False  # Pond Draws only: upstream is failed/killed/blocked/unreachable →
                               # the Draw is blocked (drains landed data, solicits nothing)
    has_received_pull: bool = False  # inbox: a Sink/trigger asked for resupply
    has_pull: bool = False  # a Pond Run is wanted in pull
    pull_m: datetime = NEVER  # minted epoch of the active pull (the freshness an Inlet stamps; the
                              # pull counterpart of a push target's value). NEVER when no pull.
    targets: list[datetime] = field(default_factory=list)  # unsatisfied push target freshnesses
    # Fault tolerance (see docs/guide/theory.md "Fault Tolerance").
    is_failed: bool = False  # a Pond Run gave up and has not been superseded by a fresher success
    is_blocked: bool = False  # this Pond is failed, or a required Source is failed/blocked/killed
    missing_asset: str | None = None  # a Ripple read an unpublished Source asset ("source.table") — the
                                      # Pond is blocked *waiting* for it, not failed (no budget, no alert).
                                      # Transient (re-derived on the next read attempt), see plans/reset.md.
    failed_f: datetime = NEVER  # freshness of the freshest Pond Run that has failed (NEVER if none)
    failures: int = 0  # failed Pond Runs since the last success (counted against retry_on_change)
    # Control (Wake/Force/Kill — see docs).
    is_killed: bool = False  # operator Kill: terminal, supersedes retries, until Wake/Force/Clear
    pull_local: bool = False  # the pending pull is a Wake — run on fresh input but do NOT solicit Sources
    standing_wake: bool = False  # a Spout: a *standing* non-propagating pull — re-arm a Wake whenever idle,
                                 # so it runs on every Source advance without ever soliciting the Source.
                                 # Sleep/Kill disarm it; Wake/Force re-arm it.
    force_pending: bool = False  # next Run is a Force (recompute) — re-run Ripples even if unchanged
    refresh_pending: bool = False  # next Run is a Refresh — the Duck wipes state first for a cold rebuild
    repairing: bool = False  # in an active repair plan: blocked from normal demand until its turn (D3)
    runs_started: int = 0
    runs_completed: int = 0
    gen_start_times: dict[int, datetime] = field(default_factory=dict)
    completion_times: list[datetime] = field(default_factory=list)

    def copy(self) -> PondState:
        return replace(
            self,
            targets=list(self.targets),
            gen_start_times=dict(self.gen_start_times),
            completion_times=list(self.completion_times),
        )


@dataclass
class RippleState:
    start_f: datetime = NEVER
    end_f: datetime = NEVER
    has_pull: bool = False
    targets: list[datetime] = field(default_factory=list)
    is_running: bool = False  # an in-flight run the engine is awaiting completion of
    started_at: datetime | None = None  # telemetry: when the in-flight run started
    runs_started: int = 0
    runs_completed: int = 0
    completion_times: list[datetime] = field(default_factory=list)

    def copy(self) -> RippleState:
        return replace(self, targets=list(self.targets), completion_times=list(self.completion_times))


# ─── Push target sets ─────────────────────────────────────────────────────────


def min_target(targets: list[datetime]) -> datetime | None:
    return min(targets) if targets else None


def max_target(targets: list[datetime]) -> datetime | None:
    return max(targets) if targets else None
