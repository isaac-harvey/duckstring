"""The Duck engine: push-only execution of Pond Runs for a single Pond.

Given ``begin_run(F)`` from the Catchment it stamps every Ripple with target ``F`` and drives them to
completion (push), with no pull, no triggers, and no notion of Sources — those all live in the
Catchment. Holding several targets at once lets a Pond's roots run ahead of its leaf through the
bottleneck (intra-Pond pipelining) with no cap. It is *told* when a Ripple finishes
(:func:`complete_ripple`); when every leaf reaches a freshness it reports a :class:`RunCompleted`.

This is what gives the Duck autonomy: a started Pond Run finishes to completion from the targets +
ledger alone, even if the Catchment is unreachable. Ripples are keyed by **name** (matching the run
ledger and the deployed code).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from .core import NEVER, RippleState, min_target


@dataclass(frozen=True)
class RunCompleted:
    """A Pond Run reached completion at freshness ``f`` (every leaf is fresh to ``f``)."""

    f: datetime


@dataclass(frozen=True)
class RunFailed:
    """A Pond Run gave up at freshness ``f`` (a Ripple failed and the Run's immediate-retry budget was
    exhausted). ``ripple`` is the Ripple that gave up — the Catchment records it and keys the Pond
    failure off ``f`` (the Run the Ripple was reaching). See docs/guide/theory.md "Fault Tolerance"."""

    f: datetime
    ripple: str


@dataclass
class WorkerState:
    # Intra-pond topology, keyed by ripple name. All parents required unless in `optional`.
    parents: dict[str, list[str]] = field(default_factory=dict)
    optional: dict[str, set[str]] = field(default_factory=dict)
    states: dict[str, RippleState] = field(default_factory=dict)
    last_completed_f: datetime = NEVER  # freshness of the last reported Pond Run completion
    retry_immediately: int = 0  # Ripple-Run retries allowed within one Pond Run
    immediate_left: dict[datetime, int] = field(default_factory=dict)  # per in-flight Run: retries left

    def clone(self) -> WorkerState:
        return WorkerState(
            parents=self.parents,
            optional=self.optional,
            states={k: v.copy() for k, v in self.states.items()},
            last_completed_f=self.last_completed_f,
            retry_immediately=self.retry_immediately,
            immediate_left=dict(self.immediate_left),
        )


def new_state(
    parents: dict[str, list[str]], optional: dict[str, set[str]] | None = None, retry_immediately: int = 0
) -> WorkerState:
    opt = optional or {}
    return WorkerState(
        parents=dict(parents),
        optional={k: set(v) for k, v in opt.items()},
        states={name: RippleState() for name in parents},
        retry_immediately=retry_immediately,
    )


def _leaves(s: WorkerState) -> list[str]:
    parented: set[str] = set()
    for ps in s.parents.values():
        parented.update(ps)
    return [name for name in s.parents if name not in parented]


def source_f(s: WorkerState, name: str) -> datetime:
    """Freshness available to a Ripple. A root's input is the Pond Run freshness it is being asked
    for (the oldest unsatisfied target); a non-root inherits min(required) / max(optional) parents."""
    parents = s.parents[name]
    if not parents:
        return min_target(s.states[name].targets) or NEVER  # root: the Pond Run being initiated
    opt = s.optional.get(name, set())
    req = [p for p in parents if p not in opt]
    if req:
        return min(s.states[p].end_f for p in req)
    return max(s.states[p].end_f for p in parents)


def begin_run(
    state: WorkerState, f: datetime, retry_immediately: int | None = None, force: bool = False
) -> WorkerState:
    """Start a Pond Run at freshness ``f``: stamp every Ripple with target ``f`` (push to completion),
    and give this Run a fresh Ripple-retry budget. ``retry_immediately`` overrides the state default
    (the Catchment passes the live budget per Run, since it is editable while the Duck is warm).
    ``force`` recomputes: it resets every Ripple's ``end_f`` (and the completion watermark) so they
    re-run even if already fresh to ``f``."""
    s = state.clone()
    budget = s.retry_immediately if retry_immediately is None else retry_immediately
    s.immediate_left.setdefault(f, budget)
    if force:
        s.last_completed_f = NEVER  # so completing at f re-reports RunCompleted
        for rs in s.states.values():
            rs.end_f = NEVER
    for rs in s.states.values():
        if f > rs.end_f and f not in rs.targets:
            rs.targets.append(f)
    return s


def _can_start(s: WorkerState, name: str) -> bool:
    rs = s.states[name]
    if rs.is_running:
        return False
    mt = min_target(rs.targets)
    return mt is not None and source_f(s, name) >= mt


def sentinel(now: datetime, state: WorkerState) -> tuple[WorkerState, list[str]]:
    """Start every runnable Ripple to a fixpoint; return the names to actually launch."""
    s = state.clone()
    launched: list[str] = []
    changed = True
    while changed:
        changed = False
        for name in list(s.states):
            if _can_start(s, name):
                rs = s.states[name]
                sf = source_f(s, name)
                rs.start_f = sf
                rs.is_running = True
                rs.started_at = now
                rs.runs_started += 1
                rs.targets = [t for t in rs.targets if t > sf]
                launched.append(name)
                changed = True
    return s, launched


def complete_ripple(state: WorkerState, name: str, now: datetime) -> tuple[WorkerState, RunCompleted | None]:
    """A Ripple's run finished. Adopt its start freshness; if every leaf has advanced to a new Pond
    Run freshness, return that completion."""
    s = state.clone()
    rs = s.states[name]
    rs.end_f = rs.start_f
    rs.is_running = False
    rs.started_at = None
    rs.runs_completed += 1
    rs.completion_times.append(now)

    new_end = min(s.states[leaf].end_f for leaf in _leaves(s))
    if new_end > s.last_completed_f:
        s.last_completed_f = new_end
        s.immediate_left.pop(new_end, None)  # this Run is done; drop its retry budget
        return s, RunCompleted(new_end)
    return s, None


def fail_ripple(
    state: WorkerState, name: str, now: datetime, retry: bool = True
) -> tuple[WorkerState, RunFailed | None]:
    """A Ripple's run errored. If the Pond Run it was reaching (``F = Ripple.start_f``) still has
    immediate-retry budget, spend one and re-arm the Ripple to run again (``sentinel`` relaunches it);
    otherwise the Run gives up and a :class:`RunFailed` is returned for the Catchment. Either way the
    Ripple is no longer running. ``retry=False`` skips the budget and gives up immediately — for a
    :class:`~duckstring.core.MissingSourceAsset`, which a re-run won't fix within the same Run."""
    s = state.clone()
    rs = s.states[name]
    f = rs.start_f
    rs.is_running = False
    rs.started_at = None
    if retry and s.immediate_left.get(f, 0) > 0:
        s.immediate_left[f] -= 1
        if f > rs.end_f and f not in rs.targets:
            rs.targets.append(f)  # retry the same Ripple, same Run
        return s, None
    s.immediate_left.pop(f, None)
    return s, RunFailed(f, name)
