"""The Flock — duckstring's over-envelope compute tier.

Work that fits the Duck runs in the Duck; a ``pond.trickle(...)`` terminal whose comprehensive
recompute would exceed the Duck's envelope is dispatched to a **serverless engine — the Flock**,
which runs the heavy read side (scan/join/filter/project); the Duck keeps the merge/diff/publish
semantics locally. The engine is **pluggable**: Athena today (the default), DuckFlock later —
the builder hook, the ``@ripple(flock=…)`` posture, and this policy layer are all engine-agnostic.

**No metering here.** OSS dispatches and runs; it never bills. An engine's *cloud* deployment
accounts for its own usage out of band (for Athena: per-tenant workgroup → CloudWatch
bytes-scanned). The engine protocol has no cost surface — dispatch returns a result relation or
``None``, nothing more.

Posture ladder (``@ripple(flock=…)`` over ``DUCKSTRING_FLOCK_MODE``, default ``upgrade``; inert
without a configured engine, so the same code runs anywhere):

- **always** — every eligible terminal recomputes on the Flock (the known-heavy declaration).
- **upgrade** — comprehensive-bound runs only: dispatch up front when *clearly* over the
  envelope, else run the local recompute bounded+materialised and fail up on OOM.
- **off** — never.

Config (env):

- ``DUCKSTRING_FLOCK_ENGINE``  — engine name (default ``athena``); unknown/unconfigured ⇒ off.
- ``DUCKSTRING_FLOCK_MODE``    — runtime default posture (``upgrade``).
- ``DUCKSTRING_FLOCK_MIN_ROWS``— explicit envelope override (rows summed over source leaves).
- ``DUCKSTRING_FLOCK_FORCE=1`` — force ``always`` (testing/demo).
- ``DUCKSTRING_DUCK_SIZE`` / ``DUCKSTRING_MEMORY_LIMIT`` — Duck-level (envelope preset; the
  DuckDB memory cap the OOM fail-up depends on).
- engine-specific config lives under the engine's own namespace (Athena:
  ``DUCKSTRING_FLOCK_ATHENA_{WORKGROUP,DATABASE,SCRATCH,REGION}``).
"""

from __future__ import annotations

import logging
import os
import uuid
from typing import Protocol

log = logging.getLogger("duckstring.flock")

FLOCK_MODES = (None, "always", "upgrade", "off")

# Duck preset → comprehensive-recompute row envelope (sum over source leaves). Deliberately
# conservative demo numbers; the size-aware estimate off sidecar stats is the E1-proper follow-up.
_SIZE_ROWS = {"s": 1_000_000, "m": 4_000_000, "l": 16_000_000, "xl": 64_000_000}


class FlockEngine(Protocol):
    """A serverless big-compute backend behind the Flock tier. One implementation per engine
    (Athena today; DuckFlock later). No metering surface — usage is accounted for out of band
    by the engine's cloud deployment."""

    def enabled(self) -> bool: ...
    def eligible(self, builder) -> str | None: ...          # None = dispatchable; else the reason
    def estimate_rows(self, builder) -> int: ...
    def dispatch(self, builder, out_pk): ...                # result relation, or None on failure


def get_engine(env=None) -> FlockEngine | None:
    """The configured engine, or ``None`` when the Flock is off (no engine / not enabled)."""
    e = env if env is not None else os.environ
    name = (e.get("DUCKSTRING_FLOCK_ENGINE") or "athena").lower()
    if name == "athena":
        from .engines.athena import AthenaEngine

        engine = AthenaEngine(e)
        return engine if engine.enabled() else None
    log.warning("flock: unknown DUCKSTRING_FLOCK_ENGINE=%r — off", name)
    return None


def enabled(env=None) -> bool:
    """Cheap gate for the builder hook — is a Flock engine configured and ready?"""
    return get_engine(env) is not None


def _min_rows(env=None) -> int:
    e = env if env is not None else os.environ
    size = e.get("DUCKSTRING_DUCK_SIZE", "s").lower()
    return int(e.get("DUCKSTRING_FLOCK_MIN_ROWS", _SIZE_ROWS.get(size, _SIZE_ROWS["s"])))


def _runtime_mode(env=None) -> str:
    e = env if env is not None else os.environ
    if e.get("DUCKSTRING_FLOCK_FORCE", "0") == "1":
        return "always"
    mode = (e.get("DUCKSTRING_FLOCK_MODE") or "upgrade").lower()
    return mode if mode in ("always", "upgrade", "off") else "upgrade"


def resolve_mode(ripple_mode: str | None, env=None) -> str:
    """The effective posture for one Ripple: its ``@ripple(flock=…)`` declaration wins; absent,
    the runtime default."""
    return ripple_mode or _runtime_mode(env)


def _probe_local(builder):
    """The local comprehensive recompute **bounded + materialised**: DuckDB is already capped at
    the Duck's memory limit (executor-wide, from ``DUCKSTRING_MEMORY_LIMIT``); land the result in
    a temp table *before any output write*, so "too big" surfaces here as a catchable
    ``OutOfMemoryException`` with zero partial-state risk — the caller fails up to the engine."""
    con = builder.ctx.con
    name = f"_ds_flock_probe_{uuid.uuid4().hex[:8]}"
    rel = builder._full_join()
    con.execute(f"CREATE TEMP TABLE {name} AS {rel.sql_query()}")
    return con.table(name)


def comprehensive(builder, out_pk, *, ripple_mode: str | None, comprehensive_bound: bool):
    """Decide + run this terminal's comprehensive recompute on the Flock. Returns a result
    relation for the terminal's own comprehensive machinery, or ``None`` (run local — the common
    case). ``comprehensive_bound`` is the caller's verdict that this run recomputes wholesale
    anyway (bootstrap / ``ivm=False``); incremental epochs pass ``False`` and never dispatch."""
    engine = get_engine()
    if engine is None:
        return None
    mode = resolve_mode(ripple_mode)
    if mode == "off":
        return None
    reason = engine.eligible(builder)
    if reason is not None:
        log.info("flock: not dispatching (%s)", reason)
        return None
    if mode == "always":
        return engine.dispatch(builder, out_pk)
    # upgrade: comprehensive-bound runs only; dispatch when clearly over, else local + OOM fail-up.
    if not comprehensive_bound:
        return None
    if engine.estimate_rows(builder) >= _min_rows():
        return engine.dispatch(builder, out_pk)
    try:
        return _probe_local(builder)
    except Exception as exc:
        import duckdb

        if not isinstance(exc, duckdb.OutOfMemoryException):
            raise
        log.warning("flock: local comprehensive hit the memory limit (%s) — failing up", str(exc)[:200])
        remote = engine.dispatch(builder, out_pk)
        if remote is None:  # the engine also failed — surface the real problem, don't loop
            raise
        return remote
