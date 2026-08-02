"""The Flock — duckstring's over-envelope compute tier.

Work that fits the Duck runs in the Duck; a ``pond.trickle(...)`` terminal whose comprehensive
recompute would exceed the Duck's envelope is dispatched to a **serverless engine — the Flock**,
which runs the heavy read side (scan/join/filter/project); the Duck keeps the merge/diff/publish
semantics locally. The engine is **pluggable**: Athena today (the default), EMR Serverless /
Exaflock later — the builder hook, the per-Pond posture, and this policy layer are all
engine-agnostic.

**No metering here.** OSS dispatches and runs; it never bills. An engine's *cloud* deployment
accounts for its own usage out of band (for Athena: per-tenant workgroup → CloudWatch
bytes-scanned). The engine protocol has no cost surface — dispatch returns a result relation or
``None``, nothing more.

Posture ladder (a **Pond-level** setting — ``pond.toml [flock] mode`` coalesced with an operator
override, threaded to the Duck as ``DUCKSTRING_FLOCK_MODE``; inert without a configured engine, so
the same code runs anywhere — plans/cloud-config.md):

- **always** — every eligible terminal recomputes on the Flock (the known-heavy declaration).
- **upgrade** — comprehensive-bound runs only: dispatch up front when *clearly* over the
  envelope, else run the local recompute bounded+materialised and fail up on OOM.
- **off** — never.

Config (env):

- ``DUCKSTRING_FLOCK_ENGINE``  — engine name (default ``athena``); unknown/unconfigured ⇒ off.
- ``DUCKSTRING_FLOCK_MODE``    — the Pond's resolved posture (off|upgrade|always; set by the Duck from
  its effective config — plans/cloud-config.md; a bare env default of ``off`` otherwise).
- ``DUCKSTRING_FLOCK_OOM_POLICY``— ``fail_up`` (default; offload on OOM) | ``fail`` (hard cap).
- ``DUCKSTRING_FLOCK_MIN_ROWS``— explicit envelope override (rows summed over source leaves).
- ``DUCKSTRING_FLOCK_FORCE=1`` — force ``always`` (testing/demo).
- ``DUCKSTRING_MEMORY_LIMIT`` — the Duck's DuckDB memory cap (the OOM fail-up depends on it); the
  "clearly over the envelope" row threshold is DERIVED from it (no abstract size preset — the box's
  real memory is the envelope). Absent ⇒ a conservative default.
- engine-specific config lives under the engine's own namespace (Athena:
  ``DUCKSTRING_FLOCK_ATHENA_{WORKGROUP,DATABASE,SCRATCH,REGION}``).
"""

from __future__ import annotations

import logging
import os
import re
import uuid
from typing import Protocol

log = logging.getLogger("duckstring.flock")

FLOCK_MODES = (None, "always", "upgrade", "off")

# The comprehensive-recompute row envelope (sum over source leaves) is derived from the Duck's real
# memory cap: ~this many rows per GiB the box can absorb before a local recompute is clearly doomed.
# Deliberately conservative; the sidecar-stats-aware estimate is the E1-proper follow-up. The OOM
# fail-up (the memory cap) is the real safety net — this only pre-empts an obviously-doomed local try.
_ROWS_PER_GIB = 500_000
_DEFAULT_MIN_ROWS = 4_000_000  # when the memory cap is unknown (a stock local Duck)

# Process-lifetime dispatch counters (this module runs inside the Duck). Shipped on the Duck's
# run_completed event so a *degrading* Flock is visible from the Catchment (/metrics, /api/status) —
# the fallback contract means a failed dispatch still produces a correct run, which otherwise looks
# identical to a working Flock while quietly paying full local compute every time.
_stats = {"dispatched": 0, "failed": 0, "last_error": None}


def stats() -> dict | None:
    """A peek at this Duck's dispatch counters — ``None`` while nothing was ever attempted."""
    if not _stats["dispatched"] and not _stats["failed"]:
        return None
    return dict(_stats)


def take_stats() -> dict | None:
    """The dispatches since the last call, **and reset** — the per-Ripple-Run delta the Duck ships on its
    ripple event, which the Catchment accumulates.

    A delta rather than a process-lifetime snapshot, and carried by the *ripple* event rather than run
    completion, because neither the process nor the run is a reliable carrier: a Duck killed mid-run
    (OOM, a reaped box) is respawned and resumes from its ledger, so the process that dispatched is often
    NOT the one that completes the Pond Run — a snapshot read at completion would silently be zero."""
    if not _stats["dispatched"] and not _stats["failed"]:
        return None
    out = dict(_stats)
    _stats.update({"dispatched": 0, "failed": 0, "last_error": None})
    return out


def _record_dispatch(engine, result, error: str | None = None) -> None:
    if result is None:
        _stats["failed"] += 1
        _stats["last_error"] = (error or getattr(engine, "last_error", None)
                                or "dispatch failed (see the Duck log)")
    else:
        _stats["dispatched"] += 1
        _stats["last_error"] = None


def _dispatch(engine, builder, out_pk):
    """Dispatch + record. The engine protocol says ``dispatch`` returns ``None`` on an engine-side
    failure, but the fallback contract (the Flock is NEVER load-bearing) must not depend on every engine
    remembering to catch — an engine that raises would fail the Pond Run instead of degrading. So the
    catch-all lives here too."""
    try:
        result = engine.dispatch(builder, out_pk)
        error = None
    except Exception as exc:  # noqa: BLE001 — the fallback contract: degrade to local, never fail the run
        log.warning("flock: engine raised (%s: %s) — running local", type(exc).__name__, str(exc)[:300])
        result, error = None, f"{type(exc).__name__}: {str(exc)[:300]}"
    _record_dispatch(engine, result, error)
    return result


class FlockEngine(Protocol):
    """A serverless big-compute backend behind the Flock tier. One implementation per engine
    (Athena today; DuckFlock later). No metering surface — usage is accounted for out of band
    by the engine's cloud deployment."""

    def enabled(self) -> bool: ...
    def eligible(self, builder) -> str | None: ...          # None = dispatchable; else the reason
    def estimate_rows(self, builder) -> int: ...
    def dispatch(self, builder, out_pk): ...                # result relation, or None on failure


def get_engine(env=None) -> FlockEngine | None:
    """The configured engine, or ``None`` when the Flock is off (no engine / not enabled).

    ``DUCKSTRING_FLOCK_ENGINE`` is either a built-in name (``athena``) or a ``module:Class`` spec — the
    open engine seam, mirroring ``DUCKSTRING_DUCK_LAUNCHER``. The class is constructed with the env
    mapping and must satisfy :class:`FlockEngine`. Unlike the launcher seam this **degrades rather than
    raises**: the Flock is never load-bearing (a missing engine just means local compute), so an
    unimportable spec warns and turns the Flock off instead of failing the Pond."""
    e = env if env is not None else os.environ
    name = (e.get("DUCKSTRING_FLOCK_ENGINE") or "athena").strip()
    if ":" in name:
        import importlib

        module_name, _, class_name = name.partition(":")
        try:
            engine = getattr(importlib.import_module(module_name), class_name)(e)
        except Exception as exc:  # noqa: BLE001 — a bad spec must not fail a Pond Run
            log.warning("flock: cannot load DUCKSTRING_FLOCK_ENGINE=%r (%s) — off", name, exc)
            return None
        return engine if engine.enabled() else None
    if name.lower() == "athena":
        from .engines.athena import AthenaEngine

        engine = AthenaEngine(e)
        return engine if engine.enabled() else None
    log.warning("flock: unknown DUCKSTRING_FLOCK_ENGINE=%r — off", name)
    return None


def enabled(env=None) -> bool:
    """Cheap gate for the builder hook — is a Flock engine configured and ready?"""
    return get_engine(env) is not None


def _mem_limit_gib(raw: str | None) -> float | None:
    """Parse a DuckDB memory-limit string (``8GB``/``512MB``/``16GiB``/a bare byte count) → GiB."""
    if not raw or not raw.strip():
        return None
    m = re.match(r"\s*([0-9.]+)\s*([kmgt]i?b|b)?\s*$", raw.lower())
    if not m:
        return None
    factor = {"b": 1 / 2**30, "kb": 1 / 2**20, "kib": 1 / 2**20, "mb": 1 / 2**10, "mib": 1 / 2**10,
              "gb": 1, "gib": 1, "tb": 1024, "tib": 1024}.get(m.group(2) or "b", 1)
    return float(m.group(1)) * factor


def _min_rows(env=None) -> int:
    """The "clearly over the envelope, dispatch up front" row threshold. Explicit override wins; else
    derived from the Duck's real memory cap; else a conservative default (no abstract size preset)."""
    e = env if env is not None else os.environ
    if e.get("DUCKSTRING_FLOCK_MIN_ROWS"):
        return int(e["DUCKSTRING_FLOCK_MIN_ROWS"])
    gib = _mem_limit_gib(e.get("DUCKSTRING_MEMORY_LIMIT"))
    return int(gib * _ROWS_PER_GIB) if gib else _DEFAULT_MIN_ROWS


def _oom_policy(env=None) -> str:
    e = env if env is not None else os.environ
    policy = (e.get("DUCKSTRING_FLOCK_OOM_POLICY") or "fail_up").lower()
    return policy if policy in ("fail_up", "fail") else "fail_up"


def _runtime_mode(env=None) -> str:
    e = env if env is not None else os.environ
    if e.get("DUCKSTRING_FLOCK_FORCE", "0") == "1":
        return "always"
    mode = (e.get("DUCKSTRING_FLOCK_MODE") or "upgrade").lower()
    return mode if mode in ("always", "upgrade", "off") else "upgrade"


def resolve_mode(pond_mode: str | None, env=None) -> str:
    """The effective posture for this run: the Pond's resolved posture (threaded through the Duck)
    wins; absent, the runtime default env."""
    return pond_mode or _runtime_mode(env)


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


def comprehensive(builder, out_pk, *, pond_mode: str | None, comprehensive_bound: bool):
    """Decide + run this terminal's comprehensive recompute on the Flock. Returns a result
    relation for the terminal's own comprehensive machinery, or ``None`` (run local — the common
    case). ``pond_mode`` is the Pond's resolved posture; ``comprehensive_bound`` is the caller's
    verdict that this run recomputes wholesale anyway (bootstrap / ``ivm=False``); incremental epochs
    pass ``False`` and never dispatch."""
    engine = get_engine()
    if engine is None:
        return None
    mode = resolve_mode(pond_mode)
    if mode == "off":
        return None
    reason = engine.eligible(builder)
    if reason is not None:
        log.info("flock: not dispatching (%s)", reason)
        return None
    if mode == "always":
        return _dispatch(engine, builder, out_pk)
    # upgrade: comprehensive-bound runs only; dispatch when clearly over, else local + OOM fail-up.
    if not comprehensive_bound:
        return None
    if engine.estimate_rows(builder) >= _min_rows():
        return _dispatch(engine, builder, out_pk)
    try:
        return _probe_local(builder)
    except Exception as exc:
        import duckdb

        if not isinstance(exc, duckdb.OutOfMemoryException):
            raise
        # oom_policy=fail is the hard cap: surface the OOM as a Pond failure, don't offload.
        if _oom_policy() == "fail":
            log.warning("flock: local comprehensive hit the memory limit and oom_policy=fail — not offloading")
            raise
        log.warning("flock: local comprehensive hit the memory limit (%s) — failing up", str(exc)[:200])
        remote = _dispatch(engine, builder, out_pk)
        if remote is None:  # the engine also failed — surface the real problem, don't loop
            raise
        return remote
