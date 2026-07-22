"""Trigger + status endpoints, backed by the freshness :class:`~duckstring.catchment.driver.Driver`.

Tap/Pulse are one-shot; Wave/Tide are standing. Tide carries a staleness **bound** (seconds), not a
cron. Status reports freshness/staleness from the engine, not generations.

Every pond-targeting route takes optional ``major`` / ``version`` query params: ``major`` picks the
major line (default: the highest deployed), ``version`` additionally requires that exact version to
be the line's currently selected artifact. The resolved target is the engine key ``name@major``.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel

from .. import auth

router = APIRouter()

_STATUS_WAIT_TICK = 0.05  # how often the status long-poll re-checks the state version
_STATUS_WAIT_TIMEOUT = 25.0  # heartbeat ceiling on a held status request


def _driver(request: Request):
    return request.app.state.driver


def _parse_f(value: str | None) -> datetime | None:
    """Parse an optional ISO-8601 demand epoch (a duct forwards the downstream's freshness)."""
    if value is None:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"invalid freshness {value!r}") from exc


def _resolve(request: Request, name: str, major: int | None, version: str | None) -> str:
    """Resolve a Pond reference to its engine key, mapping resolution errors to HTTP ones."""
    try:
        return _driver(request).resolve(name, major, version)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc.args[0])) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/status", dependencies=[auth.read])
async def status(
    request: Request, since: Optional[int] = None,
    principal: auth.Principal = Depends(auth.get_principal),
):
    """Live state. Without ``since``, returns immediately (the CLI / first load). With ``since``, it
    **long-polls**: holds until the engine state moves past that version (or a heartbeat timeout), so
    the UI updates the instant anything changes instead of on a fixed timer. The payload's ``version``
    is the token to pass back as ``since``. ``access_level`` is the caller's level — the UI gates its
    controls on it (so a read/demand key sees no dead buttons)."""
    driver = _driver(request)
    if since is not None:
        for _ in range(int(_STATUS_WAIT_TIMEOUT / _STATUS_WAIT_TICK)):
            if driver.state_version != since:
                break
            await asyncio.sleep(_STATUS_WAIT_TICK)
    # status() holds the driver lock and builds the whole payload — off the event loop so concurrent
    # requests (the draw long-polls, cross-Catchment view recursion) aren't blocked behind it.
    payload = await run_in_threadpool(driver.status)
    payload["access_level"] = auth.LEVEL_TO_NAME[principal.level]
    # The cloud-enable gate (remote data root + AWS creds) — the UI greys out remote-compute options
    # until both hold (plans/cloud-config.md).
    from .. import cloud
    payload["cloud"] = cloud.cloud_status(getattr(request.app.state, "data_root", None),
                                          getattr(request.app.state, "secret_store", None))
    return payload


def _redact_tracebacks(runs: list[dict]) -> None:
    """Strip the full traceback from each run (and its nested Ripple Runs), in place. Tracebacks can
    surface filesystem paths / connection strings, so they are full-access only; the error *message*
    is kept for every level."""
    for run in runs:
        run["traceback"] = None
        for ripple in run.get("ripples") or []:
            ripple["traceback"] = None


@router.get("/lineage", dependencies=[auth.read])
def lineage_graph(
    request: Request,
    pond: str | None = None,
    major: int | None = None,
    table: str | None = None,
    columns: bool = False,
):
    """The **observed table-level lineage** (plans/lineage.md Phase 1): the tables each Ripple actually
    read and wrote on its latest recorded run — exact (recorded at the read/write call), never inferred.
    ``pond`` narrows to one Pond (``major`` picks the line, default highest); ``table`` narrows to the
    Ripples touching that table name. Reads with ``source: null`` are the Pond's own tables.
    ``columns=true`` adds the deploy-captured **static column lineage** (Phase 2) per pond —
    ``{table: {column: [{ref, column}] | "constant" | "opaque"}}``."""
    return request.app.state.driver.lineage(pond=pond, major=major, table=table, columns=columns)


@router.get("/runs", dependencies=[auth.read])
def runs(
    request: Request,
    pond: str | None = None,
    major: int | None = None,
    version: str | None = None,
    lineage: bool = True,
    ripples: bool = False,
    limit: int = 100,
    after: str | None = None,
    before: str | None = None,
    principal: auth.Principal = Depends(auth.get_principal),
):
    """Recent Pond Run history (newest first). ``pond`` filters to that Pond and, when ``lineage``,
    its upstream sources; ``ripples`` nests each run's Ripple Runs. ``limit`` is clamped to [1, 1000].
    ``after``/``before`` (UTC ISO) bound the run start for date-range navigation. Tracebacks are
    redacted below full access (the error message is always kept)."""
    key = _resolve(request, pond, major, version) if pond is not None else None
    limit = max(1, min(limit, 1000))
    history = _driver(request).run_history(key, lineage, ripples, limit, after=after, before=before)
    if principal.level != auth.Level.FULL:
        _redact_tracebacks(history)
    return {"runs": history}


@router.post("/ponds/{name}/tap", dependencies=[auth.demand])
def tap(
    name: str, request: Request, major: int | None = None, version: str | None = None,
    m: str | None = None,
):
    """``m`` (optional ISO freshness) is the demand epoch to mint — a duct forwards the downstream's."""
    _driver(request).tap(_resolve(request, name, major, version), _parse_f(m))
    return {"ok": True}


@router.post("/ponds/{name}/pulse", dependencies=[auth.demand])
def pulse(
    name: str, request: Request, major: int | None = None, version: str | None = None,
    at: str | None = None,
):
    """``at`` (optional ISO freshness) is the push target — a duct forwards the downstream's target."""
    _driver(request).pulse(_resolve(request, name, major, version), _parse_f(at))
    return {"ok": True}


@router.post("/ponds/{name}/wave", dependencies=[auth.demand])
def wave(name: str, request: Request, major: int | None = None, version: str | None = None):
    _driver(request).wave(_resolve(request, name, major, version))
    return {"ok": True}


class _TideBody(BaseModel):
    bound_seconds: float


@router.post("/ponds/{name}/tide", dependencies=[auth.demand])
def tide(name: str, body: _TideBody, request: Request, major: int | None = None, version: str | None = None):
    key = _resolve(request, name, major, version)
    if body.bound_seconds <= 0:
        raise HTTPException(status_code=422, detail="bound_seconds must be positive")
    _driver(request).tide(key, timedelta(seconds=body.bound_seconds))
    return {"ok": True}


# ─── Control (Wake / Sleep / Force / Kill) ───────────────────────────────────


@router.post("/ponds/{name}/wake", dependencies=[auth.full])
def wake(name: str, request: Request, major: int | None = None, version: str | None = None):
    """Wake a Pond — a one-shot non-propagating pull: run once on fresh input, no upstream solicit."""
    _driver(request).wake(_resolve(request, name, major, version))
    return {"ok": True}


@router.post("/ponds/{name}/force", dependencies=[auth.full])
def force(name: str, request: Request, major: int | None = None, version: str | None = None):
    """Force a Pond to recompute now at its current freshness, even with no upstream change."""
    _driver(request).force(_resolve(request, name, major, version))
    return {"ok": True}


@router.post("/ponds/{name}/refresh", dependencies=[auth.full])
def refresh(
    name: str, request: Request, clear: bool = False,
    major: int | None = None, version: str | None = None,
):
    """Refresh a Pond — flag its next run to be a cold wipe-and-rebuild (lazy; runs nothing now).
    ``clear=true`` un-sets a pending refresh."""
    _driver(request).refresh(_resolve(request, name, major, version), clear=clear)
    return {"ok": True}


@router.post("/ponds/{name}/reset", dependencies=[auth.full])
def reset(
    name: str, request: Request, clear_history: bool = False,
    major: int | None = None, version: str | None = None,
):
    """Reset a Pond to a fresh-deploy state — scrub its registry, data, and ledger and rewind its
    freshness, keeping its code, operational config, and demand. Lazy (no forced run); requires the Pond
    idle (409). See plans/reset.md."""
    try:
        _driver(request).reset_pond(_resolve(request, name, major, version), clear_history=clear_history)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"ok": True}


@router.delete("/ponds/{name}", dependencies=[auth.full])
def remove_pond(name: str, request: Request, major: int | None = None, version: str | None = None,
                wipe: bool = False):
    """Remove (retire) one deployed major line — delete its live selection, config, on-disk runtime, and
    its own Spouts + alert channels, keeping its deployment record + run history. Downstream sinks that pin
    it block on the missing Source. Requires the line idle + demand-free (409). With ``wipe=true`` also
    purges the deployment record + run history + ``{version}/`` artifacts (as if never deployed; not
    reversible by a redeploy). See plans/remove-pond.md."""
    key = _resolve(request, name, major, version)
    n, _, mj = key.rpartition("@")
    try:
        return _driver(request).remove_pond(n, int(mj), wipe=wipe)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.delete("/ponds/{name}/tables/{table}", dependencies=[auth.full])
def delete_table(
    name: str, table: str, request: Request, major: int | None = None, version: str | None = None,
):
    """Delete one table from a Pond — its published data **and** its registry collection — now. No run:
    it reappears only if the Pond's code recreates it on a future run. Requires the Pond idle (409).
    See plans/deletes.md."""
    try:
        _driver(request).delete_table(_resolve(request, name, major, version), table)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"ok": True}


class _RepairPond(BaseModel):
    name: str
    major: Optional[int] = None


class _RepairBody(BaseModel):
    ponds: list[_RepairPond]
    downstream: bool = False


@router.post("/repair", dependencies=[auth.full])
def repair(request: Request, body: _RepairBody):
    """Repair — force-rebuild a connected set of Ponds now, in topological order. ``downstream`` extends
    the scope to all descendants. 422 if the set is disconnected (a skipped Pond in a sequence)."""
    try:
        plan = _driver(request).repair([(p.name, p.major) for p in body.ponds], downstream=body.downstream)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"ok": True, **plan}


class _BatchBody(BaseModel):
    ponds: list[_RepairPond]
    operations: list[str]
    confirm: Optional[str] = None


@router.post("/ponds/batch", dependencies=[auth.full])
def batch(request: Request, body: _BatchBody):
    """Apply a set of operations to a set of Ponds, in precedence order — the primitive behind the UI
    Selector and ``duckstring do``. ``operations`` ⊆ kill/sleep/reset/wipe/remove/clear/repair/refresh;
    Repair is exclusive with Remove/Reset, Remove implies (and subsumes) Reset. Any of reset/wipe/remove
    require ``confirm`` to equal the Catchment name. 422 on a bad op-set or a missing/wrong confirm; a
    per-Pond execution error is collected in the response, not fatal. See plans/selector_ui.md."""
    try:
        return _driver(request).batch(
            [(p.name, p.major) for p in body.ponds], body.operations, confirm=body.confirm)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/ponds/{name}/wipe-history", dependencies=[auth.full])
def wipe_history(name: str, request: Request, major: int | None = None, version: str | None = None):
    """Wipe a Pond's run history (its pond_run/ripple_run rows) — no data scrub, no state change."""
    _driver(request).wipe_history(_resolve(request, name, major, version))
    return {"ok": True}


@router.post("/ponds/{name}/kill", dependencies=[auth.full])
def kill(name: str, request: Request, major: int | None = None, version: str | None = None):
    """Kill a Pond — terminate its Duck and park it in a terminal killed state (cancels its Run)."""
    _driver(request).kill(_resolve(request, name, major, version))
    return {"ok": True}


class _SleepBody(BaseModel):
    upstream: bool = False


@router.post("/ponds/{name}/sleep", dependencies=[auth.full])
def sleep(
    name: str, request: Request, body: _SleepBody = _SleepBody(),
    major: int | None = None, version: str | None = None,
):
    """Sleep a Pond — clear its demand (push+pull) + its Ripples' pull; keep started runs completing.
    ``upstream`` also sleeps every ancestor."""
    _driver(request).sleep(_resolve(request, name, major, version), upstream=body.upstream)
    return {"ok": True}


@router.post("/ponds/{name}/untrigger", dependencies=[auth.demand])
def untrigger(name: str, request: Request, major: int | None = None, version: str | None = None):
    """Remove the standing Wave/Tide trigger from a Pond (existing work drains)."""
    _driver(request).remove_trigger(_resolve(request, name, major, version))
    return {"ok": True}


# ─── Failure management ──────────────────────────────────────────────────────


@router.post("/ponds/{name}/clear", dependencies=[auth.full])
def clear(name: str, request: Request, major: int | None = None, version: str | None = None):
    """Clear a failed Pond (the operator okay): resets its failure and unblocks downstream. No run."""
    _driver(request).clear(_resolve(request, name, major, version))
    return {"ok": True}


class _BudgetBody(BaseModel):
    immediate_retries: int = 0
    source_retries: int = 0


@router.post("/ponds/{name}/budget", dependencies=[auth.full])
def set_budget(
    name: str, body: _BudgetBody, request: Request,
    major: int | None = None, version: str | None = None,
):
    """Set the live retry budgets on a Pond (Ripple retries within a Run; Pond Runs retried on change)."""
    key = _resolve(request, name, major, version)
    if body.immediate_retries < 0 or body.source_retries < 0:
        raise HTTPException(status_code=422, detail="budgets must be non-negative")
    _driver(request).set_retry(key, body.immediate_retries, body.source_retries)
    return {"ok": True}


@router.get("/ponds/{name}/budget", dependencies=[auth.read])
def get_budget(name: str, request: Request, major: int | None = None, version: str | None = None):
    return _driver(request).retry_config(_resolve(request, name, major, version))


# ─── Duck config (prereqs D5) ────────────────────────────────────────────────


class _DuckBody(BaseModel):
    # Each field: None = keep the current override; ``clear`` drops the whole override (reverts to the
    # DECLARED pond.toml config, else the Catchment default). Effective config coalesces override ??
    # declared ?? default (plans/cloud-config.md).
    duck_target: str | None = None              # 'catchment' | a Duck Pool name | 'dedicated'
    dedicated_instance_type: str | None = None  # for duck_target='dedicated'
    dedicated_auto_stop: bool | None = None     # terminate the dedicated box on Pond-run completion
    flock_mode: str | None = None               # 'off' | 'upgrade' | 'always'
    flock_engine: str | None = None             # 'athena' | …
    oom_policy: str | None = None               # 'fail_up' | 'fail'
    clear: bool = False


@router.post("/ponds/{name}/duck", dependencies=[auth.full])
def set_duck(
    name: str, body: _DuckBody, request: Request,
    major: int | None = None, version: str | None = None,
):
    """Set the Pond's compute override (Duck target/size + Flock posture). Operator-owned; coalesces
    over the pond.toml-declared config, and persists across redeploys."""
    key = _resolve(request, name, major, version)
    try:
        _driver(request).set_duck(
            key, clear=body.clear, duck_target=body.duck_target,
            dedicated_instance_type=body.dedicated_instance_type,
            dedicated_auto_stop=body.dedicated_auto_stop, flock_mode=body.flock_mode,
            flock_engine=body.flock_engine, oom_policy=body.oom_policy,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None
    return {"ok": True}


@router.get("/ponds/{name}/duck", dependencies=[auth.read])
def get_duck(name: str, request: Request, major: int | None = None, version: str | None = None):
    """The Pond's effective compute config (override ?? declared ?? Catchment default)."""
    return _driver(request).duck_config(_resolve(request, name, major, version))


# ─── Cross-Catchment exposure (open / tap-on-get) ────────────────────────────


class _OpenBody(BaseModel):
    tap_on_get: bool = False


@router.post("/ponds/{name}/open", dependencies=[auth.full])
def open_pond(
    name: str, request: Request, body: _OpenBody = _OpenBody(),
    major: int | None = None, version: str | None = None,
):
    """Mark a Pond open — it accepts demand from any source (e.g. a downstream Catchment over a duct).
    With ``tap_on_get`` a read on the query route also fires a Tap (the snapshot is served first)."""
    _driver(request).set_pond_open(_resolve(request, name, major, version), body.tap_on_get)
    return {"ok": True}


@router.post("/ponds/{name}/close", dependencies=[auth.full])
def close_pond(name: str, request: Request, major: int | None = None, version: str | None = None):
    """Close a Pond — remove its open flag (and tap-on-get)."""
    _driver(request).unset_pond_open(_resolve(request, name, major, version))
    return {"ok": True}


# ─── Windows (batch-availability on Inlets) ──────────────────────────────────────


class _WindowBody(BaseModel):
    name: str
    start_anchor: str
    duration_seconds: int
    freq_unit: str
    freq_interval: int = 1
    valid_days: str | None = None
    until_time: str | None = None


@router.post("/ponds/{name}/windows", dependencies=[auth.full])
def add_window(
    name: str, body: _WindowBody, request: Request,
    major: int | None = None, version: str | None = None,
):
    key = _resolve(request, name, major, version)
    try:
        _driver(request).add_window(
            key, body.name, body.start_anchor, body.duration_seconds,
            body.freq_unit, body.freq_interval, body.valid_days, body.until_time,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"ok": True}


@router.get("/ponds/{name}/windows", dependencies=[auth.read])
def list_windows(name: str, request: Request, major: int | None = None, version: str | None = None):
    return {"windows": _driver(request).list_windows(_resolve(request, name, major, version))}


@router.post("/ponds/{name}/windows/{window_name}/remove", dependencies=[auth.full])
def remove_window(
    name: str, window_name: str, request: Request,
    major: int | None = None, version: str | None = None,
):
    key = _resolve(request, name, major, version)
    if not _driver(request).remove_window(key, window_name):
        raise HTTPException(status_code=404, detail=f"No window '{window_name}' on '{name}'")
    return {"ok": True}


# ─── Spouts (egress bindings) ────────────────────────────────────────────────────


class _SpoutBody(BaseModel):
    destination: str
    name: str | None = None
    table: str | None = None  # None = all of the Pond's published tables
    mode: str = "auto"


@router.post("/ponds/{name}/spouts", dependencies=[auth.full])
def add_spout(
    name: str, body: _SpoutBody, request: Request,
    major: int | None = None, version: str | None = None,
):
    """Bind a Spout to a Pond (egress its output to an external destination). 422 on a bad
    destination/mode or a duplicate name. Returns the Spout's (possibly generated) name."""
    driver = _driver(request)
    # Default the Spout's major to the SERVED major (plans/data-serving.md) — but only as a creation
    # default; the Spout is then pinned + independent of later serving promotions.
    if major is None and version is None:
        major = driver.served_major(name)
    key = _resolve(request, name, major, version)
    try:
        final = driver.add_spout(key, body.name, body.table, body.destination, body.mode)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"ok": True, "name": final}


class _SpoutTestBody(BaseModel):
    destination: str


def _probe_destination(destination: str) -> dict:
    """Resolve the driver for a destination and probe its connection — no data is written. Credentials
    (``${env:}``/``${secret:}``) resolve at probe time exactly as they would at egress. Returns
    ``{ok, error?}`` (a connection problem is data, not a 5xx). Runs in a threadpool (blocking I/O)."""
    import duckdb

    from ...egress.base import get_egress
    from ...egress.destination import DestinationError

    try:
        driver = get_egress(destination)  # validates scheme + credential syntax, picks the driver
    except (DestinationError, ValueError) as exc:
        return {"ok": False, "error": str(exc)}
    con = duckdb.connect()
    try:
        con.execute("SET TimeZone='UTC'")
        driver.test_connection(con)
    except Exception as exc:  # sanitised by the driver — safe to surface (never a credential)
        return {"ok": False, "error": str(exc)}
    finally:
        con.close()
    return {"ok": True}


@router.post("/ponds/{name}/spouts/test", dependencies=[auth.full])
async def test_spout(name: str, body: _SpoutTestBody):
    """Probe a Spout destination's connection/credentials before binding it (the add form's *Test*
    button). Writes no data; returns ``{ok}`` or ``{ok: false, error}``. The Pond ``name`` is unused —
    the probe is destination-only — but the route lives under the spout surface, full-gated."""
    return await run_in_threadpool(_probe_destination, body.destination)


@router.get("/ponds/{name}/spouts", dependencies=[auth.full])
def list_spouts(name: str, request: Request, major: int | None = None, version: str | None = None):
    return {"spouts": _driver(request).list_spouts(_resolve(request, name, major, version))}


@router.post("/ponds/{name}/spouts/{spout_name}/remove", dependencies=[auth.full])
def remove_spout(
    name: str, spout_name: str, request: Request,
    major: int | None = None, version: str | None = None,
):
    key = _resolve(request, name, major, version)
    if not _driver(request).remove_spout(key, spout_name):
        raise HTTPException(status_code=404, detail=f"No spout '{spout_name}' on '{name}'")
    return {"ok": True}


# A Spout's Control set (its standing Wake) + resync. Demand verbs (tap/wave/pulse/tide) don't apply.
_SPOUT_ACTIONS = {
    "wake": "spout_wake", "force": "spout_force", "sleep": "spout_sleep",
    "kill": "spout_kill", "clear": "spout_clear", "resync": "resync_spout",
}


@router.post("/ponds/{name}/spouts/{spout_name}/{action}", dependencies=[auth.full])
def control_spout(
    name: str, spout_name: str, action: str, request: Request,
    major: int | None = None, version: str | None = None,
):
    """Control a Spout: wake/force re-arm its standing Wake, sleep/kill disarm it, clear resets a fault,
    resync forces a full re-egress."""
    method = _SPOUT_ACTIONS.get(action)
    if method is None:
        raise HTTPException(status_code=404, detail=f"unknown spout action '{action}'")
    key = _resolve(request, name, major, version)
    if not getattr(_driver(request), method)(key, spout_name):
        raise HTTPException(status_code=404, detail=f"No spout '{spout_name}' on '{name}'")
    return {"ok": True}
