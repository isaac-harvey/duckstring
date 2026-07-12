"""DuckFlock execution routing — the embedded-driver backend (prereqs D4, embedded revision).

The DuckFlock driver is **co-resident with the Duck**: there is no HTTP client, no submit/poll —
running a plan is a subprocess call to the ``duckflock`` CLI. Per capturable ripple:

1. **Capture** the ripple's plan IR (:mod:`duckstring.trickle.capture`). Anything non-capturable
   (raw ``con`` access, callable metrics, an Ibis ``.sql()``) ⇒ run classic.
2. **Quote** — ``duckflock quote --plan plan.json`` emits ``{route, estimate, chosen_modes, why}``.
   Quote authority stays with the DuckFlock driver; the client quote is *routing advice*. A
   locally-routed plan still records a RunRecord (``--record``) so estimate calibration and mode
   stickiness see local runs too.
3. **Route** — ``local`` → execute the ripple classically (duckstring is the semantic oracle);
   ``duckflock`` → ``duckflock run --plan plan.json --result …`` as a subprocess and read the
   result doc. Driver-local statements execute right there (same process tree, same pod);
   comprehensive/over-envelope statements fan out to the Flock when distribution is configured.

**Config is environment-shaped and shared with the driver** (the subprocess inherits it):
``DUCKFLOCK_BIN`` — path to the driver CLI, **the enablement gate** (absent ⇒ classic path, zero
behaviour change); ``DUCKFLOCK_EXPRESS_ROOT`` — the scratch fabric root (absent ⇒ single-node
driver execution); ``DUCKFLOCK_TENANT``; plus whatever the driver itself reads
(``DUCKFLOCK_INVOKER``, ``DUCKFLOCK_WORKER_FUNCTION``, ``DUCKFLOCK_HTTPFS_EXT``, …).
**Escalation-disabled mode** ("local" Ducks, ``DUCKSTRING_DUCK_FLOCK=off``) is the same path with
distribution off: the plan carries an **empty** ``config.express_location`` (the existing
``--single-node`` semantics — empty, not absent, so the driver's own env fallback can't re-enable
distribution).

**Failure posture:** *any* driver failure (quote error, run error, a failed job) falls back to
classic local execution with a loud warning — the pond is never down because the offload engine
is; same-``f`` replay idempotence makes the retry safe.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import tempfile
from dataclasses import dataclass, field
from datetime import timezone
from pathlib import Path

from .trickle.capture import NonCapturable, capture_plan, envelope

log = logging.getLogger("duckstring.duckflock")

NEVER_ISO = "0001-01-01T00:00:00+00:00"  # duckstring's NEVER sentinel as isoformat (matches duckflock core)


@dataclass
class DuckflockConfig:
    """How to run the co-resident DuckFlock driver. **Absent ``bin`` ⇒ disabled** (classic path
    only), so a default-constructed config is a no-op. Populate from the environment with
    :meth:`from_env`."""

    bin: str | None = None                 # the duckflock CLI — presence enables the backend
    tenant: str = "default"
    express_root: str | None = None        # the tenant's Express root; absent ⇒ single-node driver runs
    flock: bool = True                     # False = escalation disabled (empty express_location in plans)
    major: int = 1                         # this pond's deployed major line (envelope `pond.major`)
    record_local: bool = True              # `duckflock quote --record` for local-route telemetry
    quote_timeout_s: float = 60.0
    run_timeout_s: float = 3600.0

    @property
    def enabled(self) -> bool:
        return bool(self.bin)

    @property
    def express_location(self) -> str:
        """What a plan's ``config.express_location`` carries: the Express root when distribution is
        on, else the **empty string** (never absent — absent would let the driver's own
        ``DUCKFLOCK_EXPRESS_ROOT`` fallback re-enable distribution behind duckstring's back)."""
        return self.express_root if (self.flock and self.express_root) else ""

    @classmethod
    def from_env(cls, env: dict | None = None) -> "DuckflockConfig":
        """Read ``DUCKFLOCK_{BIN,TENANT,EXPRESS_ROOT,RECORD_LOCAL}`` + ``DUCKSTRING_DUCK_FLOCK``.
        A missing ``DUCKFLOCK_BIN`` leaves the backend disabled (classic execution, no behaviour
        change)."""
        e = env if env is not None else os.environ
        return cls(
            bin=e.get("DUCKFLOCK_BIN") or None,
            tenant=e.get("DUCKFLOCK_TENANT", "default"),
            express_root=e.get("DUCKFLOCK_EXPRESS_ROOT") or None,
            flock=e.get("DUCKSTRING_DUCK_FLOCK", "on").lower() not in ("off", "0", "false"),
            record_local=e.get("DUCKFLOCK_RECORD_LOCAL", "1") != "0",
        )


@dataclass
class Outcome:
    """What :func:`execute` did. ``route`` ∈ ``classic`` (backend off / non-capturable), ``local``
    (quoted local), ``duckflock`` (driver ran it, succeeded), ``duckflock->classic`` (driver-ran-
    then-fell-back), ``quote->classic`` (quote failed → classic). ``executed_locally`` is True
    whenever the ripple ran on duckstring's own engine this call; ``ripple_result`` is what the
    ripple function returned in that case; ``result_doc`` is the DuckFlock job result when the
    driver path succeeded; ``plan`` is the captured plan document (present whenever capture
    succeeded)."""

    route: str
    executed_locally: bool
    ripple_result: object = None
    result_doc: dict | None = None
    quote: dict | None = None
    plan: dict | None = None
    warnings: list[str] = field(default_factory=list)


def _iso(dt) -> str:
    """``isoformat()`` in UTC; ``NEVER`` (or ``None``) → the canonical sentinel string."""
    from .engine.core import NEVER

    if dt is None or dt == NEVER:
        return NEVER_ISO
    return dt.astimezone(timezone.utc).isoformat()


def build_plan(pond, run_python, *, source_catalog, own_location: str, tenant: str, major: int,
               job_id: str, express_location: str = "", refresh: bool = False) -> dict:
    """Capture ``run_python`` into the full ``duckflock_plan: 1`` document for ``pond``'s current epoch.
    Raises :class:`~duckstring.trickle.capture.NonCapturable` for a ripple the recorder can't express.
    ``express_location`` is carried verbatim (empty string = single-node, distribution off);
    ``refresh`` marks a wipe-and-rebuild run (DuckFlock treats every read as bootstrap)."""
    body = capture_plan(run_python, source_catalog=source_catalog, own_location=own_location)
    cfg = {"refresh": bool(refresh), "keep_warm_ttl_s": 0, "limits": {},
           "express_location": express_location}
    return envelope(
        body, job_id=job_id, tenant=tenant,
        pond={"name": pond.name, "major": major, "version": pond.version},
        epoch={"f": _iso(pond.f), "previous_f": _iso(pond.previous_f)},
        config=cfg,
    )


def _plan_tempfile(plan: dict) -> str:
    with tempfile.NamedTemporaryFile("w", suffix=".plan.json", delete=False) as fh:
        json.dump(plan, fh)
        return fh.name


def default_quote(plan: dict, config: DuckflockConfig) -> dict:
    """Shell out to ``duckflock quote --plan <tmp> [--record]`` and parse its ``{route, …}`` JSON.
    Raises on a non-zero exit / unparseable output (the caller treats that as "route local")."""
    path = _plan_tempfile(plan)
    try:
        cmd = [config.bin, "quote", "--plan", path]
        if config.record_local and config.express_root:
            cmd.append("--record")
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=config.quote_timeout_s)
        if proc.returncode != 0:
            raise RuntimeError(f"duckflock quote exited {proc.returncode}: {proc.stderr.strip()[:400]}")
        return json.loads(proc.stdout)
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


def default_run(plan: dict, config: DuckflockConfig) -> dict:
    """Run the plan through the co-resident driver — ``duckflock run --plan <tmp> --result <tmp>``
    as a subprocess (it inherits the ``DUCKFLOCK_*`` environment: invoker, worker function, creds)
    — and return the result doc. A **failed job** exits non-zero but still writes the doc; that doc
    is returned (the caller reads ``status`` and falls back). Raises only when there is no doc to
    read (crash, timeout, unparseable output)."""
    path = _plan_tempfile(plan)
    result_path = path + ".result.json"
    try:
        proc = subprocess.run([config.bin, "run", "--plan", path, "--result", result_path],
                              capture_output=True, text=True, timeout=config.run_timeout_s)
        try:
            return json.loads(Path(result_path).read_text())
        except (OSError, json.JSONDecodeError):
            raise RuntimeError(
                f"duckflock run exited {proc.returncode} with no result doc: {proc.stderr.strip()[:400]}"
            ) from None
    finally:
        for p in (path, result_path):
            try:
                os.unlink(p)
            except OSError:
                pass


def execute(config: DuckflockConfig, pond, run_python, *, source_catalog, own_location: str = "__OWN__",
            job_id: str | None = None, quote_fn=None, run_fn=None, prepare_remote=None,
            refresh: bool = False) -> Outcome:
    """Route and run ``run_python`` for ``pond``'s current epoch. ``source_catalog(ref) ->
    {location, mode, pk, floor, f}`` supplies each source's storage facts (see
    :func:`duckstring.trickle.capture.capture_plan`). ``quote_fn``/``run_fn`` default to the
    ``duckflock`` CLI subprocess calls and are injectable for testing. ``prepare_remote(plan)`` runs
    just before the driver does (the executor's seam for seeding the outputs' prior published state
    into the plan's own location); its failure falls back like any other driver failure. Never
    raises for a DuckFlock-side problem — it degrades to classic local execution with a warning."""
    quote_fn = quote_fn or default_quote
    run_fn = run_fn or default_run

    if not config.enabled:
        return _classic(pond, run_python, "classic", [])

    warnings: list[str] = []
    try:
        plan = build_plan(
            pond, run_python, source_catalog=source_catalog, own_location=own_location,
            tenant=config.tenant, major=config.major, job_id=job_id or _job_id(pond),
            express_location=config.express_location, refresh=refresh,
        )
    except NonCapturable as exc:
        return _classic(pond, run_python, "classic", _warn(warnings, f"ripple not capturable ({exc}) — running classic"))
    except Exception as exc:
        # A capture bug (or user code the recorder trips over in an unforeseen way) must degrade,
        # not fail the ripple: classic execution is the semantic oracle, and a genuine user error
        # will raise identically there — attributed to the ripple, as it should be.
        return _classic(pond, run_python, "classic",
                        _warn(warnings, f"capture failed unexpectedly ({exc!r}) — running classic"))

    try:
        quote = quote_fn(plan, config)
    except Exception as exc:  # a broken/absent quoter must never take the pond down — route local
        _warn(warnings, f"duckflock quote failed ({exc}) — routing local")
        out = _classic(pond, run_python, "quote->classic", warnings)
        out.plan = plan
        return out

    route = (quote or {}).get("route", "duckflock")
    if route != "duckflock":
        out = _classic(pond, run_python, "local", warnings)
        out.quote = quote
        out.plan = plan
        return out

    try:
        if prepare_remote is not None:
            prepare_remote(plan)
        doc = run_fn(plan, config)
        if doc.get("status") == "success":
            return Outcome("duckflock", executed_locally=False, result_doc=doc, quote=quote,
                           plan=plan, warnings=warnings)
        _warn(warnings, f"duckflock job did not succeed ({doc.get('status')}: {doc.get('error')}) — falling back to classic")
    except Exception as exc:
        _warn(warnings, f"duckflock run failed ({exc}) — falling back to classic")
    out = _classic(pond, run_python, "duckflock->classic", warnings)
    out.quote = quote
    out.plan = plan
    return out


def seed_scratch(own_dir, scratch, outputs) -> None:
    """Seed a routed run's **scratch publish dir** with each output's prior published collections, so the
    remote engine hydrates the same prior state a long-lived registry would hold. Copies only the output
    tables' tiers (wholesale file / parts / ``__changelog`` / ``__band`` / ``__base`` / ``__droplog`` /
    the Extension-1 ``state/`` snapshots) plus a sidecar filtered to those entries — proportional to the
    output, not the pond. The scratch dir is recreated from empty each call (a stale seed would replay).

    Why a scratch dir at all: DuckFlock publishes as it completes, but a classic Pond Run publishes
    **once, at run end, behind the contract gate**. Routing an individual ripple straight at the real
    data dir would make its output visible mid-run (and skip the gate); instead the driver publishes
    to scratch, the executor hydrates scratch back into the registry, and the run's normal export ships
    it with everything else."""
    import shutil

    from . import trickle_io as trickle

    own_dir, scratch = Path(own_dir), Path(scratch)
    shutil.rmtree(scratch, ignore_errors=True)
    scratch.mkdir(parents=True, exist_ok=True)
    sidecar = trickle.load_sidecar(own_dir)
    keep: dict = {}
    for t in outputs:
        if (own_dir / f"{t}.parquet").exists():
            shutil.copy2(own_dir / f"{t}.parquet", scratch / f"{t}.parquet")
        for d in (t, trickle.changelog_name(t), trickle.warm_name(t),
                  trickle.base_dir_name(t), f"{t}{trickle.DROPLOG_SUFFIX}"):
            if (own_dir / d).is_dir():
                shutil.copytree(own_dir / d, scratch / d)
        for kind in ("agg", "acc"):
            p = own_dir / "state" / kind / t
            if p.is_dir():
                shutil.copytree(p, scratch / "state" / kind / t)
        if t in sidecar:
            keep[t] = sidecar[t]
    state = {k: v for k, v in (sidecar.get("state") or {}).items() if k.split("/", 1)[1] in set(outputs)}
    if state:
        keep["state"] = state
    if keep:
        trickle.write_sidecar(scratch, keep)


def _classic(pond, run_python, route: str, warnings: list[str]) -> Outcome:
    """Run the ripple on duckstring's own engine (the semantic oracle) and return its result."""
    result = run_python(pond)
    return Outcome(route, executed_locally=True, ripple_result=result, warnings=warnings)


def _warn(warnings: list[str], msg: str) -> list[str]:
    log.warning(msg)
    warnings.append(msg)
    return warnings


def _job_id(pond) -> str:
    return f"j-{pond.name}-{_iso(pond.f).replace(':', '_')}"
