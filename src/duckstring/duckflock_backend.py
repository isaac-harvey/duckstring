"""DuckFlock execution routing — duckstring as the DuckFlock client SDK (completion-arc Workstream M).

An always-on Ripple host decides, **before** running, whether a ripple is cheap enough to run on
duckstring's own engine (the semantic oracle) or worth offloading to DuckFlock (the serverless
executor). The decision is:

1. **Capture** the ripple's plan IR (:mod:`duckstring.trickle.capture`). Anything non-capturable
   (raw ``con`` access, callable metrics, an Ibis ``.sql()``) ⇒ run classic.
2. **Quote** — shell out to ``duckflock quote --plan plan.json`` (DuckFlock M3), which emits
   ``{route, estimate, chosen_modes, why}``. Quote authority stays with the DuckFlock driver; the
   client quote is *routing advice*.
3. **Route** — ``local`` → execute the ripple classically (duckstring is the oracle); ``duckflock``
   → ``POST /jobs`` + poll ``GET /jobs/{id}`` and surface the result doc.

**Additive & opt-in — zero behaviour change unless configured.** With no DuckFlock endpoint the
backend is disabled and every ripple runs classic. **Failure posture:** *any* DuckFlock-side failure
(quote error, submit error, a failed job) falls back to classic local execution with a loud warning —
the pond is never down because the offload engine is. **Telemetry completeness:** a locally-routed
plan still records a lightweight RunRecord (via ``duckflock quote --record``) so DuckFlock's
estimate-calibration and mode-stickiness see the local runs too.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import timezone

from .trickle.capture import NonCapturable, capture_plan, envelope

log = logging.getLogger("duckstring.duckflock")

NEVER_ISO = "0001-01-01T00:00:00+00:00"  # duckstring's NEVER sentinel as isoformat (matches duckflock core)
TENANT_HEADER = "x-duckflock-tenant"


@dataclass
class DuckflockConfig:
    """Where and how to reach DuckFlock. **Absent ``endpoint`` ⇒ disabled** (classic path only), so a
    default-constructed config is a no-op. Populate from the environment with :meth:`from_env`."""

    endpoint: str | None = None          # e.g. "http://127.0.0.1:7877" — the Cloud API surface
    tenant: str = "default"
    express_root: str | None = None       # the tenant's Express root (planner/quote can-distribute signal)
    quote_bin: str = "duckflock"           # the DuckFlock CLI (for `quote`); found on PATH by default
    major: int = 1                         # this pond's deployed major line (envelope `pond.major`)
    auth_token: str | None = None          # optional Bearer token the API `serve` may require
    record_local: bool = True              # `duckflock quote --record` for local-route telemetry
    poll_interval_s: float = 0.5
    poll_timeout_s: float = 300.0

    @property
    def enabled(self) -> bool:
        return bool(self.endpoint)

    @classmethod
    def from_env(cls, env: dict | None = None) -> "DuckflockConfig":
        """Read ``DUCKSTRING_DUCKFLOCK_{ENDPOINT,TENANT,EXPRESS_ROOT,QUOTE_BIN,MAJOR,TOKEN}``. A missing
        ``ENDPOINT`` leaves the backend disabled (classic execution, no behaviour change)."""
        e = env if env is not None else os.environ
        return cls(
            endpoint=e.get("DUCKSTRING_DUCKFLOCK_ENDPOINT") or None,
            tenant=e.get("DUCKSTRING_DUCKFLOCK_TENANT", "default"),
            express_root=e.get("DUCKSTRING_DUCKFLOCK_EXPRESS_ROOT") or None,
            quote_bin=e.get("DUCKSTRING_DUCKFLOCK_QUOTE_BIN", "duckflock"),
            major=int(e.get("DUCKSTRING_DUCKFLOCK_MAJOR", "1")),
            auth_token=e.get("DUCKSTRING_DUCKFLOCK_TOKEN") or None,
            record_local=e.get("DUCKSTRING_DUCKFLOCK_RECORD_LOCAL", "1") != "0",
        )


@dataclass
class Outcome:
    """What :func:`execute` did. ``route`` ∈ ``classic`` (backend off / non-capturable), ``local``
    (quoted local), ``duckflock`` (submitted, succeeded), ``duckflock->classic`` (submitted-then-fell-
    back), ``quote->classic`` (quote failed → classic). ``executed_locally`` is True whenever the
    ripple ran on duckstring's own engine this call; ``ripple_result`` is what the ripple function
    returned in that case; ``result_doc`` is the DuckFlock job result when the remote path succeeded."""

    route: str
    executed_locally: bool
    ripple_result: object = None
    result_doc: dict | None = None
    quote: dict | None = None
    warnings: list[str] = field(default_factory=list)


def _iso(dt) -> str:
    """``isoformat()`` in UTC; ``NEVER`` (or ``None``) → the canonical sentinel string."""
    from .engine.core import NEVER

    if dt is None or dt == NEVER:
        return NEVER_ISO
    return dt.astimezone(timezone.utc).isoformat()


def build_plan(pond, run_python, *, source_catalog, own_location: str, tenant: str, major: int,
               job_id: str, express_root: str | None = None) -> dict:
    """Capture ``run_python`` into the full ``duckflock_plan: 1`` document for ``pond``'s current epoch.
    Raises :class:`~duckstring.trickle.capture.NonCapturable` for a ripple the recorder can't express."""
    body = capture_plan(run_python, source_catalog=source_catalog, own_location=own_location)
    cfg = {"refresh": False, "keep_warm_ttl_s": 0, "limits": {}}
    if express_root is not None:
        cfg["express_location"] = express_root
    return envelope(
        body, job_id=job_id, tenant=tenant,
        pond={"name": pond.name, "major": major, "version": pond.version},
        epoch={"f": _iso(pond.f), "previous_f": _iso(pond.previous_f)},
        config=cfg,
    )


def default_quote(plan: dict, config: DuckflockConfig) -> dict:
    """Shell out to ``duckflock quote --plan <tmp> [--record]`` and parse its ``{route, …}`` JSON.
    Raises on a non-zero exit / unparseable output (the caller treats that as "route local")."""
    with tempfile.NamedTemporaryFile("w", suffix=".plan.json", delete=False) as fh:
        json.dump(plan, fh)
        path = fh.name
    try:
        cmd = [config.quote_bin, "quote", "--plan", path]
        if config.record_local and config.express_root:
            cmd.append("--record")
        env = dict(os.environ)
        if config.express_root:
            env["DUCKFLOCK_EXPRESS_ROOT"] = config.express_root
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60, env=env)
        if proc.returncode != 0:
            raise RuntimeError(f"duckflock quote exited {proc.returncode}: {proc.stderr.strip()[:400]}")
        return json.loads(proc.stdout)
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


def default_submit(plan: dict, config: DuckflockConfig) -> dict:
    """Submit ``plan`` to the DuckFlock API (``POST {endpoint}/jobs`` → 202, then poll
    ``GET {endpoint}/jobs/{id}`` until ``status`` is ``success``/``failed``), returning the result doc.
    Raises on a transport error or a poll timeout (the caller falls back to classic)."""
    body = json.dumps(plan).encode()
    headers = {"content-type": "application/json"}
    if config.auth_token:
        headers["authorization"] = f"Bearer {config.auth_token}"
    submitted = _http_json(f"{config.endpoint.rstrip('/')}/jobs", data=body, headers=headers)
    job_id = submitted.get("job_id") or plan["job_id"]

    poll_headers = {TENANT_HEADER: config.tenant}
    if config.auth_token:
        poll_headers["authorization"] = f"Bearer {config.auth_token}"
    deadline = time.monotonic() + config.poll_timeout_s
    url = f"{config.endpoint.rstrip('/')}/jobs/{job_id}"
    while True:
        doc = _http_json(url, headers=poll_headers)
        if doc.get("status") in ("success", "failed", "cancelled"):
            return doc
        if time.monotonic() >= deadline:
            raise TimeoutError(f"duckflock job {job_id} did not finish within {config.poll_timeout_s}s")
        time.sleep(config.poll_interval_s)


def _http_json(url: str, *, data: bytes | None = None, headers: dict | None = None) -> dict:
    req = urllib.request.Request(url, data=data, headers=headers or {}, method="POST" if data else "GET")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:  # a 4xx/5xx still carries a JSON body we want to surface
        raise RuntimeError(f"{url} → HTTP {exc.code}: {exc.read().decode()[:400]}") from exc


def execute(config: DuckflockConfig, pond, run_python, *, source_catalog, own_location: str = "__OWN__",
            job_id: str | None = None, quote_fn=None, submit_fn=None) -> Outcome:
    """Route and run ``run_python`` for ``pond``'s current epoch. ``source_catalog(ref) ->
    {location, mode, pk, floor, f}`` supplies each source's storage facts (see
    :func:`duckstring.trickle.capture.capture_plan`). ``quote_fn``/``submit_fn`` default to the
    ``duckflock`` CLI / HTTP API and are injectable for testing. Never raises for a DuckFlock-side
    problem — it degrades to classic local execution with a warning."""
    quote_fn = quote_fn or default_quote
    submit_fn = submit_fn or default_submit

    if not config.enabled:
        return _classic(pond, run_python, "classic", [])

    warnings: list[str] = []
    try:
        plan = build_plan(
            pond, run_python, source_catalog=source_catalog, own_location=own_location,
            tenant=config.tenant, major=config.major, job_id=job_id or _job_id(pond),
            express_root=config.express_root,
        )
    except NonCapturable as exc:
        return _classic(pond, run_python, "classic", _warn(warnings, f"ripple not capturable ({exc}) — running classic"))

    try:
        quote = quote_fn(plan, config)
    except Exception as exc:  # a broken/absent quoter must never take the pond down — route local
        _warn(warnings, f"duckflock quote failed ({exc}) — routing local")
        out = _classic(pond, run_python, "quote->classic", warnings)
        return out

    route = (quote or {}).get("route", "duckflock")
    if route != "duckflock":
        out = _classic(pond, run_python, "local", warnings)
        out.quote = quote
        return out

    try:
        doc = submit_fn(plan, config)
        if doc.get("status") == "success":
            return Outcome("duckflock", executed_locally=False, result_doc=doc, quote=quote, warnings=warnings)
        _warn(warnings, f"duckflock job did not succeed ({doc.get('status')}: {doc.get('error')}) — falling back to classic")
    except Exception as exc:
        _warn(warnings, f"duckflock submit failed ({exc}) — falling back to classic")
    out = _classic(pond, run_python, "duckflock->classic", warnings)
    out.quote = quote
    return out


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
