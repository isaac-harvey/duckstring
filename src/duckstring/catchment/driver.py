"""The Catchment driver: the freshness brain + Duck coordinator.

Holds the in-memory :class:`~duckstring.engine.EngineState` (full Ponds + Ripples, pull + push),
loaded from SQLite at startup and write-through-persisted per event. It is event-driven:

* trigger calls (``tap``/``pulse``/``wave``/``tide``/``stop``) mutate the engine, then ``_process``
  runs ``sentinel`` and dispatches each emitted ``BeginRun`` to the target Pond's Duck (spawning one
  if needed) as a queued job.
* Duck events (``on_event``) feed ``complete_ripple``, which drives the ripple pull cascade →
  more ``BeginRun``s; run history is written to ``pond_run`` / ``ripple_run``.
* ``scheduler_tick`` (called on a timer) runs ``tick`` for Tide/window clocks.

Ponds are keyed by ``"{name}@{major}"`` in the engine — each deployed major line is an independent
Pond instance — and Ripples by ``"{pond_key}.{ripple}"``. A ``threading.RLock`` guards all state;
SQLite is the durable mirror, the per-Pond ``pond.db`` ledgers the fallback.
"""

from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timedelta, timezone

from ..engine import (
    NEVER,
    EngineState,
    Pond,
    PondState,
    Ripple,
    RippleState,
    Trigger,
    Window,
    block_on_missing_asset,
    clear_missing_asset,
    clear_pond,
    complete_ripple,
    derive_blocked,
    drain_begin_runs,
    drain_passes,
    fail_pond,
    fail_ripple,
    force_pond,
    kill_pond,
    next_wake,
    pulse_pond,
    refresh_pond,
    repair_pond,
    sentinel,
    sleep_pond,
    tap_pond,
    tick,
    wake_pond,
)
from ..keys import pond_key

# A Duck is presumed dead if it holds an in-flight Run but hasn't contacted the Catchment within this
# window (the secondary, transport-level signal; process-liveness is the primary one). Comfortably
# above the Duck's long-poll timeout so a healthy hold is never mistaken for death.
_DUCK_DEAD_AFTER = timedelta(seconds=60)

# Keep an idle Duck warm for this long before reaping it. Reaping the instant a Pond goes idle, then
# respawning on the next run, races: a Pond re-armed in the window between the shutdown being sent and
# the Duck exiting ends up in-flight with a dying Duck → a spurious "Duck not running" failure. The
# grace means a Pond running on any sub-grace cadence is never reaped (so never races); truly idle
# Ponds still reap. A duct exposed this by driving _process — hence _reap_idle — far more often.
_REAP_GRACE = timedelta(seconds=30)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _split_scope(scope: str | None) -> tuple[str | None, int | None]:
    """An alert-channel scope string → ``(scope_name, scope_major)``. ``"name@major"`` → one line;
    ``"name"`` → ``(name, None)`` (a bare name — ``add_channel`` resolves it to the highest deployed major);
    ``None``/``""`` → catchment-wide."""
    if not scope:
        return None, None
    base, sep, mj = scope.rpartition("@")
    if sep and base and mj.isdigit():
        return base, int(mj)
    return scope, None


def _format_scope(scope_name: str | None, scope_major: int | None) -> str | None:
    """The inverse of :func:`_split_scope` — ``(name, major)`` → ``"name@major"``; ``(name, None)`` →
    ``"name"``; ``(None, _)`` → ``None`` (catchment-wide)."""
    if scope_name is None:
        return None
    return f"{scope_name}@{scope_major}" if scope_major is not None else scope_name


# ─── Repair scope graph helpers (D3 — see plans/refresh.md) ────────────────────


def _reach(start: str, children: dict[str, set[str]], within: set[str] | None = None) -> set[str]:
    """Forward reachability from ``start`` (excluding itself). ``within`` restricts traversal to a subset
    (edges only into nodes in ``within``) — used to ask "reachable *through the selection*"."""
    seen: set[str] = set()
    stack = [start]
    while stack:
        for c in children.get(stack.pop(), ()):
            if (within is None or c in within) and c not in seen:
                seen.add(c)
                stack.append(c)
    return seen


def _descendants(seeds: list[str], children: dict[str, set[str]]) -> set[str]:
    out: set[str] = set()
    for s in seeds:
        out |= _reach(s, children)
    return out


def _connectivity_gap(scope: set[str], children: dict[str, set[str]]) -> tuple[str, str] | None:
    """The relaxed connectivity rule: any two selected Ponds connected in the full graph must stay
    connected **within the selection**. Returns the first ``(X, Z)`` where ``Z`` is reachable from ``X``
    in the full graph but not via a path inside ``scope`` (a skipped intermediate), else ``None``."""
    for x in scope:
        reachable_in_scope = _reach(x, children, within=scope)
        for z in _reach(x, children):
            if z in scope and z not in reachable_in_scope:
                return (x, z)
    return None


def _topo_order(scope: set[str], parents: dict[str, set[str]]) -> list[str]:
    """A topological order of the induced subgraph (parents = in-scope sources). Deterministic (sorted)."""
    done: list[str] = []
    seen: set[str] = set()
    remaining = set(scope)
    while remaining:
        ready = sorted(k for k in remaining if parents[k] <= seen)
        if not ready:  # a cycle is impossible (the pond graph is a DAG), but guard anyway
            ready = sorted(remaining)
        for k in ready:
            done.append(k)
            seen.add(k)
            remaining.discard(k)
    return done


# The bulk-operation vocabulary (see plans/selector_ui.md), in application-precedence order: quiesce
# (kill/sleep), destroy state (reset), trim history (wipe), retire (remove), then recover (clear/repair/
# refresh). Reset and Wipe are independent steps (Wipe-only — clear history without a data scrub — is a
# valid case). Repair is a set-op (the connected-set rebuild); every other op is applied per Pond.
BATCH_OPS = ("kill", "sleep", "reset", "wipe", "remove", "clear", "repair", "refresh")
IRREVERSIBLE_OPS = frozenset({"reset", "wipe", "remove"})

# The Duck preset-size vocabulary (prereqs D5). Presets only — never raw instance shapes; what a
# size means physically is the launcher's business (the local subprocess ignores it).
FLOCK_MODES = ("off", "upgrade", "always")
OOM_POLICIES = ("fail_up", "fail")
POOL_PROVIDERS = ("fargate", "ec2")

# Built-in preset Duck Pools (plans/cloud-config.md §4b): Fargate task sizes, always available so a
# `pond.toml duck = "M"` works with zero pool setup — and DSC ships the SAME names backed by its own
# serverless pools, so a project transfers seamlessly (only the backend differs). Names are reserved.
PRESET_POOLS = {
    "S": {"cpu": 512, "memory": 2048},
    "M": {"cpu": 1024, "memory": 4096},
    "L": {"cpu": 2048, "memory": 8192},
    "XL": {"cpu": 4096, "memory": 16384},
}


def _preset_pool(name: str) -> dict:
    p = PRESET_POOLS[name]
    return {"name": name, "provider": "fargate", "instance_type": None, "cpu": p["cpu"],
            "memory": p["memory"], "min_instances": 0, "max_instances": 1, "idle_timeout": None,
            "keep_warm": 0, "region": None, "managed": True}


def _duck_override_row(row) -> dict:
    """A pond_duck row → the nullable override dict (plans/cloud-config.md). Sizing is concrete now
    (a pool / dedicated instance type), so there is no abstract size field."""
    keys = ("duck_target", "dedicated_instance_type", "dedicated_auto_stop",
            "flock_mode", "flock_engine", "oom_policy")
    if row is None:
        return {k: None for k in keys}
    o = dict(zip(keys, row, strict=True))
    o["dedicated_auto_stop"] = None if o["dedicated_auto_stop"] is None else bool(o["dedicated_auto_stop"])
    return o


# OpenLineage identifiers (plans/lineage.md Phase 4) — the standard event/schema URLs.
_OL_PRODUCER = "https://github.com/duckstring/duckstring"
_OL_SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/RunEvent"
_OL_SCHEMA_FACET = "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json"


class Driver:
    def __init__(self, db, root, base_url: str | None, launcher, data_root: str | None = None):
        self.db = db
        self.root = root
        # The data-plane root (object store / Volume / path); None = under the state root. Threaded into
        # every ``pond_data_dir`` lookup the Catchment makes for sidecar reads / table listing / egress.
        self.data_root = data_root
        self.base_url = base_url
        self.launcher = launcher
        self.lock = threading.RLock()
        self.state = EngineState()
        # All dicts below are keyed by the pond key "{name}@{major}" — one entry per major line.
        self.meta: dict[str, dict] = {}  # key -> {name, major, version_id, version, source_path, ...}
        self.jobs: dict[str, list[dict]] = {}  # key -> queued Duck commands
        self.last_seen: dict[str, datetime] = {}  # key -> last Duck contact (jobs poll / event)
        self._idle_since: dict[str, datetime] = {}  # key -> when the Pond went idle (reap grace clock)
        # Pond Draw transfers awaiting the poller: (pond_key, F). A Draw run is not dispatched to a
        # Duck — the poller performs the parquet fetch out-of-lock, then reports completion.
        self._pending_transfers: list[tuple[str, datetime]] = []
        # An in-progress repair plan (D3) or None: {scope, parents (in-scope), done, released}. The Driver
        # walks it imperatively in topological order, releasing each node once its in-scope parents finish.
        self._repair: dict | None = None
        # Set by the app to a thread-safe callback that wakes the duct poller. Called from _process on
        # demand-bearing operations (tap/pulse/wave/…/Duck events) so a Draw solicits its upstream
        # immediately, not on the next poll. NOT called from the poller's own observe/transfer paths.
        self._notify_cb = None
        # Wakes the egress worker (a Pond Run published → its Spouts may have work). Same cross-thread
        # shape as _notify_cb; None when no worker is attached (NoopLauncher tests).
        self._egress_cb = None
        # Spout runs the engine dispatched (instead of to a Duck), awaiting the egress worker — mirrors
        # _pending_transfers for Draws. (spout_key, f). In-memory: a crash re-dispatches on restart.
        self._pending_egress: list[tuple[str, datetime]] = []
        # Alerts (see plans/alerts.md): a thread-safe callback that wakes the alert worker when a delivery
        # is enqueued (None = no worker, e.g. NoopLauncher tests). _alerted_failures tracks keys we've fired
        # a failure/contract/spout alert for → their recovery is emitted centrally in _process when they
        # clear; _stale_breached tracks (channel_id, pond_key) currently over their freshness SLA.
        self._alert_cb = None
        self._alerted_failures: dict[str, tuple] = {}  # key → (kind, scope_pond, scope_major, f, title, message)
        self._renotify_channels = False  # any enabled channel with a re-notify interval (refreshed per sweep)
        self._stale_breached: set[tuple[int, str]] = set()
        # Per-Pond compute config (plans/cloud-config.md). The effective value coalesces:
        # override (pond_duck) ?? declared (pond_version, from pond.toml) ?? Catchment default (env).
        # A NULL override field inherits; loaded from pond_duck / pond_version in reload().
        self.duck_overrides: dict[str, dict] = {}   # key → nullable override columns
        self.duck_declared: dict[str, dict] = {}    # key → declared columns off the selected pond_version
        self.duck_pool_names: set[str] = set()      # defined Duck Pools (for the unknown-pool→catchment fallback)
        # Monotonic counter bumped on every state change — the UI long-polls /api/status against it, so
        # the display updates the instant the engine state moves rather than on a fixed timer.
        self.state_version = 0
        self.reload()

    def set_notify(self, cb) -> None:
        self._notify_cb = cb

    def set_egress_notify(self, cb) -> None:
        self._egress_cb = cb

    def _signal_egress(self) -> None:
        if self._egress_cb is not None:
            self._egress_cb()

    def set_alert_notify(self, cb) -> None:
        self._alert_cb = cb

    def _signal_alert(self) -> None:
        if self._alert_cb is not None:
            self._alert_cb()

    def identity(self) -> dict:
        """This Catchment's stable id + optional display name (see plans/cross-catchment-visibility.md)."""
        with self.lock:
            rows = dict(self.db.execute("SELECT key, value FROM catchment_meta").fetchall())
            return {"id": rows.get("id"), "name": rows.get("name")}

    # ─── Topology load ────────────────────────────────────────────────────────

    def reload(self) -> None:
        """(Re)build the engine + metadata from the database (selected Ponds only)."""
        with self.lock:
            db = self.db
            ponds: dict[str, Pond] = {}
            pond_states: dict[str, PondState] = {}
            ripples: dict[str, Ripple] = {}
            ripple_states: dict[str, RippleState] = {}
            triggers: dict[str, Trigger] = {}
            self.meta = {}
            self._incomplete: list[tuple[str, datetime]] = []  # (pond, F) runs to resume

            self.duck_pool_names = set(PRESET_POOLS) | {r[0] for r in db.execute("SELECT name FROM duck_pool")}
            name_by_pnid = {r[0]: r[1] for r in db.execute("SELECT id, name FROM pond_name")}
            rows = db.execute("""
                SELECT pn.name, p.major, p.id, p.pond_version_id, pv.version, pv.source_path, pn.kind,
                       p.is_draw, p.is_spout, pv.dbt
                FROM pond p JOIN pond_name pn ON pn.id = p.pond_name_id
                JOIN pond_version pv ON pv.id = p.pond_version_id
            """).fetchall()
            deployed = {pond_key(name, major) for name, major, *_ in rows}
            pondid_to_key = {pid: pond_key(nm, mj) for nm, mj, pid, *_ in rows}
            for name, major, pond_id, pv_id, version, source_path, kind, is_draw, is_spout, dbt in rows:
                self.meta[pond_key(name, major)] = {
                    "name": name, "major": major, "version_id": pv_id, "version": version,
                    "source_path": source_path, "pond_id": pond_id, "kind": kind,
                    "is_draw": bool(is_draw), "is_spout": bool(is_spout), "dbt": bool(dbt), "ripple_ids": {},
                }

            for name, major, pond_id, pv_id, _version, _source_path, _kind, is_draw, is_spout, _dbt in rows:
                key = pond_key(name, major)
                sources, optional, missing = [], set(), []
                for snid, smajor, required in db.execute(
                    "SELECT source_pond_name_id, source_major, required FROM pond_to_pond WHERE pond_id = ?",
                    (pond_id,),
                ):
                    skey = pond_key(name_by_pnid.get(snid, ""), smajor)
                    if skey in deployed:  # only wire sources whose (name, major) line is deployed
                        sources.append(skey)
                        if not required:
                            optional.add(skey)
                    else:
                        # A declared Source (required or optional) is absent from this Catchment —
                        # not deployed and not drawn over a duct. Hard-block until it is present.
                        missing.append(skey)
                has_missing_source = bool(missing)
                self.meta[key]["missing_sources"] = missing
                windows = [self._row_to_window(r) for r in db.execute(
                    "SELECT start_anchor, duration_seconds, freq_unit, freq_interval, valid_days, until_time "
                    "FROM pond_window WHERE pond_id = ?", (pond_id,)
                )]
                retry = db.execute(
                    "SELECT immediate_retries, source_retries FROM pond_retry WHERE pond_id = ?", (pond_id,)
                ).fetchone()
                imm, onc = retry if retry else (0, 0)
                duck_row = db.execute(
                    "SELECT duck_target, dedicated_instance_type, dedicated_auto_stop, "
                    "flock_mode, flock_engine, oom_policy FROM pond_duck WHERE pond_id = ?", (pond_id,)
                ).fetchone()
                self.duck_overrides[key] = _duck_override_row(duck_row)
                declared = db.execute(
                    "SELECT duck_pool, flock_mode, flock_engine, oom_policy FROM pond_version WHERE id = ?",
                    (pv_id,),
                ).fetchone()
                self.duck_declared[key] = {
                    "duck_target": declared[0], "flock_mode": declared[1],
                    "flock_engine": declared[2], "oom_policy": declared[3],
                } if declared else {}
                # always_run is a Pond property ORed up from its Ripples: any always_run Ripple means
                # the Pond runs every time (never engine-passed). See plans/no-change-skip.md.
                always_run = bool(db.execute(
                    "SELECT MAX(always_run) FROM ripple WHERE pond_version_id = ?", (pv_id,)
                ).fetchone()[0])
                ponds[key] = Pond(
                    id=key, name=key, sources=sources, optional_sources=optional, windows=windows,
                    retry_immediately=imm, retry_on_change=onc, is_draw=bool(is_draw),
                    is_spout=bool(is_spout), has_missing_source=has_missing_source, always_run=always_run,
                )
                pond_states[key] = self._load_pond_state(pond_id)
                if is_spout:
                    # The egress config + the standing-Wake armed state (Sleep persists as armed=0).
                    cfg = db.execute(
                        "SELECT table_name, destination, mode, armed FROM pond_spout WHERE pond_id = ?",
                        (pond_id,),
                    ).fetchone()
                    if cfg:
                        self.meta[key]["spout"] = {"table": cfg[0], "destination": cfg[1], "mode": cfg[2]}
                        self.meta[key]["source_key"] = sources[0] if sources else None
                        pond_states[key].standing_wake = bool(cfg[3])

                rip_rows = db.execute(
                    "SELECT id, name FROM ripple WHERE pond_version_id = ?", (pv_id,)
                ).fetchall()
                rid_to_rname = {rid: rname for rid, rname in rip_rows}
                for rid, rname in rip_rows:
                    self.meta[key]["ripple_ids"][rname] = rid
                for rid, rname in rip_rows:
                    parent_rids = [
                        r[0] for r in db.execute("SELECT source_id FROM ripple_to_ripple WHERE sink_id = ?", (rid,))
                    ]
                    eid = f"{key}.{rname}"
                    parents = [f"{key}.{rid_to_rname[p]}" for p in parent_rids if p in rid_to_rname]
                    ripples[eid] = Ripple(id=eid, pond_id=key, name=rname, parents=parents)
                    ripple_states[eid] = RippleState()

                # Restore execution state from run history: gen (run counts), per-Ripple freshness, and
                # any Pond Run that was still 'running' when the Catchment stopped (resumed below).
                ps = pond_states[key]
                ps.runs_started = db.execute(
                    "SELECT COUNT(*) FROM pond_run WHERE pond_version_id = ?", (pv_id,)
                ).fetchone()[0]
                ps.runs_completed = db.execute(
                    "SELECT COUNT(*) FROM pond_run WHERE pond_version_id = ? AND status = 'success'", (pv_id,)
                ).fetchone()[0]
                for rid, rname in rip_rows:
                    row = db.execute(
                        "SELECT MAX(f) FROM ripple_run WHERE pond_version_id = ? AND ripple_id = ? "
                        "AND status = 'success'", (pv_id, rid),
                    ).fetchone()
                    if row and row[0]:
                        ef = datetime.fromisoformat(row[0])
                        ripple_states[f"{key}.{rname}"].start_f = ef
                        ripple_states[f"{key}.{rname}"].end_f = ef
                for (incf,) in db.execute(
                    "SELECT f FROM pond_run WHERE pond_version_id = ? AND status = 'running'", (pv_id,)
                ):
                    self._incomplete.append((key, datetime.fromisoformat(incf)))

            for pond_id, kind, bound_ms in db.execute(
                "SELECT pond_id, kind, bound_ms FROM pond_trigger WHERE status = 'active'"
            ):
                key = pondid_to_key.get(pond_id)
                if key:
                    bound = timedelta(milliseconds=bound_ms) if bound_ms is not None else None
                    triggers[key] = Trigger(pond_id=key, kind=kind, bound=bound)

            self.state = EngineState(
                ponds=ponds, pond_states=pond_states, ripples=ripples,
                ripple_states=ripple_states, triggers=triggers,
            )
            # Recompute blocked from the freshly-loaded topology: a Source that is absent now (or has
            # since become present, e.g. a duct was added) flips has_missing_source, so the persisted
            # is_blocked may be stale. Re-derive for every Pond (propagates to Sinks).
            for pid in self.state.pond_states:
                derive_blocked(self.state, pid)
            self.jobs = {key: self.jobs.get(key, []) for key in ponds}
            self.state_version += 1  # topology/config (deploy, ducts, windows) changed

    def _load_pond_state(self, pond_id: int) -> PondState:
        row = self.db.execute(
            "SELECT start_f, end_f, d_ms, has_pull, has_received_pull, is_failed, is_blocked, failed_f, "
            "failures, is_killed, pull_local, pull_m, refresh_pending, repairing, changed_f "
            "FROM pond_state WHERE pond_id = ?",
            (pond_id,),
        ).fetchone()
        ps = PondState()
        if row:
            (sf, ef, d_ms, hp, hrp, is_failed, is_blocked, failed_f, failures, is_killed, pull_local,
             pull_m, refresh_pending, repairing, changed_f) = row
            ps.is_killed = bool(is_killed)
            ps.pull_local = bool(pull_local)
            ps.refresh_pending = bool(refresh_pending)
            ps.repairing = bool(repairing)
            ps.pull_m = datetime.fromisoformat(pull_m) if pull_m else NEVER
            ps.start_f = datetime.fromisoformat(sf) if sf else NEVER
            ps.end_f = datetime.fromisoformat(ef) if ef else NEVER
            # changed_f defaults to end_f (the migration backfill) for rows predating the column.
            ps.changed_f = datetime.fromisoformat(changed_f) if changed_f else ps.end_f
            ps.d = timedelta(milliseconds=d_ms or 0)
            ps.has_pull = bool(hp)
            ps.has_received_pull = bool(hrp)
            ps.is_failed = bool(is_failed)
            ps.is_blocked = bool(is_blocked)
            ps.failed_f = datetime.fromisoformat(failed_f) if failed_f else NEVER
            ps.failures = failures or 0
            ps.targets = [
                datetime.fromisoformat(r[0])
                for r in self.db.execute("SELECT target_f FROM pond_target WHERE pond_id = ?", (pond_id,))
            ]
        return ps

    # ─── Pond resolution ──────────────────────────────────────────────────────

    def resolve(self, name: str, major: int | None = None, version: str | None = None) -> str:
        """Resolve a Pond reference to its engine key ``"{name}@{major}"``.

        Default is the highest deployed major line. ``version`` targets that version's major line
        and must be the currently *selected* artifact for it (only selected versions execute).
        Raises KeyError (unknown pond / major line) or ValueError (conflicting / unselected version).
        """
        with self.lock:
            majors = {m["major"]: k for k, m in self.meta.items() if m["name"] == name}
            if not majors:
                raise KeyError(f"Pond '{name}' not found")
            if version is not None:
                vmajor = int(version.split(".")[0])
                if major is not None and major != vmajor:
                    raise ValueError(f"major {major} conflicts with version {version} (major {vmajor})")
                key = majors.get(vmajor)
                if key is None:
                    raise KeyError(f"No deployed major {vmajor} of Pond '{name}'")
                selected = self.meta[key]["version"]
                if selected != version:
                    raise ValueError(
                        f"Version {version} of '{name}' is not the selected version for major {vmajor} "
                        f"(selected: {selected}) — deploy it to select it"
                    )
                return key
            if major is not None:
                key = majors.get(major)
                if key is None:
                    raise KeyError(f"No deployed major {major} of Pond '{name}'")
                return key
            return majors[max(majors)]

    # ─── Triggers ─────────────────────────────────────────────────────────────

    def tap(self, pond: str, m: datetime | None = None) -> None:
        """One pull. ``m`` (a duct forwarding the downstream's demand epoch) is the freshness an Inlet
        it reaches will mint; defaults to now."""
        with self.lock:
            self.state = tap_pond(self.state, pond, _now(), m)
            self._process(_now())

    def pulse(self, pond: str, at: datetime | None = None) -> None:
        """Push a target freshness. ``at`` (a duct forwarding the downstream's target) is the demand
        epoch; defaults to now."""
        with self.lock:
            self.state = pulse_pond(self.state, pond, at or _now())
            self._process(_now())

    def wave(self, pond: str) -> None:
        with self.lock:
            self.state.triggers[pond] = Trigger(pond_id=pond, kind="wave")
            self._persist_trigger(pond, "wave", None)
            self._tick_process(_now())

    def tide(self, pond: str, bound: timedelta) -> None:
        with self.lock:
            self.state.triggers[pond] = Trigger(pond_id=pond, kind="tide", bound=bound)
            self._persist_trigger(pond, "tide", int(bound.total_seconds() * 1000))
            self._tick_process(_now())

    def wake(self, pond: str) -> None:
        """Wake — a one-shot non-propagating pull (run on fresh input; clears failure/kill)."""
        with self.lock:
            self.state = wake_pond(self.state, pond, _now())
            self._process(_now())

    def force(self, pond: str) -> None:
        """Force — recompute now at the current freshness, even with no upstream change."""
        with self.lock:
            self.state = force_pond(self.state, pond, _now())
            self._process(_now())

    def refresh(self, pond: str, clear: bool = False) -> None:
        """Refresh — flag the Pond so its next run is a cold wipe-and-rebuild. Lazy: persists the flag
        but starts nothing; the rebuild happens on the next natural run. ``clear`` un-sets it."""
        with self.lock:
            self.state = refresh_pond(self.state, pond, clear=clear)
            self._persist_state()
            self.state_version += 1

    def delete_table(self, pond: str, table: str) -> None:
        """Delete one table — its published data **and** its registry collection — from a Pond, **now**.
        No run and no freshness change: the table is simply gone, and reappears only if the Pond's code
        recreates it on a genuine future run (where the builder's absent⇒comprehensive trigger rebuilds it
        whole). Requires the Pond to be idle. See plans/deletes.md."""
        from pathlib import Path

        from ..dataplane import unpublish_table
        from ..trickle_io import base_table_name, drop_table
        from .registry import pond_connect, pond_data_dir

        with self.lock:
            ps = self.state.pond_states.get(pond)
            if ps is not None and ps.start_f > ps.end_f:
                raise ValueError("the Pond is running — delete a table when it is idle")
            # A Trickle companion (changelog/band/droplog/base) resolves to its base — the collection is one
            # deletable unit; deleting a changelog alone would corrupt the reconstructable main.
            table = base_table_name(table)
            meta = self.meta[pond]
            name, major = meta["name"], meta["major"]
            # An idle Duck still holds registry.duckdb open, so free it before dropping (it respawns on the
            # next run). Then drop the table's registry collection + its published data — no run, no rebuild.
            self.launcher.terminate(pond, wait=True)
            self._idle_since.pop(pond, None)
            con = pond_connect(Path(self.root), name, major)
            try:
                drop_table(con, table)
            finally:
                con.close()
            unpublish_table(pond_data_dir(Path(self.root), name, major, self.data_root), table)
            self.state_version += 1

    def reset_pond(self, pond: str, clear_history: bool = False) -> None:
        """Reset one Pond to a fresh-deploy state — scrub its registry, published data, and ledger, and
        rewind its freshness/fault to ``NEVER`` — while **keeping** its deployed artifact, operational
        config, and **demand**. Lazy: it forces nothing; preserved demand + standing triggers re-drive the
        rebuild when next eligible (the data dir is empty, so no downstream reads a gap — nobody reads a
        ``NEVER`` Source). Requires the Pond idle. See plans/reset.md."""
        import shutil
        from pathlib import Path

        from .registry import pond_data_dir, pond_major_dir

        with self.lock:
            ps = self.state.pond_states.get(pond)
            if ps is not None and ps.start_f > ps.end_f:
                raise ValueError("the Pond is running — reset it when idle")
            meta = self.meta[pond]
            name, major = meta["name"], meta["major"]
            # 1. Free + scrub the on-disk runtime: registry.duckdb, pond.db ledger, and the published data.
            self.launcher.terminate(pond, wait=True)
            self._idle_since.pop(pond, None)
            shutil.rmtree(pond_major_dir(Path(self.root), name, major), ignore_errors=True)
            if self.data_root is not None:  # published data lives outside the state root (bucket/Volume)
                pond_data_dir(Path(self.root), name, major, self.data_root).rmtree()
            # 2. Rewind freshness/fault in duck.db (keep demand + operational config).
            self.db.execute(
                "UPDATE pond_state SET start_f=?, end_f=?, changed_f=?, d_ms=0, is_failed=0, is_blocked=0, "
                "failed_f=?, failures=0, is_killed=0, refresh_pending=0 WHERE pond_id=?",
                (_iso(NEVER), _iso(NEVER), _iso(NEVER), _iso(NEVER), meta["pond_id"]),
            )
            if clear_history:
                self.db.execute("DELETE FROM ripple_run WHERE pond_version_id=?", (meta["version_id"],))
                self.db.execute("DELETE FROM pond_run WHERE pond_version_id=?", (meta["version_id"],))
                self.db.execute("DELETE FROM ripple_run_lineage WHERE pond_version_id=?", (meta["version_id"],))
            self.db.commit()
            # 3. Rewind the in-memory engine state (keep demand: has_pull/pull_m/targets/standing_wake).
            if ps is not None:
                ps.start_f = ps.end_f = ps.changed_f = NEVER
                ps.d = timedelta()
                ps.is_failed = ps.is_blocked = ps.is_killed = False
                ps.failed_f = NEVER
                ps.failures = 0
                ps.missing_asset = None
                ps.missing_asset_f = NEVER
                ps.refresh_pending = ps.repairing = False
                ps.runs_started = ps.runs_completed = 0
                for rid, rip in self.state.ripples.items():
                    if rip.pond_id == pond:
                        rs = self.state.ripple_states[rid]
                        rs.start_f = rs.end_f = NEVER
                        rs.is_running = False
                        rs.started_at = None
                        rs.runs_completed = 0
                derive_blocked(self.state, pond)  # re-derive this Pond + propagate to its Sinks
            self.state_version += 1
            self._process(_now())

    def reset_catchment(self, clear_history: bool = False) -> dict:
        """Reset the **whole Catchment** to a fresh-deploy state — the sanctioned replacement for
        ``rm -rf .duckstring``. Stop-the-world: terminate every Duck, scrub every line's registry, ledger,
        and published data, rewind every Pond's freshness/fault to ``NEVER`` — keeping the deployed
        artifacts, operational config, secrets, and keys. Rebuilds lazily (standing triggers + demand from
        the Inlets down). See plans/reset.md. Returns ``{"ponds": n}``."""
        import shutil
        from pathlib import Path

        from .registry import pond_data_dir

        with self.lock:
            # 1. Quiesce: stop every Duck (wait, so registry.duckdb handles are free) and drop pending work.
            for key in list(self.state.ponds):
                self.launcher.terminate(key, wait=True)
            self.jobs.clear()
            self._pending_transfers.clear()
            self._pending_egress.clear()
            self._idle_since.clear()
            lines = [(m["name"], m["major"]) for m in self.meta.values()
                     if not m.get("is_draw") and not m.get("is_spout")]
            # 2. Scrub each line's runtime: registry.duckdb + pond.db + local data/ (keep the {version}/ artifacts).
            ponds_root = Path(self.root) / "ponds"
            if ponds_root.exists():
                for name_dir in ponds_root.iterdir():
                    if name_dir.is_dir():
                        for m in name_dir.glob("m*"):
                            if m.is_dir() and m.name[1:].isdigit():
                                shutil.rmtree(m, ignore_errors=True)
            if self.data_root is not None:  # published data outside the state root
                for name, major in lines:
                    try:
                        pond_data_dir(Path(self.root), name, major, self.data_root).rmtree()
                    except Exception:
                        pass
            # 3. Rewind all runtime rows in duck.db (keep demand + topology + operational config + secrets).
            self.db.execute(
                "UPDATE pond_state SET start_f=?, end_f=?, changed_f=?, d_ms=0, is_failed=0, is_blocked=0, "
                "failed_f=?, failures=0, is_killed=0, refresh_pending=0",
                (_iso(NEVER), _iso(NEVER), _iso(NEVER), _iso(NEVER)),
            )
            self.db.execute("DELETE FROM alert_delivery")  # the notification outbox is runtime, not config
            if clear_history:
                self.db.execute("DELETE FROM ripple_run")
                self.db.execute("DELETE FROM pond_run")
                self.db.execute("DELETE FROM ripple_run_lineage")
            self.db.commit()
            # 4. Rebuild the engine from the scrubbed DB — a fresh-deploy engine (spouts/triggers re-armed).
            self.reload()
            self.state_version += 1
            self._process(_now())
            return {"ponds": len(lines)}

    def remove_pond(self, name: str, major: int, wipe: bool = False) -> dict:
        """Remove (retire) one deployed major line ``name@major`` — delete its live ``pond`` selection, its
        ``pond(id)``-keyed config, its on-disk runtime, and its **own** Spouts + alert channels — while
        **keeping** its deployment record and run history (a redeploy un-retires it). Downstream sinks that
        pin it re-derive ``has_missing_source`` and hard-block. Requires the line idle + demand-free (per the
        thesis it never checks or refuses on dependents).

        With ``wipe=True`` this is a **purge**, not a retire: after the retire steps it also deletes the
        line's deployment record (every ``pond_version`` of the major + its ``ripple``/``ripple_to_ripple``/
        ``pond_version_schema`` rows), its run history (``pond_run``/``ripple_run``), and the ``{version}/``
        artifact dirs on disk — and drops the ``pond_name`` if the last major is gone and nothing sources it,
        so the line is as if it had never been deployed (its ``/api/runs`` history vanishes too, and it is
        **not** reversible by a redeploy). See plans/remove-pond.md."""
        import shutil
        from pathlib import Path

        from .registry import pond_data_dir, pond_major_dir

        key = pond_key(name, major)
        with self.lock:
            meta = self.meta.get(key)
            if meta is None or meta.get("is_draw") or meta.get("is_spout"):
                raise ValueError(f"no removable Pond '{key}'")
            ps = self.state.pond_states.get(key)
            if ps is not None:  # guard: idle + demand-free (the whole op is atomic under the lock)
                if ps.start_f > ps.end_f:
                    raise ValueError("the Pond is running — sleep it first, then remove")
                if ps.has_pull or ps.targets or ps.standing_wake or key in self.state.triggers:
                    raise ValueError("the Pond has demand — clear it first (duckstring control sleep)")
            pond_id = meta["pond_id"]
            (pn_id,) = self.db.execute("SELECT pond_name_id FROM pond WHERE id = ?", (pond_id,)).fetchone()
            # Blast radius: sinks that pin this line (they'll block on the missing Source) + its own Spouts.
            now_blocked = sorted({r[0] for r in self.db.execute(
                "SELECT pn.name FROM pond_to_pond ptp JOIN pond p ON p.id = ptp.pond_id "
                "JOIN pond_name pn ON pn.id = p.pond_name_id "
                "WHERE ptp.source_pond_name_id = ? AND ptp.source_major = ? AND p.is_spout = 0",
                (pn_id, major))})  # Spouts of this line are removed with it, not left blocked
            spouts = sorted(k for k, m in self.meta.items() if m.get("is_spout") and m.get("source_key") == key)

            # 1. Quiesce + scrub the on-disk runtime (registry + pond.db ledger + local data). Keep the
            #    {version}/ artifacts + the deployment/history rows.
            self.launcher.terminate(key, wait=True)
            self._idle_since.pop(key, None)
            shutil.rmtree(pond_major_dir(Path(self.root), name, major), ignore_errors=True)
            if self.data_root is not None:  # published data outside the state root
                pond_data_dir(Path(self.root), name, major, self.data_root).rmtree()
            # 2. Remove its attachments: its Spouts (full purge) + its name@major alert channels
            #    (alert_delivery cascades via the FK).
            for skey in spouts:
                self._destroy_spout(skey)
            self.db.execute("DELETE FROM alert_channel WHERE scope_name = ? AND scope_major = ?", (name, major))
            # 3. Delete the pond(id)-keyed config + the pond selection row. Keep pond_version / ripple /
            #    ripple_run / pond_run / pond_version_schema / pond_name (the retained record).
            for tbl in ("pond_state", "pond_target", "pond_open", "pond_trigger", "pond_retry",
                        "pond_window", "pond_spout", "pond_duck", "pond_to_pond"):
                self.db.execute(f"DELETE FROM {tbl} WHERE pond_id = ?", (pond_id,))
            self.db.execute("DELETE FROM pond WHERE id = ?", (pond_id,))
            # 3b. --wipe: purge the deployment record + run history + {version}/ artifacts for this major,
            #     so the line is as if never deployed (not reversible by a redeploy). FK order: children first.
            if wipe:
                versions = self.db.execute(
                    "SELECT id, source_path FROM pond_version WHERE pond_name_id = ? AND major = ?",
                    (pn_id, major)).fetchall()
                vids = [v[0] for v in versions]
                if vids:
                    ph = ",".join("?" * len(vids))
                    self.db.execute(f"DELETE FROM ripple_run WHERE pond_version_id IN ({ph})", vids)
                    self.db.execute(f"DELETE FROM pond_run WHERE pond_version_id IN ({ph})", vids)
                    self.db.execute(f"DELETE FROM pond_version_schema WHERE pond_version_id IN ({ph})", vids)
                    self.db.execute(f"DELETE FROM ripple_run_lineage WHERE pond_version_id IN ({ph})", vids)
                    self.db.execute(
                        f"DELETE FROM pond_version_column_lineage WHERE pond_version_id IN ({ph})", vids)
                    self.db.execute(
                        f"DELETE FROM ripple_to_ripple WHERE sink_id IN "
                        f"(SELECT id FROM ripple WHERE pond_version_id IN ({ph}))", vids)
                    self.db.execute(f"DELETE FROM ripple WHERE pond_version_id IN ({ph})", vids)
                    self.db.execute(f"DELETE FROM pond_version WHERE id IN ({ph})", vids)
                # Drop the pond_name once its last major is gone and no live sink still sources it.
                self.db.execute(
                    "DELETE FROM pond_name WHERE id = ? "
                    "AND NOT EXISTS (SELECT 1 FROM pond_version WHERE pond_name_id = ?) "
                    "AND NOT EXISTS (SELECT 1 FROM pond_to_pond WHERE source_pond_name_id = ?)",
                    (pn_id, pn_id, pn_id))
                for _vid, source_path in versions:  # the {version}/ artifact dirs
                    shutil.rmtree(Path(self.root) / source_path, ignore_errors=True)
                pond_root = Path(self.root) / "ponds" / name  # tidy an now-empty ponds/{name}/
                if pond_root.is_dir() and not any(pond_root.iterdir()):
                    pond_root.rmdir()
            self.db.commit()
            # 4. Rebuild the engine — the line is gone; sinks re-derive has_missing_source.
            self.reload()
            self.state_version += 1
            self._process(_now())
            return {"removed": key, "spouts_removed": spouts, "now_blocked": now_blocked, "wiped": wipe}

    def is_pond_running(self, pond: str) -> bool:
        """Whether a Pond has a Run in flight (start_f advanced past end_f) — the idle gate for an Object
        delete (which must not race a run's commit_objects). Best-effort: unknown Pond ⇒ not running."""
        with self.lock:
            ps = self.state.pond_states.get(pond)
            return ps is not None and ps.start_f > ps.end_f

    def repair(self, ponds: list[tuple[str, int | None]], downstream: bool = False) -> dict:
        """Repair — force-rebuild a **connected** set of Ponds now, in topological order (steps out of
        the demand model; see ``plans/refresh.md``). Each node is wiped and rebuilt (refresh + force) once
        its in-scope parents finish, so it reads their freshly-rebuilt output. The scope is marked
        ``repairing`` (blocked from normal demand) until each node's turn. Rejects a disconnected set."""
        with self.lock:
            now = _now()
            if self._repair is not None:
                raise ValueError("a repair is already in progress on this Catchment")
            seeds = [self.resolve(n, m, None) for n, m in ponds]
            children = self._children_graph()
            scope = set(seeds)
            if downstream:
                scope |= _descendants(seeds, children)
            gap = _connectivity_gap(scope, children)
            if gap is not None:
                raise ValueError(
                    f"disconnected repair set: '{gap[0]}' reaches '{gap[1]}' only through unselected Ponds "
                    f"— include the connecting Pond(s) or pass downstream=true"
                )
            parents = {k: {p for p in self.state.ponds[k].sources if p in scope} for k in scope}
            order = _topo_order(scope, parents)
            for k in scope:  # quiesce: block normal demand, abandon any in-flight run cleanly
                ps = self.state.pond_states[k]
                if ps.start_f > ps.end_f:
                    self.launcher.terminate(k)
                    self.jobs[k] = []
                    ps.start_f = ps.end_f
                ps.repairing = True
            for k in scope:
                derive_blocked(self.state, k)
            self._repair = {"scope": scope, "parents": parents, "done": set(), "released": set()}
            for k in scope:  # release the roots (no in-scope parent)
                if not parents[k]:
                    self._release_repair(k, now)
            self._process(now)
            return {"scope": order, "downstream": downstream}

    def _release_repair(self, key: str, now: datetime) -> None:
        self.state = repair_pond(self.state, key, now)  # force + refresh: a cold rebuild at current f
        self._repair["released"].add(key)

    def _advance_repair(self, pond: str, now: datetime) -> None:
        """A scope Pond's repair run completed: mark it done, unblock it, and release any child whose
        in-scope parents are now all done. When the whole scope is done, the plan ends."""
        r = self._repair
        if r is None or pond not in r["released"] or pond in r["done"]:
            return
        r["done"].add(pond)
        self.state.pond_states[pond].repairing = False
        derive_blocked(self.state, pond)
        for k in r["scope"]:
            if k not in r["released"] and r["parents"][k] <= r["done"]:
                self._release_repair(k, now)
        if r["done"] >= r["scope"]:
            self._repair = None  # the repair plan is complete
        self._process(now)

    def _children_graph(self) -> dict[str, set[str]]:
        children: dict[str, set[str]] = {k: set() for k in self.state.ponds}
        for k, pond in self.state.ponds.items():
            for sp in pond.sources:
                if sp in children:
                    children[sp].add(k)
        return children

    def wipe_history(self, pond: str) -> None:
        """Clear a Pond's run history (its ``pond_run``/``ripple_run`` rows) — a log trim only, with **no**
        data scrub, freshness rewind, or state change. Distinct from ``reset_pond``, which also scrubs."""
        with self.lock:
            version_id = self.meta[pond]["version_id"]
            self.db.execute("DELETE FROM ripple_run WHERE pond_version_id = ?", (version_id,))
            self.db.execute("DELETE FROM pond_run WHERE pond_version_id = ?", (version_id,))
            self.db.execute("DELETE FROM ripple_run_lineage WHERE pond_version_id = ?", (version_id,))
            self.db.commit()
            self.state_version += 1

    def batch(
        self, ponds: list[tuple[str, int | None]], operations: list[str], confirm: str | None = None,
    ) -> dict:
        """Apply a set of operations to a set of Ponds in precedence order (``BATCH_OPS``) — the engine
        behind the UI Selector and ``duckstring do``. Validates the op-set (unknown op, or Repair combined
        with Remove/Reset → ValueError → 422) and, when any irreversible op (reset/wipe/remove) is present,
        requires ``confirm`` to equal this Catchment's name. Remove implies (and subsumes) Reset, and is
        **terminal per Pond** — later ops skip an already-removed line. Repair runs once over the live
        scope (its own connectivity check rejects a gappy manual selection). Per-Pond execution errors are
        collected, not fatal. Holds the lock across the whole batch, so it is atomic w.r.t. the scheduler."""
        selected = list(dict.fromkeys(operations))
        unknown = [o for o in selected if o not in BATCH_OPS]
        if unknown:
            raise ValueError(f"unknown operation(s): {', '.join(unknown)}")
        ops = [o for o in BATCH_OPS if o in selected]
        if not ops:
            raise ValueError("no operations selected")
        if "repair" in ops and ("remove" in ops or "reset" in ops):
            raise ValueError("repair cannot be combined with remove or reset")
        if "remove" in ops:  # Remove already scrubs — the standalone Reset step is redundant.
            ops = [o for o in ops if o != "reset"]

        with self.lock:
            if any(o in IRREVERSIBLE_OPS for o in ops):
                expected = self.identity().get("name") or "confirm"
                if confirm != expected:
                    raise ValueError(
                        f"irreversible operation(s) — type the catchment name ('{expected}') to confirm"
                    )
            keys = list(dict.fromkeys(self.resolve(n, m, None) for n, m in ponds))
            removed: set[str] = set()
            errors: list[dict] = []

            def _run(op: str, key: str, fn) -> None:
                try:
                    fn()
                except Exception as exc:  # best-effort per Pond — collect, don't abort the batch
                    errors.append({"pond": key, "op": op, "error": str(exc)})

            for op in ops:
                if op == "repair":
                    live = [k for k in keys if k not in removed]
                    if live:
                        _run("repair", None, lambda live=live: self.repair(
                            [(k.rpartition("@")[0], int(k.rpartition("@")[2])) for k in live]))
                    continue
                for key in [k for k in keys if k not in removed]:
                    if op == "kill":
                        _run(op, key, lambda k=key: self.kill(k))
                    elif op == "sleep":
                        _run(op, key, lambda k=key: self.sleep(k))
                    elif op == "reset":
                        _run(op, key, lambda k=key: self.reset_pond(k))
                    elif op == "wipe":
                        _run(op, key, lambda k=key: self.wipe_history(k))
                    elif op == "clear":
                        _run(op, key, lambda k=key: self.clear(k))
                    elif op == "refresh":
                        _run(op, key, lambda k=key: self.refresh(k))
                    elif op == "remove":
                        n, _, mj = key.rpartition("@")

                        def _rm(k=key, n=n, mj=mj):
                            self.remove_pond(n, int(mj), wipe=False)
                            removed.add(k)
                        _run(op, key, _rm)
            return {"operations": ops, "ponds": keys, "removed": sorted(removed), "errors": errors}

    def kill(self, pond: str) -> None:
        """Kill — terminate the Duck and park the Pond in a terminal killed state (cancels its Run)."""
        with self.lock:
            now = _now()
            ps = self.state.pond_states[pond]
            in_flight = ps.start_f if ps.start_f > ps.end_f else None
            self.state = kill_pond(self.state, pond, now)
            self.launcher.terminate(pond)  # cancel the Duck's running Ripples (kills the process)
            self.jobs[pond] = []
            if in_flight is not None:
                self._kill_pond_run(pond, _iso(in_flight), now)
            self._process(now)

    def clear(self, pond: str) -> None:
        """Operator acknowledgement: clear a Pond's failure/block (no run). Downstream Ponds blocked
        only by this failure re-derive and unblock on their own."""
        with self.lock:
            self.state = clear_pond(self.state, pond, _now())
            self._process(_now())

    def clear_on_redeploy(self, name: str, major: int) -> None:
        """Called after a (re)deploy: if the Pond was failed, clear it — a fresh artifact presumably
        fixes the cause — so it (and anything blocked downstream) can resume without a manual clear.
        Only clears a Pond's *own* failure; one merely blocked by a still-failed Source stays blocked."""
        with self.lock:
            ps = self.state.pond_states.get(pond_key(name, major))
            if ps is not None and ps.is_failed:
                self.state = clear_pond(self.state, pond_key(name, major), _now())
                self._process(_now())

    def set_retry(self, pond: str, immediate_retries: int, source_retries: int) -> None:
        """Set the live retry budgets on a Pond (persisted to pond_retry; owned by the operator)."""
        with self.lock:
            pond_id = self.meta[pond]["pond_id"]
            self.db.execute(
                "INSERT INTO pond_retry (pond_id, immediate_retries, source_retries) VALUES (?, ?, ?) "
                "ON CONFLICT(pond_id) DO UPDATE SET immediate_retries = excluded.immediate_retries, "
                "source_retries = excluded.source_retries",
                (pond_id, immediate_retries, source_retries),
            )
            self.db.commit()
            p = self.state.ponds[pond]
            p.retry_immediately = immediate_retries
            p.retry_on_change = source_retries
            self.state_version += 1  # budgets show in /api/status

    def retry_config(self, pond: str) -> dict:
        p = self.state.ponds[pond]
        return {"immediate_retries": p.retry_immediately, "source_retries": p.retry_on_change}

    @staticmethod
    def duck_defaults() -> dict:
        """The Catchment-wide compute defaults, from env. Inert for the classic subprocess Duck (the
        Flock is off with no engine configured); the Flock posture matters for remote launchers."""
        mode = (os.environ.get("DUCKSTRING_FLOCK_MODE") or "off").lower()
        return {
            "duck_target": "catchment",
            "flock_mode": mode if mode in FLOCK_MODES else "off",
            "flock_engine": os.environ.get("DUCKSTRING_FLOCK_ENGINE") or None,
            "oom_policy": (os.environ.get("DUCKSTRING_FLOCK_OOM_POLICY") or "fail_up").lower(),
        }

    _DUCK_OVERRIDE_FIELDS = ("duck_target", "dedicated_instance_type", "dedicated_auto_stop",
                             "flock_mode", "flock_engine", "oom_policy")

    def set_duck(self, pond: str, clear: bool = False, **fields) -> None:
        """Set (or with ``clear`` drop) a Pond's compute override (persisted to pond_duck; operator-owned,
        like retry budgets). Only the fields passed are changed; the rest keep their current override.
        ``clear`` reverts the Pond to its DECLARED config (pond.toml), else the Catchment default."""
        fields = {k: v for k, v in fields.items() if k in self._DUCK_OVERRIDE_FIELDS}
        if fields.get("flock_mode") is not None and fields["flock_mode"] not in FLOCK_MODES:
            raise ValueError(f"flock_mode must be one of {', '.join(FLOCK_MODES)}")
        if fields.get("oom_policy") is not None and fields["oom_policy"] not in OOM_POLICIES:
            raise ValueError(f"oom_policy must be one of {', '.join(OOM_POLICIES)}")
        with self.lock:
            pond_id = self.meta[pond]["pond_id"]
            if clear:
                self.db.execute("DELETE FROM pond_duck WHERE pond_id = ?", (pond_id,))
                self.duck_overrides[pond] = _duck_override_row(None)
            else:
                cur = self.duck_overrides.get(pond) or _duck_override_row(None)
                new = dict(cur)
                new.update(fields)
                self.db.execute(
                    "INSERT INTO pond_duck (pond_id, duck_target, dedicated_instance_type, "
                    "dedicated_auto_stop, flock_mode, flock_engine, oom_policy) "
                    "VALUES (:id, :duck_target, :dedicated_instance_type, :dedicated_auto_stop, "
                    ":flock_mode, :flock_engine, :oom_policy) "
                    "ON CONFLICT(pond_id) DO UPDATE SET duck_target = excluded.duck_target, "
                    "dedicated_instance_type = excluded.dedicated_instance_type, "
                    "dedicated_auto_stop = excluded.dedicated_auto_stop, flock_mode = excluded.flock_mode, "
                    "flock_engine = excluded.flock_engine, oom_policy = excluded.oom_policy",
                    {"id": pond_id, "duck_target": new["duck_target"],
                     "dedicated_instance_type": new["dedicated_instance_type"],
                     "dedicated_auto_stop": (None if new["dedicated_auto_stop"] is None
                                             else int(new["dedicated_auto_stop"])),
                     "flock_mode": new["flock_mode"], "flock_engine": new["flock_engine"],
                     "oom_policy": new["oom_policy"]},
                )
                self.duck_overrides[pond] = new
            self.db.commit()
            self.state_version += 1  # the config shows in /api/status

    def duck_config(self, pond: str) -> dict:
        """The Pond's EFFECTIVE compute config = override ?? declared ?? Catchment default (coalesce;
        plans/cloud-config.md). An effective ``duck_target`` naming a pool this Catchment doesn't have
        falls back to ``catchment`` (pond.toml stays portable)."""
        o = self.duck_overrides.get(pond) or _duck_override_row(None)
        decl = self.duck_declared.get(pond, {})
        d = self.duck_defaults()

        def pick(field):
            return o.get(field) if o.get(field) is not None else (
                decl.get(field) if decl.get(field) is not None else d.get(field))

        # duck_target: override wins; else the declared pool name; else the Catchment default.
        target = o["duck_target"] or decl.get("duck_target") or d["duck_target"]
        if target not in ("catchment", "dedicated") and target not in self.duck_pool_names:
            target = "catchment"  # an undefined pool → run locally (portable pond.toml)
        # Embed the resolved pool spec so the dispatching launcher can act without DB access; `remote`
        # is the routing signal (anything but the Catchment's own box).
        pool = self.get_pool(target) if target not in ("catchment", "dedicated") else None
        return {
            "duck_target": target,
            "remote": target != "catchment",
            "pool": pool,
            "dedicated_instance_type": o["dedicated_instance_type"],
            "dedicated_auto_stop": o["dedicated_auto_stop"],
            "flock_mode": pick("flock_mode"),
            "flock_engine": pick("flock_engine"),
            "oom_policy": pick("oom_policy"),
            "declared": decl,
            "override": {k: o[k] for k in self._DUCK_OVERRIDE_FIELDS},
            "defaults": d,
        }

    # ─── Duck Pools (Catchment-level named remote compute; plans/cloud-config.md) ──

    _POOL_FIELDS = ("provider", "instance_type", "cpu", "memory", "min_instances", "max_instances",
                    "idle_timeout", "keep_warm", "region")

    def _user_pools(self) -> list[dict]:
        rows = self.db.execute(
            "SELECT name, provider, instance_type, cpu, memory, min_instances, max_instances, "
            "idle_timeout, keep_warm, region FROM duck_pool ORDER BY name"
        ).fetchall()
        cols = ("name", *self._POOL_FIELDS)
        pools = [dict(zip(cols, r, strict=True)) for r in rows]
        for p in pools:
            p["provider"] = p["provider"] or "fargate"  # NULL → the default provider
            p["managed"] = False
        return pools

    def list_pools(self) -> list[dict]:
        """The presets (built-in Fargate S/M/L/XL, `managed`) followed by user-defined pools."""
        return [_preset_pool(n) for n in PRESET_POOLS] + self._user_pools()

    def add_pool(self, name: str, **fields) -> dict:
        """Create or update a user Duck Pool. Provider defaults to fargate; a preset name is reserved."""
        if not name or not name.strip():
            raise ValueError("a pool name is required")
        if name in PRESET_POOLS:
            raise ValueError(f"'{name}' is a built-in preset pool — pick another name")
        vals = {k: fields.get(k) for k in self._POOL_FIELDS}
        vals["provider"] = vals["provider"] or "fargate"
        if vals["provider"] not in POOL_PROVIDERS:
            raise ValueError(f"provider must be one of {', '.join(POOL_PROVIDERS)}")
        for k in ("min_instances", "max_instances", "keep_warm"):
            if vals[k] is not None and int(vals[k]) < 0:
                raise ValueError(f"{k} must be >= 0")
        mn, mx = vals["min_instances"], vals["max_instances"]
        if mn is not None and mx is not None and int(mn) > int(mx):
            raise ValueError("min_instances must be <= max_instances")
        with self.lock:
            self.db.execute(
                "INSERT INTO duck_pool (name, provider, instance_type, cpu, memory, min_instances, "
                "max_instances, idle_timeout, keep_warm, region) VALUES (:name, :provider, "
                ":instance_type, :cpu, :memory, COALESCE(:min_instances, 0), COALESCE(:max_instances, 1), "
                ":idle_timeout, COALESCE(:keep_warm, 0), :region) "
                "ON CONFLICT(name) DO UPDATE SET provider = excluded.provider, "
                "instance_type = excluded.instance_type, cpu = excluded.cpu, memory = excluded.memory, "
                "min_instances = excluded.min_instances, max_instances = excluded.max_instances, "
                "idle_timeout = excluded.idle_timeout, keep_warm = excluded.keep_warm, "
                "region = excluded.region",
                {"name": name, **vals},
            )
            self.db.commit()
            self.duck_pool_names.add(name)
            self.state_version += 1
        return self.get_pool(name)

    def get_pool(self, name: str) -> dict | None:
        if name in PRESET_POOLS:
            return _preset_pool(name)
        for p in self._user_pools():
            if p["name"] == name:
                return p
        return None

    def remove_pool(self, name: str) -> None:
        """Drop a user pool. Ponds pinned to it fall back to the Catchment Duck (the same unknown-pool
        rule as a portable pond.toml), so removal never strands a Pond. Presets can't be removed."""
        if name in PRESET_POOLS:
            raise ValueError(f"'{name}' is a built-in preset pool and can't be removed")
        with self.lock:
            self.db.execute("DELETE FROM duck_pool WHERE name = ?", (name,))
            self.db.commit()
            self.duck_pool_names.discard(name)
            self.state_version += 1

    # ─── Data serving: the exposure model + the served-major pointer (plans/data-serving.md) ──

    def _pond_ids(self, pond_key: str) -> tuple[int, int, int, str, int]:
        """(pond_id, pond_version_id, pond_name_id, name, major) for a pond key."""
        m = self.meta[pond_key]
        row = self.db.execute(
            "SELECT p.id, p.pond_version_id, p.pond_name_id, p.major FROM pond p WHERE p.id = ?",
            (m["pond_id"],),
        ).fetchone()
        return (row[0], row[1], row[2], m["name"], row[3])

    def _output_tables(self, pond_version_id: int) -> set[str]:
        return {r[0] for r in self.db.execute(
            'SELECT DISTINCT "table" FROM pond_version_schema WHERE pond_version_id = ?',
            (pond_version_id,))}

    def serve_detail(self, pond_key: str) -> list[dict]:
        """Per output table: its effective exposure + source (declared/override/hidden) — the Catalog
        table list. Effective = override ?? (table ∈ declared serviceable)."""
        pond_id, pv_id, _, _, _ = self._pond_ids(pond_key)
        declared = {r[0] for r in self.db.execute(
            "SELECT table_name FROM pond_version_serve WHERE pond_version_id = ?", (pv_id,))}
        overrides = {r[0]: bool(r[1]) for r in self.db.execute(
            "SELECT table_name, exposed FROM pond_serve_override WHERE pond_id = ?", (pond_id,))}
        tables = self._output_tables(pv_id) | set(overrides)
        out = []
        for t in sorted(tables):
            if t in overrides:
                exposed, source = overrides[t], "override"
            else:
                exposed, source = (t in declared), ("declared" if t in declared else "hidden")
            out.append({"table": t, "exposed": exposed, "source": source})
        return out

    def serviceable(self, pond_key: str) -> set[str]:
        """The effective exposed (serviceable) tables for a pond line — the serving core's allowlist."""
        return {d["table"] for d in self.serve_detail(pond_key) if d["exposed"]}

    def set_exposed(self, pond_key: str, table: str, exposed: bool | None) -> None:
        """Toggle the operational eye override for a table: True/False sets an explicit override,
        None clears it (reverts to the pond.toml-declared default). Persists on the pond line (survives
        a minor redeploy; a new major line starts fresh)."""
        pond_id = self.meta[pond_key]["pond_id"]
        with self.lock:
            if exposed is None:
                self.db.execute("DELETE FROM pond_serve_override WHERE pond_id = ? AND table_name = ?",
                                (pond_id, table))
            else:
                self.db.execute(
                    "INSERT INTO pond_serve_override (pond_id, table_name, exposed) VALUES (?, ?, ?) "
                    "ON CONFLICT(pond_id, table_name) DO UPDATE SET exposed = excluded.exposed",
                    (pond_id, table, int(exposed)))
            self.db.commit()
            self.state_version += 1

    def served_major(self, name: str) -> int | None:
        row = self.db.execute(
            "SELECT ps.served_major FROM pond_serve ps JOIN pond_name pn ON pn.id = ps.pond_name_id "
            "WHERE pn.name = ?", (name,)).fetchone()
        return row[0] if row else None

    def deployed_majors(self, name: str) -> list[int]:
        return [r[0] for r in self.db.execute(
            "SELECT p.major FROM pond p JOIN pond_name pn ON pn.id = p.pond_name_id "
            "WHERE pn.name = ? ORDER BY p.major", (name,))]

    def promote(self, name: str, major: int) -> None:
        """Flip the served-major pointer (blue-green). Validated: the target major must publish every
        table currently exposed on the served major, so a promotion never 404s a live query."""
        from ..keys import pond_key
        majors = self.deployed_majors(name)
        if major not in majors:
            raise ValueError(f"'{name}@{major}' is not deployed")
        current = self.served_major(name)
        if current is not None and current in majors:
            exposed_now = self.serviceable(pond_key(name, current))
            target_out = self._output_tables(self._pond_ids(pond_key(name, major))[1])
            missing = exposed_now - target_out
            if missing:
                raise ValueError(
                    f"'{name}@{major}' doesn't publish currently-served table(s): {', '.join(sorted(missing))}")
        with self.lock:
            (pn_id,) = self.db.execute("SELECT id FROM pond_name WHERE name = ?", (name,)).fetchone()
            self.db.execute(
                "INSERT INTO pond_serve (pond_name_id, served_major) VALUES (?, ?) "
                "ON CONFLICT(pond_name_id) DO UPDATE SET served_major = excluded.served_major",
                (pn_id, major))
            self.db.commit()
            self.state_version += 1

    def sleep(self, pond: str, upstream: bool = False) -> None:
        with self.lock:
            self.state = sleep_pond(self.state, pond, _now(), upstream=upstream)
            # Cancel any standing Wave/Tide trigger on every Pond the sleep reached, so it can't re-tap.
            for name in self._stop_set(pond, upstream):
                if self.state.triggers.pop(name, None) is not None:
                    self.db.execute("DELETE FROM pond_trigger WHERE pond_id = ?", (self.meta[name]["pond_id"],))
            self.db.commit()
            self._process(_now())

    def _stop_set(self, pond: str, upstream: bool) -> set[str]:
        """The Ponds a stop reaches: just the target, or the whole upstream ancestry."""
        seen: set[str] = set()
        queue = [pond]
        while queue:
            cur = queue.pop(0)
            if cur in seen:
                continue
            seen.add(cur)
            if upstream:
                queue.extend(sp for sp in self.state.ponds[cur].sources if sp not in seen)
        return seen

    def remove_trigger(self, pond: str) -> None:
        """Remove the standing Wave/Tide trigger from a Pond. Unlike stop, this leaves existing demand
        to drain naturally — it just stops new runs from being re-tapped/clocked."""
        with self.lock:
            self.state.triggers.pop(pond, None)
            self.db.execute(
                "DELETE FROM pond_trigger WHERE pond_id = ?", (self.meta[pond]["pond_id"],)
            )
            self.db.commit()
            self._process(_now())

    # ─── Windows (batch-availability on Inlets) ─────────────────────────────────

    def _row_to_window(self, row) -> Window:
        sa, dur, unit, interval, days, until = row
        return Window(
            start_anchor=datetime.fromisoformat(sa),
            duration=timedelta(seconds=dur),
            freq_unit=unit,
            freq_interval=interval,
            valid_days=frozenset(days.split(",")) if days else None,
            until=datetime.fromisoformat(until) if until else None,
        )

    def add_window(self, pond: str, name: str, start_anchor: str, duration_seconds: int,
                   freq_unit: str, freq_interval: int, valid_days: str | None = None,
                   until_time: str | None = None) -> None:
        """Add a recurring window to a Pond. Raises ValueError on a duplicate name or an overlap with
        an existing window (windows on a Pond must form a non-overlapping supply timeline)."""
        with self.lock:
            pond_id = self.meta[pond]["pond_id"]
            if self.db.execute(
                "SELECT 1 FROM pond_window WHERE pond_id = ? AND name = ?", (pond_id, name)
            ).fetchone():
                raise ValueError(f"A window named '{name}' already exists on '{pond}'")
            new_w = self._row_to_window(
                (start_anchor, duration_seconds, freq_unit, freq_interval, valid_days, until_time)
            )
            self._assert_no_overlap(pond, name, new_w)
            self.db.execute(
                "INSERT INTO pond_window (pond_id, name, start_anchor, duration_seconds, freq_unit, "
                "freq_interval, valid_days, until_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (pond_id, name, start_anchor, duration_seconds, freq_unit, freq_interval, valid_days, until_time),
            )
            self.db.commit()
            self.reload()

    def _assert_no_overlap(self, pond: str, name: str, new_w: Window) -> None:
        h0 = new_w.start_anchor
        h1 = h0 + timedelta(days=366)
        new_wins = new_w.occurrences(h0, h1, cap=500)
        for ew in self.state.ponds[pond].windows:
            for es, ee in ew.occurrences(h0, h1, cap=500):
                for ns, ne in new_wins:
                    if max(ns, es) < min(ne, ee):
                        raise ValueError(
                            f"Window '{name}' overlaps an existing window on '{pond}' "
                            f"({ns.isoformat()} – {ne.isoformat()})"
                        )

    def list_windows(self, pond: str) -> list[dict]:
        with self.lock:
            pond_id = self.meta[pond]["pond_id"]
            rows = self.db.execute(
                "SELECT name, start_anchor, duration_seconds, freq_unit, freq_interval, valid_days, "
                "until_time FROM pond_window WHERE pond_id = ? ORDER BY name", (pond_id,)
            ).fetchall()
            return [
                {"name": n, "start_anchor": sa, "duration_seconds": d, "freq_unit": u,
                 "freq_interval": i, "valid_days": vd, "until_time": ut}
                for (n, sa, d, u, i, vd, ut) in rows
            ]

    def remove_window(self, pond: str, name: str) -> bool:
        with self.lock:
            pond_id = self.meta[pond]["pond_id"]
            cur = self.db.execute(
                "DELETE FROM pond_window WHERE pond_id = ? AND name = ?", (pond_id, name)
            )
            self.db.commit()
            self.reload()
            return cur.rowcount > 0

    # ─── Spouts (egress bindings) ──────────────────────────────────────────────

    def _default_spout_name(self, source_name: str, scheme: str, table: str | None) -> str:
        """A friendly, unique-per-source name when the operator gives none: the table (or the scheme for
        an all-tables Spout), suffixed ``-2``, ``-3`` on collision."""
        base = table or scheme
        existing = {
            n.split("#", 1)[1]
            for (n,) in self.db.execute("SELECT name FROM pond_name WHERE name LIKE ?", (f"{source_name}#%",))
            if "#" in n
        }
        if base not in existing:
            return base
        i = 2
        while f"{base}-{i}" in existing:
            i += 1
        return f"{base}-{i}"

    def add_spout(self, pond: str, name: str | None, table: str | None,
                  destination: str, mode: str = "auto") -> str:
        """Bind a Spout to a Pond (its source): a real engine node (the egress dual of a Draw) that
        delivers ``table`` (or all tables) to ``destination`` in ``mode`` via the egress worker. Returns
        the Spout's (possibly generated) name. Raises ValueError on a bad destination/mode/duplicate."""
        from ..egress.destination import parse_destination, validate_mode
        from ..keys import split_pond_key

        dest = parse_destination(destination)
        validate_mode(mode)
        with self.lock:
            m = self.meta[pond]
            if dest.transactional:
                # A transactional destination does identity-based upsert/delete → it needs a primary key,
                # which only a merge Trickle declares. Reject what we can see now; a not-yet-published
                # source is caught at egress instead.
                self._assert_transactional_pk(m["name"], m["major"], table)
            src_name, major = split_pond_key(pond)
            final = name or self._default_spout_name(src_name, dest.scheme, table)
            if self.meta.get(pond_key(f"{src_name}#{final}", major), {}).get("is_spout"):
                raise ValueError(f"A spout named '{final}' already exists on '{pond}'")
            self._create_spout(src_name, major, final, destination, table, mode)
            self.db.commit()
            self.reload()
        self._signal_egress()
        return final

    def _resolve_spout(self, pond: str, name: str) -> str | None:
        from ..keys import split_pond_key

        src_name, major = split_pond_key(pond)
        skey = pond_key(f"{src_name}#{name}", major)
        return skey if self.meta.get(skey, {}).get("is_spout") else None

    def _assert_transactional_pk(self, name: str, major: int, table: str | None) -> None:
        """Reject a Spout to a transactional destination whose source table is already published without a
        primary key (a plain/overwrite Ripple). A table not yet published passes here — the worker enforces
        the requirement at egress time."""
        from pathlib import Path

        from ..trickle_io import load_sidecar
        from .registry import pond_data_dir

        sidecar = load_sidecar(pond_data_dir(Path(self.root), name, major, self.data_root))
        targets = [table] if table else list(sidecar)
        for t in targets:
            meta = sidecar.get(t)
            if meta is not None and (meta.get("mode") != "merge" or not meta.get("pk")):
                raise ValueError(
                    f"egress to a transactional destination needs a primary key — table '{t}' on '{name}' "
                    "is not a merge Trickle with a declared pk. Put a merge Trickle (.merge(pk=…)) upstream."
                )

    def list_spouts(self, pond: str) -> list[dict]:
        with self.lock:
            out = []
            for skey, sm in self.meta.items():
                if not sm.get("is_spout") or sm.get("source_key") != pond:
                    continue
                ps = self.state.pond_states[skey]
                cfg = sm.get("spout", {})
                err = None
                if ps.is_failed:
                    row = self.db.execute(
                        "SELECT error FROM pond_run WHERE pond_version_id = ? AND status = 'failed' "
                        "ORDER BY f DESC LIMIT 1", (sm["version_id"],),
                    ).fetchone()
                    err = row[0] if row else None
                out.append({
                    "name": sm["name"].split("#", 1)[1], "table": cfg.get("table"),
                    "destination": cfg.get("destination"), "mode": cfg.get("mode"),
                    "watermark": _iso(ps.end_f) if ps.end_f != NEVER else None,
                    "is_failed": ps.is_failed, "is_killed": ps.is_killed, "failures": ps.failures,
                    "error": err, "standing_wake": ps.standing_wake, "running": ps.start_f != ps.end_f,
                })
            out.sort(key=lambda s: s["name"])
            return out

    def remove_spout(self, pond: str, name: str) -> bool:
        with self.lock:
            skey = self._resolve_spout(pond, name)
            if skey is None:
                return False
            self._destroy_spout(skey)
            self.db.commit()
            self.reload()
        return True

    # The Control set on a Spout's standing Wake. Demand verbs (tap/wave/pulse/tide) do NOT apply — a
    # Spout never solicits and never takes demand. The Spout is a real node, so these mutate its engine
    # state (and persist the armed flag); failure/history flow through the normal pond_run path.
    def _spout_ctl(self, pond: str, name: str, fn) -> bool:
        with self.lock:
            skey = self._resolve_spout(pond, name)
            if skey is None:
                return False
            fn(skey, self.meta[skey]["pond_id"])
            self.db.commit()
            self._process(_now())
        self._signal_egress()
        return True

    def _arm(self, skey: str, pid: int, armed: bool) -> None:
        self.db.execute("UPDATE pond_spout SET armed = ? WHERE pond_id = ?", (1 if armed else 0, pid))
        self.state.pond_states[skey].standing_wake = armed

    def spout_wake(self, pond: str, name: str) -> bool:
        """Re-arm the standing Wake + clear any failure/kill — deliver on the next source advance."""
        def fn(skey, pid):
            self.state = clear_pond(self.state, skey, _now())  # roll the phantom + clear failed/killed
            self._arm(skey, pid, True)
        return self._spout_ctl(pond, name, fn)

    def spout_force(self, pond: str, name: str) -> bool:
        """Re-arm + re-deliver the current state now (reset the delivered freshness)."""
        def fn(skey, pid):
            self.state = clear_pond(self.state, skey, _now())
            self._arm(skey, pid, True)
            ps = self.state.pond_states[skey]
            ps.start_f = ps.end_f = NEVER  # re-fire from scratch (idempotent for the destination)
        return self._spout_ctl(pond, name, fn)

    def spout_sleep(self, pond: str, name: str) -> bool:
        """Disarm the standing Wake — no new deliveries (an in-flight one finishes)."""
        return self._spout_ctl(pond, name, lambda skey, pid: self._arm(skey, pid, False))

    def spout_kill(self, pond: str, name: str) -> bool:
        """Disarm + park (killed) until Wake/Force/Clear."""
        def fn(skey, pid):
            self.state = kill_pond(self.state, skey, _now())
            self._arm(skey, pid, False)
        return self._spout_ctl(pond, name, fn)

    def spout_clear(self, pond: str, name: str) -> bool:
        """Clear a failed/killed Spout (leaves its armed state unchanged)."""
        return self._spout_ctl(pond, name, lambda skey, pid: setattr(
            self, "state", clear_pond(self.state, skey, _now())))

    def resync_spout(self, pond: str, name: str) -> bool:
        """Force a full re-egress of the current state."""
        return self.spout_force(pond, name)

    # ─── Egress worker support (node dispatch) ──────────────────────────────────

    def take_spout_jobs(self) -> list[dict]:
        """Drain the Spout runs the engine dispatched (instead of sending them to a Duck). Each job
        carries the **source** Pond to read + the destination to write; the worker delivers out-of-lock
        and reports completion via complete_spout_run / fail_spout_run."""
        with self.lock:
            jobs = []
            for skey, f in self._pending_egress:
                sm = self.meta.get(skey)
                src_key = sm.get("source_key") if sm else None
                if src_key is None or src_key not in self.meta:
                    continue
                src, cfg = self.meta[src_key], sm.get("spout", {})
                # `f` is the Spout's run freshness (= the window end when windowed — the throttle clock);
                # `source_f` is the source's actual published freshness, which the data + CDC cursor ride.
                src_ps = self.state.pond_states.get(src_key)
                source_f = _iso(src_ps.end_f) if src_ps and src_ps.end_f > NEVER else _iso(f)
                # A table-less Spout egresses the source's SERVICEABLE set (its declared public products;
                # plans/data-serving.md) when serving is in play — resolved live per delivery. With NO
                # serviceable declaration it falls back to every table (the pre-serving behaviour, so an
                # existing egress isn't silently stopped). An explicit `table` still ships just that one.
                serviceable = set() if cfg.get("table") else self.serviceable(src_key)
                tables = sorted(serviceable) if serviceable else None
                jobs.append({
                    "spout_key": skey, "f": _iso(f), "source_f": source_f,
                    "pond_name": src["name"], "major": src["major"],
                    "table": cfg.get("table"), "tables": tables,
                    "destination": cfg.get("destination"), "mode": cfg.get("mode"),
                })
            self._pending_egress = []
            return jobs

    def complete_spout_run(self, spout_key: str, f: str) -> None:
        """The egress worker delivered freshness ``f``: complete the Spout's run (advances its freshness;
        records a success pond_run/ripple_run — the same history a Pond gets)."""
        with self.lock:
            now = _now()
            eid = f"{spout_key}.egress"
            rs = self.state.ripple_states.get(eid)
            started = _iso(rs.started_at) if rs and rs.started_at else _iso(now)
            if rs is not None:
                rs.start_f = datetime.fromisoformat(f)
                self.state = complete_ripple(self.state, eid, now)
                self._record_ripple_run(spout_key, "egress", f, "success",
                                        started_at=started, finished_at=_iso(now))
            self._finish_pond_run(spout_key, f, now)
            self._process(now, notify=False)

    def fail_spout_run(self, spout_key: str, f: str, error: str, tb: str | None = None) -> None:
        """The egress worker could not deliver: fail the Spout's run (a failed pond_run with the
        traceback — surfaced in /api/runs like any Pond — never touching the source)."""
        with self.lock:
            now = _now()
            eid = f"{spout_key}.egress"
            rs = self.state.ripple_states.get(eid)
            started = _iso(rs.started_at) if rs and rs.started_at else _iso(now)
            if rs is not None:
                rs.start_f = datetime.fromisoformat(f)
                self.state = fail_ripple(self.state, eid, now)
                self._record_ripple_run(spout_key, "egress", f, "failed",
                                        started_at=started, finished_at=_iso(now), error=error)
            self._fail_pond_run(spout_key, f, now, error, tb)
            # A Spout key is "{source}#{spout}@{major}": alert against the source Pond's name (so a
            # pond-scoped channel catches its spout failures too), naming the spout in the message.
            sm = self.meta.get(spout_key, {})
            src_key = sm.get("source_key")
            src_name = self.meta.get(src_key, {}).get("name") if src_key else None
            spout_label = sm.get("name", spout_key)
            self._alert_failure(
                spout_key, "spout", scope_pond=src_name,
                scope_major=self.meta.get(src_key, {}).get("major"), f=f,
                title=f"Spout '{spout_label}' delivery failed",
                message=f"Egress delivery for spout '{spout_label}' failed: {error or 'unknown error'}",
            )
            self._process(now, notify=False)

    # ─── Alerts (failure & freshness notifications — see plans/alerts.md) ────────

    def add_channel(self, name: str, destination: str, scope: str | None,
                    events: str = "all", stale_ms: int | None = None,
                    renotify_ms: int | None = None) -> None:
        """Create a notification channel (operational config, like a Spout). ``scope`` is ``"name@major"``
        (one line) or ``None`` (catchment-wide) — a Pond-scoped channel is always a specific major. A bare
        ``"name"`` resolves to the Pond's **highest deployed major** (like every other CLI/API surface); an
        explicit ``"name@major"`` may precede deployment. Validates the destination URI + the event list."""
        from ..alerts import normalise_events, parse_notifier_destination

        parse_notifier_destination(destination)  # scheme + ${...} syntax (does not resolve credentials)
        events = ",".join(normalise_events(events)) if events else "all"
        scope_name, scope_major = _split_scope(scope)
        if scope_name is not None and scope_major is None:  # bare name → its highest deployed major
            row = self.db.execute(
                "SELECT MAX(p.major) FROM pond p JOIN pond_name pn ON pn.id = p.pond_name_id WHERE pn.name = ?",
                (scope_name,),
            ).fetchone()
            if row is None or row[0] is None:
                raise ValueError(f"Pond '{scope_name}' is not deployed — give a major, e.g. '{scope_name}@1'")
            scope_major = row[0]
        with self.lock:
            existing = self.db.execute("SELECT 1 FROM alert_channel WHERE name = ?", (name,)).fetchone()
            if existing:
                raise ValueError(f"An alert channel named '{name}' already exists")
            self.db.execute(
                "INSERT INTO alert_channel (name, destination, scope_name, scope_major, events, stale_ms, "
                "renotify_ms) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (name, destination, scope_name, scope_major, events, stale_ms, renotify_ms),
            )
            self.db.commit()

    def list_channels(self) -> list[dict]:
        with self.lock:
            rows = self.db.execute(
                "SELECT name, destination, scope_name, scope_major, events, stale_ms, enabled, created_at, "
                "renotify_ms FROM alert_channel ORDER BY name"
            ).fetchall()
            return [
                {"name": r[0], "destination": r[1], "scope": _format_scope(r[2], r[3]), "events": r[4],
                 "stale_ms": r[5], "enabled": bool(r[6]), "created_at": r[7], "renotify_ms": r[8]}
                for r in rows
            ]

    def remove_channel(self, name: str) -> bool:
        with self.lock:
            cur = self.db.execute("DELETE FROM alert_channel WHERE name = ?", (name,))
            self.db.commit()
            return cur.rowcount > 0

    def channel_destination(self, name: str) -> str | None:
        with self.lock:
            row = self.db.execute("SELECT destination FROM alert_channel WHERE name = ?", (name,)).fetchone()
            return row[0] if row else None

    def deliveries(self, limit: int = 100) -> list[dict]:
        """Recent alert deliveries (the audit log) — newest first."""
        with self.lock:
            rows = self.db.execute(
                "SELECT c.name, d.event_kind, d.pond_name, d.severity, d.status, d.attempts, d.error, "
                "d.created_at, d.sent_at FROM alert_delivery d JOIN alert_channel c ON c.id = d.channel_id "
                "ORDER BY d.id DESC LIMIT ?", (min(int(limit), 1000),),
            ).fetchall()
            return [
                {"channel": r[0], "kind": r[1], "pond": r[2], "severity": r[3], "status": r[4],
                 "attempts": r[5], "error": r[6], "created_at": r[7], "sent_at": r[8]}
                for r in rows
            ]

    def take_alert_deliveries(self, limit: int = 50) -> list[dict]:
        """Pending deliveries for the alert worker to send: each carries its channel's destination + the
        rendered payload. The worker delivers out-of-lock and reports via mark_delivery_sent/failed."""
        with self.lock:
            rows = self.db.execute(
                "SELECT d.id, c.destination, d.payload, d.attempts FROM alert_delivery d "
                "JOIN alert_channel c ON c.id = d.channel_id WHERE d.status = 'pending' "
                "ORDER BY d.id LIMIT ?", (int(limit),),
            ).fetchall()
            return [{"id": r[0], "destination": r[1], "payload": json.loads(r[2]), "attempts": r[3]} for r in rows]

    def mark_delivery_sent(self, delivery_id: int) -> None:
        with self.lock:
            self.db.execute(
                "UPDATE alert_delivery SET status = 'sent', sent_at = ?, error = NULL WHERE id = ?",
                (_iso(_now()), delivery_id),
            )
            self.db.commit()

    def mark_delivery_failed(self, delivery_id: int, error: str, max_attempts: int) -> None:
        """Record a failed send: bump attempts, park 'failed' at the cap (stop retrying a dead channel,
        but keep the row auditable), else leave 'pending' for the next worker tick."""
        with self.lock:
            row = self.db.execute("SELECT attempts FROM alert_delivery WHERE id = ?", (delivery_id,)).fetchone()
            if row is None:
                return
            attempts = row[0] + 1
            status = "failed" if attempts >= max_attempts else "pending"
            self.db.execute(
                "UPDATE alert_delivery SET attempts = ?, status = ?, error = ? WHERE id = ?",
                (attempts, status, error, delivery_id),
            )
            self.db.commit()

    def _catchment_display(self) -> str | None:
        name = self.db.execute("SELECT value FROM catchment_meta WHERE key = 'name'").fetchone()
        return name[0] if name and name[0] else None

    def _emit_alert(self, kind: str, *, scope_pond: str | None, scope_major: int | None = None,
                    severity: str, title: str, message: str, f: str | None = None,
                    detail: dict | None = None, match_kinds: tuple[str, ...] | None = None) -> None:
        """Enqueue one ``alert_delivery`` per matching enabled channel (dedup-fenced), then wake the alert
        worker. Matching = the channel's scope (catchment-wide, or this Pond's name) AND its ``events``
        including any of ``match_kinds`` (default: just ``kind``). A ``recovery`` passes the originating kind
        too, so a channel subscribed to ``failure``/``freshness`` also hears when it clears — without having
        to also subscribe to ``recovery``. Wrapped so a bug in alerting can never break a Pond Run."""
        try:
            from ..alerts import AlertEvent, normalise_events

            wanted = match_kinds or (kind,)
            channels = self.db.execute(
                "SELECT id, events, scope_name, scope_major, renotify_ms FROM alert_channel WHERE enabled = 1"
            ).fetchall()
            if not channels:
                return
            enqueued = False
            for cid, events, scope_name, ch_major, renotify_ms in channels:
                # A Pond-scoped channel matches exactly its (name, major); catchment-wide (name NULL) matches all.
                if scope_name is not None and (scope_name != scope_pond or ch_major != scope_major):
                    continue
                subscribed = normalise_events(events)
                if not any(k in subscribed for k in wanted):
                    continue
                event = AlertEvent(
                    kind=kind, pond=scope_pond, title=title, message=message, severity=severity,
                    f=f, catchment=self._catchment_display(), detail=detail or {},
                )
                dedup = f"{kind}:{scope_pond or '-'}:{f or '-'}"
                # Re-notify cadence (opt-in): a channel with renotify_ms fences per TIME BUCKET, not per
                # episode — the tick's re-emission then passes the UNIQUE fence once per interval while
                # the episode persists. A recovery stays once-per-episode (nothing re-fires it).
                if renotify_ms and kind != "recovery":
                    bucket = int(_now().timestamp() * 1000 // int(renotify_ms))
                    dedup = f"{dedup}:r{bucket}"
                cur = self.db.execute(
                    "INSERT OR IGNORE INTO alert_delivery "
                    "(channel_id, dedup_key, event_kind, pond_name, severity, payload) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (cid, dedup, kind, scope_pond, severity, json.dumps(event.to_payload())),
                )
                enqueued = enqueued or cur.rowcount > 0
            self.db.commit()
            if enqueued:
                self._signal_alert()
        except Exception as exc:  # noqa: BLE001 — alerting must never break the engine
            print(f"[catchment] alert emit failed ({kind}): {exc}", flush=True)

    def _alert_failure(self, key: str, kind: str, *, scope_pond: str | None, scope_major: int | None,
                       f: str | None, title: str, message: str) -> None:
        """Fire a failure/contract/spout alert and remember the key so its recovery is emitted when it
        clears (in _process). ``scope_pond``/``scope_major`` are the Pond name@major a channel scopes
        against."""
        blocked = sorted(
            self.meta[k]["name"] for k, ps in self.state.pond_states.items()
            if ps.is_blocked and k in self.meta and not self.meta[k].get("is_spout")
        )
        detail = {"blocked_downstream": blocked} if blocked else {}
        self._emit_alert(kind, scope_pond=scope_pond, scope_major=scope_major, severity="error",
                         title=title, message=message, f=f, detail=detail)
        # Remember the episode (for the recovery on clear, and for _check_renotify's re-emission —
        # title/message ride along so a re-notify repeats the original alert verbatim).
        self._alerted_failures[key] = (kind, scope_pond, scope_major, f, title, message)

    def _emit_recoveries(self) -> None:
        """Emit a `recovery` for any Pond/Spout that was alerted as failed and has since cleared. Called
        from _process, so every clear path (a fresher run, wake/force/clear, redeploy) is covered once."""
        for key in list(self._alerted_failures):
            ps = self.state.pond_states.get(key)
            if ps is not None and (ps.is_failed or ps.is_killed):
                continue  # still down (or killed — a kill is intentional, not a recovery)
            kind, scope_pond, scope_major, _f, _title, _message = self._alerted_failures.pop(key)
            label = self.meta.get(key, {}).get("name", key)
            self._emit_alert(
                "recovery", scope_pond=scope_pond, scope_major=scope_major, severity="info",
                title=f"Recovered: {label}", message=f"'{label}' recovered from a {kind} failure.",
                f=_iso(ps.end_f) if ps is not None and ps.end_f != NEVER else None,
                match_kinds=("recovery", kind),  # also reaches channels that only asked for the failure kind
            )

    def _check_freshness(self, now: datetime) -> None:
        """The tick-driven freshness-SLA sweep (alongside _check_liveness). For each enabled channel with a
        ``stale_ms`` bound, alert a scoped Pond whose staleness exceeds it, and recover it when it advances."""
        channels = self.db.execute(
            "SELECT id, scope_name, scope_major, events, stale_ms FROM alert_channel "
            "WHERE enabled = 1 AND stale_ms IS NOT NULL"
        ).fetchall()
        if not channels:
            return
        self._renotify_channels = self._any_renotify()  # computed once per sweep
        for cid, scope_name, ch_major, events, stale_ms in channels:
            from ..alerts import normalise_events
            if "freshness" not in normalise_events(events):
                continue
            bound = timedelta(milliseconds=stale_ms)
            for key, ps in self.state.pond_states.items():
                m = self.meta.get(key)
                if m is None or m.get("is_spout") or m.get("is_draw"):
                    continue
                name, major = m["name"], m["major"]
                if scope_name is not None and (scope_name != name or ch_major != major):
                    continue
                if ps.end_f == NEVER:  # never run → nothing to be stale about yet
                    continue
                stale = (now - ps.end_f) > bound
                token = (cid, key)
                if stale:
                    first = token not in self._stale_breached
                    self._stale_breached.add(token)
                    # Emitted on the transition AND on every later tick while breached: for a channel
                    # without renotify_ms the dedup fence swallows the repeats (once per episode); with
                    # it, the bucketed key re-fires once per interval (the re-notify cadence).
                    age = int((now - ps.end_f).total_seconds())
                    if first or self._renotify_channels:
                        self._emit_alert(
                            "freshness", scope_pond=name, scope_major=major, severity="warning",
                            title=f"'{name}' is stale", f=_iso(ps.end_f),
                            message=f"'{name}' has not been fresh for {age}s (SLA {int(stale_ms / 1000)}s).",
                            detail={"stale_seconds": age},
                        )
                elif not stale and token in self._stale_breached:
                    self._stale_breached.discard(token)
                    self._emit_alert(
                        "recovery", scope_pond=name, scope_major=major, severity="info", f=_iso(ps.end_f),
                        title=f"'{name}' is fresh again",
                        message=f"'{name}' advanced back within its freshness SLA.",
                        match_kinds=("recovery", "freshness"),
                    )

    def _any_renotify(self) -> bool:
        return self.db.execute(
            "SELECT 1 FROM alert_channel WHERE enabled = 1 AND renotify_ms IS NOT NULL LIMIT 1"
        ).fetchone() is not None

    def _check_renotify(self, now: datetime) -> None:
        """The tick-driven re-notify sweep (alongside _check_freshness): while a **failure episode
        persists** (the Pond is still failed), re-emit the original alert. Channels without
        ``renotify_ms`` swallow the repeat at their once-per-episode dedup fence; channels with it
        re-fire once per interval via the time-bucketed key (see _emit_alert). Freshness re-notify rides
        _check_freshness's own sweep. No-op without a renotify channel — zero cost on the default path."""
        if not self._alerted_failures or not self._any_renotify():
            return
        for key, (kind, scope_pond, scope_major, f, title, message) in list(self._alerted_failures.items()):
            ps = self.state.pond_states.get(key)
            if ps is None or not ps.is_failed or ps.is_killed:
                continue  # cleared (recovery handles it) or killed (intentional — never re-notified)
            self._emit_alert(kind, scope_pond=scope_pond, scope_major=scope_major, severity="error",
                             title=title, message=message, f=f)

    # ─── Duck events ──────────────────────────────────────────────────────────

    def on_event(self, pond: str, payload: dict) -> None:
        with self.lock:
            now = _now()
            self.last_seen[pond] = now  # any event proves the Duck is alive
            kind = payload.get("kind")
            f = payload.get("f")
            status = payload.get("status", "success")
            if kind == "ripple":
                rname = payload["ripple"]
                eid = f"{pond}.{rname}"
                if eid in self.state.ripple_states:
                    # Trust the Duck's run freshness: stamp start_f from the event so the completion is
                    # recorded correctly even for a resumed run the Catchment didn't model the start of.
                    if f:
                        self.state.ripple_states[eid].start_f = datetime.fromisoformat(f)
                    if status == "success":
                        # changed=False on the Run-completing ripple event holds this Pond's changed_f
                        # (a pass: pond.skip() / empty delta) so downstream skips. See no-change-skip.md.
                        self.state = complete_ripple(self.state, eid, now, changed=payload.get("changed", True))
                    # A "failed" ripple event is a within-budget immediate retry: record the attempt for
                    # history; the engine keeps modelling the Ripple as in-flight (the Duck relaunched it).
                    self._record_ripple_run(
                        pond, rname, f, status,
                        started_at=payload.get("started_at"),
                        finished_at=payload.get("finished_at") or _iso(now),
                        retry=payload.get("retry", 0),
                        error=payload.get("error"), traceback=payload.get("traceback"),
                    )
                    if payload.get("lineage"):
                        self._record_lineage(pond, rname, f, payload.get("retry", 0), payload["lineage"])
                    self._process(now)
            elif kind == "failed":
                # The Pond Run gave up at this Ripple's freshness: fail the Pond (and block downstream).
                rname = payload["ripple"]
                eid = f"{pond}.{rname}"
                if eid in self.state.ripple_states:
                    if f:
                        self.state.ripple_states[eid].start_f = datetime.fromisoformat(f)
                    self.state = fail_ripple(self.state, eid, now)
                    err, tb = payload.get("error"), payload.get("traceback")
                    self._fail_pond_run(pond, f, now, err, tb)  # upsert the pond_run row first (ripple_run FK)
                    self._record_ripple_run(
                        pond, rname, f, "failed",
                        started_at=payload.get("started_at"),
                        finished_at=payload.get("finished_at") or _iso(now),
                        retry=payload.get("retry", 0),
                        error=err, traceback=tb,
                    )
                    self._process(now)  # settle the cascade first, so the blast radius is accurate
                    name = self.meta.get(pond, {}).get("name", pond)
                    self._alert_failure(pond, "failure", scope_pond=name,
                                        scope_major=self.meta.get(pond, {}).get("major"), f=f,
                                        title=f"Pond '{name}' failed",
                                        message=f"Ripple '{rname}' failed: {err or 'unknown error'}")
            elif kind == "missing_source":
                # A Ripple read a Source asset that isn't published (deleted, or not produced yet). Park the
                # Pond blocked-with-a-reason — NOT failed: no retry-budget burn, no failure alert. It
                # recovers when the Source republishes something fresher. See plans/reset.md.
                rname = payload["ripple"]
                eid = f"{pond}.{rname}"
                if eid in self.state.ripple_states:
                    if f:
                        self.state.ripple_states[eid].start_f = datetime.fromisoformat(f)
                    reason = f"{payload.get('source')}.{payload.get('table')}"
                    self.state = block_on_missing_asset(self.state, pond, reason, now)
                    msg = f"waiting for '{reason}' to be published"
                    self._fail_pond_run(pond, f, now, msg, None)  # history: the Run couldn't complete
                    self._record_ripple_run(
                        pond, rname, f, "failed",
                        started_at=payload.get("started_at"),
                        finished_at=payload.get("finished_at") or _iso(now),
                        retry=payload.get("retry", 0), error=msg,
                    )
                    self._process(now)
            elif kind == "run_completed":
                self.state = clear_missing_asset(self.state, pond)  # a clean read → the wait (if any) is over
                self._finish_pond_run(pond, f, now, changed=payload.get("changed", True))
                # Freeze the published output schema as the version's contract (the substrate the
                # additive gate and min_version enforcement build on).
                if payload.get("schema"):
                    self._capture_schema(pond, payload["schema"])
                self._emit_openlineage(pond, f, payload.get("schema"))  # catalog emission (opt-in channels)
                self._process(now)
                self._advance_repair(pond, now)  # if this Pond was a repair step, release its children
                self._signal_egress()  # the Pond published → wake the egress worker for its Spouts
            elif kind == "contract_failed":
                # The Duck refused to publish: the output broke the major line's additive contract.
                # Fail the Pond at this Run (keeping last-good data) and block downstream, like any failure.
                self._fail_whole_pond(pond, now, payload.get("error"), None, alert_kind="contract")
            elif kind == "pond_failed":
                # A Duck-level error (e.g. a failed ledger write): fail the whole Pond at its most
                # recently started Run. The Duck exits after reporting; liveness will not double-fail.
                self._fail_whole_pond(pond, now, payload.get("error"), payload.get("traceback"))

    def resume_incomplete(self) -> None:
        """Re-dispatch Pond Runs that were in flight when the Catchment stopped, and service any
        restored demand. The Duck reconciles each run against its ledger (re-running only the
        incomplete Ripples) and replays the completions the Catchment missed. Call once at startup."""
        with self.lock:
            now = _now()
            for name, f in self._incomplete:
                self._dispatch_begin_run(name, f, now)
            self._incomplete = []
            self._process(now)

    def take_jobs(self, pond: str) -> list[dict]:
        with self.lock:
            self.last_seen[pond] = _now()  # the Duck is alive — it just polled
            jobs = self.jobs.get(pond, [])
            self.jobs[pond] = []
            return jobs

    # ─── Pond Draws (cross-Catchment) ───────────────────────────────────────────

    def draws(self) -> list[dict]:
        """Every Pond Draw, for the poller: its key/name/major and whether downstream demand wants
        the upstream solicited (a pull/push is pending but the upstream hasn't offered it yet)."""
        with self.lock:
            out = []
            for key, m in self.meta.items():
                if not m.get("is_draw"):
                    continue
                ps = self.state.pond_states[key]
                real_targets = [t for t in ps.targets if t > NEVER]
                if ps.remote_down:
                    target = pull_m = None  # blocked upstream: solicit nothing
                else:
                    # Forward the draw's outstanding demand upstream carrying its epoch, so the upstream
                    # Inlet mints the SAME freshness: the max push target, and the pull epoch.
                    target = _iso(max(real_targets)) if real_targets else None
                    pull_m = _iso(ps.pull_m) if (ps.has_pull and ps.pull_m > NEVER) else None
                out.append({
                    "key": key, "name": m["name"], "major": m["major"],
                    "target": target, "pull_m": pull_m,
                })
            return out

    def observe_remote(
        self, pond: str, remote_f: datetime | None, *, down: bool = False,
    ) -> None:
        """The poller reports an upstream Pond's freshness + reachability for a Draw. Mirror them and
        run the cascade — a transfer starts if there is downstream demand and the upstream is fresher."""
        with self.lock:
            ps = self.state.pond_states.get(pond)
            if ps is None or not self.meta.get(pond, {}).get("is_draw"):
                return
            if remote_f is not None:
                ps.remote_f = remote_f
            if ps.remote_down != down:
                ps.remote_down = down
                derive_blocked(self.state, pond)
            self._process(_now(), notify=False)  # poller-driven; transfers handled in this cycle

    def pond_observation(self, pond: str) -> dict:
        """A Pond's freshness + down-state, for the producer's ``…/wait`` long-poll (a downstream
        Catchment blocks on this until its drawn Pond advances)."""
        with self.lock:
            ps = self.state.pond_states.get(pond)
            if ps is None:
                return {"end_f": None, "down": False}
            down = ps.is_failed or ps.is_killed or ps.is_blocked
            return {"end_f": _iso(ps.end_f) if ps.end_f != NEVER else None, "down": down}

    def take_transfers(self) -> list[dict]:
        """Drain the Pond Draw transfers the poller should perform (fetch + land the parquet)."""
        with self.lock:
            out = []
            for key, f in self._pending_transfers:
                m = self.meta.get(key)
                if m is not None:
                    out.append({"key": key, "name": m["name"], "major": m["major"], "f": _iso(f)})
            self._pending_transfers = []
            return out

    def complete_draw_transfer(self, pond: str, f: str) -> None:
        """The poller finished landing a Draw's parquet at freshness ``f``: complete its transfer
        ripple (advancing the Draw's freshness, which cascades to downstream Sinks)."""
        with self.lock:
            now = _now()
            eid = f"{pond}.draw"
            rs = self.state.ripple_states.get(eid)
            if rs is None:
                return
            started = _iso(rs.started_at) if rs.started_at else _iso(now)
            rs.start_f = datetime.fromisoformat(f)
            self.state = complete_ripple(self.state, eid, now)
            self._record_ripple_run(pond, "draw", f, "success", started_at=started, finished_at=_iso(now))
            self._finish_pond_run(pond, f, now)
            self._process(now, notify=False)  # poller-driven

    def fail_draw_transfer(self, pond: str, f: str, error: str) -> None:
        """The poller could not land a Draw's parquet: fail the transfer (blocks downstream until the
        next successful poll/transfer)."""
        with self.lock:
            now = _now()
            eid = f"{pond}.draw"
            rs = self.state.ripple_states.get(eid)
            if rs is None:
                return
            started = _iso(rs.started_at) if rs.started_at else _iso(now)
            rs.start_f = datetime.fromisoformat(f)
            self.state = fail_ripple(self.state, eid, now)
            self._fail_pond_run(pond, f, now, error, None)
            self._record_ripple_run(pond, "draw", f, "failed", started_at=started,
                                    finished_at=_iso(now), error=error)
            self._process(now, notify=False)  # poller-driven

    # ─── Producer exposure (open / tap-on-get) ──────────────────────────────────

    def set_pond_open(self, pond: str, tap_on_get: bool) -> None:
        """Mark a Pond open (accepts demand from any source). Under single-level auth this is a no-op
        gate; its live effect is ``tap_on_get`` (a read on the query route fires a Tap)."""
        with self.lock:
            pid = self.meta[pond]["pond_id"]
            self.db.execute(
                "INSERT INTO pond_open (pond_id, tap_on_get) VALUES (?, ?) "
                "ON CONFLICT(pond_id) DO UPDATE SET tap_on_get = excluded.tap_on_get",
                (pid, int(tap_on_get)),
            )
            self.db.commit()

    def unset_pond_open(self, pond: str) -> None:
        with self.lock:
            self.db.execute("DELETE FROM pond_open WHERE pond_id = ?", (self.meta[pond]["pond_id"],))
            self.db.commit()

    def pond_tap_on_get(self, pond: str) -> bool:
        with self.lock:
            m = self.meta.get(pond)
            if m is None:
                return False
            row = self.db.execute(
                "SELECT tap_on_get FROM pond_open WHERE pond_id = ?", (m["pond_id"],)
            ).fetchone()
            return bool(row and row[0])

    # ─── Ducts (consumer side) ───────────────────────────────────────────────────

    def create_duct(
        self, origin: str, remote_url: str, auth_headers: dict | None, upstream_id: str | None = None
    ) -> None:
        """Register (or update) a conduit from an upstream Catchment. ``auth_headers`` are the request
        headers to attach when dialling it — a secret at rest (duck.db is 0600). ``upstream_id`` is the
        upstream's stable identity (for cross-mesh edge resolution + cycle cutting)."""
        with self.lock:
            self.db.execute(
                "INSERT INTO duct (origin_catchment, remote_url, auth_json, upstream_id) "
                "VALUES (?, ?, ?, ?) ON CONFLICT(origin_catchment) DO UPDATE SET "
                "remote_url = excluded.remote_url, auth_json = excluded.auth_json, "
                "upstream_id = excluded.upstream_id",
                (origin, remote_url, json.dumps(auth_headers) if auth_headers else None, upstream_id),
            )
            self.db.commit()

    def destroy_duct(self, origin: str) -> bool:
        with self.lock:
            row = self.db.execute("SELECT id FROM duct WHERE origin_catchment = ?", (origin,)).fetchone()
            if row is None:
                return False
            duct_id = row[0]
            for src_name, major in self.db.execute(
                "SELECT source_pond_name, major FROM duct_to_pond WHERE duct_id = ?", (duct_id,)
            ).fetchall():
                self._destroy_draw(src_name, major)
            self.db.execute("DELETE FROM duct_to_pond WHERE duct_id = ?", (duct_id,))
            self.db.execute("DELETE FROM duct WHERE id = ?", (duct_id,))
            self.db.commit()
            self.reload()
            return True

    def add_duct_pond(self, origin: str, pond_name: str, major: int, incremental: bool = False) -> None:
        with self.lock:
            row = self.db.execute("SELECT id FROM duct WHERE origin_catchment = ?", (origin,)).fetchone()
            if row is None:
                raise KeyError(f"No duct from '{origin}' — create it first")
            self._create_draw(pond_name, major)  # raises ValueError on a local-Pond collision
            self.db.execute(
                "INSERT OR REPLACE INTO duct_to_pond (duct_id, source_pond_name, major, incremental) "
                "VALUES (?, ?, ?, ?)",
                (row[0], pond_name, major, int(incremental)),
            )
            self.db.commit()
            self.reload()

    def remove_duct_pond(self, origin: str, pond_name: str, major: int) -> bool:
        with self.lock:
            row = self.db.execute("SELECT id FROM duct WHERE origin_catchment = ?", (origin,)).fetchone()
            if row is None:
                return False
            cur = self.db.execute(
                "DELETE FROM duct_to_pond WHERE duct_id = ? AND source_pond_name = ? AND major = ?",
                (row[0], pond_name, major),
            )
            self._destroy_draw(pond_name, major)
            self.db.commit()
            self.reload()
            return cur.rowcount > 0

    def list_ducts(self) -> list[dict]:
        """Ducts + their drawn Ponds, for the CLI/API (auth redacted)."""
        with self.lock:
            out = []
            for did, origin, url in self.db.execute(
                "SELECT id, origin_catchment, remote_url FROM duct ORDER BY origin_catchment"
            ).fetchall():
                members = [
                    {"pond": n, "major": mj, "incremental": bool(inc)}
                    for n, mj, inc in self.db.execute(
                        "SELECT source_pond_name, major, incremental FROM duct_to_pond "
                        "WHERE duct_id = ? ORDER BY source_pond_name, major", (did,)
                    )
                ]
                out.append({"origin": origin, "remote_url": url, "ponds": members})
            return out

    def duct_targets(self) -> list[dict]:
        """Ducts with auth resolved — for the poller only (never serialised to a client)."""
        with self.lock:
            out = []
            for did, origin, url, auth_json, upstream_id in self.db.execute(
                "SELECT id, origin_catchment, remote_url, auth_json, upstream_id FROM duct"
            ).fetchall():
                members = []
                for n, mj in self.db.execute(
                    "SELECT source_pond_name, major FROM duct_to_pond WHERE duct_id = ?", (did,)
                ):
                    ps = self.state.pond_states.get(pond_key(n, mj))
                    rf = ps.remote_f if ps is not None else NEVER
                    members.append({
                        "name": n, "major": mj,
                        "remote_f": _iso(rf) if rf != NEVER else None,  # the poller's wait baseline
                        "remote_down": ps.remote_down if ps is not None else False,  # last-known down-state
                    })
                out.append({
                    "origin": origin, "remote_url": url, "upstream_id": upstream_id,
                    "auth": json.loads(auth_json) if auth_json else {},
                    "members": members,
                })
            return out

    def _create_draw(self, name: str, major: int) -> None:
        """Materialise a Pond Draw's identity rows (caller holds the lock and reloads). Real but
        synthetic: kind='inlet', is_draw=1, a single immutable pond_version + one ``"draw"`` ripple."""
        db = self.db
        db.execute("INSERT OR IGNORE INTO pond_name (name, kind) VALUES (?, 'inlet')", (name,))
        db.execute("UPDATE pond_name SET kind = 'inlet' WHERE name = ?", (name,))
        (pn_id,) = db.execute("SELECT id FROM pond_name WHERE name = ?", (name,)).fetchone()

        existing = db.execute(
            "SELECT is_draw FROM pond WHERE pond_name_id = ? AND major = ?", (pn_id, major)
        ).fetchone()
        if existing is not None and not existing[0]:
            raise ValueError(f"A local Pond '{name}@{major}' already exists — cannot draw it over a duct")

        version = f"{major}.0.0"
        db.execute(
            "INSERT OR IGNORE INTO pond_version (pond_name_id, version, major, source_path) "
            "VALUES (?, ?, ?, ?)",
            (pn_id, version, major, f"draw://{name}@{major}"),
        )
        (pv_id,) = db.execute(
            "SELECT id FROM pond_version WHERE pond_name_id = ? AND version = ?", (pn_id, version)
        ).fetchone()
        db.execute("INSERT OR IGNORE INTO ripple (pond_version_id, name) VALUES (?, 'draw')", (pv_id,))
        db.execute(
            "INSERT INTO pond (pond_name_id, major, pond_version_id, is_draw) VALUES (?, ?, ?, 1) "
            "ON CONFLICT(pond_name_id, major) DO UPDATE SET pond_version_id = excluded.pond_version_id, "
            "is_draw = 1",
            (pn_id, major, pv_id),
        )

    def _destroy_draw(self, name: str, major: int) -> None:
        """Remove a Pond Draw's identity + state rows (caller holds the lock and reloads). Leaves the
        ``pond_name`` placeholder so a Sink that still references it keeps its source row."""
        db = self.db
        row = db.execute("SELECT id FROM pond_name WHERE name = ?", (name,)).fetchone()
        if row is None:
            return
        pn_id = row[0]
        prow = db.execute(
            "SELECT id, pond_version_id, is_draw FROM pond WHERE pond_name_id = ? AND major = ?",
            (pn_id, major),
        ).fetchone()
        if prow is None or not prow[2]:
            return  # not a Draw — never remove a real local Pond here
        pond_id, pv_id = prow[0], prow[1]
        db.execute("DELETE FROM ripple_run WHERE pond_version_id = ?", (pv_id,))
        db.execute("DELETE FROM pond_run WHERE pond_version_id = ?", (pv_id,))
        db.execute("DELETE FROM ripple_run_lineage WHERE pond_version_id = ?", (pv_id,))
        for tbl in ("pond_state", "pond_target", "pond_open", "pond_trigger", "pond_retry", "pond_window"):
            db.execute(f"DELETE FROM {tbl} WHERE pond_id = ?", (pond_id,))
        db.execute("DELETE FROM pond WHERE id = ?", (pond_id,))
        rids = [r[0] for r in db.execute("SELECT id FROM ripple WHERE pond_version_id = ?", (pv_id,))]
        if rids:
            marks = ",".join("?" * len(rids))
            db.execute(
                f"DELETE FROM ripple_to_ripple WHERE sink_id IN ({marks}) OR source_id IN ({marks})",
                rids * 2,
            )
        db.execute("DELETE FROM ripple WHERE pond_version_id = ?", (pv_id,))
        db.execute("DELETE FROM pond_version WHERE id = ?", (pv_id,))

    def _create_spout(self, source_name: str, source_major: int, spout: str,
                      destination: str, table: str | None, mode: str) -> str:
        """Materialise a Spout's identity rows (caller holds the lock and reloads). The egress dual of a
        Draw: kind='outlet', is_spout=1, a synthetic pond_version + one 'egress' ripple, wired to its
        source via pond_to_pond. Returns the Spout's pond key. Mirrors :meth:`_create_draw`."""
        db = self.db
        sname = f"{source_name}#{spout}"
        db.execute("INSERT OR IGNORE INTO pond_name (name, kind) VALUES (?, 'outlet')", (sname,))
        (pn_id,) = db.execute("SELECT id FROM pond_name WHERE name = ?", (sname,)).fetchone()
        (src_pn_id,) = db.execute("SELECT id FROM pond_name WHERE name = ?", (source_name,)).fetchone()

        version = f"{source_major}.0.0"
        db.execute(
            "INSERT OR IGNORE INTO pond_version (pond_name_id, version, major, source_path) VALUES (?, ?, ?, ?)",
            (pn_id, version, source_major, f"spout://{sname}@{source_major}"),
        )
        (pv_id,) = db.execute(
            "SELECT id FROM pond_version WHERE pond_name_id = ? AND version = ?", (pn_id, version)
        ).fetchone()
        db.execute("INSERT OR IGNORE INTO ripple (pond_version_id, name) VALUES (?, 'egress')", (pv_id,))
        db.execute(
            "INSERT INTO pond (pond_name_id, major, pond_version_id, is_spout) VALUES (?, ?, ?, 1) "
            "ON CONFLICT(pond_name_id, major) DO UPDATE SET pond_version_id = excluded.pond_version_id, "
            "is_spout = 1",
            (pn_id, source_major, pv_id),
        )
        (spout_pid,) = db.execute(
            "SELECT id FROM pond WHERE pond_name_id = ? AND major = ?", (pn_id, source_major)
        ).fetchone()
        db.execute(
            "INSERT OR IGNORE INTO pond_to_pond (pond_id, source_pond_name_id, source_major, required) "
            "VALUES (?, ?, ?, 1)", (spout_pid, src_pn_id, source_major),
        )
        db.execute(
            "INSERT INTO pond_spout (pond_id, table_name, destination, mode, armed) VALUES (?, ?, ?, ?, 1) "
            "ON CONFLICT(pond_id) DO UPDATE SET table_name = excluded.table_name, "
            "destination = excluded.destination, mode = excluded.mode",
            (spout_pid, table, destination, mode),
        )
        return pond_key(sname, source_major)

    def _destroy_spout(self, spout_key: str) -> bool:
        """Remove a Spout's identity + state + history rows (caller holds the lock and reloads)."""
        db = self.db
        meta = self.meta.get(spout_key)
        if meta is None or not meta.get("is_spout"):
            return False
        pond_id, pv_id = meta["pond_id"], meta["version_id"]
        (pn_id,) = db.execute("SELECT pond_name_id FROM pond WHERE id = ?", (pond_id,)).fetchone()
        db.execute("DELETE FROM ripple_run WHERE pond_version_id = ?", (pv_id,))
        db.execute("DELETE FROM pond_run WHERE pond_version_id = ?", (pv_id,))
        db.execute("DELETE FROM ripple_run_lineage WHERE pond_version_id = ?", (pv_id,))
        for tbl in ("pond_state", "pond_target", "pond_open", "pond_trigger", "pond_retry",
                    "pond_window", "pond_spout", "pond_duck", "pond_to_pond"):
            db.execute(f"DELETE FROM {tbl} WHERE pond_id = ?", (pond_id,))
        db.execute("DELETE FROM pond WHERE id = ?", (pond_id,))
        db.execute("DELETE FROM ripple WHERE pond_version_id = ?", (pv_id,))
        db.execute("DELETE FROM pond_version WHERE id = ?", (pv_id,))
        db.execute("DELETE FROM pond_name WHERE id = ? AND NOT EXISTS "
                   "(SELECT 1 FROM pond_version WHERE pond_name_id = ?)", (pn_id, pn_id))
        return True

    # ─── Scheduling ───────────────────────────────────────────────────────────

    def next_wake(self) -> datetime | None:
        with self.lock:
            return next_wake(_now(), self.state)

    def scheduler_tick(self) -> None:
        with self.lock:
            now = _now()
            self._check_liveness(now)
            self._tick_process(now)
            self._check_freshness(now)
            self._check_renotify(now)

    def _check_liveness(self, now: datetime) -> None:
        """Fail any Pond whose Duck has died (process gone) or fallen silent (no contact) while a Run
        is in flight, attributing it to that Run (``start_f``). Only for launchers that own real Duck
        processes — the NoopLauncher (tests) has nothing to watch."""
        if not self.launcher.manages_processes:
            return
        for pond in list(self.state.ponds):
            p = self.state.ponds[pond]
            if p.is_draw or p.is_spout:  # no Duck process — the poller/egress worker drives these
                continue
            ps = self.state.pond_states[pond]
            if ps.is_blocked or ps.is_killed:  # killed Ponds are intentionally down — don't re-fail
                continue
            # In flight, and fresher than any recorded failure — so a retry-on-change Run draws a fresh
            # liveness check, but an already-failed Run is not re-failed.
            if not (ps.start_f > ps.end_f and ps.start_f > ps.failed_f):
                continue
            last = self.last_seen.get(pond)
            dead = not self.launcher.is_running(pond)
            silent = last is not None and (now - last) > _DUCK_DEAD_AFTER
            if dead:
                self._fail_whole_pond(pond, now, "Duck process is not running (it crashed or exited)")
            elif silent:
                self._fail_whole_pond(pond, now, "Lost contact with the Duck (no events received)")

    # ─── Core processing ──────────────────────────────────────────────────────

    def _tick_process(self, now: datetime) -> None:
        self.state = tick(now, self.state)
        self._process(now)

    def _process(self, now: datetime, notify: bool = True) -> None:
        self.state, _started = sentinel(now, self.state)
        for cmd in drain_begin_runs(self.state):
            self._dispatch_begin_run(cmd.pond_id, cmd.f, now, force=cmd.force, refresh=cmd.refresh,
                                     sources_changed=cmd.sources_changed)
        for pid, f in drain_passes(self.state):
            self._record_pass(pid, f, now)
        self._persist_state()
        self._reap_idle()
        self._emit_recoveries()  # any alerted failure that has cleared → one `recovery` notification
        self.state_version += 1  # state moved → release any /api/status long-poll
        # Wake the poller so a Draw forwards new demand to its upstream at once. The poller's own
        # observe/transfer paths pass notify=False (they're handled in-cycle) to avoid a busy loop.
        if notify and self._notify_cb is not None:
            self._notify_cb()

    def _dispatch_begin_run(
        self, pond: str, f: datetime, now: datetime, force: bool = False, refresh: bool = False,
        sources_changed: bool = True,
    ) -> None:
        meta = self.meta[pond]
        # A Pond Draw is not run by a Duck: record the Run as running and hand the parquet transfer to
        # the poller (it fetches out-of-lock, then reports completion via complete_draw_transfer).
        if meta.get("is_draw"):
            self.db.execute(
                "INSERT OR IGNORE INTO pond_run (pond_version_id, f, started_at, status) "
                "VALUES (?, ?, ?, 'running')",
                (meta["version_id"], _iso(f), _iso(now)),
            )
            self.db.commit()
            if (pond, f) not in self._pending_transfers:
                self._pending_transfers.append((pond, f))
            return
        # A Spout is the egress dual: not run by a Duck either — record the Run and hand the delivery to
        # the egress worker (it reads the source + writes out-of-lock, then reports via complete/fail).
        if meta.get("is_spout"):
            self.db.execute(
                "INSERT OR IGNORE INTO pond_run (pond_version_id, f, started_at, status) "
                "VALUES (?, ?, ?, 'running')",
                (meta["version_id"], _iso(f), _iso(now)),
            )
            self.db.commit()
            if (pond, f) not in self._pending_egress:
                self._pending_egress.append((pond, f))
            self._signal_egress()
            return
        self.launcher.ensure(pond, meta["version"], meta["source_path"], duck=self.duck_config(pond))
        self.last_seen[pond] = now  # grace clock: a freshly (re)spawned Duck isn't immediately stale
        self._idle_since.pop(pond, None)  # it's running again — reset its reap grace clock
        # Cancel any not-yet-collected shutdown: this Pond is running again, so the Duck must not exit.
        self.jobs[pond] = [j for j in self.jobs.get(pond, []) if j.get("kind") != "shutdown"]
        self.jobs[pond].append({
            "kind": "begin_run", "f": _iso(f), "force": force, "refresh": refresh,
            "sources_changed": sources_changed,  # backs pond.sources_changed() (for always_run gating)
            "immediate_retries": self.state.ponds[pond].retry_immediately,  # live budget, per Run
            # The prior completed run's freshness (the pond's end_f *before* this run advances it),
            # carried to the Ripples as pond.previous_f. A Refresh reads its Sources in full, so NEVER.
            "previous_f": _iso(NEVER) if refresh else _iso(self.state.pond_states[pond].end_f),
            # The major line's additive schema contract this Run must keep (vetted by the Duck before
            # publishing); None for a first run or a deliberate rollback (governed by min_version).
            "contract": self._contract_for(pond),
        })
        # Write started_at as tz-aware ISO (UTC) to match finished_at; the SQLite `datetime('now')`
        # default is naive and would be misread as local time by the UI. A Force re-opens the Run.
        self.db.execute(
            "INSERT OR REPLACE INTO pond_run (pond_version_id, f, started_at, status) VALUES (?, ?, ?, 'running')"
            if force else
            "INSERT OR IGNORE INTO pond_run (pond_version_id, f, started_at, status) VALUES (?, ?, ?, 'running')",
            (meta["version_id"], _iso(f), _iso(now)),
        )
        self.db.commit()

    def _reap_idle(self) -> None:
        # Keep all Ducks warm while any standing trigger is active (a Wave/Tide will run them again
        # shortly) — reaping mid-cycle would thrash on respawns. Only reap once fully quiescent.
        if self.state.triggers:
            self._idle_since.clear()
            return
        now = _now()
        for name in self.state.ponds:
            ps = self.state.pond_states[name]
            busy = any(
                self.state.ripple_states[rid].is_running
                for rid in self.state.ripples
                if self.state.ripples[rid].pond_id == name
            )
            idle = (not busy and not ps.targets and not ps.has_pull and not self.jobs.get(name)
                    and self.launcher.is_running(name))
            if not idle:
                self._idle_since.pop(name, None)
                continue
            # Reap only after the Pond has been continuously idle for the grace period — so a Pond
            # that re-runs on any sub-grace cadence keeps its Duck and never hits the reap/respawn race.
            since = self._idle_since.setdefault(name, now)
            if now - since >= _REAP_GRACE:
                self.jobs.setdefault(name, []).append({"kind": "shutdown"})
                self._idle_since.pop(name, None)

    # ─── History + persistence ────────────────────────────────────────────────

    def _record_ripple_run(
        self, pond: str, rname: str, f: str, status: str, started_at: str | None, finished_at: str,
        retry: int = 0, error: str | None = None, traceback: str | None = None,
    ) -> None:
        meta = self.meta[pond]
        rid = meta["ripple_ids"].get(rname)
        if rid is None:
            return
        # Keyed on (pond_version, f, ripple, retry): each attempt is its own row — the retry trace.
        self.db.execute(
            "INSERT OR REPLACE INTO ripple_run "
            "(pond_version_id, f, ripple_id, retry, started_at, finished_at, status, error, traceback) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (meta["version_id"], f, rid, retry, started_at, finished_at, status, error, traceback),
        )
        self.db.commit()

    def _finish_pond_run(self, pond: str, f: str, now: datetime, changed: bool = True) -> None:
        meta = self.meta[pond]
        self.db.execute(
            "UPDATE pond_run SET finished_at = ?, status = 'success', changed = ? "
            "WHERE pond_version_id = ? AND f = ?",
            (_iso(now), int(changed), meta["version_id"], f),
        )
        self.db.commit()

    def _record_pass(self, pond: str, f: datetime, now: datetime) -> None:
        """An engine-synthesised pass (no Duck): record an instant, successful no-change pond_run so the
        history is honest and reload's run counts stay correct (see plans/no-change-skip.md)."""
        meta = self.meta[pond]
        fi = _iso(f)
        self.db.execute(
            "INSERT OR IGNORE INTO pond_run (pond_version_id, f, started_at, finished_at, status, changed) "
            "VALUES (?, ?, ?, ?, 'success', 0)",
            (meta["version_id"], fi, _iso(now), _iso(now)),
        )
        self.db.commit()

    # ─── Version contract (schema) ───────────────────────────────────────────────

    def _contract_for(self, pond: str) -> dict | None:
        """The major line's additive contract this Pond's next Run must remain a superset of, or
        ``None`` when there is nothing to enforce: the first run on the major (no schema captured yet),
        or a deliberate rollback to a version at or below the high-water (``min_version`` governs that —
        the schema gate is **forward-only**). The contract is the schema of the highest accepted version
        on the major."""
        from ..keys import version_key

        meta = self.meta[pond]
        rows = self.db.execute(
            'SELECT pv.version, s."table", s."column", s.type FROM pond_version_schema s '
            "JOIN pond_version pv ON pv.id = s.pond_version_id "
            "JOIN pond_name pn ON pn.id = pv.pond_name_id "
            "WHERE pn.name = ? AND pv.major = ?",
            (meta["name"], meta["major"]),
        ).fetchall()
        if not rows:
            return None
        by_version: dict[str, dict] = {}
        for ver, table, column, type_ in rows:
            by_version.setdefault(ver, {}).setdefault(table, {})[column] = type_
        high_water = max(by_version, key=version_key)
        if version_key(meta["version"]) < version_key(high_water):
            return None  # rollback — governed by min_version, not the forward-only schema gate
        return by_version[high_water]

    def _record_lineage(self, pond: str, ripple: str, f: str, retry: int, lineage: dict) -> None:
        """Persist a Ripple attempt's observed reads/writes (plans/lineage.md Phase 1). Idempotent on
        replay (INSERT OR IGNORE over the full row key); wrapped so a lineage write can never fail a run
        — lineage is observability, not orchestration."""
        try:
            vid = self.meta[pond]["version_id"]
            rows = [(vid, ripple, f, retry, "read", s or "", t) for s, t in lineage.get("reads", [])]
            rows += [(vid, ripple, f, retry, "write", "", t) for t in lineage.get("writes", [])]
            self.db.executemany(
                "INSERT OR IGNORE INTO ripple_run_lineage "
                "(pond_version_id, ripple, f, retry, direction, source_name, table_name) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)", rows,
            )
            self.db.commit()
        except Exception as exc:  # noqa: BLE001 — never let bookkeeping break a run
            print(f"[catchment] lineage record failed ({pond}.{ripple}): {exc}", flush=True)

    def lineage(self, pond: str | None = None, major: int | None = None, table: str | None = None,
                columns: bool = False) -> dict:
        """The observed table-level lineage graph (plans/lineage.md Phase 1), from each Ripple's **latest
        recorded run** on each selected version. ``pond`` narrows to one Pond (its selected version on
        ``major``, default the highest deployed); ``table`` narrows to edges touching that table name.
        Returns ``{"ponds": [{"id", "name", "major", "version", "ripples": [{"ripple", "f", "reads":
        [{"source", "table"}], "writes": [table]}]}]}`` — reads with ``source: null`` are own tables."""
        with self.lock:
            keys = [k for k in self.meta if not self.meta[k].get("is_spout") and not self.meta[k].get("is_draw")]
            if pond is not None:
                keys = [k for k in keys if self.meta[k]["name"] == pond
                        and (major is None or self.meta[k]["major"] == major)]
                if pond is not None and major is None and len(keys) > 1:  # bare name → highest major
                    keys = [max(keys, key=lambda k: self.meta[k]["major"])]
            out = []
            for key in sorted(keys):
                m = self.meta[key]
                rows = self.db.execute(
                    "SELECT l.ripple, l.f, l.direction, l.source_name, l.table_name "
                    "FROM ripple_run_lineage l "
                    "WHERE l.pond_version_id = ? AND l.f = ("
                    "  SELECT MAX(l2.f) FROM ripple_run_lineage l2 "
                    "  WHERE l2.pond_version_id = l.pond_version_id AND l2.ripple = l.ripple) "
                    "ORDER BY l.ripple, l.direction, l.source_name, l.table_name",
                    (m["version_id"],),
                ).fetchall()
                ripples: dict[str, dict] = {}
                for rname, f, direction, source, tname in rows:
                    r = ripples.setdefault(rname, {"ripple": rname, "f": f, "reads": [], "writes": []})
                    if direction == "read":
                        r["reads"].append({"source": source or None, "table": tname})
                    else:
                        r["writes"].append(tname)
                if table is not None:  # narrow to ripples touching this table name
                    ripples = {n: r for n, r in ripples.items()
                               if table in r["writes"] or any(rd["table"] == table for rd in r["reads"])}
                entry = {"id": key, "name": m["name"], "major": m["major"], "version": m["version"],
                         "ripples": sorted(ripples.values(), key=lambda r: r["ripple"])}
                if columns:
                    entry["columns"] = self._column_lineage_of(m["version_id"], table)
                if pond is None and not ripples and not entry.get("columns"):
                    continue  # the catchment-wide view lists only ponds with recorded lineage
                out.append(entry)
            return {"ponds": out}

    def _column_lineage_of(self, version_id: int, table: str | None = None) -> dict:
        """The deploy-captured static column lineage (plans/lineage.md Phase 2) for one version:
        ``{table: {column: [{"ref", "column"}] | "constant" | "opaque"} | "opaque"}``. Absent columns
        were simply not captured (a non-capturable ripple) — absent, never guessed."""
        rows = self.db.execute(
            'SELECT "table", "column", kind, src_ref, src_column FROM pond_version_column_lineage '
            'WHERE pond_version_id = ? ORDER BY "table", "column", src_ref, src_column', (version_id,),
        ).fetchall()
        out: dict = {}
        for tname, col, kind, src_ref, src_col in rows:
            if table is not None and tname != table:
                continue
            if col == "" and kind == "opaque":
                out[tname] = "opaque"  # the whole table is unprovable (a .sql() output)
                continue
            t = out.setdefault(tname, {})
            if not isinstance(t, dict):
                continue
            if kind == "exact":
                t.setdefault(col, []).append({"ref": src_ref, "column": src_col})
            else:
                t[col] = kind  # "constant" | "opaque"
        return out

    def trace_run(self, pond_name: str, major: int, row_f: str) -> dict:
        """Temporal provenance for a row freshness (plans/lineage.md Phase 4): the run that produced it
        (version, timings, status), its input window ``(previous_f, f]`` (the bracket every Source was
        read over — ``previous_f`` is the prior *successful* run), and the declared Sources."""
        with self.lock:
            row = self.db.execute(
                "SELECT pr.f, pv.version, pr.started_at, pr.finished_at, pr.status "
                "FROM pond_run pr JOIN pond_version pv ON pv.id = pr.pond_version_id "
                "JOIN pond_name pn ON pn.id = pv.pond_name_id "
                "WHERE pn.name = ? AND pv.major = ? AND pr.f = ? "
                "ORDER BY pr.finished_at DESC LIMIT 1", (pond_name, major, row_f),
            ).fetchone()
            prev = self.db.execute(
                "SELECT MAX(pr.f) FROM pond_run pr JOIN pond_version pv ON pv.id = pr.pond_version_id "
                "JOIN pond_name pn ON pn.id = pv.pond_name_id "
                "WHERE pn.name = ? AND pv.major = ? AND pr.f < ? AND pr.status = 'success'",
                (pond_name, major, row_f),
            ).fetchone()
            key = f"{pond_name}@{major}"
            sources = sorted(
                self.meta[sk]["name"] for sk in
                (self.state.ponds[key].sources if key in self.state.ponds else [])
                if sk in self.meta
            )
            run = None
            if row is not None:
                run = {"f": row[0], "version": row[1], "started_at": row[2],
                       "finished_at": row[3], "status": row[4]}
            return {"run": run,
                    "window": {"previous_f": prev[0] if prev else None, "f": row_f},
                    "sources": sources}

    def _emit_openlineage(self, pond: str, f: str, schema: dict | None) -> None:
        """Emit a standard OpenLineage RunEvent (COMPLETE) for a finished Pond Run to any channel
        subscribed to the ``openlineage`` kind (plans/lineage.md Phase 4 — the integrate-don't-compete
        move: one emitter slots Duckstring into whatever catalog a team already runs). The event body is
        assembled from facts already in hand — the run identity, the observed table reads/writes
        (``ripple_run_lineage`` at this ``f``), and the captured output schema as facets — and delivered
        through the alert outbox (retries, audit, a catalog outage never touches a run). Deliberately
        NOT in the ``all`` subscription (a Slack channel must not receive raw catalog events); a channel
        opts in with ``--on openlineage``. Wrapped: emission can never break a run."""
        try:
            import uuid

            if not self._openlineage_channels():
                return  # no subscriber — build nothing
            m = self.meta[pond]
            cid = self._catchment_uuid() or "catchment"
            namespace = f"duckstring://{cid}"
            rows = self.db.execute(
                "SELECT DISTINCT direction, source_name, table_name FROM ripple_run_lineage "
                "WHERE pond_version_id = ? AND f = ?", (m["version_id"], f),
            ).fetchall()
            inputs = [{"namespace": namespace, "name": f"{src}.{t}"}
                      for d, src, t in sorted(rows) if d == "read" and src]
            out_names = sorted({t for d, src, t in rows if d == "write"} | set((schema or {}).keys()))
            outputs = []
            for t in out_names:
                ds: dict = {"namespace": namespace, "name": f"{m['name']}.{t}"}
                if schema and t in schema:
                    ds["facets"] = {"schema": {
                        "_producer": _OL_PRODUCER, "_schemaURL": _OL_SCHEMA_FACET,
                        "fields": [{"name": c, "type": ty} for c, ty in schema[t].items()],
                    }}
                outputs.append(ds)
            event = {
                "eventType": "COMPLETE",
                "eventTime": _iso(_now()),
                "producer": _OL_PRODUCER,
                "schemaURL": _OL_SCHEMA_URL,
                "run": {"runId": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{cid}:{pond}:{f}"))},
                "job": {"namespace": namespace, "name": pond},
                "inputs": inputs,
                "outputs": outputs,
            }
            self._emit_alert(
                "openlineage", scope_pond=m["name"], scope_major=m["major"], severity="info",
                title=f"OpenLineage: {pond} run complete", message=f"Run at {f} completed.",
                f=f, detail={"event": event},
            )
        except Exception as exc:  # noqa: BLE001 — lineage emission must never break a run
            print(f"[catchment] openlineage emit failed ({pond}): {exc}", flush=True)

    def _openlineage_channels(self) -> bool:
        return self.db.execute(
            "SELECT 1 FROM alert_channel WHERE enabled = 1 AND events LIKE '%openlineage%' LIMIT 1"
        ).fetchone() is not None

    def _catchment_uuid(self) -> str | None:
        row = self.db.execute("SELECT value FROM catchment_meta WHERE key = 'id'").fetchone()
        return row[0] if row and row[0] else None

    def _capture_schema(self, pond: str, schema: dict) -> None:
        """Freeze a Pond version's published output schema as its contract (idempotent upsert, keyed on
        ``pond_version``). Only reached for accepted runs — the Duck publishes only what passed the gate."""
        vid = self.meta[pond]["version_id"]
        self.db.execute("DELETE FROM pond_version_schema WHERE pond_version_id = ?", (vid,))
        for table, columns in schema.items():
            for column, type_ in columns.items():
                self.db.execute(
                    'INSERT INTO pond_version_schema (pond_version_id, "table", "column", type) '
                    "VALUES (?, ?, ?, ?)",
                    (vid, table, column, type_),
                )
        self.db.commit()

    def _fail_whole_pond(
        self, pond: str, now: datetime, error: str | None = None, tb: str | None = None,
        alert_kind: str = "failure",
    ) -> None:
        """Fail a Pond with no single culprit Ripple (dead/silent Duck, or a reported Duck-level
        error): mark its most recently started Run failed and run the cascade (which may re-dispatch a
        retry-on-change Run, respawning a Duck). No-op if nothing is in flight."""
        ps = self.state.pond_states[pond]
        if ps.start_f <= ps.end_f:
            return
        f = _iso(ps.start_f)
        self.state = fail_pond(self.state, pond, now)
        self._fail_pond_run(pond, f, now, error, tb)
        self._process(now)  # settle the cascade first, so the blast radius (blocked downstream) is accurate
        name = self.meta.get(pond, {}).get("name", pond)
        verb = "broke its version contract" if alert_kind == "contract" else "failed"
        self._alert_failure(pond, alert_kind, scope_pond=name,
                            scope_major=self.meta.get(pond, {}).get("major"), f=f,
                            title=f"Pond '{name}' {verb}",
                            message=f"Pond '{name}' {verb}: {error or 'unknown error'}")

    def _fail_pond_run(
        self, pond: str, f: str, now: datetime, error: str | None = None, tb: str | None = None
    ) -> None:
        meta = self.meta[pond]
        self.db.execute(
            "INSERT INTO pond_run (pond_version_id, f, started_at, finished_at, status, error, traceback) "
            "VALUES (?, ?, ?, ?, 'failed', ?, ?) ON CONFLICT(pond_version_id, f) DO UPDATE SET "
            "finished_at = excluded.finished_at, status = 'failed', error = excluded.error, "
            "traceback = excluded.traceback",
            (meta["version_id"], f, _iso(now), _iso(now), error, tb),
        )
        self.db.commit()

    def _kill_pond_run(self, pond: str, f: str, now: datetime) -> None:
        meta = self.meta[pond]
        self.db.execute(
            "INSERT INTO pond_run (pond_version_id, f, started_at, finished_at, status, error) "
            "VALUES (?, ?, ?, ?, 'killed', 'Killed by operator') ON CONFLICT(pond_version_id, f) DO UPDATE SET "
            "finished_at = excluded.finished_at, status = 'killed', error = excluded.error",
            (meta["version_id"], f, _iso(now), _iso(now)),
        )
        self.db.commit()

    def _persist_trigger(self, pond: str, kind: str, bound_ms: int | None) -> None:
        self.db.execute(
            "INSERT INTO pond_trigger (pond_id, kind, bound_ms) VALUES (?, ?, ?) "
            "ON CONFLICT(pond_id) DO UPDATE SET kind = excluded.kind, bound_ms = excluded.bound_ms, status = 'active'",
            (self.meta[pond]["pond_id"], kind, bound_ms),
        )
        self.db.commit()

    def _persist_state(self) -> None:
        for name, ps in self.state.pond_states.items():
            pond_id = self.meta[name]["pond_id"]
            self.db.execute(
                "INSERT INTO pond_state (pond_id, start_f, end_f, d_ms, has_pull, has_received_pull, "
                "is_failed, is_blocked, failed_f, failures, is_killed, pull_local, pull_m, "
                "refresh_pending, repairing, changed_f) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(pond_id) DO UPDATE SET "
                "start_f = excluded.start_f, end_f = excluded.end_f, d_ms = excluded.d_ms, "
                "has_pull = excluded.has_pull, has_received_pull = excluded.has_received_pull, "
                "is_failed = excluded.is_failed, is_blocked = excluded.is_blocked, "
                "failed_f = excluded.failed_f, failures = excluded.failures, "
                "is_killed = excluded.is_killed, pull_local = excluded.pull_local, pull_m = excluded.pull_m, "
                "refresh_pending = excluded.refresh_pending, repairing = excluded.repairing, "
                "changed_f = excluded.changed_f",
                (
                    pond_id,
                    _iso(ps.start_f) if ps.start_f != NEVER else None,
                    _iso(ps.end_f) if ps.end_f != NEVER else None,
                    int(ps.d.total_seconds() * 1000),
                    int(ps.has_pull),
                    int(ps.has_received_pull),
                    int(ps.is_failed),
                    int(ps.is_blocked),
                    _iso(ps.failed_f) if ps.failed_f != NEVER else None,
                    ps.failures,
                    int(ps.is_killed),
                    int(ps.pull_local),
                    _iso(ps.pull_m) if ps.pull_m != NEVER else None,
                    int(ps.refresh_pending),
                    int(ps.repairing),
                    _iso(ps.changed_f) if ps.changed_f != NEVER else None,
                ),
            )
            self.db.execute("DELETE FROM pond_target WHERE pond_id = ?", (pond_id,))
            for t in ps.targets:
                self.db.execute(
                    "INSERT OR IGNORE INTO pond_target (pond_id, target_f) VALUES (?, ?)", (pond_id, _iso(t))
                )
        self.db.commit()

    # ─── Status ───────────────────────────────────────────────────────────────

    def _exported_tables(self, key: str) -> set[str]:
        """Names of the tables this major line has published to its data dir (the exported Parquet/
        Iceberg snapshot). Best-effort — a data-read hiccup must never break ``status()``; a Draw has
        no local output. ``list_tables`` globs the flat sidecar, so it needs no Iceberg extension."""
        from pathlib import Path

        from ..dataplane import get_data_plane
        from .registry import pond_data_dir

        meta = self.meta.get(key, {})
        if meta.get("is_draw"):
            return set()
        try:
            data_dir = pond_data_dir(Path(self.root), meta["name"], meta["major"], self.data_root)
            return set(get_data_plane().list_tables(data_dir))
        except Exception:
            return set()

    def _has_objects(self, key: str) -> bool:
        """Whether this major line has published any non-tabular Object — gates the viewer's Objects tab."""
        from pathlib import Path

        from ..objects import list_objects
        from .registry import pond_data_dir

        meta = self.meta.get(key, {})
        if meta.get("is_draw"):
            return False
        try:
            data_dir = pond_data_dir(Path(self.root), meta["name"], meta["major"], self.data_root)
            return bool(list_objects(data_dir))
        except Exception:
            return False

    def status(self) -> dict:
        with self.lock:
            from ..engine import NEVER, min_target
            ts = lambda dt: _iso(dt) if dt is not None and dt != NEVER else None  # noqa: E731

            def _demand_status(rs, running: bool) -> str:
                if running:
                    return "running"
                if rs.has_pull or rs.targets:
                    return "queued"
                return "idle"

            ponds = []
            for key in self.state.ponds:
                ps = self.state.pond_states[key]
                # Whether this major line has published any tables — gates the Pond's data viewer.
                has_tables = bool(self._exported_tables(key))
                has_objects = self._has_objects(key)
                # Ripples belonging to this Pond, with their live per-Ripple state and intra-Pond edges.
                ripples = []
                ripple_edges = []
                for rid, rip in self.state.ripples.items():
                    if rip.pond_id != key:
                        continue
                    rs = self.state.ripple_states[rid]
                    ripples.append({
                        "name": rip.name,
                        "status": _demand_status(rs, rs.is_running),
                        "gen": rs.runs_started,
                        "runs_completed": rs.runs_completed,
                        "has_pull": rs.has_pull,
                        "target_f": ts(min_target(rs.targets)),
                        "start_f": ts(rs.start_f),
                        "end_f": ts(rs.end_f),
                    })
                    for parent in rip.parents:
                        psrc = self.state.ripples.get(parent)
                        if psrc is not None and psrc.pond_id == key:
                            ripple_edges.append([psrc.name, rip.name])

                busy = any(r["status"] == "running" for r in ripples)
                # Failure/kill/block take precedence over demand state so a stalled Pond reads truthfully.
                if ps.is_failed:
                    st = "failed"
                elif ps.is_killed:
                    st = "killed"
                elif ps.repairing:
                    st = "repairing"
                elif ps.is_blocked:
                    st = "blocked"
                else:
                    st = _demand_status(ps, busy)

                # Why is it blocked? Required Sources that are themselves down (failed/killed/blocked).
                pond = self.state.ponds[key]
                blocked_by = [
                    sp for sp in pond.sources if sp not in pond.optional_sources and (
                        self.state.pond_states[sp].is_failed
                        or self.state.pond_states[sp].is_blocked
                        or self.state.pond_states[sp].is_killed
                    )
                ]
                # The failure message (freshest failed Run), shown when failed — plus its sub-reason:
                # a contract failure carries the stable CONTRACT_PREFIX in its stored message, so the
                # kind is derivable here (and after a restart) with no extra state.
                error = None
                failure_kind = None
                if ps.is_failed:
                    from ..schema_contract import CONTRACT_PREFIX

                    row = self.db.execute(
                        "SELECT error FROM pond_run WHERE pond_version_id = ? AND status = 'failed' "
                        "ORDER BY f DESC LIMIT 1", (self.meta[key]["version_id"],),
                    ).fetchone()
                    error = row[0] if row else None
                    failure_kind = "contract" if (error or "").startswith(CONTRACT_PREFIX) else "error"

                trig = self.state.triggers.get(key)
                trigger = None
                if trig is not None:
                    trigger = {
                        "kind": trig.kind,
                        "bound_ms": int(trig.bound.total_seconds() * 1000) if trig.bound is not None else None,
                    }

                ponds.append({
                    "id": key,
                    "name": self.meta[key]["name"],
                    "major": self.meta[key]["major"],
                    "kind": self.meta[key]["kind"],
                    "is_draw": self.meta[key].get("is_draw", False),
                    "is_spout": self.meta[key].get("is_spout", False),
                    "dbt": self.meta[key].get("dbt", False),  # dbt-mode Pond (models are Ripples) — UI flag
                    # A Spout's egress config + armed state, for the node's control panel.
                    "spout": (
                        {**self.meta[key]["spout"], "armed": ps.standing_wake}
                        if self.meta[key].get("is_spout") and self.meta[key].get("spout") else None
                    ),
                    "version": self.meta[key]["version"],
                    "has_tables": has_tables,
                    "has_objects": has_objects,
                    "status": st,
                    "gen": ps.runs_started,
                    "runs_completed": ps.runs_completed,
                    "has_pull": ps.has_pull,
                    "target_f": ts(min_target(ps.targets)),
                    "start_f": ts(ps.start_f),
                    "end_f": ts(ps.end_f),
                    "changed_f": ts(ps.changed_f),  # content freshness: held across a pass (no-change run)
                    "d_ms": int(ps.d.total_seconds() * 1000),
                    "trigger": trigger,
                    "is_failed": ps.is_failed,
                    "is_blocked": ps.is_blocked,
                    "blocked_reason": (f"waiting for '{ps.missing_asset}'" if ps.missing_asset else None),
                    "is_killed": ps.is_killed,
                    "refresh_pending": ps.refresh_pending,
                    "repairing": ps.repairing,
                    "failed_f": ts(ps.failed_f),
                    "failures": ps.failures,
                    "missing_sources": self.meta[key].get("missing_sources", []),
                    "blocked_by": blocked_by,
                    "error": error,
                    "failure_kind": failure_kind,  # "contract" | "error" | null — the failed sub-reason
                    "immediate_retries": self.state.ponds[key].retry_immediately,
                    "source_retries": self.state.ponds[key].retry_on_change,
                    # The effective compute config (target/size + Flock posture) + its declared/override
                    # provenance. Draws/Spouts are not run by a Duck, so they carry none.
                    "duck": (None if self.meta[key].get("is_draw") or self.meta[key].get("is_spout")
                             else self.duck_config(key)),
                    "ripples": ripples,
                    "ripple_edges": ripple_edges,
                })
            # Edge endpoints are pond keys ("name@major") — match entries on their "id". A Spout is a
            # real node now, so it and its source→spout edge fall out of `ponds`/`edges` (dashed in the UI
            # like a Draw, distinguished by `is_spout`).
            edges = [[s, key] for key, pond in self.state.ponds.items() for s in pond.sources]

            rows = dict(self.db.execute("SELECT key, value FROM catchment_meta").fetchall())
            return {
                "catchment": {"id": rows.get("id"), "name": rows.get("name")},
                "version": self.state_version,  # the /api/status long-poll's change token
                "ponds": ponds, "edges": edges,
            }

    def metrics_snapshot(self) -> dict:
        """Raw numbers for the Prometheus ``/metrics`` endpoint (rendered by ``routes/metrics.py``). Per
        engine node: freshness/delivery lag, state flags, and the runs-completed counter — plus cumulative
        failed Pond Runs and alert-delivery counts from the DB (survive restarts, so they read as counters)."""
        with self.lock:
            now = _now()
            nodes = []
            for key in self.state.ponds:
                m = self.meta[key]
                ps = self.state.pond_states[key]
                lag = (now - ps.end_f).total_seconds() if ps.end_f != NEVER else None
                node = {
                    "name": m["name"], "major": m["major"], "kind": m["kind"],
                    "is_spout": bool(m.get("is_spout")), "is_draw": bool(m.get("is_draw")),
                    "lag_seconds": lag, "runs_completed": ps.runs_completed,
                    "is_failed": ps.is_failed, "is_blocked": ps.is_blocked, "is_killed": ps.is_killed,
                }
                # Compute-cost signal (plans/cloud-config.md increment 4): where the Duck runs + its
                # Flock posture, so a scrape can attribute cost. Draws/Spouts have no Duck.
                if not node["is_spout"] and not node["is_draw"]:
                    dc = self.duck_config(key)
                    node["duck_target"] = dc["duck_target"]
                    node["flock_mode"] = dc["flock_mode"]
                    node["flock_engine"] = dc["flock_engine"] or "none"
                nodes.append(node)
            # Cumulative Duck execution seconds per (name, major) — summed Ripple-Run wall-clock spans
            # (the closest compute-cost proxy without tracking instance uptime). Monotonic across restarts.
            runtimes = {
                (r[0], r[1]): r[2] for r in self.db.execute(
                    "SELECT pn.name, pv.major, "
                    "SUM((julianday(rr.finished_at) - julianday(rr.started_at)) * 86400) "
                    "FROM ripple_run rr JOIN pond_version pv ON pv.id = rr.pond_version_id "
                    "JOIN pond_name pn ON pn.id = pv.pond_name_id "
                    "WHERE rr.started_at IS NOT NULL AND rr.finished_at IS NOT NULL "
                    "GROUP BY pn.name, pv.major"
                ).fetchall() if r[2] is not None
            }
            # Cumulative failed Pond Runs per (name, major) — a monotonic counter across restarts.
            failures = {
                (r[0], r[1]): r[2] for r in self.db.execute(
                    "SELECT pn.name, pv.major, COUNT(*) FROM pond_run pr "
                    "JOIN pond_version pv ON pv.id = pr.pond_version_id "
                    "JOIN pond_name pn ON pn.id = pv.pond_name_id "
                    "WHERE pr.status = 'failed' GROUP BY pn.name, pv.major"
                ).fetchall()
            }
            deliveries = dict(self.db.execute(
                "SELECT status, COUNT(*) FROM alert_delivery GROUP BY status"
            ).fetchall())
            return {"nodes": nodes, "failures": failures, "alert_deliveries": deliveries,
                    "runtimes": runtimes}

    def view_fragment(self, scope: list[str] | None) -> dict:
        """This Catchment's slice of the recursive lineage view (see plans/cross-catchment-visibility.md):
        the in-scope Ponds (``scope`` keys + their ancestors here; all local Ponds when ``scope`` is
        None) with state + intra-Catchment edges, plus the ducts to expand for the next hop. The route
        does the cross-Catchment fan-out + merge; this is the pure local part."""
        with self.lock:
            full = self.status()
            all_keys = {p["id"] for p in full["ponds"]}
            if scope is None:
                in_scope = all_keys
            else:
                in_scope = self._ancestor_keys([k for k in scope if k in all_keys]) & all_keys
            ponds = [p for p in full["ponds"] if p["id"] in in_scope]
            edges = [[s, k] for s, k in full["edges"] if s in in_scope and k in in_scope]
            ducts = []
            for duct in self.duct_targets():
                drawn = [pond_key(m["name"], m["major"]) for m in duct["members"]
                         if pond_key(m["name"], m["major"]) in in_scope]
                if drawn:
                    ducts.append({
                        "upstream_id": duct["upstream_id"], "remote_url": duct["remote_url"],
                        "auth": duct["auth"], "drawn": drawn,
                    })
            return {"catchment": full["catchment"], "ponds": ponds, "edges": edges, "ducts": ducts}

    def _ancestor_keys(self, keys: list[str]) -> set[str]:
        """``keys`` plus all upstream (source) Pond keys reachable from them (BFS over engine sources)."""
        seen: set[str] = set()
        queue = list(keys)
        while queue:
            k = queue.pop()
            if k in seen:
                continue
            seen.add(k)
            pond = self.state.ponds.get(k)
            if pond is not None:
                queue.extend(pond.sources)
        return seen

    def _ancestors(self, name: str) -> set[str]:
        """``name`` plus all upstream (source) Pond names reachable from it (BFS over engine sources)."""
        seen = {name}
        queue = [name]
        while queue:
            n = queue.pop()
            pond = self.state.ponds.get(n)
            if pond is None:
                continue
            for src in pond.sources:
                if src not in seen:
                    seen.add(src)
                    queue.append(src)
        return seen

    def run_history(self, pond: str | None, lineage: bool, ripples: bool, limit: int,
                    after: str | None = None, before: str | None = None) -> list[dict]:
        """Recent Pond Runs (newest first), optionally filtered to ``pond`` (an engine key,
        ``name@major``) and — when ``lineage`` — its upstream sources. History within a major line
        spans every version that ran on it. Ripple Runs are nested only when ``ripples`` is set.
        ``after``/``before`` (UTC ISO) bound the run's ``started_at`` for date-range navigation."""
        with self.lock:
            params: list = []
            conds: list[str] = []
            if pond is not None:
                keys = self._ancestors(pond) if lineage else {pond}
                conds.append(f"(pn.name || '@' || pv.major) IN ({','.join('?' * len(keys))})")
                params.extend(sorted(keys))
            if after is not None:
                conds.append("pr.started_at >= ?")
                params.append(after)
            if before is not None:
                conds.append("pr.started_at <= ?")
                params.append(before)
            where = ("WHERE " + " AND ".join(conds)) if conds else ""
            # Anchor the window: with an ``after`` bound, take the first ``limit`` runs *from* it (ascending
            # — "from → to/now"); otherwise the most recent ``limit`` (descending — the default feed, and
            # the ``before``-only case = the runs just prior to it).
            order = "ASC" if after is not None else "DESC"
            rows = self.db.execute(
                "SELECT pn.name, pv.major, pv.version, pr.pond_version_id, pr.f, pr.started_at, pr.finished_at, "
                "pr.status, pr.error, pr.traceback "
                "FROM pond_run pr "
                "JOIN pond_version pv ON pv.id = pr.pond_version_id "
                "JOIN pond_name pn ON pn.id = pv.pond_name_id "
                f"{where} ORDER BY pr.started_at {order}, pr.f {order} LIMIT ?",
                (*params, limit),
            ).fetchall()

            runs = []
            for pname, major, version, pv_id, f, started_at, finished_at, status, error, tb in rows:
                run = {
                    "pond": pname, "major": major, "id": pond_key(pname, major), "version": version, "f": f,
                    "started_at": started_at, "finished_at": finished_at, "status": status,
                    "error": error, "traceback": tb,
                }
                if ripples:
                    rrows = self.db.execute(
                        "SELECT r.name, rr.started_at, rr.finished_at, rr.status, rr.retry, rr.error, rr.traceback "
                        "FROM ripple_run rr JOIN ripple r ON r.id = rr.ripple_id "
                        "WHERE rr.pond_version_id = ? AND rr.f = ? "
                        "ORDER BY COALESCE(rr.finished_at, rr.started_at), rr.retry",
                        (pv_id, f),
                    ).fetchall()
                    run["ripples"] = [
                        {"ripple": rn, "started_at": rsa, "finished_at": rfa, "status": rst,
                         "retry": rt, "error": rerr, "traceback": rtb}
                        for (rn, rsa, rfa, rst, rt, rerr, rtb) in rrows
                    ]
                runs.append(run)
            return runs
