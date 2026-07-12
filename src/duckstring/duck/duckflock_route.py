"""The Duck-side DuckFlock routing hook — where :mod:`duckstring.duckflock_backend` meets the
:class:`~duckstring.duck.executor.RippleExecutor`.

Enabled per Catchment/Duck by ``DUCKSTRING_DUCKFLOCK_ENDPOINT`` (see
:class:`~duckstring.duckflock_backend.DuckflockConfig`); absent, the executor runs every ripple on the
classic path unchanged. Enabled, each ripple is captured → quoted → routed:

- **local** (or non-capturable / any DuckFlock failure) → the ripple runs classically on the
  executor's registry cursor, exactly as before.
- **duckflock** → the remote job publishes into a per-run **scratch dir** seeded with the outputs'
  prior published state (:func:`~duckstring.duckflock_backend.seed_scratch`), and the results are
  hydrated back into the registry (:func:`~duckstring.dataplane.hydrate_registry`) — so downstream
  ripples, the run-end export, and the contract gate all see them exactly as if the ripple had run
  locally. Nothing publishes to the real data dir mid-run.

v1 constraints: local ``own_data_dir`` only (an object-store ``DUCKSTRING_DATA_ROOT`` routes classic
with a warning — seeding from a bucket is a follow-up), and no synthesized ``pond.skip()`` for an
all-unchanged remote result (the skip decision belongs to the user's ripple code, which didn't run —
a missed pass optimisation, never a correctness issue).
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from ..catchment.registry import pond_data_dir, pond_major_dir
from ..duckflock_backend import DuckflockConfig, Outcome, execute, log, seed_scratch

SCRATCH_DIR = "duckflock_scratch"


def route_ripple(ex, config: DuckflockConfig, func, f, previous_f, *,
                 sources_changed: bool = True, skip_sink=None, refresh: bool = False,
                 quote_fn=None, submit_fn=None) -> Outcome:
    """Route one ripple ``func`` through the DuckFlock backend for executor ``ex``. Runs the ripple
    (locally or remotely) to completion; the classic fallback inside :func:`execute` means this call
    completes the ripple whatever DuckFlock does. Raises only what the ripple itself raises."""
    from ..core import Pond
    from ..dataplane import hydrate_registry
    from ..trickle.io import load_sidecar

    if not getattr(pond_data_dir(ex.root, ex.pond_name, ex.major, ex.data_root), "is_local", False):
        log.warning("duckflock routing needs a local own data dir (object-store DUCKSTRING_DATA_ROOT "
                    "seeding is not supported yet) — running classic")
        config = replace(config, endpoint=None)  # disabled → execute() takes the classic path

    config = replace(config, major=ex.major)  # the envelope carries the executor's real major line
    scratch = pond_major_dir(ex.root, ex.pond_name, ex.major) / SCRATCH_DIR
    own_dir = Path(str(ex.own_data_dir.root)) if hasattr(ex.own_data_dir, "root") else Path(str(ex.own_data_dir))

    def source_catalog(ref: str) -> dict:
        src, table = ref.split(".", 1)
        ddir = pond_data_dir(ex.root, src, ex.source_majors.get(src, 1), ex.data_root)
        sc = load_sidecar(ddir).get(table, {})
        entry = {"location": str(getattr(ddir, "root", ddir)), "mode": sc.get("mode"),
                 "pk": sc.get("pk"), "floor": sc.get("floor"), "f": sc.get("f")}
        if sc.get("stats"):
            entry["stats"] = sc["stats"]  # Extension-2 hints → the quote can actually size the plan
        return entry

    def prepare_remote(plan: dict) -> None:
        outputs = [s["output"]["name"] for s in plan["statements"]]
        if refresh:
            import shutil

            shutil.rmtree(scratch, ignore_errors=True)  # a refresh drops prior state — empty seed
            scratch.mkdir(parents=True, exist_ok=True)
        else:
            seed_scratch(own_dir, scratch, outputs)

    con = ex._cursor()
    try:
        pond = Pond(
            name=ex.pond_name, version=ex.version, con=con, root=ex.root,
            source_majors=ex.source_majors, f=f, previous_f=previous_f, data_root=ex.data_root,
            sources_changed=sources_changed, skip_sink=skip_sink,
            staging_dir=ex.staging_dir, own_data_dir=ex.own_data_dir,
        )
        out = execute(
            config, pond, func, source_catalog=source_catalog, own_location=str(scratch),
            prepare_remote=prepare_remote, refresh=refresh, quote_fn=quote_fn, submit_fn=submit_fn,
        )
        if out.route == "duckflock":  # remote success → read the scratch results back into the registry
            outputs = [s["output"]["name"] for s in out.plan["statements"]]
            hydrate_registry(con, scratch, tables=outputs)
            import shutil

            shutil.rmtree(scratch, ignore_errors=True)
        return out
    finally:
        con.close()
