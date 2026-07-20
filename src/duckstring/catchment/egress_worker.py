"""The egress worker — delivers a Spout's source output to its external destination.

A Spout is a real engine node (the egress dual of a Pond Draw): the engine decides when it runs (a
standing Wake) and dispatches the run here instead of to a Duck — this worker is "the Spout's Duck". One
async task in the Catchment process (outbound I/O, like the duct poller): each pass it drains the runs the
engine dispatched (:meth:`Driver.take_spout_jobs`), delivers each via its scheme's driver, and reports
completion (:meth:`Driver.complete_spout_run` / :meth:`Driver.fail_spout_run`). Because completion flows
through the normal pond_run path, a Spout gets the same run history + traceback + /api/runs as any Pond.

A delivery failure fails *only the Spout's* run (with the traceback) and never the source — a Spout is
terminal, so it blocks nothing.
"""

from __future__ import annotations

import asyncio
import traceback
from pathlib import Path

from fastapi.concurrency import run_in_threadpool

_RECONCILE_INTERVAL = 5.0  # self-healing tick (also catches anything a missed wake left pending)
_PER_SPOUT_TIMEOUT = 60.0  # ceiling on one Spout's delivery, so a slow destination can't starve others


def _egress_spout(root: Path, job: dict, data_root: str | None = None) -> None:
    """Deliver one Spout (blocking — runs in the thread pool). Reads the Pond's published tables via the
    data plane and writes each to the destination. Raises on any failure (the caller records it).

    A **transactional, delta-capable** destination (Postgres) syncs *incrementally*: read the changelog
    delta over ``(destination watermark, f]`` and ``apply_delta`` (upserts + deletes), falling back to a
    full reload on a full read (bootstrap / coverage-miss / a changed overwrite source). Others snapshot."""
    from datetime import datetime

    import duckdb

    from ..dataplane import get_data_plane
    from ..egress.base import get_egress
    from ..trickle.context import NEVER
    from ..trickle_io import load_sidecar, read_delta
    from .registry import pond_data_dir

    driver = get_egress(job["destination"])  # resolves the scheme's driver (+ validates the URI)
    caps = driver.capabilities()
    data_dir = pond_data_dir(Path(root), job["pond_name"], job["major"], data_root)
    dp = get_data_plane()
    # The data + the CDC cursor ride the **source's** real freshness; the Spout's run freshness (job["f"])
    # is only the engine/throttle clock (the window end, when windowed) — used for completion, not the data.
    # A real job (take_spout_jobs) carries ISO strings; the delta/as-of machinery needs datetimes.
    raw_f = job.get("source_f") or job["f"]
    f = datetime.fromisoformat(raw_f) if isinstance(raw_f, str) else raw_f

    con = duckdb.connect()  # in-memory: reads the exported snapshot, never the live registry
    try:
        con.execute("SET TimeZone='UTC'")
        dp.prepare(con)
        data_dir.duckdb_setup(con)  # object store → httpfs + credentials (no-op local)
        sidecar = load_sidecar(data_dir)
        # An explicit table ships just that one; else the source's serviceable set (resolved by
        # take_spout_jobs), falling back to every published table only when serving isn't in play.
        if job["table"]:
            tables = [job["table"]]
        elif job.get("tables") is not None:
            tables = [t for t in job["tables"] if t in set(dp.list_tables(data_dir))]
        else:
            tables = dp.list_tables(data_dir)
        mode = (job.get("mode") or "auto").lower()
        for table in tables:
            pk = sidecar.get(table, {}).get("pk") or None
            if mode == "append" and getattr(caps, "mirrors_layout", False):
                # The incremental object-store path (opt-in — `mode=append`): mirror the table's
                # published collection to the destination, syncing only new/changed artifacts. The
                # destination stays a readable Duckstring layout (parts + tiers + sidecar).
                entry = sidecar.get(table)
                if entry is None:
                    raise ValueError(
                        f"mode=append mirrors a published Duckstring collection, but table {table!r} "
                        f"has no sidecar entry at the source — use mode=full for a plain snapshot"
                    )
                driver.mirror(data_dir, table, entry, f=f)
            elif caps.supports_delta and caps.transactional:
                if not pk:  # the transactional-PK requirement, enforced at egress for a not-yet-checked source
                    raise ValueError(
                        f"egress to a transactional destination needs a primary key — table {table!r} is "
                        "not a merge Trickle with a declared pk (put a .merge(pk=…) before this Spout)"
                    )
                previous_f = driver.watermark(con, table) or NEVER  # the in-destination cursor (exactly-once)
                delta = read_delta(con, data_dir, table, previous_f, f, dp=dp)
                if delta.is_full:  # bootstrap / coverage-miss / changed overwrite source → reload
                    driver.write_full(con, con.sql(dp.read_select(data_dir, table)), table=table, pk=pk, f=f)
                else:
                    driver.apply_delta(con, delta, table=table, pk=pk, f=f)
            else:
                driver.write_full(con, con.sql(dp.read_select(data_dir, table)), table=table, pk=pk, f=f)
    finally:
        con.close()


async def _drain(driver, root: Path) -> None:
    for job in driver.take_spout_jobs():
        try:
            await asyncio.wait_for(
                run_in_threadpool(_egress_spout, root, job, getattr(driver, "data_root", None)),
                timeout=_PER_SPOUT_TIMEOUT,
            )
        except Exception as exc:  # noqa: BLE001 — any delivery error fails the Spout's run, never the source
            driver.fail_spout_run(job["spout_key"], job["f"], f"{type(exc).__name__}: {exc}", traceback.format_exc())
        else:
            driver.complete_spout_run(job["spout_key"], job["f"])


async def run_egress_worker(driver, root, wake: asyncio.Event) -> None:
    """Drain the Spout runs the engine dispatched, on each wake (a Pond published / a control verb) or
    on the reconcile tick. Cancelled on shutdown."""
    root = Path(root)
    while True:
        try:
            await asyncio.wait_for(wake.wait(), timeout=_RECONCILE_INTERVAL)
        except asyncio.TimeoutError:
            pass  # periodic reconcile
        wake.clear()
        try:
            await _drain(driver, root)
        except Exception as exc:  # keep the loop alive
            print(f"[catchment] egress worker error: {exc}", flush=True)
