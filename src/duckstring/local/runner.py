from __future__ import annotations

import shutil
import time
import traceback as tb
from dataclasses import dataclass
from pathlib import Path

from ..core import Pond, collect_ripples, import_pond_module, retry_on_lock
from .project import Project


@dataclass
class RippleResult:
    name: str
    status: str  # "ok" | "failed"
    duration_s: float = 0.0
    error: str | None = None
    traceback: str | None = None


@dataclass
class RunResult:
    seeded: bool
    ripples: list[RippleResult]

    @property
    def ok(self) -> bool:
        return all(r.status == "ok" for r in self.ripples)


def _load_ripples(project: Project) -> list[dict]:
    entry = project.dir / project.ripples_entry
    if not entry.exists():
        raise FileNotFoundError(f"no ripples entrypoint at {project.ripples_entry} — is this a Pond project?")
    try:
        import_pond_module(project.dir, project.ripples_entry)
    except Exception:
        collect_ripples()
        raise
    return collect_ripples()


def _topo_order(ripples: list[dict]) -> list[str]:
    func_to_name = {r["func"]: r["name"] for r in ripples}
    parents = {
        r["name"]: [func_to_name[p] for p in r["parents"] if p in func_to_name]
        for r in ripples
    }
    order: list[str] = []
    done: set[str] = set()
    remaining = dict(parents)
    while remaining:
        ready = sorted(n for n, ps in remaining.items() if all(p in done for p in ps))
        if not ready:
            raise ValueError(f"cycle in ripple graph: {', '.join(sorted(remaining))}")
        for n in ready:
            order.append(n)
            done.add(n)
            remaining.pop(n)
    return order


def _registry_connect(project: Project):
    import duckdb

    return retry_on_lock(lambda: duckdb.connect(str(project.out_dir / "registry.duckdb")))


def _seed(project: Project) -> bool:
    """Copy a self-puddle (``puddles/ponds/{name}/data/*.parquet``) into the run registry as the
    prior state, so an incremental run starts from the same point every time (rerun-idempotent)."""
    from ..dataplane import get_data_plane

    dp = get_data_plane()
    seed_dir = project.snapshot_dir(project.name)
    tables = dp.list_tables(seed_dir)
    if not tables:
        return False
    con = _registry_connect(project)
    try:
        dp.prepare(con)  # ready the connection to read the published format
        for table in tables:
            con.execute(f'CREATE OR REPLACE TABLE "{table}" AS {dp.read_select(seed_dir, table)}')
    finally:
        con.close()
    return True


def _staging_dir(project: Project) -> Path:
    from ..objects import STAGING_DIR

    return project.out_dir / STAGING_DIR


def _export(project: Project, f=None) -> None:
    registry = project.out_dir / "registry.duckdb"
    if not registry.exists():
        return
    import duckdb

    from ..dataplane import get_data_plane
    from ..objects import commit_objects

    def _copy() -> None:
        con = duckdb.connect(str(registry))
        try:
            get_data_plane().export(con, project.out_dir, mode="overwrite", f=f)
        finally:
            con.close()

    retry_on_lock(_copy)
    # Objects publish after the table export (which writes the sidecar); the staging under out_dir is
    # created during the run and cleared here.
    commit_objects(_staging_dir(project), project.out_dir, f)


def run_pond(project: Project, ripple: str | None = None, fresh: bool = False) -> RunResult:
    """One local Pond Run against the hydrated puddles. A full run resets ``puddles/out/`` (seeding
    it from a self-puddle unless ``fresh``) and executes every Ripple in topo order; ``ripple`` runs
    a single Ripple against the existing registry. Stops at the first failure; exports whatever the
    registry holds either way."""
    ripples = _load_ripples(project)
    by_name = {r["name"]: r for r in ripples}
    order = _topo_order(ripples)

    if ripple is not None:
        if ripple not in by_name:
            raise ValueError(f"no ripple '{ripple}' — this Pond has: {', '.join(order)}")
        project.out_dir.mkdir(parents=True, exist_ok=True)
        targets = [ripple]
        seeded = False
    else:
        shutil.rmtree(project.out_dir, ignore_errors=True)
        project.out_dir.mkdir(parents=True)
        seeded = False if fresh else _seed(project)
        targets = order

    from datetime import datetime, timezone

    run_f = datetime.now(timezone.utc)  # one freshness for the whole local run, like a deployed Pond Run
    # The prior run's freshness, mirroring the deployed pond.previous_f: when this run is seeded from a
    # self-puddle (an incremental rerun), the previous local run's f; NEVER on a fresh/first run.
    previous_f = _read_previous_f(project) if seeded else None
    results: list[RippleResult] = []
    for name in targets:
        started = time.perf_counter()
        try:
            con = _registry_connect(project)
            try:
                by_name[name]["func"](
                    Pond(project.name, project.version, con, root=project.puddles_dir,
                         f=run_f, previous_f=previous_f,
                         staging_dir=_staging_dir(project), own_data_dir=project.out_dir)
                )
            finally:
                con.close()
            results.append(RippleResult(name, "ok", time.perf_counter() - started))
        except Exception as exc:
            results.append(
                RippleResult(
                    name, "failed", time.perf_counter() - started,
                    error=f"{type(exc).__name__}: {exc}", traceback=tb.format_exc(),
                )
            )
            break

    _export(project, run_f)
    _write_previous_f(project, run_f)  # record this run's freshness for the next run's previous_f
    return RunResult(seeded=seeded, ripples=results)


def _run_f_marker(project: Project) -> Path:
    # Lives in puddles/ (parent of out/), so a full run's out_dir wipe doesn't clear the prior f.
    return project.puddles_dir / ".run_f"


def _read_previous_f(project: Project):
    marker = _run_f_marker(project)
    if not marker.exists():
        return None
    from datetime import datetime
    try:
        return datetime.fromisoformat(marker.read_text(encoding="utf-8").strip())
    except ValueError:
        return None


def _write_previous_f(project: Project, run_f) -> None:
    marker = _run_f_marker(project)
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(run_f.isoformat(), encoding="utf-8")
