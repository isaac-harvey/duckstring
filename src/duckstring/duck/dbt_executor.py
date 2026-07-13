"""DbtExecutor — the Duck's executor for a **dbt-mode** Pond (see ``plans/dbt.md`` / ``dbt_mode.py``).

Same interface as :class:`~duckstring.duck.executor.RippleExecutor` (``submit``/``export``/``wipe``/
``shutdown``), but a Ripple isn't a Python function — it's a dbt **model**. Running one invokes dbt scoped
to that model (``dbt run --select model``) against the Pond's own DuckDB registry, after materialising the
cross-Pond Sources the model reads into the registry as the relations dbt resolves them to.

**Why no persistent registry connection** (the one structural difference from ``RippleExecutor``): dbt owns
its own DuckDB connection to the registry file for the duration of a ``run``. Two open connections to one
DuckDB file in a process raise a file-handle conflict, so this executor never holds one — every step
(source materialisation, the dbt run, the export) opens a transient connection and a single ``_lock``
serialises them so no two ever overlap.
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from ..catchment.registry import pond_data_dir, pond_major_dir, pond_registry_path
from .executor import _export_data


class DbtExecutor:
    def __init__(self, pond_name: str, major: int, version: str, source_path: str, root: Path,
                 data_root: str | None = None):
        from ..core import read_pond_toml
        from ..dbt_mode import parse_manifest, project_dir, write_profile
        from ..keys import spec_major

        self.pond_name = pond_name
        self.major = major
        self.version = version
        self.root = root
        self.data_root = data_root
        self.registry_path = pond_registry_path(root, pond_name, major)
        self.registry_path.parent.mkdir(parents=True, exist_ok=True)
        self.own_data_dir = pond_data_dir(root, pond_name, major, data_root)

        source_dir = root / source_path
        info = read_pond_toml(source_dir)
        self.proj_dir = project_dir(source_dir, info)
        self.declared_sources = set(info.get("sources", {}).keys())
        self.source_majors = {s: spec_major(spec) for s, spec in info.get("sources", {}).items()}

        # dbt's writable areas, isolated per major line under the Pond's dir; the profile points dbt at
        # this line's registry. Parse once for the per-model source lookup materialisation needs.
        major_dir = pond_major_dir(root, pond_name, major)
        self.profiles_dir = write_profile(major_dir / "dbt_profiles", str(self.registry_path))
        self.target_dir = major_dir / "dbt_target"
        self.manifest = parse_manifest(self.proj_dir, self.profiles_dir)
        from ..dbt_mode import manifest_topology

        self._parents = manifest_topology(self.manifest)  # model → parent models (lineage's declared half)

        # One lock over ALL registry access (materialise / dbt run / export / wipe) so no two DuckDB
        # connections to the registry file are ever open at once.
        self._lock = threading.Lock()
        self._pool = ThreadPoolExecutor(max_workers=1)

    def submit(self, ripple_name, f, previous_f, on_done, on_error,
               sources_changed: bool = True, skip_sink=None):
        """Run the dbt model ``ripple_name`` at freshness ``f``. dbt models are plain overwrite nodes, so
        ``sources_changed``/``skip_sink`` (the no-change-skip hooks) don't apply. Callbacks fire on the
        pool thread, matching RippleExecutor — ``on_done(name, started, finished, lineage)``, where the
        model's lineage is the cross-Pond sources it materialised (observed via the Pond handle) plus its
        parent models (from the manifest — dbt's ``ref()`` graph is declared, no observation needed)."""
        timing: dict = {}

        def _task():
            timing["started"] = datetime.now(timezone.utc)
            with self._lock:
                src_reads = self._materialize(ripple_name, f, previous_f)  # opens + closes its own connection
                self._run(ripple_name)                                      # dbt opens + closes its own connection
            parents = self._parents.get(ripple_name, [])
            timing["lineage"] = {
                "reads": sorted(src_reads + [[None, p] for p in parents], key=lambda p: (p[0] or "", p[1])),
                "writes": [ripple_name],
            }

        fut = self._pool.submit(_task)

        def _cb(fut):
            exc = fut.exception()
            finished = datetime.now(timezone.utc)
            started = timing.get("started", finished)
            if exc:
                on_error(ripple_name, exc, started, finished)
            else:
                on_done(ripple_name, started, finished, timing.get("lineage"))

        fut.add_done_callback(_cb)
        return fut

    def _materialize(self, model_name, f, previous_f) -> list:
        import duckdb

        from ..core import Pond
        from ..dbt_mode import materialize_sources

        con = duckdb.connect(str(self.registry_path))
        try:
            pond = Pond(
                name=self.pond_name, version=self.version, con=con, root=Path(self.root),
                source_majors=self.source_majors, f=f, previous_f=previous_f, data_root=self.data_root,
            )
            materialize_sources(pond, self.manifest, model_name, self.declared_sources)
            return pond.take_lineage()["reads"]  # the cross-Pond source tables this model consumed
        finally:
            con.close()

    def _run(self, model_name) -> None:
        from ..dbt_mode import run_model

        run_model(self.proj_dir, self.profiles_dir, model_name, self.target_dir)

    def export(self, f: datetime | None = None, contract=None) -> dict | None:
        """Publish the models (the ``main``-schema tables dbt built) via the data plane, contract-gated.
        The materialised Source relations live in their own schemas, so the ``main``-only export never
        republishes them."""
        import duckdb

        with self._lock:
            con = duckdb.connect(str(self.registry_path))
            return _export_data(con.cursor(), self.own_data_dir, f, contract)

    def wipe(self) -> None:
        """A Refresh's cold reset: drop the model tables so the next run rebuilds from scratch. Source
        schemas are left (re-materialised per run anyway)."""
        import duckdb

        from ..core import retry_on_lock

        def _drop() -> None:
            with self._lock:
                con = duckdb.connect(str(self.registry_path))
                try:
                    for (v,) in con.execute(
                        "SELECT view_name FROM duckdb_views() WHERE schema_name = 'main' AND NOT internal"
                    ).fetchall():
                        con.execute(f'DROP VIEW IF EXISTS "{v}"')
                    for (t,) in con.execute(
                        "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main'"
                    ).fetchall():
                        con.execute(f'DROP TABLE IF EXISTS "{t}"')
                finally:
                    con.close()

        retry_on_lock(_drop)

    def shutdown(self) -> None:
        self._pool.shutdown(wait=True)
