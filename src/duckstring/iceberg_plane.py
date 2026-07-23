"""The Iceberg data-plane backend — the default (``DUCKSTRING_DATA_PLANE`` unset or ``iceberg``).

An Apache Iceberg base layer over the Parquet files we already write: it adds **snapshots** (one
overwrite commit per Pond Run, stamped with the run's freshness ``f``) and **schema metadata**, the
substrate Phase 2 contracts and the later Trickle work build on. The data files stay Parquet — this is
a metadata/catalog layer, not a file-format swap.

Layout (per ``name@major`` line, so the major-line isolation ``ponds/{name}/m{major}/`` already gives
is preserved physically, not just by namespace — and there is no shared catalog for concurrent Ducks
to contend on):

- a :class:`~duckstring.iceberg_catalog.FileCatalog` (a JSON-pointer pyiceberg catalog, **no
  SQLAlchemy**) at ``{data_dir}/catalog.json``, warehouse rooted at ``data_dir``;
- one namespace, ``pond`` — the catalog is already isolated to one line, so the table is ``pond.{table}``.

Writes go through pyiceberg (Arrow ``overwrite``); reads go through DuckDB's ``iceberg`` extension
(``iceberg_scan`` on the snapshot's metadata file). A **flat ``{table}.parquet`` copy is written
alongside** each commit: it keeps the unchanged consumers working behaviour-neutrally — the duct/draw
file transfer, the direct file-serve, and the transitional read of a Source that hasn't re-exported to
Iceberg yet. The ``catalog.json`` and the Iceberg metadata/data under ``data_dir`` are included in
``catchment archive`` by the existing root walk (download while quiescent).

**Iceberg is only used for bounded *overwrite* tables** (plain Ripple output) — where its per-run
snapshot + as-of read earn their keep at O(1) metadata cost. The **append-only** Trickle tables (an
append history, a merge ``__changelog``, an ``__droplog``) and a merge **main** base are *not* committed
to Iceberg: an append-only table's current snapshot must reference every data file ever appended, so an
Iceberg commit's metadata grows O(runs) — unbounded — for no read benefit over the flat per-run parts,
which prune just as well by Parquet file stats / ``_duckstring_f``. Those tables are served from the flat
layer (the parts directory the sidecar export already writes), so their publish is O(change) per run, and
``_raw_read_select`` falls back to the flat read whenever a table isn't in the catalog.
"""

from __future__ import annotations

from pathlib import Path

from .dataplane import DataPlane, ParquetDataPlane, _as_storage

_NAMESPACE = "pond"  # the single namespace within each per-line catalog
F_PROP = "duckstring.f"  # snapshot summary property carrying the Pond Run's freshness

# Snapshot/metadata retention. Iceberg accrues a new data file + manifests + snapshot + metadata.json per
# commit, and pyiceberg 0.11 expires snapshots from the *metadata* only (it leaves the files on disk), so
# without an explicit prune every overwrite Pond Run leaks the previous full copy plus a pile of manifest
# avros, forever. We keep the most-recent N snapshots (the as-of read seam can still reach that far back)
# and reclaim everything no surviving snapshot references. The metadata.json files are bounded by pyiceberg
# itself via the cleanup properties below, set at table creation.
_DEFAULT_KEEP_SNAPSHOTS = 5
_CLEANUP_PROPS = {  # let pyiceberg delete superseded metadata.json files itself
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": str(_DEFAULT_KEEP_SNAPSHOTS),
}


def _keep_snapshots() -> int:
    import os

    try:
        return max(1, int(os.environ.get("DUCKSTRING_ICEBERG_KEEP_SNAPSHOTS", _DEFAULT_KEEP_SNAPSHOTS)))
    except ValueError:
        return _DEFAULT_KEEP_SNAPSHOTS


class IcebergDataPlane(DataPlane):
    def __init__(self) -> None:
        # The flat-Parquet sidecar: the compat copy for draws, direct-serve, and the legacy fallback.
        self._parquet = ParquetDataPlane()

    # ─── catalog ──────────────────────────────────────────────────────────────

    def _catalog(self, data_dir):
        from .iceberg_catalog import FileCatalog

        storage = _as_storage(data_dir)
        storage.mkdir()
        # The catalog's pointer object (catalog.json) and its warehouse both live in the data location —
        # local path or object store. The Storage routes the pointer PUT/GET; iceberg_properties carries
        # pyiceberg's own FileIO credentials for an object store (empty for local).
        cat = FileCatalog(
            "duckstring",
            warehouse=storage.warehouse_location(),
            pointer_storage=storage,
            **storage.iceberg_properties(),
        )
        cat.create_namespace_if_not_exists(_NAMESPACE)
        return cat

    def _load(self, data_dir, table: str):
        """The Iceberg table, or ``None`` if this line has no such table yet (pre-Iceberg Source, a table
        never written → served from the flat parts layer)."""
        from pyiceberg.exceptions import NoSuchTableError

        storage = _as_storage(data_dir)
        if not storage.exists("catalog.json"):
            return None
        cat = self._catalog(storage)
        try:
            return cat.load_table(f"{_NAMESPACE}.{table}")
        except NoSuchTableError:
            return None

    # ─── write ──────────────────────────────────────────────────────────────────

    def export(self, con, data_dir: Path, *, mode: str = "overwrite", f=None) -> None:
        from . import trickle_io
        from .dataplane import _check_mode, publish_plan

        _check_mode(mode)
        data_dir = _as_storage(data_dir)
        # Validates the publish set (Trickle tables exempt from the reserved-column check), writes the
        # Trickle mode/PK sidecar, and returns the tables to commit — all before any write.
        tables = publish_plan(con, data_dir, f)
        # Flat-Parquet sidecar first (also the consistent fallback if the Iceberg commit fails). This also
        # runs ``data_dir.duckdb_setup(con)`` so the export connection can COPY to an object store.
        self._parquet.export(con, data_dir, mode=mode, f=f)
        # Only plain **overwrite** tables go to Iceberg. A merge **main** is log-structured (a base + the
        # changelog) reconstructed on read; an **append-only** table (append history, ``__changelog``,
        # ``__droplog``) grows unboundedly in Iceberg metadata (its current snapshot references every data
        # file ever appended). Both are served from the flat parts layer the sidecar export already wrote
        # above, so they are skipped here and the flat read serves them.
        meta = trickle_io.read_meta(con)
        committable = [t for t in tables
                       if not (self._is_incremental(t, meta) or meta.get(t, {}).get("mode") == "merge")]
        # Build the catalog only when there is actually something to commit — a merge/append-only Pond (the
        # whole Trickle path) commits nothing, and building it would cost a wasted catalog.json round-trip.
        if committable:
            # Stamp _duckstring_f (a TIMESTAMPTZ) as UTC for Arrow: pyiceberg accepts only UTC-tz timestamps,
            # and a registry written under a local session tz would otherwise fetch as e.g. tz=Australia/…
            con.execute("SET TimeZone='UTC'")
            cat = self._catalog(data_dir)
            for table in committable:
                arrow = con.execute(f'SELECT * FROM "{table}"').fetch_arrow_table()
                self._commit(cat, table, arrow, f, data_dir)

    @staticmethod
    def _is_incremental(table: str, meta: dict) -> bool:
        """A Trickle append history, or a Trickle's ``__changelog`` / ``__droplog`` companion — the
        append-only tables. A merge *main* (``meta[table]['mode'] == 'merge'``) is overwrite, not
        incremental."""
        from .trickle_io import CHANGELOG_SUFFIX, DROPLOG_SUFFIX

        if meta.get(table, {}).get("mode") == "append":
            return True
        for suffix in (CHANGELOG_SUFFIX, DROPLOG_SUFFIX):
            if table.endswith(suffix) and table[: -len(suffix)] in meta:
                return True
        return False

    def _commit(self, cat, table: str, arrow, f, data_dir) -> None:
        import warnings

        from pyiceberg.exceptions import NoSuchTableError

        ident = f"{_NAMESPACE}.{table}"
        props = {F_PROP: f.isoformat()} if f is not None else {}

        def _create():
            cat.create_namespace_if_not_exists(_NAMESPACE)
            return self._create_table(cat, ident, arrow.schema)

        def _overwrite(tbl):
            with warnings.catch_warnings():
                # Overwriting a fresh/empty table warns "Delete operation did not match any records" —
                # expected on every first write of an overwrite Ripple; suppress the noise.
                warnings.filterwarnings("ignore", message="Delete operation did not match any records")
                tbl.overwrite(arrow, snapshot_properties=props)

        try:
            tbl = cat.load_table(ident)
        except NoSuchTableError:
            tbl = _create()

        try:
            _overwrite(tbl)
        except Exception:
            # A Ripple is overwrite-per-run; if the output schema changed since the table was created,
            # overwrite can't reconcile it. Recreate the table at the new schema (snapshot history is a
            # Phase-2/Trickle concern; an overwrite Ripple keeps no history anyway).
            cat.drop_table(ident)
            _overwrite(_create())
        # Overwrite leaves the prior run's full data file referenced only by the now-superseded snapshot —
        # reclaim it (and the stale manifests) once that snapshot ages out.
        self._prune(cat, table, data_dir)

    @staticmethod
    def _create_table(cat, ident: str, schema):
        """Create the table with pyiceberg's metadata-cleanup properties set, so superseded
        ``*.metadata.json`` files are deleted on each commit rather than accumulating forever."""
        return cat.create_table(ident, schema=schema, properties=dict(_CLEANUP_PROPS))

    # ─── prune (bound on-disk growth) ─────────────────────────────────────────────

    def _prune(self, cat, table: str, data_dir) -> None:
        """Keep only the most-recent ``_keep_snapshots()`` snapshots and physically remove any data /
        manifest files no surviving snapshot references. Space-only — correctness rides the current
        snapshot (always retained) and the consumer's window read — so any failure here is swallowed: a
        Pond Run must never fail on housekeeping. Only overwrite tables reach here now, so each pruned
        snapshot sheds its superseded full data file plus the stale manifests/metadata."""
        try:
            ident = f"{_NAMESPACE}.{table}"
            tbl = cat.load_table(ident)
            keep = _keep_snapshots()
            snaps = sorted(tbl.snapshots(), key=lambda s: s.timestamp_ms)
            if len(snaps) > keep:
                # Drop the oldest; the current snapshot is the newest, so it's never in this set.
                expire = [s.snapshot_id for s in snaps[:-keep]]
                tbl.maintenance.expire_snapshots().by_ids(expire).commit()
                tbl = cat.load_table(ident)
            self._gc_orphan_files(tbl, data_dir, table)
        except Exception:  # pragma: no cover - housekeeping must never break a run
            import logging

            logging.getLogger(__name__).debug("iceberg prune skipped for %s", table, exc_info=True)

    @staticmethod
    def _gc_orphan_files(tbl, data_dir, table: str) -> None:
        """Delete files under the table's ``data/`` and ``metadata/`` dirs that no surviving snapshot (or
        the retained metadata log / current metadata pointer) references — the orphans left behind when a
        snapshot is expired (pyiceberg 0.11 expires metadata only, never the files). Works on local **and**
        object storage: the table lives at ``{warehouse}/pond/{table}/`` so we sweep through the
        :class:`~duckstring.storage.Storage` (``data_dir.child("pond", table, sub)``), comparing file
        **basenames** (uuid-named, collision-free across data/metadata) against the live set."""
        def _base(p) -> str:
            return str(p).rstrip("/").rsplit("/", 1)[-1]

        io = tbl.io
        live: set[str] = {_base(tbl.metadata_location)}
        for entry in tbl.metadata.metadata_log:
            live.add(_base(entry.metadata_file))
        for snap in tbl.snapshots():
            if snap.manifest_list:
                live.add(_base(snap.manifest_list))
            for mf in snap.manifests(io):
                live.add(_base(mf.manifest_path))
                for e in mf.fetch_manifest_entry(io, discard_deleted=False):
                    live.add(_base(e.data_file.file_path))

        for sub in ("data", "metadata"):
            store = data_dir.child(_NAMESPACE, table, sub)
            for name in store.names():
                if name not in live:
                    store.remove(name)

    # ─── read ──────────────────────────────────────────────────────────────────

    def prepare(self, con) -> None:
        try:
            con.execute("LOAD iceberg")
        except Exception:
            con.execute("INSTALL iceberg")
            con.execute("LOAD iceberg")

    def _flat_read_select(self, data_dir: Path, table: str, *, as_of=None) -> str:
        # The always-flat operands (merge base + companions, append-only tables) live only on the flat parts
        # layer; read them directly and skip the pyiceberg catalog build entirely (no catalog.json I/O).
        return self._parquet._raw_read_select(data_dir, table, as_of=as_of)

    def _raw_read_select(self, data_dir: Path, table: str, *, as_of=None) -> str:
        tbl = self._load(data_dir, table)
        if tbl is None:
            # Not in the catalog (a merge base, served from the flat layer; or a Source not yet re-exported
            # to Iceberg) → read its flat Parquet. The base-class read_select handles merge reconstruction.
            return self._parquet._raw_read_select(data_dir, table, as_of=as_of)
        ml = tbl.metadata_location.replace("'", "''")
        snap = self._snapshot_for(tbl, as_of) if as_of is not None else None
        if snap is not None:
            return f"SELECT * FROM iceberg_scan('{ml}', snapshot_from_id => {snap})"
        return f"SELECT * FROM iceberg_scan('{ml}')"

    def _snapshot_for(self, tbl, as_of):
        """The id of the last-committed snapshot whose stamped ``f`` is ``<= as_of`` — the as-of read seam.
        None when no snapshot is eligible (consumer's freshness predates the Source's first run).

        ``tbl.snapshots()`` yields in commit order. One overwrite commit produces TWO snapshots stamped with
        the *same* ``f`` — a DELETE then an APPEND — so ties must NOT break on ``snapshot_id`` (random): that
        can resolve to the empty DELETE. Pick the latest in commit order (``timestamp_ms`` then position) =
        the final committed state at that freshness."""
        from datetime import datetime

        eligible = []
        for i, s in enumerate(tbl.snapshots()):  # commit order
            stamp = s.summary.additional_properties.get(F_PROP) if s.summary else None
            if stamp and datetime.fromisoformat(stamp) <= as_of:
                eligible.append((s.timestamp_ms, i, s.snapshot_id))
        if not eligible:
            return None
        return max(eligible)[2]

    def list_tables(self, data_dir: Path) -> list[str]:
        # The flat sidecar is written for every published table, so its listing is the publish set.
        return self._parquet.list_tables(data_dir)

    def table_path(self, data_dir: Path, table: str) -> Path | None:
        return self._parquet.table_path(data_dir, table)
