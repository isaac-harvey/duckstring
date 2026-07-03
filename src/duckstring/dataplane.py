"""The **data plane** — how a Pond *publishes* its tables for, and *reads* them from, other Ponds.

This is the cross-Pond interchange layer, distinct from the DuckDB registry where Ripples compute.
Today it is whole-table Parquet replace (overwrite-per-run); the :class:`DataPlane` interface is the
seam an Iceberg snapshot/catalog backend slots into later (see ``plans/data-plane-iceberg.md``)
*without touching call sites*. It already carries the shape that work needs:

- a write ``mode`` — ``"overwrite"`` now; ``"append"`` / ``"merge"`` are **reserved** for Trickle and
  raise until implemented, so call sites route a mode rather than baking overwrite in;
- a per-run freshness stamp ``f`` — a no-op against plain Parquet (no snapshot metadata), but the hook
  an Iceberg backend records on each snapshot so a run is resolvable from its freshness;
- the reserved ``_duckstring_*`` system-column namespace, rejected at write so future framework columns
  (``_duckstring_f`` and siblings) can be claimed without a later breaking rename.
"""

from __future__ import annotations

from pathlib import Path

from .storage import LocalStorage, Storage, get_storage


def _as_storage(data_dir) -> Storage:
    """Coerce a ``data_dir`` to a :class:`~duckstring.storage.Storage`. Accepts a ``Storage`` (the
    runtime passes one, possibly object-store-backed) **or** a local ``Path``/str (tests and standalone
    callers), so every data-plane entry point works with either."""
    return data_dir if isinstance(data_dir, Storage) else get_storage(data_dir)

# System columns are framework-owned and persisted; the WHOLE prefix is reserved (not a single name),
# leaving room for siblings (``_duckstring_f`` for freshness, ``_duckstring_d`` for the Z-set weight, …).
# The Trickle subpackage owns this namespace (its system columns live in it); re-exported here so the data
# plane and Trickle share a single source of truth (see duckstring/trickle/context.py).
from .trickle.context import SYSTEM_PREFIX as RESERVED_PREFIX  # noqa: E402

# Write modes the interface can express. Only ``overwrite`` is implemented in Phase 1; the others are
# the history-preserving Trickle write paths, reserved here so call sites don't hard-code a mode.
WRITE_MODES = ("overwrite", "append", "merge")


class ReservedColumnError(ValueError):
    """A published table carries a column in the reserved ``_duckstring_*`` namespace."""


class DataPlane:
    """The cross-Pond data interchange contract. Backends implement publish (``export``) and consume
    (``read_select`` / ``list_tables`` / ``table_path``)."""

    def export(self, con, data_dir: Path, *, mode: str = "overwrite", f=None) -> None:
        """Publish every table in ``con``'s registry to ``data_dir`` for cross-Pond consumption.

        ``mode`` selects the write semantic (only ``"overwrite"`` in Phase 1). ``f`` is the run's
        freshness, recorded by backends that snapshot. Rejects any table carrying a reserved
        (``_duckstring_*``) column."""
        raise NotImplementedError

    def prepare(self, con) -> None:
        """Make ``con`` able to read this backend's published tables (e.g. load a DuckDB extension).
        Idempotent; a no-op for the Parquet backend. Call once before using ``read_select`` on ``con``."""

    def read_select(self, data_dir: Path, table: str, *, as_of=None) -> str:
        """A DuckDB ``SELECT`` over a published Source ``table``, for registering as a view or relation.
        ``as_of`` (a freshness) is the **as-of read seam**: the Source snapshot whose ``f <= as_of``;
        ``None`` reads the latest. Raises :class:`FileNotFoundError` when the Source has not published it yet.

        A **merge Trickle main** is log-structured (a base + the ``__changelog``), so it is *reconstructed*
        here (latest-per-PK over the base ⊎ the changelog newer than the fold watermark ``f_base``); every
        other table is a direct physical read (:meth:`_raw_read_select`)."""
        from .trickle.io import load_sidecar

        data_dir = _as_storage(data_dir)
        meta = load_sidecar(data_dir).get(table, {})
        if meta.get("mode") == "merge":
            return self._reconstruct_select(data_dir, table, meta, as_of)
        return self._raw_read_select(data_dir, table, as_of=as_of)

    def _raw_read_select(self, data_dir: Path, table: str, *, as_of=None) -> str:
        """A direct physical ``SELECT`` over a published table (no reconstruction) — the backend's storage
        read. Used as-is for append/overwrite tables and as the base/changelog operands of a merge read."""
        raise NotImplementedError

    def _reconstruct_select(self, data_dir: Path, table: str, meta: dict, as_of=None) -> str:
        """Reconstruct a merge main's current state from its cold base + the warm tier (``__band``) ⊎ hot
        ``__changelog`` (all read via :meth:`_raw_read_select`), per
        :func:`duckstring.trickle.io.reconstruct_sql`. The warm + hot freshness ranges are disjoint, so their
        union is the changelog above the cold-base watermark with no double-count."""
        from datetime import datetime

        from .trickle.io import changelog_name, reconstruct_sql, warm_name

        clogs = []
        for companion in (changelog_name(table), warm_name(table)):
            try:
                clogs.append(self._raw_read_select(data_dir, companion, as_of=as_of))
            except FileNotFoundError:
                pass
        try:
            base_sql = self._raw_read_select(data_dir, table, as_of=as_of)
        except FileNotFoundError:
            base_sql = None
        if not clogs:
            if base_sql is None:
                raise FileNotFoundError(data_dir.uri(table))
            return base_sql
        clog_sql = " UNION ALL BY NAME ".join(f"({c})" for c in clogs)
        f_base = datetime.fromisoformat(meta["f_base"]) if meta.get("f_base") else None
        return reconstruct_sql(base_sql, clog_sql, f_base, tuple(meta.get("pk", ())), upper=as_of)

    def consolidated_count_select(self, data_dir: Path, table: str, meta: dict, as_of=None) -> str:
        """A scalar ``SELECT`` for the merge main's **current-state row count**, computed *without scanning the
        base data*: ``count(cold base)`` (Parquet metadata, no scan) **+** the net Z-set weight
        ``sum(_duckstring_d)`` of the changelog (warm ⊎ hot) above the fold watermark ``f_base``. For a valid
        merge log each present row nets to weight ``+1``, an update to ``0`` and a delete to ``-1``, so this
        equals ``count(*)`` over :meth:`_reconstruct_select` — but as metadata + a small delta scan instead of
        a full base scan. ``as_of`` clamps both tiers (the read seam), same as the reconstruct."""
        from datetime import datetime

        from .trickle.io import D_COL, F_COL, _ts, changelog_name, warm_name

        data_dir = _as_storage(data_dir)
        clogs = []
        for companion in (changelog_name(table), warm_name(table)):
            try:
                clogs.append(self._raw_read_select(data_dir, companion, as_of=as_of))
            except FileNotFoundError:
                pass
        try:
            base_sql = self._raw_read_select(data_dir, table, as_of=as_of)
        except FileNotFoundError:
            base_sql = None
        base_cnt = f"(SELECT count(*) FROM ({base_sql}))" if base_sql else "0"
        if not clogs:
            return f"SELECT {base_cnt}"
        clog_sql = " UNION ALL BY NAME ".join(f"({c})" for c in clogs)
        lo = f' WHERE "{F_COL}" > {_ts(datetime.fromisoformat(meta["f_base"]))}' if meta.get("f_base") else ""
        delta = f'(SELECT coalesce(sum("{D_COL}"), 0) FROM ({clog_sql}){lo})'
        return f"SELECT {base_cnt} + {delta}"

    def list_tables(self, data_dir) -> list[str]:
        """The names of the tables a Pond has published into ``data_dir``."""
        raise NotImplementedError

    def table_path(self, data_dir, table: str) -> Path | None:
        """The single on-disk artifact for ``table`` (a local ``Path``) — used to serve a file directly.
        ``None`` when the data plane is on an object store (no local path; use :meth:`files_for`)."""
        data_dir = _as_storage(data_dir)
        if not isinstance(data_dir, LocalStorage):
            return None
        from . import trickle_io as trickle

        d = data_dir.root / table
        if d.is_dir():
            return d  # an append-only parts directory
        base = data_dir.root / trickle.base_dir_name(table)
        if base.is_dir():
            return base  # a log-structured merge-main base (chunk directory)
        return data_dir.root / f"{table}.parquet"

    def files_for(self, data_dir, table: str) -> list[tuple[tuple[str, ...], str]]:
        """The published files comprising ``table`` as ``(storage_parts, arcname)`` pairs — for serving the
        raw Parquet (a single file, an append-only parts directory, or a merge-main base chunk dir) over the
        duct/ripple routes, **independent of the storage backend**. The caller reads each file's bytes via
        ``data_dir.read_bytes(*storage_parts)`` and writes it into a zip under ``arcname``."""
        data_dir = _as_storage(data_dir)
        from . import trickle_io as trickle

        parts = trickle.table_parts(data_dir, table)
        if parts:
            return [((table, n), f"{table}/{n}") for n in parts]
        chunks = trickle.base_chunks(data_dir, table)
        if chunks:
            bd = trickle.base_dir_name(table)
            return [((bd, n), f"{bd}/{n}") for n in chunks]
        if data_dir.exists(f"{table}.parquet"):
            return [((f"{table}.parquet",), f"{table}.parquet")]
        return []


def _read_parquet_glob(glob: str, as_of=None) -> str:
    """A ``SELECT`` over a glob of Parquet parts/chunks, optionally clamped to the **as-of** freshness
    (``_duckstring_f <= as_of``) — a row-level predicate DuckDB stat-prunes to whole files where the parts
    are ``_duckstring_f``-homogeneous (the append/changelog case) and a partial scan otherwise (a base
    chunk spans a freshness range).

    ``union_by_name`` is **required**: the parts in a directory are written independently across runs (and
    across redeploys/rebuilds), so their column order can drift. Without it ``read_parquet`` aligns a glob
    **positionally** (first file's schema wins), which silently reads a user column's values into the
    ``_duckstring_d`` slot — a corruption that surfaces as an absurd ``SUM(_duckstring_d)`` (BIGINT overflow)
    on the reconstruct. By name, columns always bind correctly and a missing column reads as NULL."""
    from .trickle.io import F_COL, _ts

    sel = f"SELECT * FROM read_parquet('{glob}', union_by_name=true)"
    if as_of is not None:
        sel += f' WHERE "{F_COL}" <= {_ts(as_of)}'
    return sel


def _check_mode(mode: str) -> None:
    if mode == "overwrite":
        return
    if mode in WRITE_MODES:
        raise NotImplementedError(
            f"write mode {mode!r} is reserved for Trickle (history-preserving append/merge) and is not "
            f"implemented yet — Ripples write 'overwrite'"
        )
    raise ValueError(f"unknown write mode {mode!r} (expected one of {', '.join(WRITE_MODES)})")


def _reserved_columns(con, table: str) -> list[str]:
    # DESCRIBE's first column is the column name; flag any in the reserved namespace.
    return [
        row[0] for row in con.execute(f'DESCRIBE "{table}"').fetchall()
        if str(row[0]).startswith(RESERVED_PREFIX)
    ]


def registry_tables(con) -> list[str]:
    """The table names a Pond has written into ``con``'s registry — the publish set. Tables in the
    reserved ``_duckstring_*`` namespace (Trickle's mode/PK meta) are framework-internal, never published.

    Only **base tables** count: a Pond's real output is always a table (``write_table`` create+rename, or
    a Trickle main/changelog), never a view. ``read_table("source.table")`` registers each foreign Source
    as a same-named *view* so SQL can ``FROM table`` it — those must NOT be published (``SHOW TABLES``
    lists views too, so this filtered ``duckdb_tables()`` query is what stops a Pond from re-exporting a
    full copy of every Source it reads)."""
    return [
        t for (t,) in con.execute(
            "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main' ORDER BY table_name"
        ).fetchall() if not t.startswith(RESERVED_PREFIX)
    ]


def validate_publish(con, table: str) -> None:
    """Reject a table carrying a column in the reserved ``_duckstring_*`` namespace (framework-owned)."""
    reserved = _reserved_columns(con, table)
    if reserved:
        raise ReservedColumnError(
            f"table '{table}' has column(s) {', '.join(reserved)} in the reserved "
            f"'{RESERVED_PREFIX}*' namespace — these names are framework-owned; rename them"
        )


def publish_plan(con, data_dir: Path, f=None) -> list[str]:
    """Validate the publish set, write the ``_trickle.json`` sidecar, and return the tables to publish.

    Trickle tables (a clean *main* + its ``__changelog`` Z-set companion) legitimately carry
    ``_duckstring_*`` system columns, so they are exempt from the reserved-column check that guards plain
    overwrite output. The sidecar carries one entry per published *base* table — ``{mode, pk, floor}`` for
    a Trickle, ``{mode: "overwrite"}`` for plain output — each stamped with this run's freshness ``f`` so a
    cross-Pond reader can resolve a Trickle's coverage *and* detect whether an overwrite source advanced
    (its ``f`` vs the consumer's ``previous_f``). Call this *before* writing anything so a reserved-column
    violation aborts the whole publish (last-good left intact)."""
    from datetime import timezone

    from . import trickle_io as trickle

    meta = trickle.read_meta(con)
    changelogs = {trickle.changelog_name(t) for t in meta}
    droplogs = {f"{t}{trickle.DROPLOG_SUFFIX}" for t in meta}
    warms = {trickle.warm_name(t) for t in meta}
    tables = registry_tables(con)
    f_iso = f.astimezone(timezone.utc).isoformat() if f is not None else None
    payload: dict[str, dict] = {}
    for table in tables:
        if table in meta or table in changelogs or table in droplogs or table in warms:
            continue  # Trickle base/companion — base added below; the __changelog/__band/__droplog
            #            companions are exported as files (reserved system columns) but take no sidecar entry.
        validate_publish(con, table)
        payload[table] = {"mode": "overwrite", "f": f_iso}
    for base, m in meta.items():
        entry = {"mode": m["mode"], "pk": list(m["pk"]), "floor": m.get("floor"), "f": f_iso}
        if m["mode"] == "merge":
            entry["f_base"] = m.get("f_base")  # fold watermark — the read reconstructs base ⊎ changelog>f_base
        payload[base] = entry
    # Objects persist until overwritten (not per-run declared), so carry their sidecar section forward —
    # this run's staged Object commits fold their fresh entries in afterwards (see objects.commit_objects).
    existing_objects = trickle.load_sidecar(data_dir).get("objects")
    if existing_objects:
        payload["objects"] = existing_objects
    trickle.write_sidecar(data_dir, payload)
    return tables


def unpublish_table(data_dir, name: str) -> None:
    """Remove a table's **published** collection from ``data_dir`` — every physical form (wholesale
    ``{name}.parquet``, the per-run parts dir ``{name}/``, the ``__changelog`` / ``__band`` / ``__base`` /
    ``__droplog`` companion dirs) **and** its sidecar entry. The registry side is dropped separately
    (:func:`duckstring.trickle.io.drop_table`); together they are one table delete (see plans/deletes.md).
    Idempotent — missing forms are skipped."""
    from . import trickle_io as trickle

    store = _as_storage(data_dir)
    store.remove(f"{name}.parquet")
    for d in (name, trickle.changelog_name(name), trickle.warm_name(name),
              trickle.base_dir_name(name), f"{name}{trickle.DROPLOG_SUFFIX}"):
        store.rmtree(d)
    sidecar = trickle.load_sidecar(store)
    if name in sidecar:
        del sidecar[name]
        trickle.write_sidecar(store, sidecar)


class ParquetDataPlane(DataPlane):
    """The zero-dependency default. A plain overwrite output is one ``{table}.parquet`` file, overwritten
    per run. An **append-only** Trickle table (append history, ``__changelog``, ``__droplog``) is a
    *directory* of per-run parts ``{table}/{f}.parquet`` (O(change) per run). A **merge main** is
    log-structured: its ``__changelog`` publishes per run (parts) and its base ``{table}.parquet`` is
    rewritten only at a **checkpoint** (when the changelog since the fold watermark outgrows the base, past
    ``DUCKSTRING_COMPACT_THRESHOLD``); reads reconstruct base ⊎ changelog (see :meth:`DataPlane.read_select`)."""

    def export(self, con, data_dir, *, mode: str = "overwrite", f=None) -> None:
        from . import trickle_io as trickle
        from .core import retry_on_lock

        _check_mode(mode)
        data_dir = _as_storage(data_dir)
        data_dir.duckdb_setup(con)  # object store → load httpfs + register the credential secret (no-op local)
        data_dir.mkdir()

        tables = publish_plan(con, data_dir, f)
        meta = trickle.read_meta(con)
        incremental = trickle.incremental_tables(meta) if f is not None else set()
        merge_mains = {t for t, m in meta.items() if m.get("mode") == "merge"}

        def _export() -> None:
            for table in tables:
                if table in incremental:
                    _export_parts(con, data_dir, table, f)
                elif table in merge_mains:
                    continue  # the base is published only at a checkpoint (below), not per run
                else:
                    with data_dir.copy_to(f"{table}.parquet") as uri:
                        con.execute(f'COPY "{table}" TO \'{uri}\' (FORMAT PARQUET)')
            for main in merge_mains:
                _publish_tiered_main(con, data_dir, main, f)
            if merge_mains:  # a checkpoint may have advanced f_base → refresh the sidecar
                publish_plan(con, data_dir, f)

        retry_on_lock(_export)

    def _raw_read_select(self, data_dir, table: str, *, as_of=None) -> str:
        from . import trickle_io as trickle

        data_dir = _as_storage(data_dir)
        if trickle.table_parts(data_dir, table):  # append-only parts directory → union the parts
            return _read_parquet_glob(data_dir.glob("*.parquet", table), as_of)
        if trickle.base_chunks(data_dir, table):  # log-structured merge-main base → union its chunks
            return _read_parquet_glob(data_dir.glob("*.parquet", trickle.base_dir_name(table)), as_of)
        if not data_dir.exists(f"{table}.parquet"):
            raise FileNotFoundError(data_dir.uri(f"{table}.parquet"))
        return f"SELECT * FROM read_parquet('{data_dir.uri(table + '.parquet')}')"

    def list_tables(self, data_dir) -> list[str]:
        from . import trickle_io as trickle

        data_dir = _as_storage(data_dir)
        if not data_dir.exists():
            return []
        files = {n[: -len(".parquet")] for n in data_dir.parquet_names()}
        parts = set(trickle.part_tables(data_dir))
        # A merge main is reconstructed from its changelog; it is a published table even before its base
        # exists (no checkpoint yet → no `{main}.parquet`), so surface it from the sidecar.
        mains = {t for t, m in trickle.load_sidecar(data_dir).items() if m.get("mode") == "merge"}
        return sorted(files | parts | mains)


def _export_parts(con, data_dir, table: str, f) -> None:
    """Publish an append-only ``table`` as a directory of per-run Parquet parts. Writes one
    ``_duckstring_f``-homogeneous file per registry freshness not already on disk (so a normal run writes
    just its new slice, and a rebuild/restore backfills any missing parts), and drops parts whose freshness
    is no longer in the registry (mirroring retention). Idempotent on replay.

    When the table is **empty** (a bootstrap-only changelog with no rows yet), a schema-only marker part is
    still written at the run's ``f`` so the table stays readable as an empty relation — a consumer covered
    by the floor then sees an *empty* delta, not a coverage-miss full read."""
    from . import trickle_io as trickle

    part_store = data_dir.child(table)
    part_store.mkdir()
    reg_fs = {r[0] for r in con.execute(f'SELECT DISTINCT "{trickle.F_COL}" FROM "{table}"').fetchall()
              if r[0] is not None}
    if not reg_fs and f is not None:
        reg_fs = {f}  # synthesize a 0-row marker part (the `WHERE = f` below selects nothing → empty part)
    existing = {trickle.part_f(n): n for n in part_store.parquet_names()}
    # Write the parts not yet on disk; *always* (re)write the current run's f, whose content may have changed
    # this run (a same-f re-merge, or a replay) — older f's are immutable history and are skipped if present.
    to_write = (reg_fs - set(existing)) | ({f} if (f is not None and f in reg_fs) else set())
    for fi in to_write:
        with part_store.copy_to(trickle.part_name(fi)) as uri:
            con.execute(
                f'COPY (SELECT * FROM "{table}" WHERE "{trickle.F_COL}" = {trickle._ts(fi)}) '
                f"TO '{uri}' (FORMAT PARQUET)"
            )
    for fi, name in existing.items():  # drop parts the registry no longer retains (or the superseded marker)
        if fi not in reg_fs:
            part_store.remove(name)


def _compact_threshold(con=None, main=None) -> int:
    """The checkpoint floor / target base-chunk size in bytes. A merge main checkpoints when its
    changelog-since-the-fold-watermark outgrows ``max(base size, this)`` — so it never checkpoints below
    this, and otherwise at ~k=1 (changelog ≥ base). Resolution order: a per-table override recorded at the
    merge write (``merge_table(..., compact_threshold=)``), else the catchment-level
    ``DUCKSTRING_COMPACT_THRESHOLD`` env, else 256 MiB."""
    import os

    if con is not None and main is not None:
        from . import trickle_io as trickle

        override = trickle.read_meta(con).get(main, {}).get("compact_threshold")
        if override is not None:
            return int(override)
    return int(os.environ.get("DUCKSTRING_COMPACT_THRESHOLD", str(256 * 1024 * 1024)))


def _base_bytes(data_dir, main: str) -> int:
    """The on-disk size of a merge main's published base — the sum of its ``{main}__base/`` chunks, or the
    legacy single ``{main}.parquet`` file. ``0`` when no base has been published yet."""
    from . import trickle_io as trickle

    chunks = trickle.base_chunks(data_dir, main)
    if chunks:
        base = data_dir.child(trickle.base_dir_name(main))
        return sum(base.size(n) for n in chunks)
    return data_dir.size(f"{main}.parquet")


def _publish_tiered_main(con, data_dir: Path, main: str, f) -> None:
    """Maintain a merge main's tiered storage at publish time (see plans/trickle-main-incremental.md):

    - **Cold compaction** (rare, O(base)) when the warm tier has grown to match the cold base (k=1): fold
      base + warm + hot ``≤ f`` into a fresh clean base, republish the base chunks, and clear the warm bands.
    - **Warm fold** (frequent, cheap) otherwise, once the hot changelog has accumulated past ``2×`` the chunk
      threshold: move its older slice into a warm band, leaving a ``~threshold`` hot window for caught-up
      consumers' delta reads, and publish the new band.

    The hot changelog parts themselves are exported by the main publish loop; here we only re-sync them after
    a fold/compaction trims the registry changelog."""
    from . import trickle_io as trickle

    threshold = _compact_threshold(con, main)
    clog, warm = trickle.changelog_name(main), trickle.warm_name(main)
    warm_store = data_dir.child(warm)
    warm_bytes = sum(warm_store.size(n) for n in trickle.table_parts(data_dir, warm))
    cold_bytes = _base_bytes(data_dir, main)

    clog_store = data_dir.child(clog)
    f_warm = trickle._f_warm(con, main) or trickle._f_base(con, main)
    hot = [n for n in trickle.table_parts(data_dir, clog)
           if f_warm is None or trickle.part_f(n) > f_warm]
    hot_bytes = sum(clog_store.size(n) for n in hot)

    # Cold compaction (k=1: warm ≥ cold), or the **bootstrap** of the very first base directly from the hot
    # changelog (no warm tier yet) so a fresh main folds straight to cold rather than via a warm round-trip.
    bootstrap = cold_bytes == 0 and (warm_bytes + hot_bytes) >= threshold
    if warm_bytes >= max(cold_bytes, threshold) or bootstrap:  # cold compaction (k=1: warm ≥ cold)
        trickle.checkpoint(con, main, f)  # fold base+warm+hot≤f → clean base; clear warm; advance f_base/f_warm
        if trickle._table_exists(con, main):
            _publish_base_chunks(con, data_dir, main, f, threshold)
        if data_dir.is_dir(warm):
            data_dir.rmtree(warm)  # the warm bands are now folded into the cold base
        _export_parts(con, data_dir, clog, f)  # re-sync the hot parts after retention trim
        return

    if hot_bytes >= 2 * threshold:  # warm fold: pack the oldest hot parts, leaving a ~threshold hot window
        warm_target, remaining = None, hot_bytes
        for n in hot[:-1]:  # oldest-first; never the newest part → a caught-up consumer's latest delta stays
            if remaining <= threshold:
                break
            remaining -= clog_store.size(n)
            warm_target = trickle.part_f(n)
        if warm_target is not None:
            trickle.fold_warm(con, main, warm_target)
            _export_bands(con, data_dir, main)
            _export_parts(con, data_dir, clog, f)  # drop the folded hot parts (now in the warm band)


def _export_bands(con, data_dir: Path, main: str) -> None:
    """Publish the merge main's warm tier as freshness-range **band** files (``{main}__band/{f}.parquet``),
    one per fold, append-only. Each band keeps its rows' original ``_duckstring_f`` (so as-of reads stay
    correct) and is named by its upper freshness. Idempotent: a band already on disk is not rewritten."""
    from . import trickle_io as trickle

    warm = trickle.warm_name(main)
    f_warm = trickle._f_warm(con, main)
    if not trickle._table_exists(con, warm) or f_warm is None:
        return
    band_store = data_dir.child(warm)
    band_store.mkdir()
    dest_name = trickle.part_name(f_warm)
    if band_store.exists(dest_name):  # replay-idempotent
        return
    published = [trickle.part_f(n) for n in band_store.parquet_names()]
    last_hi = max(published) if published else None
    fb = f'"{trickle.F_COL}"'
    lo = f"{fb} > {trickle._ts(last_hi)} AND " if last_hi is not None else ""
    with band_store.copy_to(dest_name) as uri:
        con.execute(
            f'COPY (SELECT * FROM "{warm}" WHERE {lo}{fb} <= {trickle._ts(f_warm)}) '
            f"TO '{uri}' (FORMAT PARQUET)"
        )


def _publish_base_chunks(con, data_dir: Path, main: str, f, chunk_bytes: int) -> None:
    """Publish the registry base table ``main`` as a directory of size-bounded, freshness-ordered Parquet
    chunks (``{main}__base/``). **Lock-free, overlap-safe**: the new chunks are written under this
    checkpoint's unique token, then the chunks of any *other* token are removed — a concurrent reader that
    momentarily sees both old and new chunks reconstructs latest-per-PK over base ⊎ changelog, which is
    idempotent (the published sidecar's ``f_base`` only advances *after* this returns, so the changelog
    still covers any row a stale chunk would otherwise resurrect). Replaces a legacy single-file base."""
    from . import trickle_io as trickle

    base_name = trickle.base_dir_name(main)
    base_store = data_dir.child(base_name)
    base_store.mkdir()
    token = trickle.part_name(f)[: -len(".parquet")]  # unique per checkpoint, freshness-ordered
    staging_name = base_name + ".tmp"
    staging_store = data_dir.child(staging_name)
    fb = trickle._q(trickle.F_COL)
    size = max(1, int(chunk_bytes))
    written = []
    with data_dir.copy_dir_to(staging_name) as staging_uri:  # clears staging, yields the dir target
        con.execute(
            f'COPY (SELECT * FROM "{main}" ORDER BY {fb}) '
            f"TO '{staging_uri}' (FORMAT PARQUET, FILE_SIZE_BYTES {size})"
        )
        for i, name in enumerate(staging_store.parquet_names()):  # commit each staged chunk under our token
            dest = f"{token}__{i}.parquet"
            staging_store.move_into(base_store, name, dest)
            written.append(dest)
    data_dir.rmtree(staging_name)
    for old in base_store.parquet_names():  # drop the previous checkpoint's chunks (different token)
        if old not in written:
            base_store.remove(old)
    data_dir.remove(f"{main}.parquet")  # supersede a legacy single-file base (no-op if absent)


def get_data_plane() -> DataPlane:
    """The active data-plane backend, selected by ``DUCKSTRING_DATA_PLANE``:

    - ``iceberg`` (default) — the Apache Iceberg base layer (snapshots + schema metadata over the
      Parquet data files); its deps are in core, so it's available out of the box;
    - ``parquet`` — the whole-table Parquet plane, the opt-out for the lightest footprint or for an
      offline Catchment that can't fetch DuckDB's iceberg extension.

    Iceberg is the default because the version-contract (schema) and incremental work build on its
    metadata; ``parquet`` stays a first-class fallback."""
    import os

    backend = os.environ.get("DUCKSTRING_DATA_PLANE", "iceberg").lower()
    if backend == "parquet":
        return ParquetDataPlane()
    if backend == "iceberg":
        try:
            from .iceberg_plane import IcebergDataPlane
        except ImportError as exc:  # pragma: no cover - pyiceberg is a core dep, but guard a stripped install
            raise NotImplementedError(
                "the iceberg data plane needs pyiceberg (a core dependency) — reinstall duckstring, "
                "or set DUCKSTRING_DATA_PLANE=parquet for the lighter plane"
            ) from exc
        return IcebergDataPlane()
    raise ValueError(
        f"unknown DUCKSTRING_DATA_PLANE {backend!r} (expected 'iceberg' or 'parquet')"
    )
