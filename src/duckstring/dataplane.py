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
        mode = meta.get("mode")
        if mode == "merge":
            return self._reconstruct_select(data_dir, table, meta, as_of)
        if mode == "append":
            # An append-only Trickle is served from the flat parts layer — never the Iceberg catalog. Read it
            # flat directly so the Iceberg plane doesn't build (and pay ~0.4s of catalog.json I/O over S3 for)
            # a catalog it will only miss in.
            return self._flat_read_select(data_dir, table, as_of=as_of)
        return self._raw_read_select(data_dir, table, as_of=as_of)

    def _raw_read_select(self, data_dir: Path, table: str, *, as_of=None) -> str:
        """A direct physical ``SELECT`` over a published table (no reconstruction) — the backend's storage
        read. Used as-is for an **overwrite** table (which may live in the Iceberg catalog) and as the flat
        fallback of :meth:`_flat_read_select`."""
        raise NotImplementedError

    def _flat_read_select(self, data_dir: Path, table: str, *, as_of=None) -> str:
        """A physical read that MUST bypass any catalog/metadata layer — the operands that are **always** flat
        Parquet: a merge main's cold base and its ``__changelog`` / ``__band`` companions, and an append-only
        table. The Iceberg base layer is overwrite-only, so these are never committed to it; reading them flat
        skips the per-read pyiceberg catalog build (the dominant cost of a merge/append pipeline over S3). The
        base backend has no catalog, so this defaults to the raw read; :class:`IcebergDataPlane` overrides it."""
        return self._raw_read_select(data_dir, table, as_of=as_of)

    def _reconstruct_select(self, data_dir: Path, table: str, meta: dict, as_of=None) -> str:
        """Reconstruct a merge main's current state from its cold base + the warm tier (``__band``) ⊎ hot
        ``__changelog`` (all read flat via :meth:`_flat_read_select`), per
        :func:`duckstring.trickle.io.reconstruct_sql`. The warm + hot freshness ranges are disjoint, so their
        union is the changelog above the cold-base watermark with no double-count.

        S3-frugal: the cold **base** is read only when the sidecar says one exists (``f_base`` set = the main has
        been checkpointed). An un-checkpointed main (the whole changelog is the state — the common case) then
        skips a guaranteed-miss base probe, saving its round-trips over an object store."""
        from datetime import datetime

        from .trickle.io import changelog_name, reconstruct_sql, warm_name

        clogs = []
        for companion in (changelog_name(table), warm_name(table)):
            try:
                clogs.append(self._flat_read_select(data_dir, companion, as_of=as_of))
            except FileNotFoundError:
                pass
        # Attempt the base regardless of f_base: a merge main can have a base with f_base unset — a **legacy
        # single-file** base (`{table}.parquet`), or one restored/seeded outside the checkpoint path. Gating on
        # f_base here dropped that base silently. FileNotFoundError (no base yet — the common un-checkpointed
        # case) is the honest signal; the flat read raises it cheaply.
        try:
            base_sql = self._flat_read_select(data_dir, table, as_of=as_of)
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
                clogs.append(self._flat_read_select(data_dir, companion, as_of=as_of))
            except FileNotFoundError:
                pass
        base_sql = None  # attempt the base regardless of f_base (a legacy single-file base has none) — see
        try:            # _reconstruct_select; FileNotFoundError = no base yet (the common un-checkpointed case)
            base_sql = self._flat_read_select(data_dir, table, as_of=as_of)
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
            _export_companions(con, data_dir, f)  # state-format Extension 1 (runs after the sidecar is final)
            _enrich_sidecar(con, data_dir, f)     # state-format Extension 2 (stats/schema hints, best-effort)

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
    just its new slice, and a rebuild/restore backfills any missing parts). Idempotent on replay.

    **Pruning is floor-anchored, never absence-inferred** (plans/s3-resident-state.md step 1): a published
    part is dropped only when its freshness fell below the table's **floor** — the explicit, positive
    signal every genuine drop advances (retention ``_apply_retention`` → floor; a warm fold → floor; a
    checkpoint/refresh re-bootstrap → floor). A part the registry merely *lacks* is KEPT: absence is what a
    partially-hydrated registry looks like (a fresh box that pulled only the hot window — the future
    no-hydration path), and inferring a drop from it deletes real history.

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
    from datetime import datetime

    floor_iso = trickle.read_meta(con).get(trickle.base_table_name(table), {}).get("floor")
    floor = datetime.fromisoformat(floor_iso) if floor_iso else None
    # The floor's boundary is INCLUSIVE only for a merge __changelog: a warm fold moves the slice
    # `(lo, target]` into the band and sets floor = target, so the boundary part must go (keeping it
    # would double-count on read). For an append history/droplog the boundary stays STRICT — retention
    # keeps its cutoff rows in the registry (shielded by `fi in reg_fs`), and pruning an at-floor part
    # that is merely un-hydrated would lose real rows from full reads.
    inclusive = table.endswith(trickle.CHANGELOG_SUFFIX)
    for fi, name in existing.items():
        if fi in reg_fs or floor is None:
            continue
        if fi <= floor if inclusive else fi < floor:
            part_store.remove(name)  # genuinely dropped (retention / fold / re-bootstrap raised the floor)


def _export_companions(con, data_dir, f) -> None:
    """Publish the registry aggregate/accumulate **state companions** as *state-format Extension 1*
    snapshots (see ``plans/state-format.md`` — the DuckFlock consumer's normative layout, mirrored by
    the Rust driver's ``publish_companions``).

    Incremental ``.aggregate()`` / ``.accumulate()`` keep their cross-run fold state in registry-only
    companion tables (``_duckstring_agg_{table}`` / ``_duckstring_acc_{table}``). A registry-less host
    (or a Duck recovering from registry loss) can only resume incremental compute if that state is
    published, so each run writes one **whole-companion snapshot** to::

        {data_dir}/state/{agg|acc}/{table}/{f}.parquet

    Snapshot (not log) semantics: the previous snapshot is pruned after the sidecar commit — exactly the
    latest per companion is kept (companions are one-row-per-group; a table that outgrows snapshotting has
    outgrown accumulate/aggregate). The snapshots live under ``state/`` so ``read_table`` / ``/api/data``
    globs never expose them (the reserved-prefix rule already hides the registry versions). Listed in the
    sidecar's ``state`` section as ``{"agg/{table}": {"f": …}, …}`` so a reader pairs the companion at a
    freshness with the main at the same freshness. Additive: no existing read path changes."""
    from . import trickle_io as trickle

    if f is None:  # a snapshot is stamped with the run freshness; nothing to key an f-named part on
        return
    data_dir = _as_storage(data_dir)
    f_iso = f.astimezone(_utc()).isoformat()
    written: list[tuple[str, str, str]] = []  # (kind, table, kept part name)
    state_section: dict[str, dict] = {}
    for base in trickle.read_meta(con):
        for prefix, kind in ((trickle.AGG_STATE_PREFIX, "agg"), (trickle.ACC_STATE_PREFIX, "acc")):
            reg = f"{prefix}{base}"
            if not trickle._table_exists(con, reg):
                continue
            if con.execute(f'SELECT count(*) FROM "{reg}"').fetchone()[0] == 0:
                continue
            store = data_dir.child("state", kind, base)
            store.mkdir()
            part = trickle.part_name(f)
            with store.copy_to(part) as uri:
                con.execute(f'COPY (SELECT * FROM "{reg}") TO \'{uri}\' (FORMAT PARQUET)')
            state_section[f"{kind}/{base}"] = {"f": f_iso}
            written.append((kind, base, part))
    if state_section:  # record the snapshots in the sidecar's `state` section (skipped by read/diff surfaces)
        sidecar = trickle.load_sidecar(data_dir)
        sidecar["state"] = state_section
        trickle.write_sidecar(data_dir, sidecar)
    for kind, base, keep in written:  # prune superseded snapshots — keep exactly the latest per companion
        store = data_dir.child("state", kind, base)
        for old in store.parquet_names():
            if old != keep:
                store.remove(old)


def _utc():
    from datetime import timezone

    return timezone.utc


def _published_bytes(data_dir, table: str) -> int:
    """Total published bytes backing ``table``'s current-state read — the wholesale file + per-run parts
    + warm bands + changelog parts + cold base chunks. A planner size hint (mirrors the Rust driver's
    ``published_bytes``)."""
    from . import trickle_io as trickle

    total = data_dir.size(f"{table}.parquet")
    for t in (table, trickle.changelog_name(table), trickle.warm_name(table)):
        for n in trickle.table_parts(data_dir, t):
            total += data_dir.size(t, n)
    base = trickle.base_dir_name(table)
    for n in trickle.base_chunks(data_dir, table):
        total += data_dir.size(base, n)
    return total


def _delta_rows_last(con, table: str, mode: str, f) -> int:
    """This run's delta size: the count of rows stamped at ``f`` in the table's delta structure — the
    merge **changelog**, the append **base**, or (overwrite) the whole rewrite."""
    from . import trickle_io as trickle

    if mode == "overwrite":
        return int(con.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0])
    delta_table = trickle.changelog_name(table) if mode == "merge" else table
    if f is None or not trickle._table_exists(con, delta_table):
        return 0
    return int(con.execute(
        f'SELECT count(*) FROM "{delta_table}" WHERE "{trickle.F_COL}" = {trickle._ts(f)}'
    ).fetchone()[0])


def _entry_schema(con, table: str) -> dict | None:
    """The user-column schema (name → DuckDB type) of a registry table, ``_duckstring_*`` system columns
    excluded — described from the base table, or its changelog when no base relation exists (a young
    merge main pre-checkpoint). ``None`` when neither exists."""
    from . import trickle_io as trickle
    from .trickle.context import SYSTEM_PREFIX

    src = table
    if not trickle._table_exists(con, src):
        src = trickle.changelog_name(table)
        if not trickle._table_exists(con, src):
            return None
    rel = con.sql(f'SELECT * FROM "{src}" LIMIT 0')
    return {c: str(t) for c, t in zip(rel.columns, rel.types, strict=True) if not c.startswith(SYSTEM_PREFIX)}


def _enrich_sidecar(con, data_dir, f) -> None:
    """Stamp each sidecar entry with *state-format Extension 2* planner hints (see the DuckFlock
    ``plans/state-format.md``, mirrored from the Rust driver's ``write_publish_sidecar``): per entry
    ``stats: {rows, bytes, delta_rows_last}``, a user-column ``schema`` map, and ``format: 2``.

    These are what let a routing/planning consumer (the ``duckflock quote`` client, the DuckFlock driver)
    **estimate without opening Parquet footers** — hints, never load-bearing (footers stay the source of
    truth; the conformance differ compares only the named ``mode/pk/floor/f/f_base`` fields, so the
    extension is additive on the wire too). **Best-effort:** a failure to compute a hint never breaks a
    publish — the entry just goes un-stamped."""
    from . import trickle_io as trickle

    data_dir = _as_storage(data_dir)
    sidecar = trickle.load_sidecar(data_dir)
    changed = False
    for table, entry in sidecar.items():
        if table in ("objects", "state") or not isinstance(entry, dict):
            continue
        mode = entry.get("mode", "overwrite")
        try:
            delta = _delta_rows_last(con, table, mode, f)
            if mode == "overwrite":
                rows = delta  # the whole rewrite is the current state
            else:
                rows = trickle.count_current(con, table) if mode == "merge" else int(
                    con.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0])
            entry["stats"] = {"rows": rows, "bytes": _published_bytes(data_dir, table), "delta_rows_last": delta}
            schema = _entry_schema(con, table)
            if schema is not None:
                entry["schema"] = schema
            entry["format"] = 2
            changed = True
        except Exception:  # a hint, not a publish invariant
            continue
    if changed:
        trickle.write_sidecar(data_dir, sidecar)


def hydrate_registry(con, data_dir, tables=None) -> list[str]:
    """Rebuild registry state **from the published layout** — the recovery inverse of :meth:`export`
    (mirrors the DuckFlock driver's ``hydrate_output``; full-collection, because export mirrors the
    registry back and a part left unhydrated would be pruned as retention-dropped).

    For each sidecar base entry (or just ``tables`` when given): the base/main (overwrite wholesale file;
    append parts; a merge main's cold base chunks), the ``__changelog`` / ``__band`` / ``__droplog``
    companion parts, the meta row (mode/pk/floor from the sidecar; ``f_base`` from the sidecar;
    ``f_warm`` from the newest published band's part name), and the *state-format Extension 1*
    agg/acc accumulator snapshots (``state/{agg|acc}/{table}/``). Tables are ``CREATE OR REPLACE``d,
    so hydration is idempotent and safe over a partially-present registry.

    Two callers: **Duck registry-loss recovery** (the registry *file* is gone — host loss, migration,
    scale-to-zero — but the published state survives; see ``RippleExecutor``) and the **DuckFlock
    routing path** (a remotely-executed ripple's outputs land in a scratch publish dir and are read
    back so the run's normal export + contract gate + downstream ripples see them). Returns the
    hydrated base-table names."""
    from . import trickle_io as trickle

    store = _as_storage(data_dir)
    store.duckdb_setup(con)  # configure object-store creds on this connection (a no-op for local)
    plane = ParquetDataPlane()  # hydration reads the flat layer (every backend also writes it)
    sidecar = trickle.load_sidecar(store)
    hydrated: list[str] = []
    for table, entry in sidecar.items():
        if table in ("objects", "state") or not isinstance(entry, dict):
            continue
        if tables is not None and table not in tables:
            continue
        mode = entry.get("mode", "overwrite")
        loaded = False
        if mode == "merge":
            # The registry main is the COLD BASE (a young merge main pre-checkpoint has none) — never the
            # reconstruction; reconstruct happens at read time over base ⊎ changelog. Register it as a VIEW over
            # the published S3 chunks, NOT a materialised copy: the base can be arbitrarily large (TB), a fresh
            # Duck must not copy it down to run a small delta, and a steady-state incremental run never reads it
            # (only a checkpoint / comprehensive fallback does, and they read on demand). See
            # plans/s3-resident-state.md. The changelog/warm below stay materialised for now (later steps).
            if trickle.base_chunks(store, table) or store.exists(f"{table}.parquet"):
                sql = plane._raw_read_select(store, table)
                trickle._drop_relation(con, table)
                con.execute(f'CREATE VIEW {trickle._q(table)} AS {sql}')
                loaded = True
        else:  # append parts dir, or the overwrite wholesale file
            if trickle.table_parts(store, table) or store.exists(f"{table}.parquet"):
                sql = plane._raw_read_select(store, table)
                con.execute(f'CREATE OR REPLACE TABLE {trickle._q(table)} AS {sql}')
                loaded = True
        for companion in (trickle.changelog_name(table), trickle.warm_name(table),
                          f"{table}{trickle.DROPLOG_SUFFIX}"):
            if trickle.table_parts(store, companion):
                sql = plane._raw_read_select(store, companion)
                con.execute(f'CREATE OR REPLACE TABLE {trickle._q(companion)} AS {sql}')
                loaded = True
        if mode in ("merge", "append") and loaded:
            from datetime import datetime

            trickle._record_meta(con, table, mode, tuple(entry.get("pk") or ()))
            if entry.get("floor"):
                trickle._advance_floor(con, table, bootstrap_f=datetime.fromisoformat(entry["floor"]))
            if entry.get("f_base"):
                trickle._set_f_base(con, table, datetime.fromisoformat(entry["f_base"]))
            band_fs = [trickle.part_f(n) for n in trickle.table_parts(store, trickle.warm_name(table))]
            f_warm = max(band_fs) if band_fs else (
                datetime.fromisoformat(entry["f_base"]) if entry.get("f_base") else None)
            if f_warm is not None:
                trickle._set_f_warm(con, table, f_warm)
        # Extension 1: the agg/acc accumulator snapshots (latest snapshot per companion).
        for kind, prefix in (("agg", trickle.AGG_STATE_PREFIX), ("acc", trickle.ACC_STATE_PREFIX)):
            snap_store = store.child("state", kind, table)
            snaps = snap_store.parquet_names()
            if snaps:
                uri = snap_store.uri(snaps[-1])
                con.execute(
                    f'CREATE OR REPLACE TABLE {trickle._q(prefix + table)} AS '
                    f"SELECT * FROM read_parquet('{uri}')"
                )
                loaded = True
        if loaded:
            hydrated.append(table)
    return hydrated


# The Iceberg catalog layer is LOCAL-ONLY under local-first publish: its metadata files embed absolute
# warehouse paths, so byte-copying them to another location yields pointers into the producer's filesystem.
# The persisted layer is the FLAT layout (parts + wholesale files + sidecar) — complete and canonical; every
# reader falls back to the flat read when no catalog.json is present (`IcebergDataPlane._load` → None).
_PERSIST_SKIP = frozenset({"catalog.json", "pond"})


def persist_tree(local_dir, dest) -> int:
    """Mirror a Pond's locally-published output to its durable **persist layer** (plans/persist.md) —
    the async Duck-side reconcile behind ``persisted_f``. Returns the number of files uploaded.

    Semantics per entry kind:

    - **top-level files** (the sidecar, wholesale ``{table}.parquet`` overwrite output) — always uploaded
      (rewritten per run; small, or the table's whole content by design);
    - **directory files** (append/changelog/band parts, base chunks, state snapshots) — immutable and
      idempotent **by name**: upload only what the destination lacks, and prune destination files their
      local directory no longer holds (retention trims, checkpoint token swaps, warm folds, snapshot
      pruning all propagate);
    - **directories removed locally** (a folded-away ``__band/``, a dropped table's parts) — removed at
      the destination;
    - the **Iceberg catalog** (``catalog.json`` + the ``pond/`` warehouse) — skipped: local-only (its
      metadata embeds absolute local paths; the flat layer is the canonical persisted form).

    **Safety guard**: a local dir with no ``_trickle.json`` sidecar has published nothing — the mirror
    refuses to touch the destination at all (a fresh/lost box must never wipe the durable layer).
    Replayable: a mirror that dies mid-way re-runs idempotently (parts by name, wholesale re-upload)."""
    import json
    from datetime import datetime

    from .trickle.io import base_table_name

    local = _as_storage(local_dir)
    root = local.root  # persist mirrors FROM a local publish dir by definition
    if not (root / "_trickle.json").exists():
        return 0  # nothing published locally — never touch the durable layer from an empty source
    try:
        sidecar = json.loads((root / "_trickle.json").read_text())
    except Exception:
        sidecar = {}

    def _drop_before(dirname: str):
        """The prune watermark for a directory of freshness-named files — **retention is an explicit
        signal, never inferred from absence** (plans/s3-resident-state.md step 1): a destination part is
        removed only when the LOCAL SIDECAR's floor (or, for base chunks, ``f_base``) says its freshness
        was genuinely dropped (retention / warm fold / checkpoint token supersession / re-bootstrap). A
        part merely absent locally is KEPT — absence is what a partial local (a future partially-hydrated
        box) looks like, and pruning on it would delete real history from the durable plane. ``None`` =
        no watermark → never prune this dir's files."""
        from .trickle.io import BASE_SUFFIX

        entry = sidecar.get(base_table_name(dirname))
        if not isinstance(entry, dict):
            return None
        from .trickle.io import CHANGELOG_SUFFIX

        if dirname.endswith(BASE_SUFFIX):
            # Base chunks: a checkpoint's token supersession — STRICTLY older tokens only. The CURRENT
            # token equals f_base, and pruning it when merely absent locally would delete the live base.
            iso = entry.get("f_base")
            return (datetime.fromisoformat(iso), False) if iso else None
        # Per-run parts: the floor. INCLUSIVE only for a merge __changelog (a warm fold moves the
        # boundary part's rows into the band — leaving it would double-count on plane reads); STRICT for
        # an append history/droplog (an at-floor part holds real rows a full read needs).
        iso = entry.get("floor")
        return (datetime.fromisoformat(iso), dirname.endswith(CHANGELOG_SUFFIX)) if iso else None

    copied = 0
    local_dirs: set[str] = set()
    files: list = []
    # Data first, the SIDECAR LAST: `_trickle.json` is the mirror's commit marker — a cross-Pool reader
    # trusts its freshness claims, so at every instant the mirrored data must be a superset of what the
    # mirrored sidecar declares. Uploading it before its parts would expose a torn window (sidecar at f,
    # parts for f still in flight → a delta read misses rows). Directories, then plain files, then it.
    for p in sorted(root.iterdir()):
        if p.name in _PERSIST_SKIP or p.name.endswith(".tmp"):
            continue
        if p.is_file():
            files.append(p)
        elif p.is_dir():
            local_dirs.add(p.name)
            copied += _mirror_dir(p, dest, (p.name,), drop_before=_drop_before(p.name))
    for p in sorted(files, key=lambda q: q.name == "_trickle.json"):  # sidecar sorts last
        dest.put_file(p, p.name)
        copied += 1
    # Prune destination directories only for tables the local sidecar no longer DECLARES (a table
    # dropped/unpublished — an explicit signal); a declared table's missing local dir is left alone.
    for name in dest.subdir_names():
        if name in local_dirs or name in _PERSIST_SKIP or name == "state":
            continue
        if base_table_name(name) not in sidecar:
            dest.rmtree(name)
    return copied


def _mirror_dir(src: Path, dest, parts: tuple[str, ...], drop_before=None) -> int:
    """Reconcile one directory of immutable, name-addressed files: upload the locally-present-but-missing;
    prune a destination file only when ``drop_before`` (the explicit retention/fold/checkpoint watermark)
    covers its parsed freshness — with one exception: the ``state/`` snapshot tree (``drop_before`` None
    at its root) keeps exact keep-latest semantics, pruning what local no longer holds (each snapshot dir
    is rewritten wholesale every run, so local is always complete there)."""
    from .trickle.io import part_f

    copied = 0
    existing = set(dest.names(*parts))
    local_files: set[str] = set()
    local_dirs: set[str] = set()
    state_tree = parts[0] == "state"
    for p in sorted(src.iterdir()):
        if p.name.endswith(".tmp"):
            continue
        if p.is_dir():
            local_dirs.add(p.name)
            copied += _mirror_dir(p, dest, parts + (p.name,), drop_before=drop_before)
            continue
        local_files.add(p.name)
        if p.name not in existing:
            dest.put_file(p, *parts, p.name)
            copied += 1
    for name in existing - local_files:
        if state_tree:
            dest.remove(*parts, name)  # keep-latest snapshots: local is authoritative + complete
        elif drop_before is not None:
            mark, inclusive = drop_before
            ff = _file_f(name, part_f)
            if ff is not None and (ff <= mark if inclusive else ff < mark):
                dest.remove(*parts, name)  # genuinely dropped — the watermark says so
    for name in set(dest.child(*parts).subdir_names()) - local_dirs:
        if state_tree:
            dest.rmtree(*parts, name)
    return copied


def _file_f(name: str, part_f) -> "datetime | None":  # noqa: F821 - forward ref for the docstring type
    """The freshness encoded in a published file name: a per-run part (``{f}.parquet``) or a base chunk
    (``{token}__{i}.parquet``, token = the checkpoint f). ``None`` if unparseable (never pruned)."""
    stem = name[: -len(".parquet")] if name.endswith(".parquet") else name
    if "__" in stem:
        stem = stem.rsplit("__", 1)[0]  # a base chunk's token
    try:
        return part_f(stem)
    except Exception:
        return None


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
        trickle.checkpoint(con, main, f)  # fold base+warm+hot≤f → clean base (a local table); clear warm
        if trickle._table_exists(con, main):
            _publish_base_chunks(con, data_dir, main, f, threshold)
            _review_base(con, data_dir, main)  # drop the local base; point the registry at the published S3 chunks
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


def _review_base(con, data_dir: Path, main: str) -> None:
    """After a checkpoint materialised the new base as a registry table and published it as S3 chunks, drop the
    local table and re-register ``main`` as a **view** over those chunks — so the (possibly huge) base is not
    held in the registry between checkpoints. Symmetric with :func:`hydrate_registry`'s base branch; a
    steady-state run never reads it, and a later checkpoint / comprehensive read hits S3 on demand. See
    plans/s3-resident-state.md."""
    from . import trickle_io as trickle

    data_dir = _as_storage(data_dir)
    if not trickle.base_chunks(data_dir, main):  # nothing published (shouldn't happen post-publish) → leave as-is
        return
    sql = ParquetDataPlane()._raw_read_select(data_dir, main)  # the base chunks glob (flat layer)
    trickle._drop_relation(con, main)
    con.execute(f'CREATE VIEW {trickle._q(main)} AS {sql}')


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
