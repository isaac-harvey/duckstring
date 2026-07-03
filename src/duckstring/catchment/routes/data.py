"""Read-only data access for Outlets, served from each Pond's **exported** Parquet snapshot
(``ponds/{pond}/data/{table}.parquet``) — the published, consistent output of a successful run — via
an in-memory DuckDB connection. It never opens the live ``registry.duckdb`` a Duck is writing to, so a
data query never contends with (or blocks) a running Pond."""

from __future__ import annotations

import io
import tempfile
import zipfile
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response
from pydantic import BaseModel

from .. import auth

router = APIRouter()


def _resolve_major(request: Request, pond_name: str, major: Optional[int], version: Optional[str]) -> int:
    """The major line whose exported data to read: explicit ``major``, the ``version``'s major, or
    the highest deployed major of the Pond (falling back to the highest ``m*`` dir on disk — exported
    data outlives a deployment). Data is per major line, not per version."""
    if major is not None:
        return major
    if version is not None:
        return int(version.split(".")[0])
    db = request.app.state.db
    row = db.execute(
        "SELECT MAX(p.major) FROM pond p JOIN pond_name pn ON pn.id = p.pond_name_id WHERE pn.name = ?",
        (pond_name,),
    ).fetchone()
    if row is not None and row[0] is not None:
        return row[0]
    on_disk = sorted(
        int(d.name[1:])
        for d in (Path(request.app.state.root) / "ponds" / pond_name).glob("m*")
        if d.is_dir() and d.name[1:].isdigit()
    )
    if on_disk:
        return on_disk[-1]
    raise HTTPException(status_code=404, detail=f"Pond '{pond_name}' not found")


def _data_dir(request: Request, pond_name: str, major: int):
    from ..registry import pond_data_dir

    data_root = getattr(request.app.state, "data_root", None)
    return pond_data_dir(Path(request.app.state.root), pond_name, major, data_root)


def _open_pond(request: Request, pond_name: str, major: int):
    """An in-memory DuckDB connection with the Pond's exported tables registered as views — under a
    schema named after the Pond, and in ``main`` — so queries can name them ``"pond"."table"`` or
    bare. Reads the Parquet snapshot, not the live registry, so there is no cross-process lock."""
    import duckdb

    from ...dataplane import get_data_plane

    dp = get_data_plane()
    data_dir = _data_dir(request, pond_name, major)
    con = duckdb.connect()  # in-memory: no file, no lock, no contention
    con.execute("SET TimeZone='UTC'")  # Trickle freshness is UTC; read/compare/render consistently
    dp.prepare(con)  # ready the connection to read the published format (e.g. load the iceberg ext)
    data_dir.duckdb_setup(con)  # object store → httpfs + credentials (no-op for local)
    con.execute(f'CREATE SCHEMA IF NOT EXISTS "{pond_name}"')
    for table in dp.list_tables(data_dir):
        select = dp.read_select(data_dir, table)
        con.execute(f'CREATE VIEW "{pond_name}"."{table}" AS {select}')
        con.execute(f'CREATE OR REPLACE VIEW "{table}" AS {select}')
    return con


@router.get("/ponds/{name}/versions/{version}", dependencies=[auth.read])
def get_pond_version(name: str, version: str, request: Request):
    db = request.app.state.db
    row = db.execute(
        """SELECT pv.id FROM pond_version pv
           JOIN pond_name pn ON pn.id = pv.pond_name_id
           WHERE pn.name = ? AND pv.version = ?""",
        (name, version),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"No version {version} of pond '{name}'")
    # "active" = this version is the one the pond pointer currently selects.
    selected = db.execute(
        "SELECT 1 FROM pond WHERE pond_version_id = ?", (row[0],)
    ).fetchone()
    return {"name": name, "version": version, "is_active": bool(selected)}


class QueryRequest(BaseModel):
    pond: str
    major: Optional[int] = None
    version: Optional[str] = None
    ripple: Optional[str] = None
    sql: Optional[str] = None
    format: Optional[str] = None


@router.get("/ponds/{outlet}/ripples/{ripple_name}", dependencies=[auth.read])
def get_ripple(
    outlet: str, ripple_name: str, request: Request,
    major: Optional[int] = None, version: Optional[str] = None,
):
    # Serve the published file directly — no DuckDB needed. A wholesale table is one file; an append-only
    # Trickle table is a directory of per-run parts, zipped under "{ripple_name}/".
    from ...dataplane import get_data_plane

    m = _resolve_major(request, outlet, major, version)
    data_dir = _data_dir(request, outlet, m)
    entries = get_data_plane().files_for(data_dir, ripple_name)
    if not entries:
        raise HTTPException(status_code=404, detail=f"No data for {outlet}.{ripple_name} (major {m})")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for storage_parts, arcname in entries:
            zf.writestr(arcname, data_dir.read_bytes(*storage_parts))
    return Response(content=buf.getvalue(), media_type="application/zip")


def _maybe_tap_on_get(request: Request, pond: str, major: Optional[int], version: Optional[str]) -> None:
    """If the Pond is open with tap-on-get, fire a Tap (the snapshot is served first — we never block
    on freshness). Best-effort: a data query must never fail because of this."""
    driver = getattr(request.app.state, "driver", None)
    if driver is None:
        return
    try:
        key = driver.resolve(pond, major, version)
        if driver.pond_tap_on_get(key):
            driver.tap(key)
    except Exception:
        pass


# ─── Trickle-aware browsing helpers ──────────────────────────────────────────

_F = "_duckstring_f"  # Trickle freshness stamp
_D = "_duckstring_d"  # Z-set weight (+1 present / -1 retraction) on a merge changelog


def _qi(ident: str) -> str:
    """Quote a SQL identifier."""
    return '"' + ident.replace('"', '""') + '"'


def _ts_lit(iso: str) -> str:
    """A validated TIMESTAMPTZ literal from a client-supplied freshness/datetime (guards injection)."""
    from datetime import datetime

    return f"TIMESTAMPTZ '{datetime.fromisoformat(iso).isoformat()}'"


def _sidecar(request: Request, name: str, major: int) -> dict:
    from ...trickle_io import load_sidecar

    return load_sidecar(_data_dir(request, name, major))


def _trickle_base_sql(
    pond: str, table: str, mode: str, pk: list, f_lo, f_hi, *, f_base=None, main_ref=None, clog_ref=None
) -> str:
    """The browse query for a Trickle, windowed to ``[f_lo, f_hi]`` (inclusive, either bound optional):

    - **append** — the history table itself, filtered by ``_duckstring_f``.
    - **merge** — the reconstructed *main* (current state), which already carries each row's last-write
      ``_duckstring_f``, joined to its consolidated changelog only for the **update count** and to surface
      the records the changelog retired. Each row carries ``_duckstring_f``, ``_duckstring_active`` (``+1``
      present / ``-1`` deleted — its last image is shown), and ``_duckstring_updates`` (count of ``+1``
      changelog events). With a window set, only records changed inside it are shown (inner join); with
      none, every current row is (left join).
      The result is emitted **unordered**, and the pager (:func:`query_page`) leaves it that way by default so
      the ``LIMIT/OFFSET`` pushes down to the Parquet scan — a page reads only its own rows (row groups skipped
      by count) and the changelog join touches only those, instead of scanning the whole base. A column sort is
      **opt-in** (the user clicking a header): only then does the pager add an ``ORDER BY`` and pay the
      full-scan Top-N. This matches the append view, which is likewise scan-ordered.

    ``main_ref``/``clog_ref`` override the source identifiers for the main / changelog. The merge main is
    **log-structured** (base + changelog, reconstructed on read) and **expensive to scan** at scale, so this
    query references it **exactly once** (``_active``): the current rows come straight from the reconstructed
    main, while the deleted records and the update counts are derived from the changelog alone. A record is
    *deleted* iff its latest changelog event (by freshness, retractions last) is a retraction ``d < 0``; the
    reconstruct has already dropped exactly those PKs from the main, so surfacing them needs no second pass.

    ``f_base`` is the cold-base watermark: the changelog ``<= f_base`` is already folded into the base, so the
    changelog read is filtered to ``> f_base`` (mirroring the reconstruct). Without this a large, *checkpointed*
    main re-scans its whole **retained** changelog history (still on disk inside the retention window) for the
    update count / delete detection, when only the post-base slice matters — so the counts and delete tombstones
    are *since the last checkpoint*. ``None`` (not-yet-checkpointed) reads the changelog in full (it is the main).
    """
    from ...trickle_io import changelog_name

    sch = _qi(pond)
    fcol, dcol = _qi(_F), _qi(_D)
    conds = []
    if f_lo:
        conds.append(f"{fcol} >= {_ts_lit(f_lo)}")
    if f_hi:
        conds.append(f"{fcol} <= {_ts_lit(f_hi)}")
    where = (" WHERE " + " AND ".join(conds)) if conds else ""
    windowed = bool(conds)  # a *user* freshness window (drives inner vs left join) — f_base is not one
    if mode == "append":
        return f"SELECT * FROM {main_ref or f'{sch}.{_qi(table)}'}{where}"
    main = main_ref or f"{sch}.{_qi(table)}"  # reconstructed merge main — already carries _duckstring_f per row
    clog = clog_ref or f"{sch}.{_qi(changelog_name(table))}"
    # Read only the changelog above the cold base — the rest is retained history already folded into it.
    clw_conds = list(conds)
    if f_base:
        clw_conds.append(f"{fcol} > {_ts_lit(f_base)}")
    clw_where = (" WHERE " + " AND ".join(clw_conds)) if clw_conds else ""
    pkq = [_qi(c) for c in pk]
    part = ", ".join(pkq)
    join = "JOIN" if windowed else "LEFT JOIN"  # windowed → only records changed in the window
    on_ma = " AND ".join(f"_m.{c} IS NOT DISTINCT FROM _a.{c}" for c in pkq)
    on_da = " AND ".join(f"_dl.{c} IS NOT DISTINCT FROM _a.{c}" for c in pkq)
    return (
        f"WITH _clw AS (SELECT * FROM {clog}{clw_where}), "
        f"_a AS (SELECT {part}, max({fcol}) AS _ds_fmax, "
        f"sum(CASE WHEN {dcol} > 0 THEN 1 ELSE 0 END) AS _ds_upd FROM _clw GROUP BY {part}), "
        f"_dl AS (SELECT *, row_number() OVER (PARTITION BY {part} "
        f"ORDER BY {fcol} DESC, {dcol} DESC) AS _ds_rn FROM _clw), "
        # The main carries _duckstring_f; the changelog join only supplies the update count now.
        f"_active AS (SELECT _m.*, 1 AS \"_duckstring_active\", "
        f"coalesce(_a._ds_upd, 0) AS \"_duckstring_updates\" "
        f"FROM {main} _m {join} _a ON {on_ma}), "
        f"_deleted AS (SELECT _dl.* EXCLUDE ({dcol}, {fcol}, _ds_rn), _a._ds_fmax AS {fcol}, "
        f"-1 AS \"_duckstring_active\", _a._ds_upd AS \"_duckstring_updates\" "
        f"FROM _dl JOIN _a ON {on_da} WHERE _dl._ds_rn = 1 AND _dl.{dcol} < 0) "
        f"SELECT * FROM _active UNION ALL BY NAME SELECT * FROM _deleted"
    )


@router.get("/ponds/{name}/tables", dependencies=[auth.read])
def list_pond_tables(
    name: str, request: Request, major: Optional[int] = None, version: Optional[str] = None,
):
    """The tables this Pond's selected major line has published — the data viewer's table picker. Each
    carries its Trickle ``mode`` (``append``/``merge``, else ``None``) and ``pk`` from the sidecar, so the
    viewer can offer the freshness window + consolidated view. A merge changelog (``X__changelog``) is a
    plain table here (not in the sidecar) — it stays raw-navigable, unchanged."""
    from ...dataplane import get_data_plane

    m = _resolve_major(request, name, major, version)
    try:
        tables = get_data_plane().list_tables(_data_dir(request, name, m))
    except Exception:
        tables = []
    sidecar = _sidecar(request, name, m)
    out = []
    for t in tables:
        meta = sidecar.get(t) or {}
        mode = meta.get("mode")
        out.append({
            "name": t,
            "trickle": mode if mode in ("append", "merge") else None,
            "pk": list(meta.get("pk") or []),
        })
    return {"tables": out}


@router.get("/ponds/{name}/objects", dependencies=[auth.read])
def list_pond_objects(
    name: str, request: Request, major: Optional[int] = None, version: Optional[str] = None,
):
    """The non-tabular Objects this Pond's selected major line has published (name, byte size, last
    freshness, file-vs-directory) — the Data Viewer's Objects list. See ``plans/objects.md``."""
    from ...objects import list_objects

    m = _resolve_major(request, name, major, version)
    try:
        objs = list_objects(_data_dir(request, name, m))
    except Exception:
        objs = {}
    out = [
        {"name": n, "size": e.get("size"), "f": e.get("f"), "is_dir": bool(e.get("is_dir")), "ext": e.get("ext", "")}
        for n, e in sorted(objs.items())
    ]
    return {"objects": out}


@router.get("/ponds/{name}/objects/{obj}", dependencies=[auth.read])
def get_pond_object(
    name: str, obj: str, request: Request, major: Optional[int] = None, version: Optional[str] = None,
):
    """Download one Object — a single file inline, a directory Object zipped."""
    from ...objects import OBJECTS_DIR, list_objects, object_path

    m = _resolve_major(request, name, major, version)
    data_dir = _data_dir(request, name, m)
    entry = list_objects(data_dir).get(obj)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"No object '{obj}' for {name} (major {m})")
    if entry["is_dir"]:
        scratch = Path(tempfile.mkdtemp(prefix="duckstring-obj-dl-"))
        try:
            local = object_path(data_dir, obj, scratch)  # real path (local) or a download (object store)
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
                for f in sorted(local.rglob("*")):
                    if f.is_file():
                        zf.writestr(str(f.relative_to(local)), f.read_bytes())
            body = buf.getvalue()
        finally:
            import shutil

            shutil.rmtree(scratch, ignore_errors=True)
        return Response(
            content=body, media_type="application/zip",
            headers={"Content-Disposition": f'attachment; filename="{obj}.zip"'},
        )
    data = data_dir.read_bytes(OBJECTS_DIR, obj, f"data{entry['ext']}")
    return Response(
        content=data, media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{obj}{entry["ext"]}"'},
    )


@router.delete("/ponds/{name}/objects/{obj}", dependencies=[auth.full])
def delete_pond_object(
    name: str, obj: str, request: Request, major: Optional[int] = None, version: Optional[str] = None,
):
    """Delete one non-tabular Object from a Pond — its payload + sidecar entry (no registry, no run).
    Gated on the Pond being idle so it can't race a run's commit_objects re-adding the entry (409)."""
    from ...objects import delete_object

    m = _resolve_major(request, name, major, version)
    driver = getattr(request.app.state, "driver", None)
    if driver is not None:
        try:
            key = driver.resolve(name, major, version)
            if driver.is_pond_running(key):
                raise HTTPException(status_code=409, detail="the Pond is running — delete an Object when it is idle")
        except HTTPException:
            raise
        except Exception:
            pass  # not a live engine Pond (exported data outlives a deployment) — allow the delete
    delete_object(_data_dir(request, name, m), obj)
    return {"ok": True}


@router.get("/ponds/{name}/freshness", dependencies=[auth.read])
def pond_freshness(
    name: str, request: Request, table: str,
    major: Optional[int] = None, version: Optional[str] = None,
):
    """The distinct run freshnesses (newest first, ≤100) of a Trickle table — the window selector's
    options. Read from the append history or, for a merge, its changelog. ``floor`` is the coverage
    watermark below which history isn't retained."""
    from ...trickle_io import changelog_name

    m = _resolve_major(request, name, major, version)
    meta = _sidecar(request, name, m).get(table) or {}
    mode = meta.get("mode")
    if mode not in ("append", "merge"):
        return {"freshness": [], "floor": None}
    src = table if mode == "append" else changelog_name(table)
    con = _open_pond(request, name, m)
    fcol = _qi(_F)
    try:
        rows = con.execute(
            f"SELECT DISTINCT {fcol} AS f FROM {_qi(name)}.{_qi(src)} "
            f"WHERE {fcol} IS NOT NULL ORDER BY f DESC LIMIT 100"
        ).fetchall()
        fr = [r[0].isoformat() if hasattr(r[0], "isoformat") else str(r[0]) for r in rows]
        return {"freshness": fr, "floor": meta.get("floor")}
    except Exception:
        return {"freshness": [], "floor": meta.get("floor")}
    finally:
        con.close()


class CountRequest(BaseModel):
    pond: str
    major: Optional[int] = None
    version: Optional[str] = None
    table: Optional[str] = None
    sql: Optional[str] = None
    # Trickle browse: build the (windowed, consolidated) base query server-side instead of `sql`/`table`.
    trickle: Optional[str] = None  # 'append' | 'merge'
    pk: Optional[list] = None
    f_lo: Optional[str] = None  # inclusive lower freshness bound (ISO), or None = unbounded
    f_hi: Optional[str] = None  # inclusive upper freshness bound (ISO), or None = unbounded


def _merge_f_base(request: Request, body, major: int):
    """The cold-base watermark ``f_base`` of a merge Trickle main — the freshness up to which the changelog
    is already folded into the clean base. The browse only needs changelog rows **newer** than this; the
    retained history ``<= f_base`` is redundant (it lives in the base) and re-reading it is what made a large,
    *checkpointed* table's browse scan its whole retained log. ``None`` for non-merge or a not-yet-checkpointed
    main (then the changelog is the whole main and is read in full)."""
    if body.trickle != "merge":
        return None
    return (_sidecar(request, body.pond, major).get(body.table) or {}).get("f_base")


def _base_sql(body, *, f_base=None) -> str:
    """The query a count/page operates on: a Trickle's windowed/consolidated view (its changelog read clamped
    to ``> f_base``), a custom ``sql``, or a plain ``SELECT *`` of a table. The reconstructed merge main is
    referenced as a view — never materialised — so the pager's Top-N streams it instead of spilling a
    gigabyte-scale copy to temp."""
    if body.trickle in ("append", "merge"):
        return _trickle_base_sql(
            body.pond, body.table, body.trickle, body.pk or [], body.f_lo, body.f_hi, f_base=f_base
        )
    if body.sql:
        return body.sql
    if body.table:
        return f"SELECT * FROM {_qi(body.pond)}.{_qi(body.table)}"
    return f"SELECT * FROM {_qi(body.pond)}"


def _merge_deleted_count_sql(pond: str, table: str, pk: list, f_base) -> str:
    """Count the deleted tombstones the browse shows — distinct PKs in the hot changelog above ``f_base`` whose
    latest event (freshness desc, retractions last) is a retraction ``d < 0`` — matching :func:`_trickle_base_sql`'s
    ``_deleted``. A small scan of the post-base changelog (not the base)."""
    from ...trickle_io import changelog_name

    clog = f"{_qi(pond)}.{_qi(changelog_name(table))}"
    fcol, dcol = _qi(_F), _qi(_D)
    part = ", ".join(_qi(c) for c in pk)
    where = f" WHERE {fcol} > {_ts_lit(f_base)}" if f_base else ""
    return (
        f"SELECT count(*) FROM (SELECT {dcol}, row_number() OVER (PARTITION BY {part} "
        f"ORDER BY {fcol} DESC, {dcol} DESC) AS _ds_rn FROM {clog}{where}) WHERE _ds_rn = 1 AND {dcol} < 0"
    )


@router.post("/query/count", dependencies=[auth.read])
def query_count(body: CountRequest, request: Request):
    """Total rows of the (default, custom, or Trickle) query — sizes the data viewer's virtual scroll. A
    bare ``COUNT(*)`` over a Parquet table is metadata-fast (no scan).

    The **unwindowed current state of a merge** main is counted *without scanning the base*: the active count is
    ``count(cold base)`` (metadata) + the changelog's net Z-set weight (:meth:`DataPlane.consolidated_count_select`),
    plus the small deleted-tombstone count — so the scrollbar sizes near-instantly even at hundreds of millions of
    rows. A *windowed* merge count falls through to the (already small) windowed changelog query."""
    m = _resolve_major(request, body.pond, body.major, body.version)
    con = _open_pond(request, body.pond, m)
    try:
        if body.trickle == "merge" and body.pk and not (body.f_lo or body.f_hi):
            from ...dataplane import get_data_plane

            meta = _sidecar(request, body.pond, m).get(body.table) or {}
            active_sql = get_data_plane().consolidated_count_select(_data_dir(request, body.pond, m), body.table, meta)
            active = con.execute(active_sql).fetchone()[0]
            deleted = con.execute(_merge_deleted_count_sql(body.pond, body.table, body.pk, meta.get("f_base"))).fetchone()[0]
            return {"count": int(active) + int(deleted)}
        base = _base_sql(body, f_base=_merge_f_base(request, body, m))
        (count,) = con.execute(f"SELECT COUNT(*) FROM ({base}) AS _ds_count").fetchone()
        return {"count": count}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        con.close()


class PageRequest(BaseModel):
    pond: str
    major: Optional[int] = None
    version: Optional[str] = None
    table: Optional[str] = None  # the table to browse (default query) — ignored when `sql` is set
    sql: Optional[str] = None  # a custom query; overrides the default `SELECT * FROM table`
    trickle: Optional[str] = None  # 'append' | 'merge' — server-built windowed/consolidated base query
    pk: Optional[list] = None
    f_lo: Optional[str] = None
    f_hi: Optional[str] = None
    order_by: Optional[str] = None  # opt-in sort column (default = the base order: PK / scan, no sort cost)
    order_desc: bool = False
    limit: int = 200
    offset: int = 0


def _json_safe(v):
    """Coerce a DuckDB cell to something JSON can carry: primitives pass through, everything else
    (datetimes, Decimals, blobs, lists, structs) is stringified for display in the grid."""
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    return str(v)


@router.post("/query/page", dependencies=[auth.read])
def query_page(body: PageRequest, request: Request):
    """A paged read for the data viewer: runs the (default or custom) query as a subquery with
    ``LIMIT/OFFSET`` and returns ordered columns + row arrays + a ``has_more`` flag. Wrapping means a
    user's own ``LIMIT`` still caps the result while the grid pages within it. One row beyond the page
    is fetched to detect ``has_more`` without a separate count."""
    _maybe_tap_on_get(request, body.pond, body.major, body.version)
    m = _resolve_major(request, body.pond, body.major, body.version)
    con = _open_pond(request, body.pond, m)
    limit = max(1, min(body.limit, 5000))
    offset = max(0, body.offset)
    base = _base_sql(body, f_base=_merge_f_base(request, body, m))
    # Order (if asked) + page at this single level. The default is the **cheap scan order**: with no ORDER BY
    # the LIMIT/OFFSET pushes down to the Parquet scan (row groups skipped by count), so a page reads only its
    # ~CHUNK rows and the changelog join touches only those — O(page), not O(table). An explicit column sort
    # (the user clicking a header — quoted → injection-safe; unknown column → 400) pays the full-scan Top-N it
    # genuinely needs. So a merge Trickle no longer force-sorts its (gigabyte-scale) base by PK on every page.
    order = f" ORDER BY {_qi(body.order_by)} {'DESC' if body.order_desc else 'ASC'}" if body.order_by else ""
    try:
        rel = con.execute(f"SELECT * FROM ({base}) AS _ds_page{order} LIMIT {limit + 1} OFFSET {offset}")
        cols = [d[0] for d in rel.description] if rel.description else []
        fetched = rel.fetchall()
        has_more = len(fetched) > limit
        rows = [[_json_safe(c) for c in row] for row in fetched[:limit]]
        return {"columns": cols, "rows": rows, "has_more": has_more}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        con.close()


class HistoryRequest(BaseModel):
    pond: str
    major: Optional[int] = None
    version: Optional[str] = None
    table: str
    pk: dict  # {pk_column: value} identifying the record


@router.post("/query/history", dependencies=[auth.read])
def query_history(body: HistoryRequest, request: Request):
    """The changelog history of one record (merge Trickle), **newest freshness first**, one row per run
    it changed in, labelled with a ``_duckstring_event``: ``create`` (a ``+1`` only), ``update`` (a
    ``-1`` of the old image and a ``+1`` of the new in the same run), or ``delete`` (a ``-1`` only). The
    representative image is the surviving ``+1`` (create/update) or the retracted ``-1`` (delete).

    When the oldest recorded run is an *update*, its ``-1`` holds the **original** (pre-changelog /
    bootstrap) image — otherwise unseen — so it's surfaced as a synthetic ``create`` at the ``floor``
    freshness, sorted to the bottom. (A create-first or delete-only record already shows its original.)
    PK values are bound as parameters (never interpolated)."""
    from ...trickle_io import changelog_name

    m = _resolve_major(request, body.pond, body.major, body.version)
    con = _open_pond(request, body.pond, m)
    clog = f"{_qi(body.pond)}.{_qi(changelog_name(body.table))}"
    fcol, dcol = _qi(_F), _qi(_D)
    floor = (_sidecar(request, body.pond, m).get(body.table) or {}).get("floor")
    floor_lit = _ts_lit(floor) if floor else "NULL"
    conds, params = [], []
    for col, val in body.pk.items():
        conds.append(f"{_qi(col)} IS NOT DISTINCT FROM ?")
        params.append(val)
    where = (" WHERE " + " AND ".join(conds)) if conds else ""
    # Materialise the record's changelog rows into a temp table first. The parameterised scan over the
    # (iceberg_scan-backed) view serialises fine on its own, but the collapse below references those rows
    # several times (GROUP BY + windowed self-join + the original-image lookup), producing iceberg_scans
    # under joins — which can't be serialised inside the resulting prepared plan ("IcebergScan
    # serialization not implemented"). Running the analytic step over the local temp sidesteps it.
    analytic = (
        f"WITH _g AS (SELECT {fcol} AS _ds_f, bool_or({dcol} > 0) AS _ds_pos, bool_or({dcol} < 0) AS _ds_neg "
        f"FROM _ds_hist GROUP BY {fcol}), "
        f"_r AS (SELECT *, row_number() OVER (PARTITION BY {fcol} ORDER BY {dcol} DESC) AS _ds_rn FROM _ds_hist), "
        f"_events AS (SELECT _r.* EXCLUDE ({dcol}, _ds_rn), "
        f"CASE WHEN _g._ds_pos AND _g._ds_neg THEN 'update' "
        f"WHEN _g._ds_pos THEN 'create' ELSE 'delete' END AS \"_duckstring_event\" "
        f"FROM _r JOIN _g ON _r.{fcol} = _g._ds_f WHERE _r._ds_rn = 1), "
        # The original image: the retraction at the oldest run, but only when that run is an update
        # (it also has a +1) — re-stamped as a create at the floor freshness.
        f"_orig AS (SELECT _h.* EXCLUDE ({dcol}, {fcol}), {floor_lit} AS {fcol}, 'create' AS \"_duckstring_event\" "
        f"FROM _ds_hist _h WHERE _h.{fcol} = (SELECT min({fcol}) FROM _ds_hist) AND _h.{dcol} < 0 "
        f"AND EXISTS (SELECT 1 FROM _ds_hist _p WHERE _p.{fcol} = (SELECT min({fcol}) FROM _ds_hist) "
        f"AND _p.{dcol} > 0) LIMIT 1) "
        f"SELECT * FROM (SELECT * FROM _events UNION ALL BY NAME SELECT * FROM _orig) "
        f"ORDER BY {fcol} DESC NULLS LAST LIMIT 2000"
    )
    try:
        con.execute(f"CREATE OR REPLACE TEMP TABLE _ds_hist AS SELECT * FROM {clog}{where}", params)
        rel = con.execute(analytic)
        cols = [d[0] for d in rel.description] if rel.description else []
        rows = [[_json_safe(c) for c in row] for row in rel.fetchall()]
        return {"columns": cols, "rows": rows}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        con.close()


@router.post("/query", dependencies=[auth.read])
def query(body: QueryRequest, request: Request):
    _maybe_tap_on_get(request, body.pond, body.major, body.version)
    con = _open_pond(request, body.pond, _resolve_major(request, body.pond, body.major, body.version))
    sql = body.sql
    if not sql:
        if body.ripple:
            sql = f'SELECT * FROM "{body.pond}"."{body.ripple}" LIMIT 10'
        else:
            sql = f'SELECT * FROM "{body.pond}" LIMIT 10'

    fmt = (body.format or "").lower()
    try:
        if fmt:
            suffix_map = {"csv": ".csv", "json": ".json", "parquet": ".parquet"}
            media_map = {"csv": "text/csv", "json": "application/json", "parquet": "application/octet-stream"}
            with tempfile.NamedTemporaryFile(suffix=suffix_map.get(fmt, ".bin"), delete=False) as f:
                tmp_path = f.name
            try:
                con.execute(f"COPY ({sql}) TO '{tmp_path}' (FORMAT {fmt.upper()})")
                data = Path(tmp_path).read_bytes()
            finally:
                Path(tmp_path).unlink(missing_ok=True)
            return Response(content=data, media_type=media_map.get(fmt, "application/octet-stream"))

        rel = con.execute(sql)
        if rel.description is None:
            return []
        cols = [d[0] for d in rel.description]
        return [dict(zip(cols, row, strict=False)) for row in rel.fetchall()]
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        con.close()
