"""The draw route — the cross-Catchment data-transfer channel.

A downstream Catchment's poller fetches a producing Pond's full exported Parquet over this route and
lands it in its own landing zone (`ponds/{name}/m{major}/data/`). It streams the **raw** Parquet
files (a zip), unlike `/api/query` which runs SQL. Reads off the exported snapshot, never the live
registry, so it never contends with a running Duck. tap-on-get lives on `/api/query`, not here —
so a Catchment draw never triggers it.
"""

from __future__ import annotations

import asyncio
import io
import zipfile
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response

from .. import auth
from .data import _data_dir, _resolve_major

router = APIRouter()

_WAIT_TICK = 0.1  # how often the long-poll re-checks the Pond's freshness


@router.get("/draw/{name}/{major}/wait", dependencies=[auth.read])
async def draw_wait(
    name: str, major: int, request: Request,
    after: Optional[str] = None, down: bool = False, timeout: float = 20.0,
):
    """Long-poll: block until this Pond line's freshness advances past ``after``, or its down-state
    *changes* from ``down`` (the consumer passes the state it already knows), or ``timeout``, then
    return ``{end_f, down}``. A downstream Catchment's poller holds this so a Draw transfers the
    instant the upstream is fresh — no poll-interval latency. Returning on a down **transition** (not
    a persistent down) is what stops the poller spinning when an upstream is durably blocked. Dial-back
    preserved: the consumer holds the connection, the producer never calls back."""
    driver = getattr(request.app.state, "driver", None)
    if driver is None:
        raise HTTPException(status_code=503, detail="driver not ready")
    try:
        key = driver.resolve(name, major, None)
    except (KeyError, ValueError) as exc:
        raise HTTPException(status_code=404, detail=str(exc.args[0] if exc.args else exc)) from exc

    after_dt = datetime.fromisoformat(after) if after else None
    for _ in range(max(1, int(timeout / _WAIT_TICK))):
        obs = driver.pond_observation(key)
        end_f = obs["end_f"]
        advanced = end_f is not None and (after_dt is None or datetime.fromisoformat(end_f) > after_dt)
        if advanced or obs["down"] != down:
            return obs
        await asyncio.sleep(_WAIT_TICK)
    return driver.pond_observation(key)


@router.get("/draw/{name}/{major}", dependencies=[auth.read])
def draw(name: str, major: int, request: Request, tables: Optional[str] = None, after: Optional[str] = None,
         base_after: Optional[str] = None, objects_after: Optional[str] = None):
    """Stream a Pond line's exported Parquet as a zip. ``tables`` (comma-separated) optionally restricts
    the set — reserved for per-Ripple duct scope; default is every table.

    ``after`` (a consumer's already-landed ``_duckstring_f``) makes a Trickle transfer **incremental**:
    an append-only table (append history, ``__changelog``, ``__band`` warm bands, ``__droplog``) is a
    directory of per-run parts, and only the parts newer than ``after`` are shipped (the small delta); a
    plain Ripple output is a single file, always wholesale. A merge main's **cold base** (``__base/`` chunks)
    is wholesale but rewritten only at a rare cold compaction, so it ships only when its fold watermark
    ``f_base`` advanced past ``base_after`` (the consumer's held cold-base freshness) — otherwise the large
    base is not re-sent. Omit ``after``/``base_after`` (bootstrap) → the whole set."""
    from datetime import datetime

    from ...trickle_io import BASE_SUFFIX, SIDECAR, base_chunks, base_dir_name, load_sidecar, part_f, part_tables, table_parts

    m = _resolve_major(request, name, major, None)
    data_dir = _data_dir(request, name, m)
    wanted = {t.strip() for t in tables.split(",")} if tables else None
    after_dt = datetime.fromisoformat(after) if after else None
    base_after_dt = datetime.fromisoformat(base_after) if base_after else None

    files = sorted(n for n in data_dir.parquet_names()
                   if wanted is None or n[: -len(".parquet")] in wanted)  # wholesale single-file tables
    dirs = [t for t in part_tables(data_dir) if wanted is None or t in wanted]
    base_dirs = sorted(
        d[: -len(BASE_SUFFIX)] for d in data_dir.subdir_names()
        if d.endswith(BASE_SUFFIX) and (wanted is None or d[: -len(BASE_SUFFIX)] in wanted)
    )
    if not files and not dirs and not base_dirs and not data_dir.exists():
        raise HTTPException(status_code=404, detail=f"No exported data for '{name}' (major {m})")
    sidecar_meta = load_sidecar(data_dir)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for n in files:  # wholesale single-file tables (legacy merge base / plain output)
            zf.writestr(n, data_dir.read_bytes(n))
        for main in base_dirs:  # cold base chunks — wholesale, but only if f_base advanced past the consumer's
            fb = sidecar_meta.get(main, {}).get("f_base")
            if base_after_dt is not None and fb is not None and datetime.fromisoformat(fb) <= base_after_dt:
                continue  # the consumer already holds this cold base — don't re-ship it
            base = data_dir.child(base_dir_name(main))
            for chunk in base_chunks(data_dir, main):
                zf.writestr(f"{main}{BASE_SUFFIX}/{chunk}", base.read_bytes(chunk))
        for table in dirs:  # append-only parts → ship only the parts newer than `after`
            part_store = data_dir.child(table)
            for part in table_parts(data_dir, table):
                if after_dt is None or part_f(part) > after_dt:
                    zf.writestr(f"{table}/{part}", part_store.read_bytes(part))
        # Non-tabular Objects — overwrite-only, so ship each object subtree whose freshness advanced past
        # the consumer's held watermark (`objects_after`, the min f it holds). Bootstrap ships all.
        _ship_objects(zf, data_dir, sidecar_meta.get("objects") or {}, objects_after)
        # The Trickle mode/PK sidecar travels with the data so the consuming Catchment's read_delta can
        # resolve a Trickle source (mode/PK aren't in the downstream's duck.db). Harmless for plain Ponds.
        sidecar = data_dir.read_text(SIDECAR)
        if sidecar is not None:
            zf.writestr(SIDECAR, sidecar)
    return Response(content=buf.getvalue(), media_type="application/zip")


def _ship_objects(zf, data_dir, obj_entries: dict, objects_after: Optional[str]) -> None:
    """Zip each published Object's payload under ``objects/{name}/…`` — those newer than ``objects_after``.
    Materialises via ``object_path`` (a no-copy local read, or a download for an object store)."""
    import shutil
    import tempfile
    from pathlib import Path

    from ...objects import OBJECTS_DIR, object_path

    after_dt = datetime.fromisoformat(objects_after) if objects_after else None
    scratch = Path(tempfile.mkdtemp(prefix="duckstring-obj-draw-"))
    try:
        for oname, entry in obj_entries.items():
            of = entry.get("f")
            if after_dt is not None and of is not None and datetime.fromisoformat(of) <= after_dt:
                continue  # the consumer already holds this Object
            local = object_path(data_dir, oname, scratch)
            if entry.get("is_dir"):
                for f in sorted(local.rglob("*")):
                    if f.is_file():
                        zf.writestr(f"{OBJECTS_DIR}/{oname}/{f.relative_to(local).as_posix()}", f.read_bytes())
            else:
                zf.writestr(f"{OBJECTS_DIR}/{oname}/{local.name}", local.read_bytes())
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
