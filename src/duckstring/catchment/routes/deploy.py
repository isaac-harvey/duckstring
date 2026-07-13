from __future__ import annotations

import io
import shutil
import subprocess
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from ...keys import version_key as _version_key
from .. import auth

router = APIRouter()


def _read_toml(text: str) -> dict:
    if sys.version_info >= (3, 11):
        import tomllib
        return tomllib.loads(text)
    import tomli
    return tomli.loads(text)


def _parse_version(s: str) -> tuple[int, str, bool]:
    """Parse "1.2.3" or "1.2.3?" → (major, min_version, required)."""
    required = not s.endswith("?")
    ver = s.rstrip("?")
    major = int(ver.split(".")[0])
    return major, ver, required


def _selected_version(db, source_name: str, major: int) -> str | None:
    """The version currently *selected* for ``source_name@major`` (the ``pond`` pointer), or None when
    that major line isn't deployed yet."""
    row = db.execute(
        "SELECT pv.version FROM pond p "
        "JOIN pond_version pv ON pv.id = p.pond_version_id "
        "JOIN pond_name pn ON pn.id = p.pond_name_id "
        "WHERE pn.name = ? AND p.major = ?",
        (source_name, major),
    ).fetchone()
    return row[0] if row else None


def _check_version_contracts(db, pn_id: int, name: str, version: str, cfg: dict) -> None:
    """Enforce the ``min_version`` contract on this deploy (raises ValueError → HTTP 422 on violation):

    1. **As a Sink** — each Source pin's selected version (within the pinned major) must be ``>=`` the
       pin's ``min_version``. An undeployed Source is skipped (the Source's own deploy will re-check).
    2. **As a Source** — selecting ``version`` for this major must not regress below the ``min_version``
       any already-deployed downstream Sink pins on this major (a downgrade that would break them)."""
    # (1) This Pond as a Sink under-pinning its Sources.
    for src_name, ver_str in cfg["sources"].items():
        src_major, src_min, _required = _parse_version(ver_str)
        selected = _selected_version(db, src_name, src_major)
        if selected is not None and _version_key(selected) < _version_key(src_min):
            raise ValueError(
                f"'{name}' pins '{src_name}' at >= {src_min} (major {src_major}), but the deployed "
                f"selected version is {selected} — deploy {src_name} {src_min} or newer first, "
                f"or relax the pin in pond.toml"
            )

    # (2) This Pond as a Source whose new selected version regresses an existing downstream pin.
    this_major = int(version.split(".")[0])
    for min_ver, sink_name in db.execute(
        "SELECT p2p.min_version, sink_pn.name FROM pond_to_pond p2p "
        "JOIN pond sink ON sink.id = p2p.pond_id "
        "JOIN pond_name sink_pn ON sink_pn.id = sink.pond_name_id "
        "WHERE p2p.source_pond_name_id = ? AND p2p.source_major = ? AND p2p.min_version IS NOT NULL",
        (pn_id, this_major),
    ).fetchall():
        if _version_key(version) < _version_key(min_ver):
            raise ValueError(
                f"selecting '{name}' {version} would break '{sink_name}', which pins '{name}' at "
                f">= {min_ver} (major {this_major}) — deploy a version >= {min_ver}, "
                f"or bump the major as the sanctioned breaking-change escape hatch"
            )


def _pond_config(toml_path: Path) -> dict:
    """Extract the deploy-relevant config from pond.toml: sources, retries, kind, dbt_project. (Windows
    are operational config managed via `duckstring trigger window`, not declared at deploy time.)"""
    cfg = {"sources": {}, "immediate_retries": 0, "source_retries": 0, "kind": None, "dbt_project": None}
    if not toml_path.exists():
        return cfg
    info = _read_toml(toml_path.read_text(encoding="utf-8"))
    cfg["sources"] = info.get("sources", {})
    pond = info.get("pond", {})
    cfg["immediate_retries"] = pond.get("immediate_retries", 0)
    cfg["source_retries"] = pond.get("source_retries", 0)
    cfg["kind"] = pond.get("type")
    cfg["dbt_project"] = pond.get("dbt_project")
    return cfg


def _discover_ripples(source_dir: Path) -> list[dict]:
    from duckstring.core import collect_ripples, import_pond_module, pond_entrypoints, read_pond_toml
    from duckstring.dbt_mode import dbt_project_subpath

    info = read_pond_toml(source_dir)
    if dbt_project_subpath(info):
        return _discover_dbt_ripples(source_dir, info)  # dbt-mode: models → ripples (no @ripple code)

    ripples_entry, _ = pond_entrypoints(info)
    if not (source_dir / ripples_entry).exists():
        return []
    try:
        import_pond_module(source_dir, ripples_entry)
        return collect_ripples()
    except Exception:
        collect_ripples()
        return []


def _discover_dbt_ripples(source_dir: Path, info: dict) -> list[dict]:
    """Translate a dbt-mode Pond's model graph into ripple rows (a model = a Ripple). Raises ValueError
    (→ 422) with dbt's own error text if the project can't be parsed."""
    import tempfile

    from duckstring.dbt_mode import DbtError, manifest_to_ripples, parse_manifest, project_dir, write_profile

    proj = project_dir(source_dir, info)
    if not (proj / "dbt_project.yml").exists():
        raise ValueError(f"dbt_project points at '{proj.name}/' but no dbt_project.yml is there")
    try:
        with tempfile.TemporaryDirectory() as tmp:
            profiles = write_profile(Path(tmp), ":memory:")  # parse touches no registry
            manifest = parse_manifest(proj, profiles)
        return manifest_to_ripples(manifest)
    except DbtError as exc:
        raise ValueError(str(exc)) from exc


def _incoming_topology(ripples) -> dict[str, frozenset]:
    """The intra-Pond graph as {ripple_name: frozenset(parent_names)} — for comparing a redeploy
    against what's already stored (parents arrive as function refs, resolved to names here)."""
    func_to_name = {r["func"]: r["name"] for r in ripples}
    names = {r["name"] for r in ripples}
    sig: dict[str, frozenset] = {}
    for r in ripples:
        parents = set()
        for pf in r["parents"]:
            pn = func_to_name.get(pf, getattr(pf, "__name__", None))
            if pn and pn in names:
                parents.add(pn)
        sig[r["name"]] = frozenset(parents)
    return sig


def _existing_topology(db, pv_id) -> dict[str, frozenset]:
    """The stored intra-Pond graph for a pond_version, in the same shape as _incoming_topology."""
    id_to_name = dict(db.execute("SELECT id, name FROM ripple WHERE pond_version_id = ?", (pv_id,)).fetchall())
    parents: dict[str, set] = {nm: set() for nm in id_to_name.values()}
    for sink_id, source_id in db.execute(
        "SELECT sink_id, source_id FROM ripple_to_ripple WHERE sink_id IN "
        "(SELECT id FROM ripple WHERE pond_version_id = ?)",
        (pv_id,),
    ):
        if sink_id in id_to_name and source_id in id_to_name:
            parents[id_to_name[sink_id]].add(id_to_name[source_id])
    return {nm: frozenset(ps) for nm, ps in parents.items()}


def _capture_column_lineage(db, name: str, version: str, ripples: list[dict]) -> None:
    """Static column lineage at deploy (plans/lineage.md Phase 2): capture each ripple's plan
    (:mod:`duckstring.trickle.capture`) and walk its column derivations
    (:mod:`duckstring.trickle.lineage`), storing rows per ``pond_version``. **Best-effort and
    per-ripple**: a non-capturable ripple (raw ``con`` access, callable metrics, plain
    ``read_table``+``write_table`` code) simply contributes no rows — exact or absent, never a failed
    deploy. Source schemas come from the deployed sources' captured contracts
    (``pond_version_schema``) when available; explicit ``alias.col`` references stay provable without
    them. Recomputed wholesale each deploy (lineage is a property of the version)."""
    try:
        from duckstring.trickle.capture import NonCapturable, capture_plan
        from duckstring.trickle.lineage import column_lineage

        (pv_id,) = db.execute(
            "SELECT pv.id FROM pond_version pv JOIN pond_name pn ON pn.id = pv.pond_name_id "
            "WHERE pn.name = ? AND pv.version = ?", (name, version),
        ).fetchone()

        def source_catalog(ref: str) -> dict:
            src, table = ref.split(".", 1)
            cols = [r[0] for r in db.execute(
                'SELECT DISTINCT s."column" FROM pond_version_schema s '
                "JOIN pond_version pv ON pv.id = s.pond_version_id "
                "JOIN pond_name pn ON pn.id = pv.pond_name_id "
                'WHERE pn.name = ? AND s."table" = ?', (src, table),
            ).fetchall()]
            entry = {"location": "__SRC__", "mode": None, "pk": None, "floor": None, "f": None}
            if cols:
                entry["schema"] = {c: "" for c in cols}
            return entry

        rows: list[tuple] = []
        for r in ripples:
            func = r.get("func")
            if not callable(func):
                continue  # dbt-mode rows carry model-name strings — Phase 3 (SQL parsing) territory
            try:
                body = capture_plan(lambda host, fn=func: fn(host), source_catalog=source_catalog)
            except NonCapturable:
                continue
            except Exception:
                continue  # a ripple import-time quirk must never fail lineage capture
            for table, cols in column_lineage(body).items():
                if cols is None:
                    rows.append((pv_id, table, "", "opaque", "", ""))
                    continue
                for col, prov in cols.items():
                    if prov is None:
                        rows.append((pv_id, table, col, "opaque", "", ""))
                    elif not prov:
                        rows.append((pv_id, table, col, "constant", "", ""))
                    else:
                        rows.extend((pv_id, table, col, "exact", ref, sc) for ref, sc in sorted(prov))
        with db:
            db.execute("DELETE FROM pond_version_column_lineage WHERE pond_version_id = ?", (pv_id,))
            db.executemany(
                'INSERT OR IGNORE INTO pond_version_column_lineage '
                '(pond_version_id, "table", "column", kind, src_ref, src_column) VALUES (?, ?, ?, ?, ?, ?)',
                rows,
            )
    except Exception as exc:  # noqa: BLE001 — lineage is observability; a deploy must never fail on it
        print(f"[catchment] column-lineage capture failed for {name}@{version}: {exc}", flush=True)


def _register(db, name, version, kind, source_path, cfg, ripples) -> None:
    major = int(version.split(".")[0])
    deployed_at = datetime.now(timezone.utc).isoformat()
    with db:
        db.execute("INSERT OR IGNORE INTO pond_name (name, kind) VALUES (?, ?)", (name, kind))
        db.execute("UPDATE pond_name SET kind = ? WHERE name = ?", (kind, name))
        (pn_id,) = db.execute("SELECT id FROM pond_name WHERE name = ?", (name,)).fetchone()

        existing = db.execute(
            "SELECT id FROM pond_version WHERE pond_name_id = ? AND version = ?", (pn_id, version)
        ).fetchone()
        if existing:
            pv_id = existing[0]
            # Redeploying an already-known version (e.g. re-activating a removed/retired line, or a plain
            # re-push). A pond_version is meant to be an immutable artifact, so preserve its run history
            # by default — only touch the ripple rows when the intra-Pond topology genuinely changed.
            if _existing_topology(db, pv_id) != _incoming_topology(ripples):
                # Topology changed under the same version: the old ripple rows must be rewritten, and
                # ripple_run references them (FK), so its rows can't survive. pond_run keys only on the
                # version, so the Pond's run history is kept — we lose only the per-ripple attempt detail.
                db.execute("DELETE FROM ripple_run WHERE pond_version_id = ?", (pv_id,))
                ripple_ids = [r[0] for r in db.execute("SELECT id FROM ripple WHERE pond_version_id = ?", (pv_id,))]
                if ripple_ids:
                    marks = ",".join("?" * len(ripple_ids))
                    db.execute(
                        f"DELETE FROM ripple_to_ripple WHERE sink_id IN ({marks}) OR source_id IN ({marks})",
                        ripple_ids * 2,
                    )
                    db.execute("DELETE FROM ripple WHERE pond_version_id = ?", (pv_id,))
            db.execute(
                "UPDATE pond_version SET source_path = ?, major = ?, immediate_retries = ?, "
                "source_retries = ?, deployed_at = ? WHERE id = ?",
                (source_path, major, cfg["immediate_retries"], cfg["source_retries"], deployed_at, pv_id),
            )
        else:
            db.execute(
                "INSERT INTO pond_version (pond_name_id, version, major, source_path, "
                "immediate_retries, source_retries, deployed_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (pn_id, version, major, source_path, cfg["immediate_retries"], cfg["source_retries"], deployed_at),
            )
            (pv_id,) = db.execute(
                "SELECT id FROM pond_version WHERE pond_name_id = ? AND version = ?", (pn_id, version)
            ).fetchone()

        # Select this version for (pond_name, major) — the `pond` pointer.
        db.execute(
            "INSERT INTO pond (pond_name_id, major, pond_version_id) VALUES (?, ?, ?) "
            "ON CONFLICT(pond_name_id, major) DO UPDATE SET pond_version_id = excluded.pond_version_id",
            (pn_id, major, pv_id),
        )
        (pond_id,) = db.execute(
            "SELECT id FROM pond WHERE pond_name_id = ? AND major = ?", (pn_id, major)
        ).fetchone()

        # Seed the live retry budgets from the pond.toml defaults, but only on first creation —
        # operator edits via `duckstring control failure-budget` then survive redeploys.
        db.execute(
            "INSERT OR IGNORE INTO pond_retry (pond_id, immediate_retries, source_retries) VALUES (?, ?, ?)",
            (pond_id, cfg["immediate_retries"], cfg["source_retries"]),
        )

        name_to_id: dict[str, int] = {}
        for r in ripples:
            db.execute(
                "INSERT OR IGNORE INTO ripple (pond_version_id, name, always_run) VALUES (?, ?, ?)",
                (pv_id, r["name"], int(r.get("always_run", False))),
            )
            (rid,) = db.execute(
                "SELECT id FROM ripple WHERE pond_version_id = ? AND name = ?", (pv_id, r["name"])
            ).fetchone()
            name_to_id[r["name"]] = rid
        func_to_name = {r["func"]: r["name"] for r in ripples}
        for r in ripples:
            sink = name_to_id[r["name"]]
            for parent_func in r["parents"]:
                pn = func_to_name.get(parent_func, getattr(parent_func, "__name__", None))
                if pn and pn in name_to_id:
                    db.execute(
                        "INSERT OR IGNORE INTO ripple_to_ripple (sink_id, source_id) VALUES (?, ?)",
                        (sink, name_to_id[pn]),
                    )

        db.execute("DELETE FROM pond_to_pond WHERE pond_id = ?", (pond_id,))
        for src_name, ver_str in cfg["sources"].items():
            src_major, src_min, src_required = _parse_version(ver_str)
            db.execute("INSERT OR IGNORE INTO pond_name (name, kind) VALUES (?, 'pond')", (src_name,))
            (src_pn_id,) = db.execute("SELECT id FROM pond_name WHERE name = ?", (src_name,)).fetchone()
            db.execute(
                "INSERT OR REPLACE INTO pond_to_pond "
                "(pond_id, source_pond_name_id, source_major, required, min_version) VALUES (?, ?, ?, ?, ?)",
                (pond_id, src_pn_id, src_major, int(src_required), src_min),
            )

        _check_version_contracts(db, pn_id, name, version, cfg)

        from ..dag import assert_no_cycles
        assert_no_cycles(db)


class _GitBody(BaseModel):
    name: str
    version: str
    type: str = "pond"
    git_ref: str
    repo_url: str


@router.post("/deploy", dependencies=[auth.full])
async def deploy(request: Request):
    db = request.app.state.db
    root: Path = request.app.state.root
    ct = request.headers.get("content-type", "")

    if "multipart/form-data" in ct:
        form = await request.form()
        name = form["name"]
        version = form["version"]
        kind = form.get("type", "pond")
        archive_bytes = await form["pond"].read()

        dest = root / "ponds" / name / version
        if dest.exists():
            shutil.rmtree(dest)
        dest.mkdir(parents=True)
        try:
            with zipfile.ZipFile(io.BytesIO(archive_bytes)) as zf:
                zf.extractall(dest)
        except zipfile.BadZipFile as exc:
            shutil.rmtree(dest, ignore_errors=True)
            raise HTTPException(status_code=422, detail="Uploaded file is not a valid zip archive") from exc
    else:
        body = _GitBody(**(await request.json()))
        name, version, kind = body.name, body.version, body.type
        dest = root / "ponds" / name / version
        if dest.exists():
            shutil.rmtree(dest)
        dest.mkdir(parents=True)
        try:
            subprocess.run(["git", "clone", body.repo_url, str(dest)], check=True, capture_output=True)
            subprocess.run(["git", "-C", str(dest), "checkout", body.git_ref], check=True, capture_output=True)
        except subprocess.CalledProcessError as exc:
            shutil.rmtree(dest, ignore_errors=True)
            raise HTTPException(status_code=422, detail=f"git clone failed: {exc.stderr.decode()}") from exc

    cfg = _pond_config(dest / "pond.toml")
    if cfg["kind"]:
        kind = cfg["kind"]
    ripples = _discover_ripples(dest)
    source_path = f"ponds/{name}/{version}"
    try:
        _register(db, name, version, kind, source_path, cfg, ripples)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    _capture_column_lineage(db, name, version, ripples)  # best-effort; never fails a deploy

    if getattr(request.app.state, "driver", None) is not None:
        request.app.state.driver.reload()
        # A fix redeploy auto-clears the failure on the deployed major line.
        request.app.state.driver.clear_on_redeploy(name, int(version.split(".")[0]))
    return {"ok": True}
