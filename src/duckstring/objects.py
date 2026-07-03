"""Objects — non-tabular Pond outputs (an ML model, a serialised artifact, a blob).

An **Object** is a *named*, non-tabular artifact a Ripple publishes alongside its tables, resolved
cross-Pond by the same ``(name@major, data_dir)`` addressing tables use. It is **ripple-only** and
**overwrite-only** (one current version per ``name@major``; no history/retention — that is a possible
later Trickle extension). See ``plans/objects.md``.

Layout under a Pond line's ``data_dir`` (local or object store), addressed via the :class:`Storage` seam:

    {data_dir}/objects/{name}/data{ext}     # a single-file Object (ext preserved, e.g. `.pkl`)
    {data_dir}/objects/{name}/<tree>        # a directory Object (published/read as one unit)

The ``_trickle.json`` sidecar gains an ``"objects"`` section — ``{name: {f, is_dir, ext, size}}`` —
carried forward across runs (Objects **persist until overwritten**, like a table, not per-run declared).

Write is **staged then committed at export** so publish stays atomic: a Ripple exception after a
``write_object`` leaves last-good intact (the commit only runs once the run's export passes the contract
gate). Staging lives in the Duck's local state dir; the commit uploads to ``data_dir``.
"""

from __future__ import annotations

import json
import re
import shutil
from pathlib import Path

from .storage import Storage, get_storage

OBJECTS_DIR = "objects"
OBJECTS_KEY = "objects"  # the sidecar section holding Object entries (see plans/objects.md)
STAGING_DIR = ".object_staging"  # per-line local staging, swept on commit / wipe
_MANIFEST = "manifest.json"
_PAYLOAD = "payload"
_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*$")


class ObjectError(ValueError):
    """An invalid Object name or write."""


def validate_object_name(name: str) -> str:
    if not isinstance(name, str) or not _NAME_RE.match(name):
        raise ObjectError(
            f"invalid object name {name!r}: use letters, digits and underscores (a letter/underscore first)"
        )
    if name.startswith("_duckstring_"):
        raise ObjectError(f"object name {name!r} uses the reserved '_duckstring_' prefix")
    return name


def _as_storage(data_dir) -> Storage:
    return data_dir if isinstance(data_dir, Storage) else get_storage(data_dir)


# ─── write: stage during the run, commit at export ─────────────────────────────


def stage_object(staging_root: Path, name: str, src) -> None:
    """Stage one Object write locally (payload + manifest). ``src`` is a filesystem ``Path`` (a file or a
    directory), raw ``bytes``, or a binary file-like. The payload is written in its final shape under
    ``{staging_root}/{name}/payload/`` so the commit is a single directory publish."""
    validate_object_name(name)
    dest = staging_root / name
    shutil.rmtree(dest, ignore_errors=True)  # a later same-name write in one run wins
    payload = dest / _PAYLOAD
    payload.mkdir(parents=True, exist_ok=True)

    if isinstance(src, (str, Path)):
        p = Path(src).expanduser()
        if not p.exists():
            raise ObjectError(f"object {name!r}: source path does not exist: {p}")
        if p.is_dir():
            shutil.rmtree(payload)
            shutil.copytree(p, payload)
            is_dir, ext = True, ""
        else:
            ext = p.suffix
            shutil.copyfile(p, payload / f"data{ext}")
            is_dir = False
    else:
        data = src if isinstance(src, (bytes, bytearray)) else src.read()
        if not isinstance(data, (bytes, bytearray)):
            raise ObjectError(f"object {name!r}: expected bytes, a path, or a binary file-like, got {type(src).__name__}")
        (payload / "data").write_bytes(bytes(data))
        is_dir, ext = False, ""

    size = sum(f.stat().st_size for f in payload.rglob("*") if f.is_file())
    (dest / _MANIFEST).write_text(json.dumps({"is_dir": is_dir, "ext": ext, "size": size}))


def staged_names(staging_root: Path) -> list[str]:
    if not staging_root.is_dir():
        return []
    return sorted(p.name for p in staging_root.iterdir() if (p / _MANIFEST).exists())


def commit_objects(staging_root: Path, data_dir, f) -> dict[str, dict]:
    """Publish every staged Object into ``{data_dir}/objects/{name}/``, **fold** their entries into the
    sidecar's ``objects`` section (upsert — Objects persist across runs), and clear the staging root. Must
    run *after* the table export (which writes the sidecar carrying the prior ``objects`` section forward).
    Returns the newly-committed entries (``{}`` when nothing was staged)."""
    from datetime import timezone

    from .trickle.io import load_sidecar, write_sidecar

    names = staged_names(staging_root)
    if not names:
        shutil.rmtree(staging_root, ignore_errors=True)
        return {}
    store = _as_storage(data_dir)
    f_iso = f.astimezone(timezone.utc).isoformat() if f is not None else None
    entries: dict[str, dict] = {}
    for name in names:
        manifest = json.loads((staging_root / name / _MANIFEST).read_text())
        store.put_tree(staging_root / name / _PAYLOAD, OBJECTS_DIR, name)
        entries[name] = {"f": f_iso, "is_dir": manifest["is_dir"], "ext": manifest["ext"], "size": manifest["size"]}
    sidecar = load_sidecar(store)
    section = dict(sidecar.get(OBJECTS_KEY) or {})
    section.update(entries)
    sidecar[OBJECTS_KEY] = section
    write_sidecar(store, sidecar)
    shutil.rmtree(staging_root, ignore_errors=True)
    return entries


def read_staged(staging_root: Path, name: str) -> bytes | None:
    """A single-file Object staged (this run, not yet committed) — its bytes, or ``None`` if not staged.
    Lets a later Ripple in the same run read an Object an earlier Ripple wrote."""
    manifest = _staged_manifest(staging_root, name)
    if manifest is None:
        return None
    if manifest["is_dir"]:
        raise ObjectError(f"object '{name}' is a directory — use object_path() to get a local path")
    return (staging_root / name / _PAYLOAD / f"data{manifest['ext']}").read_bytes()


def staged_object_path(staging_root: Path, name: str) -> Path | None:
    """A local path to a staged (this-run, uncommitted) Object, or ``None`` if not staged."""
    manifest = _staged_manifest(staging_root, name)
    if manifest is None:
        return None
    payload = staging_root / name / _PAYLOAD
    return payload if manifest["is_dir"] else payload / f"data{manifest['ext']}"


def _staged_manifest(staging_root: Path, name: str) -> dict | None:
    m = Path(staging_root) / name / _MANIFEST
    return json.loads(m.read_text()) if m.exists() else None


def write_object_now(data_dir, name: str, src, f=None) -> None:
    """Stage-and-commit one Object in a single call (no run/abort-safety context) — used to seed a puddle
    Object for local testing. ``data_dir`` is where it publishes (a :class:`Storage` or path)."""
    import tempfile

    staging = Path(tempfile.mkdtemp(prefix="duckstring-obj-stage-"))
    try:
        stage_object(staging, name, src)
        commit_objects(staging, data_dir, f)
    finally:
        shutil.rmtree(staging, ignore_errors=True)


# ─── read ──────────────────────────────────────────────────────────────────────


def list_objects(data_dir) -> dict[str, dict]:
    """The published Object entries for a Pond line, from the sidecar's ``objects`` section."""
    from .trickle.io import load_sidecar

    return load_sidecar(data_dir).get(OBJECTS_KEY, {}) or {}


def _entry(data_dir, name: str) -> dict:
    entry = list_objects(data_dir).get(name)
    if entry is None:
        raise FileNotFoundError(
            f"No published object '{name}' — has the producing Pond completed a successful run that writes it?"
        )
    return entry


def _payload_parts(name: str, entry: dict) -> tuple[str, ...]:
    """The storage parts addressing an Object's payload: the containing dir for a directory Object, the
    single ``data{ext}`` file for a single-file Object."""
    if entry["is_dir"]:
        return (OBJECTS_DIR, name)
    return (OBJECTS_DIR, name, f"data{entry['ext']}")


def read_object(data_dir, name: str) -> bytes:
    """A single-file Object's bytes. Raises for a directory Object (use :func:`object_path`)."""
    entry = _entry(data_dir, name)
    if entry["is_dir"]:
        raise ObjectError(f"object '{name}' is a directory — use object_path() to get a local path")
    return _as_storage(data_dir).read_bytes(*_payload_parts(name, entry))


def delete_object(data_dir, name: str) -> None:
    """Remove a published Object — its ``objects/{name}/`` payload + its sidecar entry (see
    ``plans/deletes.md``). No registry involved. Idempotent."""
    from .trickle.io import load_sidecar, write_sidecar

    store = _as_storage(data_dir)
    store.rmtree(OBJECTS_DIR, name)
    sidecar = load_sidecar(store)
    section = sidecar.get(OBJECTS_KEY)
    if section and name in section:
        del section[name]
        if section:
            sidecar[OBJECTS_KEY] = section
        else:
            sidecar.pop(OBJECTS_KEY, None)
        write_sidecar(store, sidecar)


def object_path(data_dir, name: str, scratch: Path) -> Path:
    """A local path to an Object's payload — the real path for a local backend, a download into ``scratch``
    for an object store. A single-file Object resolves to its file, a directory Object to its directory."""
    entry = _entry(data_dir, name)
    store = _as_storage(data_dir)
    parts = _payload_parts(name, entry)
    dest = scratch / name / (f"data{entry['ext']}" if not entry["is_dir"] else "")
    return store.fetch(dest, *parts)
