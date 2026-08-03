from pathlib import Path

import duckdb

from ..storage import LocalStorage, Storage, get_storage


def connect(path: Path) -> duckdb.DuckDBPyConnection:
    path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path))


def pond_major_dir(root: Path, pond_name: str, major: int) -> Path:
    """The runtime **state** storage dir for one major line of a Pond — its working ``registry.duckdb``
    and ``pond.db`` ledger. Each major executes independently, so registry/ledger are per-(name, major).
    ``m{major}`` cannot collide with the version dirs deploy writes alongside it (``ponds/{name}/{semver}/``).

    This is **hot state**: it always lives on the local POSIX state root (never an object store). The
    *data* a Pond publishes goes to :func:`pond_data_dir`, which may be elsewhere (a bucket / Volume)."""
    return root / "ponds" / pond_name / f"m{major}"


def pond_registry_path(root: Path, pond_name: str, major: int) -> Path:
    return pond_major_dir(root, pond_name, major) / "registry.duckdb"


def pond_data_dir(root: Path, pond_name: str, major: int, data_root: str | None = None) -> Storage:
    """The data-plane storage location one major line publishes its tables into — a :class:`Storage`,
    not a ``Path``, so it can be a local directory **or** an object store / Volume URI.

    With ``data_root`` unset the data lives under the state root (``{root}/ponds/{name}/m{major}/data`` —
    today's exact layout). With ``data_root`` set (a path or ``s3://``/``gs://``/``abfss://`` URI) the
    line's data is ``{data_root}/{name}/m{major}/data``."""
    if data_root is None:
        return LocalStorage(pond_major_dir(root, pond_name, major) / "data")
    return get_storage(data_root).child(pond_name, f"m{major}", "data")


def pond_connect(root: Path, pond_name: str, major: int) -> duckdb.DuckDBPyConnection:
    return connect(pond_registry_path(root, pond_name, major))


def local_publish_f(local: Storage) -> str | None:
    """The freshness the LOCAL publish backs, from its ``_trickle.json`` sidecar (the max stamp across
    its tables) — ``None`` if there is no readable sidecar. Same content anchor the Pool-loss
    reconciliation uses."""
    from ..trickle_io import load_sidecar
    from .driver import _plane_freshness

    try:
        return _plane_freshness(load_sidecar(local))
    except Exception:
        return None


def resolve_data_dir(root: Path, pond_name: str, major: int, data_root: str | None = None,
                     expected_f: str | None = None) -> Storage:
    """The data-plane location to **read** one major line from, resolved local-first (plans/persist.md):
    a Pond co-located on this Pool (its Duck publishes to the local layout) is read from the local root —
    the fast handoff — and only a Pond with no local publish (a remote-Pool Duck writing straight to the
    data root) is read from ``data_root``. The local ``_trickle.json`` sidecar is the publish marker:
    every export writes one, so its presence = this line publishes here.

    Writers never use this — a publish location is explicit (:func:`pond_data_dir`), not probed.

    ``expected_f`` is the freshness the *Catchment* knows this line has published (its ``changed_f`` —
    the content anchor, since a pass publishes nothing). Given it, a local publish that is BEHIND that
    is treated as the stale leftover it is and the read falls through to the data root. That closes the
    move-to-a-remote-Pool hole: the local sidecar survives the move and would otherwise silently shadow
    fresher remote data — a wrong answer, not just a slow one. Without ``expected_f`` (a puddle run, or
    any caller with no engine state) the probe is the old presence check, which is never wrong for a
    line that has not moved.

    Reading the local sidecar is one small local file read; the data root is only touched when the local
    copy actually looks stale, so the fast handoff keeps costing nothing."""
    if data_root is None:
        return pond_data_dir(root, pond_name, major, None)
    local = pond_data_dir(root, pond_name, major, None)
    if not local.exists("_trickle.json"):
        return pond_data_dir(root, pond_name, major, data_root)
    if expected_f is not None:
        local_f = local_publish_f(local)
        if local_f is None or local_f < expected_f:
            return pond_data_dir(root, pond_name, major, data_root)
    return local
