"""Catchment-level endpoints: health, and the state download (`usage` + `archive`).

The archive is a tar stream of the whole Catchment root — the database, deployed artifacts,
exported data, registries, and ledgers. SQLite files (`duck.db`, the Duck ledgers) are added as
consistent snapshots via the backup API; live WAL sidecars are skipped (the snapshot subsumes
them). DuckDB registries are copied as-is, so download while the Catchment is quiescent if you
need the registries to be coherent.
"""

from __future__ import annotations

import io
import queue
import sqlite3
import tarfile
import tempfile
import threading
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from .. import auth

router = APIRouter()

_SKIP_SUFFIXES = (".db-wal", ".db-shm")  # subsumed by the SQLite snapshot
_SKIP_NAMES = {"secrets.json", "secrets.json.tmp"}  # the write-only secret store never travels in a bundle


def _db(request: Request) -> sqlite3.Connection:
    return request.app.state.db


@router.get("/health")
def health(request: Request):
    _db(request).execute("SELECT 1")
    return {"status": "ok"}


@router.get("/catchment/identity", dependencies=[auth.read])
def identity(request: Request):
    """This Catchment's stable id + optional display name — how a downstream resolves cross-mesh
    identity (which upstream a duct points at, and cutting cycles in the recursive lineage view)."""
    rows = dict(_db(request).execute("SELECT key, value FROM catchment_meta").fetchall())
    return {"id": rows.get("id"), "name": rows.get("name")}


# ─── Cloud settings (the data-plane target + the cloud-enable gate) ───────────────


@router.get("/catchment/settings", dependencies=[auth.read])
def get_settings(request: Request):
    """The Catchment's cloud config: the data-plane target + the cloud-enable gate (remote data root +
    AWS creds) and its reasons, so the UI can grey out remote-compute options and explain why. Also
    ``has_data`` — once true the data root is set-once (see PUT)."""
    from .. import cloud
    from ..cloud_backends import refresh_credential_status

    app = request.app
    refresh_credential_status(app.state)  # TTL-throttled background STS check; reads the cache here
    status = cloud.cloud_status(getattr(app.state, "data_root", None),
                                getattr(app.state, "secret_store", None),
                                getattr(app.state, "cloud_creds", None))
    status["has_data"] = cloud.has_published_data(_db(request))
    return status


def _catchment_name(db) -> str:
    """The catchment's display name (the switch-confirm token), falling back to its id when unnamed."""
    rows = dict(db.execute("SELECT key, value FROM catchment_meta").fetchall())
    return rows.get("name") or rows.get("id") or "unknown"


@router.get("/catchment/migration", dependencies=[auth.read])
def get_migration(request: Request):
    """The in-flight/last data-plane migration's progress: ``{status, target, pond, total_files,
    total_bytes, copied_files, copied_bytes, error}`` (``status`` ∈ idle/copying/adopting/done/failed).
    Read-gated so the UI can show a progress bar."""
    return request.app.state.driver.migration_status()


class _SettingsBody(BaseModel):
    data_root: str = ""       # an object-store URI (s3://…, gs://…) / path; empty → local (the state root)
    confirm: str | None = None  # must equal the catchment name to SWITCH once a root / data already exists
    mode: str = "empty"       # "empty" (reset+dormant) | "adopt" (pick up the target) | "migrate" (copy+adopt)


@router.put("/catchment/settings", dependencies=[auth.full])
def put_settings(request: Request, body: _SettingsBody):
    """Attach or **switch** the data-plane target (empty ``data_root`` → back to local). Switching leaves
    every Pond **as if its data had been deleted** and **dormant** — it does NOT auto-rebuild (so there is
    a window to hand-copy non-rederivable data into the new location before anything runs); the OLD
    location's data is left intact as a backup / hand-migration source. Because it empties + clears demand,
    a switch away from an existing root — or a Catchment that already has published data — requires
    ``confirm`` == the catchment name (so it can't happen by accident). Applied live (future Duck spawns
    use the new root) and persisted. Full-gated (it moves where all data lives)."""
    from ...storage import get_storage
    from .. import cloud
    from ..data_lease import acquire_lease, release_lease

    app = request.app
    db = _db(request)
    if body.mode not in ("empty", "adopt", "migrate"):
        raise HTTPException(status_code=422, detail="mode must be 'empty', 'adopt', or 'migrate'")
    new = (body.data_root or "").strip() or None  # empty → local
    current = getattr(app.state, "data_root", None)
    if new == current:
        return {**cloud.cloud_status(current, app.state.secret_store, getattr(app.state, "cloud_creds", None)),
                "unchanged": True}
    # A switch rebuilds the pipeline — gate it behind the catchment name once there's an existing root or
    # any published data, so it isn't done by accident (a first attach on an empty Catchment is free).
    name = _catchment_name(db)
    if (current is not None or cloud.has_published_data(db)) and (body.confirm or "") != name:
        raise HTTPException(status_code=422, detail=(
            f"switching the data root empties the data plane (every Pond is left with no data and idle — "
            f"no auto-rebuild; the old location is kept as a backup); confirm by passing the catchment "
            f"name: {name!r}"))
    owner_id = dict(db.execute("SELECT key, value FROM catchment_meta").fetchall()).get("id") or "unknown"
    # Take the single-writer lease on the new store (object stores only; local needs none). get_storage
    # validates the scheme.
    new_store = None
    if new is not None:
        try:
            new_store = get_storage(new)
        except Exception as exc:
            raise HTTPException(status_code=422, detail=f"unusable data root: {exc}") from None
        try:
            acquire_lease(new_store, owner_id)
        except Exception as exc:
            raise HTTPException(status_code=409, detail=f"data root is in use: {exc}") from None
    old_lease = getattr(app.state, "data_lease", None)

    def _commit_switch() -> None:
        # Persist the new root, release the previous store's lease, and refresh the cloud gate live (a
        # switch can flip cloud enabled if creds are already present) — the backends + the cred banner.
        cloud.set_setting(db, cloud.DATA_ROOT_KEY, new)  # None → deletes the setting (back to local)
        if old_lease is not None:
            try:
                release_lease(old_lease[0], old_lease[1])
            except Exception:
                pass
        app.state.data_root = new
        app.state.data_lease = (new_store, owner_id) if new_store is not None else None
        try:
            from ..cloud_backends import refresh_cloud_backends, refresh_credential_status
            refresh_cloud_backends(app)
            refresh_credential_status(app.state, force=True)
        except Exception:
            pass

    # 'migrate' copies the data first, which can be long — run it in the background and report progress via
    # GET /api/catchment/migration; the switch commits only once the copy+adopt succeeds.
    if body.mode == "migrate":
        import threading

        def _run_migration() -> None:
            try:
                app.state.driver.migrate(new)
                _commit_switch()
            except Exception:
                # Migration failed → the old plane stays live; drop the NEW store lease we acquired.
                if new_store is not None:
                    try:
                        release_lease(new_store, owner_id)
                    except Exception:
                        pass

        threading.Thread(target=_run_migration, daemon=True).start()
        return {**cloud.cloud_status(current, app.state.secret_store, getattr(app.state, "cloud_creds", None)),
                "migrating": True}

    # Switch: quiesce, re-point (old location untouched). 'empty' resets + goes dormant; 'adopt' picks up
    # data already in the target (freshness from its sidecars) and resumes with no rebuild.
    app.state.driver.switch_data_root(new, mode=body.mode)
    _commit_switch()
    return cloud.cloud_status(new, app.state.secret_store, getattr(app.state, "cloud_creds", None))


# ─── Duck Pools (Catchment-level named remote compute) ───────────────────────────


class _PoolBody(BaseModel):
    name: str
    provider: str | None = None       # 'fargate' (default) | 'ec2'
    instance_type: str | None = None  # EC2 pools
    cpu: int | None = None            # Fargate task cpu units (256 = 0.25 vCPU)
    memory: int | None = None         # Fargate task memory (MiB)
    min_instances: int | None = None
    max_instances: int | None = None
    idle_timeout: int | None = None   # seconds before scale-down
    keep_warm: int | None = None      # spare capacity beyond current load
    region: str | None = None


@router.get("/catchment/duck-pools", dependencies=[auth.read])
def list_duck_pools(request: Request):
    """The defined Duck Pools. A Pool is inert until a remote (EC2) launcher is configured — this is
    the config a pond.toml `duck = "<pool>"` or an operator override resolves against."""
    return {"pools": request.app.state.driver.list_pools()}


@router.post("/catchment/duck-pools", dependencies=[auth.full])
def upsert_duck_pool(request: Request, body: _PoolBody):
    """Create or update a named Duck Pool (it provisions billable infra, so full-gated)."""
    try:
        return request.app.state.driver.add_pool(
            body.name, provider=body.provider, instance_type=body.instance_type, cpu=body.cpu,
            memory=body.memory, min_instances=body.min_instances, max_instances=body.max_instances,
            idle_timeout=body.idle_timeout, keep_warm=body.keep_warm, region=body.region)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None


@router.delete("/catchment/duck-pools/{name}", dependencies=[auth.full])
def delete_duck_pool(request: Request, name: str):
    """Drop a pool. Ponds pinned to it fall back to the Catchment Duck (never stranded)."""
    request.app.state.driver.remove_pool(name)
    return {"ok": True}


# ─── AWS discovery (instance types) + the cloud-enable verification probe ─────────
#
# Both use the control-plane AWS creds (env / profile / role, or an AWS_* secret loaded into the env by
# cloud.load_aws_env). Any AWS failure is a 200 result ({available|ok: false, error}), never a 5xx — the
# UI degrades to free-text entry and shows the reason rather than erroring.

_INSTANCE_TYPES: dict[str, list[dict]] = {}  # region → sorted spec list; process-lifetime cache


def _aws_error(exc: Exception) -> str:
    """A short, non-secret message for the UI. boto3 error strings carry the operation + error code,
    not credentials, but cap the length so a stray verbose message can't dominate the panel."""
    msg = str(exc) or exc.__class__.__name__
    return msg if len(msg) <= 300 else msg[:297] + "..."


def _region_from_env() -> str | None:
    import os
    return os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")


def _describe_instance_types(client) -> list[dict]:
    """Every current-generation EC2 instance type in the client's region, with vCPU / memory / GPU —
    enough to render an informative, sortable dropdown."""
    out: list[dict] = []
    paginator = client.get_paginator("describe_instance_types")
    for page in paginator.paginate(Filters=[{"Name": "current-generation", "Values": ["true"]}]):
        for it in page.get("InstanceTypes", []):
            gpus = sum(g.get("Count", 0) for g in it.get("GpuInfo", {}).get("Gpus", []))
            out.append({
                "name": it["InstanceType"],
                "vcpu": it.get("VCpuInfo", {}).get("DefaultVCpus"),
                "memory_gib": round(it.get("MemoryInfo", {}).get("SizeInMiB", 0) / 1024, 1),
                "gpu": gpus,
            })
    out.sort(key=lambda t: (t.get("vcpu") or 0, t.get("memory_gib") or 0, t["name"]))
    return out


@router.get("/catchment/instance-types", dependencies=[auth.full])
def instance_types(request: Request, region: str | None = None):
    """EC2 instance types offered in ``region`` (current generation) with vCPU/memory/GPU — feeds the
    compute dropdowns so an operator doesn't type an instance type blind. Full-gated (control-plane
    creds). Cached per region for the process lifetime."""
    region = (region or "").strip() or _region_from_env()
    if not region:
        return {"available": False, "error": "no region — pass ?region= or set AWS_DEFAULT_REGION", "types": []}
    if region in _INSTANCE_TYPES:
        return {"available": True, "region": region, "types": _INSTANCE_TYPES[region]}
    try:
        import boto3  # lazy — only a cloud-enabled Catchment ever needs it
        client = boto3.client("ec2", region_name=region)
        types = _describe_instance_types(client)
    except Exception as exc:
        return {"available": False, "region": region, "error": _aws_error(exc), "types": []}
    _INSTANCE_TYPES[region] = types
    return {"available": True, "region": region, "types": types}


class _VerifyBody(BaseModel):
    data_root: str | None = None  # optional: also probe this S3 bucket is writable (before committing it)


def _probe_s3_writable(boto3, uri: str, region: str | None) -> None:
    """Put and delete a tiny probe object under the data-root prefix — proves write access without
    leaving anything behind. Raises the boto3 error on failure."""
    rest = uri.split("://", 1)[1]
    bucket, _, prefix = rest.partition("/")
    key = f"{prefix.rstrip('/')}/.duckstring-probe" if prefix else ".duckstring-probe"
    s3 = boto3.client("s3", **({"region_name": region} if region else {}))
    s3.put_object(Bucket=bucket, Key=key, Body=b"duckstring")
    s3.delete_object(Bucket=bucket, Key=key)


@router.post("/catchment/cloud/verify", dependencies=[auth.full])
def verify_cloud(request: Request, body: _VerifyBody = _VerifyBody()):
    """Probe the control-plane AWS creds (STS ``GetCallerIdentity``) and, when a ``data_root`` is given,
    that its S3 bucket is writable (a probe object put+delete) — so the UI can confirm before committing
    the set-once data root. Returns ``{ok, account?, arn?, region?, bucket_ok?, bucket_error?, error?}``;
    a credential/permission problem is a 200 ``{ok: false, error}``, not a 5xx. Full-gated."""
    region = _region_from_env()
    signing = region or "us-east-1"  # STS/S3 clients need a region to sign; the identity is global
    try:
        import boto3
    except Exception:
        return {"ok": False, "error": "boto3 is not installed on the Catchment"}
    from ..cloud_backends import set_credential_status
    try:
        ident = boto3.client("sts", region_name=signing).get_caller_identity()
    except Exception as exc:
        err = _aws_error(exc)
        set_credential_status(request.app.state, False, err)  # update the persistent banner immediately
        return {"ok": False, "region": region, "error": err}
    set_credential_status(request.app.state, True, None)
    result = {"ok": True, "account": ident.get("Account"), "arn": ident.get("Arn"), "region": region}
    root = (body.data_root or "").strip()
    if root and root.lower().startswith("s3://"):
        try:
            _probe_s3_writable(boto3, root, signing)
            result["bucket_ok"] = True
        except Exception as exc:
            result["bucket_ok"] = False
            result["bucket_error"] = _aws_error(exc)
    return result


class _ResetBody(BaseModel):
    clear_history: bool = False


@router.post("/catchment/reset", dependencies=[auth.full])
def reset_catchment(request: Request, body: _ResetBody = _ResetBody()):
    """Reset the whole Catchment to a fresh-deploy state — scrub every Pond's registry, data, and ledger
    and rewind all freshness, keeping the deployed bundles, operational config, secrets, and keys. The
    sanctioned replacement for ``rm -rf .duckstring``; stop-the-world. See plans/reset.md."""
    return request.app.state.driver.reset_catchment(clear_history=body.clear_history)


class _RotateBody(BaseModel):
    levels: list[str] | None = None  # subset to reroll; None = all three


@router.post("/catchment/keys/rotate", dependencies=[auth.full])
def rotate_keys(request: Request, body: _RotateBody = _RotateBody()):
    """Reroll the API keys for the given access levels (default all), returning the new plaintext keys
    **once** — they are stored only as hashes. The internal Duck token is untouched, so running Ducks
    keep authenticating. Requires full access (so a leaked read/demand key can't escalate)."""
    try:
        keys = auth.generate(_db(request), body.levels)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"keys": keys}


def _root_files(root: Path) -> list[tuple[Path, str]]:
    """Every regular file in the root as (path, root-relative arcname), WAL sidecars skipped."""
    files = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.name.endswith(_SKIP_SUFFIXES) or path.name in _SKIP_NAMES:
            continue
        files.append((path, path.relative_to(root).as_posix()))
    return files


def _tar_size(file_sizes: list[int]) -> int:
    """The size of an uncompressed tar of files with these sizes (512-byte header + block-padded
    content per file, 1024-byte end marker) — lets the client show a real progress total."""
    total = sum(512 + ((size + 511) // 512) * 512 for size in file_sizes)
    return total + 1024


@router.get("/catchment/usage", dependencies=[auth.full])
def usage(request: Request):
    """The root's total state size — what `catchment download` would pull. ``archive_bytes`` is a
    close estimate of the tar the archive endpoint streams (SQLite snapshots and long path headers
    can shift it slightly) — good enough for a progress total."""
    files = _root_files(Path(request.app.state.root))
    sizes = [p.stat().st_size for p, _ in files]
    return {"total_bytes": sum(sizes), "file_count": len(files), "archive_bytes": _tar_size(sizes)}


def _sqlite_snapshot(path: Path, tmpdir: str) -> Path:
    """A consistent point-in-time copy of a (possibly live, WAL-mode) SQLite database."""
    dest = Path(tmpdir) / f"{abs(hash(str(path)))}-{path.name}"
    src = sqlite3.connect(str(path))
    dst = sqlite3.connect(str(dest))
    try:
        with dst:
            src.backup(dst)
    finally:
        src.close()
        dst.close()
    return dest


class _QueueWriter(io.RawIOBase):
    """File-like adapter: tarfile writes blocks, the response generator drains them."""

    def __init__(self, q: queue.Queue):
        self.q = q

    def writable(self) -> bool:
        return True

    def write(self, b) -> int:
        self.q.put(bytes(b))
        return len(b)


@router.get("/catchment/archive", dependencies=[auth.full])
def archive(request: Request):
    """Stream the Catchment root as an uncompressed tar (no server-side temp copy of the data;
    SQLite files are snapshotted one at a time)."""
    root = Path(request.app.state.root)
    files = _root_files(root)
    q: queue.Queue = queue.Queue(maxsize=64)  # bounded: production blocks until the client drains

    def produce() -> None:
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                with tarfile.open(fileobj=_QueueWriter(q), mode="w|") as tar:
                    for path, arcname in files:
                        src = _sqlite_snapshot(path, tmpdir) if path.suffix == ".db" else path
                        tar.add(src, arcname=arcname, recursive=False)
        finally:
            q.put(None)

    threading.Thread(target=produce, daemon=True).start()

    def stream():
        while (chunk := q.get()) is not None:
            yield chunk

    return StreamingResponse(
        stream(),
        media_type="application/x-tar",
        headers={"Content-Disposition": 'attachment; filename="catchment.tar"'},
    )
