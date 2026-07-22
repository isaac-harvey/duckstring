"""Catchment-level cloud config (plans/cloud-config.md, increment 2).

The persisted ``catchment_setting`` key/value store (migration 021) plus the **cloud-enable gate**:
remote compute needs shared object storage a remote box can read, so the gate is *S3 data root +
AWS credentials present*. The data-plane target is a **persisted setting** (settable after the
Catchment is made), but **set-once in practice** — switching it once data exists would strand that
data, so the setter refuses a populated Catchment (a migration is deferred).
"""

from __future__ import annotations

import functools
import os
import sqlite3

DATA_ROOT_KEY = "data_root"
_REMOTE_SCHEMES = ("s3", "gs")


# ─── the generic key/value setting store ─────────────────────────────────────────

def get_setting(con: sqlite3.Connection, key: str) -> str | None:
    row = con.execute("SELECT value FROM catchment_setting WHERE key = ?", (key,)).fetchone()
    return row[0] if row else None


def set_setting(con: sqlite3.Connection, key: str, value: str | None) -> None:
    if value is None:
        con.execute("DELETE FROM catchment_setting WHERE key = ?", (key,))
    else:
        con.execute(
            "INSERT INTO catchment_setting (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
    con.commit()


def all_settings(con: sqlite3.Connection) -> dict[str, str]:
    return dict(con.execute("SELECT key, value FROM catchment_setting").fetchall())


def has_published_data(con: sqlite3.Connection) -> bool:
    """Whether the Catchment has ever published output — a successful Pond Run. The set-once guard
    keys off this: while it is False the data root is freely settable; once True, switching it would
    strand the published data, so the setter refuses."""
    row = con.execute("SELECT 1 FROM pond_run WHERE status = 'success' LIMIT 1").fetchone()
    return row is not None


# ─── the cloud-enable gate ──────────────────────────────────────────────────────

def is_remote(data_root: str | None) -> bool:
    """A data root a remote box can read — an object store (s3://, gs://). A local path / None means
    only Catchment Ducks are launchable (no shared storage)."""
    return bool(data_root) and data_root.split("://", 1)[0].lower() in _REMOTE_SCHEMES


def aws_configured(secret_store=None) -> bool:
    """AWS credentials the control plane can use to launch/submit: env creds/profile/role, or an
    ``AWS_*`` secret in the write-only store (worker boxes themselves use instance roles — creds here
    are the control-plane half)."""
    if any(os.environ.get(k) for k in ("AWS_ACCESS_KEY_ID", "AWS_PROFILE", "AWS_ROLE_ARN",
                                       "AWS_WEB_IDENTITY_TOKEN_FILE")):
        return True
    if secret_store is not None:
        try:
            if any(n["name"].startswith("AWS_") for n in secret_store.names()):
                return True
        except Exception:
            pass
    # Fall back to the botocore credential chain (a `[default]` profile, SSO, an instance role) so a
    # plain `aws configure` enables cloud — not only explicit env vars. Cached (process-lifetime).
    return _chain_has_credentials()


@functools.lru_cache(maxsize=1)
def _chain_has_credentials() -> bool:
    try:
        import boto3
        return boto3.Session().get_credentials() is not None
    except Exception:
        return False


AWS_SECRET_PREFIX = "AWS_"


def load_aws_env(secret_store=None) -> None:
    """Inject any ``AWS_*`` secrets into the process environment so botocore's default credential chain
    actually picks them up. ``aws_configured`` already *counts* an ``AWS_*`` secret as creds (the gate);
    this gives that its teeth — every boto3 client is built off the ambient chain, so without this a
    secret-stored key would flip the gate green but real launches would fail with no ambient creds.

    Real environment variables win (``setdefault``), so an operator's explicit env is never overridden.
    Called at startup and after a matching secret is set (see routes/secrets)."""
    if secret_store is None:
        return
    try:
        names = [n["name"] for n in secret_store.names() if n["name"].startswith(AWS_SECRET_PREFIX)]
    except Exception:
        return
    changed = False
    for name in names:
        if name not in os.environ:
            value = secret_store.get(name)
            if value is not None:
                os.environ[name] = value
                changed = True
    if changed:
        _chain_has_credentials.cache_clear()


def validate_credentials(region: str | None = None) -> str | None:
    """A *live* credential check: None if the control-plane AWS creds authenticate (STS
    GetCallerIdentity), else a short error string. Deliberately **separate** from the presence-based
    ``aws_configured`` gate — the gate must stay deterministic and network-free (a transient STS blip
    must never flip cloud off and strand a running setup), so validity is surfaced (a boot warning, the
    Verify button), not folded into the gate. A region is required only to sign the request."""
    region = region or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"
    try:
        import boto3
        boto3.client("sts", region_name=region).get_caller_identity()
        return None
    except Exception as exc:
        return str(exc) or exc.__class__.__name__


def cloud_status(data_root: str | None, secret_store=None) -> dict:
    """The gate + its reasons — surfaced on /api/status and the settings endpoint so the UI can grey
    out remote-compute options and explain why."""
    remote = is_remote(data_root)
    aws = aws_configured(secret_store)
    return {
        "data_root": data_root,
        "data_root_remote": remote,
        "aws_configured": aws,
        "cloud_enabled": remote and aws,
    }
