"""Catchment-level cloud config (plans/cloud-config.md, increment 2).

The persisted ``catchment_setting`` key/value store (migration 021) plus the **cloud-enable gate**:
remote compute needs shared object storage a remote box can read, so the gate is *S3 data root +
AWS credentials present*. The data-plane target is a **persisted setting** (settable after the
Catchment is made), but **set-once in practice** — switching it once data exists would strand that
data, so the setter refuses a populated Catchment (a migration is deferred).
"""

from __future__ import annotations

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
            return any(n["name"].startswith("AWS_") for n in secret_store.names())
        except Exception:
            return False
    return False


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
