"""Cloud config increment 2 (plans/cloud-config.md): the persisted data-plane setting + the
cloud-enable gate. The ``catchment_setting`` key/value store, the set-once guard, the
``/api/catchment/settings`` + ``/api/status`` surface, and boot reading the persisted root."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from duckstring.catchment import cloud
from duckstring.catchment.db import connect, migrate
from duckstring.catchment.driver import Driver
from duckstring.catchment.launcher import NoopLauncher
from duckstring.catchment.routes import router
from duckstring.catchment.routes.deploy import _register

pytestmark = pytest.mark.timeout(5)

_RIPPLES = [{"func": "f1", "name": "r1", "parents": []}]


@pytest.fixture(autouse=True)
def _no_ambient_aws(monkeypatch):
    """Neutralise the botocore credential-chain fallback so the gate is deterministic regardless of the
    machine's ambient AWS creds (a dev laptop with `aws configure` would otherwise read as configured)."""
    cloud._chain_has_credentials.cache_clear()
    monkeypatch.setattr(cloud, "_chain_has_credentials", lambda: False)


def _cfg():
    return {"sources": {}, "immediate_retries": 0, "source_retries": 0, "kind": "inlet"}


class _FakeSecrets:
    def __init__(self, names=()):
        self._names = list(names)

    def names(self):
        return [{"name": n, "set_at": "t"} for n in self._names]


def _client(tmp_path, *, data_root=None, secret_names=()):
    db = connect(tmp_path / "duck.db")
    migrate(db)
    _register(db, "src", "1.0.0", "inlet", "ponds/src/1.0.0", _cfg(), _RIPPLES)
    driver = Driver(db, tmp_path, "http://x", NoopLauncher(), data_root=data_root)
    app = FastAPI()
    app.include_router(router, prefix="/api")
    app.state.driver = driver
    app.state.db = db
    app.state.data_root = data_root
    app.state.data_lease = None
    app.state.launcher = driver.launcher
    app.state.secret_store = _FakeSecrets(secret_names)
    return TestClient(app), db


# ─── the gate helper ─────────────────────────────────────────────────────────────


def test_cloud_gate_needs_remote_root_and_creds(monkeypatch):
    for k in ("AWS_ACCESS_KEY_ID", "AWS_PROFILE", "AWS_ROLE_ARN", "AWS_WEB_IDENTITY_TOKEN_FILE"):
        monkeypatch.delenv(k, raising=False)
    aws = _FakeSecrets(["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"])
    none = _FakeSecrets([])
    assert cloud.cloud_status("s3://b/p", aws)["cloud_enabled"] is True
    assert cloud.cloud_status("s3://b/p", none)["cloud_enabled"] is False   # no creds
    assert cloud.cloud_status(None, aws)["cloud_enabled"] is False          # local root
    assert cloud.cloud_status("/local/path", aws)["cloud_enabled"] is False  # non-remote root


def test_env_creds_count_as_configured(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIA…")
    assert cloud.aws_configured(_FakeSecrets([])) is True


def test_botocore_chain_counts_as_configured(monkeypatch):
    # A plain `aws configure` (default profile, no env vars, no AWS_* secret) still enables cloud via
    # the botocore credential chain — the false-negative found against real AWS.
    for k in ("AWS_ACCESS_KEY_ID", "AWS_PROFILE", "AWS_ROLE_ARN", "AWS_WEB_IDENTITY_TOKEN_FILE"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(cloud, "_chain_has_credentials", lambda: True)
    assert cloud.aws_configured(_FakeSecrets([])) is True


# ─── the settings endpoints ──────────────────────────────────────────────────────


def test_settings_get_reports_gate(tmp_path, monkeypatch):
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    client, _ = _client(tmp_path)
    got = client.get("/api/catchment/settings").json()
    assert got["data_root"] is None and got["cloud_enabled"] is False and got["has_data"] is False


def test_put_settings_attaches_root_and_persists(tmp_path, monkeypatch):
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    client, db = _client(tmp_path, secret_names=["AWS_ACCESS_KEY_ID"])
    # A local path is a valid shared root (get_storage handles it) but not "remote" → cloud stays off.
    root = str(tmp_path / "lake")
    r = client.put("/api/catchment/settings", json={"data_root": root})
    assert r.status_code == 200 and r.json()["data_root"] == root
    # Persisted to catchment_setting + applied live to the driver.
    assert cloud.get_setting(db, cloud.DATA_ROOT_KEY) == root
    assert client.app.state.driver.data_root == root
    # A local root is not "remote", so the gate stays off even with creds.
    assert client.get("/api/catchment/settings").json()["cloud_enabled"] is False


def _confirm_token(client) -> str:
    ident = client.get("/api/catchment/identity").json()
    return ident.get("name") or ident.get("id") or "unknown"


def test_put_settings_requires_confirm_once_data_exists(tmp_path):
    client, db = _client(tmp_path)
    # Simulate a published run against the registered pond_version.
    db.execute(
        "INSERT INTO pond_run (pond_version_id, f, status) "
        "SELECT id, '2026-01-01T00:00:00+00:00', 'success' FROM pond_version LIMIT 1"
    )
    db.commit()
    lake = str(tmp_path / "lake")
    # Switching now rebuilds, so it needs the catchment-name confirmation (was: refused outright).
    r = client.put("/api/catchment/settings", json={"data_root": lake})
    assert r.status_code == 422 and "confirm" in r.json()["detail"]
    ok = client.put("/api/catchment/settings", json={"data_root": lake, "confirm": _confirm_token(client)})
    assert ok.status_code == 200 and ok.json()["data_root"] == lake
    assert client.app.state.driver.data_root == lake


def test_switch_configured_root_needs_confirm_and_can_go_local(tmp_path):
    client, _ = _client(tmp_path, data_root="s3://bucket/a")
    # No confirm → refused BEFORE any store access (so the reject needs no network).
    r = client.put("/api/catchment/settings", json={"data_root": "s3://bucket/b"})
    assert r.status_code == 422
    # Idempotent same-value set is fine.
    ok = client.put("/api/catchment/settings", json={"data_root": "s3://bucket/a"})
    assert ok.status_code == 200 and ok.json().get("unchanged") is True
    # Switching back to local (empty data root) with the confirmation succeeds and re-points the driver.
    back = client.put("/api/catchment/settings", json={"data_root": "", "confirm": _confirm_token(client)})
    assert back.status_code == 200 and back.json()["data_root"] is None
    assert client.app.state.driver.data_root is None


def test_switch_data_root_rewinds_and_preserves_old(tmp_path):
    from duckstring.catchment.driver import _iso
    from duckstring.engine.core import NEVER

    client, db = _client(tmp_path)
    driver = client.app.state.driver
    # Give the line a non-cold freshness and drop a file under the (local) old plane to prove it survives.
    old_file = tmp_path / "ponds" / "src" / "backup.parquet"
    old_file.parent.mkdir(parents=True, exist_ok=True)
    old_file.write_text("published")
    db.execute("UPDATE pond_state SET end_f='2026-01-01T00:00:00+00:00', refresh_pending=0")
    db.commit()
    driver.reload()

    driver.switch_data_root(str(tmp_path / "plane2"))

    assert driver.data_root == str(tmp_path / "plane2")   # re-pointed
    (end_f,) = db.execute("SELECT end_f FROM pond_state").fetchone()
    # Freshness rewound off the old value → rebuild into the new plane (the engine persists NEVER as NULL).
    assert end_f != "2026-01-01T00:00:00+00:00"
    assert end_f in (None, _iso(NEVER))
    assert old_file.exists()           # NON-destructive: the old location's data is kept as a backup


def test_status_carries_cloud_block(tmp_path, monkeypatch):
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    client, _ = _client(tmp_path, data_root="s3://bucket/p", secret_names=["AWS_ACCESS_KEY_ID"])
    payload = client.get("/api/status").json()
    assert payload["cloud"]["cloud_enabled"] is True
    assert payload["cloud"]["data_root"] == "s3://bucket/p"


def test_boot_reads_persisted_data_root(tmp_path, monkeypatch):
    monkeypatch.delenv("DUCKSTRING_DATA_ROOT", raising=False)
    monkeypatch.setenv("DUCKSTRING_DISABLE_DUCKS", "1")
    db = connect(tmp_path / "duck.db")
    migrate(db)
    # A local shared path — a valid persisted data root that engages the lease without a network call.
    persisted = str(tmp_path / "lake")
    cloud.set_setting(db, cloud.DATA_ROOT_KEY, persisted)
    db.close()
    from duckstring.catchment.app import create_app

    app = create_app(tmp_path, base_url="http://127.0.0.1:1")
    assert app.state.data_root == persisted
