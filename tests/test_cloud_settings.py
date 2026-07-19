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


def test_put_settings_is_set_once_after_data(tmp_path, monkeypatch):
    client, db = _client(tmp_path)
    # Simulate a published run against the registered pond_version.
    db.execute(
        "INSERT INTO pond_run (pond_version_id, f, status) "
        "SELECT id, '2026-01-01T00:00:00+00:00', 'success' FROM pond_version LIMIT 1"
    )
    db.commit()
    r = client.put("/api/catchment/settings", json={"data_root": str(tmp_path / "lake")})
    assert r.status_code == 409 and "set-once" in r.json()["detail"]


def test_put_settings_rejects_changing_configured_root(tmp_path):
    client, _ = _client(tmp_path, data_root="s3://bucket/a")
    r = client.put("/api/catchment/settings", json={"data_root": "s3://bucket/b"})
    assert r.status_code == 409
    # Idempotent same-value set is fine.
    ok = client.put("/api/catchment/settings", json={"data_root": "s3://bucket/a"})
    assert ok.status_code == 200 and ok.json().get("unchanged") is True


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
