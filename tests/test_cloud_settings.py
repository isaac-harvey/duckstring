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


def test_revert_to_local_needs_no_confirm(tmp_path):
    # Reverting to local disk is the escape hatch — always allowed, no catchment-name confirmation (it
    # needs no creds and keeps the old location intact).
    client, _ = _client(tmp_path, data_root="s3://bucket/a")
    r = client.put("/api/catchment/settings", json={"data_root": "", "mode": "adopt"})
    assert r.status_code == 200 and r.json()["data_root"] is None
    assert client.app.state.driver.data_root is None


def test_switch_data_root_empties_and_stays_dormant(tmp_path):
    from duckstring.catchment.driver import _iso
    from duckstring.engine.core import NEVER

    client, db = _client(tmp_path)
    driver = client.app.state.driver
    key = next(iter(driver.meta))
    # Give the line a non-cold freshness + a standing Wave, and drop a file under the (local) old plane.
    old_file = tmp_path / "ponds" / "src" / "backup.parquet"
    old_file.parent.mkdir(parents=True, exist_ok=True)
    old_file.write_text("published")
    db.execute("UPDATE pond_state SET start_f='2026-01-01T00:00:00+00:00', end_f='2026-01-01T00:00:00+00:00', has_pull=1")
    db.commit()
    driver.reload()
    driver.wave(key)  # a standing pull that must NOT survive the switch
    assert db.execute("SELECT COUNT(*) FROM pond_trigger").fetchone()[0] == 1

    driver.switch_data_root(str(tmp_path / "plane2"))

    assert driver.data_root == str(tmp_path / "plane2")   # re-pointed
    start_f, end_f, has_pull = db.execute("SELECT start_f, end_f, has_pull FROM pond_state").fetchone()
    assert start_f in (None, _iso(NEVER)) and end_f in (None, _iso(NEVER))  # emptied (as if data deleted)
    assert has_pull == 0                                                    # pull demand cleared
    assert db.execute("SELECT COUNT(*) FROM pond_trigger").fetchone()[0] == 0  # standing trigger cleared
    assert key not in driver.state.triggers                                # → dormant, no auto-run
    assert old_file.exists()                                               # old location kept as a backup


def test_put_settings_rejects_bare_relative_root(tmp_path):
    client, _ = _client(tmp_path)
    # A bare bucket name (no scheme) would become a local relative dir — reject it up front.
    r = client.put("/api/catchment/settings", json={"data_root": "mybucket"})
    assert r.status_code == 422 and "s3://" in r.json()["detail"]
    # An absolute path is a valid (local) root.
    ok = client.put("/api/catchment/settings", json={"data_root": str(tmp_path / "lake")})
    assert ok.status_code == 200


def test_switch_data_root_adopt_restores_freshness_from_plane(tmp_path):
    import json

    from duckstring.catchment.registry import pond_data_dir, pond_major_dir

    client, db = _client(tmp_path)
    driver = client.app.state.driver
    key = next(iter(driver.meta))
    m = driver.meta[key]
    name, major, pond_id = m["name"], m["major"], m["pond_id"]

    driver.wave(key)  # a standing Wave that must SURVIVE adopt (unlike the empty switch)
    # Stale local hot state from the old plane — adopt must drop it so the next run re-hydrates.
    major_dir = pond_major_dir(tmp_path, name, major)
    major_dir.mkdir(parents=True, exist_ok=True)
    (major_dir / "registry.duckdb").write_text("stale")
    (major_dir / "pond.db").write_text("stale")
    # The target plane already holds data at freshness F (recorded in its sidecar).
    F = "2026-05-05T00:00:00+00:00"
    plane = pond_data_dir(tmp_path, name, major, str(tmp_path / "planeB"))
    plane.write_text(json.dumps({"orders": {"mode": "overwrite", "f": F}}), "_trickle.json")

    driver.switch_data_root(str(tmp_path / "planeB"), mode="adopt")

    start_f, end_f = db.execute("SELECT start_f, end_f FROM pond_state WHERE pond_id=?", (pond_id,)).fetchone()
    assert start_f == F and end_f == F                          # freshness adopted from the plane
    assert not (major_dir / "registry.duckdb").exists()         # hot state dropped → re-hydrate from target
    assert not (major_dir / "pond.db").exists()
    assert db.execute("SELECT COUNT(*) FROM pond_trigger").fetchone()[0] == 1  # demand kept → resumes


def test_switch_data_root_adopt_empty_target_is_cold(tmp_path):
    from duckstring.catchment.driver import _iso
    from duckstring.engine.core import NEVER

    client, db = _client(tmp_path)
    driver = client.app.state.driver
    key = next(iter(driver.meta))
    pond_id = driver.meta[key]["pond_id"]
    driver.wave(key)  # materialise a pond_state row
    db.execute("UPDATE pond_state SET end_f='2026-01-01T00:00:00+00:00' WHERE pond_id=?", (pond_id,))
    db.commit()
    driver.reload()

    driver.switch_data_root(str(tmp_path / "empty_plane"), mode="adopt")  # no sidecar there

    (end_f,) = db.execute("SELECT end_f FROM pond_state WHERE pond_id=?", (pond_id,)).fetchone()
    assert end_f in (None, _iso(NEVER))  # empty target → cold (nothing to adopt)


def test_switch_data_root_migrate_copies_and_preserves_freshness(tmp_path):
    import json

    from duckstring.catchment.registry import pond_data_dir

    client, db = _client(tmp_path, data_root=str(tmp_path / "planeA"))
    driver = client.app.state.driver
    key = next(iter(driver.meta))
    m = driver.meta[key]
    name, major, pond_id = m["name"], m["major"], m["pond_id"]
    driver.wave(key)  # demand that must survive the migrate
    # Populate plane A FIRST (bytes precede claims — the reload below reconciles freshness against the
    # plane, and a claim no plane backs would be regressed as lost content; plans/persist.md phase 4):
    # the flat, self-contained files (carried) + Iceberg catalog/namespace (skipped).
    F = "2026-06-06T00:00:00+00:00"
    a = pond_data_dir(tmp_path, name, major, str(tmp_path / "planeA"))
    a.write_text(json.dumps({"orders": {"mode": "overwrite", "f": F}}), "_trickle.json")
    a.write_bytes(b"parquet-bytes", "orders.parquet")
    a.write_text("{}", "catalog.json")                       # Iceberg pointer — must NOT travel
    a.child("pond.db").write_bytes(b"meta", "v1.metadata.json")  # Iceberg namespace — must NOT travel
    # The Catchment is currently fresh at F (migrate carries a verbatim copy, so this must be KEPT — not
    # reset by re-reading the target sidecar).
    db.execute("UPDATE pond_state SET start_f=?, end_f=?, changed_f=? WHERE pond_id=?", (F, F, F, pond_id))
    db.commit()
    driver.reload()

    driver.migrate(str(tmp_path / "planeB"))  # blocking: copies + re-points (runs to completion here)

    b = pond_data_dir(tmp_path, name, major, str(tmp_path / "planeB"))
    assert b.read_bytes("orders.parquet") == b"parquet-bytes"   # flat data copied
    assert b.read_text("_trickle.json") is not None             # sidecar copied
    assert not b.exists("catalog.json")                         # Iceberg catalog skipped
    assert not b.exists("pond.db")                              # Iceberg namespace skipped
    assert a.read_bytes("orders.parquet") == b"parquet-bytes"   # old location left intact (backup)
    assert driver.data_root == str(tmp_path / "planeB")         # re-pointed
    start_f, end_f = db.execute("SELECT start_f, end_f FROM pond_state WHERE pond_id=?", (pond_id,)).fetchone()
    assert start_f == F and end_f == F                          # freshness PRESERVED (no reset)
    assert db.execute("SELECT COUNT(*) FROM pond_trigger").fetchone()[0] == 1  # demand kept → resumes
    mig = driver.migration_status()
    assert mig["status"] == "done" and mig["copied_files"] == mig["total_files"] and mig["total_files"] >= 1


def test_status_omits_cloud_block(tmp_path, monkeypatch):
    # The cloud gate is polled separately (GET /catchment/settings) now — kept off the ~1 s status poll.
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    client, _ = _client(tmp_path, data_root="s3://bucket/p", secret_names=["AWS_ACCESS_KEY_ID"])
    assert "cloud" not in client.get("/api/status").json()
    gate = client.get("/api/catchment/settings").json()
    assert gate["cloud_enabled"] is True and gate["data_root"] == "s3://bucket/p"


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
