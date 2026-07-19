"""Per-Pond compute config (plans/cloud-config.md) + remote-Duck enablement (prereqs D6).

The ``pond_duck`` OVERRIDE table coalesced over the ``pond_version`` DECLARED config (from
pond.toml) over the Catchment default (env): Duck target/size + Flock posture. Plus the
``/api/ponds/{name}/duck`` routes, the config riding ``/api/status``, the launcher receiving the
effective config on ``ensure``, the pluggable-launcher spec (``DUCKSTRING_DUCK_LAUNCHER``), and the
duck-channel artifact bundle a remote Duck boots from.
"""

from __future__ import annotations

import io
import tarfile

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from duckstring.catchment.db import connect, migrate
from duckstring.catchment.driver import Driver
from duckstring.catchment.launcher import NoopLauncher, load_launcher_class
from duckstring.catchment.routes import router
from duckstring.catchment.routes.deploy import _register

pytestmark = pytest.mark.timeout(5)

_RIPPLES = [{"func": "f1", "name": "r1", "parents": []}]


def _cfg(sources=None, kind="inlet", **declared):
    cfg = {"sources": sources or {}, "immediate_retries": 0, "source_retries": 0, "kind": kind,
           "duck_pool": None, "flock_mode": None, "flock_engine": None, "oom_policy": None}
    cfg.update(declared)
    return cfg


def _driver(tmp_path, launcher=None, cfg=None):
    db = connect(tmp_path / "duck.db")
    migrate(db)
    _register(db, "src", "1.0.0", "inlet", "ponds/src/1.0.0", cfg or _cfg(), _RIPPLES)
    return Driver(db, tmp_path, "http://x", launcher or NoopLauncher())


def _client(driver):
    app = FastAPI()
    app.include_router(router, prefix="/api")
    app.state.driver = driver
    return TestClient(app)


# ─── defaults, overrides, persistence ───────────────────────────────────────────


def _clear_env(monkeypatch):
    for k in ("DUCKSTRING_DUCK_SIZE", "DUCKSTRING_FLOCK_MODE", "DUCKSTRING_FLOCK_ENGINE",
              "DUCKSTRING_FLOCK_OOM_POLICY"):
        monkeypatch.delenv(k, raising=False)


def test_duck_defaults_come_from_env(tmp_path, monkeypatch):
    _clear_env(monkeypatch)
    d = _driver(tmp_path)
    cfg = d.duck_config("src@1")
    assert cfg["size"] == "s" and cfg["duck_target"] == "catchment"
    assert cfg["flock_mode"] == "off" and cfg["oom_policy"] == "fail_up"
    assert all(v is None for v in cfg["override"].values())

    monkeypatch.setenv("DUCKSTRING_DUCK_SIZE", "m")
    monkeypatch.setenv("DUCKSTRING_FLOCK_MODE", "upgrade")
    cfg = d.duck_config("src@1")
    assert cfg["size"] == "m" and cfg["flock_mode"] == "upgrade"


def test_declared_config_flows_from_pond_toml(tmp_path, monkeypatch):
    _clear_env(monkeypatch)
    # A pool named in pond.toml that DOES exist → used; a Flock posture declared → effective.
    d = _driver(tmp_path, cfg=_cfg(duck_pool="heavy", flock_mode="always", oom_policy="fail"))
    d.db.execute("INSERT INTO duck_pool (name, max_instances) VALUES ('heavy', 2)")
    d.reload()
    cfg = d.duck_config("src@1")
    assert cfg["duck_target"] == "heavy" and cfg["flock_mode"] == "always" and cfg["oom_policy"] == "fail"
    # The source is DECLARED, not an override.
    assert cfg["declared"]["duck_target"] == "heavy"
    assert cfg["override"]["flock_mode"] is None


def test_unknown_pool_falls_back_to_catchment(tmp_path, monkeypatch):
    _clear_env(monkeypatch)
    # pond.toml names a pool this Catchment doesn't have → run locally (portable pond.toml).
    d = _driver(tmp_path, cfg=_cfg(duck_pool="nowhere"))
    assert d.duck_config("src@1")["duck_target"] == "catchment"


def test_override_coalesces_over_declared(tmp_path, monkeypatch):
    _clear_env(monkeypatch)
    d = _driver(tmp_path, cfg=_cfg(flock_mode="upgrade"))
    assert d.duck_config("src@1")["flock_mode"] == "upgrade"  # declared
    d.set_duck("src@1", flock_mode="off")                      # override wins
    assert d.duck_config("src@1")["flock_mode"] == "off"
    d.set_duck("src@1", clear=True)                            # reverts to DECLARED, not raw default
    assert d.duck_config("src@1")["flock_mode"] == "upgrade"


def test_set_duck_overrides_and_clears(tmp_path, monkeypatch):
    _clear_env(monkeypatch)
    d = _driver(tmp_path)
    d.set_duck("src@1", size="l")
    assert d.duck_config("src@1")["size"] == "l"
    assert d.duck_config("src@1")["flock_mode"] == "off"  # untouched → default
    d.set_duck("src@1", flock_mode="always")  # partial update keeps the size override
    cfg = d.duck_config("src@1")
    assert cfg["size"] == "l" and cfg["flock_mode"] == "always"
    d.set_duck("src@1", clear=True)
    cfg = d.duck_config("src@1")
    assert cfg["size"] == "s" and cfg["flock_mode"] == "off"
    assert all(v is None for v in cfg["override"].values())


def test_set_duck_rejects_unknown_values(tmp_path):
    d = _driver(tmp_path)
    with pytest.raises(ValueError):
        d.set_duck("src@1", size="xxl")
    with pytest.raises(ValueError):
        d.set_duck("src@1", flock_mode="sometimes")
    with pytest.raises(ValueError):
        d.set_duck("src@1", oom_policy="explode")


def test_duck_override_survives_reload(tmp_path):
    d = _driver(tmp_path)
    d.set_duck("src@1", size="xl", flock_mode="always")
    d.reload()
    cfg = d.duck_config("src@1")
    assert cfg["override"]["size"] == "xl" and cfg["override"]["flock_mode"] == "always"


def test_launcher_receives_effective_config_on_dispatch(tmp_path, monkeypatch):
    _clear_env(monkeypatch)

    seen = {}

    class SpyLauncher(NoopLauncher):
        def ensure(self, pond_key, version, source_path, duck=None):
            seen[pond_key] = duck

    d = _driver(tmp_path, launcher=SpyLauncher())
    d.set_duck("src@1", size="m", flock_mode="upgrade")
    d.tap("src@1")
    assert seen["src@1"]["size"] == "m" and seen["src@1"]["flock_mode"] == "upgrade"


def test_status_and_routes_roundtrip(tmp_path, monkeypatch):
    _clear_env(monkeypatch)
    d = _driver(tmp_path)
    client = _client(d)

    got = client.get("/api/ponds/src/duck").json()
    assert got["size"] == "s" and got["flock_mode"] == "off"

    assert client.post("/api/ponds/src/duck",
                       json={"size": "l", "flock_mode": "always"}).status_code == 200
    got = client.get("/api/ponds/src/duck").json()
    assert got["size"] == "l" and got["flock_mode"] == "always"

    pond = next(p for p in client.get("/api/status").json()["ponds"] if p["name"] == "src")
    assert pond["duck"]["size"] == "l" and pond["duck"]["flock_mode"] == "always"

    assert client.post("/api/ponds/src/duck", json={"size": "huge"}).status_code == 422
    assert client.post("/api/ponds/src/duck", json={"clear": True}).status_code == 200
    assert client.get("/api/ponds/src/duck").json()["size"] == "s"


def test_subprocess_launcher_passes_duck_env(tmp_path, monkeypatch):
    import duckstring.catchment.launcher as launcher_mod

    captured = {}

    class _FakeProc:
        def poll(self):
            return None

    def fake_popen(cmd, env=None, **kw):
        captured["cmd"], captured["env"] = cmd, env
        return _FakeProc()

    monkeypatch.setattr(launcher_mod.subprocess, "Popen", fake_popen)
    launcher = launcher_mod.SubprocessLauncher(tmp_path, "http://127.0.0.1:1", token="t")
    launcher.ensure("p@1", "1.0.0", "ponds/p/1.0.0",
                    duck={"size": "m", "flock_mode": "upgrade", "oom_policy": "fail"})
    assert captured["env"]["DUCKSTRING_DUCK_SIZE"] == "m"
    assert captured["env"]["DUCKSTRING_FLOCK_MODE"] == "upgrade"
    assert captured["env"]["DUCKSTRING_FLOCK_OOM_POLICY"] == "fail"


# ─── D6: the pluggable launcher spec ─────────────────────────────────────────────


def test_load_launcher_class_resolves_and_rejects(tmp_path):
    cls = load_launcher_class("duckstring.catchment.launcher:NoopLauncher")
    assert cls is NoopLauncher
    with pytest.raises(ValueError):
        load_launcher_class("not-a-spec")
    with pytest.raises(ModuleNotFoundError):
        load_launcher_class("no.such.module:Thing")


def test_create_app_honours_launcher_env(tmp_path, monkeypatch):
    from duckstring.catchment.app import create_app

    monkeypatch.delenv("DUCKSTRING_DISABLE_DUCKS", raising=False)
    monkeypatch.setenv("DUCKSTRING_DUCK_LAUNCHER", "duckstring.catchment.launcher:NoopLauncher")
    with TestClient(create_app(tmp_path, base_url="http://127.0.0.1:1")) as client:
        assert client.get("/api/health").status_code == 200
        assert type(client.app.state.launcher) is NoopLauncher


# ─── D6: the artifact bundle a remote Duck boots from ────────────────────────────


def test_artifact_route_streams_source_bundle(tmp_path):
    d = _driver(tmp_path)
    src_dir = tmp_path / "ponds" / "src" / "1.0.0"
    (src_dir / "src").mkdir(parents=True)
    (src_dir / "pond.toml").write_text('[pond]\nname = "src"\nversion = "1.0.0"\n')
    (src_dir / "src" / "pond.py").write_text("# ripples\n")

    client = _client(d)
    r = client.get("/api/duck/src/1/artifact")
    assert r.status_code == 200
    assert r.headers["X-Duckstring-Version"] == "1.0.0"

    from duckstring.duck.__main__ import _unpack_artifact

    dest = tmp_path / "duckside" / "ponds" / "src" / "1.0.0"
    _unpack_artifact(r.content, dest)
    assert (dest / "pond.toml").read_text().startswith("[pond]")
    assert (dest / "src" / "pond.py").exists()

    assert client.get("/api/duck/nope/1/artifact").status_code == 404


def test_artifact_unpack_rejects_escaping_member(tmp_path):
    from duckstring.duck.__main__ import _unpack_artifact

    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        info = tarfile.TarInfo("../evil.py")
        payload = b"boom"
        info.size = len(payload)
        tar.addfile(info, io.BytesIO(payload))
    # 3.12+ raises tarfile's filter error; the manual fallback raises ValueError — either way it must
    # refuse the member and write nothing outside dest.
    with pytest.raises((tarfile.TarError, ValueError, OSError)):
        _unpack_artifact(buf.getvalue(), tmp_path / "dest")
    assert not (tmp_path / "evil.py").exists()
