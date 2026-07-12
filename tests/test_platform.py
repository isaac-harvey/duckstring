"""Platform-hosted Catchments (Posit Connect-style): the bind address is the platform's choice, so
the Duck dial-back address is learned from the first request's ASGI scope, with spawns deferred
until then; ``duckstring.catchment.asgi`` is the env-configured entry the platform runs."""

from __future__ import annotations

import importlib
import io
import socket
import threading
import time
import zipfile

import httpx
import pytest
import uvicorn

pytestmark = pytest.mark.timeout(10)


# ─── Deferred Duck spawning ───────────────────────────────────────────────────────


class _FakeProc:
    def poll(self):
        return None

    def terminate(self):
        pass


def test_launcher_defers_spawn_until_base_url(monkeypatch, tmp_path):
    import duckstring.catchment.launcher as launcher_mod

    spawned: list[list[str]] = []
    monkeypatch.setattr(
        launcher_mod.subprocess, "Popen", lambda cmd, **kw: (spawned.append(cmd), _FakeProc())[1]
    )

    launcher = launcher_mod.SubprocessLauncher(tmp_path, None, token="t")
    launcher.ensure("p@1", "1.0.0", "ponds/p/1.0.0")
    assert spawned == []  # no address yet — deferred
    assert launcher.is_running("p@1")  # but owned, so liveness doesn't fail it meanwhile

    launcher.set_base_url("http://127.0.0.1:9999")
    assert len(spawned) == 1
    assert "http://127.0.0.1:9999" in spawned[0]
    assert launcher.is_running("p@1")

    launcher.set_base_url("http://127.0.0.1:9999")  # idempotent: nothing left pending
    assert len(spawned) == 1


def test_launcher_terminate_drops_pending(monkeypatch, tmp_path):
    import duckstring.catchment.launcher as launcher_mod

    spawned: list[list[str]] = []
    monkeypatch.setattr(
        launcher_mod.subprocess, "Popen", lambda cmd, **kw: (spawned.append(cmd), _FakeProc())[1]
    )

    launcher = launcher_mod.SubprocessLauncher(tmp_path, None, token="t")
    launcher.ensure("p@1", "1.0.0", "ponds/p/1.0.0")
    launcher.terminate("p@1")
    assert not launcher.is_running("p@1")
    launcher.set_base_url("http://127.0.0.1:9999")
    assert spawned == []  # the killed pending spawn never happens


# ─── Dial-back learned from the first request (the Posit Connect situation) ───────


@pytest.mark.timeout(60)
def test_dialback_learned_from_request_e2e(tmp_path_factory, monkeypatch):
    """No DUCKSTRING_CATCHMENT_URL, an arbitrary port the app was never told about: the address is
    learned from the first request, deferred Ducks spawn, and a real run completes."""
    from duckstring.catchment.app import create_app

    root = tmp_path_factory.mktemp("platform_root")
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    url = f"http://127.0.0.1:{port}"
    monkeypatch.delenv("DUCKSTRING_DISABLE_DUCKS", raising=False)  # real Ducks
    monkeypatch.delenv("DUCKSTRING_CATCHMENT_URL", raising=False)  # the platform situation

    config = uvicorn.Config(create_app(root), host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            httpx.get(f"{url}/api/health", timeout=1.0)
            break
        except Exception:
            time.sleep(0.05)

    try:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("pond.toml", '[pond]\nname = "inlet"\nversion = "1.0.0"\ntype = "inlet"\n')
            zf.writestr(
                "src/pond.py",
                "from duckstring import ripple\n\n"
                "@ripple\n"
                "def make(pond):\n"
                "    pond.write_table('event', pond.con.sql('SELECT 1 AS id'))\n",
            )
        r = httpx.post(
            f"{url}/api/deploy",
            files={"pond": ("pond.zip", buf.getvalue(), "application/zip")},
            data={"name": "inlet", "version": "1.0.0", "type": "inlet"},
            timeout=15.0,
        )
        assert r.status_code == 200, r.text

        httpx.post(f"{url}/api/ponds/inlet/pulse", timeout=5.0)

        def fresh() -> bool:
            ponds = httpx.get(f"{url}/api/status", timeout=5.0).json()["ponds"]
            p = next((x for x in ponds if x["id"] == "inlet@1"), None)
            return p is not None and p.get("end_f") is not None

        deadline = time.monotonic() + 45
        while time.monotonic() < deadline and not fresh():
            time.sleep(0.25)
        assert fresh(), "the Duck should dial the learned address and complete the run"
    finally:
        server.should_exit = True
        thread.join(timeout=5)


# ─── duckstring.catchment.asgi ─────────────────────────────────────────────────────


def test_asgi_module_defaults_to_cwd_dot_duckstring(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("DUCKSTRING_ROOT", raising=False)
    import duckstring.catchment.asgi as asgi

    mod = importlib.reload(asgi)
    assert mod.app.state.root == tmp_path / ".duckstring"
    assert (tmp_path / ".duckstring" / "duck.db").exists()
    assert mod.app.state.base_url is None  # learned from the first request


def test_asgi_module_honours_env(tmp_path, monkeypatch):
    monkeypatch.setenv("DUCKSTRING_ROOT", str(tmp_path / "elsewhere"))
    monkeypatch.setenv("DUCKSTRING_API_KEY", "k")
    import duckstring.catchment.asgi as asgi

    mod = importlib.reload(asgi)
    assert mod.app.state.root == tmp_path / "elsewhere"
    assert mod.app.state.api_key == "k"
