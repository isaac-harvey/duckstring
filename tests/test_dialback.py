"""RemoteDialback — the reachable Catchment URL shared across remote backends (plans/cloud-config.md).
The public-URL contract is the regression guard for a bug caught by a real Fargate run: an explicit
DUCKSTRING_CATCHMENT_PUBLIC_URL must be the dial-back URL, not the local bind (else a cloud Duck dials
127.0.0.1 and gets connection-refused)."""

from __future__ import annotations

import pytest

from duckstring.catchment.dialback import RemoteDialback

pytestmark = pytest.mark.timeout(5)


class _Relay:
    def __init__(self):
        self.started = self.stopped = False

    def configured(self):
        return True

    def start_async(self, on_ready):
        self.started = True

    def stop(self):
        self.stopped = True


def test_public_url_wins_over_bind_and_relay():
    # A reachable public URL (a tunnel / hosted URL) is the dial-back address immediately — no relay,
    # not the local bind.
    d = RemoteDialback("http://127.0.0.1:7475", relay=_Relay(), public_url="https://x.trycloudflare.com")
    assert d.url == "https://x.trycloudflare.com"
    drained = []
    d.register(lambda: drained.append(True))
    d.request()  # already resolved → must not kick the relay
    assert d.relay.started is False


def test_no_relay_resolves_to_bind_on_set_base_url():
    d = RemoteDialback("http://127.0.0.1:7475", relay=None)
    assert d.url == "http://127.0.0.1:7475"  # directly reachable bind


def test_relay_defers_until_ready_then_drains():
    relay = _Relay()
    d = RemoteDialback(None, relay=relay)          # local/unknown bind + a relay → URL unknown
    assert d.url is None
    drained = []
    d.register(lambda: drained.append(True))
    d.request()
    assert relay.started and not drained           # kicked the relay, still pending
    d._resolve("https://relay.example:7474")       # tunnel up
    assert d.url == "https://relay.example:7474" and drained == [True]


def test_create_app_wires_public_url_into_the_dialback(tmp_path, monkeypatch):
    # The end-to-end wiring the Fargate run exposed: create_app must thread PUBLIC_URL into the launcher's
    # dial-back so a cloud Duck dials the tunnel, not the bind. The launcher is built in the lifespan, so
    # enter it via TestClient (with the S3 lease + creds stubbed — no network).
    from fastapi.testclient import TestClient

    import duckstring.catchment.app as app_mod
    from duckstring.catchment import cloud, data_lease
    from duckstring.catchment.app import create_app

    monkeypatch.delenv("DUCKSTRING_DISABLE_DUCKS", raising=False)
    monkeypatch.setenv("DUCKSTRING_CATCHMENT_PUBLIC_URL", "https://tunnel.example")
    monkeypatch.setattr(cloud, "aws_configured", lambda *a, **k: True)
    monkeypatch.setattr(data_lease, "acquire_lease", lambda *a, **k: {})
    monkeypatch.setattr(app_mod, "acquire_lease", lambda *a, **k: {}, raising=False)

    app = create_app(tmp_path, base_url="http://127.0.0.1:7475", data_root="s3://bucket/data")
    with TestClient(app):
        fargate = app.state.launcher.remotes["fargate"]
        assert fargate.remote_base_url == "https://tunnel.example"
