"""The duck channel's job delivery, and how hard a Duck asks for work.

Delivery is a poll, not a push: ``take_jobs`` DRAINS the queue, so holding the connection would let a
Duck killed mid-poll swallow work meant for its replacement (verified — it broke reset and
missing-source recovery, and ASGI offers no reliable disconnect signal for an idle GET to close that
window). Making it a true long poll needs a spawn token so the server knows which Duck owns the queue.

Until then the cost is controlled at the client: an idle Duck backs off instead of asking ten times a
second, which was invisible beside a local Catchment but meant continuous round-trips per REMOTE Duck —
and a log so noisy it hindered debugging a live pool agent.
"""

from __future__ import annotations

import socket
import threading
import time

import httpx
import pytest

from duckstring.duck.__main__ import _POLL_MAX_S, _POLL_MIN_S, _poll_delay

pytestmark = pytest.mark.timeout(30)

_CFG = {"sources": {}, "immediate_retries": 0, "source_retries": 0, "kind": "inlet"}
_RIPPLES = [{"func": "f1", "name": "r1", "parents": []}]


def test_idle_polling_backs_off_and_is_bounded():
    delays = [_poll_delay(n) for n in range(8)]
    assert delays[0] == _POLL_MIN_S, "the first idle poll must stay responsive"
    assert delays == sorted(delays), "backoff must be monotonic"
    assert max(delays) == _POLL_MAX_S, "and bounded, or dispatch latency grows without limit"
    # The point of the change: an idle minute costs far fewer requests than a flat 0.1s would.
    flat = 60 / _POLL_MIN_S
    backed_off = 60 / _POLL_MAX_S
    assert backed_off <= flat / 10


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def channel(tmp_path, monkeypatch):
    """A live Catchment with one Pond and NO Ducks — so queued jobs stay queued for us to observe."""
    import uvicorn

    from duckstring.catchment.app import create_app
    from duckstring.catchment.db import connect, migrate
    from duckstring.catchment.routes.deploy import _register

    monkeypatch.setenv("DUCKSTRING_DISABLE_DUCKS", "1")
    db = connect(tmp_path / "duck.db")
    migrate(db)
    _register(db, "src", "1.0.0", "inlet", "ponds/src/1.0.0", _CFG, _RIPPLES)
    db.close()

    app = create_app(tmp_path)
    port = _free_port()
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{port}"
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            httpx.get(f"{url}/api/health", timeout=1.0)
            break
        except Exception:
            time.sleep(0.05)
    else:
        raise RuntimeError("Catchment did not start")
    yield url, app
    server.should_exit = True
    thread.join(timeout=5)


def test_the_poll_answers_at_once_and_delivers_queued_work(channel):
    """It must NOT hold: a held connection is what let a dead Duck drain another's queue."""
    url, app = channel
    started = time.monotonic()
    r = httpx.get(f"{url}/api/duck/src/1/jobs", params={"wait": 20.0}, timeout=10.0)
    assert r.status_code == 200 and r.json()["jobs"] == []
    assert time.monotonic() - started < 2.0, "the poll held the connection — see the module docstring"

    app.state.driver.pulse("src@1")
    jobs = httpx.get(f"{url}/api/duck/src/1/jobs", params={"wait": 20.0}, timeout=10.0).json()["jobs"]
    assert jobs and jobs[0]["kind"] == "begin_run"


def test_jobs_are_delivered_once(channel):
    """Delivery is destructive by design — the second reader gets nothing. This is precisely why a held
    connection is unsafe without knowing which Duck owns the queue."""
    url, app = channel
    app.state.driver.pulse("src@1")
    first = httpx.get(f"{url}/api/duck/src/1/jobs", timeout=10.0).json()["jobs"]
    second = httpx.get(f"{url}/api/duck/src/1/jobs", timeout=10.0).json()["jobs"]
    assert first and not second
