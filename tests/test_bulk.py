"""Bulk Pond operations — ``Driver.batch`` (the UI Selector / ``duckstring do`` engine) and the
``wipe_history`` log-trim. See plans/selector_ui.md."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from duckstring.catchment.db import connect, migrate
from duckstring.catchment.driver import Driver
from duckstring.catchment.launcher import NoopLauncher
from duckstring.catchment.routes import router
from duckstring.catchment.routes.deploy import _register

pytestmark = pytest.mark.timeout(5)

_RIPPLES = [{"func": "f1", "name": "r1", "parents": []}]


def _cfg(sources=None, kind="inlet"):
    return {"sources": sources or {}, "immediate_retries": 0, "source_retries": 0, "kind": kind}


def _driver(tmp_path, name="testcatch"):
    db = connect(tmp_path / "duck.db")
    migrate(db)
    db.execute("INSERT OR REPLACE INTO catchment_meta (key, value) VALUES ('name', ?)", (name,))
    db.commit()
    # a → b → c  (an inlet feeding a chain), plus an isolated `solo` inlet.
    _register(db, "a", "1.0.0", "inlet", "ponds/a/1.0.0", _cfg(), _RIPPLES)
    _register(db, "b", "1.0.0", "pond", "ponds/b/1.0.0", _cfg(sources={"a": "1.0.0"}), _RIPPLES)
    _register(db, "c", "1.0.0", "pond", "ponds/c/1.0.0", _cfg(sources={"b": "1.0.0"}), _RIPPLES)
    _register(db, "solo", "1.0.0", "inlet", "ponds/solo/1.0.0", _cfg(), _RIPPLES)
    return Driver(db, tmp_path, "http://x", NoopLauncher())


# ─── validation ──────────────────────────────────────────────────────────────


def test_unknown_operation_rejected(tmp_path):
    d = _driver(tmp_path)
    with pytest.raises(ValueError, match="unknown operation"):
        d.batch([("a", None)], ["frobnicate"])


def test_no_operations_rejected(tmp_path):
    d = _driver(tmp_path)
    with pytest.raises(ValueError, match="no operations"):
        d.batch([("a", None)], [])


def test_repair_excludes_remove_and_reset(tmp_path):
    d = _driver(tmp_path)
    with pytest.raises(ValueError, match="repair cannot be combined"):
        d.batch([("a", None)], ["repair", "remove"])
    with pytest.raises(ValueError, match="repair cannot be combined"):
        d.batch([("a", None)], ["repair", "reset"])


# ─── confirmation gate ─────────────────────────────────────────────────────────


def test_irreversible_requires_matching_confirm(tmp_path):
    d = _driver(tmp_path, name="my-catch")
    with pytest.raises(ValueError, match="type the catchment name"):
        d.batch([("a", None)], ["reset"])  # no confirm
    with pytest.raises(ValueError, match="type the catchment name"):
        d.batch([("a", None)], ["reset"], confirm="wrong")
    # correct name authorises it
    res = d.batch([("a", None)], ["reset"], confirm="my-catch")
    assert res["errors"] == []
    assert res["operations"] == ["reset"]


def test_reversible_ops_need_no_confirm(tmp_path):
    d = _driver(tmp_path)
    res = d.batch([("a", None)], ["sleep", "clear"], confirm=None)
    assert res["errors"] == []
    assert res["operations"] == ["sleep", "clear"]


# ─── op composition ────────────────────────────────────────────────────────────


def test_remove_subsumes_reset(tmp_path):
    d = _driver(tmp_path)
    # remove + reset → the redundant standalone reset is dropped; only remove runs.
    res = d.batch([("solo", None)], ["reset", "remove"], confirm="testcatch")
    assert res["operations"] == ["remove"]
    assert res["removed"] == ["solo@1"]
    assert "solo@1" not in d.state.ponds


def test_precedence_order_preserved(tmp_path):
    d = _driver(tmp_path)
    res = d.batch([("solo", None)], ["refresh", "clear", "wipe", "sleep", "kill"], confirm="testcatch")
    assert res["operations"] == ["kill", "sleep", "wipe", "clear", "refresh"]


def test_remove_is_terminal_per_pond(tmp_path):
    d = _driver(tmp_path)
    # refresh after remove: the line is gone, so refresh silently skips it (no error).
    res = d.batch([("solo", None)], ["remove", "refresh"], confirm="testcatch")
    assert res["removed"] == ["solo@1"]
    assert res["errors"] == []
    assert "solo@1" not in d.state.ponds


def test_bulk_sleep_clears_demand(tmp_path):
    d = _driver(tmp_path)
    d.wave("a@1")  # a standing pull
    assert "a@1" in d.state.triggers
    res = d.batch([("a", None)], ["sleep"])
    assert res["errors"] == []
    assert "a@1" not in d.state.triggers


# ─── wipe history ──────────────────────────────────────────────────────────────


def test_wipe_history_clears_runs_without_touching_state(tmp_path):
    d = _driver(tmp_path)
    vid = d.meta["a@1"]["version_id"]
    d.db.execute(
        "INSERT INTO pond_run (pond_version_id, f, status) VALUES (?, '2020-01-01T00:00:00+00:00', 'success')",
        (vid,),
    )
    d.db.commit()
    ps = d.state.pond_states["a@1"]
    ps.d = ps.d  # touch nothing; snapshot start_f
    before = ps.start_f
    d.wipe_history("a@1")
    rows = d.db.execute("SELECT COUNT(*) FROM pond_run WHERE pond_version_id = ?", (vid,)).fetchone()[0]
    assert rows == 0
    assert d.state.pond_states["a@1"].start_f == before  # freshness untouched


def test_batch_wipe_history(tmp_path):
    d = _driver(tmp_path)
    vid = d.meta["b@1"]["version_id"]
    d.db.execute(
        "INSERT INTO pond_run (pond_version_id, f, status) VALUES (?, '2020-01-01T00:00:00+00:00', 'success')",
        (vid,),
    )
    d.db.commit()
    res = d.batch([("b", None)], ["wipe"], confirm="testcatch")
    assert res["errors"] == []
    rows = d.db.execute("SELECT COUNT(*) FROM pond_run WHERE pond_version_id = ?", (vid,)).fetchone()[0]
    assert rows == 0


# ─── HTTP route ────────────────────────────────────────────────────────────────


def _client(driver):
    app = FastAPI()
    app.include_router(router, prefix="/api")
    app.state.driver = driver
    return TestClient(app)


def test_batch_route_confirm_and_apply(tmp_path):
    client = _client(_driver(tmp_path, name="c1"))
    # missing confirm for an irreversible op → 422
    r = client.post("/api/ponds/batch", json={"ponds": [{"name": "a"}], "operations": ["reset"]})
    assert r.status_code == 422
    # bad op-set → 422
    r = client.post("/api/ponds/batch", json={"ponds": [{"name": "a"}], "operations": ["repair", "reset"], "confirm": "c1"})
    assert r.status_code == 422
    # a reversible op applies with no confirm
    r = client.post("/api/ponds/batch", json={"ponds": [{"name": "a"}], "operations": ["sleep"]})
    assert r.status_code == 200
    assert r.json()["operations"] == ["sleep"]


def test_wipe_history_route(tmp_path):
    d = _driver(tmp_path)
    vid = d.meta["a@1"]["version_id"]
    d.db.execute(
        "INSERT INTO pond_run (pond_version_id, f, status) VALUES (?, '2020-01-01T00:00:00+00:00', 'success')",
        (vid,),
    )
    d.db.commit()
    r = _client(d).post("/api/ponds/a/wipe-history")
    assert r.status_code == 200
    assert d.db.execute("SELECT COUNT(*) FROM pond_run WHERE pond_version_id = ?", (vid,)).fetchone()[0] == 0
