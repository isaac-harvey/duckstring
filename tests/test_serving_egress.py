"""Data serving increment 5: egress aligns with serving — a table-less Spout egresses the SERVICEABLE
set (not every table), and a new Spout defaults its major to the served major (then stays independent)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from duckstring.catchment.db import connect, migrate
from duckstring.catchment.driver import Driver
from duckstring.catchment.launcher import NoopLauncher
from duckstring.catchment.routes import router
from duckstring.catchment.routes.deploy import _register
from duckstring.keys import pond_key

pytestmark = pytest.mark.timeout(5)

_RIPPLES = [{"func": "f", "name": "agg", "parents": []}]


def _cfg(serve_tables=None):
    return {"sources": {}, "immediate_retries": 0, "source_retries": 0, "kind": "outlet",
            "serve_tables": serve_tables or []}


def _pv(db, name, version):
    return db.execute("SELECT pv.id FROM pond_version pv JOIN pond_name pn ON pn.id = pv.pond_name_id "
                      "WHERE pn.name = ? AND pv.version = ?", (name, version)).fetchone()[0]


def _schema(db, pv, tables):
    for t in tables:
        db.execute('INSERT OR IGNORE INTO pond_version_schema (pond_version_id, "table", "column", type) '
                   "VALUES (?, ?, 'id', 'INTEGER')", (pv, t))
    db.commit()


def test_table_less_spout_egresses_the_serviceable_set(tmp_path):
    db = connect(tmp_path / "duck.db")
    migrate(db)
    _register(db, "sales", "1.0.0", "outlet", "ponds/sales/1.0.0", _cfg(["revenue"]), _RIPPLES)
    _schema(db, _pv(db, "sales", "1.0.0"), ["revenue", "secret_internal"])
    d = Driver(db, tmp_path, "http://x", NoopLauncher())
    name = d.add_spout(pond_key("sales", 1), None, None, "file:///tmp/out")  # no table → serviceable set

    # Dispatch the Spout's run and inspect the job the worker will act on.
    d._pending_egress.append((pond_key(f"sales#{name}", 1), datetime.now(timezone.utc)))
    job = next(j for j in d.take_spout_jobs() if j["spout_key"] == pond_key(f"sales#{name}", 1))
    assert job["table"] is None
    assert job["tables"] == ["revenue"]  # the declared serviceable set, not secret_internal


def test_new_spout_defaults_to_the_served_major(tmp_path):
    db = connect(tmp_path / "duck.db")
    migrate(db)
    _register(db, "sales", "1.0.0", "outlet", "ponds/sales/1.0.0", _cfg(["revenue"]), _RIPPLES)
    _schema(db, _pv(db, "sales", "1.0.0"), ["revenue"])
    _register(db, "sales", "2.0.0", "outlet", "ponds/sales/2.0.0", _cfg(["revenue"]), _RIPPLES)
    _schema(db, _pv(db, "sales", "2.0.0"), ["revenue"])
    d = Driver(db, tmp_path, "http://x", NoopLauncher())
    assert d.served_major("sales") == 1  # served stays on v1 despite v2 being highest

    app = FastAPI()
    app.include_router(router, prefix="/api")
    app.state.driver = d
    app.state.db = db
    client = TestClient(app)
    # Add a Spout WITHOUT a major → it should bind the served major (1), not the highest (2).
    r = client.post("/api/ponds/sales/spouts", json={"destination": "file:///tmp/out"})
    assert r.status_code == 200
    assert client.get("/api/ponds/sales/spouts", params={"major": 1}).json()["spouts"]      # on v1 (served)
    assert client.get("/api/ponds/sales/spouts", params={"major": 2}).json()["spouts"] == []  # not v2 (highest)
