"""Data serving increment 2: the /api/serve surface (catalog, query, promote, expose) + the route audit
still boots with the new routes classified."""

from __future__ import annotations

import duckdb
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from duckstring.catchment.db import connect, migrate
from duckstring.catchment.driver import Driver
from duckstring.catchment.launcher import NoopLauncher
from duckstring.catchment.registry import pond_data_dir
from duckstring.catchment.routes import router
from duckstring.catchment.routes.deploy import _register

pytestmark = pytest.mark.timeout(10)

_RIPPLES = [{"func": "f", "name": "r", "parents": []}]


def _cfg(serve_tables=None):
    return {"sources": {}, "immediate_retries": 0, "source_retries": 0, "kind": "outlet",
            "serve_tables": serve_tables or []}


def _pv(db, name, version):
    return db.execute("SELECT pv.id FROM pond_version pv JOIN pond_name pn ON pn.id = pv.pond_name_id "
                      "WHERE pn.name = ? AND pv.version = ?", (name, version)).fetchone()[0]


def _schema(db, pv_id, tables):
    for t in tables:
        db.execute('INSERT OR IGNORE INTO pond_version_schema (pond_version_id, "table", "column", type) '
                   "VALUES (?, ?, 'id', 'INTEGER')", (pv_id, t))
    db.commit()


def _publish(root, name, major, table, n):
    data_dir = pond_data_dir(root, name, major, None)
    data_dir.mkdir()
    con = duckdb.connect()
    with data_dir.copy_to(f"{table}.parquet") as uri:
        con.execute(f"COPY (SELECT range AS id FROM range({n})) TO '{uri}' (FORMAT PARQUET)")
    con.close()


def _client(tmp_path, monkeypatch):
    monkeypatch.setenv("DUCKSTRING_DATA_PLANE", "parquet")
    db = connect(tmp_path / "duck.db")
    migrate(db)
    _register(db, "sales", "1.0.0", "outlet", "ponds/sales/1.0.0", _cfg(["revenue"]), _RIPPLES)
    _schema(db, _pv(db, "sales", "1.0.0"), ["revenue", "secret_internal"])
    _publish(tmp_path, "sales", 1, "revenue", 7)
    _publish(tmp_path, "sales", 1, "secret_internal", 3)
    driver = Driver(db, tmp_path, "http://x", NoopLauncher())
    app = FastAPI()
    app.include_router(router, prefix="/api")
    app.state.driver = driver
    app.state.db = db
    return TestClient(app), driver, db


def test_catalog_lists_ponds_and_exposure(tmp_path, monkeypatch):
    client, _, _ = _client(tmp_path, monkeypatch)
    cat = client.get("/api/serve").json()
    sales = next(p for p in cat["ponds"] if p["name"] == "sales")
    assert sales["served_major"] == 1 and sales["majors"] == [1]
    # Open catchment = full → sees every output table with its source.
    tables = {t["table"]: t for t in sales["tables"]}
    assert tables["revenue"]["exposed"] and tables["revenue"]["source"] == "declared"
    assert not tables["secret_internal"]["exposed"] and tables["secret_internal"]["source"] == "hidden"


def test_query_through_the_core(tmp_path, monkeypatch):
    client, _, _ = _client(tmp_path, monkeypatch)
    res = client.post("/api/serve/query", json={"sql": "SELECT count(*) AS n FROM sales.revenue"}).json()
    assert res["columns"] == ["n"] and res["rows"] == [[7]]
    # A bad query surfaces a 400, not a 500.
    assert client.post("/api/serve/query", json={"sql": "SELECT * FROM nope.nope"}).status_code == 400


def test_expose_toggle_and_query_updates(tmp_path, monkeypatch):
    client, _, _ = _client(tmp_path, monkeypatch)
    # Expose the hidden table → it becomes queryable via the bare schema (warm cache refreshes on the
    # state bump).
    assert client.post("/api/ponds/sales/serve/expose",
                       json={"table": "secret_internal", "exposed": True}).status_code == 200
    res = client.post("/api/serve/query", json={"sql": "SELECT count(*) FROM sales.secret_internal"}).json()
    assert res["rows"] == [[3]]
    # Hide it again → clearing works too.
    client.post("/api/ponds/sales/serve/expose", json={"table": "secret_internal", "exposed": None})
    cat = client.get("/api/serve").json()
    tables = {t["table"] for t in cat["ponds"][0]["tables"] if t["exposed"]}
    assert tables == {"revenue"}


def test_promote_validates(tmp_path, monkeypatch):
    client, driver, db = _client(tmp_path, monkeypatch)
    # A v2 missing the served table → promote 422.
    _register(db, "sales", "2.0.0", "outlet", "ponds/sales/2.0.0", _cfg(["revenue_v2"]), _RIPPLES)
    _schema(db, _pv(db, "sales", "2.0.0"), ["revenue_v2"])
    driver.reload()
    assert client.post("/api/ponds/sales/serve/promote", json={"major": 2}).status_code == 422
    # Add the served table to v2 → promote succeeds.
    _schema(db, _pv(db, "sales", "2.0.0"), ["revenue"])
    driver.reload()
    assert client.post("/api/ponds/sales/serve/promote", json={"major": 2}).status_code == 200
    assert driver.served_major("sales") == 2


def test_route_audit_boots_with_serving_routes(tmp_path, monkeypatch):
    # audit_routes (create_app) fails closed on any unclassified /api route — so a green boot proves the
    # new /serve routes are auth-classified.
    monkeypatch.setenv("DUCKSTRING_DISABLE_DUCKS", "1")
    from duckstring.catchment.app import create_app

    with TestClient(create_app(tmp_path, base_url="http://127.0.0.1:1")) as c:
        assert c.get("/api/health").status_code == 200
