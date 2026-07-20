"""Data serving increment 3: the Postgres wire adapter. Driven with a real pg client (pg8000, which
uses the EXTENDED protocol like BI tools/JDBC), so a green run validates the wire — startup/auth, the
extended Parse/Bind/Describe/Execute flow, results, and DuckDB answering catalog introspection."""

from __future__ import annotations

import duckdb
import pytest

from duckstring.catchment.db import connect, migrate
from duckstring.catchment.driver import Driver
from duckstring.catchment.launcher import NoopLauncher
from duckstring.catchment.pg_wire import PgWireServer
from duckstring.catchment.registry import pond_data_dir
from duckstring.catchment.routes.deploy import _register

pg8000 = pytest.importorskip("pg8000.native")

pytestmark = pytest.mark.timeout(15)

_RIPPLES = [{"func": "f", "name": "r", "parents": []}]


def _cfg(serve_tables):
    return {"sources": {}, "immediate_retries": 0, "source_retries": 0, "kind": "outlet",
            "serve_tables": serve_tables}


def _pv(db, name, version):
    return db.execute("SELECT pv.id FROM pond_version pv JOIN pond_name pn ON pn.id = pv.pond_name_id "
                      "WHERE pn.name = ? AND pv.version = ?", (name, version)).fetchone()[0]


def _server(tmp_path, monkeypatch):
    monkeypatch.setenv("DUCKSTRING_DATA_PLANE", "parquet")
    db = connect(tmp_path / "duck.db")
    migrate(db)
    _register(db, "sales", "1.0.0", "outlet", "ponds/sales/1.0.0", _cfg(["revenue"]), _RIPPLES)
    pv = _pv(db, "sales", "1.0.0")
    for t in ("revenue", "secret_internal"):
        db.execute('INSERT OR IGNORE INTO pond_version_schema (pond_version_id, "table", "column", type) '
                   "VALUES (?, ?, 'id', 'INTEGER')", (pv, t))
    db.commit()
    for table, n in (("revenue", 7), ("secret_internal", 3)):
        data_dir = pond_data_dir(tmp_path, "sales", 1, None)
        data_dir.mkdir()
        con = duckdb.connect()
        with data_dir.copy_to(f"{table}.parquet") as uri:
            con.execute(f"COPY (SELECT range AS id FROM range({n})) TO '{uri}' (FORMAT PARQUET)")
        con.close()
    driver = Driver(db, tmp_path, "http://x", NoopLauncher())
    server = PgWireServer(driver, host="127.0.0.1", port=0)
    port = server.start()
    return server, port


def test_pg_client_queries_the_serving_surface(tmp_path, monkeypatch):
    server, port = _server(tmp_path, monkeypatch)
    try:
        conn = pg8000.Connection("duck", host="127.0.0.1", port=port, database="clouddemo", password="")
        # A real pg client (extended protocol) runs a query and parses the rows.
        rows = conn.run("SELECT count(*) AS n FROM sales.revenue")
        assert rows == [[7]]
        # An explicit _v1 pin works.
        assert conn.run("SELECT count(*) FROM sales_v1.revenue") == [[7]]
        # DuckDB answers information_schema introspection over the served schemas.
        schemas = {r[0] for r in conn.run(
            "SELECT DISTINCT table_schema FROM information_schema.tables")}
        assert "sales" in schemas and "sales_v1" in schemas
        # A bad query surfaces a pg error, not a hang.
        from pg8000.native import DatabaseError
        with pytest.raises(DatabaseError):
            conn.run("SELECT * FROM does_not_exist")
        conn.close()
    finally:
        server.stop()
