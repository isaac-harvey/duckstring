"""Data serving increment 4: the Arrow Flight adapter. Driven with a real pyarrow Flight client — a
green run validates the columnar serving path (ticket=SQL → Arrow stream) + get_flight_info."""

from __future__ import annotations

import duckdb
import pytest

from duckstring.catchment.db import connect, migrate
from duckstring.catchment.driver import Driver
from duckstring.catchment.launcher import NoopLauncher
from duckstring.catchment.registry import pond_data_dir
from duckstring.catchment.routes.deploy import _register

flight = pytest.importorskip("pyarrow.flight")

pytestmark = pytest.mark.timeout(15)

_RIPPLES = [{"func": "f", "name": "r", "parents": []}]


def _server(tmp_path, monkeypatch):
    monkeypatch.setenv("DUCKSTRING_DATA_PLANE", "parquet")
    from duckstring.catchment.flight_sql import FlightSqlServer

    db = connect(tmp_path / "duck.db")
    migrate(db)
    cfg = {"sources": {}, "immediate_retries": 0, "source_retries": 0, "kind": "outlet",
           "serve_tables": ["revenue"]}
    _register(db, "sales", "1.0.0", "outlet", "ponds/sales/1.0.0", cfg, _RIPPLES)
    pv = db.execute("SELECT id FROM pond_version LIMIT 1").fetchone()[0]
    db.execute('INSERT INTO pond_version_schema (pond_version_id, "table", "column", type) '
               "VALUES (?, 'revenue', 'id', 'INTEGER')", (pv,))
    db.commit()
    data_dir = pond_data_dir(tmp_path, "sales", 1, None)
    data_dir.mkdir()
    con = duckdb.connect()
    with data_dir.copy_to("revenue.parquet") as uri:
        con.execute(f"COPY (SELECT range AS id FROM range(7)) TO '{uri}' (FORMAT PARQUET)")
    con.close()
    driver = Driver(db, tmp_path, "http://x", NoopLauncher())
    server = FlightSqlServer(driver, host="127.0.0.1", port=0)
    server.start()
    return server


def test_flight_client_queries_arrow(tmp_path, monkeypatch):
    server = _server(tmp_path, monkeypatch)
    try:
        client = flight.connect(f"grpc://127.0.0.1:{server.port}")
        # do_get with a SQL ticket streams Arrow.
        reader = client.do_get(flight.Ticket(b"SELECT count(*) AS n FROM sales.revenue"))
        table = reader.read_all()
        assert table.num_rows == 1 and table.column("n")[0].as_py() == 7

        # get_flight_info returns the schema + a ticket a client can then do_get.
        desc = flight.FlightDescriptor.for_command(b"SELECT id FROM sales.revenue")
        info = client.get_flight_info(desc)
        assert "id" in info.schema.names
        rows = client.do_get(info.endpoints[0].ticket).read_all()
        assert rows.num_rows == 7
    finally:
        server.stop()
