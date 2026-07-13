"""Observed table-level lineage (plans/lineage.md Phase 1): the Pond handle records every read/write it
brokers (exact, at the moment of the call — never inferred), the executor drains it per Ripple Run, the
Duck ships it on the ripple event, and the Catchment persists it (idempotent on replay) and serves the
graph at GET /api/lineage. Lineage is observability: a recording failure never fails a run."""

from __future__ import annotations

from datetime import datetime, timezone

import duckdb
import pytest

import duckstring.trickle_io as T
from duckstring.catchment.db import connect, migrate
from duckstring.catchment.driver import Driver
from duckstring.catchment.launcher import NoopLauncher
from duckstring.catchment.routes.deploy import _register
from duckstring.core import Pond
from duckstring.dataplane import ParquetDataPlane
from duckstring.engine.core import NEVER

pytestmark = pytest.mark.timeout(10)
UTC = timezone.utc


def ts(hour: int) -> datetime:
    return datetime(2026, 6, 16, hour, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _parquet_plane(monkeypatch):
    monkeypatch.setenv("DUCKSTRING_DATA_PLANE", "parquet")


# ── the Pond handle records what it brokers ─────────────────────────────────────


def _sources(tmp_path):
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    T.merge_table(con, "order_line", con.sql("SELECT 1 AS id, 'p1' AS pid, 2 AS qty"), ts(1), ("id",))
    ParquetDataPlane().export(con, tmp_path / "ponds" / "sales" / "m1" / "data", f=ts(1))
    con.close()


def _pond(tmp_path):
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    return Pond("o", "1.0.0", con, root=tmp_path, source_majors={"sales": 1}, f=ts(1), previous_f=NEVER)


def test_handle_records_reads_and_writes(tmp_path):
    _sources(tmp_path)
    pond = _pond(tmp_path)
    pond.read_table("sales.order_line")                       # foreign read
    pond.write_table("clean", pond.con.sql("SELECT 1 AS id"))  # write
    pond.read_table("clean")                                   # own read
    pond.append_table("history", pond.con.sql("SELECT 1 AS id"), pk="id")
    pond.merge_table("state", pond.con.sql("SELECT 1 AS id"), pk="id")

    lin = pond.take_lineage()
    assert lin == {
        "reads": [[None, "clean"], ["sales", "order_line"]],
        "writes": ["clean", "history", "state"],
    }
    # take_lineage drains — a second call starts fresh (per-Ripple semantics).
    assert pond.take_lineage() == {"reads": [], "writes": []}
    pond.con.close()


def test_handle_records_delta_and_builder_writes(tmp_path):
    """read_delta records the source; a builder terminal's registry-direct write reaches the record via
    the record_lineage_write host hook (trickle/ stays dependency-free)."""
    _sources(tmp_path)
    pond = _pond(tmp_path)
    pond.read_delta("sales.order_line")
    pond.trickle("sales.order_line").merge("mirror", pk="id")
    lin = pond.take_lineage()
    assert ["sales", "order_line"] in lin["reads"]
    assert lin["writes"] == ["mirror"]
    pond.con.close()


# ── the Catchment persists it and serves the graph ──────────────────────────────

_CFG_INLET = {"sources": {}, "immediate_retries": 0, "source_retries": 0, "kind": "inlet"}
_RIPPLES = [{"func": "load", "name": "load", "parents": []}]


def _driver(tmp_path):
    db = connect(tmp_path / "duck.db")
    migrate(db)
    _register(db, "src", "1.0.0", "inlet", "ponds/src/1.0.0", _CFG_INLET, _RIPPLES)
    return Driver(db, tmp_path, "http://x", NoopLauncher())


LIN = {"reads": [["sales", "order_line"], [None, "clean"]], "writes": ["clean", "state"]}


def _run_with_lineage(d, f=None):
    d.pulse("src@1")
    f = f or d.state.pond_states["src@1"].start_f.isoformat()
    d.on_event("src@1", {"kind": "ripple", "f": f, "ripple": "load", "status": "success", "lineage": LIN})
    d.on_event("src@1", {"kind": "run_completed", "f": f, "status": "success"})
    return f


def test_driver_persists_and_serves_lineage(tmp_path):
    d = _driver(tmp_path)
    f = _run_with_lineage(d)

    graph = d.lineage()
    assert len(graph["ponds"]) == 1
    p = graph["ponds"][0]
    assert (p["id"], p["name"], p["version"]) == ("src@1", "src", "1.0.0")
    (r,) = p["ripples"]
    assert r["ripple"] == "load" and r["f"] == f
    assert {"source": "sales", "table": "order_line"} in r["reads"]
    assert {"source": None, "table": "clean"} in r["reads"]  # own read round-trips as null
    assert r["writes"] == ["clean", "state"]

    # replaying the same event (Duck reconnect) does not duplicate rows
    d.on_event("src@1", {"kind": "ripple", "f": f, "ripple": "load", "status": "success", "lineage": LIN})
    rows = d.db.execute("SELECT count(*) FROM ripple_run_lineage").fetchone()[0]
    assert rows == 4  # 2 reads + 2 writes, once

    # the graph reports the LATEST run per ripple
    import time

    time.sleep(0.01)
    f2 = _run_with_lineage(d)
    graph2 = d.lineage(pond="src")
    assert graph2["ponds"][0]["ripples"][0]["f"] == f2

    # table filter narrows to ripples touching the name; a miss is empty
    assert d.lineage(pond="src", table="state")["ponds"][0]["ripples"]
    assert not d.lineage(pond="src", table="nope")["ponds"][0]["ripples"]


def test_lineage_failure_never_breaks_the_run(tmp_path):
    d = _driver(tmp_path)

    class BoomDB:  # sqlite3.Connection attrs are read-only — proxy it instead
        def __init__(self, real):
            self._real = real

        def __getattr__(self, name):
            return getattr(self._real, name)

        def executemany(self, *a, **k):
            raise RuntimeError("disk full")

    d.db = BoomDB(d.db)
    f = _run_with_lineage(d)  # would raise inside _record_lineage without the guard
    assert d.state.pond_states["src@1"].end_f.isoformat() == f  # the run still completed


def test_lineage_route_is_read_gated(tmp_path, monkeypatch):
    """GET /api/lineage exists, is classified (the auth audit fails closed at create_app), and needs
    only read access."""
    monkeypatch.setenv("DUCKSTRING_DISABLE_DUCKS", "1")
    from fastapi.testclient import TestClient

    from duckstring.catchment.app import create_app

    app = create_app(tmp_path)
    with TestClient(app) as client:
        resp = client.get("/api/lineage")
        assert resp.status_code == 200
        assert resp.json() == {"ponds": []}
