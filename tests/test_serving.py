"""Data serving increment 1 (plans/data-serving.md): the exposure model (declared serviceable +
operational override + reset-on-new-major + served-major/promote) and the sandboxed serving core."""

from __future__ import annotations

import tempfile
from pathlib import Path

import duckdb
import pytest

from duckstring.catchment.db import connect, migrate
from duckstring.catchment.driver import Driver
from duckstring.catchment.launcher import NoopLauncher
from duckstring.catchment.registry import pond_data_dir
from duckstring.catchment.routes.deploy import _register
from duckstring.catchment.serving import serving_query
from duckstring.keys import pond_key

pytestmark = pytest.mark.timeout(10)

_RIPPLES = [{"func": "f", "name": "r", "parents": []}]


def _cfg(serve_tables=None, **extra):
    cfg = {"sources": {}, "immediate_retries": 0, "source_retries": 0, "kind": "outlet",
           "serve_tables": serve_tables or []}
    cfg.update(extra)
    return cfg


def _pv_id(db, name, version):
    return db.execute(
        "SELECT pv.id FROM pond_version pv JOIN pond_name pn ON pn.id = pv.pond_name_id "
        "WHERE pn.name = ? AND pv.version = ?", (name, version)).fetchone()[0]


def _add_output_tables(db, pv_id, tables):
    for t in tables:
        db.execute('INSERT OR IGNORE INTO pond_version_schema (pond_version_id, "table", "column", type) '
                   "VALUES (?, ?, 'id', 'INTEGER')", (pv_id, t))
    db.commit()


def _driver(tmp_path, launcher=None):
    db = connect(tmp_path / "duck.db")
    migrate(db)
    return db, lambda: Driver(db, tmp_path, "http://x", launcher or NoopLauncher())


# ─── the exposure model ─────────────────────────────────────────────────────────


def test_declared_serviceable_and_override(tmp_path):
    db, mk = _driver(tmp_path)
    _register(db, "sales", "1.0.0", "outlet", "ponds/sales/1.0.0", _cfg(serve_tables=["revenue"]), _RIPPLES)
    _add_output_tables(db, _pv_id(db, "sales", "1.0.0"), ["revenue", "secret_internal"])
    d = mk()
    key = pond_key("sales", 1)

    assert d.serviceable(key) == {"revenue"}  # declared; the intermediate stays hidden
    detail = {r["table"]: r for r in d.serve_detail(key)}
    assert detail["revenue"]["exposed"] and detail["revenue"]["source"] == "declared"
    assert not detail["secret_internal"]["exposed"] and detail["secret_internal"]["source"] == "hidden"

    d.set_exposed(key, "secret_internal", True)   # eye on a non-declared table
    assert d.serviceable(key) == {"revenue", "secret_internal"}
    d.set_exposed(key, "revenue", False)          # eye off a declared table
    assert d.serviceable(key) == {"secret_internal"}
    d.set_exposed(key, "revenue", None)           # clear → back to declared
    assert d.serviceable(key) == {"secret_internal", "revenue"}


def test_override_resets_on_new_major(tmp_path):
    db, mk = _driver(tmp_path)
    _register(db, "sales", "1.0.0", "outlet", "ponds/sales/1.0.0", _cfg(serve_tables=["revenue"]), _RIPPLES)
    _add_output_tables(db, _pv_id(db, "sales", "1.0.0"), ["revenue"])
    d = mk()
    d.set_exposed(pond_key("sales", 1), "revenue", False)  # hide on v1
    assert d.serviceable(pond_key("sales", 1)) == set()

    # A NEW major line starts fresh from pond.toml (no override carry-over).
    _register(db, "sales", "2.0.0", "outlet", "ponds/sales/2.0.0", _cfg(serve_tables=["revenue"]), _RIPPLES)
    _add_output_tables(db, _pv_id(db, "sales", "2.0.0"), ["revenue"])
    d = mk()
    assert d.serviceable(pond_key("sales", 2)) == {"revenue"}   # declared default, not v1's hide
    assert d.serviceable(pond_key("sales", 1)) == set()          # v1's override persists on its line


def test_served_major_and_promote(tmp_path):
    db, mk = _driver(tmp_path)
    _register(db, "sales", "1.0.0", "outlet", "ponds/sales/1.0.0", _cfg(serve_tables=["revenue"]), _RIPPLES)
    _add_output_tables(db, _pv_id(db, "sales", "1.0.0"), ["revenue"])
    d = mk()
    assert d.served_major("sales") == 1  # first deploy auto-serves

    # Deploy v2 that does NOT publish `revenue` → promote refused (would 404 a live query).
    _register(db, "sales", "2.0.0", "outlet", "ponds/sales/2.0.0", _cfg(serve_tables=["revenue_v2"]), _RIPPLES)
    _add_output_tables(db, _pv_id(db, "sales", "2.0.0"), ["revenue_v2"])
    d = mk()
    assert d.served_major("sales") == 1  # a new major does NOT auto-advance the served pointer
    with pytest.raises(ValueError, match="doesn't publish"):
        d.promote("sales", 2)

    # A v2 that DOES publish the served table promotes cleanly.
    _add_output_tables(db, _pv_id(db, "sales", "2.0.0"), ["revenue"])
    d = mk()
    d.promote("sales", 2)
    assert d.served_major("sales") == 2
    with pytest.raises(ValueError, match="not deployed"):
        d.promote("sales", 9)


# ─── the sandboxed serving core ──────────────────────────────────────────────────


def _publish(tmp_path, name, major, table, n):
    """Write a plain-overwrite Parquet table to the pond's data dir (what the parquet plane reads)."""
    data_dir = pond_data_dir(tmp_path, name, major, None)
    data_dir.mkdir()
    con = duckdb.connect()
    with data_dir.copy_to(f"{table}.parquet") as uri:
        con.execute(f"COPY (SELECT range AS id FROM range({n})) TO '{uri}' (FORMAT PARQUET)")
    con.close()


def test_serving_core_sandboxed(tmp_path, monkeypatch):
    monkeypatch.setenv("DUCKSTRING_DATA_PLANE", "parquet")
    db, mk = _driver(tmp_path)
    _register(db, "sales", "1.0.0", "outlet", "ponds/sales/1.0.0", _cfg(serve_tables=["revenue"]), _RIPPLES)
    _add_output_tables(db, _pv_id(db, "sales", "1.0.0"), ["revenue", "secret_internal"])
    _publish(tmp_path, "sales", 1, "revenue", 7)
    _publish(tmp_path, "sales", 1, "secret_internal", 3)
    d = mk()

    # The served (bare) schema serves the serviceable table.
    res = serving_query(d, "SELECT count(*) AS n FROM sales.revenue")
    assert res["columns"] == ["n"] and res["rows"] == [(7,)]
    # The explicit _v1 form works too (pinning).
    assert serving_query(d, "SELECT count(*) FROM sales_v1.revenue")["rows"] == [(7,)]

    # A non-serviceable table is not in the surface at all.
    with pytest.raises(duckdb.Error):
        serving_query(d, "SELECT * FROM sales.secret_internal")

    # The sandbox blocks escaping the allowlist via external file access.
    with pytest.raises(duckdb.Error):
        serving_query(d, f"SELECT * FROM read_parquet('{tmp_path}/ponds/sales/m1/data/secret_internal.parquet')")

    # And specifically NOT from the system temp directory. DuckDB exempts temp_directory from the
    # external-access lock (it must be able to spill), so pointing that at the shared system temp used to
    # hand a read user every Parquet under /tmp. Only Linux CI caught it — on macOS the path above does
    # not sit inside gettempdir(), so the check above passed while the hole was wide open.
    probe = Path(tempfile.mkdtemp()) / "not_yours.parquet"
    con = duckdb.connect()
    con.execute(f"COPY (SELECT 1 AS x) TO '{probe}' (FORMAT PARQUET)")
    con.close()
    with pytest.raises(duckdb.Error):
        serving_query(d, f"SELECT * FROM read_parquet('{probe}')")


def test_full_connection_sees_all_tables(tmp_path, monkeypatch):
    monkeypatch.setenv("DUCKSTRING_DATA_PLANE", "parquet")
    db, mk = _driver(tmp_path)
    _register(db, "sales", "1.0.0", "outlet", "ponds/sales/1.0.0", _cfg(serve_tables=["revenue"]), _RIPPLES)
    _add_output_tables(db, _pv_id(db, "sales", "1.0.0"), ["revenue", "secret_internal"])
    _publish(tmp_path, "sales", 1, "revenue", 7)
    _publish(tmp_path, "sales", 1, "secret_internal", 3)
    d = mk()
    # An ops (full) connection sees every output table, not just the serviceable ones.
    assert serving_query(d, "SELECT count(*) FROM sales.secret_internal", sandboxed=False)["rows"] == [(3,)]
