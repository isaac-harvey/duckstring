"""Postgres CDC egress driver. The transport is the DuckDB ``postgres`` extension — the driver writes
plain DuckDB SQL against an ATTACHed database — so the full apply/upsert/delete/reload/watermark logic is
exercised here against a **DuckDB-attached** destination (the same SQL path). A real-Postgres write e2e
(containerised) is the CI follow-up, gated below.

It also covers the transactional **primary-key gate** (a Ripple → Postgres Spout is refused) and the
worker's incremental path (changelog delta → upserts + deletes; full read → reload)."""

from __future__ import annotations

from datetime import datetime, timezone

import duckdb
import pytest

from duckstring.egress import get_egress
from duckstring.egress.postgres import PostgresEgressDriver

pytestmark = pytest.mark.timeout(15)
UTC = timezone.utc


@pytest.fixture(autouse=True)
def _parquet_plane(monkeypatch):
    monkeypatch.setenv("DUCKSTRING_DATA_PLANE", "parquet")  # offline/flat reads in the worker


# ─── A driver whose ATTACH points at a DuckDB database (faithful to the SQL path) ──


class _DuckPg(PostgresEgressDriver):
    """PostgresEgressDriver with the one Postgres-specific seam (the ATTACH) pointed at a DuckDB file,
    so the apply/upsert/delete/watermark SQL runs for real."""

    def __init__(self, dbfile):
        super().__init__(_dest("postgres://u@h/db"))
        self._dbfile = str(dbfile)

    def _attach(self, con) -> str:
        if self._prefix is None:
            con.execute(f"ATTACH '{self._dbfile}' AS pgdest")
            self._prefix = '"pgdest"."main"'
        return self._prefix


def _dest(uri):
    from duckstring.egress.destination import parse_destination
    return parse_destination(uri)


def _driver(tmp_path):
    return _DuckPg(tmp_path / "dest.duckdb")


def _con():
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    return con


def _read(con, table):
    """Read the delivered table back through the worker's own connection (it still holds the ATTACH)."""
    return con.execute(f'SELECT * FROM pgdest.main."{table}" ORDER BY 1').fetchall()


F1 = datetime(2026, 6, 1, tzinfo=UTC)
F2 = datetime(2026, 6, 2, tzinfo=UTC)


# ─── capabilities + scheme resolution ─────────────────────────────────────────


def test_postgres_scheme_resolves_with_full_capabilities():
    caps = get_egress("postgres://u@h/db").capabilities()
    assert (caps.supports_delta, caps.supports_delete, caps.transactional) == (True, True, True)
    assert get_egress("postgresql://u@h/db").capabilities().transactional is True


def test_conn_string_strips_schema_and_resolves_env(monkeypatch):
    monkeypatch.setenv("PGPASS", "s3cr3t")
    drv = get_egress("postgres://app:${env:PGPASS}@db:5432/analytics?schema=reporting&sslmode=require")
    assert drv._schema() == "reporting"
    conn = drv._conn_string()
    assert "s3cr3t" in conn and "schema=" not in conn and "sslmode=require" in conn


# ─── write_full (reload) against the attached DuckDB ──────────────────────────


def test_write_full_creates_table_reloads_and_sets_watermark(tmp_path):
    drv = _driver(tmp_path)
    con = _con()
    drv.write_full(con, con.sql("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(id, name)"),
                   table="dim", pk=["id"], f=F1)
    assert _read(con, "dim") == [(1, "a"), (2, "b")]
    assert drv.watermark(con, "dim") == F1

    # A later reload replaces the contents wholesale.
    drv.write_full(con, con.sql("SELECT * FROM (VALUES (2, 'B'), (3, 'c')) t(id, name)"),
                   table="dim", pk=["id"], f=F2)
    assert _read(con, "dim") == [(2, "B"), (3, "c")]
    assert drv.watermark(con, "dim") == F2


# ─── apply_delta (CDC: upserts + deletes) ─────────────────────────────────────


def _delta(con, pk, upserts_sql, deletes_sql):
    """A minimal stand-in Delta: .upserts (full rows) + .deletes (pk rows). Mirrors trickle_io.Delta's
    shape (what apply_delta consumes), so this exercises the driver without a full Trickle pipeline."""
    class _D:
        is_full = False
    d = _D()
    d.upserts = con.sql(upserts_sql)
    d.deletes = con.sql(deletes_sql)
    return d


def test_apply_delta_upserts_and_deletes_in_one_pass(tmp_path):
    drv = _driver(tmp_path)
    con = _con()
    # Seed the destination.
    drv.write_full(con, con.sql("SELECT * FROM (VALUES (1,10),(2,20),(3,30)) t(id,amt)"),
                   table="fact", pk=["id"], f=F1)

    # Δ: update id=2 → 99, insert id=4, delete id=3.
    delta = _delta(
        con, ["id"],
        "SELECT * FROM (VALUES (2, 99), (4, 40)) t(id, amt)",   # upserts (present rows)
        "SELECT * FROM (VALUES (3)) t(id)",                      # deletes (pk only)
    )
    drv.apply_delta(con, delta, table="fact", pk=["id"], f=F2)

    assert _read(con, "fact") == [(1, 10), (2, 99), (4, 40)]  # id=3 gone, id=2 updated, id=4 added
    assert drv.watermark(con, "fact") == F2


def test_apply_delta_empty_is_noop_but_advances_watermark(tmp_path):
    drv = _driver(tmp_path)
    con = _con()
    drv.write_full(con, con.sql("SELECT 1 AS id, 5 AS amt"), table="fact", pk=["id"], f=F1)
    empty = _delta(con, ["id"], "SELECT * FROM (VALUES (1, 5)) t(id, amt) WHERE 1=0",
                   "SELECT * FROM (VALUES (1)) t(id) WHERE 1=0")
    drv.apply_delta(con, empty, table="fact", pk=["id"], f=F2)
    assert _read(con, "fact") == [(1, 5)]
    assert drv.watermark(con, "fact") == F2  # the cursor still advances (idempotent re-read on retry)


def test_apply_delta_composite_pk(tmp_path):
    drv = _driver(tmp_path)
    con = _con()
    drv.write_full(con, con.sql("SELECT * FROM (VALUES ('eu',1,7),('us',1,9)) t(region,id,v)"),
                   table="g", pk=["region", "id"], f=F1)
    delta = _delta(con, ["region", "id"],
                   "SELECT * FROM (VALUES ('eu',1,70)) t(region,id,v)",
                   "SELECT * FROM (VALUES ('us',1)) t(region,id)")
    drv.apply_delta(con, delta, table="g", pk=["region", "id"], f=F2)
    assert _read(con, "g") == [("eu", 1, 70)]


def test_apply_delta_without_pk_raises():
    drv = get_egress("postgres://u@h/db")
    con = _con()
    with pytest.raises(ValueError, match="needs a primary key"):
        drv.apply_delta(con, object(), table="t", pk=None, f=F1)


def test_watermark_none_before_any_write(tmp_path):
    assert _driver(tmp_path).watermark(_con(), "never") is None


# ─── The transactional primary-key gate (refuse a Ripple → Postgres at creation) ──


def _published_driver(tmp_path, sidecar):
    from duckstring.catchment.db import connect, migrate
    from duckstring.catchment.driver import Driver
    from duckstring.catchment.launcher import NoopLauncher
    from duckstring.catchment.registry import pond_data_dir
    from duckstring.catchment.routes.deploy import _register
    from duckstring.trickle_io import write_sidecar

    db = connect(tmp_path / "duck.db")
    migrate(db)
    _register(db, "sales", "1.0.0", "outlet", "ponds/sales/1.0.0",
              {"sources": {}, "immediate_retries": 0, "source_retries": 0, "kind": "outlet"},
              [{"func": "f", "name": "agg", "parents": []}])
    data_dir = pond_data_dir(tmp_path, "sales", 1).root
    data_dir.mkdir(parents=True, exist_ok=True)
    write_sidecar(data_dir, sidecar)
    return Driver(db, tmp_path, "http://x", NoopLauncher())


def test_add_spout_pk_gate_rejects_non_trickle_to_postgres(tmp_path):
    d = _published_driver(tmp_path, {
        "revenue": {"mode": "merge", "pk": ["id"], "floor": None, "f_base": None, "f": F1.isoformat()},
        "snapshot": {"mode": "overwrite", "f": F1.isoformat()},
    })
    # a merge Trickle with a declared pk → allowed
    d.add_spout("sales@1", "rev", "revenue", "postgres://u@h/db", "auto")
    # a plain/overwrite table → refused with the signpost
    with pytest.raises(ValueError, match="needs a primary key"):
        d.add_spout("sales@1", "snap", "snapshot", "postgres://u@h/db", "auto")
    # all-tables → refused because one table (snapshot) has no pk
    with pytest.raises(ValueError, match="needs a primary key"):
        d.add_spout("sales@1", "all", None, "postgres://u@h/db", "auto")
    # the same overwrite table to a non-transactional destination → allowed
    d.add_spout("sales@1", "lake", "snapshot", f"file://{tmp_path}/out", "auto")
    # a table not yet published → allowed at creation (the worker enforces at egress)
    d.add_spout("sales@1", "future", "not_published_yet", "postgres://u@h/db", "auto")


# ─── The worker routes the changelog: apply_delta vs reload ───────────────────


def test_worker_routes_delta_vs_full(tmp_path, monkeypatch):
    import duckstring.egress.base as base
    import duckstring.trickle_io as tio
    from duckstring.catchment.egress_worker import _egress_spout
    from duckstring.catchment.registry import pond_data_dir
    from duckstring.egress.base import Capabilities
    from duckstring.trickle_io import write_sidecar

    data_dir = pond_data_dir(tmp_path, "sales", 1).root
    data_dir.mkdir(parents=True, exist_ok=True)
    duckdb.connect().execute(f"COPY (SELECT 1 AS id, 2 AS amt) TO '{data_dir / 'fact.parquet'}' (FORMAT PARQUET)")
    write_sidecar(data_dir, {"fact": {"mode": "merge", "pk": ["id"], "floor": None, "f_base": None, "f": F1.isoformat()}})

    calls, read_args = [], []

    class Fake:
        def capabilities(self):
            return Capabilities(supports_delta=True, supports_delete=True, transactional=True)

        def watermark(self, con, table):
            return F1

        def write_full(self, con, rel, *, table, pk, f):
            calls.append(("full", table, pk, f))

        def apply_delta(self, con, delta, *, table, pk, f):
            calls.append(("delta", table, pk, f))

    class _Delta:
        def __init__(self, is_full):
            self.is_full = is_full

    monkeypatch.setattr(base, "get_egress", lambda dest: Fake())

    def fake_read_delta(con, dd, table, previous_f, f, *, dp):
        read_args.append((table, previous_f, f))
        return _Delta(is_full=False)

    monkeypatch.setattr(tio, "read_delta", fake_read_delta)
    job = {"pond_name": "sales", "major": 1, "table": None, "destination": "postgres://u@h/db", "f": F2}
    _egress_spout(tmp_path, job)
    assert calls == [("delta", "fact", ["id"], F2)]
    assert read_args == [("fact", F1, F2)]  # previous_f is the in-destination watermark

    # A full read (bootstrap / coverage-miss) → reload, not an incremental apply.
    calls.clear()
    monkeypatch.setattr(tio, "read_delta", lambda *a, **k: _Delta(is_full=True))
    _egress_spout(tmp_path, job)
    assert calls == [("full", "fact", ["id"], F2)]


# ─── A real containerised Postgres (CI only) ──────────────────────────────────


@pytest.mark.skipif("DUCKSTRING_TEST_PG" not in __import__("os").environ,
                    reason="set DUCKSTRING_TEST_PG to a libpq URI (postgres://user:pass@host:port/db)")
@pytest.mark.timeout(60)
def test_postgres_real_e2e(tmp_path):
    """The real thing (CI: a postgres service container): a merge Trickle delivered incrementally over
    two epochs through the REAL DuckDB postgres extension — bootstrap full load, then a delta apply with
    an update + a delete, exactly-once via the in-destination watermark, all through the worker path."""
    import os

    import duckstring.trickle_io as T
    from duckstring.catchment.egress_worker import _egress_spout
    from duckstring.catchment.registry import pond_data_dir
    from duckstring.dataplane import ParquetDataPlane

    dsn = os.environ["DUCKSTRING_TEST_PG"]
    src = _con()
    data_dir = pond_data_dir(tmp_path, "sales", 1).root
    job = {"pond_name": "sales", "major": 1, "table": "fact", "destination": dsn, "f": F1.isoformat()}

    # Epoch 1 (bootstrap → full load): 2 rows.
    T.merge_table(src, "fact", src.sql("SELECT * FROM (VALUES (1,'a'),(2,'b')) t(id,v)"), F1, ("id",))
    ParquetDataPlane().export(src, data_dir, f=F1)
    _egress_spout(tmp_path, job)

    # Epoch 2 (incremental delta): 2 updated, 1 deleted, 3 inserted.
    T.merge_table(src, "fact", src.sql("SELECT * FROM (VALUES (2,'B'),(3,'c')) t(id,v)"), F2, ("id",))
    ParquetDataPlane().export(src, data_dir, f=F2)
    _egress_spout(tmp_path, dict(job, f=F2.isoformat()))

    # Verify at the destination through a fresh ATTACH (the driver's own transport).
    ver = _con()
    ver.execute("INSTALL postgres; LOAD postgres")
    pg_uri = dsn.split("?")[0]
    ver.execute(f"ATTACH '{pg_uri}' AS verify (TYPE postgres)")
    rows = ver.execute('SELECT id, v FROM verify.public."fact" ORDER BY id').fetchall()
    assert rows == [(2, "B"), (3, "c")]  # 1 deleted, 2 updated, 3 inserted
    wm = ver.execute(
        "SELECT f FROM verify.public._duckstring_egress WHERE table_name = 'fact'"
    ).fetchone()[0]
    assert str(wm).startswith(F2.isoformat()[:19])  # exactly-once cursor advanced to the delivered epoch

    # Re-delivery at the same epoch is a no-op (the watermark already covers it → empty delta).
    _egress_spout(tmp_path, dict(job, f=F2.isoformat()))
    rows2 = ver.execute('SELECT id, v FROM verify.public."fact" ORDER BY id').fetchall()
    assert rows2 == rows
    ver.close()
    src.close()
