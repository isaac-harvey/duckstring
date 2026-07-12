"""Registry hydration — rebuilding registry state from the published layout (the recovery inverse of
export, mirroring the DuckFlock driver's hydration). Covers the merge/append/overwrite tiers, the meta
row (mode/pk/floor/f_base/f_warm), the Extension-1 agg/acc snapshots, resumption equivalence (an
incremental next epoch on a hydrated registry equals an uninterrupted run), and the Duck executor's
registry-file-loss recovery trigger (a wipe must NOT re-hydrate)."""

from __future__ import annotations

from datetime import datetime, timezone

import duckdb
import pytest

from duckstring import agg
from duckstring import trickle_io as T
from duckstring.core import Pond
from duckstring.dataplane import ParquetDataPlane, hydrate_registry
from duckstring.engine.core import NEVER

UTC = timezone.utc


def ts(hour: int) -> datetime:
    return datetime(2026, 6, 16, hour, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _parquet_plane(monkeypatch):
    monkeypatch.setenv("DUCKSTRING_DATA_PLANE", "parquet")


def _con():
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    return con


def test_hydrate_round_trip_merge_append_overwrite(tmp_path):
    d = tmp_path / "data"
    src = _con()
    T.merge_table(src, "m", src.sql("SELECT * FROM (VALUES (1,'a'),(2,'b')) t(id,v)"), ts(1), ("id",))
    T.append_table(src, "a", src.sql("SELECT * FROM (VALUES (1,5),(2,6)) t(id,x)"), ts(1), ("id",))
    src.execute("CREATE TABLE o AS SELECT 1 AS id, 'x' AS lbl")
    ParquetDataPlane().export(src, d, f=ts(1))
    T.merge_table(src, "m", src.sql("SELECT * FROM (VALUES (1,'a'),(2,'B'),(3,'c')) t(id,v)"), ts(2), ("id",))
    T.append_table(src, "a", src.sql("SELECT 3 AS id, 7 AS x"), ts(2), ("id",))
    ParquetDataPlane().export(src, d, f=ts(2))

    fresh = _con()
    hydrated = hydrate_registry(fresh, d)
    assert set(hydrated) == {"m", "a", "o"}

    # meta reproduced from the sidecar
    meta = T.read_meta(fresh)
    assert meta["m"]["mode"] == "merge" and meta["m"]["pk"] == ["id"]
    assert meta["a"]["mode"] == "append"
    assert meta["m"]["floor"] == ts(1).isoformat()

    # current state reproduced (merge reconstructs base ⊎ changelog; append is its history)
    assert sorted(T.current_state(fresh, "m").fetchall()) == [(1, "a"), (2, "B"), (3, "c")]
    assert fresh.execute('SELECT count(*) FROM "a"').fetchone()[0] == 3
    assert fresh.execute("SELECT * FROM o").fetchall() == [(1, "x")]

    # the changelog window still answers a delta read (registry rows match the source registry's)
    src_clog = sorted(src.execute('SELECT id, v, _duckstring_d FROM "m__changelog"').fetchall())
    hyd_clog = sorted(fresh.execute('SELECT id, v, _duckstring_d FROM "m__changelog"').fetchall())
    assert hyd_clog == src_clog
    src.close()
    fresh.close()


def test_hydrate_resumes_incremental_aggregate(tmp_path):
    """The resumption-sufficiency property: publish (with an agg companion) → lose the registry →
    hydrate → run the NEXT epoch incrementally — identical output to the uninterrupted run."""
    def sink_run(con, root, f, pf):
        p = Pond("o", "1.0.0", con, root=root, source_majors={"fact": 1}, f=f, previous_f=pf)
        p.trickle("fact.f").aggregate(by="k", n=agg.count(), mn=agg.min("v")).merge("by_k", pk="k")
        ParquetDataPlane().export(con, root / "ponds" / "o" / "m1" / "data", f=f)

    def publish_fact(con, root, rows_sql, f):
        T.merge_table(con, "f", con.sql(rows_sql), f, ("id",))
        ParquetDataPlane().export(con, root / "ponds" / "fact" / "m1" / "data", f=f)

    def read_out(root):
        con = _con()
        sql = ParquetDataPlane().read_select(root / "ponds" / "o" / "m1" / "data", "by_k")
        rows = sorted(con.sql(f"SELECT k, n, mn FROM ({sql})").fetchall())
        con.close()
        return rows

    e1 = "SELECT * FROM (VALUES (1,10,'A'),(2,20,'A'),(3,5,'B')) t(id,v,k)"
    e2 = "SELECT * FROM (VALUES (1,10,'A'),(3,5,'B'),(4,1,'B')) t(id,v,k)"

    # Reference: uninterrupted two-epoch run.
    ref = tmp_path / "ref"
    fact_ref, sink_ref = _con(), _con()
    publish_fact(fact_ref, ref, e1, ts(1))
    sink_run(sink_ref, ref, ts(1), NEVER)
    publish_fact(fact_ref, ref, e2, ts(2))
    sink_run(sink_ref, ref, ts(2), ts(1))
    want = read_out(ref)

    # Recovery: epoch 1, then the sink registry is LOST; hydrate a fresh one and run epoch 2.
    rec = tmp_path / "rec"
    fact, sink = _con(), _con()
    publish_fact(fact, rec, e1, ts(1))
    sink_run(sink, rec, ts(1), NEVER)
    sink.close()  # the registry is gone

    revived = _con()
    hydrate_registry(revived, rec / "ponds" / "o" / "m1" / "data")
    # the Extension-1 snapshot restored the raw accumulators
    assert revived.execute('SELECT count(*) FROM "_duckstring_agg_by_k"').fetchone()[0] == 2
    publish_fact(fact, rec, e2, ts(2))
    sink_run(revived, rec, ts(2), ts(1))  # incremental (previous_f=ts(1), fully covered)
    assert read_out(rec) == want == [("A", 1, 10), ("B", 2, 1)]
    fact_ref.close(), sink_ref.close(), fact.close(), revived.close()


def test_executor_recovers_lost_registry_but_not_wipe(tmp_path):
    """The Duck executor hydrates ONLY when the registry file is missing (storage loss). A Refresh's
    wipe empties the file in place — the recovery trigger must not undo it."""
    from duckstring.duck.executor import RippleExecutor

    # a minimal deployed source tree (the executor reads pond.toml for [sources])
    src_dir = tmp_path / "sources" / "o"
    (src_dir / "src").mkdir(parents=True)
    (src_dir / "pond.toml").write_text('[pond]\nname = "o"\nversion = "1.0.0"\n')
    (src_dir / "src" / "pond.py").write_text("")

    # publish state for pond o@m1 from a throwaway registry
    data_dir = tmp_path / "ponds" / "o" / "m1" / "data"
    con = _con()
    T.merge_table(con, "m", con.sql("SELECT 1 AS id, 'a' AS v"), ts(1), ("id",))
    ParquetDataPlane().export(con, data_dir, f=ts(1))
    con.close()

    def registry_tables(ex):
        return {r[0] for r in ex._registry.execute(
            "SELECT table_name FROM duckdb_tables() WHERE schema_name='main'").fetchall()}

    # 1. registry file missing → hydrated
    ex = RippleExecutor("o", 1, "1.0.0", "sources/o", tmp_path)
    assert "m__changelog" in registry_tables(ex)
    assert T.read_meta(ex._registry).get("m", {}).get("mode") == "merge"

    # 2. wipe empties it; a SECOND executor over the (existing, now-empty) file must NOT re-hydrate
    ex.wipe()
    assert "m__changelog" not in registry_tables(ex)
    ex.shutdown()
    ex2 = RippleExecutor("o", 1, "1.0.0", "sources/o", tmp_path)
    assert registry_tables(ex2) == set()
    ex2.shutdown()
