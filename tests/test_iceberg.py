"""The Iceberg data-plane backend (Phase 1 of plans/data-plane-iceberg.md): pyiceberg overwrite
commits with an ``f``-stamped snapshot per run, DuckDB ``iceberg_scan`` reads, the as-of resolver, the
reserved-namespace guard, and the flat-Parquet sidecar + legacy fallback. Only *overwrite* tables go to
Iceberg; append-only Trickle tables (append history, ``__changelog``, ``__droplog``) and a merge main
base are served from the flat per-run parts layer (Iceberg metadata would grow O(runs) for them).
Skipped without the extra."""

from __future__ import annotations

from datetime import datetime, timezone

import duckdb
import pytest

pytest.importorskip("pyiceberg")  # the only data-plane dep; SQLAlchemy is deliberately NOT required

from duckstring.dataplane import ReservedColumnError, get_data_plane  # noqa: E402
from duckstring.iceberg_plane import F_PROP, IcebergDataPlane  # noqa: E402

UTC = timezone.utc


def _con(sql: str):
    con = duckdb.connect()
    con.execute(sql)
    return con


def test_roundtrip_write_then_read(tmp_path):
    dp = IcebergDataPlane()
    con = _con("CREATE TABLE event AS SELECT * FROM (VALUES (1,'a'),(2,'b')) t(id,val)")
    dp.export(con, tmp_path, f=datetime(2026, 6, 16, tzinfo=UTC))

    assert dp.list_tables(tmp_path) == ["event"]
    rcon = duckdb.connect()
    dp.prepare(rcon)
    assert sorted(rcon.sql(dp.read_select(tmp_path, "event")).fetchall()) == [(1, "a"), (2, "b")]


def test_flat_parquet_sidecar_and_catalog_written(tmp_path):
    dp = IcebergDataPlane()
    dp.export(_con("CREATE TABLE event AS SELECT 1 AS id"), tmp_path, f=datetime(2026, 6, 16, tzinfo=UTC))
    # The compat sidecar (for draws / direct-serve / fallback) and the per-line catalog both exist.
    assert (tmp_path / "event.parquet").exists()
    assert (tmp_path / "catalog.json").exists()
    assert dp.table_path(tmp_path, "event") == tmp_path / "event.parquet"


def test_one_snapshot_per_run_stamped_with_f(tmp_path):
    dp = IcebergDataPlane()
    con = _con("CREATE TABLE event AS SELECT 1 AS id")
    dp.export(con, tmp_path, f=datetime(2026, 6, 16, 0, tzinfo=UTC))
    con.execute("DROP TABLE event; CREATE TABLE event AS SELECT 2 AS id")
    dp.export(con, tmp_path, f=datetime(2026, 6, 16, 1, tzinfo=UTC))

    tbl = dp._load(tmp_path, "event")
    stamps = {
        s.summary.additional_properties.get(F_PROP)
        for s in tbl.snapshots() if s.summary
    }
    assert "2026-06-16T00:00:00+00:00" in stamps
    assert "2026-06-16T01:00:00+00:00" in stamps


def _data_files(tmp_path, table):
    import os

    d = tmp_path / "pond" / table / "data"
    return len(os.listdir(d)) if d.is_dir() else 0


def _flat_parts(tmp_path, table):
    """The append-only flat per-run part files of ``table`` (the layer that serves it now)."""
    d = tmp_path / table
    return sorted(p.name for p in d.glob("*.parquet")) if d.is_dir() else []


def test_prune_bounds_overwrite_data_files_keeping_latest(tmp_path, monkeypatch):
    """An overwrite table (merge main / plain Ripple) rewrites a full data file each run; the prune keeps
    only the most-recent N snapshots and reclaims the superseded data files — so disk stays bounded while
    the latest state still reads."""
    monkeypatch.setenv("DUCKSTRING_ICEBERG_KEEP_SNAPSHOTS", "2")
    dp = IcebergDataPlane()
    con = _con("CREATE TABLE t AS SELECT 0 AS id, 0 AS v")
    for k in range(8):
        con.execute(f"CREATE OR REPLACE TABLE t AS SELECT i AS id, {k} AS v FROM range(50) x(i)")
        dp.export(con, tmp_path, f=datetime(2026, 6, 16, k, tzinfo=UTC))

    assert _data_files(tmp_path, "t") <= 3, "overwrite data files not reclaimed"
    rcon = duckdb.connect()
    dp.prepare(rcon)
    assert rcon.sql(f"SELECT DISTINCT v FROM ({dp.read_select(tmp_path, 't')})").fetchall() == [(7,)]


def test_append_history_served_from_flat_parts_not_iceberg(tmp_path, monkeypatch):
    """An append history is NOT committed to Iceberg (its current snapshot would reference every appended
    file, so the metadata grows O(runs) — unbounded for no read benefit). It is served from the flat
    per-run parts the sidecar export writes (one O(change) part per run), and the full history still reads."""
    from duckstring import trickle_io as T

    monkeypatch.setenv("DUCKSTRING_ICEBERG_KEEP_SNAPSHOTS", "2")
    dp = IcebergDataPlane()
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    for k in range(6):
        batch = con.sql(f"SELECT {k} * 10 + i AS id, {k} AS v FROM range(10) x(i)")
        T.append_table(con, "hist", batch, datetime(2026, 6, 16, k, tzinfo=UTC), ("id",))
        dp.export(con, tmp_path, f=datetime(2026, 6, 16, k, tzinfo=UTC))

    assert dp._load(tmp_path, "hist") is None, "append history must not be committed to Iceberg"
    assert len(_flat_parts(tmp_path, "hist")) == 6, "one flat part per run"
    rcon = duckdb.connect()
    dp.prepare(rcon)
    assert rcon.sql(f"SELECT count(*) FROM ({dp.read_select(tmp_path, 'hist')})").fetchone() == (60,)


def test_merge_export_survives_warm_fold_then_checkpoint(tmp_path, monkeypatch):
    """A merge main's warm band (``__band``) must never be handed to the Iceberg commit loop. It is dropped
    when a checkpoint folds it into the cold base; if a run both holds a warm band (from an earlier fold) and
    checkpoints, the commit loop would otherwise reference the now-gone ``__band`` table and crash. Drive many
    small merge runs at a tiny compact threshold so folds + checkpoints straddle runs, and assert every export
    succeeds and the reconstructed state stays correct."""
    from duckstring import trickle_io as T

    monkeypatch.setenv("DUCKSTRING_COMPACT_THRESHOLD", "1")  # tiny → eager warm folds + cold compactions
    dp = IcebergDataPlane()
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    for k in range(8):
        state = con.sql(f"SELECT i AS id, '{k}'||i AS v FROM range(50) x(i)")  # full-rewrite each run
        T.merge_table(con, "dim", state, datetime(2026, 6, 16, k, tzinfo=UTC), ("id",))
        dp.export(con, tmp_path, f=datetime(2026, 6, 16, k, tzinfo=UTC))  # must not raise

    assert dp._load(tmp_path, "dim__band") is None, "a warm band must never be committed to Iceberg"
    rcon = duckdb.connect()
    dp.prepare(rcon)
    rcon.execute("SET TimeZone='UTC'")
    assert rcon.sql(f"SELECT count(*) FROM ({dp.read_select(tmp_path, 'dim')})").fetchone() == (50,)


def test_as_of_reads_the_snapshot_for_that_freshness(tmp_path):
    dp = IcebergDataPlane()
    con = _con("CREATE TABLE event AS SELECT * FROM (VALUES (1,'a'),(2,'b')) t(id,val)")
    dp.export(con, tmp_path, f=datetime(2026, 6, 16, 0, tzinfo=UTC))
    con.execute("DROP TABLE event; CREATE TABLE event AS SELECT * FROM (VALUES (9,'z')) t(id,val)")
    dp.export(con, tmp_path, f=datetime(2026, 6, 16, 1, tzinfo=UTC))

    rcon = duckdb.connect()
    dp.prepare(rcon)
    latest = dp.read_select(tmp_path, "event")
    as_of = dp.read_select(tmp_path, "event", as_of=datetime(2026, 6, 16, 0, 30, tzinfo=UTC))
    assert sorted(rcon.sql(latest).fetchall()) == [(9, "z")]
    assert sorted(rcon.sql(as_of).fetchall()) == [(1, "a"), (2, "b")]


def test_reserved_namespace_rejected_before_any_commit(tmp_path):
    dp = IcebergDataPlane()
    con = _con("CREATE TABLE event AS SELECT 1 AS id, 2 AS _duckstring_f")
    with pytest.raises(ReservedColumnError):
        dp.export(con, tmp_path, f=datetime(2026, 6, 16, tzinfo=UTC))
    assert dp._load(tmp_path, "event") is None  # nothing committed


def test_fallback_to_flat_parquet_when_no_catalog(tmp_path):
    # A Source published by the legacy Parquet plane (no Iceberg catalog) is still readable.
    duckdb.sql("SELECT 7 AS id").write_parquet(str(tmp_path / "event.parquet"))
    dp = IcebergDataPlane()
    rcon = duckdb.connect()
    dp.prepare(rcon)
    assert rcon.sql(dp.read_select(tmp_path, "event")).fetchall() == [(7,)]


def test_schema_change_recreates_and_keeps_exporting(tmp_path):
    dp = IcebergDataPlane()
    con = _con("CREATE TABLE event AS SELECT 1 AS id")
    dp.export(con, tmp_path, f=datetime(2026, 6, 16, 0, tzinfo=UTC))
    con.execute("DROP TABLE event; CREATE TABLE event AS SELECT 1 AS id, 'x' AS extra")
    dp.export(con, tmp_path, f=datetime(2026, 6, 16, 1, tzinfo=UTC))  # additive change must not break export
    rcon = duckdb.connect()
    dp.prepare(rcon)
    assert rcon.sql(dp.read_select(tmp_path, "event")).fetchall() == [(1, "x")]


def test_get_data_plane_iceberg_via_env(monkeypatch):
    monkeypatch.setenv("DUCKSTRING_DATA_PLANE", "iceberg")
    assert isinstance(get_data_plane(), IcebergDataPlane)


def test_pond_read_table_foreign_under_iceberg(tmp_path, monkeypatch):
    monkeypatch.setenv("DUCKSTRING_DATA_PLANE", "iceberg")
    from duckstring.core import Pond

    src_data = tmp_path / "ponds" / "src" / "m1" / "data"
    src_data.mkdir(parents=True)
    get_data_plane().export(
        _con("CREATE TABLE event AS SELECT * FROM (VALUES (1,'a')) t(id,val)"),
        src_data, f=datetime(2026, 6, 16, tzinfo=UTC),
    )

    rcon = duckdb.connect()
    pond = Pond("snk", "1.0.0", rcon, root=tmp_path, source_majors={"src": 1})
    rel = pond.read_table("src.event")
    assert rel.fetchall() == [(1, "a")]
    # The registered view (referenced by table name) reads the Iceberg snapshot.
    assert rcon.sql("SELECT val FROM event WHERE id = 1").fetchall() == [("a",)]


def test_read_table_pins_to_run_freshness_not_too_fresh(tmp_path, monkeypatch):
    # A Pond Run spans wall-clock time; an upstream Source can republish mid-run. read_table must pin to
    # the run's freshness `f` (the as-of snapshot), never read a too-fresh republish (intra-run read skew).
    monkeypatch.setenv("DUCKSTRING_DATA_PLANE", "iceberg")
    from duckstring.core import Pond

    src_data = tmp_path / "ponds" / "src" / "m1" / "data"
    src_data.mkdir(parents=True)
    dp = get_data_plane()
    dp.export(_con("CREATE TABLE event AS SELECT * FROM (VALUES (1,'a')) t(id,val)"),
              src_data, f=datetime(2026, 6, 16, tzinfo=UTC))
    # The Source republishes a fresher snapshot partway through the sink's run.
    dp.export(_con("CREATE TABLE event AS SELECT * FROM (VALUES (1,'b')) t(id,val)"),
              src_data, f=datetime(2026, 6, 18, tzinfo=UTC))

    def _read_at(f):
        pond = Pond("snk", "1.0.0", duckdb.connect(), root=tmp_path, source_majors={"src": 1}, f=f)
        return pond.read_table("src.event").fetchall()

    # A run pinned between the two publishes sees the old snapshot, not the too-fresh 'b'.
    assert _read_at(datetime(2026, 6, 17, tzinfo=UTC)) == [(1, "a")]
    # A run at/after the republish sees the new snapshot.
    assert _read_at(datetime(2026, 6, 18, tzinfo=UTC)) == [(1, "b")]


def test_trickle_append_published_as_flat_parts_and_window_reads(tmp_path):
    # An append Trickle is published as flat per-run parts (O(change), not an Iceberg commit), and the
    # window read returns only the rows in (previous_f, f].
    from duckstring import trickle_io as T

    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    dp = IcebergDataPlane()

    def run(idval, hour):
        T.append_table(con, "event", con.sql(f"SELECT {idval} AS id"),
                       datetime(2026, 6, 16, hour, tzinfo=UTC), ("id",))
        dp.export(con, tmp_path, f=datetime(2026, 6, 16, hour, tzinfo=UTC))

    run(1, 1)
    run(2, 2)
    run(3, 3)

    # Three runs → three flat parts (one _duckstring_f-homogeneous file each), not an Iceberg table.
    assert dp._load(tmp_path, "event") is None
    assert len(_flat_parts(tmp_path, "event")) == 3
    # The sidecar travelled, so a fresh reader resolves the append mode and windows correctly.
    assert T.load_sidecar(tmp_path)["event"]["mode"] == "append"
    rcon = duckdb.connect()
    dp.prepare(rcon)
    d = T.read_delta(rcon, tmp_path, "event",
                     previous_f=datetime(2026, 6, 16, 1, tzinfo=UTC),
                     f=datetime(2026, 6, 16, 3, tzinfo=UTC), dp=dp)
    assert sorted(d.upserts.fetchall()) == [(2,), (3,)]  # excludes run 1, includes run 3


def test_append_droplog_published_as_flat_parts(tmp_path):
    # A builder .append(fail_on_conflict=False)'s `{name}__droplog` companion is an append-only table, so
    # it publishes as flat per-run parts (not an Iceberg commit) — and the main stays frozen on a dropped
    # conflict (the past is never rewritten).
    from duckstring import trickle_io as T

    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    dp = IcebergDataPlane()
    dp.prepare(con)

    def z(rows):
        vals = ", ".join(f"({i}, '{v}', {d})" for i, v, d in rows)
        return con.sql(f"SELECT * FROM (VALUES {vals}) t(id, v, _duckstring_d)")

    T.append_zset(con, "enriched", z([(1, "a", 1), (2, "b", 1)]), datetime(2026, 6, 16, 1, tzinfo=UTC), ("id",))
    dp.export(con, tmp_path, f=datetime(2026, 6, 16, 1, tzinfo=UTC))
    # Run 2: id 1 changes (retraction + conflicting insert → dropped, history frozen) and id 3 is new.
    T.append_zset(con, "enriched", z([(1, "a", -1), (1, "A", 1), (3, "c", 1)]),
                  datetime(2026, 6, 16, 2, tzinfo=UTC), ("id",), fail_on_conflict=False)
    dp.export(con, tmp_path, f=datetime(2026, 6, 16, 2, tzinfo=UTC))

    rcon = duckdb.connect()
    dp.prepare(rcon)
    main = sorted(rcon.sql(f"SELECT id, v FROM ({dp.read_select(tmp_path, 'enriched')})").fetchall())
    assert main == [(1, "a"), (2, "b"), (3, "c")]  # id 1 stayed 'a' (the past was not rewritten)
    drops = sorted(rcon.sql(f"SELECT id, v, _duckstring_d FROM ({dp.read_select(tmp_path, 'enriched__droplog')})").fetchall())
    assert drops == [(1, "A", 1), (1, "a", -1)]
    # The droplog is served from flat parts (one per run that dropped), never committed to Iceberg.
    assert dp._load(tmp_path, "enriched__droplog") is None
    assert len(_flat_parts(tmp_path, "enriched__droplog")) == 1  # only run 2 dropped
    assert "enriched__droplog" not in T.load_sidecar(tmp_path)  # companion, no sidecar entry


def test_trickle_merge_main_overwrite_changelog_append_over_iceberg(tmp_path):
    # A merge Trickle: the clean main is overwritten (current state, no tombstones) while the changelog
    # grows by append — and a delta read collapses the changelog window per PK to the latest op.
    from duckstring import trickle_io as T

    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    dp = IcebergDataPlane()

    def state(rows, hour):
        vals = ", ".join(f"({i}, '{v}')" for i, v in rows)
        T.merge_table(con, "dim", con.sql(f"SELECT * FROM (VALUES {vals}) t(id, v)"),
                      datetime(2026, 6, 16, hour, tzinfo=UTC), ("id",))
        dp.export(con, tmp_path, f=datetime(2026, 6, 16, hour, tzinfo=UTC))

    state([(1, "a"), (2, "b")], 1)
    state([(1, "A")], 2)  # 1 updated, 2 deleted

    rcon = duckdb.connect()
    dp.prepare(rcon)
    # The main is the clean current state, system columns stripped.
    assert sorted(T._strip_system(rcon.sql(dp.read_select(tmp_path, "dim"))).fetchall()) == [(1, "A")]
    d = T.read_delta(rcon, tmp_path, "dim",
                     previous_f=datetime(2026, 6, 16, 1, tzinfo=UTC),
                     f=datetime(2026, 6, 16, 2, tzinfo=UTC), dp=dp)
    assert sorted(d.upserts.fetchall()) == [(1, "A")]
    assert d.deletes.fetchall() == [(2,)]


def test_trickle_retention_drops_expired_flat_parts(tmp_path):
    # Registry retention (retain_n) trims old changelog/history rows; the flat parts layer mirrors it by
    # dropping the parts the registry no longer retains, so it doesn't grow unbounded. Space-only — a reader
    # behind the floor still reads correctly via the full-read fallback.
    from duckstring import trickle_io as T

    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    dp = IcebergDataPlane()
    for h in (1, 2, 3, 4):
        T.append_table(con, "event", con.sql(f"SELECT {h} AS id"),
                       datetime(2026, 6, 16, h, tzinfo=UTC), ("id",), retain_n=2)
        dp.export(con, tmp_path, f=datetime(2026, 6, 16, h, tzinfo=UTC))

    assert dp._load(tmp_path, "event") is None
    assert len(_flat_parts(tmp_path, "event")) == 2  # newest 2 runs retained, expired parts dropped
    rcon = duckdb.connect()
    rcon.execute("SET TimeZone='UTC'")
    dp.prepare(rcon)
    assert sorted(r[0] for r in rcon.sql(dp.read_select(tmp_path, "event")).fetchall()) == [3, 4]


def test_trickle_iceberg_replay_appends_no_duplicates(tmp_path):
    # Flat parts are named by their run freshness and are immutable, so a replay at the same f (or a
    # re-export) overwrites the same part — it adds nothing, no duplicate rows.
    from duckstring import trickle_io as T

    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    dp = IcebergDataPlane()
    T.append_table(con, "event", con.sql("SELECT 1 AS id"), datetime(2026, 6, 16, 1, tzinfo=UTC), ("id",))
    dp.export(con, tmp_path, f=datetime(2026, 6, 16, 1, tzinfo=UTC))
    dp.export(con, tmp_path, f=datetime(2026, 6, 16, 1, tzinfo=UTC))  # replay / re-export at the same f

    rcon = duckdb.connect()
    rcon.execute("SET TimeZone='UTC'")
    dp.prepare(rcon)
    assert sorted(r[0] for r in rcon.sql(dp.read_select(tmp_path, "event")).fetchall()) == [1]  # no dup


def test_data_viewer_browse_and_history_on_iceberg(catchment_client, tmp_path):
    """Regression: the data viewer's merge browse and per-record history must work over the *Iceberg*
    plane. History collapses the changelog with a windowed self-join + a bound PK param; over an
    iceberg_scan that previously raised "IcebergScan serialization not implemented" (the analytic step
    is now run over a materialised temp table). Browse is the consolidated view; both must be 200."""
    from duckstring import trickle_io

    dp = IcebergDataPlane()
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    data_dir = tmp_path / "ponds" / "p" / "m1" / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    last = None
    for f_iso, rows in [
        ("2026-01-01T00:00:00+00:00", [(1, "a", 10.0), (2, "b", 20.0)]),
        ("2026-01-02T00:00:00+00:00", [(1, "a", 15.0), (3, "c", 30.0)]),  # id1 updated, id2 deleted, id3 added
    ]:
        con.execute("CREATE OR REPLACE TEMP TABLE _s(id BIGINT, name VARCHAR, price DOUBLE)")
        con.executemany("INSERT INTO _s VALUES (?, ?, ?)", rows)
        last = datetime.fromisoformat(f_iso)
        trickle_io.merge_table(con, "priced", con.sql("SELECT * FROM _s"), last, ("id",))
    dp.export(con, data_dir, f=last)
    con.close()

    # Browse (consolidated view) — the changelog scanned several times under joins/windows, no params.
    page = catchment_client.post(
        "/api/query/page", json={"pond": "p", "table": "priced", "trickle": "merge", "pk": ["id"], "limit": 100}
    )
    assert page.status_code == 200, page.text
    assert {r[page.json()["columns"].index("id")] for r in page.json()["rows"]} == {1, 2, 3}  # id4 absent

    # History (param + windowed self-join over iceberg_scan) — the path that used to 500.
    hist = catchment_client.post("/api/query/history", json={"pond": "p", "table": "priced", "pk": {"id": 1}})
    assert hist.status_code == 200, hist.text
    body = hist.json()
    idx = {c: i for i, c in enumerate(body["columns"])}
    # The f2 update, plus the original bootstrap image recovered as a synthetic 'create' at the bottom.
    assert [row[idx["_duckstring_event"]] for row in body["rows"]] == ["update", "create"]
    assert body["rows"][-1][idx["price"]] == 10.0
