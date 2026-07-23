from __future__ import annotations

from duckstring.cli import app


def _seed(root, pond: str, table: str):
    """Write an exported Parquet snapshot at ponds/{pond}/data/{table}.parquet — the read-only data
    API serves from these, exactly as a real Pond's run export would produce."""
    import duckdb

    data_dir = root / "ponds" / pond / "m1" / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    dest = str(data_dir / f"{table}.parquet").replace("'", "''")
    con = duckdb.connect()
    con.execute(f"COPY (SELECT 1 AS id, 'a' AS val UNION ALL SELECT 2, 'b') TO '{dest}' (FORMAT PARQUET)")
    con.close()


# ── get ───────────────────────────────────────────────────────────────────────


def test_get_calls_correct_endpoint(runner, tmp_path, monkeypatch, catchment_root, live_catchment):
    _seed(catchment_root, "outlet", "daily")
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["get", "outlet", "daily"])
    assert result.exit_code == 0, result.output


def test_get_explicit_catchment(runner, tmp_path, monkeypatch, catchment_root, live_catchment):
    _seed(catchment_root, "outlet", "daily")
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["get", "outlet", "daily", "-c", "dev"])
    assert result.exit_code == 0, result.output


def test_get_writes_to_default_path(runner, tmp_path, monkeypatch, catchment_root, live_catchment):
    _seed(catchment_root, "outlet", "daily")
    monkeypatch.chdir(tmp_path)
    runner.invoke(app, ["get", "outlet", "daily"])
    out = tmp_path / "ponds" / "outlet" / "daily"
    assert out.exists()
    assert (out / "daily.parquet").exists()


def test_get_writes_to_custom_path(runner, tmp_path, monkeypatch, catchment_root, live_catchment):
    _seed(catchment_root, "outlet", "daily")
    monkeypatch.chdir(tmp_path)
    custom = tmp_path / "my_output"
    result = runner.invoke(app, ["get", "outlet", "daily", "--path", str(custom)])
    assert result.exit_code == 0, result.output
    assert (custom / "daily.parquet").exists()


def test_get_unknown_catchment_exits(runner):
    result = runner.invoke(app, ["get", "outlet", "daily", "-c", "nonexistent"])
    assert result.exit_code != 0


def test_get_missing_ripple_exits(runner, tmp_path, monkeypatch, live_catchment):
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["get", "ghost", "nothing"])
    assert result.exit_code != 0


# ── query ─────────────────────────────────────────────────────────────────────


def test_query_with_ripple_returns_rows(runner, catchment_root, live_catchment):
    _seed(catchment_root, "outlet", "daily")
    result = runner.invoke(app, ["query", "outlet", "daily"])
    assert result.exit_code == 0, result.output
    assert "id" in result.output


def test_query_explicit_catchment(runner, catchment_root, live_catchment):
    _seed(catchment_root, "outlet", "daily")
    result = runner.invoke(app, ["query", "outlet", "daily", "-c", "dev"])
    assert result.exit_code == 0, result.output


def test_query_custom_sql(runner, catchment_root, live_catchment):
    _seed(catchment_root, "outlet", "daily")
    result = runner.invoke(app, ["query", "outlet", "--sql", "SELECT 42 AS answer"])
    assert result.exit_code == 0, result.output
    assert "42" in result.output


def test_query_sql_from_file(runner, tmp_path, monkeypatch, catchment_root, live_catchment):
    _seed(catchment_root, "outlet", "daily")
    monkeypatch.chdir(tmp_path)
    sql_file = tmp_path / "query.sql"
    sql_file.write_text('SELECT * FROM "outlet"."daily" LIMIT 1')
    result = runner.invoke(app, ["query", "outlet", "--sql", "@query.sql"])
    assert result.exit_code == 0, result.output
    assert "id" in result.output


def test_query_missing_sql_file_exits(runner, tmp_path, monkeypatch, live_catchment):
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["query", "outlet", "--sql", "@nonexistent.sql"])
    assert result.exit_code != 0


def test_query_prints_table(runner, catchment_root, live_catchment):
    _seed(catchment_root, "outlet", "daily")
    result = runner.invoke(app, ["query", "outlet", "daily"])
    assert result.exit_code == 0, result.output
    assert "a" in result.output
    assert "b" in result.output


def test_query_empty_result(runner, catchment_root, live_catchment):
    _seed(catchment_root, "outlet", "daily")
    result = runner.invoke(app, ["query", "outlet", "--sql", 'SELECT * FROM "outlet"."daily" WHERE 1=0'])
    assert result.exit_code == 0
    assert "No results" in result.output


def test_query_csv_writes_file(runner, tmp_path, monkeypatch, catchment_root, live_catchment):
    _seed(catchment_root, "outlet", "daily")
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["query", "outlet", "daily", "--csv", "out.csv"])
    assert result.exit_code == 0, result.output
    out = tmp_path / "ponds" / "outlet" / "daily" / "out.csv"
    assert out.exists()
    assert b"id" in out.read_bytes()


def test_query_csv_custom_path(runner, tmp_path, monkeypatch, catchment_root, live_catchment):
    _seed(catchment_root, "outlet", "daily")
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["query", "outlet", "daily", "--csv", "out.csv", "--path", "."])
    assert result.exit_code == 0, result.output
    assert (tmp_path / "out.csv").exists()


def test_query_json_format(runner, tmp_path, monkeypatch, catchment_root, live_catchment):
    _seed(catchment_root, "outlet", "daily")
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["query", "outlet", "daily", "--json", "out.json"])
    assert result.exit_code == 0, result.output
    out = tmp_path / "ponds" / "outlet" / "daily" / "out.json"
    assert out.exists()


def test_query_unknown_catchment_exits(runner):
    result = runner.invoke(app, ["query", "outlet", "-c", "nonexistent"])
    assert result.exit_code != 0


def test_browse_con_caches_per_data_version(monkeypatch):
    """The merge browse cache materialises the reconstruction once (a local ``_ds_browse`` table), reuses
    the warm connection while the publish version is unchanged, and rebuilds when it changes."""
    import types

    import duckdb

    from duckstring.catchment.routes import data as datamod

    driver = types.SimpleNamespace(data_version=1)
    req = types.SimpleNamespace(app=types.SimpleNamespace(state=types.SimpleNamespace(driver=driver)))
    monkeypatch.setattr(datamod, "_open_pond", lambda request, pond, major: duckdb.connect())

    c1 = datamod._browse_con(req, "p", 1, "t", "SELECT 42 AS x")
    assert c1.execute("SELECT x FROM _ds_browse").fetchone()[0] == 42
    assert datamod._browse_con(req, "p", 1, "t", "SELECT 42 AS x") is c1  # warm reuse (same publish)
    driver.data_version = 2
    assert datamod._browse_con(req, "p", 1, "t", "SELECT 42 AS x") is not c1  # rebuilt on publish


# ── /api/query/page (the data viewer's paged read) ──────────────────────────────


def _seed_n(root, pond: str, table: str, n: int):
    """An exported Parquet snapshot of `n` rows (id 0..n-1) at ponds/{pond}/m1/data/{table}.parquet."""
    import duckdb

    data_dir = root / "ponds" / pond / "m1" / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    dest = str(data_dir / f"{table}.parquet").replace("'", "''")
    con = duckdb.connect()
    con.execute(f"COPY (SELECT i AS id, i * 10 AS v FROM range({n}) t(i)) TO '{dest}' (FORMAT PARQUET)")
    con.close()


def _serve_pond(client, name: str, tables: list[str]) -> None:
    """Register + schema-capture a pond line so the **catchment-wide serving core** (which the custom-``sql``
    branch of /query/page + /query/count now runs through) can see its published tables. A per-pond
    ``table`` browse reads the disk snapshot directly and needs none of this."""
    from duckstring.catchment.routes.deploy import _register

    db = client.app.state.db
    _register(db, name, "1.0.0", "outlet", f"ponds/{name}/1.0.0",
              {"sources": {}, "immediate_retries": 0, "source_retries": 0, "kind": "outlet", "serve_tables": tables},
              [{"func": "f", "name": "r", "parents": []}])
    pv = db.execute("SELECT pv.id FROM pond_version pv JOIN pond_name pn ON pn.id = pv.pond_name_id "
                    "WHERE pn.name = ? AND pv.version = '1.0.0'", (name,)).fetchone()[0]
    for t in tables:
        db.execute('INSERT OR IGNORE INTO pond_version_schema (pond_version_id, "table", "column", type) '
                   "VALUES (?, ?, 'id', 'INTEGER')", (pv, t))
    db.commit()
    client.app.state.driver.reload()


def test_query_page_paginates_with_has_more(catchment_client, tmp_path):
    _seed_n(tmp_path, "outlet", "daily", 5)
    r = catchment_client.post("/api/query/page", json={"pond": "outlet", "table": "daily", "limit": 2, "offset": 0})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["columns"] == ["id", "v"]
    assert body["rows"] == [[0, 0], [1, 10]]
    assert body["has_more"] is True

    # Last page: one row left, no more.
    r2 = catchment_client.post("/api/query/page", json={"pond": "outlet", "table": "daily", "limit": 2, "offset": 4})
    body2 = r2.json()
    assert body2["rows"] == [[4, 40]]
    assert body2["has_more"] is False


def test_query_page_wraps_custom_sql(catchment_client, tmp_path):
    _seed_n(tmp_path, "outlet", "daily", 10)
    _serve_pond(catchment_client, "outlet", ["daily"])
    # A custom query with its own LIMIT — the page wraps it as a subquery, so the user's cap still
    # bounds the result while the page reads within it.
    r = catchment_client.post(
        "/api/query/page",
        json={"pond": "outlet", "sql": 'SELECT id FROM "outlet"."daily" LIMIT 3', "limit": 2, "offset": 0},
    )
    body = r.json()
    assert body["columns"] == ["id"]
    assert body["rows"] == [[0], [1]]
    assert body["has_more"] is True  # one more within the LIMIT 3
    r2 = catchment_client.post(
        "/api/query/page",
        json={"pond": "outlet", "sql": 'SELECT id FROM "outlet"."daily" LIMIT 3', "limit": 2, "offset": 2},
    )
    assert r2.json()["rows"] == [[2]]
    assert r2.json()["has_more"] is False


def _seed_typed(root, pond, table):
    """An exported table with FLOAT/DOUBLE/DECIMAL columns for predicate-pushdown regression coverage."""
    import duckdb

    data_dir = root / "ponds" / pond / "m1" / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    dest = str(data_dir / f"{table}.parquet").replace("'", "''")
    con = duckdb.connect()
    con.execute(
        f"COPY (SELECT i AS id, (i*1.5)::FLOAT AS f_real, (i*1.5)::DOUBLE AS f_dbl, "
        f"(i*1.5)::DECIMAL(10,2) AS f_dec FROM range(20) t(i)) TO '{dest}' (FORMAT PARQUET)"
    )
    con.close()


def test_less_than_predicate_on_floats(catchment_client, tmp_path, monkeypatch):
    # Regression: a `<` predicate on FLOAT/DOUBLE/DECIMAL must return rows (not silently zero) through
    # both /query/count and the wrapped /query/page — and agree with each other.
    monkeypatch.setenv("DUCKSTRING_DATA_PLANE", "parquet")
    _seed_typed(tmp_path, "p", "t")
    _serve_pond(catchment_client, "p", ["t"])
    for col in ("f_real", "f_dbl", "f_dec"):
        sql = f'SELECT * FROM "p"."t" WHERE {col} < 5 LIMIT 1000'
        count = catchment_client.post("/api/query/count", json={"pond": "p", "sql": sql}).json()["count"]
        page = catchment_client.post("/api/query/page", json={"pond": "p", "sql": sql, "limit": 500}).json()
        assert count == 4, f"{col} < 5 count"
        assert len(page["rows"]) == 4, f"{col} < 5 page"


def test_query_page_bad_sql_is_400(catchment_client, tmp_path):
    _seed_n(tmp_path, "outlet", "daily", 1)
    r = catchment_client.post("/api/query/page", json={"pond": "outlet", "sql": "SELECT nope FROM missing"})
    assert r.status_code == 400


def test_query_page_order_by_column(catchment_client, tmp_path):
    _seed_n(tmp_path, "outlet", "daily", 5)  # rows id 0..4 in ascending scan order
    page = catchment_client.post(
        "/api/query/page", json={"pond": "outlet", "table": "daily", "order_by": "id", "order_desc": True, "limit": 100}
    ).json()
    idx = {c: i for i, c in enumerate(page["columns"])}
    assert [r[idx["id"]] for r in page["rows"]] == [4, 3, 2, 1, 0]


def test_query_page_order_by_unknown_column_400(catchment_client, tmp_path):
    _seed_n(tmp_path, "outlet", "daily", 2)
    r = catchment_client.post("/api/query/page", json={"pond": "outlet", "table": "daily", "order_by": "nope"})
    assert r.status_code == 400


def test_list_pond_tables(catchment_client, tmp_path):
    _seed_n(tmp_path, "outlet", "daily", 1)
    _seed_n(tmp_path, "outlet", "hourly", 1)
    r = catchment_client.get("/api/ponds/outlet/tables")
    assert r.status_code == 200
    tables = r.json()["tables"]
    assert sorted(t["name"] for t in tables) == ["daily", "hourly"]
    # Plain (non-Trickle) tables carry no trickle mode / pk.
    assert all(t["trickle"] is None and t["pk"] == [] for t in tables)


def test_list_pond_tables_empty(catchment_client, tmp_path):
    # An unknown pond with no exported data resolves to no tables (no error).
    (tmp_path / "ponds" / "ghost" / "m1" / "data").mkdir(parents=True)
    assert catchment_client.get("/api/ponds/ghost/tables").json()["tables"] == []


def test_query_count_table_and_sql(catchment_client, tmp_path):
    _seed_n(tmp_path, "outlet", "daily", 7)
    _serve_pond(catchment_client, "outlet", ["daily"])
    assert catchment_client.post("/api/query/count", json={"pond": "outlet", "table": "daily"}).json()["count"] == 7
    # A custom query's count reflects its own shape (here a LIMIT). Custom sql runs through the serving core.
    r = catchment_client.post(
        "/api/query/count", json={"pond": "outlet", "sql": 'SELECT * FROM "outlet"."daily" LIMIT 3'}
    )
    assert r.json()["count"] == 3


def test_query_count_bad_sql_is_400(catchment_client, tmp_path):
    _seed_n(tmp_path, "outlet", "daily", 1)
    r = catchment_client.post("/api/query/count", json={"pond": "outlet", "sql": "SELECT * FROM missing"})
    assert r.status_code == 400


# ── Trickle browse (merge consolidation + freshness window + history) ───────────


def _seed_merge_trickle(root, pond, table, pk, runs):
    """Build a real merge Trickle (main + Z-set changelog + sidecar) by replaying `runs`, each a
    ``(f_iso, [(id, name, price), ...])`` complete-state snapshot, then export the flat Parquet."""
    from datetime import datetime

    import duckdb

    from duckstring import trickle_io
    from duckstring.dataplane import ParquetDataPlane

    data_dir = root / "ponds" / pond / "m1" / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    for f_iso, rows in runs:
        con.execute("CREATE OR REPLACE TEMP TABLE _state(id BIGINT, name VARCHAR, price DOUBLE)")
        con.executemany("INSERT INTO _state VALUES (?, ?, ?)", rows)
        trickle_io.merge_table(con, table, con.sql("SELECT * FROM _state"), datetime.fromisoformat(f_iso), pk)
    ParquetDataPlane().export(con, data_dir)
    con.close()


# id1 updated, id2 deleted, id3 added at f2; id4 untouched since the f1 bootstrap.
_MERGE_RUNS = [
    ("2026-01-01T00:00:00+00:00", [(1, "a", 10.0), (2, "b", 20.0), (4, "d", 40.0)]),
    ("2026-01-02T00:00:00+00:00", [(1, "a", 15.0), (3, "c", 30.0), (4, "d", 40.0)]),
]


def test_tables_flags_trickle_mode_and_pk(catchment_client, tmp_path, monkeypatch):
    monkeypatch.setenv("DUCKSTRING_DATA_PLANE", "parquet")
    _seed_merge_trickle(tmp_path, "p", "priced", ("id",), _MERGE_RUNS)
    by = {t["name"]: t for t in catchment_client.get("/api/ponds/p/tables").json()["tables"]}
    assert by["priced"]["trickle"] == "merge" and by["priced"]["pk"] == ["id"]
    # The changelog companion stays a plain table — raw-navigable, unchanged.
    assert by["priced__changelog"]["trickle"] is None


def test_freshness_lists_incremental_runs(catchment_client, tmp_path, monkeypatch):
    monkeypatch.setenv("DUCKSTRING_DATA_PLANE", "parquet")
    _seed_merge_trickle(tmp_path, "p", "priced", ("id",), _MERGE_RUNS)
    fr = catchment_client.get("/api/ponds/p/freshness?table=priced").json()
    # The main is log-structured, so the bootstrap (f1) writes the changelog too — both runs show
    # (newest first); floor anchors at f1.
    assert len(fr["freshness"]) == 2
    assert fr["freshness"][0].startswith("2026-01-02")
    assert fr["freshness"][1].startswith("2026-01-01")
    assert fr["floor"].startswith("2026-01-01")


def _rows_by_id(page):
    idx = {c: i for i, c in enumerate(page["columns"])}
    return idx, {r[idx["id"]]: r for r in page["rows"]}


def test_merge_consolidation_full_state(catchment_client, tmp_path, monkeypatch):
    monkeypatch.setenv("DUCKSTRING_DATA_PLANE", "parquet")
    _seed_merge_trickle(tmp_path, "p", "priced", ("id",), _MERGE_RUNS)
    page = catchment_client.post(
        "/api/query/page", json={"pond": "p", "table": "priced", "trickle": "merge", "pk": ["id"], "limit": 100}
    ).json()
    idx, by = _rows_by_id(page)
    assert {"_duckstring_active", "_duckstring_updates", "_duckstring_f"} <= set(page["columns"])
    # id1 updated (active), id3 inserted (active), id4 untouched since bootstrap (active), id2 deleted.
    assert [by[i][idx["_duckstring_active"]] for i in (1, 3, 4)] == [1, 1, 1]
    assert by[2][idx["_duckstring_active"]] == -1
    assert by[2][idx["name"]] == "b"  # the deleted row's last image survives for display
    # The bootstrap create counts as a +1 write event: id1 was created (f1) then updated (f2) → 2; id4 was
    # only created → 1.
    assert by[1][idx["_duckstring_updates"]] == 2 and by[4][idx["_duckstring_updates"]] == 1
    # id4 carries its own last-write freshness (the bootstrap, f1) — every row has a freshness from the main.
    assert by[4][idx["_duckstring_f"]] is not None and by[4][idx["_duckstring_f"]].startswith("2026-01-01")


def test_merge_default_is_scan_order_all_rows(catchment_client, tmp_path, monkeypatch):
    # The default page is the cheap scan order (no ORDER BY) so the LIMIT pushes down to the Parquet scan —
    # a big merge main is never force-sorted per page. Every current + deleted row is still present.
    monkeypatch.setenv("DUCKSTRING_DATA_PLANE", "parquet")
    _seed_merge_trickle(tmp_path, "p", "priced", ("id",), _MERGE_RUNS)
    page = catchment_client.post(
        "/api/query/page", json={"pond": "p", "table": "priced", "trickle": "merge", "pk": ["id"], "limit": 100}
    ).json()
    idx = {c: i for i, c in enumerate(page["columns"])}
    assert {r[idx["id"]] for r in page["rows"]} == {1, 2, 3, 4}  # all rows (incl. the deleted id2), any order


def test_merge_order_by_pk_is_opt_in(catchment_client, tmp_path, monkeypatch):
    # PK order is available on demand (clicking the header) — only then is the full-scan sort paid.
    monkeypatch.setenv("DUCKSTRING_DATA_PLANE", "parquet")
    _seed_merge_trickle(tmp_path, "p", "priced", ("id",), _MERGE_RUNS)
    page = catchment_client.post(
        "/api/query/page",
        json={"pond": "p", "table": "priced", "trickle": "merge", "pk": ["id"], "order_by": "id", "limit": 100},
    ).json()
    idx = {c: i for i, c in enumerate(page["columns"])}
    ids = [r[idx["id"]] for r in page["rows"]]
    assert ids == [1, 2, 3, 4]  # ascending PK (incl. the deleted id2)


def test_merge_order_by_freshness(catchment_client, tmp_path, monkeypatch):
    # Opt-in: the consolidated view can be re-sorted by any column, including freshness.
    monkeypatch.setenv("DUCKSTRING_DATA_PLANE", "parquet")
    _seed_merge_trickle(tmp_path, "p", "priced", ("id",), _MERGE_RUNS)
    page = catchment_client.post(
        "/api/query/page",
        json={"pond": "p", "table": "priced", "trickle": "merge", "pk": ["id"],
              "order_by": "_duckstring_f", "order_desc": True, "limit": 100},
    ).json()
    idx = {c: i for i, c in enumerate(page["columns"])}
    fs = [r[idx["_duckstring_f"]] for r in page["rows"]]
    assert fs == sorted(fs, reverse=True)
    assert page["rows"][-1][idx["id"]] == 4  # id4 at the floor freshness sorts last


def test_merge_window_shows_only_changed_records(catchment_client, tmp_path, monkeypatch):
    monkeypatch.setenv("DUCKSTRING_DATA_PLANE", "parquet")
    _seed_merge_trickle(tmp_path, "p", "priced", ("id",), _MERGE_RUNS)
    f2 = catchment_client.get("/api/ponds/p/freshness?table=priced").json()["freshness"][0]
    page = catchment_client.post(
        "/api/query/page",
        json={"pond": "p", "table": "priced", "trickle": "merge", "pk": ["id"], "f_lo": f2, "f_hi": f2, "limit": 100},
    ).json()
    _, by = _rows_by_id(page)
    assert set(by) == {1, 2, 3}  # id4 untouched in this run → excluded


def test_merge_count_matches(catchment_client, tmp_path, monkeypatch):
    monkeypatch.setenv("DUCKSTRING_DATA_PLANE", "parquet")
    _seed_merge_trickle(tmp_path, "p", "priced", ("id",), _MERGE_RUNS)
    n = catchment_client.post(
        "/api/query/count", json={"pond": "p", "table": "priced", "trickle": "merge", "pk": ["id"]}
    ).json()["count"]
    assert n == 4  # 3 active + 1 deleted


def test_merge_count_fast_path_matches_checkpointed(catchment_client, tmp_path, monkeypatch):
    """The fast count (base metadata + changelog net weight + tombstones) must equal the page's total even
    once the main is checkpointed (a real f_base, with retained pre-base changelog history)."""
    from datetime import datetime

    import duckdb

    from duckstring import trickle_io
    from duckstring.dataplane import ParquetDataPlane

    monkeypatch.setenv("DUCKSTRING_DATA_PLANE", "parquet")
    monkeypatch.setenv("DUCKSTRING_COMPACT_THRESHOLD", "1024")  # tiny → force a checkpoint (real base + f_base)
    data_dir = tmp_path / "ponds" / "p" / "m1" / "data"
    data_dir.mkdir(parents=True)
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    dp = ParquetDataPlane()
    runs = [
        ("2026-01-01T00:00:00+00:00", [(1, 10), (2, 20), (3, 30), (4, 40)]),
        ("2026-01-02T00:00:00+00:00", [(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]),  # +insert 5 → checkpoint
        ("2026-01-03T00:00:00+00:00", [(1, 11), (2, 20), (3, 30), (4, 40), (5, 50)]),  # update 1
        ("2026-01-04T00:00:00+00:00", [(1, 11), (3, 30), (4, 40), (5, 50)]),           # delete 2
    ]
    for f_iso, rows in runs:
        con.execute("CREATE OR REPLACE TEMP TABLE _s(id BIGINT, v BIGINT)")
        con.executemany("INSERT INTO _s VALUES (?, ?)", rows)
        f = datetime.fromisoformat(f_iso)
        trickle_io.merge_table(con, "t", con.sql("SELECT * FROM _s"), f, ("id",), retain_n=10**9)
        dp.export(con, data_dir, f=f)
    con.close()
    assert trickle_io.load_sidecar(data_dir).get("t", {}).get("f_base")  # actually checkpointed

    body = {"pond": "p", "table": "t", "trickle": "merge", "pk": ["id"]}
    count = catchment_client.post("/api/query/count", json=body).json()["count"]
    page = catchment_client.post("/api/query/page", json={**body, "limit": 1000}).json()
    idx = {c: i for i, c in enumerate(page["columns"])}
    # The fast count (base metadata + changelog net weight + tombstones) must equal the rows the page shows.
    assert count == len(page["rows"])
    active = {r[idx["id"]] for r in page["rows"] if r[idx["_duckstring_active"]] > 0}
    assert active == {1, 3, 4, 5}  # current state (id2 deleted)


def _seed_append_trickle(root, pond, table, runs):
    """Build an append Trickle (insert-only history + sidecar) by appending each run's rows."""
    from datetime import datetime

    import duckdb

    from duckstring import trickle_io
    from duckstring.dataplane import ParquetDataPlane

    data_dir = root / "ponds" / pond / "m1" / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    for f_iso, rows in runs:
        con.execute("CREATE OR REPLACE TEMP TABLE _e(id BIGINT, amt DOUBLE)")
        con.executemany("INSERT INTO _e VALUES (?, ?)", rows)
        trickle_io.append_table(con, table, con.sql("SELECT * FROM _e"), datetime.fromisoformat(f_iso), ("id",))
    ParquetDataPlane().export(con, data_dir)
    con.close()


def test_append_window_filters_by_freshness(catchment_client, tmp_path, monkeypatch):
    monkeypatch.setenv("DUCKSTRING_DATA_PLANE", "parquet")
    _seed_append_trickle(tmp_path, "p", "orders", [
        ("2026-01-01T00:00:00+00:00", [(1, 1.0), (2, 2.0)]),
        ("2026-01-02T00:00:00+00:00", [(3, 3.0)]),
    ])
    by = {t["name"]: t for t in catchment_client.get("/api/ponds/p/tables").json()["tables"]}
    assert by["orders"]["trickle"] == "append"
    # Append history records every run's freshness (no bootstrap exclusion).
    fr = catchment_client.get("/api/ponds/p/freshness?table=orders").json()["freshness"]
    assert len(fr) == 2
    # Full history = 3 rows; windowed to the latest run = 1 row.
    full = catchment_client.post(
        "/api/query/count", json={"pond": "p", "table": "orders", "trickle": "append", "pk": ["id"]}
    ).json()["count"]
    assert full == 3
    f2 = fr[0]
    win = catchment_client.post(
        "/api/query/count",
        json={"pond": "p", "table": "orders", "trickle": "append", "pk": ["id"], "f_lo": f2, "f_hi": f2},
    ).json()["count"]
    assert win == 1


def test_merge_row_history(catchment_client, tmp_path, monkeypatch):
    monkeypatch.setenv("DUCKSTRING_DATA_PLANE", "parquet")
    _seed_merge_trickle(tmp_path, "p", "priced", ("id",), _MERGE_RUNS)
    # id1 was bootstrap-created (10.0) then updated at f2 (15.0). The update is shown, and the original
    # bootstrap image is surfaced as a synthetic 'create' at the bottom (from the oldest -1).
    hist = catchment_client.post(
        "/api/query/history", json={"pond": "p", "table": "priced", "pk": {"id": 1}}
    ).json()
    idx = {c: i for i, c in enumerate(hist["columns"])}
    assert "_duckstring_d" not in hist["columns"]  # collapsed to an event label, not raw weights
    assert [r[idx["_duckstring_event"]] for r in hist["rows"]] == ["update", "create"]
    assert hist["rows"][0][idx["price"]] == 15.0  # the surviving (+1) image
    assert hist["rows"][-1][idx["price"]] == 10.0  # the recovered original (bottom)
    assert hist["rows"][-1][idx["_duckstring_f"]].startswith("2026-01-01")  # at the floor freshness


def test_merge_history_create_and_delete_events(catchment_client, tmp_path, monkeypatch):
    monkeypatch.setenv("DUCKSTRING_DATA_PLANE", "parquet")
    _seed_merge_trickle(tmp_path, "p", "priced", ("id",), _MERGE_RUNS)
    idx_of = lambda h: {c: i for i, c in enumerate(h["columns"])}  # noqa: E731
    # id3 was inserted at f2 → 'create'.
    h3 = catchment_client.post("/api/query/history", json={"pond": "p", "table": "priced", "pk": {"id": 3}}).json()
    assert [r[idx_of(h3)["_duckstring_event"]] for r in h3["rows"]] == ["create"]
    # id2 was created at bootstrap (f1) then deleted at f2 → 'delete' over 'create' (newest first), the
    # delete showing the retracted image.
    h2 = catchment_client.post("/api/query/history", json={"pond": "p", "table": "priced", "pk": {"id": 2}}).json()
    i2 = idx_of(h2)
    assert [r[i2["_duckstring_event"]] for r in h2["rows"]] == ["delete", "create"]
    assert h2["rows"][0][i2["name"]] == "b"
