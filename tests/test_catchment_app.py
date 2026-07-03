from __future__ import annotations

import io
import sqlite3
import sys
import zipfile
from pathlib import Path

import pytest

from duckstring.catchment.db import connect, migrate

pytestmark = pytest.mark.timeout(5)

_DEMO_DIR = Path(__file__).parent.parent / "src" / "duckstring" / "demo"


# ---------------------------------------------------------------------------
# DB unit tests — no HTTP layer
# ---------------------------------------------------------------------------

def test_migrate_creates_tables(tmp_path):
    con = connect(tmp_path / "duck.db")
    migrate(con)
    tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"pond_name", "pond", "pond_version", "ripple", "ripple_to_ripple", "pond_to_pond",
            "pond_state", "pond_target", "pond_window", "pond_trigger", "pond_run", "ripple_run"} <= tables


def test_migrate_is_idempotent(tmp_path):
    con = connect(tmp_path / "duck.db")
    migrate(con)
    migrate(con)  # second call must not raise or duplicate data
    versions = [r[0] for r in con.execute("SELECT version FROM schema_migrations ORDER BY version")]
    assert versions == sorted(set(versions))


def test_foreign_keys_enforced(tmp_path):
    con = connect(tmp_path / "duck.db")
    migrate(con)
    with pytest.raises(sqlite3.IntegrityError):
        con.execute("INSERT INTO pond_version (pond_name_id, version, major, source_path) VALUES (999, '1.0.0', 1, 'x')")
        con.commit()


# ---------------------------------------------------------------------------
# HTTP layer tests — use catchment_client fixture from conftest
# ---------------------------------------------------------------------------

def test_health(catchment_client):
    r = catchment_client.get("/api/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_db_available_on_app_state(catchment_client):
    assert catchment_client.app.state.db is not None


def test_root_dir_on_app_state(catchment_client, tmp_path):
    # tmp_path is the same fixture instance shared within the test,
    # but catchment_client uses its own tmp_path injection — just check type.
    assert catchment_client.app.state.root.is_dir()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_pond_zip(*, toml_text: str, pond_py_text: str = "") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("pond.toml", toml_text)
        if pond_py_text:
            zf.writestr("src/pond.py", pond_py_text)
    return buf.getvalue()


def _deploy(client, *, name: str, version: str, kind: str, toml_text: str, pond_py_text: str = ""):
    archive = make_pond_zip(toml_text=toml_text, pond_py_text=pond_py_text)
    return client.post(
        "/api/deploy",
        files={"pond": ("pond.zip", archive, "application/zip")},
        data={"name": name, "version": version, "type": kind},
    )


def _db(catchment_client):
    return catchment_client.app.state.db


def _seed(catchment_client, pond: str, ripple: str):
    """Seed an exported Parquet snapshot (two rows) — the read-only data API serves from these."""
    import duckdb
    data_dir = catchment_client.app.state.root / "ponds" / pond / "m1" / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    dest = str(data_dir / f"{ripple}.parquet").replace("'", "''")
    con = duckdb.connect()
    con.execute(f"COPY (SELECT 1 AS id, 'a' AS val UNION ALL SELECT 2, 'b') TO '{dest}' (FORMAT PARQUET)")
    con.close()


# ---------------------------------------------------------------------------
# Deploy tests
# ---------------------------------------------------------------------------

INLET_TOML = """\
[pond]
name = "inlet"
version = "1.0.0"
type = "inlet"
"""

POND_TOML = """\
[pond]
name = "mypond"
version = "1.0.0"

[sources]
inlet = "1.0.0"
"""

OUTLET_TOML = """\
[pond]
name = "outlet"
version = "2.0.0"
type = "outlet"

[sources]
mypond = "1.0.0"
upstream = "2.0.0?"
"""

POND_PY_TWO_RIPPLES = """\
from duckstring import ripple

@ripple
def load(pond): ...

@ripple(parents=[load])
def clean(pond): ...
"""


def _selected_version(db, name):
    row = db.execute(
        "SELECT pv.version FROM pond p JOIN pond_version pv ON pv.id = p.pond_version_id "
        "JOIN pond_name pn ON pn.id = p.pond_name_id WHERE pn.name = ?", (name,)
    ).fetchall()
    return {r[0] for r in row}


def test_deploy_local_registers_pond(catchment_client):
    r = _deploy(catchment_client, name="inlet", version="1.0.0", kind="inlet", toml_text=INLET_TOML)
    assert r.status_code == 200
    row = _db(catchment_client).execute("SELECT name, kind FROM pond_name WHERE name = 'inlet'").fetchone()
    assert row == ("inlet", "inlet")


def test_deploy_local_registers_version(catchment_client):
    r = _deploy(catchment_client, name="inlet", version="1.0.0", kind="inlet", toml_text=INLET_TOML)
    assert r.status_code == 200
    db = _db(catchment_client)
    row = db.execute("SELECT version, major, source_path FROM pond_version WHERE version = '1.0.0'").fetchone()
    assert row == ("1.0.0", 1, "ponds/inlet/1.0.0")
    assert _selected_version(db, "inlet") == {"1.0.0"}  # the pond pointer selects it


def test_deploy_local_registers_sources(catchment_client):
    _deploy(catchment_client, name="inlet", version="1.0.0", kind="inlet", toml_text=INLET_TOML)
    r = _deploy(catchment_client, name="mypond", version="1.0.0", kind="pond", toml_text=POND_TOML)
    assert r.status_code == 200

    db = _db(catchment_client)
    (pond_id,) = db.execute(
        "SELECT p.id FROM pond p JOIN pond_name pn ON pn.id = p.pond_name_id WHERE pn.name = 'mypond'"
    ).fetchone()
    edges = db.execute(
        "SELECT src.name, e.source_major, e.min_version, e.required "
        "FROM pond_to_pond e JOIN pond_name src ON src.id = e.source_pond_name_id WHERE e.pond_id = ?",
        (pond_id,),
    ).fetchall()
    assert edges == [("inlet", 1, "1.0.0", 1)]


def test_deploy_local_required_and_optional_sources(catchment_client):
    r = _deploy(catchment_client, name="outlet", version="2.0.0", kind="outlet", toml_text=OUTLET_TOML)
    assert r.status_code == 200

    db = _db(catchment_client)
    (pond_id,) = db.execute(
        "SELECT p.id FROM pond p JOIN pond_name pn ON pn.id = p.pond_name_id WHERE pn.name = 'outlet'"
    ).fetchone()
    edges = {
        row[0]: row[1]
        for row in db.execute(
            "SELECT src.name, e.required FROM pond_to_pond e "
            "JOIN pond_name src ON src.id = e.source_pond_name_id WHERE e.pond_id = ?",
            (pond_id,),
        ).fetchall()
    }
    assert edges["mypond"] == 1    # required
    assert edges["upstream"] == 0  # optional (?)


def test_deploy_local_registers_ripples(catchment_client):
    r = _deploy(
        catchment_client,
        name="inlet", version="1.0.0", kind="inlet",
        toml_text=INLET_TOML, pond_py_text=POND_PY_TWO_RIPPLES,
    )
    assert r.status_code == 200

    db = _db(catchment_client)
    (version_id,) = db.execute(
        "SELECT pv.id FROM pond_version pv JOIN pond_name pn ON pn.id = pv.pond_name_id WHERE pn.name = 'inlet'"
    ).fetchone()
    ripples = {r[0] for r in db.execute("SELECT name FROM ripple WHERE pond_version_id = ?", (version_id,)).fetchall()}
    assert ripples == {"load", "clean"}


def test_deploy_local_registers_ripple_edges(catchment_client):
    r = _deploy(
        catchment_client,
        name="inlet", version="1.0.0", kind="inlet",
        toml_text=INLET_TOML, pond_py_text=POND_PY_TWO_RIPPLES,
    )
    assert r.status_code == 200

    db = _db(catchment_client)
    (version_id,) = db.execute(
        "SELECT pv.id FROM pond_version pv JOIN pond_name pn ON pn.id = pv.pond_name_id WHERE pn.name = 'inlet'"
    ).fetchone()
    edges = db.execute(
        """SELECT sink.name, src.name FROM ripple_to_ripple e
           JOIN ripple sink ON sink.id = e.sink_id
           JOIN ripple src  ON src.id  = e.source_id
           WHERE sink.pond_version_id = ?""",
        (version_id,),
    ).fetchall()
    assert edges == [("clean", "load")]


def test_deploy_selects_new_version(catchment_client):
    _deploy(catchment_client, name="inlet", version="1.0.0", kind="inlet", toml_text=INLET_TOML)
    new_toml = INLET_TOML.replace("1.0.0", "1.1.0")
    _deploy(catchment_client, name="inlet", version="1.1.0", kind="inlet", toml_text=new_toml)

    db = _db(catchment_client)
    assert _selected_version(db, "inlet") == {"1.1.0"}  # pointer now selects the new version
    assert {r[0] for r in db.execute("SELECT version FROM pond_version")} == {"1.0.0", "1.1.0"}


def test_redeploy_same_version_keeps_run_history(catchment_client):
    """Redeploying an already-known version (e.g. re-activating a retired line) must preserve its run
    history — a pond_version is an immutable artifact. Regression: the old path nuked pond_run/ripple_run."""
    _deploy(catchment_client, name="inlet", version="1.0.0", kind="inlet",
            toml_text=INLET_TOML, pond_py_text=POND_PY_TWO_RIPPLES)
    db = _db(catchment_client)
    (pv_id,) = db.execute(
        "SELECT pv.id FROM pond_version pv JOIN pond_name pn ON pn.id = pv.pond_name_id WHERE pn.name = 'inlet'"
    ).fetchone()
    (load_id,) = db.execute("SELECT id FROM ripple WHERE pond_version_id = ? AND name = 'load'", (pv_id,)).fetchone()
    with db:
        db.execute("INSERT INTO pond_run (pond_version_id, f, started_at, status) VALUES (?, ?, ?, 'success')",
                   (pv_id, "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:00+00:00"))
        db.execute("INSERT INTO ripple_run (ripple_id, pond_version_id, f, retry, status) VALUES (?, ?, ?, 0, 'success')",
                   (load_id, pv_id, "2026-01-01T00:00:00+00:00"))

    # Redeploy the identical artifact — history survives and the pond_version id is unchanged.
    _deploy(catchment_client, name="inlet", version="1.0.0", kind="inlet",
            toml_text=INLET_TOML, pond_py_text=POND_PY_TWO_RIPPLES)
    assert db.execute("SELECT count(*) FROM pond_run WHERE pond_version_id = ?", (pv_id,)).fetchone()[0] == 1
    assert db.execute("SELECT count(*) FROM ripple_run WHERE pond_version_id = ?", (pv_id,)).fetchone()[0] == 1


def test_redeploy_changed_topology_keeps_pond_runs(catchment_client):
    """A genuine topology change under the same version must rewrite the ripple rows (ripple_run goes with
    them, FK), but the Pond's own run history (pond_run, keyed on the version) is still kept."""
    _deploy(catchment_client, name="inlet", version="1.0.0", kind="inlet",
            toml_text=INLET_TOML, pond_py_text=POND_PY_TWO_RIPPLES)
    db = _db(catchment_client)
    (pv_id,) = db.execute(
        "SELECT pv.id FROM pond_version pv JOIN pond_name pn ON pn.id = pv.pond_name_id WHERE pn.name = 'inlet'"
    ).fetchone()
    with db:
        db.execute("INSERT INTO pond_run (pond_version_id, f, started_at, status) VALUES (?, ?, ?, 'success')",
                   (pv_id, "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:00+00:00"))

    # Redeploy the same version with a different intra-Pond graph (drop the edge).
    one_ripple = "from duckstring import ripple\n\n@ripple\ndef load(pond): ...\n"
    _deploy(catchment_client, name="inlet", version="1.0.0", kind="inlet",
            toml_text=INLET_TOML, pond_py_text=one_ripple)
    assert {r[0] for r in db.execute("SELECT name FROM ripple WHERE pond_version_id = ?", (pv_id,))} == {"load"}
    assert db.execute("SELECT count(*) FROM pond_run WHERE pond_version_id = ?", (pv_id,)).fetchone()[0] == 1


def test_deploy_two_majors_both_selected(catchment_client):
    _deploy(catchment_client, name="inlet", version="1.0.0", kind="inlet", toml_text=INLET_TOML)
    v2_toml = INLET_TOML.replace("1.0.0", "2.0.0")
    _deploy(catchment_client, name="inlet", version="2.0.0", kind="inlet", toml_text=v2_toml)

    db = _db(catchment_client)
    # One selected Pond per major line — both majors are selected.
    assert _selected_version(db, "inlet") == {"1.0.0", "2.0.0"}


def test_deploy_bad_zip_returns_422(catchment_client):
    r = catchment_client.post(
        "/api/deploy",
        files={"pond": ("pond.zip", b"not a zip", "application/zip")},
        data={"name": "x", "version": "1.0.0", "type": "pond"},
    )
    assert r.status_code == 422


def test_deploy_cycle_returns_422(catchment_client):
    # a → b → a is a cycle; deploy of b (which declares a as source) should be rejected
    # if a already declares b as its source.
    _deploy(catchment_client, name="a", version="1.0.0", kind="pond",
            toml_text='[pond]\nname="a"\nversion="1.0.0"\n\n[sources]\nb="1.0.0"\n')
    r = _deploy(catchment_client, name="b", version="1.0.0", kind="pond",
                toml_text='[pond]\nname="b"\nversion="1.0.0"\n\n[sources]\na="1.0.0"\n')
    assert r.status_code == 422
    assert "Cycle" in r.json()["detail"]

    # b's version must not have been registered (transaction rolled back)
    db = _db(catchment_client)
    assert db.execute(
        "SELECT COUNT(*) FROM pond_version pv JOIN pond_name pn ON pn.id = pv.pond_name_id WHERE pn.name = 'b'"
    ).fetchone()[0] == 0


# ---------------------------------------------------------------------------
# Data — query tests
# ---------------------------------------------------------------------------

def test_query_returns_rows(catchment_client):
    _seed(catchment_client, "outlet", "daily")
    r = catchment_client.post("/api/query", json={"pond": "outlet", "ripple": "daily"})
    assert r.status_code == 200
    rows = r.json()
    assert isinstance(rows, list)
    assert len(rows) == 2
    assert rows[0]["id"] == 1


def test_query_custom_sql(catchment_client):
    _seed(catchment_client, "outlet", "daily")
    r = catchment_client.post("/api/query", json={"pond": "outlet", "sql": 'SELECT * FROM "outlet"."daily" WHERE id = 1'})
    assert r.status_code == 200
    assert len(r.json()) == 1


def test_query_csv_format(catchment_client):
    _seed(catchment_client, "outlet", "daily")
    r = catchment_client.post("/api/query", json={"pond": "outlet", "ripple": "daily", "format": "csv"})
    assert r.status_code == 200
    assert b"id" in r.content
    assert b"val" in r.content


def test_query_parquet_format(catchment_client):
    _seed(catchment_client, "outlet", "daily")
    r = catchment_client.post("/api/query", json={"pond": "outlet", "ripple": "daily", "format": "parquet"})
    assert r.status_code == 200
    # Parquet magic bytes
    assert r.content[:4] == b"PAR1"


def test_query_unknown_pond_returns_404(catchment_client):
    r = catchment_client.post("/api/query", json={"pond": "ghost", "ripple": "nothing"})
    assert r.status_code == 404


def test_query_missing_relation_returns_400(catchment_client):
    _seed(catchment_client, "outlet", "daily")
    r = catchment_client.post("/api/query", json={"pond": "outlet", "ripple": "nothing"})
    assert r.status_code == 400


# ---------------------------------------------------------------------------
# Data — get ripple tests
# ---------------------------------------------------------------------------

def test_data_get_returns_zip(catchment_client):
    _seed(catchment_client, "outlet", "daily")
    r = catchment_client.get("/api/ponds/outlet/ripples/daily")
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/zip"
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        assert "daily.parquet" in zf.namelist()


def test_data_get_missing_returns_404(catchment_client):
    r = catchment_client.get("/api/ponds/ghost/ripples/nothing")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Demo pond deployment
# ---------------------------------------------------------------------------

def _read_toml(path: Path) -> dict:
    if sys.version_info >= (3, 11):
        import tomllib
        return tomllib.loads(path.read_text(encoding="utf-8"))
    import tomli
    return tomli.loads(path.read_text(encoding="utf-8"))


def _zip_dir(path: Path) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in sorted(path.rglob("*")):
            if f.is_file():
                zf.write(f, f.relative_to(path))
    return buf.getvalue()


def _deploy_demo(client, name: str):
    pond_dir = _DEMO_DIR / name
    info = _read_toml(pond_dir / "pond.toml")["pond"]
    return client.post(
        "/api/deploy",
        files={"pond": ("pond.zip", _zip_dir(pond_dir), "application/zip")},
        data={"name": info["name"], "version": info["version"], "type": info.get("type", "pond")},
    )


def test_deploy_demo_ponds(catchment_client):
    for name in ("transactions", "products", "sales", "reports"):
        r = _deploy_demo(catchment_client, name)
        assert r.status_code == 200, f"Deploy of {name} failed: {r.text}"

    db = _db(catchment_client)

    # All four ponds registered and selected
    selected = {
        row[0]
        for row in db.execute(
            "SELECT pn.name FROM pond p JOIN pond_name pn ON pn.id = p.pond_name_id"
        ).fetchall()
    }
    assert selected == {"transactions", "products", "sales", "reports"}

    # Inter-pond source edges
    edges = {
        (row[0], row[1])
        for row in db.execute(
            """SELECT snk.name, src.name
               FROM pond_to_pond e
               JOIN pond p ON p.id = e.pond_id
               JOIN pond_name snk ON snk.id = p.pond_name_id
               JOIN pond_name src ON src.id = e.source_pond_name_id"""
        ).fetchall()
    }
    assert ("sales", "transactions") in edges
    assert ("sales", "products") in edges
    assert ("reports", "sales") in edges

    # Ripples registered for each pond
    for pond_name, expected in [
        ("transactions", {"ingest"}),
        ("products", {"ingest"}),
        ("sales", {"daily_sales", "price_tiers", "join_lines"}),
        ("reports", {"monthly_summary"}),
    ]:
        ripples = {
            row[0]
            for row in db.execute(
                """SELECT r.name FROM ripple r
                   JOIN pond_version pv ON pv.id = r.pond_version_id
                   JOIN pond_name pn ON pn.id = pv.pond_name_id
                   WHERE pn.name = ?""",
                (pond_name,),
            ).fetchall()
        }
        assert ripples == expected, f"Wrong ripples for {pond_name}: {ripples}"


# ---------------------------------------------------------------------------
# Wave tests
# ---------------------------------------------------------------------------

def test_wave_unknown_outlet_404(catchment_client):
    r = catchment_client.post("/api/ponds/nonexistent/wave")
    assert r.status_code == 404


def _trigger_row(db, name):
    return db.execute(
        "SELECT pt.kind, pt.bound_ms, pt.status FROM pond_trigger pt "
        "JOIN pond_name pn ON pn.id = (SELECT pond_name_id FROM pond WHERE id = pt.pond_id) "
        "WHERE pn.name = ?", (name,)
    ).fetchall()


def test_wave_registers_trigger(catchment_client):
    _deploy(catchment_client, name="outlet", version="1.0.0", kind="outlet", toml_text=OUTLET_TOML.split("[sources]")[0])
    r = catchment_client.post("/api/ponds/outlet/wave")
    assert r.status_code == 200
    rows = _trigger_row(_db(catchment_client), "outlet")
    assert rows == [("wave", None, "active")]


def test_wave_idempotent(catchment_client):
    _deploy(catchment_client, name="outlet", version="1.0.0", kind="outlet", toml_text=OUTLET_TOML.split("[sources]")[0])
    catchment_client.post("/api/ponds/outlet/wave")
    catchment_client.post("/api/ponds/outlet/wave")
    assert len(_trigger_row(_db(catchment_client), "outlet")) == 1  # single upserted trigger


# ---------------------------------------------------------------------------
# Tide tests (staleness bound, not cron)
# ---------------------------------------------------------------------------

OUTLET_ONLY_TOML = """\
[pond]
name = "outlet"
version = "2.0.0"
type = "outlet"
"""


def test_tide_unknown_outlet_404(catchment_client):
    r = catchment_client.post("/api/ponds/nonexistent/tide", json={"bound_seconds": 60})
    assert r.status_code == 404


def test_tide_invalid_bound_422(catchment_client):
    _deploy(catchment_client, name="outlet", version="2.0.0", kind="outlet", toml_text=OUTLET_ONLY_TOML)
    r = catchment_client.post("/api/ponds/outlet/tide", json={"bound_seconds": 0})
    assert r.status_code == 422


def test_tide_registers_bound(catchment_client):
    _deploy(catchment_client, name="outlet", version="2.0.0", kind="outlet", toml_text=OUTLET_ONLY_TOML)
    r = catchment_client.post("/api/ponds/outlet/tide", json={"bound_seconds": 3600})
    assert r.status_code == 200
    assert _trigger_row(_db(catchment_client), "outlet") == [("tide", 3_600_000, "active")]


def test_tide_updates_existing_bound(catchment_client):
    _deploy(catchment_client, name="outlet", version="2.0.0", kind="outlet", toml_text=OUTLET_ONLY_TOML)
    catchment_client.post("/api/ponds/outlet/tide", json={"bound_seconds": 3600})
    catchment_client.post("/api/ponds/outlet/tide", json={"bound_seconds": 30})
    assert _trigger_row(_db(catchment_client), "outlet") == [("tide", 30_000, "active")]


def test_stop_cancels_standing_trigger(catchment_client):
    _deploy(catchment_client, name="outlet", version="2.0.0", kind="outlet", toml_text=OUTLET_ONLY_TOML)
    catchment_client.post("/api/ponds/outlet/wave")
    assert _trigger_row(_db(catchment_client), "outlet") == [("wave", None, "active")]
    r = catchment_client.post("/api/ponds/outlet/sleep")
    assert r.status_code == 200
    assert _trigger_row(_db(catchment_client), "outlet") == []


def test_untrigger_removes_standing_trigger(catchment_client):
    _deploy(catchment_client, name="outlet", version="2.0.0", kind="outlet", toml_text=OUTLET_ONLY_TOML)
    catchment_client.post("/api/ponds/outlet/tide", json={"bound_seconds": 3600})
    assert _trigger_row(_db(catchment_client), "outlet") == [("tide", 3_600_000, "active")]
    r = catchment_client.post("/api/ponds/outlet/untrigger")
    assert r.status_code == 200
    assert _trigger_row(_db(catchment_client), "outlet") == []


def test_untrigger_unknown_outlet_404(catchment_client):
    r = catchment_client.post("/api/ponds/nonexistent/untrigger")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Failure management: retry budgets + clear
# ---------------------------------------------------------------------------

RETRY_TOML = """\
[pond]
name = "outlet"
version = "2.0.0"
type = "outlet"
immediate_retries = 2
source_retries = 1
"""


def _pond_status(client, name):
    ponds = client.get("/api/status").json()["ponds"]
    return next(p for p in ponds if p["name"] == name)


def test_deploy_seeds_retry_budget_from_toml(catchment_client):
    _deploy(catchment_client, name="outlet", version="2.0.0", kind="outlet", toml_text=RETRY_TOML)
    got = catchment_client.get("/api/ponds/outlet/budget").json()
    assert got == {"immediate_retries": 2, "source_retries": 1}


def test_budget_set_and_get(catchment_client):
    _deploy(catchment_client, name="outlet", version="2.0.0", kind="outlet", toml_text=OUTLET_ONLY_TOML)
    assert catchment_client.get("/api/ponds/outlet/budget").json() == {"immediate_retries": 0, "source_retries": 0}
    r = catchment_client.post("/api/ponds/outlet/budget", json={"immediate_retries": 3, "source_retries": 2})
    assert r.status_code == 200
    assert catchment_client.get("/api/ponds/outlet/budget").json() == {"immediate_retries": 3, "source_retries": 2}
    # status surfaces the live budgets too
    assert _pond_status(catchment_client, "outlet")["source_retries"] == 2


def test_budget_negative_422(catchment_client):
    _deploy(catchment_client, name="outlet", version="2.0.0", kind="outlet", toml_text=OUTLET_ONLY_TOML)
    r = catchment_client.post("/api/ponds/outlet/budget", json={"immediate_retries": -1, "source_retries": 0})
    assert r.status_code == 422


def test_clear_unknown_404(catchment_client):
    assert catchment_client.post("/api/ponds/nonexistent/clear").status_code == 404


def test_failed_event_marks_pond_then_clears(catchment_client):
    _deploy(catchment_client, name="outlet", version="2.0.0", kind="outlet",
            toml_text=OUTLET_ONLY_TOML, pond_py_text=POND_PY_TWO_RIPPLES)
    # The Duck reports a Ripple gave up: the Pond fails (and, with no Sinks, just blocks itself).
    r = catchment_client.post(
        "/api/duck/outlet/2/events",
        json={"kind": "failed", "ripple": "load", "f": "2026-01-01T00:00:00+00:00", "status": "failed"},
    )
    assert r.status_code == 200
    st = _pond_status(catchment_client, "outlet")
    assert st["status"] == "failed" and st["is_failed"] and st["is_blocked"] and st["failures"] == 1

    # The failure survives a reload (persisted to pond_state)...
    catchment_client.app.state.driver.reload()
    assert _pond_status(catchment_client, "outlet")["is_failed"]

    # ...and an operator clear resets it.
    assert catchment_client.post("/api/ponds/outlet/clear").status_code == 200
    st = _pond_status(catchment_client, "outlet")
    assert not st["is_failed"] and not st["is_blocked"] and st["failures"] == 0


def test_kill_endpoint_parks_pond(catchment_client):
    _deploy(catchment_client, name="outlet", version="2.0.0", kind="outlet",
            toml_text=OUTLET_ONLY_TOML, pond_py_text=POND_PY_TWO_RIPPLES)
    catchment_client.post("/api/ponds/outlet/wake")  # a Run is in flight
    assert catchment_client.post("/api/ponds/outlet/kill").status_code == 200
    st = _pond_status(catchment_client, "outlet")
    assert st["is_killed"] and st["status"] == "killed"
    # Clear lifts the kill.
    catchment_client.post("/api/ponds/outlet/clear")
    assert not _pond_status(catchment_client, "outlet")["is_killed"]


def test_failed_ripple_error_surfaced_in_history(catchment_client):
    # A failed Ripple's error message reaches run history (per-Ripple and at the Pond Run level) so
    # the UI can show it.
    _deploy(catchment_client, name="outlet", version="2.0.0", kind="outlet",
            toml_text=OUTLET_ONLY_TOML, pond_py_text=POND_PY_TWO_RIPPLES)
    catchment_client.post("/api/ponds/outlet/wake")  # a Run in flight
    f = catchment_client.get("/api/runs?pond=outlet&lineage=false").json()["runs"][0]["f"]
    catchment_client.post("/api/duck/outlet/2/events", json={
        "kind": "failed", "ripple": "load", "f": f, "status": "failed", "retry": 0,
        "error": "ValueError: boom", "traceback": "Traceback (most recent call last):\n  ...\nValueError: boom",
    })
    run = next(r for r in catchment_client.get("/api/runs?pond=outlet&ripples=true&lineage=false").json()["runs"]
               if r["f"] == f)
    assert run["error"] == "ValueError: boom" and run["traceback"].startswith("Traceback")
    load = next(rr for rr in run["ripples"] if rr["ripple"] == "load")
    assert load["error"] == "ValueError: boom" and load["traceback"].startswith("Traceback")


def test_pond_failed_event_fails_whole_pond(catchment_client):
    # A Duck-level error (reported as a `pond_failed` event) fails the whole Pond at its in-flight Run.
    _deploy(catchment_client, name="outlet", version="2.0.0", kind="outlet",
            toml_text=OUTLET_ONLY_TOML, pond_py_text=POND_PY_TWO_RIPPLES)
    catchment_client.post("/api/ponds/outlet/wake")  # puts a Run in flight
    r = catchment_client.post(
        "/api/duck/outlet/2/events",
        json={"kind": "pond_failed", "f": "2026-01-01T00:00:00+00:00", "status": "failed"},
    )
    assert r.status_code == 200
    st = _pond_status(catchment_client, "outlet")
    assert st["is_failed"] and st["is_blocked"] and st["failures"] == 1


def test_ripple_retry_trace_recorded(catchment_client):
    # Each attempt of a Ripple lands as its own ripple_run row (keyed on retry) and run history
    # returns the full trace. (start creates the pond_run the Duck's events key off.)
    _deploy(catchment_client, name="outlet", version="2.0.0", kind="outlet",
            toml_text=OUTLET_ONLY_TOML, pond_py_text=POND_PY_TWO_RIPPLES)
    catchment_client.post("/api/ponds/outlet/wake")
    runs = catchment_client.get("/api/runs?pond=outlet&lineage=false").json()["runs"]
    assert runs, "start should create a Pond Run"
    f = runs[0]["f"]
    # attempt 0 errored (immediate retry), attempt 1 succeeded.
    catchment_client.post("/api/duck/outlet/2/events",
                          json={"kind": "ripple", "ripple": "load", "f": f, "status": "failed", "retry": 0})
    catchment_client.post("/api/duck/outlet/2/events",
                          json={"kind": "ripple", "ripple": "load", "f": f, "status": "success", "retry": 1})
    runs = catchment_client.get("/api/runs?pond=outlet&ripples=true&lineage=false").json()["runs"]
    load_rows = [rr for run in runs if run["f"] == f for rr in run["ripples"] if rr["ripple"] == "load"]
    assert [(rr["retry"], rr["status"]) for rr in load_rows] == [(0, "failed"), (1, "success")]


def test_redeploy_same_version_after_run_history(catchment_client):
    # A failed Run leaves ripple_run/pond_run history; redeploying the same version rewrites the
    # topology and must clear that history first (else the ripple DELETE hits ripple_run's FK).
    _deploy(catchment_client, name="outlet", version="2.0.0", kind="outlet",
            toml_text=OUTLET_ONLY_TOML, pond_py_text=POND_PY_TWO_RIPPLES)
    catchment_client.post(
        "/api/duck/outlet/2/events",
        json={"kind": "failed", "ripple": "load", "f": "2026-01-01T00:00:00+00:00", "status": "failed"},
    )
    assert _pond_status(catchment_client, "outlet")["is_failed"]
    r = _deploy(catchment_client, name="outlet", version="2.0.0", kind="outlet",
                toml_text=OUTLET_ONLY_TOML, pond_py_text=POND_PY_TWO_RIPPLES)
    assert r.status_code == 200
    # Redeploying the (fixed) artifact auto-clears the failure — no manual `control clear` needed.
    assert not _pond_status(catchment_client, "outlet")["is_failed"]
