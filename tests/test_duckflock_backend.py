"""DuckFlock execution routing (duckstring as the DuckFlock client SDK). The backend captures a
ripple's plan, quotes it (``duckflock quote``), and either runs it classically on duckstring's own
engine (``route: local``) or submits it to DuckFlock (``route: duckflock``). Every DuckFlock-side
failure degrades to classic local execution with a warning; with no endpoint configured the backend
is a no-op. Offline tests inject fake quote/submit seams; a gated test drives the real ``duckflock``
binary end-to-end (skipped unless ``DUCKFLOCK_BIN`` is set)."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

from duckstring.core import Pond
from duckstring.dataplane import ParquetDataPlane
from duckstring.duckflock_backend import DuckflockConfig, execute
from duckstring.engine.core import NEVER

UTC = timezone.utc


def ts(hour: int) -> datetime:
    return datetime(2026, 6, 16, hour, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _parquet_plane(monkeypatch):
    monkeypatch.setenv("DUCKSTRING_DATA_PLANE", "parquet")


def _publish_source(tmp_path, rows_sql, f, *, table="t", pond="src"):
    """Publish a merge Trickle source ``pond.table`` and return (its data dir, a source_catalog resolver)."""
    import duckstring.trickle_io as T

    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    T.merge_table(con, table, con.sql(rows_sql), f, ("id",))
    data_dir = tmp_path / "ponds" / pond / "m1" / "data"
    ParquetDataPlane().export(con, data_dir, f=f)
    con.close()

    def source_catalog(ref, *, stats=None):
        p, t = ref.split(".", 1)
        sc = T.load_sidecar(tmp_path / "ponds" / p / "m1" / "data").get(t, {})
        entry = {"location": str(tmp_path / "ponds" / p / "m1" / "data"), "mode": sc.get("mode"),
                 "pk": sc.get("pk"), "floor": sc.get("floor"), "f": sc.get("f")}
        if stats is not None:
            entry["stats"] = stats
        return entry

    return data_dir, source_catalog


def _sink(tmp_path, f, previous_f):
    con = duckdb.connect(str(tmp_path / "sink.duckdb"))
    con.execute("SET TimeZone='UTC'")
    return Pond("o", "1.0.0", con, root=tmp_path, source_majors={"src": 1}, f=f, previous_f=previous_f)


def _run_python(pond):
    out = pond.trickle("src.t").merge("out", pk="id")
    return [("out", out.was_changed())]


def _own_rows(pond, tmp_path):
    ParquetDataPlane().export(pond.con, tmp_path / "ponds" / "o" / "m1" / "data", f=pond.f)
    sql = ParquetDataPlane().read_select(tmp_path / "ponds" / "o" / "m1" / "data", "out")
    return sorted(pond.con.sql(f"SELECT id, v FROM ({sql})").fetchall())


# ─── the classic (disabled / fallback) path executes on duckstring's own engine ──


def test_disabled_config_runs_classic(tmp_path):
    _, source_catalog = _publish_source(tmp_path, "SELECT 1 AS id, 'a' AS v", ts(1))
    pond = _sink(tmp_path, ts(1), NEVER)

    out = execute(DuckflockConfig(), pond, _run_python, source_catalog=source_catalog)
    assert out.route == "classic"
    assert out.executed_locally
    assert out.ripple_result == [("out", True)]
    assert _own_rows(pond, tmp_path) == [(1, "a")]  # the ripple actually ran + published


def test_route_local_runs_classic(tmp_path):
    _, source_catalog = _publish_source(tmp_path, "SELECT 1 AS id, 'a' AS v", ts(1))
    pond = _sink(tmp_path, ts(1), NEVER)
    cfg = DuckflockConfig(endpoint="http://fake", tenant="acme")

    def quote_fn(plan, config):
        assert plan["duckflock_plan"] == 1 and plan["pond"]["name"] == "o"
        return {"route": "local", "why": "tiny", "chosen_modes": ["comprehensive"]}

    def submit_fn(plan, config):
        raise AssertionError("must not submit a locally-routed plan")

    out = execute(cfg, pond, _run_python, source_catalog=source_catalog, quote_fn=quote_fn, submit_fn=submit_fn)
    assert out.route == "local" and out.executed_locally
    assert out.quote["route"] == "local"
    assert _own_rows(pond, tmp_path) == [(1, "a")]


# ─── the duckflock route submits and surfaces the result, without local execution ──


def test_route_duckflock_submits_and_surfaces_result(tmp_path):
    _, source_catalog = _publish_source(tmp_path, "SELECT 1 AS id, 'a' AS v", ts(1))
    pond = _sink(tmp_path, ts(2), ts(1))
    cfg = DuckflockConfig(endpoint="http://fake", tenant="acme")
    seen = {}

    def quote_fn(plan, config):
        return {"route": "duckflock", "why": "big"}

    def submit_fn(plan, config):
        seen["job_id"] = plan["job_id"]
        seen["epoch"] = plan["epoch"]
        return {"job_id": plan["job_id"], "status": "success",
                "statements": [{"output": "out", "changed": True}], "execution": "single_node"}

    out = execute(cfg, pond, _run_python, source_catalog=source_catalog, quote_fn=quote_fn, submit_fn=submit_fn)
    assert out.route == "duckflock"
    assert not out.executed_locally
    assert out.result_doc["status"] == "success"
    assert seen["epoch"] == {"f": ts(2).isoformat(), "previous_f": ts(1).isoformat()}
    # duckstring did NOT run the ripple locally → its registry never produced `out`.
    assert not _table_exists(pond.con, "out")


def test_never_previous_f_serialises_to_sentinel(tmp_path):
    _, source_catalog = _publish_source(tmp_path, "SELECT 1 AS id, 'a' AS v", ts(1))
    pond = _sink(tmp_path, ts(1), NEVER)
    grabbed = {}

    def quote_fn(plan, config):
        grabbed["epoch"] = plan["epoch"]
        return {"route": "duckflock"}

    execute(DuckflockConfig(endpoint="http://fake"), pond, _run_python, source_catalog=source_catalog,
            quote_fn=quote_fn, submit_fn=lambda p, c: {"status": "success"})
    assert grabbed["epoch"]["previous_f"] == "0001-01-01T00:00:00+00:00"


# ─── failure posture: every DuckFlock-side problem degrades to classic + a warning ──


def test_quote_failure_falls_back_to_classic(tmp_path):
    _, source_catalog = _publish_source(tmp_path, "SELECT 1 AS id, 'a' AS v", ts(1))
    pond = _sink(tmp_path, ts(1), NEVER)

    def quote_fn(plan, config):
        raise RuntimeError("duckflock binary missing")

    out = execute(DuckflockConfig(endpoint="http://fake"), pond, _run_python,
                  source_catalog=source_catalog, quote_fn=quote_fn)
    assert out.route == "quote->classic" and out.executed_locally
    assert any("quote failed" in w for w in out.warnings)
    assert _own_rows(pond, tmp_path) == [(1, "a")]


def test_submit_failure_falls_back_to_classic(tmp_path):
    _, source_catalog = _publish_source(tmp_path, "SELECT 1 AS id, 'a' AS v", ts(1))
    pond = _sink(tmp_path, ts(1), NEVER)

    out = execute(
        DuckflockConfig(endpoint="http://fake"), pond, _run_python, source_catalog=source_catalog,
        quote_fn=lambda p, c: {"route": "duckflock"},
        submit_fn=lambda p, c: (_ for _ in ()).throw(ConnectionError("endpoint down")),
    )
    assert out.route == "duckflock->classic" and out.executed_locally
    assert any("submit failed" in w for w in out.warnings)
    assert _own_rows(pond, tmp_path) == [(1, "a")]


def test_failed_job_falls_back_to_classic(tmp_path):
    _, source_catalog = _publish_source(tmp_path, "SELECT 1 AS id, 'a' AS v", ts(1))
    pond = _sink(tmp_path, ts(1), NEVER)

    out = execute(
        DuckflockConfig(endpoint="http://fake"), pond, _run_python, source_catalog=source_catalog,
        quote_fn=lambda p, c: {"route": "duckflock"},
        submit_fn=lambda p, c: {"status": "failed", "error": {"code": "BUILD_ERROR", "message": "boom"}},
    )
    assert out.route == "duckflock->classic" and out.executed_locally
    assert any("did not succeed" in w for w in out.warnings)
    assert _own_rows(pond, tmp_path) == [(1, "a")]


def test_non_capturable_ripple_runs_classic(tmp_path):
    """A ripple with a callable metric (``agg.reduce``) can't be expressed in the plan IR — the
    recorder raises NonCapturable, so the backend never quotes and runs it on the classic path."""
    _, source_catalog = _publish_source(tmp_path, "SELECT 1 AS id, 'a' AS v", ts(1))
    pond = _sink(tmp_path, ts(1), NEVER)

    from duckstring import agg

    def reduce_ripple(p):
        out = (p.trickle("src.t").along("id")
               .aggregate(by="v", r=agg.reduce(lambda s, row: (s, s), 0)).merge("out"))
        return [("out", out.was_changed())]

    called = {"quote": False}

    def quote_fn(plan, config):
        called["quote"] = True
        return {"route": "duckflock"}

    out = execute(DuckflockConfig(endpoint="http://fake"), pond, reduce_ripple,
                  source_catalog=source_catalog, quote_fn=quote_fn)
    assert out.route == "classic" and out.executed_locally
    assert not called["quote"]  # capture failed before quoting
    assert any("not capturable" in w for w in out.warnings)


# ─── config from the environment ────────────────────────────────────────────────


def test_config_from_env_defaults_disabled(monkeypatch):
    for k in list(os.environ):
        if k.startswith("DUCKSTRING_DUCKFLOCK_"):
            monkeypatch.delenv(k, raising=False)
    assert not DuckflockConfig.from_env().enabled
    monkeypatch.setenv("DUCKSTRING_DUCKFLOCK_ENDPOINT", "http://x:7877")
    monkeypatch.setenv("DUCKSTRING_DUCKFLOCK_TENANT", "acme")
    monkeypatch.setenv("DUCKSTRING_DUCKFLOCK_MAJOR", "3")
    cfg = DuckflockConfig.from_env()
    assert cfg.enabled and cfg.tenant == "acme" and cfg.major == 3


# ─── gated end-to-end against the REAL duckflock binary (the conformance property) ──


@pytest.mark.skipif(not os.environ.get("DUCKFLOCK_BIN"), reason="set DUCKFLOCK_BIN to the duckflock CLI")
def test_real_duckflock_route_matches_classic(tmp_path):
    """The plane-state-identity property from the duckstring side: a plan routed to the REAL duckflock
    engine (via `duckflock run` as the submit seam) publishes byte-identical output to classic
    execution — and a stats-tagged trivial plan routes local."""
    duckflock = os.environ["DUCKFLOCK_BIN"]
    _, source_catalog = _publish_source(tmp_path, "SELECT * FROM (VALUES (1,'a'),(2,'b')) t(id,v)", ts(1))

    # Real locations: the own output goes to a real, duckflock-writable dir.
    own_dir = tmp_path / "ponds" / "o" / "m1" / "data"
    own_dir.mkdir(parents=True, exist_ok=True)

    def cli_run_submit(plan, config):
        with tempfile.NamedTemporaryFile("w", suffix=".plan.json", delete=False) as pf:
            json.dump(plan, pf)
            ppath = pf.name
        rpath = ppath + ".result.json"
        subprocess.run([duckflock, "run", "--plan", ppath, "--result", rpath],
                       check=True, capture_output=True, text=True)
        return json.loads(Path(rpath).read_text())

    cfg = DuckflockConfig(endpoint="http://unused", tenant="test", quote_bin=duckflock, record_local=False)

    # (1) no catalog stats → the real quote routes `duckflock`; run it and capture the published output.
    pond = _sink(tmp_path, ts(1), NEVER)
    out = execute(cfg, pond, _run_python, source_catalog=source_catalog,
                  own_location=str(own_dir), submit_fn=cli_run_submit)
    assert out.route == "duckflock", out.warnings
    rcon = duckdb.connect()
    rcon.execute("SET TimeZone='UTC'")
    remote_sql = ParquetDataPlane().read_select(own_dir, "out")
    remote = sorted(rcon.sql(f"SELECT id, v FROM ({remote_sql})").fetchall())
    rcon.close()

    # classic reference
    ref_root = tmp_path / "ref"
    _publish_source(ref_root, "SELECT * FROM (VALUES (1,'a'),(2,'b')) t(id,v)", ts(1))
    ref_pond = _sink(ref_root, ts(1), NEVER)
    _run_python(ref_pond)
    classic = _own_rows(ref_pond, ref_root)
    assert remote == classic == [(1, "a"), (2, "b")]

    # (2) tiny catalog stats → the real quote routes `local` (executes on duckstring).
    broot = tmp_path / "b"
    broot.mkdir(parents=True, exist_ok=True)
    _, sc_stats = _publish_source(broot, "SELECT * FROM (VALUES (1,'a')) t(id,v)", ts(1))
    pond2 = _sink(broot, ts(1), NEVER)
    out2 = execute(cfg, pond2, _run_python,
                   source_catalog=lambda ref: sc_stats(ref, stats={"rows": 1, "bytes": 16}),
                   submit_fn=cli_run_submit)
    assert out2.route == "local" and out2.executed_locally


def _table_exists(con, name):
    return con.execute("SELECT 1 FROM duckdb_tables() WHERE table_name = ?", [name]).fetchone() is not None
