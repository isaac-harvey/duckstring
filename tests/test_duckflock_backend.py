"""DuckFlock execution routing (the embedded-driver backend). The backend captures a ripple's
plan, quotes it (``duckflock quote``), and either runs it classically on duckstring's own engine
(``route: local``) or through the co-resident driver (``duckflock run`` as a subprocess). Every
DuckFlock-side failure degrades to classic local execution with a warning; with no ``DUCKFLOCK_BIN``
configured the backend is a no-op. Offline tests inject fake quote/run seams; a gated test drives
the real ``duckflock`` binary end-to-end (skipped unless ``DUCKFLOCK_BIN`` is set)."""

from __future__ import annotations

import os
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
    cfg = DuckflockConfig(bin="duckflock", tenant="acme")

    def quote_fn(plan, config):
        assert plan["duckflock_plan"] == 1 and plan["pond"]["name"] == "o"
        return {"route": "local", "why": "tiny", "chosen_modes": ["comprehensive"]}

    def run_fn(plan, config):
        raise AssertionError("must not run a locally-routed plan through the driver")

    out = execute(cfg, pond, _run_python, source_catalog=source_catalog, quote_fn=quote_fn, run_fn=run_fn)
    assert out.route == "local" and out.executed_locally
    assert out.quote["route"] == "local"
    assert _own_rows(pond, tmp_path) == [(1, "a")]


# ─── the duckflock route runs the driver and surfaces the result, without local execution ──


def test_route_duckflock_runs_driver_and_surfaces_result(tmp_path):
    _, source_catalog = _publish_source(tmp_path, "SELECT 1 AS id, 'a' AS v", ts(1))
    pond = _sink(tmp_path, ts(2), ts(1))
    cfg = DuckflockConfig(bin="duckflock", tenant="acme")
    seen = {}

    def quote_fn(plan, config):
        return {"route": "duckflock", "why": "big"}

    def run_fn(plan, config):
        seen["job_id"] = plan["job_id"]
        seen["epoch"] = plan["epoch"]
        return {"job_id": plan["job_id"], "status": "success",
                "statements": [{"output": "out", "changed": True}], "execution": "single_node"}

    out = execute(cfg, pond, _run_python, source_catalog=source_catalog, quote_fn=quote_fn, run_fn=run_fn)
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

    execute(DuckflockConfig(bin="duckflock"), pond, _run_python, source_catalog=source_catalog,
            quote_fn=quote_fn, run_fn=lambda p, c: {"status": "success"})
    assert grabbed["epoch"]["previous_f"] == "0001-01-01T00:00:00+00:00"


# ─── failure posture: every DuckFlock-side problem degrades to classic + a warning ──


def test_quote_failure_falls_back_to_classic(tmp_path):
    _, source_catalog = _publish_source(tmp_path, "SELECT 1 AS id, 'a' AS v", ts(1))
    pond = _sink(tmp_path, ts(1), NEVER)

    def quote_fn(plan, config):
        raise RuntimeError("duckflock binary missing")

    out = execute(DuckflockConfig(bin="duckflock"), pond, _run_python,
                  source_catalog=source_catalog, quote_fn=quote_fn)
    assert out.route == "quote->classic" and out.executed_locally
    assert any("quote failed" in w for w in out.warnings)
    assert _own_rows(pond, tmp_path) == [(1, "a")]


def test_run_failure_falls_back_to_classic(tmp_path):
    _, source_catalog = _publish_source(tmp_path, "SELECT 1 AS id, 'a' AS v", ts(1))
    pond = _sink(tmp_path, ts(1), NEVER)

    out = execute(
        DuckflockConfig(bin="duckflock"), pond, _run_python, source_catalog=source_catalog,
        quote_fn=lambda p, c: {"route": "duckflock"},
        run_fn=lambda p, c: (_ for _ in ()).throw(ConnectionError("driver crashed")),
    )
    assert out.route == "duckflock->classic" and out.executed_locally
    assert any("run failed" in w for w in out.warnings)
    assert _own_rows(pond, tmp_path) == [(1, "a")]


def test_failed_job_falls_back_to_classic(tmp_path):
    _, source_catalog = _publish_source(tmp_path, "SELECT 1 AS id, 'a' AS v", ts(1))
    pond = _sink(tmp_path, ts(1), NEVER)

    out = execute(
        DuckflockConfig(bin="duckflock"), pond, _run_python, source_catalog=source_catalog,
        quote_fn=lambda p, c: {"route": "duckflock"},
        run_fn=lambda p, c: {"status": "failed", "error": {"code": "BUILD_ERROR", "message": "boom"}},
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

    out = execute(DuckflockConfig(bin="duckflock"), pond, reduce_ripple,
                  source_catalog=source_catalog, quote_fn=quote_fn)
    assert out.route == "classic" and out.executed_locally
    assert not called["quote"]  # capture failed before quoting
    assert any("not capturable" in w for w in out.warnings)


# ─── config from the environment ────────────────────────────────────────────────


def test_config_from_env_defaults_disabled(monkeypatch):
    for k in ("DUCKFLOCK_BIN", "DUCKFLOCK_TENANT", "DUCKFLOCK_EXPRESS_ROOT",
              "DUCKFLOCK_RECORD_LOCAL", "DUCKSTRING_DUCK_FLOCK"):
        monkeypatch.delenv(k, raising=False)
    assert not DuckflockConfig.from_env().enabled
    monkeypatch.setenv("DUCKFLOCK_BIN", "/opt/duckflock/duckflock")
    monkeypatch.setenv("DUCKFLOCK_TENANT", "acme")
    monkeypatch.setenv("DUCKFLOCK_EXPRESS_ROOT", "/scratch/express")
    cfg = DuckflockConfig.from_env()
    assert cfg.enabled and cfg.tenant == "acme" and cfg.flock
    assert cfg.express_location == "/scratch/express"
    # escalation-disabled mode ("local" Ducks): distribution off ⇒ EMPTY express_location, never
    # absent (absent would let the driver's own env fallback re-enable distribution).
    monkeypatch.setenv("DUCKSTRING_DUCK_FLOCK", "off")
    assert DuckflockConfig.from_env().express_location == ""


def test_flock_off_plans_carry_empty_express_location(tmp_path):
    _, source_catalog = _publish_source(tmp_path, "SELECT 1 AS id, 'a' AS v", ts(1))
    pond = _sink(tmp_path, ts(1), NEVER)
    cfg = DuckflockConfig(bin="duckflock", express_root="/scratch/express", flock=False)
    grabbed = {}

    def quote_fn(plan, config):
        grabbed["config"] = plan["config"]
        return {"route": "local"}

    execute(cfg, pond, _run_python, source_catalog=source_catalog, quote_fn=quote_fn)
    assert grabbed["config"]["express_location"] == ""


# ─── the Duck executor routing hook (duck/duckflock_route.py) ────────────────────


def _executor(tmp_path, name="o"):
    """A RippleExecutor over a minimal deployed source tree (root = tmp_path)."""
    from duckstring.duck.executor import RippleExecutor

    src_dir = tmp_path / "sources" / name
    (src_dir / "src").mkdir(parents=True, exist_ok=True)
    (src_dir / "pond.toml").write_text(f'[pond]\nname = "{name}"\nversion = "1.0.0"\n[sources]\nsrc = "1"\n')
    (src_dir / "src" / "pond.py").write_text("")
    return RippleExecutor(name, 1, "1.0.0", f"sources/{name}", tmp_path)


def _fake_remote_submit(tmp_path):
    """A run seam that behaves like the DuckFlock driver: hydrate the plan's own location (the
    scratch seed = prior state), run the ripple's semantics against it, publish back to scratch."""
    from datetime import datetime

    from duckstring.dataplane import hydrate_registry

    def run(plan, config):
        scratch = Path(next(e["location"] for e in plan["catalog"].values() if "duckflock_scratch" in e["location"]))
        rcon = duckdb.connect()
        rcon.execute("SET TimeZone='UTC'")
        if not plan["config"]["refresh"]:
            hydrate_registry(rcon, scratch)
        p = Pond(plan["pond"]["name"], plan["pond"]["version"], rcon, root=tmp_path,
                 source_majors={"src": 1},
                 f=datetime.fromisoformat(plan["epoch"]["f"]),
                 previous_f=datetime.fromisoformat(plan["epoch"]["previous_f"]))
        result = _run_python(p)
        ParquetDataPlane().export(rcon, scratch, f=p.f)
        rcon.close()
        return {"status": "success",
                "statements": [{"output": o, "changed": bool(c)} for o, c in result]}

    return run


def test_route_ripple_remote_two_epochs_matches_classic(tmp_path):
    """The executor routing round-trip: seed scratch with prior state → 'remote' execution → publish to
    scratch → hydrate back into the registry → the run-end export ships it. Two epochs (the second
    incremental over the first's hydrated state) match classic execution exactly."""
    from duckstring.duck.duckflock_route import route_ripple

    _, _sc = _publish_source(tmp_path, "SELECT * FROM (VALUES (1,'a'),(2,'b')) t(id,v)", ts(1))
    ex = _executor(tmp_path)
    cfg = DuckflockConfig(bin="duckflock", tenant="t")
    quote = lambda p, c: {"route": "duckflock"}  # noqa: E731
    run = _fake_remote_submit(tmp_path)

    out1 = route_ripple(ex, cfg, _run_python, ts(1), NEVER, quote_fn=quote, run_fn=run)
    assert out1.route == "duckflock" and not out1.executed_locally
    ex.export(f=ts(1))  # the normal run-end publish ships the hydrated result

    _publish_source(tmp_path, "SELECT * FROM (VALUES (1,'a'),(2,'B'),(3,'c')) t(id,v)", ts(2))
    out2 = route_ripple(ex, cfg, _run_python, ts(2), ts(1), quote_fn=quote, run_fn=run)
    assert out2.route == "duckflock"
    ex.export(f=ts(2))

    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    sql = ParquetDataPlane().read_select(tmp_path / "ponds" / "o" / "m1" / "data", "out")
    got = sorted(con.sql(f"SELECT id, v FROM ({sql})").fetchall())
    assert got == [(1, "a"), (2, "B"), (3, "c")]
    # the registry holds the hydrated Trickle collection (downstream ripples/export see it as local)
    import duckstring.trickle_io as T

    assert T.read_meta(ex._registry).get("out", {}).get("mode") == "merge"
    assert not (tmp_path / "ponds" / "o" / "m1" / "duckflock_scratch").exists()  # cleaned after hydrate
    con.close()
    ex.shutdown()

    # classic reference over the same two epochs
    ref = tmp_path / "ref"
    _publish_source(ref, "SELECT * FROM (VALUES (1,'a'),(2,'b')) t(id,v)", ts(1))
    p1 = _sink(ref, ts(1), NEVER)
    _run_python(p1)
    ParquetDataPlane().export(p1.con, ref / "ponds" / "o" / "m1" / "data", f=ts(1))
    _publish_source(ref, "SELECT * FROM (VALUES (1,'a'),(2,'B'),(3,'c')) t(id,v)", ts(2))
    p2 = Pond("o", "1.0.0", p1.con, root=ref, source_majors={"src": 1}, f=ts(2), previous_f=ts(1))
    _run_python(p2)
    ParquetDataPlane().export(p2.con, ref / "ponds" / "o" / "m1" / "data", f=ts(2))
    rcon = duckdb.connect()
    rcon.execute("SET TimeZone='UTC'")
    ref_sql = ParquetDataPlane().read_select(ref / "ponds" / "o" / "m1" / "data", "out")
    assert sorted(rcon.sql(f"SELECT id, v FROM ({ref_sql})").fetchall()) == got
    rcon.close()
    p1.con.close()


def test_route_ripple_remote_failure_runs_classic_on_registry(tmp_path):
    """A remote failure inside the routed path still completes the ripple classically ON THE EXECUTOR'S
    REGISTRY — the run continues as if routing never existed."""
    from duckstring.duck.duckflock_route import route_ripple

    _publish_source(tmp_path, "SELECT 1 AS id, 'a' AS v", ts(1))
    ex = _executor(tmp_path)
    out = route_ripple(
        ex, DuckflockConfig(bin="duckflock"), _run_python, ts(1), NEVER,
        quote_fn=lambda p, c: {"route": "duckflock"},
        run_fn=lambda p, c: (_ for _ in ()).throw(ConnectionError("down")),
    )
    assert out.route == "duckflock->classic" and out.executed_locally
    ex.export(f=ts(1))
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    sql = ParquetDataPlane().read_select(tmp_path / "ponds" / "o" / "m1" / "data", "out")
    assert sorted(con.sql(f"SELECT id, v FROM ({sql})").fetchall()) == [(1, "a")]
    con.close()
    ex.shutdown()


def test_route_ripple_refresh_carries_flag_and_empty_seed(tmp_path):
    """After a wipe (a Refresh run), a routed plan carries config.refresh and the scratch seed is
    EMPTY — the remote engine must treat prior state as absent, mirroring the local cold rebuild."""
    from duckstring.duck.duckflock_route import route_ripple

    _publish_source(tmp_path, "SELECT 1 AS id, 'a' AS v", ts(1))
    ex = _executor(tmp_path)
    quote = lambda p, c: {"route": "duckflock"}  # noqa: E731
    run = _fake_remote_submit(tmp_path)
    route_ripple(ex, DuckflockConfig(bin="duckflock"), _run_python, ts(1), NEVER,
                 quote_fn=quote, run_fn=run)
    ex.export(f=ts(1))
    ex.wipe()
    assert ex._refresh_pending

    seen = {}

    def spy_submit(plan, config):
        scratch = Path(next(e["location"] for e in plan["catalog"].values()
                            if "duckflock_scratch" in e["location"]))
        seen["refresh"] = plan["config"]["refresh"]
        seen["seeded"] = sorted(p.name for p in scratch.iterdir()) if scratch.exists() else []
        return run(plan, config)

    route_ripple(ex, DuckflockConfig(bin="duckflock"), _run_python, ts(2), NEVER,
                 refresh=ex._refresh_pending, quote_fn=quote, run_fn=spy_submit)
    assert seen["refresh"] is True
    assert seen["seeded"] == []  # no prior state shipped to a refresh run
    ex.export(f=ts(2))
    assert not ex._refresh_pending  # cleared once the rebuild published
    ex.shutdown()


def test_executor_submit_routes_when_enabled(tmp_path, monkeypatch):
    """The executor's _task branches to route_ripple iff DUCKFLOCK_BIN is set."""
    import duckstring.duck.duckflock_route as dr
    import duckstring.duck.executor as exmod

    _publish_source(tmp_path, "SELECT 1 AS id, 'a' AS v", ts(1))
    ex = _executor(tmp_path)
    monkeypatch.setattr(exmod, "_load_ripple", lambda *a: _run_python)
    calls = []

    def spy_route(e, cfg, func, f, pf, **kw):
        calls.append(cfg.bin)
        func_con = e._cursor()
        try:
            func(Pond("o", "1.0.0", func_con, root=tmp_path, source_majors={"src": 1}, f=f, previous_f=pf))
        finally:
            func_con.close()

    monkeypatch.setattr(dr, "route_ripple", spy_route)

    done, err = [], []
    monkeypatch.delenv("DUCKFLOCK_BIN", raising=False)
    ex.submit("r", ts(1), NEVER, lambda *a: done.append(a), lambda *a: err.append(a)).result()
    assert not calls and not err  # disabled → classic, no routing

    monkeypatch.setenv("DUCKFLOCK_BIN", "/opt/duckflock/duckflock")
    ex.submit("r", ts(2), ts(1), lambda *a: done.append(a), lambda *a: err.append(a)).result()
    import time

    time.sleep(0.05)  # let the done-callback fire
    assert calls == ["/opt/duckflock/duckflock"] and not err
    ex.shutdown()


# ─── gated end-to-end against the REAL duckflock binary (the conformance property) ──


@pytest.mark.skipif(not os.environ.get("DUCKFLOCK_BIN"), reason="set DUCKFLOCK_BIN to the duckflock CLI")
def test_real_duckflock_route_matches_classic(tmp_path):
    """The plane-state-identity property from the duckstring side: a plan routed through the REAL
    co-resident driver (the default `duckflock run` subprocess path — no injected seams) publishes
    byte-identical output to classic execution — and a stats-tagged trivial plan routes local."""
    duckflock = os.environ["DUCKFLOCK_BIN"]
    _, source_catalog = _publish_source(tmp_path, "SELECT * FROM (VALUES (1,'a'),(2,'b')) t(id,v)", ts(1))

    # Real locations: the own output goes to a real, duckflock-writable dir.
    own_dir = tmp_path / "ponds" / "o" / "m1" / "data"
    own_dir.mkdir(parents=True, exist_ok=True)

    cfg = DuckflockConfig(bin=duckflock, tenant="test", record_local=False)

    # (1) no catalog stats → the real quote routes `duckflock`; run it and capture the published output.
    pond = _sink(tmp_path, ts(1), NEVER)
    out = execute(cfg, pond, _run_python, source_catalog=source_catalog, own_location=str(own_dir))
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
                   source_catalog=lambda ref: sc_stats(ref, stats={"rows": 1, "bytes": 16}))
    assert out2.route == "local" and out2.executed_locally


def _table_exists(con, name):
    return con.execute("SELECT 1 FROM duckdb_tables() WHERE table_name = ?", [name]).fetchone() is not None
