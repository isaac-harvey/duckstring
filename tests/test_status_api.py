"""Read-API surface for the UI: the enriched ``/api/status`` payload (ripple-level state + intra-Pond
edges, ``d_ms``, standing trigger) and the ``/api/runs`` history feed (Pond Runs, newest first,
optionally filtered to a Pond + its upstream lineage, with nested Ripple Runs).
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from duckstring.catchment.db import connect, migrate
from duckstring.catchment.driver import Driver
from duckstring.catchment.launcher import NoopLauncher
from duckstring.catchment.routes import router
from duckstring.catchment.routes.deploy import _register

pytestmark = pytest.mark.timeout(5)

_RIPPLES = [{"func": "f1", "name": "r1", "parents": []}, {"func": "f2", "name": "r2", "parents": ["f1"]}]


def _cfg(sources=None, kind="inlet"):
    return {"sources": sources or {}, "immediate_retries": 0, "source_retries": 0, "kind": kind}


def _driver(tmp_path):
    db = connect(tmp_path / "duck.db")
    migrate(db)
    # src (inlet) → snk (pond depends on src). Both have a two-ripple chain r1 → r2.
    _register(db, "src", "1.0.0", "inlet", "ponds/src/1.0.0", _cfg(), _RIPPLES)
    _register(db, "snk", "1.0.0", "pond", "ponds/snk/1.0.0", _cfg(sources={"src": "1.0.0"}), _RIPPLES)
    return Driver(db, tmp_path, "http://x", NoopLauncher())


def _pond(status, name):
    return next(p for p in status["ponds"] if p["name"] == name)


def _complete_run(driver, pond):
    """Drive a full Pond Run to completion via simulated Duck events (r1 then r2)."""
    f = driver.state.pond_states[pond].start_f.isoformat()
    driver.on_event(pond, {"kind": "ripple", "f": f, "ripple": "r1", "status": "success"})
    driver.on_event(pond, {"kind": "ripple", "f": f, "ripple": "r2", "status": "success"})
    driver.on_event(pond, {"kind": "run_completed", "f": f})
    return f


# ─── /api/status enrichment ──────────────────────────────────────────────────────


def test_status_exposes_ripples_and_intra_pond_edges(tmp_path):
    d = _driver(tmp_path)
    snk = _pond(d.status(), "snk")
    assert {r["name"] for r in snk["ripples"]} == {"r1", "r2"}
    assert snk["ripple_edges"] == [["r1", "r2"]]
    for r in snk["ripples"]:
        assert set(r) == {
            "name", "status", "gen", "runs_completed",
            "has_pull", "target_f", "start_f", "end_f",
        }
        assert r["status"] == "idle"
    assert snk["has_tables"] is False  # nothing exported yet


def test_status_flags_ponds_with_exported_tables(tmp_path):
    import duckdb

    d = _driver(tmp_path)
    assert _pond(d.status(), "src")["has_tables"] is False  # nothing exported

    # Export any table under src's data dir — the Pond now reports has_tables.
    data_dir = tmp_path / "ponds" / "src" / "m1" / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"COPY (SELECT 1 AS id) TO '{data_dir / 'anything.parquet'}' (FORMAT PARQUET)")
    con.close()

    assert _pond(d.status(), "src")["has_tables"] is True
    assert _pond(d.status(), "snk")["has_tables"] is False  # only src exported


def test_status_exposes_d_ms_and_null_trigger_by_default(tmp_path):
    d = _driver(tmp_path)
    snk = _pond(d.status(), "snk")
    assert snk["d_ms"] == 0
    assert snk["trigger"] is None


def test_status_reports_standing_wave_and_tide_triggers(tmp_path):
    d = _driver(tmp_path)
    d.wave("snk@1")
    assert _pond(d.status(), "snk")["trigger"] == {"kind": "wave", "bound_ms": None}

    d.tide("snk@1", timedelta(seconds=2.5))
    assert _pond(d.status(), "snk")["trigger"] == {"kind": "tide", "bound_ms": 2500}


def test_status_running_ripple_propagates_to_pond(tmp_path):
    d = _driver(tmp_path)
    d.state.ripple_states["snk@1.r1"].is_running = True  # an in-flight Ripple
    snk = _pond(d.status(), "snk")
    assert snk["status"] == "running", "a running Ripple makes its Pond running"
    assert next(r for r in snk["ripples"] if r["name"] == "r1")["status"] == "running"
    assert next(r for r in snk["ripples"] if r["name"] == "r2")["status"] == "idle"


# ─── Duck liveness ─────────────────────────────────────────────────────────────


class _DeadLauncher(NoopLauncher):
    """A launcher that owns processes (so liveness is checked) but reports them all dead."""

    manages_processes = True

    def is_running(self, pond_name: str) -> bool:
        return False


def _inlet_driver(tmp_path, db_name, launcher):
    from duckstring.catchment.db import connect, migrate

    db = connect(tmp_path / db_name)
    migrate(db)
    _register(db, "src", "1.0.0", "inlet", "ponds/src/1.0.0", _cfg(), _RIPPLES)
    return Driver(db, tmp_path, "http://x", launcher)


def test_check_liveness_fails_dead_duck(tmp_path):
    from duckstring.catchment.driver import _now

    d = _inlet_driver(tmp_path, "duck.db", _DeadLauncher())
    d.pulse("src@1")  # a Pond Run is now in flight (start_f > end_f), nothing completes it
    assert d.state.pond_states["src@1"].start_f > d.state.pond_states["src@1"].end_f
    d._check_liveness(_now())
    src = d.state.pond_states["src@1"]
    assert src.is_failed and src.failures == 1  # the dead Duck's Run failed at start_f


def test_check_liveness_skipped_without_process_launcher(tmp_path):
    from duckstring.catchment.driver import _now

    d = _inlet_driver(tmp_path, "duck2.db", NoopLauncher())  # manages_processes = False
    d.pulse("src@1")
    d._check_liveness(_now())
    assert not d.state.pond_states["src@1"].is_failed  # nothing to watch → never failed on liveness


# ─── Control: Force / Kill ─────────────────────────────────────────────────────


def test_force_dispatches_force_flag(tmp_path):
    d = _driver(tmp_path)
    d.pulse("src@1")
    _complete_run(d, "src@1")  # src is now current (start_f == end_f)
    d.jobs["src@1"] = []  # drop the prior dispatch
    d.force("src@1")
    assert any(j["kind"] == "begin_run" and j.get("force") for j in d.jobs.get("src@1", []))


def test_clear_failed_pond_is_not_refailed_by_liveness(tmp_path):
    from duckstring.catchment.driver import _now

    d = _inlet_driver(tmp_path, "clr.db", _DeadLauncher())  # manages_processes, but reports Ducks dead
    d.pulse("src@1")  # a Run in flight (start_f > end_f)
    f = d.state.pond_states["src@1"].start_f.isoformat()
    d.on_event("src@1", {"kind": "failed", "ripple": "r1", "f": f, "status": "failed", "error": "boom"})
    assert d.state.pond_states["src@1"].is_failed
    d.clear("src@1")
    assert not d.state.pond_states["src@1"].is_failed
    # The liveness sweep must NOT re-fail it — clearing abandoned the phantom in-flight Run.
    d._check_liveness(_now())
    assert not d.state.pond_states["src@1"].is_failed
    assert d.state.pond_states["src@1"].start_f == d.state.pond_states["src@1"].end_f  # idle, not in-flight


def test_kill_terminates_duck_and_parks(tmp_path):
    terminated = []

    class _RecordingLauncher(NoopLauncher):
        manages_processes = True

        def terminate(self, pond_name: str) -> None:
            terminated.append(pond_name)

    d = _inlet_driver(tmp_path, "kill.db", _RecordingLauncher())
    d.pulse("src@1")  # a Run in flight
    d.kill("src@1")
    assert d.state.pond_states["src@1"].is_killed
    assert terminated == ["src@1"]  # the Duck was terminated
    # Killed supersedes demand: a Tap does nothing until cleared.
    started = d.state.pond_states["src@1"].runs_started
    d.tap("src@1")
    assert d.state.pond_states["src@1"].runs_started == started


# ─── /api/runs history ───────────────────────────────────────────────────────────


def test_run_history_newest_first_and_records_freshness(tmp_path):
    d = _driver(tmp_path)
    d.pulse("src@1")
    f = _complete_run(d, "src@1")
    runs = d.run_history(None, lineage=True, ripples=False, limit=100)
    assert len(runs) == 1
    assert runs[0]["pond"] == "src" and runs[0]["f"] == f
    assert runs[0]["status"] == "success" and runs[0]["finished_at"] is not None
    assert "ripples" not in runs[0]


def test_run_history_lineage_filter(tmp_path):
    d = _driver(tmp_path)
    d.pulse("src@1")
    _complete_run(d, "src@1")

    # snk's lineage includes its upstream source src.
    with_lineage = d.run_history("snk@1", lineage=True, ripples=False, limit=100)
    assert {r["pond"] for r in with_lineage} == {"src"}

    # Without lineage, only snk's own runs (none yet).
    without = d.run_history("snk@1", lineage=False, ripples=False, limit=100)
    assert without == []


def test_run_history_nests_ripple_runs_when_requested(tmp_path):
    d = _driver(tmp_path)
    d.pulse("src@1")
    _complete_run(d, "src@1")
    runs = d.run_history("src@1", lineage=False, ripples=True, limit=100)
    assert len(runs) == 1
    nested = {r["ripple"]: r["status"] for r in runs[0]["ripples"]}
    assert nested == {"r1": "success", "r2": "success"}


def test_run_history_date_range_filters_started_at(tmp_path):
    # The plot's date-range navigation bounds a run's started_at (UTC ISO).
    d = _driver(tmp_path)
    d.pulse("src@1")
    f = _complete_run(d, "src@1")
    d.db.execute("UPDATE pond_run SET started_at = '2020-06-15T10:00:00+00:00' WHERE f = ?", (f,))
    d.db.commit()

    def hist(**kw):
        return d.run_history("src@1", lineage=False, ripples=False, limit=100, **kw)

    assert [r["f"] for r in hist(after="2020-06-01T00:00:00+00:00", before="2020-07-01T00:00:00+00:00")] == [f]
    assert hist(before="2020-06-01T00:00:00+00:00") == []  # the run is after the upper bound
    assert hist(after="2020-07-01T00:00:00+00:00") == []   # the run is before the lower bound


# ─── HTTP layer ──────────────────────────────────────────────────────────────────


def _client(driver):
    app = FastAPI()
    app.include_router(router, prefix="/api")
    app.state.driver = driver
    return TestClient(app)


def test_status_version_bumps_and_long_poll_returns_on_change(tmp_path):
    d = _driver(tmp_path)
    client = _client(d)
    v0 = client.get("/api/status").json()["version"]  # no `since` → immediate
    d.tap("src@1")  # a state change bumps the version
    # `since` the pre-change version → the version has already moved, so the long-poll returns at once.
    body = client.get("/api/status", params={"since": v0}).json()
    assert body["version"] > v0


def test_begin_run_cancels_a_pending_shutdown(tmp_path):
    # Re-arming a Pond must cancel a not-yet-collected reap shutdown, so the Duck doesn't exit out
    # from under an in-flight run (the "Duck process is not running" race).
    from datetime import datetime, timezone

    d = _driver(tmp_path)
    d.jobs["src@1"] = [{"kind": "shutdown"}]
    now = datetime.now(timezone.utc)
    d._dispatch_begin_run("src@1", now, now)
    kinds = [j["kind"] for j in d.jobs["src@1"]]
    assert "shutdown" not in kinds and "begin_run" in kinds


def test_runs_route_params_and_unknown_pond(tmp_path):
    client = _client(_driver(tmp_path))
    assert client.get("/api/runs").json() == {"runs": []}
    assert client.get("/api/runs", params={"pond": "nope"}).status_code == 404
    # limit is clamped (no error) and the enriched status round-trips over HTTP.
    assert client.get("/api/runs", params={"limit": 100000}).status_code == 200
    snk = _pond(client.get("/api/status").json(), "snk")
    assert snk["ripple_edges"] == [["r1", "r2"]]
