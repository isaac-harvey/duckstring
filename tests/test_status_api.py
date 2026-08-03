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

    # has_tables is cached per data_version (the ~1s status poll must never re-list storage — over an
    # object store that made /api/status take seconds). Every real data arrival bumps it (run
    # completion, persist, draw, deploy/reload); this test wrote the file BEHIND the driver's back, so
    # it does what an operator hand-placing data would: reload.
    d.reload()
    assert _pond(d.status(), "src")["has_tables"] is True
    assert _pond(d.status(), "snk")["has_tables"] is False  # only src exported


def test_status_flags_dbt_mode_pond(tmp_path):
    db = connect(tmp_path / "duck.db")
    migrate(db)
    _register(db, "py", "1.0.0", "pond", "ponds/py/1.0.0", _cfg(kind="pond"), _RIPPLES)
    _register(db, "shop", "1.0.0", "pond", "ponds/shop/1.0.0",
              {**_cfg(kind="pond"), "dbt_project": "dbt/"}, _RIPPLES)
    d = Driver(db, tmp_path, "http://x", NoopLauncher())
    st = {p["id"]: p for p in d.status()["ponds"]}
    assert st["shop@1"]["dbt"] is True
    assert st["py@1"]["dbt"] is False


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


def test_silent_duck_clears_launcher_record_and_carries_diagnosis(tmp_path):
    """The gate-run wedge: a remote Duck that launched but never dialled back must (a) be torn down so
    the backend's ensure() re-spawns on the next attempt — a stale task record made every retry a silent
    no-op — and (b) carry the provider's diagnosis (e.g. ECS stoppedReason) in the failure message."""
    from datetime import timedelta

    terminated = []

    class _SilentLauncher(NoopLauncher):
        manages_processes = True

        def is_running(self, pond_name: str) -> bool:
            return True  # a live task record — the wedge precondition

        def diagnose(self, pond_name: str) -> str:
            return "fargate: task STOPPED; TaskFailedToStart; boom"

        def terminate(self, pond_name: str, wait: bool = False) -> None:
            terminated.append(pond_name)

    from duckstring.catchment.driver import _now

    d = _inlet_driver(tmp_path, "silent.db", _SilentLauncher())
    d.pulse("src@1")  # a Run in flight
    d.take_jobs("src@1")  # it spoke once, so the steady-state silence window applies (not spawn grace)
    d.last_seen["src@1"] = _now() - timedelta(seconds=120)  # contact aged past the heartbeat
    d._check_liveness(_now())
    assert d.state.pond_states["src@1"].is_failed
    assert "src@1" in terminated, "the silent Duck's record must be cleared so a retry re-spawns"
    (error,) = d.db.execute("SELECT error FROM pond_run WHERE status = 'failed'").fetchone()
    assert "Lost contact" in error and "TaskFailedToStart" in error  # the provider's reason travels


def test_a_remote_duck_gets_its_providers_startup_grace(tmp_path):
    """First contact is a different quantity from steady-state silence: a local subprocess talks in a
    second, an EC2 instance boots an OS first. Judging a cold EC2 spawn by the 60 s silence window failed
    every first run on a fresh pool, however well configured. The grace applies only until it speaks."""
    from datetime import timedelta

    d = _inlet_driver(tmp_path, "grace.db", _DeadLauncher())
    key = "src@1"
    d._awaiting_first_contact.add(key)

    d.duck_config = lambda _k: {"remote": False, "pool": None}
    assert d._silence_window(key) == timedelta(seconds=60)
    d.duck_config = lambda _k: {"remote": True, "pool": {"provider": "fargate"}}
    assert d._silence_window(key) == timedelta(minutes=3)
    d.duck_config = lambda _k: {"remote": True, "pool": {"provider": "ec2"}}
    assert d._silence_window(key) == timedelta(minutes=8)

    # Once the Duck speaks — here by collecting its jobs, the very first thing it does — the
    # steady-state window governs: a warm EC2 Duck that goes quiet is still caught in 60 s.
    d.take_jobs(key)
    assert key not in d._awaiting_first_contact
    assert d._silence_window(key) == timedelta(seconds=60)


def test_dead_duck_clears_launcher_record(tmp_path):
    from duckstring.catchment.driver import _now

    terminated = []

    class _DeadRecordingLauncher(_DeadLauncher):
        def terminate(self, pond_name: str, wait: bool = False) -> None:
            terminated.append(pond_name)

    d = _inlet_driver(tmp_path, "deadrec.db", _DeadRecordingLauncher())
    d.pulse("src@1")
    d._check_liveness(_now())
    assert d.state.pond_states["src@1"].is_failed
    assert "src@1" in terminated


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
    # The plot's range navigation bounds a run's started_at at second granularity. Stored values are
    # Python isoformat (UTC, +00:00); the bounds arrive as the frontend sends them (JS toISOString → Z).
    d = _driver(tmp_path)
    d.pulse("src@1")
    f = _complete_run(d, "src@1")
    d.db.execute("UPDATE pond_run SET started_at = '2026-07-22T14:35:40.500000+00:00' WHERE f = ?", (f,))
    d.db.commit()

    def hist(**kw):
        return d.run_history("src@1", lineage=False, ripples=False, limit=100, **kw)

    # A tight window around the run (Z-suffixed bounds vs +00:00-stored) selects it…
    assert [r["f"] for r in hist(after="2026-07-22T14:35:30.000Z", before="2026-07-22T14:36:00.000Z")] == [f]
    # …and a window seconds off — same day — excludes it (granularity is seconds, not days).
    assert hist(after="2026-07-22T14:35:45.000Z") == []
    assert hist(before="2026-07-22T14:35:35.000Z") == []


def test_run_history_range_anchoring_and_order(tmp_path):
    # The window anchors on the bound: "from" → the first `limit` runs ascending from it; "to"-only (and
    # the default) → the most-recent `limit`, descending.
    d = _driver(tmp_path)
    pv = d.db.execute("SELECT pv.id FROM pond_version pv JOIN pond_name pn ON pn.id = pv.pond_name_id "
                      "WHERE pn.name = 'src'").fetchone()[0]
    for h in (10, 11, 12):
        ts = f"2026-07-22T{h:02d}:00:00+00:00"
        d.db.execute("INSERT INTO pond_run (pond_version_id, f, started_at, finished_at, status) "
                     "VALUES (?, ?, ?, ?, 'success')", (pv, ts, ts, ts))
    d.db.commit()

    def hours(**kw):
        return [r["started_at"][11:13] for r in d.run_history("src@1", lineage=False, ripples=False, **kw)]

    assert hours(limit=2) == ["12", "11"]                                   # default: most recent
    assert hours(limit=2, after="2026-07-22T10:30:00Z") == ["11", "12"]      # from → forward (ascending)
    assert hours(limit=2, before="2026-07-22T11:30:00Z") == ["11", "10"]     # to-only → the runs just prior


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


def _run_rows(d):
    return [r[0] for r in d.db.execute("SELECT status FROM pond_run ORDER BY f").fetchall()]


def test_clear_closes_the_run_it_abandons(tmp_path):
    """`clear` rolls start_f back to end_f so the halted Run is not re-failed by liveness — which also
    means liveness can never CLOSE it. Without this the Run sits at 'running' in the history forever,
    looking like work still in progress."""
    from duckstring.catchment.driver import _now

    d = _inlet_driver(tmp_path, "clearrun.db", _DeadLauncher())
    d.pulse("src@1")
    assert _run_rows(d) == ["running"]

    d.clear("src@1")                # abandons the in-flight Run by design
    d._check_liveness(_now())       # that Run is not in flight any more → liveness never touches it
    assert "running" not in _run_rows(d), "clear left an orphaned 'running' row"


def test_abandoning_a_run_records_why_and_spares_finished_ones(tmp_path):
    """The closure carries its reason, and never rewrites a Run the Duck already reported."""
    from duckstring.catchment.driver import _now

    d = _inlet_driver(tmp_path, "abandon.db", _DeadLauncher())
    d.pulse("src@1")
    f = d.state.pond_states["src@1"].start_f.isoformat()

    d._abandon_pond_run("src@1", f, _now())
    status, err = d.db.execute("SELECT status, error FROM pond_run WHERE f = ?", (f,)).fetchone()
    assert status == "failed" and "abandoned by an operator clear" in err

    # Re-running it must not touch the now-finished row (nor invent one for an unknown f).
    d._abandon_pond_run("src@1", f, _now())
    d._abandon_pond_run("src@1", "2099-01-01T00:00:00+00:00", _now())
    assert d.db.execute("SELECT count(*) FROM pond_run").fetchone()[0] == 1
    (err2,) = d.db.execute("SELECT error FROM pond_run WHERE f = ?", (f,)).fetchone()
    assert err2 == err


def test_a_pond_that_moves_on_closes_runs_left_behind(tmp_path):
    """The general invariant: a Pond not in flight has no 'running' rows. A Run is normally closed only by
    the Duck reporting IT, so any path where the Pond advances without that report — a vanished Duck, a
    replaced spawn — strands one. Reconciliation covers the paths not yet enumerated, which matters
    because the live sighting resisted reproduction."""
    d = _inlet_driver(tmp_path, "moved.db", _DeadLauncher())
    d.pulse("src@1")
    stale_f = d.state.pond_states["src@1"].start_f.isoformat()
    assert _run_rows(d) == ["running"]

    # A later Run completes without the earlier one ever being reported.
    later = d.state.pond_states["src@1"].start_f + timedelta(seconds=30)
    d._dispatch_begin_run("src@1", later, later)
    d.on_event("src@1", {"kind": "ripple", "ripple": "r1", "f": later.isoformat(), "status": "success"})
    d.on_event("src@1", {"kind": "run_completed", "f": later.isoformat()})

    rows = dict(d.db.execute("SELECT f, status FROM pond_run").fetchall())
    assert rows[later.isoformat()] == "success"
    assert rows[stale_f] == "failed", f"the abandoned earlier Run was left behind: {rows}"


def test_closing_abandoned_runs_never_rewrites_a_reported_outcome(tmp_path):
    """A Run the Duck already reported keeps its own outcome — reconciliation only touches rows still
    sitting at 'running'."""
    d = _inlet_driver(tmp_path, "keep.db", _DeadLauncher())
    d.pulse("src@1")
    first = d.state.pond_states["src@1"].start_f
    d.on_event("src@1", {"kind": "failed", "ripple": "r1", "f": first.isoformat(),
                         "status": "failed", "error": "boom"})
    later = first + timedelta(seconds=30)
    d._dispatch_begin_run("src@1", later, later)
    d.on_event("src@1", {"kind": "ripple", "ripple": "r1", "f": later.isoformat(), "status": "success"})
    d.on_event("src@1", {"kind": "run_completed", "f": later.isoformat()})

    rows = dict(d.db.execute("SELECT f, status FROM pond_run").fetchall())
    assert rows[first.isoformat()] == "failed"
    (err,) = d.db.execute("SELECT error FROM pond_run WHERE f = ?", (first.isoformat(),)).fetchone()
    assert err == "boom", "the Duck's own failure message was overwritten"
