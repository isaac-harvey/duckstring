"""The Prometheus /metrics endpoint (see plans/alerts.md "Metrics"). Renders the text-exposition format
from the engine state + a couple of DB rollups; it lives at the root (not /api), is unauthenticated, and
must not break the ``/api`` access-level audit."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from duckstring.catchment.db import connect, migrate
from duckstring.catchment.driver import Driver, _now
from duckstring.catchment.launcher import NoopLauncher
from duckstring.catchment.routes.deploy import _register
from duckstring.catchment.routes.metrics import render_metrics
from duckstring.keys import pond_key

pytestmark = pytest.mark.timeout(5)

_CFG = {"sources": {}, "immediate_retries": 0, "source_retries": 0, "kind": "outlet"}


def _driver(tmp_path):
    db = connect(tmp_path / "duck.db")
    migrate(db)
    _register(db, "sales", "1.0.0", "outlet", "ponds/sales/1.0.0", _CFG,
              [{"func": "f", "name": "agg", "parents": []}])
    return Driver(db, tmp_path, "http://x", NoopLauncher())


def _parse(text: str) -> dict[str, float]:
    """Map each 'metric{labels} value' sample line → value (ignoring # HELP/# TYPE)."""
    out = {}
    for line in text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        key, _, value = line.rpartition(" ")
        out[key] = float(value)
    return out


def test_render_shapes_and_up(tmp_path):
    text = render_metrics(_driver(tmp_path).metrics_snapshot())
    assert "# TYPE duckstring_up gauge" in text
    assert "# HELP duckstring_pond_freshness_lag_seconds" in text
    samples = _parse(text)
    assert samples["duckstring_up"] == 1
    # A freshly-registered, never-run Pond exposes state flags but no freshness lag (no end_f yet).
    assert samples['duckstring_pond_failed{pond="sales",major="1"}'] == 0
    assert not any(k.startswith("duckstring_pond_freshness_lag_seconds{") for k in samples)


def test_freshness_lag_and_failure_flag(tmp_path):
    d = _driver(tmp_path)
    key = pond_key("sales", 1)
    ps = d.state.pond_states[key]
    from datetime import timedelta
    ps.end_f = _now() - timedelta(seconds=90)
    samples = _parse(render_metrics(d.metrics_snapshot()))
    assert samples['duckstring_pond_freshness_lag_seconds{pond="sales",major="1",kind="outlet"}'] >= 89

    # A failure bumps the flag *and* the cumulative counter.
    now = _now()
    ps.start_f = now
    d._fail_whole_pond(key, now, "boom")
    samples = _parse(render_metrics(d.metrics_snapshot()))
    assert samples['duckstring_pond_failed{pond="sales",major="1"}'] == 1
    assert samples['duckstring_pond_failures_total{pond="sales",major="1"}'] == 1


def test_alert_delivery_counts(tmp_path):
    d = _driver(tmp_path)
    d.add_channel("ops", "https://x/ops", scope=None, events="failure")
    d._emit_alert("failure", scope_pond="sales", severity="error", title="t", message="m", f="F1")
    samples = _parse(render_metrics(d.metrics_snapshot()))
    assert samples['duckstring_alert_deliveries_total{status="pending"}'] == 1
    assert samples['duckstring_alert_deliveries_total{status="sent"}'] == 0


def test_label_escaping():
    # A spout name carries a '#'; a hostile value with quotes/backslashes must be escaped, not break parsing.
    snap = {
        "nodes": [{"name": 'a"b\\c', "major": 1, "kind": "outlet", "is_spout": True, "is_draw": False,
                   "lag_seconds": 5.0, "runs_completed": 0, "is_failed": False, "is_blocked": False,
                   "is_killed": False}],
        "failures": {}, "alert_deliveries": {},
    }
    text = render_metrics(snap)
    assert 'spout="a\\"b\\\\c"' in text


def test_endpoint_is_open_and_outside_api_audit():
    from fastapi.testclient import TestClient

    from duckstring.catchment.app import create_app

    # A configured full key gates /api — but /metrics is unauthenticated (the scraper posture). create_app
    # also runs audit_routes at boot; if /metrics were an unclassified /api route it would raise here.
    app = create_app(Path(tempfile.mkdtemp()), api_key="secret-full-key")
    with TestClient(app) as c:
        assert c.get("/api/status").status_code == 401  # /api needs the key
        r = c.get("/metrics")                            # /metrics does not
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/plain; version=0.0.4")
        assert "duckstring_up 1" in r.text


def test_cost_families_target_flock_runtime(tmp_path):
    """The compute-cost signals (plans/cloud-config.md increment 4): duck target, Flock posture, and
    cumulative Duck execution seconds."""
    d = _driver(tmp_path)
    key = pond_key("sales", 1)
    d.add_pool("heavy", instance_type="m6i.large")
    d.set_duck(key, duck_target="heavy", flock_mode="upgrade", flock_engine="athena")
    # A completed Ripple Run gives a runtime span to sum.
    (pv_id,) = d.db.execute("SELECT id FROM pond_version LIMIT 1").fetchone()
    (rip_id,) = d.db.execute("SELECT id FROM ripple WHERE pond_version_id = ?", (pv_id,)).fetchone()
    d.db.execute("INSERT INTO pond_run (pond_version_id, f, status) "
                 "VALUES (?, '2026-01-01T00:00:00+00:00', 'success')", (pv_id,))
    d.db.execute(
        "INSERT INTO ripple_run (pond_version_id, ripple_id, f, retry, status, started_at, finished_at) "
        "VALUES (?, ?, '2026-01-01T00:00:00+00:00', 0, 'success', "
        "'2026-01-01T00:00:00+00:00', '2026-01-01T00:00:12+00:00')",
        (pv_id, rip_id),
    )
    d.db.commit()
    samples = _parse(render_metrics(d.metrics_snapshot()))
    assert samples['duckstring_pond_duck_target{pond="sales",major="1",target="heavy"}'] == 1
    assert samples['duckstring_pond_flock{pond="sales",major="1",mode="upgrade",engine="athena"}'] == 1
    assert samples['duckstring_pond_run_seconds_total{pond="sales",major="1"}'] == pytest.approx(12, abs=0.1)


def test_flock_dispatch_counters_ride_run_completed(tmp_path):
    """A failed Flock dispatch still completes the run locally — the counters (shipped by the Duck on
    run_completed) are the only tell it is degrading. They surface on /metrics and as the pond's
    flock_error on /api/status; absent entirely until a Duck ever reports them."""
    d = _driver(tmp_path)
    key = pond_key("sales", 1)
    text = render_metrics(d.metrics_snapshot())
    assert "flock_dispatched_total{" not in text  # nothing reported yet → no samples
    d.pulse(key)
    f = d.state.pond_states[key].start_f.isoformat()
    d.on_event(key, {"kind": "ripple", "ripple": "agg", "f": f, "status": "success"})
    d.on_event(key, {"kind": "run_completed", "f": f, "status": "success",
                     "flock": {"dispatched": 3, "failed": 2, "last_error": "AccessDenied: nope"}})
    samples = _parse(render_metrics(d.metrics_snapshot()))
    assert samples['duckstring_flock_dispatched_total{pond="sales",major="1"}'] == 3
    assert samples['duckstring_flock_dispatch_failures_total{pond="sales",major="1"}'] == 2
    pond = next(p for p in d.status()["ponds"] if p["id"] == key)
    assert pond["flock_error"] == "AccessDenied: nope"
