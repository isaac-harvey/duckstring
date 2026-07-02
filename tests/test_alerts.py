"""Alerts — failure & freshness notifications (see plans/alerts.md).

Covers the notifier seam (destination/event validation, the registry, the Slack-compatible + sanitised
webhook payload), channel CRUD + persistence across a restart, `_emit_alert` routing/scope/dedup, the
failure→recovery lifecycle through the engine, and the tick-driven freshness-SLA sweep. Execution against a
real SMTP/webhook backend is out of scope here (the worker just calls `Notifier.send`)."""

from __future__ import annotations

from datetime import timedelta

import pytest

from duckstring.alerts import (
    AlertEvent,
    NotifierError,
    get_notifier,
    normalise_events,
    parse_notifier_destination,
)
from duckstring.catchment.db import connect, migrate
from duckstring.catchment.driver import Driver, _now
from duckstring.catchment.launcher import NoopLauncher
from duckstring.catchment.routes.deploy import _register
from duckstring.engine.core import NEVER
from duckstring.keys import pond_key

pytestmark = pytest.mark.timeout(5)

_CFG = {"sources": {}, "immediate_retries": 0, "source_retries": 0, "kind": "outlet"}


def _driver(tmp_path):
    db = connect(tmp_path / "duck.db")
    migrate(db)
    _register(db, "sales", "1.0.0", "outlet", "ponds/sales/1.0.0", _CFG,
              [{"func": "f", "name": "agg", "parents": []}])
    return Driver(db, tmp_path, "http://x", NoopLauncher())


def _deliveries(driver):
    return driver.db.execute(
        "SELECT event_kind, pond_name, severity, dedup_key FROM alert_delivery ORDER BY id"
    ).fetchall()


# ─── Notifier seam: destination + event validation ──────────────────────────────


def test_parse_destination_known_schemes():
    assert parse_notifier_destination("https://hooks.example/x").scheme == "https"
    assert parse_notifier_destination("http://localhost:9000/hook").scheme == "http"
    assert parse_notifier_destination("mailto:ops@example.com").scheme == "mailto"


def test_parse_destination_rejects_unknown_and_empty():
    with pytest.raises(NotifierError, match="unsupported alert destination scheme"):
        parse_notifier_destination("ftp://host/x")
    with pytest.raises(NotifierError):
        parse_notifier_destination("")
    with pytest.raises(NotifierError, match="no scheme"):
        parse_notifier_destination("not-a-uri")


def test_parse_destination_validates_credential_syntax():
    d = parse_notifier_destination("https://x/${secret:HOOK}")
    assert "${secret:HOOK}" in d.raw
    with pytest.raises(NotifierError):
        parse_notifier_destination("https://x/${env:}")


def test_normalise_events():
    assert normalise_events("all") == normalise_events(None)
    assert normalise_events("failure,recovery") == ("failure", "recovery")
    assert normalise_events("failure,failure") == ("failure",)  # de-duplicated
    with pytest.raises(ValueError, match="unknown alert event"):
        normalise_events("failure,explode")


def test_get_notifier_resolves_by_scheme():
    assert type(get_notifier("https://x/y")).__name__ == "WebhookNotifier"
    assert type(get_notifier("mailto:a@b.com?smtp=h:25")).__name__ == "EmailNotifier"


def test_email_requires_recipient_and_smtp():
    with pytest.raises(NotifierError, match="SMTP server"):
        get_notifier("mailto:a@b.com")  # no ?smtp= and no DUCKSTRING_SMTP_HOST


# ─── Webhook payload: Slack-compatible + sanitised ──────────────────────────────


def test_webhook_send_posts_slack_compatible_sanitised_payload(monkeypatch):
    captured = {}

    class _Resp:
        def raise_for_status(self):
            return None

    def _fake_post(url, json, timeout):
        captured["url"] = url
        captured["json"] = json
        return _Resp()

    import httpx
    monkeypatch.setattr(httpx, "post", _fake_post)

    get_notifier("https://hooks.example/svc").send(AlertEvent(
        kind="failure", pond="sales", title="Pond 'sales' failed",
        message="Ripple 'agg' failed: boom", f="2026-07-01T00:00:00+00:00",
    ))
    body = captured["json"]
    assert body["text"].startswith("ERROR")  # Slack renders `text`
    assert body["kind"] == "failure" and body["pond"] == "sales"
    # Sanitised: a traceback never leaves the Catchment.
    assert "traceback" not in body and "traceback" not in body.get("detail", {})


def test_webhook_send_sanitises_http_error(monkeypatch):
    import httpx

    class _Resp:
        status_code = 500

        def raise_for_status(self):
            raise httpx.HTTPStatusError("nope", request=None, response=self)

    monkeypatch.setattr(httpx, "post", lambda url, json, timeout: _Resp())
    with pytest.raises(NotifierError, match="returned 500"):
        get_notifier("https://hooks.example/secret-token").send(
            AlertEvent(kind="failure", pond="x", title="t", message="m"))


# ─── Channel CRUD + persistence ─────────────────────────────────────────────────


def test_channel_crud_and_persistence_across_restart(tmp_path):
    d = _driver(tmp_path)
    d.add_channel("ops", "https://hooks.example/x", scope=None, events="failure,recovery")
    d.add_channel("sales-sla", "mailto:me@x.com?smtp=h:25", scope="sales", events="freshness",
                  stale_ms=3_600_000)
    chans = {c["name"]: c for c in d.list_channels()}
    assert chans["ops"]["scope"] is None and chans["ops"]["events"] == "failure,recovery"
    assert chans["sales-sla"]["scope"] == "sales" and chans["sales-sla"]["stale_ms"] == 3_600_000

    # A restart (a fresh Driver over the same DB) sees the persisted channels.
    d2 = Driver(connect(tmp_path / "duck.db"), tmp_path, "http://x", NoopLauncher())
    assert {c["name"] for c in d2.list_channels()} == {"ops", "sales-sla"}
    assert d2.remove_channel("ops") is True
    assert d2.remove_channel("ops") is False  # already gone
    assert {c["name"] for c in d2.list_channels()} == {"sales-sla"}


def test_add_channel_rejects_bad_destination_and_duplicate(tmp_path):
    d = _driver(tmp_path)
    with pytest.raises(ValueError):
        d.add_channel("bad", "ftp://x", scope=None)
    d.add_channel("ops", "https://x/y", scope=None)
    with pytest.raises(ValueError, match="already exists"):
        d.add_channel("ops", "https://x/z", scope=None)


# ─── _emit_alert: routing, scope, event filter, dedup ───────────────────────────


def test_emit_routes_by_scope_and_event_filter(tmp_path):
    d = _driver(tmp_path)
    d.add_channel("wide", "https://x/wide", scope=None, events="failure")
    d.add_channel("sales-only", "https://x/sales", scope="sales", events="failure")
    d.add_channel("other-pond", "https://x/other", scope="orders", events="failure")
    d.add_channel("recovery-only", "https://x/rec", scope=None, events="recovery")

    d._emit_alert("failure", scope_pond="sales", severity="error", title="t", message="m", f="F1")
    # wide (catchment-wide) + sales-only (scope match) fire; other-pond (scope miss) + recovery-only
    # (event miss) do not.
    got = d.db.execute(
        "SELECT c.name FROM alert_delivery d JOIN alert_channel c ON c.id = d.channel_id"
    ).fetchall()
    assert sorted(r[0] for r in got) == ["sales-only", "wide"]


def test_emit_scope_is_name_at_major(tmp_path):
    """A channel scoped to name@major matches only that line; a bare-name scope matches any major;
    catchment-wide matches all (plans/remove-pond.md prerequisite)."""
    d = _driver(tmp_path)
    d.add_channel("wide", "https://x/wide", scope=None, events="failure")
    d.add_channel("sales-any", "https://x/any", scope="sales", events="failure")     # any major
    d.add_channel("sales-m1", "https://x/m1", scope="sales@1", events="failure")      # one line
    d.add_channel("sales-m2", "https://x/m2", scope="sales@2", events="failure")

    def fired(major):
        d.db.execute("DELETE FROM alert_delivery")
        d._emit_alert("failure", scope_pond="sales", scope_major=major, severity="error",
                      title="t", message="m", f="F1")
        return sorted(r[0] for r in d.db.execute(
            "SELECT c.name FROM alert_delivery a JOIN alert_channel c ON c.id = a.channel_id"))

    assert fired(1) == ["sales-any", "sales-m1", "wide"]
    assert fired(2) == ["sales-any", "sales-m2", "wide"]
    # The scope round-trips through the DB as "name@major" / "name".
    scopes = {c["name"]: c["scope"] for c in d.list_channels()}
    assert scopes["sales-m1"] == "sales@1" and scopes["sales-any"] == "sales" and scopes["wide"] is None


def test_emit_dedup_is_per_episode(tmp_path):
    d = _driver(tmp_path)
    d.add_channel("wide", "https://x/wide", scope=None, events="failure")
    d._emit_alert("failure", scope_pond="sales", severity="error", title="t", message="m", f="F1")
    d._emit_alert("failure", scope_pond="sales", severity="error", title="t", message="m", f="F1")
    assert len(_deliveries(d)) == 1  # same episode (same f) → one alert
    d._emit_alert("failure", scope_pond="sales", severity="error", title="t", message="m", f="F2")
    assert len(_deliveries(d)) == 2  # a new failed freshness → a new alert


def test_emit_noop_without_channels(tmp_path):
    d = _driver(tmp_path)
    d._emit_alert("failure", scope_pond="sales", severity="error", title="t", message="m", f="F1")
    assert _deliveries(d) == []


# ─── Failure → recovery lifecycle through the engine ────────────────────────────


def test_failure_then_recovery_lifecycle(tmp_path):
    d = _driver(tmp_path)
    d.add_channel("ops", "https://x/ops", scope=None, events="failure,recovery")
    key = pond_key("sales", 1)
    ps = d.state.pond_states[key]

    # A Run is in flight (start_f > end_f) → fail it.
    now = _now()
    ps.start_f = now
    d._fail_whole_pond(key, now, "kaboom")
    kinds = [row[0] for row in _deliveries(d)]
    assert kinds == ["failure"]
    assert d.state.pond_states[key].is_failed is True

    # Clear the failure → the next _process emits exactly one recovery.
    d.clear(key)
    kinds = [row[0] for row in _deliveries(d)]
    assert kinds == ["failure", "recovery"]
    # Idempotent: another _process does not re-emit.
    d._process(_now())
    assert [row[0] for row in _deliveries(d)] == ["failure", "recovery"]


def test_killed_pond_is_not_a_recovery(tmp_path):
    d = _driver(tmp_path)
    d.add_channel("ops", "https://x/ops", scope=None, events="failure,recovery")
    key = pond_key("sales", 1)
    now = _now()
    d.state.pond_states[key].start_f = now
    d._fail_whole_pond(key, now, "kaboom")
    d.kill(key)  # a kill is intentional — not a recovery
    assert [row[0] for row in _deliveries(d)] == ["failure"]


# ─── Freshness SLA sweep ────────────────────────────────────────────────────────


def test_freshness_breach_then_recovery(tmp_path):
    d = _driver(tmp_path)
    d.add_channel("sla", "https://x/sla", scope="sales", events="freshness", stale_ms=60_000)
    key = pond_key("sales", 1)
    ps = d.state.pond_states[key]

    now = _now()
    ps.end_f = now - timedelta(seconds=120)  # 2 min stale, SLA 60s
    d._check_freshness(now)
    assert [(r[0], r[2]) for r in _deliveries(d)] == [("freshness", "warning")]

    # Still stale on the next tick → no duplicate (in-memory breach set + outbox dedup).
    d._check_freshness(now)
    assert len(_deliveries(d)) == 1

    # It advances back within SLA → one recovery.
    ps.end_f = now
    d._check_freshness(now)
    assert [r[0] for r in _deliveries(d)] == ["freshness", "recovery"]


def test_freshness_skips_never_run_pond(tmp_path):
    d = _driver(tmp_path)
    d.add_channel("sla", "https://x/sla", scope=None, events="freshness", stale_ms=1_000)
    assert d.state.pond_states[pond_key("sales", 1)].end_f == NEVER
    d._check_freshness(_now())
    assert _deliveries(d) == []  # never-run → nothing to be stale about


# ─── Worker delivery bookkeeping ────────────────────────────────────────────────


def test_worker_marks_sent_and_parks_failed(tmp_path):
    d = _driver(tmp_path)
    d.add_channel("ok", "https://x/ok", scope=None, events="failure")
    d.add_channel("bad", "https://x/bad", scope=None, events="failure")
    d._emit_alert("failure", scope_pond="sales", severity="error", title="t", message="m", f="F1")

    pending = d.take_alert_deliveries()
    assert len(pending) == 2
    by_dest = {p["destination"]: p["id"] for p in pending}

    d.mark_delivery_sent(by_dest["https://x/ok"])
    # A permanently-failing channel: bump attempts until it parks 'failed' at the cap.
    bad_id = by_dest["https://x/bad"]
    for _ in range(6):
        d.mark_delivery_failed(bad_id, "boom", max_attempts=6)

    log = {r["channel"]: r for r in d.deliveries()}
    assert log["ok"]["status"] == "sent"
    assert log["bad"]["status"] == "failed" and log["bad"]["attempts"] == 6
    # A parked delivery is no longer offered to the worker.
    assert d.take_alert_deliveries() == []
