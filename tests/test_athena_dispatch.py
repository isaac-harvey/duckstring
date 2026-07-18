"""The Athena dispatch surface: the ``@ripple(athena=…)`` declaration, mode resolution
(ripple > runtime default > unconfigured ⇒ off), shape eligibility, and the upgrade posture's
OOM fail-up (bounded, materialised local probe → Athena retry). The Athena *execution* side is
exercised by injection — no AWS in the suite."""

from __future__ import annotations

from datetime import datetime, timezone

import duckdb
import pytest

from duckstring import athena_backend as ab
from duckstring.core import Pond, collect_ripples, ripple

UTC = timezone.utc

ATHENA_ENV = {
    "DUCKSTRING_ATHENA_WORKGROUP": "wg",
    "DUCKSTRING_GLUE_DATABASE": "db",
    "DUCKSTRING_ATHENA_SCRATCH": "s3://bucket/scratch",
}


# ─── declaration ────────────────────────────────────────────────────────────────


def test_ripple_athena_declaration_is_stamped_and_registered():
    @ripple(athena="always")
    def heavy(pond): ...

    @ripple
    def plain(pond): ...

    regs = {r["name"]: r for r in collect_ripples()}
    assert heavy._ds_athena == "always"
    assert regs["heavy"]["athena"] == "always"
    assert plain._ds_athena is None and regs["plain"]["athena"] is None


def test_ripple_athena_rejects_unknown_mode():
    with pytest.raises(ValueError, match="athena='sometimes'"):
        @ripple(athena="sometimes")
        def bad(pond): ...


# ─── mode resolution ────────────────────────────────────────────────────────────


def test_unconfigured_runtime_is_off_regardless_of_declaration(monkeypatch):
    for var in ATHENA_ENV:
        monkeypatch.delenv(var, raising=False)
    cfg = ab.Config.from_env()
    assert not cfg.enabled
    assert cfg.resolve_mode("always") == "off"


def test_ripple_declaration_beats_runtime_default(monkeypatch):
    for k, v in ATHENA_ENV.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("DUCKSTRING_ATHENA_MODE", "always")
    cfg = ab.Config.from_env()
    assert cfg.resolve_mode(None) == "always"
    assert cfg.resolve_mode("off") == "off"
    assert cfg.resolve_mode("upgrade") == "upgrade"


def test_runtime_default_is_upgrade(monkeypatch):
    for k, v in ATHENA_ENV.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("DUCKSTRING_ATHENA_MODE", raising=False)
    monkeypatch.delenv("DUCKSTRING_ATHENA_FORCE", raising=False)
    assert ab.Config.from_env().resolve_mode(None) == "upgrade"


# ─── the terminal hook, end to end against a real registry ──────────────────────


def _pond(tmp_path, athena=None):
    con = duckdb.connect(str(tmp_path / "reg.duckdb"))
    pond = Pond(name="p", version="1", con=con, root=tmp_path,
                f=datetime(2026, 7, 18, tzinfo=UTC), athena=athena)
    return pond


def _mark_source(pond):
    # Publish `src` as a merge Trickle via the real write path (registers meta + main).
    pond.merge_table("src", pond.con.sql("SELECT range AS id, range % 7 AS v FROM range(1000)"),
                     pk="id")


def test_always_mode_dispatches_via_injected_engine(tmp_path, monkeypatch):
    for k, v in ATHENA_ENV.items():
        monkeypatch.setenv(k, v)
    pond = _pond(tmp_path, athena="always")
    _mark_source(pond)
    calls = {}

    def fake_comprehensive(builder, out_pk):
        calls["dispatched"] = True
        return builder.ctx.read_table("src").project("id, v * 10 AS v10")

    monkeypatch.setattr(ab, "comprehensive", fake_comprehensive)
    pond.trickle("src").select("s0.id, s0.v * 10 AS v10").merge("out", pk="id")
    assert calls.get("dispatched")
    net = pond.con.execute(
        "SELECT count(*), sum(v10) FROM out__changelog WHERE _duckstring_d = 1"
    ).fetchone()
    assert net[0] == 1000


def test_upgrade_mode_fails_up_on_oom(tmp_path, monkeypatch):
    for k, v in ATHENA_ENV.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("DUCKSTRING_ATHENA_MIN_ROWS", "10000000")  # never clearly-over
    pond = _pond(tmp_path)  # no declaration → runtime default (upgrade)
    _mark_source(pond)
    calls = {}

    def oom_probe(builder, cfg):
        raise duckdb.OutOfMemoryException("boom: memory limit")

    def fake_comprehensive(builder, out_pk):
        calls["failed_up"] = True
        return builder.ctx.read_table("src").project("id, v")

    monkeypatch.setattr(ab, "probe_local", oom_probe)
    monkeypatch.setattr(ab, "comprehensive", fake_comprehensive)
    pond.trickle("src").select("s0.id, s0.v").merge("out", pk="id")
    assert calls.get("failed_up")


def test_upgrade_mode_incremental_epochs_stay_local(tmp_path, monkeypatch):
    for k, v in ATHENA_ENV.items():
        monkeypatch.setenv(k, v)
    pond = _pond(tmp_path)
    _mark_source(pond)

    def explode(*a, **k):
        raise AssertionError("dispatch machinery must not run for an incremental epoch")

    # Bootstrap (comprehensive-bound): the probe path runs — let it, for real.
    pond.trickle("src").select("s0.id, s0.v").merge("out", pk="id")
    # A later epoch with `out` extant is incremental-bound: the hook must bail before
    # touching any backend machinery.
    monkeypatch.setattr(ab, "comprehensive", explode)
    monkeypatch.setattr(ab, "probe_local", explode)
    monkeypatch.setattr(ab, "clearly_over", explode)
    b = pond.trickle("src").select("s0.id, s0.v")
    assert b._athena_comprehensive("out", ("id",), True) is None


def test_off_declaration_disables_even_when_runtime_says_always(tmp_path, monkeypatch):
    for k, v in ATHENA_ENV.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("DUCKSTRING_ATHENA_MODE", "always")
    pond = _pond(tmp_path, athena="off")
    _mark_source(pond)

    def explode(*a, **k):
        raise AssertionError("athena='off' must never dispatch")

    monkeypatch.setattr(ab, "comprehensive", explode)
    pond.trickle("src").select("s0.id, s0.v").merge("out", pk="id")
