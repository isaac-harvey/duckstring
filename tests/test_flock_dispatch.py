"""The Flock dispatch surface: the ``@ripple(flock=…)`` declaration, mode resolution
(ripple > runtime default > unconfigured ⇒ off), engine selection, and the upgrade posture's
OOM fail-up (bounded, materialised local probe → engine dispatch). The engine is faked — no
AWS in the suite; the real Athena engine is exercised by the cloud e2e."""

from __future__ import annotations

from datetime import datetime, timezone

import duckdb
import pytest

from duckstring import flock
from duckstring.core import Pond, collect_ripples, ripple

UTC = timezone.utc

# Enough env to make the default (athena) engine report enabled.
FLOCK_ENV = {
    "DUCKSTRING_FLOCK_ATHENA_WORKGROUP": "wg",
    "DUCKSTRING_FLOCK_ATHENA_DATABASE": "db",
    "DUCKSTRING_FLOCK_ATHENA_SCRATCH": "s3://bucket/scratch",
}


class FakeEngine:
    """A FlockEngine that records calls and returns a caller-supplied relation — the injection
    point for the terminal-level tests (monkeypatch ``flock.get_engine``)."""

    def __init__(self, *, rows=100, result_fn=None, eligible=None):
        self.rows, self.result_fn, self._eligible = rows, result_fn, eligible
        self.dispatched = False

    def enabled(self):
        return True

    def eligible(self, builder):
        return self._eligible

    def estimate_rows(self, builder):
        return self.rows

    def dispatch(self, builder, out_pk):
        self.dispatched = True
        return self.result_fn(builder) if self.result_fn else None


# ─── declaration ────────────────────────────────────────────────────────────────


def test_ripple_flock_declaration_is_stamped_and_registered():
    @ripple(flock="always")
    def heavy(pond): ...

    @ripple
    def plain(pond): ...

    regs = {r["name"]: r for r in collect_ripples()}
    assert heavy._ds_flock == "always"
    assert regs["heavy"]["flock"] == "always"
    assert plain._ds_flock is None and regs["plain"]["flock"] is None


def test_ripple_flock_rejects_unknown_mode():
    with pytest.raises(ValueError, match="flock='sometimes'"):
        @ripple(flock="sometimes")
        def bad(pond): ...


# ─── engine selection + mode resolution ─────────────────────────────────────────


def test_unconfigured_runtime_has_no_engine():
    assert flock.get_engine({}) is None
    assert not flock.enabled({})


def test_default_engine_is_athena_when_configured():
    engine = flock.get_engine(FLOCK_ENV)
    assert engine is not None and engine.__class__.__name__ == "AthenaEngine"


def test_unknown_engine_is_off():
    assert flock.get_engine({**FLOCK_ENV, "DUCKSTRING_FLOCK_ENGINE": "nope"}) is None


def test_ripple_declaration_beats_runtime_default():
    assert flock.resolve_mode("always", {"DUCKSTRING_FLOCK_MODE": "off"}) == "always"
    assert flock.resolve_mode("off", {"DUCKSTRING_FLOCK_MODE": "always"}) == "off"
    assert flock.resolve_mode(None, {"DUCKSTRING_FLOCK_MODE": "always"}) == "always"


def test_runtime_default_is_upgrade():
    assert flock.resolve_mode(None, {}) == "upgrade"


# ─── the terminal hook, end to end against a real registry ──────────────────────


def _pond(tmp_path, flock_mode=None):
    con = duckdb.connect(str(tmp_path / "reg.duckdb"))
    pond = Pond(name="p", version="1", con=con, root=tmp_path,
                f=datetime(2026, 7, 19, tzinfo=UTC), flock=flock_mode)
    return pond


def _mark_source(pond):
    # Publish `src` as a merge Trickle via the real write path (registers meta + main).
    pond.merge_table("src", pond.con.sql("SELECT range AS id, range % 7 AS v FROM range(1000)"),
                     pk="id")


def test_always_mode_dispatches_via_injected_engine(tmp_path, monkeypatch):
    for k, v in FLOCK_ENV.items():
        monkeypatch.setenv(k, v)
    pond = _pond(tmp_path, flock_mode="always")
    _mark_source(pond)
    engine = FakeEngine(result_fn=lambda b: b.ctx.read_table("src").project("id, v * 10 AS v10"))
    monkeypatch.setattr(flock, "get_engine", lambda env=None: engine)

    pond.trickle("src").select("s0.id, s0.v * 10 AS v10").merge("out", pk="id")
    assert engine.dispatched
    net = pond.con.execute(
        "SELECT count(*) FROM out__changelog WHERE _duckstring_d = 1"
    ).fetchone()[0]
    assert net == 1000


def test_upgrade_mode_fails_up_on_oom(tmp_path, monkeypatch):
    for k, v in FLOCK_ENV.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("DUCKSTRING_FLOCK_MIN_ROWS", "10000000")  # never clearly-over
    pond = _pond(tmp_path)  # no declaration → runtime default (upgrade)
    _mark_source(pond)
    engine = FakeEngine(rows=1000, result_fn=lambda b: b.ctx.read_table("src").project("id, v"))
    monkeypatch.setattr(flock, "get_engine", lambda env=None: engine)

    def oom_probe(builder):
        raise duckdb.OutOfMemoryException("boom: memory limit")

    monkeypatch.setattr(flock, "_probe_local", oom_probe)
    pond.trickle("src").select("s0.id, s0.v").merge("out", pk="id")
    assert engine.dispatched  # failed up to the engine


def test_upgrade_mode_clearly_over_dispatches_up_front(tmp_path, monkeypatch):
    for k, v in FLOCK_ENV.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("DUCKSTRING_FLOCK_MIN_ROWS", "100")  # 1000 source rows ≥ 100 → over
    pond = _pond(tmp_path)
    _mark_source(pond)
    engine = FakeEngine(rows=1000, result_fn=lambda b: b.ctx.read_table("src").project("id, v"))
    monkeypatch.setattr(flock, "get_engine", lambda env=None: engine)

    def no_probe(builder):
        raise AssertionError("clearly-over must dispatch up front, not probe locally")

    monkeypatch.setattr(flock, "_probe_local", no_probe)
    pond.trickle("src").select("s0.id, s0.v").merge("out", pk="id")
    assert engine.dispatched


def test_incremental_epoch_never_dispatches(tmp_path, monkeypatch):
    for k, v in FLOCK_ENV.items():
        monkeypatch.setenv(k, v)
    pond = _pond(tmp_path)
    _mark_source(pond)
    engine = FakeEngine(result_fn=lambda b: b.ctx.read_table("src").project("id, v"))
    monkeypatch.setattr(flock, "get_engine", lambda env=None: engine)
    # comprehensive_bound=False (an incremental epoch) must bail before any engine call.
    b = pond.trickle("src").select("s0.id, s0.v")
    assert flock.comprehensive(b, ("id",), ripple_mode=None, comprehensive_bound=False) is None
    assert not engine.dispatched


def test_off_declaration_disables_even_when_runtime_says_always(tmp_path, monkeypatch):
    for k, v in FLOCK_ENV.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("DUCKSTRING_FLOCK_MODE", "always")
    pond = _pond(tmp_path, flock_mode="off")
    _mark_source(pond)
    engine = FakeEngine(result_fn=lambda b: b.ctx.read_table("src").project("id, v"))
    monkeypatch.setattr(flock, "get_engine", lambda env=None: engine)

    pond.trickle("src").select("s0.id, s0.v").merge("out", pk="id")
    assert not engine.dispatched
