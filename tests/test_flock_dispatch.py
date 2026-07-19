"""The Flock dispatch surface: the Pond-level posture (Pond mode > runtime default > unconfigured
⇒ off), engine selection, the upgrade posture's OOM fail-up (bounded, materialised local probe →
engine dispatch), and the fail policy (hard cap). The engine is faked — no AWS in the suite; the
real Athena engine is exercised by the cloud e2e."""

from __future__ import annotations

from datetime import datetime, timezone

import duckdb
import pytest

from duckstring import flock
from duckstring.core import Pond

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


# ─── engine selection + mode resolution ─────────────────────────────────────────


def test_unconfigured_runtime_has_no_engine():
    assert flock.get_engine({}) is None
    assert not flock.enabled({})


def test_default_engine_is_athena_when_configured():
    engine = flock.get_engine(FLOCK_ENV)
    assert engine is not None and engine.__class__.__name__ == "AthenaEngine"


def test_unknown_engine_is_off():
    assert flock.get_engine({**FLOCK_ENV, "DUCKSTRING_FLOCK_ENGINE": "nope"}) is None


def test_pond_posture_beats_runtime_default():
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
    assert flock.comprehensive(b, ("id",), pond_mode=None, comprehensive_bound=False) is None
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


def test_oom_policy_fail_is_a_hard_cap(tmp_path, monkeypatch):
    for k, v in FLOCK_ENV.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("DUCKSTRING_FLOCK_MIN_ROWS", "10000000")  # never clearly-over → local probe
    monkeypatch.setenv("DUCKSTRING_FLOCK_OOM_POLICY", "fail")
    pond = _pond(tmp_path)
    _mark_source(pond)
    engine = FakeEngine(rows=1000, result_fn=lambda b: b.ctx.read_table("src").project("id, v"))
    monkeypatch.setattr(flock, "get_engine", lambda env=None: engine)

    def oom_probe(builder):
        raise duckdb.OutOfMemoryException("boom: memory limit")

    monkeypatch.setattr(flock, "_probe_local", oom_probe)
    # oom_policy=fail: the OOM surfaces (a Pond failure), the engine is NOT offloaded to.
    with pytest.raises(duckdb.OutOfMemoryException):
        pond.trickle("src").select("s0.id, s0.v").merge("out", pk="id")
    assert not engine.dispatched


def test_min_rows_derives_from_memory_cap_not_size():
    # No abstract size preset anymore: the "clearly over" threshold comes from the real memory cap,
    # an explicit override wins, and a stock Duck (no cap) gets the conservative default.
    assert flock._min_rows({"DUCKSTRING_FLOCK_MIN_ROWS": "123"}) == 123
    assert flock._min_rows({"DUCKSTRING_MEMORY_LIMIT": "8GB"}) == 8 * flock._ROWS_PER_GIB
    assert flock._min_rows({"DUCKSTRING_MEMORY_LIMIT": "512MB"}) == flock._ROWS_PER_GIB // 2
    assert flock._min_rows({}) == flock._DEFAULT_MIN_ROWS
