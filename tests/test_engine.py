"""Behavioural tests for the pure orchestration engine (``duckstring.engine``).

The engine has no run durations and no clock of its own — it is told when runs finish. So these
tests own a small :class:`Driver` that plays the future orchestrator: it holds sim-time ``now`` and
per-Ripple durations, launches whatever ``sentinel`` reports, and feeds completions back. Sim-time is
just the ``now`` argument, fully decoupled from wall-clock — the loop never sleeps. A 120 s sim is
~1200 pure calls and runs in milliseconds.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from duckstring.engine import (
    NEVER,
    EngineState,
    Pond,
    PondState,
    Ripple,
    RippleState,
    Trigger,
    Window,
    block_on_missing_asset,
    clear_missing_asset,
    clear_pond,
    complete_ripple,
    derive_blocked,
    fail_pond,
    fail_ripple,
    force_pond,
    kill_pond,
    next_wake,
    pond_set_has_pull,
    pond_source_f,
    pulse_pond,
    ripple_source_f,
    sentinel,
    sleep_pond,
    tap_pond,
    tick,
    wake_pond,
)

UTC = timezone.utc
T0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
STEP = timedelta(milliseconds=100)


def secs(x: float) -> timedelta:
    return timedelta(seconds=x)


# ─── Topology construction ────────────────────────────────────────────────────


def build(ponds: list[Pond], ripples: list[Ripple], triggers: list[Trigger] | None = None) -> EngineState:
    return EngineState(
        ponds={p.id: p for p in ponds},
        pond_states={p.id: PondState() for p in ponds},
        ripples={r.id: r for r in ripples},
        ripple_states={r.id: RippleState() for r in ripples},
        triggers={t.pond_id: t for t in (triggers or [])},
    )


def test_standing_wake_delivers_on_source_advance_without_soliciting():
    """A Spout node (standing Wake): runs whenever its Source is fresher, never solicits the Source
    (a Wake, not a Wave), and does not re-run until the Source advances again."""
    src = Pond("src", "src")
    spt = Pond("spt", "spt", sources=["src"], is_spout=True)  # terminal, no Ripples — run by the worker
    s = build([src, spt], [Ripple("r", "src", "r")])
    s.pond_states["spt"].standing_wake = True

    s.pond_states["src"].start_f = s.pond_states["src"].end_f = T0  # the Source publishes a freshness
    s = tick(T0, s)
    s, _ = sentinel(T0, s)
    assert s.pond_states["spt"].start_f == T0                       # delivered at the Source's freshness
    assert s.pond_states["src"].has_received_pull is False          # a Wake never solicits the Source
    assert "spt" in [b.pond_id for b in s.pending_begin_runs]

    # Complete the delivery; the Source isn't fresher → it does not re-run.
    s.pond_states["spt"].end_f = s.pond_states["spt"].start_f
    s.pending_begin_runs.clear()
    s = tick(T0 + STEP, s)
    s, _ = sentinel(T0 + STEP, s)
    assert s.pending_begin_runs == []

    # The Source advances → it delivers again.
    s.pond_states["src"].start_f = s.pond_states["src"].end_f = T0 + secs(10)
    s = tick(T0 + secs(10), s)
    s, _ = sentinel(T0 + secs(10), s)
    assert s.pond_states["spt"].start_f == T0 + secs(10)
    assert "spt" in [b.pond_id for b in s.pending_begin_runs]


def test_windowed_spout_fires_once_per_window():
    """A windowed Spout uses the same window mechanics as an Inlet: it fires once per window, stamped
    with the window end (the throttle clock), not the source freshness."""
    win = Window(start_anchor=T0, duration=timedelta(days=1), freq_unit="DAY", freq_interval=1)  # back-to-back daily
    spt = Pond("spt", "spt", sources=["src"], is_spout=True, windows=[win])
    s = build([Pond("src", "src"), spt], [])
    s.pond_states["spt"].standing_wake = True
    s.pond_states["src"].start_f = s.pond_states["src"].end_f = datetime(2026, 6, 10, 12, 0, tzinfo=UTC)

    at = datetime(2026, 6, 10, 12, 0, tzinfo=UTC)
    s = tick(at, s)
    s, _ = sentinel(at, s)
    assert s.pond_states["spt"].start_f == datetime(2026, 6, 11, 0, 0, tzinfo=UTC)  # the window end, not 12:00
    assert "spt" in [b.pond_id for b in s.pending_begin_runs]

    # Later in the same window → no re-fire (window end unchanged).
    s.pond_states["spt"].end_f = s.pond_states["spt"].start_f
    s.pending_begin_runs.clear()
    later = datetime(2026, 6, 10, 18, 0, tzinfo=UTC)
    s = tick(later, s)
    s, _ = sentinel(later, s)
    assert s.pending_begin_runs == []

    # Next window → fires again, stamped with the next window's end.
    nxt = datetime(2026, 6, 11, 1, 0, tzinfo=UTC)
    s.pond_states["src"].end_f = nxt
    s = tick(nxt, s)
    s, _ = sentinel(nxt, s)
    assert s.pond_states["spt"].start_f == datetime(2026, 6, 12, 0, 0, tzinfo=UTC)


def test_windowed_spout_holds_in_gap_and_before_source():
    future = Window(start_anchor=datetime(2030, 1, 1, tzinfo=UTC), duration=timedelta(hours=1),
                    freq_unit="DAY", freq_interval=1)  # opens only in 2030 → a gap now
    spt = Pond("spt", "spt", sources=["src"], is_spout=True, windows=[future])
    s = build([Pond("src", "src"), spt], [])
    s.pond_states["spt"].standing_wake = True
    s.pond_states["src"].end_f = datetime(2026, 6, 10, tzinfo=UTC)
    at = datetime(2026, 6, 10, 12, tzinfo=UTC)
    s = tick(at, s)
    s, _ = sentinel(at, s)
    assert s.pending_begin_runs == []  # in a window gap → delivery holds

    # An active window but the source has never published → still nothing to deliver.
    live = Window(start_anchor=T0, duration=timedelta(days=1), freq_unit="DAY", freq_interval=1)
    spt2 = Pond("spt2", "spt2", sources=["src2"], is_spout=True, windows=[live])
    s2 = build([Pond("src2", "src2"), spt2], [])
    s2.pond_states["spt2"].standing_wake = True  # src2.end_f stays NEVER
    s2 = tick(at, s2)
    s2, _ = sentinel(at, s2)
    assert s2.pending_begin_runs == []


def test_standing_wake_parked_when_failed_or_killed():
    spt = Pond("spt", "spt", sources=["src"], is_spout=True)
    s = build([Pond("src", "src"), spt], [])
    s.pond_states["spt"].standing_wake = True
    s.pond_states["src"].start_f = s.pond_states["src"].end_f = T0
    for flag in ("is_failed", "is_killed"):
        st = s.clone()
        setattr(st.pond_states["spt"], flag, True)
        st = tick(T0, st)
        st, _ = sentinel(T0, st)
        assert st.pending_begin_runs == []  # parked → no delivery


def chain_topology() -> tuple[EngineState, dict[str, timedelta]]:
    """p1 (inlet: r1, r2 -> r3) -> p2 (single ripple s1). All Ripples 1 s."""
    ponds = [Pond("p1", "p1"), Pond("p2", "p2", sources=["p1"])]
    ripples = [
        Ripple("r1", "p1", "r1"),
        Ripple("r2", "p1", "r2"),
        Ripple("r3", "p1", "r3", parents=["r1", "r2"]),
        Ripple("s1", "p2", "s1"),
    ]
    durations = {"r1": secs(1), "r2": secs(1), "r3": secs(1), "s1": secs(1)}
    return build(ponds, ripples), durations


def demo_topology(triggers: list[Trigger] | None = None) -> tuple[EngineState, dict[str, timedelta]]:
    """The reference demo: lead ~7 s, bottleneck (sales.join) = 3 s.

    transactions[1s] (inlet) ─┐
    products[2s]     (inlet) ─┴→ sales[daily 2s, tiers 1s (roots) → join 3s (leaf)] → reports[1s]
    """
    ponds = [
        Pond("transactions", "transactions"),
        Pond("products", "products"),
        Pond("sales", "sales", sources=["transactions", "products"]),
        Pond("reports", "reports", sources=["sales"]),
    ]
    ripples = [
        Ripple("tx", "transactions", "ingest"),
        Ripple("pr", "products", "ingest"),
        Ripple("daily", "sales", "daily"),
        Ripple("tiers", "sales", "tiers"),
        Ripple("join", "sales", "join", parents=["daily", "tiers"]),
        Ripple("monthly", "reports", "monthly"),
    ]
    durations = {
        "tx": secs(1),
        "pr": secs(2),
        "daily": secs(2),
        "tiers": secs(1),
        "join": secs(3),
        "monthly": secs(1),
    }
    return build(ponds, ripples, triggers), durations


def diamond_topology(triggers: list[Trigger] | None = None) -> tuple[EngineState, dict[str, timedelta]]:
    """Ponds S -> A, S -> B, and X consuming both A and B. One Ripple each, all 1 s."""
    ponds = [
        Pond("S", "S"),
        Pond("A", "A", sources=["S"]),
        Pond("B", "B", sources=["S"]),
        Pond("X", "X", sources=["A", "B"]),
    ]
    ripples = [Ripple(n.lower(), n, n) for n in ("S", "A", "B", "X")]
    durations = {"s": secs(1), "a": secs(1), "b": secs(1), "x": secs(1)}
    return build(ponds, ripples, triggers), durations


# ─── Driver (plays the orchestrator) ──────────────────────────────────────────


class Driver:
    def __init__(self, state: EngineState, durations: dict[str, timedelta], now: datetime = T0):
        self.state = state
        self.durations = durations
        self.now = now
        self.inflight: dict[str, datetime] = {}  # rid -> scheduled completion time
        self.fail_counts: dict[str, int] = {}  # rid -> remaining runs to fail instead of complete
        self.unchanged: set[str] = set()  # rids whose completions report no output change (changed=False)

    def fail_next(self, rid: str, n: int = 1) -> None:
        """Make the next ``n`` runs of ``rid`` error (the Duck gave up) rather than complete."""
        self.fail_counts[rid] = self.fail_counts.get(rid, 0) + n

    def mark_unchanged(self, rid: str) -> None:
        """Make ``rid``'s completions report no output change (a Trickle empty delta / pond.skip), so
        the Pond holds its ``changed_f`` — simulating a Duck reporting ``changed=False``."""
        self.unchanged.add(rid)

    def _react(self) -> None:
        self.state, started = sentinel(self.now, self.state)
        for rid in started:
            self.inflight[rid] = self.now + self.durations[rid]

    def tap(self, pid: str) -> None:
        self.state = tap_pond(self.state, pid, self.now)
        self._react()

    def pulse(self, pid: str) -> None:
        self.state = pulse_pond(self.state, pid, self.now)
        self._react()

    def stop(self, pid: str, upstream: bool = False) -> None:
        self.state = sleep_pond(self.state, pid, self.now, upstream=upstream)
        # Halt for the test: also drop any standing trigger so it can't re-tap.
        self.state.triggers = {k: v for k, v in self.state.triggers.items() if k != pid}
        self._react()

    def step(self) -> None:
        due = [rid for rid, t in self.inflight.items() if t <= self.now]
        for rid in due:
            del self.inflight[rid]
            if self.fail_counts.get(rid, 0) > 0:
                self.fail_counts[rid] -= 1
                self.state = fail_ripple(self.state, rid, self.now)
            else:
                self.state = complete_ripple(self.state, rid, self.now, changed=rid not in self.unchanged)
        self.state = tick(self.now, self.state)
        self._react()
        self.now += STEP

    def run(self, seconds: float, stop_when=None) -> None:
        steps = int(seconds / 0.1)
        for _ in range(steps):
            self.step()
            if stop_when is not None and stop_when(self.state):
                return

    def run_until_completions(self, pid: str, n: int, max_seconds: float) -> None:
        self.run(max_seconds, stop_when=lambda s: s.pond_states[pid].runs_completed >= n)


def gaps_seconds(times: list[datetime]) -> list[float]:
    return [(b - a).total_seconds() for a, b in zip(times, times[1:], strict=False)]


# ─── Unit tests (pure helpers) ────────────────────────────────────────────────


@pytest.mark.timeout(1)
def test_pond_source_f_live_inlet():
    s, _ = chain_topology()
    f, d = pond_source_f(s, "p1", T0)
    assert f == T0 and d == timedelta(0)


@pytest.mark.timeout(1)
def test_pond_source_f_windowed():
    pond = Pond("w", "w", windows=[Window(start_anchor=T0, duration=secs(20), freq_unit="MINUTE", freq_interval=1)])
    s = build([pond], [Ripple("wr", "w", "wr")])
    # Inside the window [00:00:00, 00:00:20): F = window end, D = duration.
    f, d = pond_source_f(s, "w", T0 + secs(5))
    assert f == T0 + secs(20) and d == secs(20)
    # In the gap [00:00:20, 00:01:00): cannot run.
    f, d = pond_source_f(s, "w", T0 + secs(30))
    assert f is None


@pytest.mark.timeout(1)
def test_pond_source_f_required_vs_optional():
    a, b = PondState(end_f=T0 + secs(5)), PondState(end_f=T0 + secs(2))
    base = Pond("c", "c", sources=["a", "b"])
    s = build([Pond("a", "a"), Pond("b", "b"), base], [])
    s.pond_states["a"], s.pond_states["b"] = a, b
    # Both required: stalest (min) wins.
    f, _ = pond_source_f(s, "c", T0)
    assert f == T0 + secs(2)
    # b optional: freshest required (a) wins... with only-optional it would be max.
    s.ponds["c"].optional_sources = {"b"}
    f, _ = pond_source_f(s, "c", T0)
    assert f == T0 + secs(5)


@pytest.mark.timeout(1)
def test_ripple_source_f_root_and_parents():
    s, _ = chain_topology()
    s.pond_states["p1"].start_f = T0 + secs(3)
    assert ripple_source_f(s, "r1") == T0 + secs(3)  # root → pond.start_f
    s.ripple_states["r1"].end_f = T0 + secs(4)
    s.ripple_states["r2"].end_f = T0 + secs(1)
    assert ripple_source_f(s, "r3") == T0 + secs(1)  # required min
    s.ripples["r3"].optional_parents = {"r2"}
    assert ripple_source_f(s, "r3") == T0 + secs(4)  # only r1 required


@pytest.mark.timeout(1)
def test_startf_propagation_guard():
    # A source already running ahead (start_f > child.start_f) is NOT re-armed by a pull cascade.
    s, _ = chain_topology()
    s.pond_states["p1"].start_f = T0 + secs(5)  # p1 running ahead
    s.pond_states["p1"].end_f = T0 + secs(4)
    s.pond_states["p2"].start_f = T0 + secs(1)  # p2 behind
    pond_set_has_pull(s, "p2", T0 + secs(10), T0 + secs(10))
    assert not s.pond_states["p1"].has_pull  # skipped — its in-flight run will satisfy the demand


@pytest.mark.timeout(1)
def test_inlet_stamps_push_epoch_not_run_now():
    # Minted freshness: an Inlet that runs LATER than the push that demanded it stamps the demand
    # epoch (the target), not its run-now. (Same-tick runs are unchanged because now == target.)
    s = build([Pond("i", "i")], [Ripple("ir", "i", "ir")])
    epoch = T0 + secs(5)
    s.pond_states["i"].targets = [epoch]
    s2, started = sentinel(T0 + secs(10), s)  # runs at T0+10, but the demand epoch is T0+5
    assert "ir" in started
    assert s2.pond_states["i"].start_f == epoch


@pytest.mark.timeout(1)
def test_inlet_stamps_pull_epoch_not_run_now():
    s = build([Pond("i", "i")], [Ripple("ir", "i", "ir")])
    epoch = T0 + secs(5)
    s.pond_states["i"].has_pull = True
    s.pond_states["i"].pull_m = epoch
    s.ripple_states["ir"].has_pull = True
    s2, _ = sentinel(T0 + secs(10), s)
    assert s2.pond_states["i"].start_f == epoch


@pytest.mark.timeout(1)
def test_force_inlet_ignores_never_sentinel_and_stamps_now():
    # Force adds a NEVER target; it must be filtered out so the Inlet stamps `now`, not datetime.min.
    s = build([Pond("i", "i")], [Ripple("ir", "i", "ir")])
    s = force_pond(s, "i", T0)
    s2, _ = sentinel(T0 + secs(3), s)
    assert s2.pond_states["i"].start_f == T0 + secs(3)


@pytest.mark.timeout(1)
def test_completion_prunes_a_target_added_mid_run():
    # A target added during a Run (valid then: t > end_f) must not linger past completion to fire a
    # second Run at the same freshness.
    pond = Pond("p", "p")
    s = build([pond], [Ripple("r", "p", "r")])
    T = T0 + secs(5)
    s.pond_states["p"].start_f = T  # mid-run at freshness T
    s.pond_states["p"].end_f = T0
    s.ripple_states["r"].start_f = T
    s.ripple_states["r"].is_running = True
    s.pond_states["p"].targets = [T]  # a duplicate target arrived during the Run

    s = complete_ripple(s, "r", T0 + secs(6))
    assert s.pond_states["p"].end_f == T
    assert s.pond_states["p"].targets == []  # satisfied target pruned
    _, started = sentinel(T0 + secs(7), s)
    assert started == []  # no spurious re-run


@pytest.mark.timeout(2)
def test_pulse_freshness_uniform_across_staggered_diamond():
    # S -> A, S -> B, X <- A,B with B slower: a Pulse reaches all; every node ends at the pulse epoch.
    s, durations = diamond_topology()
    durations["b"] = secs(3)
    d = Driver(s, durations)
    d.pulse("X")
    d.run(10)
    for pid in ("S", "A", "B", "X"):
        assert d.state.pond_states[pid].end_f == T0


@pytest.mark.timeout(1)
def test_next_wake_window_and_tide():
    pond = Pond("w", "w", windows=[Window(start_anchor=T0, duration=secs(20), freq_unit="MINUTE", freq_interval=1)])
    s = build([pond], [Ripple("wr", "w", "wr")])
    # Inside the window → next wake is its close at 00:00:20.
    assert next_wake(T0 + secs(5), s) == T0 + secs(20)
    # In the gap → next wake is the next open at 00:01:00.
    assert next_wake(T0 + secs(30), s) == T0 + secs(60)
    # Tide: deadline = reference (start_f) + bound.
    s2 = build([Pond("p", "p")], [Ripple("pr", "p", "pr")], [Trigger("p", "tide", secs(10))])
    s2.pond_states["p"].start_f = T0
    assert next_wake(T0 + secs(2), s2) == T0 + secs(10)


# ─── Simulation tests ─────────────────────────────────────────────────────────


@pytest.mark.timeout(5)
def test_tap_two_pond_chain():
    s, dur = chain_topology()
    d = Driver(s, dur)
    d.tap("p2")
    d.run(20)
    assert d.state.pond_states["p1"].runs_completed == 3
    assert d.state.pond_states["p2"].runs_completed == 1


@pytest.mark.timeout(5)
def test_pulse_two_pond_chain_coherent():
    s, dur = chain_topology()
    d = Driver(s, dur)
    d.pulse("p2")
    d.run(20)
    p1, p2 = d.state.pond_states["p1"], d.state.pond_states["p2"]
    assert p1.runs_completed == 1
    assert p2.runs_completed == 1
    assert p1.end_f == p2.end_f  # whole chain at one coherent freshness


@pytest.mark.timeout(5)
def test_wave_settles_to_bottleneck():
    s, dur = demo_topology([Trigger("reports", "wave")])
    d = Driver(s, dur)
    d.run_until_completions("reports", 6, max_seconds=60)
    # Steady-state completion cadence == the 3 s bottleneck (join), inlets included (no over-pull).
    for pid in ("reports", "sales", "transactions", "products"):
        g = gaps_seconds(d.state.pond_states[pid].completion_times)[-3:]
        assert g, f"{pid} did not complete enough times"
        for gap in g:
            assert abs(gap - 3.0) < 0.2, f"{pid} cadence {g} != 3.0s"


@pytest.mark.timeout(5)
@pytest.mark.parametrize("bound,expected", [(3, 3.0), (4, 4.0), (7, 7.0), (10, 10.0), (20, 20.0)])
def test_tide_cadence_matches_bound(bound, expected):
    s, dur = demo_topology([Trigger("reports", "tide", secs(bound))])
    d = Driver(s, dur)
    d.run_until_completions("reports", 4, max_seconds=10 + 3.5 * bound)
    g = gaps_seconds(d.state.pond_states["reports"].completion_times)[-2:]
    assert g, "reports did not complete enough times"
    for gap in g:
        assert abs(gap - expected) < 0.25, f"bound {bound}: cadence {g} != {expected}"


@pytest.mark.timeout(5)
def test_tide_below_bottleneck_throttles():
    # A 2 s bound is below the 3 s bottleneck → completions throttle to 3 s, pulses pipeline.
    s, dur = demo_topology([Trigger("reports", "tide", secs(2))])
    d = Driver(s, dur)
    max_targets = 0

    def watch(state):
        nonlocal max_targets
        max_targets = max(max_targets, *(len(ps.targets) for ps in state.pond_states.values()))
        return state.pond_states["reports"].runs_completed >= 5

    d.run(60, stop_when=watch)
    g = gaps_seconds(d.state.pond_states["reports"].completion_times)[-2:]
    for gap in g:
        assert abs(gap - 3.0) < 0.3, f"throttled cadence {g} != 3.0s"
    assert max_targets > 1  # several Pulses in flight at once (pipelining)


@pytest.mark.timeout(5)
def test_restore_demand_re_solicits_a_stranded_source():
    """The demand-restoration invariant (plans/reset.md): a downstream still holding a standing pull whose
    Source no longer mirrors it (e.g. the Source was reset to NEVER, or a cascade was lost) gets its Source
    re-solicited on the next tick. Without it the Source sits idle forever and the downstream never runs."""
    src = Pond("src", "src")
    dwn = Pond("dwn", "dwn", sources=["src"])
    s = build([src, dwn], [Ripple("r", "dwn", "r")])
    s.pond_states["dwn"].start_f = s.pond_states["dwn"].end_f = T0
    s.pond_states["dwn"].has_pull = True             # preserved demand (the intent to keep running)
    s.pond_states["src"].start_f = s.pond_states["src"].end_f = NEVER  # reset: cleared to a fresh slate
    assert s.pond_states["src"].has_pull is False

    s = tick(T0 + STEP, s)
    assert s.pond_states["src"].has_pull is True      # re-solicited (cold-start cascade)


@pytest.mark.timeout(5)
def test_restore_demand_parks_under_blocked_source_and_heals_on_unblock():
    """Re-solicitation refuses a blocked Source (as all demand does), so a path parks under a blocked
    Source and heals on the first tick after it unblocks — the fix for the standing gap where a blocked
    Pond drops incoming demand instead of parking it."""
    src = Pond("src", "src")
    dwn = Pond("dwn", "dwn", sources=["src"])
    s = build([src, dwn], [Ripple("r", "dwn", "r")])
    s.pond_states["dwn"].start_f = s.pond_states["dwn"].end_f = T0
    s.pond_states["dwn"].has_pull = True
    s.pond_states["src"].start_f = s.pond_states["src"].end_f = NEVER
    s.pond_states["src"].is_blocked = True

    s = tick(T0 + STEP, s)
    assert s.pond_states["src"].has_pull is False      # blocked → parks, not re-solicited

    s.pond_states["src"].is_blocked = False
    s = tick(T0 + 2 * STEP, s)
    assert s.pond_states["src"].has_pull is True        # healed on the next tick


@pytest.mark.timeout(5)
def test_missing_source_asset_blocks_with_reason_not_failed():
    """A read of an unpublished Source asset (plans/reset.md Mechanism 2) parks the Pond blocked-with-a-
    reason — not failed: no budget burn, no ``failed_f`` — abandons the incomplete Run, and propagates
    ``blocked`` downstream. ``clear_missing_asset`` (a later clean read) releases it and the downstream."""
    src = Pond("src", "src")
    mid = Pond("mid", "mid", sources=["src"])
    dwn = Pond("dwn", "dwn", sources=["mid"])
    s = build([src, mid, dwn], [Ripple("r", "mid", "r"), Ripple("r2", "dwn", "r2")])
    # `mid` started a Run (reached start_f=T0) then read src.x and missed.
    s.pond_states["mid"].start_f = T0
    s.pond_states["mid"].end_f = NEVER
    s.ripple_states["r"].start_f = T0
    s.ripple_states["r"].is_running = True

    s = block_on_missing_asset(s, "mid", "src.x", T0 + STEP)
    m = s.pond_states["mid"]
    assert m.missing_asset == "src.x"
    assert m.is_blocked is True and m.is_failed is False   # blocked, not failed
    assert m.failures == 0 and m.failed_f == NEVER          # no budget burn
    assert m.start_f == m.end_f                             # incomplete Run abandoned (liveness won't fail it)
    assert not s.ripple_states["r"].is_running
    assert s.pond_states["dwn"].is_blocked is True          # propagated downstream

    s = clear_missing_asset(s, "mid")                       # a later clean read
    assert s.pond_states["mid"].missing_asset is None
    assert s.pond_states["mid"].is_blocked is False
    assert s.pond_states["dwn"].is_blocked is False         # downstream released too


@pytest.mark.timeout(5)
def test_restore_demand_quiescent_graph_no_spurious_demand():
    """A fully-idle graph with no outstanding demand stays quiescent — the invariant only re-derives held
    demand, it never invents it."""
    src = Pond("src", "src")
    dwn = Pond("dwn", "dwn", sources=["src"])
    s = build([src, dwn], [Ripple("r", "dwn", "r")])
    for pid in ("src", "dwn"):
        s.pond_states[pid].start_f = s.pond_states[pid].end_f = T0
    s = tick(T0 + STEP, s)
    s, _ = sentinel(T0 + STEP, s)
    assert not any(ps.has_pull for ps in s.pond_states.values())
    assert s.pending_begin_runs == []


@pytest.mark.timeout(5)
def test_push_precision_diamond():
    # Standing Wave on B churns it; one Pulse on X must run X exactly once despite the forked path.
    s, dur = diamond_topology([Trigger("B", "wave")])
    d = Driver(s, dur)
    d.run(2)  # let the Wave on B get going
    d.pulse("X")
    d.run(20)
    assert d.state.pond_states["X"].runs_completed == 1
    assert d.state.pond_states["B"].runs_completed > 3  # B churned many times


@pytest.mark.timeout(5)
def test_windowed_inlet_throttles_chain():
    inlet = Pond("w", "w", windows=[Window(start_anchor=T0, duration=secs(20), freq_unit="MINUTE", freq_interval=1)])
    child = Pond("c", "c", sources=["w"])
    state = build([inlet, child], [Ripple("wr", "w", "wr"), Ripple("cr", "c", "cr")], [Trigger("c", "wave")])
    d = Driver(state, {"wr": secs(1), "cr": secs(1)})
    d.run(150)
    # One run per minute window → child completions ~60 s apart.
    g = gaps_seconds(d.state.pond_states["c"].completion_times)
    assert g, "child never completed"
    assert all(abs(gap - 60.0) < 0.5 for gap in g), f"chain not throttled to window period: {g}"
    # The windowed inlet runs at most once per window.
    assert d.state.pond_states["w"].runs_completed <= 3


@pytest.mark.timeout(1)
def test_stop_local_clears_demand_keeps_ripple_push():
    # stop_pond on the target Pond only: clears its push+pull and its Ripples' pull, KEEPS Ripple
    # push targets, and does not touch upstream.
    s, _ = chain_topology()
    far = T0 + secs(99)
    s.pond_states["p2"].has_pull = True
    s.pond_states["p2"].targets = [far]
    s.ripple_states["s1"].has_pull = True
    s.ripple_states["s1"].targets = [far]
    s.pond_states["p1"].has_pull = True  # upstream demand
    out = sleep_pond(s, "p2", T0)
    assert not out.pond_states["p2"].has_pull and not out.pond_states["p2"].targets
    assert not out.ripple_states["s1"].has_pull          # ripple pull cleared
    assert out.ripple_states["s1"].targets == [far]      # ripple push kept (started run completes)
    assert out.pond_states["p1"].has_pull                # upstream untouched (no --upstream)


@pytest.mark.timeout(1)
def test_stop_upstream_propagates():
    s, _ = chain_topology()
    s.pond_states["p1"].has_pull = True
    s.pond_states["p2"].has_pull = True
    out = sleep_pond(s, "p2", T0, upstream=True)
    assert not out.pond_states["p2"].has_pull
    assert not out.pond_states["p1"].has_pull            # propagated to the source


@pytest.mark.timeout(5)
def test_stop_drains_then_halts():
    s, dur = chain_topology()
    d = Driver(s, dur)
    d.state.triggers = {"p2": Trigger("p2", "wave")}
    d.run(10)  # run a few cycles
    assert d.state.pond_states["p2"].runs_completed > 1
    d.stop("p2", upstream=True)
    # In-flight runs drain (ripple push kept), then nothing new starts.
    d.run(5)
    settled = d.state.pond_states["p2"].runs_completed
    d.run(10)
    assert d.state.pond_states["p2"].runs_completed == settled


# ─── Fault tolerance ──────────────────────────────────────────────────────────


@pytest.mark.timeout(5)
def test_failure_without_retry_fails_and_blocks():
    # No retry budget: a Ripple giving up fails its Pond, which won't run again on its own.
    s, dur = chain_topology()
    d = Driver(s, dur)
    d.fail_next("s1", 1)
    d.tap("p2")
    d.run(20)
    p2 = d.state.pond_states["p2"]
    assert p2.is_failed and p2.is_blocked
    assert p2.failures == 1 and p2.runs_completed == 0
    assert not d.state.pond_states["p1"].is_failed  # no stop signal travels upstream


@pytest.mark.timeout(5)
def test_retry_on_change_recovers():
    # With budget 1 and a Source that keeps moving, a single failure is retried and recovers.
    s, dur = chain_topology()
    s.ponds["p2"].retry_on_change = 1
    s.triggers["p1"] = Trigger("p1", "wave")  # p1 advances on its own, independent of the failure
    d = Driver(s, dur)
    d.fail_next("s1", 1)  # only the first p2 run fails
    d.tap("p2")
    d.run(30, stop_when=lambda st: st.pond_states["p2"].runs_completed >= 1)
    p2 = d.state.pond_states["p2"]
    assert p2.runs_completed >= 1
    assert not p2.is_failed and not p2.is_blocked and p2.failed_f == NEVER


@pytest.mark.timeout(5)
def test_retry_on_change_exhausts_to_terminal():
    # Budget 1, but every attempt fails: original + one retry = 2 failures, then it stays failed.
    s, dur = chain_topology()
    s.ponds["p2"].retry_on_change = 1
    s.triggers["p1"] = Trigger("p1", "wave")
    d = Driver(s, dur)
    d.fail_next("s1", 9)
    d.tap("p2")
    d.run(40)
    p2 = d.state.pond_states["p2"]
    assert p2.is_failed and p2.runs_completed == 0
    assert p2.failures == 2  # no further retries once the budget is spent


@pytest.mark.timeout(1)
def test_blocked_pond_drains_available_output_without_soliciting():
    # p1 produced F1, then failed on a later generation. A blocked p2 may still consume F1 — it just
    # won't re-arm p1 for anything fresher.
    s, _ = chain_topology()
    f1 = T0 + secs(5)
    s.pond_states["p1"].start_f = T0 + secs(6)
    s.pond_states["p1"].end_f = f1
    s.pond_states["p1"].is_failed = True
    s.pond_states["p1"].failed_f = T0 + secs(6)
    s.pond_states["p1"].failures = 1
    derive_blocked(s, "p1")
    assert s.pond_states["p1"].is_blocked and s.pond_states["p2"].is_blocked

    s.pond_states["p2"].has_pull = True
    s.ripple_states["s1"].has_pull = True
    out, started = sentinel(T0 + secs(10), s)
    assert out.pond_states["p2"].start_f == f1  # drained the available generation
    assert "s1" in started
    assert not out.pond_states["p1"].has_received_pull  # never solicited the failed Source


@pytest.mark.timeout(5)
def test_block_propagates_downstream_and_clears():
    # p1's leaf keeps failing → p1 failed, and p2 blocks by deriving from its failed Source. Clearing
    # p1 unblocks p2 on its own.
    s, dur = chain_topology()
    d = Driver(s, dur)
    d.fail_next("r3", 9)
    d.tap("p2")
    d.run(15)
    p1, p2 = d.state.pond_states["p1"], d.state.pond_states["p2"]
    assert p1.is_failed and p1.is_blocked
    assert p2.is_blocked and not p2.is_failed  # blocked, not failed — it produced no failure itself
    assert p2.runs_completed == 0

    d.state = clear_pond(d.state, "p1", d.now)
    d._react()
    assert not d.state.pond_states["p1"].is_blocked
    assert not d.state.pond_states["p2"].is_blocked


@pytest.mark.timeout(1)
def test_blocked_pond_ignores_new_demand():
    s, _ = chain_topology()
    for f in ("is_failed", "is_blocked"):
        setattr(s.pond_states["p2"], f, True)
    s.pond_states["p2"].failed_f = T0 + secs(2)
    s.pond_states["p2"].failures = 1
    assert not tap_pond(s, "p2", T0).pond_states["p2"].has_received_pull  # pull ignored
    assert pulse_pond(s, "p2", T0).pond_states["p2"].targets == []        # push ignored


@pytest.mark.timeout(1)
def test_fail_pond_attributes_to_start_f_and_blocks():
    # A whole-Pond failure (dead Duck / Duck-level error) pins failedF to the most recently started
    # Run (startF), stops its Ripples, and blocks downstream.
    s, _ = chain_topology()
    s.pond_states["p1"].start_f = T0 + secs(5)
    s.pond_states["p1"].end_f = T0 + secs(2)
    s.ripple_states["r1"].is_running = True
    out = fail_pond(s, "p1", T0 + secs(10))
    p1 = out.pond_states["p1"]
    assert p1.is_failed and p1.failed_f == T0 + secs(5) and p1.failures == 1
    assert not out.ripple_states["r1"].is_running
    assert out.pond_states["p2"].is_blocked  # downstream derives the block

    # No-op when nothing is in flight (the latest Run already completed).
    s2, _ = chain_topology()
    s2.pond_states["p1"].start_f = s2.pond_states["p1"].end_f = T0 + secs(3)
    assert not fail_pond(s2, "p1", T0 + secs(9)).pond_states["p1"].is_failed


@pytest.mark.timeout(5)
def test_wake_clears_failure_and_runs():
    # Wake is a non-propagating pull: it clears the failure and runs once because fresh input is
    # available (p1 produced T0+1s, p2 never ran), without soliciting p1.
    s, dur = chain_topology()
    s.pond_states["p1"].start_f = s.pond_states["p1"].end_f = T0 + secs(1)  # p1 has output to consume
    ps = s.pond_states["p2"]
    ps.is_failed = ps.is_blocked = True
    ps.failed_f = T0 + secs(2)
    ps.failures = 1
    d = Driver(s, dur)
    d.state = wake_pond(d.state, "p2", d.now)
    assert not d.state.pond_states["p1"].has_received_pull  # non-propagating: p1 not solicited
    d._react()
    d.run(10)
    p2 = d.state.pond_states["p2"]
    assert not p2.is_failed and not p2.is_blocked
    assert p2.runs_completed >= 1


@pytest.mark.timeout(5)
def test_wake_no_op_when_current():
    # Nothing fresher than the last Run → Wake parks a one-shot pull and runs nothing (no urgency).
    s, dur = chain_topology()
    f = T0 + secs(1)
    s.pond_states["p1"].start_f = s.pond_states["p1"].end_f = f
    s.pond_states["p2"].start_f = s.pond_states["p2"].end_f = f  # p2 already current with p1
    d = Driver(s, dur)
    d.state = wake_pond(d.state, "p2", d.now)
    d.run(5)
    assert d.state.pond_states["p2"].runs_completed == 0  # sourceF not > startF → no run


@pytest.mark.timeout(5)
def test_force_recomputes_without_advancing_freshness():
    # A fully-current Pond: Force re-runs all Ripples at the same freshness and does NOT propagate
    # downstream (endF returns unchanged).
    s, dur = chain_topology()
    f = T0 + secs(1)
    s.pond_states["p1"].start_f = s.pond_states["p1"].end_f = f
    s.pond_states["p2"].start_f = s.pond_states["p2"].end_f = f
    s.ripple_states["s1"].start_f = s.ripple_states["s1"].end_f = f
    d = Driver(s, dur)
    before = d.state.pond_states["p2"].runs_completed
    d.state = force_pond(d.state, "p2", d.now)
    cmds = [c for c in d.state.pending_begin_runs]
    d._react()
    d.run(5)
    p2 = d.state.pond_states["p2"]
    assert p2.runs_completed == before + 1  # it re-ran
    assert p2.end_f == f  # freshness unchanged → downstream sees no change
    # the dispatched Run carried the force flag
    assert any(c.pond_id == "p2" and c.force for c in cmds) or any(
        c.pond_id == "p2" and c.force for c in d.state.pending_begin_runs
    )


@pytest.mark.timeout(1)
def test_kill_parks_terminal_and_blocks_downstream():
    s, _ = chain_topology()
    # p1 has a Run in flight; Kill cancels it.
    s.pond_states["p1"].start_f = T0 + secs(5)
    s.pond_states["p1"].end_f = T0 + secs(2)
    s.ripple_states["r1"].is_running = True
    out = kill_pond(s, "p1", T0 + secs(6))
    assert out.pond_states["p1"].is_killed
    assert not out.ripple_states["r1"].is_running
    assert out.pond_states["p2"].is_blocked  # downstream blocked by the killed Source
    # Killed supersedes demand: a fresh Tap does nothing until cleared.
    out2, _ = sentinel(T0 + secs(7), tap_pond(out, "p1", T0 + secs(7)))
    assert out2.pond_states["p1"].runs_started == s.pond_states["p1"].runs_started  # no new run
    # Clear lifts it and unblocks downstream.
    cleared = clear_pond(out, "p1", T0 + secs(8))
    assert not cleared.pond_states["p1"].is_killed and not cleared.pond_states["p2"].is_blocked


# ─── No-change skip: content freshness (changed_f) and the Pond pass ───────────
# plans/no-change-skip.md. A Pond with Sources whose content is unchanged since it last ran is
# completed in-engine as a *pass* (no BeginRun): freshness/heartbeat advance, changed_f is held.


def _settle_chain_at(s: EngineState, f: datetime) -> None:
    """Put the p1 -> p2 chain in a clean idle state, both Ponds run and changed at freshness ``f``."""
    for pid in ("p1", "p2"):
        ps = s.pond_states[pid]
        ps.start_f = ps.end_f = ps.changed_f = f
    for rid in ("r1", "r2", "r3", "s1"):
        s.ripple_states[rid].start_f = s.ripple_states[rid].end_f = f


@pytest.mark.timeout(1)
def test_unchanged_source_makes_sink_pass_without_dispatch():
    s, _ = chain_topology()
    f1 = T0 + secs(5)
    _settle_chain_at(s, f1)
    # p1 republishes at f2 with NO content change (changed_f stays f1); its ripples advance.
    f2 = T0 + secs(10)
    s.pond_states["p1"].start_f = s.pond_states["p1"].end_f = f2
    for rid in ("r1", "r2", "r3"):
        s.ripple_states[rid].start_f = s.ripple_states[rid].end_f = f2
    # Demand p2 (a pull wanting fresher input).
    s.pond_states["p2"].has_pull = True
    s.ripple_states["s1"].has_pull = True

    out, started = sentinel(f2, s)
    p2 = out.pond_states["p2"]
    assert "s1" not in started  # no Ripple ran — the Pond passed
    assert not out.pending_begin_runs  # and no Duck was dispatched
    assert p2.end_f == f2  # freshness (the heartbeat) advanced
    assert p2.changed_f == f1  # but the content mark is held
    assert p2.runs_completed == s.pond_states["p2"].runs_completed + 1  # the pass is a completed Run


@pytest.mark.timeout(1)
def test_changed_source_makes_sink_do_real_work():
    s, _ = chain_topology()
    f1 = T0 + secs(5)
    _settle_chain_at(s, f1)
    f2 = T0 + secs(10)
    s.pond_states["p1"].start_f = s.pond_states["p1"].end_f = s.pond_states["p1"].changed_f = f2
    for rid in ("r1", "r2", "r3"):
        s.ripple_states[rid].start_f = s.ripple_states[rid].end_f = f2
    s.pond_states["p2"].has_pull = True
    s.ripple_states["s1"].has_pull = True

    out, started = sentinel(f2, s)
    assert "s1" in started  # p1 changed → p2 does real work
    assert any(br.pond_id == "p2" for br in out.pending_begin_runs)  # dispatched to a Duck


@pytest.mark.timeout(1)
def test_skip_compares_prior_freshness_not_new_startf():
    """The operand correctness case (plans/no-change-skip.md). X reads A and B. A: endF 8 / changedF 6;
    B: endF 10 / changedF 7; X last ran at priorF 5. B changed at 7 — newer than X's prior freshness —
    so X must do real work, even though 7 < X's new startF (min(8,10)=8). The wrong ``>= startF`` rule
    would pass here and miss B's change."""
    s, _ = diamond_topology()
    s.pond_states["A"].start_f = s.pond_states["A"].end_f = T0 + secs(8)
    s.pond_states["A"].changed_f = T0 + secs(6)
    s.pond_states["B"].start_f = s.pond_states["B"].end_f = T0 + secs(10)
    s.pond_states["B"].changed_f = T0 + secs(7)
    s.pond_states["X"].start_f = s.pond_states["X"].end_f = s.pond_states["X"].changed_f = T0 + secs(5)
    s.pond_states["X"].has_pull = True
    s.ripple_states["x"].has_pull = True

    out, started = sentinel(T0 + secs(12), s)
    assert out.pond_states["X"].start_f == T0 + secs(8)  # ran at min(8, 10)
    assert "x" in started  # real work — B's change at 7 (> priorF 5) is incorporated
    assert any(br.pond_id == "X" for br in out.pending_begin_runs)


@pytest.mark.timeout(1)
def test_always_run_pond_dispatches_despite_unchanged_sources():
    s, _ = chain_topology()
    s.ponds["p2"].always_run = True  # a side-effecting Pond must run every time
    f1 = T0 + secs(5)
    _settle_chain_at(s, f1)
    f2 = T0 + secs(10)
    s.pond_states["p1"].start_f = s.pond_states["p1"].end_f = f2  # republish, changed_f held at f1
    for rid in ("r1", "r2", "r3"):
        s.ripple_states[rid].start_f = s.ripple_states[rid].end_f = f2
    s.pond_states["p2"].has_pull = True
    s.ripple_states["s1"].has_pull = True

    out, started = sentinel(f2, s)
    assert "s1" in started  # always_run bypasses the pass
    assert any(br.pond_id == "p2" for br in out.pending_begin_runs)


@pytest.mark.timeout(5)
def test_wave_steady_state_only_inlet_runs_when_nothing_changes():
    """Under a Wave, an Inlet that stops changing leaves the whole interior quiet: the Inlet keeps
    polling (real runs), every Sink passes (freshness climbs, no Ripple work)."""
    s, dur = chain_topology()
    s.triggers["p2"] = Trigger("p2", "wave")
    d = Driver(s, dur)
    d.run(10)  # reach steady state
    d.mark_unchanged("r3")  # p1's leaf — p1's output stops changing from here
    d.run(2)  # let one more p1 run land as unchanged
    s1_runs = d.state.ripple_states["s1"].runs_started
    p2_completed = d.state.pond_states["p2"].runs_completed
    p2_changed_f = d.state.pond_states["p2"].changed_f
    d.run(10)
    p1, p2 = d.state.pond_states["p1"], d.state.pond_states["p2"]
    assert p1.end_f > p2_changed_f  # the Inlet kept polling (freshness advanced)
    assert p2.runs_completed > p2_completed  # p2 kept completing — as passes
    assert d.state.ripple_states["s1"].runs_started == s1_runs  # but its Ripple never ran again
    assert p2.changed_f == p2_changed_f  # content mark frozen
