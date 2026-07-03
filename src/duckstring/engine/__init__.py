"""Duckstring orchestration engine (freshness/push-token model; see ``docs/guide/theory.md``).

Two deployable engines share a core:

- :mod:`.catchment` — the full state machine (Ponds + Ripples, pull + push) the Catchment runs. Its
  public functions are re-exported here, so ``duckstring.engine`` is the composed engine used by the
  validated simulation in ``tests/test_engine.py`` and by the Catchment runtime.
- :mod:`.worker` — the push-only engine each Duck runs to execute a Pond Run to completion.
- :mod:`.pond` — the per-Pond run ledger (``ponds/{base_pond}/pond.db``).

Pure: no FastAPI/DB/HTTP imports here.
"""

from __future__ import annotations

from . import pond, worker
from .catchment import (
    EngineState,
    block_on_missing_asset,
    clear_missing_asset,
    clear_pond,
    complete_ripple,
    derive_blocked,
    drain_begin_runs,
    drain_passes,
    fail_pond,
    fail_ripple,
    force_pond,
    kill_pond,
    next_wake,
    pond_add_target,
    pond_receive_pull,
    pond_set_has_pull,
    pond_source_f,
    pulse_pond,
    refresh_pond,
    repair_pond,
    required_sinks,
    ripple_add_target,
    ripple_set_has_pull,
    ripple_source_f,
    sentinel,
    sleep_pond,
    tap_pond,
    tick,
    wake_pond,
)
from .core import (
    NEVER,
    ZERO,
    BeginRun,
    Pond,
    PondState,
    Ripple,
    RippleState,
    Trigger,
    Window,
    max_target,
    min_target,
)

__all__ = [
    "NEVER",
    "ZERO",
    "BeginRun",
    "Pond",
    "PondState",
    "Ripple",
    "RippleState",
    "Trigger",
    "Window",
    "EngineState",
    "min_target",
    "max_target",
    "pond_source_f",
    "ripple_source_f",
    "pond_receive_pull",
    "pond_set_has_pull",
    "ripple_set_has_pull",
    "pond_add_target",
    "ripple_add_target",
    "complete_ripple",
    "fail_ripple",
    "fail_pond",
    "required_sinks",
    "derive_blocked",
    "block_on_missing_asset",
    "clear_missing_asset",
    "tap_pond",
    "pulse_pond",
    "sleep_pond",
    "wake_pond",
    "force_pond",
    "refresh_pond",
    "repair_pond",
    "kill_pond",
    "clear_pond",
    "sentinel",
    "tick",
    "next_wake",
    "drain_begin_runs",
    "drain_passes",
    "worker",
    "pond",
]
