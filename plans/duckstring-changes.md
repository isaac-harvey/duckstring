# Duckstring changes for DuckFlock — the cross-repo plan

**Written 2026-07-10, in the DuckFlock repo, designed to be run in the duckstring repo.** Copy
this file (or point a session at this path) and execute there. DuckFlock (`private/duckflock`)
is the consumer; its docs are normative where referenced: `plans/plan-ir.md` (the IR + §Capture),
`plans/state-format.md` (published layout + Extension 1), `CLAUDE.md` (the engine-divergence
record). Everything here is additive to duckstring — no behaviour change for existing users
unless they opt in.

**Cross-repo gate (every stage):** duckstring's own test suite green, **and** DuckFlock's
conformance re-run from the duckflock repo:

```sh
cd ../../private/duckflock/tests/conformance
PYTHONPATH=../../../../public/duckstring/src DUCKSTRING_DATA_PLANE=parquet \
  ../../.venv/bin/python generate.py && \
PYTHONPATH=../../../../public/duckstring/src DUCKSTRING_DATA_PLANE=parquet \
  ../../.venv/bin/python run_conformance.py && \
PYTHONPATH=../../../../public/duckstring/src DUCKSTRING_DATA_PLANE=parquet \
  ../../.venv/bin/python run_conformance.py --distributed
```

(Regenerating fixtures after a duckstring change is the two-sided drift check. If a change is
*supposed* to alter fixtures — stage D1 below — the conformance diff is the review artifact.)

---

## D1 — the spine-PK same-`f` replay bug (fix first; it's a data-loss bug)

**Bug** (found building the Rust port; recorded in duckflock `CLAUDE.md`): the spine-PK append
fast path loses the current run's rows on a same-`f` replay. `_new_spine_rows` filters candidate
rows against output history **including** rows stamped at the current `f` (the first attempt's
own output), then `append_zset`'s replay DELETE removes those rows — and nothing re-inserts them.
First runs are unaffected; any at-least-once execution environment (retries, DuckFlock's replay
idempotence contract) silently drops the epoch's rows.

**Fix** (mirrors the Rust port's proven divergence): exclude rows stamped at the current run's
`f` from the history prefilter in `_new_spine_rows` (trickle_builder.py / trickle_io.py — find
the fast-path prefilter). Identical behaviour on first runs; replay-safe on re-runs.

**Test:** run a spine-PK append ripple twice at the same `f` over the same input; assert the
output equals the single-run output (today the second run deletes the rows). Then re-run the
DuckFlock conformance gate — the Rust engine already implements the fixed semantics, so the
`spine` scenario fixtures should now be *generated* replay-safe too (regenerate + verify 55/55).

## D2 — publish agg/acc state companions (state-format Extension 1)

**Gap:** incremental aggregate/accumulate state (the registry `agg`/`acc` companion tables) is
engine-internal; a different host (DuckFlock) can only resume incremental compute if the
companions are published. DuckFlock's driver already publishes and hydrates the layout
`state/{agg|acc}/{table}/{f}.parquet`; duckflock's conformance generator fakes the
duckstring side via `tests/conformance/lib.py::export_companions` (~30 lines — the reference
implementation to port).

**Change:** in duckstring's publish/export step (trickle_io.py — where changelog/band parts are
written), export each output's agg/acc companion snapshot to
`state/{agg|acc}/{table}/{f}.parquet` under the pond's data dir. Snapshot semantics (full
rewrite per `f`, not a delta); same-`f` replay overwrites. Gate behind a pond/config flag if
there's any concern about extra writes for non-DuckFlock users (default ON is fine — the
files are small and it makes every pond DuckFlock-resumable).

**Test:** duckstring-side round-trip (publish → wipe registry → hydrate from companions →
identical incremental next-epoch result). Then delete `export_companions` from duckflock's
conformance `lib.py` + the generator's call site, regenerate, 55/55 — the deletion *is* the
acceptance test.

## D3 — plan capture (plan-ir.md §Capture — the big one)

**Goal:** a ripple author writes normal duckstring code; capture emits the DuckFlock plan IR
(`duckflock_plan: 1` JSON) instead of executing. This retires the hand-declared IR in
duckflock's `tests/conformance/scenarios.py` and is the prerequisite for D4 (routing).

Per the spec (normative: duckflock `plans/plan-ir.md` §Capture):

- A **plan-capturing Pond handle**: runs the user's ripple function with a handle whose
  `trickle()` returns a recording builder — the existing `TrickleBuilder` minus execution.
  `merge()`/`append()`/`accumulate()`/`aggregate()` append a statement to the plan and return
  a chained handle whose source ref is the output name (matching `_chain`). `read_table`/
  `read_delta` register catalog entries (location, mode, pk, sidecar stats if present).
- **Positional aliases resolved at capture** (`_prepare_leaves` logic) so the IR carries final
  aliases. Note the `.sql()` subtlety duckflock's conformance pinned: after `.join()`, `.sql()`
  exposes the composed relation under the **left leaf's alias** — capture must record that.
- **Non-capturable ⇒ fail loud, run classic**: raw `con` access, callable metrics
  (`agg.reduce`/`acc.scan`), `pond.skip()` logic beyond `was_changed` — mark the ripple
  non-capturable; capture is opt-in per ripple and never silently wrong.
- **Serialization details that must match byte-for-byte** (duckflock deserializes with
  `preserve_order` — mutate-column and metric order is semantic): emit dicts in declaration
  order; timestamps as `isoformat()` (`NEVER` = `0001-01-01T00:00:00+00:00`); `p`, `ivm`,
  `key_filter`, retention options carried exactly as the builder holds them.
- Version with the envelope; additive fields only within `duckflock_plan: 1`.

**Acceptance:** for every scenario in duckflock's `tests/conformance/scenarios.py`, capture of
the Python builder calls must produce IR **equal** (as JSON, order-sensitive) to the
hand-declared IR sitting next to them — write this as a duckstring test importing the scenario
definitions, or as a duckflock-side harness step. Then swap `generate.py` to captured IR and
delete the hand declarations (the deletion is the acceptance).

## D4 — execution routing: duckstring as the DuckFlock client SDK

**Goal** (from duckflock `plans/completion-arc.md` Workstream M): the always-on Ripple host
decides *before* submitting — run trivially local (duckstring's own engine), or submit the
captured plan to DuckFlock. Quote authority stays with the DuckFlock driver; the client quote
is routing advice.

- A `duckflock` execution backend for ponds: capture (D3) → shell out to `duckflock quote
  --plan plan.json` (M3 duckflock-side; JSON out: `{route: local|duckflock, estimate, why}`)
  → local: execute the ripple classically; duckflock: `POST /jobs`, poll `GET /jobs/{id}`,
  surface the result doc. Config: DuckFlock API endpoint + tenant + express root; absent
  config ⇒ classic path (zero behaviour change).
- **Telemetry completeness:** when routed local, still write a lightweight RunRecord (or call
  the quote CLI's `--record` flag, M3) so estimate-error calibration and mode stickiness see
  local runs.
- Failure posture: any DuckFlock-side failure falls back to classic local execution with a loud
  warning (the pond must never be down because the offload engine is).

**Test:** a pond configured with a fake/emulated DuckFlock endpoint routes a big ripple to it
and a trivial one locally; both produce plane state identical to classic execution (the
conformance property, exercised from the duckstring side for the first time).

## Sequencing

D1 (bug fix, independent) → D2 (small, deletes duckflock duplication) → D3 (the big surface)
→ D4 (depends on D3 + duckflock M3). After D3 lands, tell the duckflock side: conformance
scenarios switch to captured IR (a duckflock commit), and `plans/plan-ir.md` §Capture gets
marked done.
