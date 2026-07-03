# Reset — a clean slate for a Pond or the whole Catchment

Status: **implemented.** Mechanism 1 (`engine.restore_demand`) + Mechanism 2 (`MissingSourceAsset` →
blocked-with-a-reason) + `Driver.reset_pond` / `reset_catchment` + the routes / CLI (`control reset`,
`catchment reset`) / UI (Sidebar Reset, catchment "Reset all") are all wired and tested. Deferred (noted
below): Mechanism 2's auto-*solicit* of an idle Source + its stale-`f` escalation. The sanctioned
replacement for "something feels weird with the `.duckstring` files, so I `rm -rf` it and start over." Two
scopes:

- **`reset {pond}`** — scrub one Pond's runtime state, keeping its deployed code and operational config.
- **`catchment reset`** — do that for every Pond at once: the whole runtime back to a fresh-deploy state,
  keeping the deployed bundles, operational config, secrets, and keys. The honest fix after a package
  upgrade left on-disk state in a stale format.

**Both are lazy.** Reset (like delete) never forces a run — it clears state; the pipeline rebuilds when it
is next genuinely demanded. What makes that safe is not reset itself but two engine properties it depends
on, which don't fully exist yet and are the first things to build (below).

## The core rule — reset touches data and freshness, never demand

A Pond's runtime is three separable things: **data** (registry + published `data/`), the **freshness
clock** (`start_f`/`end_f`/`changed_f`), and **demand** (`targets`, `has_pull`/`pull_m`, standing
triggers). Reset scrubs data, rewinds freshness, and **preserves demand** — demand is the user's intent,
not state to scrub. So a reset Pond with pending or standing demand simply re-runs when eligible and
rebuilds from scratch; no *new* demand is injected (lazy is preserved), the existing intent re-drives it.

This is why **a Pond needn't be stopped before a reset/delete** (Q7): demand isn't a hazard, it's the
rebuild driver. (A standing Wave re-tapping after a reset is the design working, not a race.)

## The two prerequisites (build these first — they stand alone and also harden delete)

The demand model propagates **one-shot, at inject time, and never re-derives**: a pull cascades upstream
only when the token is first set (`pond_set_has_pull`'s `if has_pull: return` guard means a Pond already
holding a token never re-solicits), and a push target propagates only inside `pond_add_target`'s recursion
at Pulse time. `tick` re-arms only Waves and Tides. So the sequence *"D's demand reached S → S ran and
cleared its share → S's state is scrubbed before D consumed it"* strands D: it holds a token/target nothing
will satisfy, and nothing re-solicits S. **This stranding already exists without reset** — a *blocked* Pond
*refuses* incoming demand (`pond_receive_pull`/`pond_add_target` both early-return on `is_blocked`) rather
than parking it, so a Pulse accepted downstream while a Source is blocked is silently dropped at the
Source and never re-propagates when it unblocks. Reset just makes it easy to hit. Fix it generically:

### Mechanism 1 — a demand-restoration invariant (Q1, Q3, Q4)

Demand here is **derivable from the sinks**: an unsatisfied target `t` on D implies every required Source
holds `end_f ≥ t` *or* a target `≥ t`; a stuck pull on D implies its behind Sources (the cold-start test
`S.start_f ≤ D.start_f`) hold pulls. Add a periodic sweep (`engine.restore_demand`, run from the Driver's
scheduler tick beside `_check_liveness`) that re-establishes exactly that: for each unsatisfied sink
target, re-`pond_add_target(S, t)`; for each stuck non-local pull-holder, re-`pond_receive_pull(S,
pull_m)`. Both callees are **already idempotent** (set semantics / the token guard) and **already refuse
blocked Ponds** — so the invariant *automatically parks while a Source is blocked and heals on the first
tick after it unblocks*, closing the pre-existing gap for free.

This is the answer to **Q4**: don't overload Blocked to *restore* demand — Blocked is the downstream-facing
**parking** half; the pull/push cascade is the upstream-facing **restoration** half; the invariant composes
them. Propagation stays event-driven for latency but becomes eventually-consistent by construction — demand
lost to a reset, a crash between propagate-and-persist, or a future bug all heal within a tick. Exclusions:
`pull_local`/Spout wakes (non-propagating by design), killed Ponds (parked terminal), repair scopes.

### Mechanism 2 — a typed "missing Source asset" condition (Q2, Q5)

The engine can't know which tables a consumer reads (that's code, discovered at run time), so an absent
asset is necessarily detected **at the read**. Today it surfaces as a generic Ripple failure — burning
retry budget, marking D *failed*, firing failure alerts — all wrong, because nothing about D is broken.
Instead:

- `read_table` / `read_delta` raise a typed **`MissingSourceAsset(source, table)`** (the "has the Source
  completed a successful run?" `FileNotFoundError` becomes this).
- The Duck reports it as a **distinct event kind** (not a ripple error).
- The Catchment then **(a)** parks D **blocked-with-a-reason** — a `blocked_reason` = "waiting for
  `{source}.{table}`" — burning **no** budget and firing **no** failure alert; **(b)** converts D's demand
  into a **solicitation**: re-arms a real pull on S at D's epoch (so S republishes); **(c)** auto-recovers
  via the existing on-change path when S publishes fresher and D reads clean.
- **Loop guard / escalation** *(deferred)*: if S has since published at a *fresher* `f` and the asset is
  *still* missing, escalate to a genuine `failed` — S's code no longer produces the table, a topology break
  the operator must fix, not something to retry forever.

**Built (core):** `MissingSourceAsset` (`core.py`) from both foreign reads; the Duck's `ripple_missing_source`
→ a distinct `missing_source` event (no immediate-retry spend); `engine.block_on_missing_asset` /
`clear_missing_asset` + `PondState.missing_asset` folded into `derive_blocked`; the Catchment parks
blocked-with-a-reason (no `fail_ripple`, no budget, no alert) and clears on the next clean `run_completed`;
`blocked_reason` on `/api/status` + the Sidebar. Recovery is automatic when S republishes **fresher** (the
common live-pipeline case — the parked Pond holds a non-propagating pull and re-runs on S's advance).
**Deferred:** auto-*solicit* (forcing an idle S to rebuild a genuinely-deleted asset — today the operator
re-triggers S) and the stale-`f` escalation. `missing_asset` is transient (not persisted — re-derived on
the next read attempt after a restart).

**Q5 answered:** the Pond that had things deleted/reset is **healthy** — never failed, never blocked; its
next genuine run rebuilds whatever its code still produces. Only a consumer that actually *hits* the gap
parks, and it parks as **blocked-with-a-reason**, not failed — **no new top-level state**, just a
`blocked_reason` surfaced in status/UI (the precedence `failed → killed → blocked → running → queued →
idle` is untouched). And it makes delete's promise fully coherent: *a deleted asset rebuilds lazily, when
next genuinely demanded — where a consumer's read-miss counts as demand.* A table nobody reads stays gone;
a table someone needs returns within one solicited run.

## Kept vs cleared

**Always kept** (this is what makes it a reset, not an undeploy):

- The deployed **artifact** — `ponds/{name}/{version}/` + the `pond_name`/`pond_version`/`pond`/
  `pond_to_pond` identity + topology rows.
- **Operational config** — `pond_trigger` (standing Wave/Tide), `pond_window`, `pond_spout`, `pond_retry`
  (budgets), `alert_channel`. Survives, like it survives a redeploy.
- **Demand** — `pond_target` and the pull tokens are **preserved** (the core rule); reset rewinds freshness
  but not intent.
- Catchment-level: `secrets.json`, `config.toml`, `catchment_key`/`catchment_meta`, registrations.

**Cleared** (a Pond line = `ponds/{name}/m{major}/`):

- `registry.duckdb` (all tables + Trickle state).
- `data/` — the whole published dir: every table collection, all Objects, the sidecar, the Iceberg catalog.
  (No orphan-prune subtlety any more — reset clears the lot at reset time; see freshness below.)
- `pond.db` (the Duck's ledger).
- **Freshness** in `pond_state` → `NEVER` (`start_f`/`end_f`/`changed_f`), fault fields cleared. **Demand
  fields kept.**
- **Optional** (`--clear-history`, default keep): `pond_run`/`ripple_run`. `pond_version_schema` (the
  additive contract high-water) is **kept** — a reset shouldn't lower a major line's contract.

## Freshness — rewind to NEVER, uniformly, no data gap

With demand preserved and Mechanism 1 in place, **both scopes rewind freshness to `NEVER`** — the earlier
"per-Pond must rebuild forward / post-rebuild orphan-prune" reasoning dissolves:

- A downstream consumer **cannot start a run against a `NEVER` Source** (`can_start_pond`: it needs
  `sourceF > startF`, and `NEVER` is not), so the cleared `data/` has **no gap to guard** — nobody reads an
  empty Source. (A manual `force` on a consumer during the window lands safely in Mechanism 2's
  missing-asset path.)
- The reset Pond re-runs from scratch on its next eligible demand at a **forward** freshness (wall-clock),
  `previous_f = NEVER` → full Source reads + re-bootstrap → floor raised, so a downstream then
  coverage-misses and reloads normally.
- Catchment reset rewinds *everything* together — a consistent fresh-deploy state, exactly what a first
  deploy looks like. Standing triggers (kept) re-drive it; a pull-only Catchment waits for a Tap.

So reset becomes almost pure **state-clearing** — the demand-model correctness lives in the two
prerequisites, not in reset.

## Mechanism (reset itself)

### Per-Pond reset (`Driver.reset_pond`)

1. **Idle-gate** — reject (409) if a Run is in flight (`start_f > end_f`), exactly like delete. Deletes and
   resets are barred during a run (**Q6**): the Duck holds the registry single-writer and a mid-run scrub
   races the run's own export. The operator `sleep`s first if they want it now — no hidden termination.
2. **Terminate the Duck** (free `registry.duckdb`); delete the `registry.duckdb` + `pond.db` files and
   `rmtree` the `data/` dir.
3. **`duck.db`**: rewind `pond_state` freshness/fault to fresh-deploy (`NEVER`, `_clear_halt`), **keep**
   `pond_target` + pull tokens; optionally clear `pond_run`/`ripple_run`.
4. **Reload** this Pond's engine state; the demand-restoration invariant + preserved demand re-drive the
   rebuild on the next eligible tick. `--downstream` extends the scrub to the connected subtree.

### Catchment reset (`Driver.reset_catchment`)

1. **Quiesce all** — terminate every Duck, cancel pending jobs (stop-the-world; the confirm dialog says so).
2. **Filesystem sweep** of `ponds/{name}/m*/` — delete `registry.duckdb` + `pond.db` + `data/` per line,
   keeping the `ponds/{name}/{version}/` artifact dirs.
3. **`duck.db` runtime reset** — every `pond_state` → initial (`NEVER`, fault cleared); keep demand +
   topology + all operational-config tables; clear the `alert_delivery` outbox; optionally clear history.
4. **Reload** — a fresh-deploy engine; rebuild lazily (triggers + demand) or, with `--rebuild`, drive a Tap
   at the Outlets.

Draws/Spouts (real Ponds): a reset clears a Draw's landed data (it re-draws) and a Spout re-delivers from
scratch (overlaps `resync`).

## Surfaces

- **CLI**: `duckstring reset {pond} [--downstream] [--clear-history] [-y]` and
  `duckstring catchment reset [--rebuild] [--clear-history] [-y]`. Destructive → a confirmation prompt; the
  Catchment form spells out the blast radius (N Ponds, keeps deploys/config/secrets).
- **API** (both `dependencies=[auth.full]`): `POST /api/ponds/{name}/reset`, `POST /api/catchment/reset`.
  409 if a targeted Pond is mid-run.
- **UI**: a per-Pond **Reset** in the Sidebar Control set (beside Force/Refresh/Kill) and a catchment-wide
  **Reset** under `ControlsPanel` (beside Secrets/Alerts, full-only), each behind the themed `ConfirmDialog`
  with the blast radius spelled out.

## Relationship to undeploy (out of scope, the sibling)

Reset **keeps** the Pond (artifact + identity + config) and scrubs runtime. **Undeploy** removes the Pond
entirely and must handle downstream that still pins it. Plan separately; it shares reset's clearing step.

## Build order

1. **Mechanism 1** — `engine.restore_demand` + wire into the scheduler tick. Pure engine; behavioural
   tests over the sim (stranded pull re-solicited; stranded target re-propagated; parks under a blocked
   Source and heals on unblock).
2. **Mechanism 2** — `MissingSourceAsset` in `core.read_table`/`read_delta`; the Duck event kind; the
   Catchment park-blocked-with-reason + solicit + on-change recover + stale-`f` escalation; `blocked_reason`
   on status. (Also retro-fixes delete's downstream, which today burns budget + alarms on a read-miss.)
3. **`Driver.reset_pond`** + route + CLI + UI + tests.
4. **`Driver.reset_catchment`** + route + CLI + UI + tests.

## Tests

- **Mechanism 1**: a Pond whose pull/target was dropped is re-solicited within a tick; a blocked-Source
  path parks then heals on unblock; no spurious demand on a quiescent graph.
- **Mechanism 2**: deleting a table a downstream reads parks the downstream *blocked-with-reason* (no
  failure, no budget burn), solicits the Source, and recovers when it republishes; a permanently-removed
  table (Source ran fresher, still missing) escalates to failed.
- **Per-Pond reset**: scrubs registry + data (incl. an Object) + ledger; the Pond rebuilds on preserved
  demand; a standing window/spout/trigger survives; freshness rewound; idle-gate rejects mid-run.
- **Catchment reset**: runtime cleared, artifacts + config + secrets intact; the chain rebuilds from Inlets
  down (real-Duck e2e); `--rebuild` drives it without a manual Tap.
