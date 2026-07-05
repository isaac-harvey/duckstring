# Selector UI + bulk Pond operations

Two threads:

1. **Top-bar cleanup** — collapse the growing top-right button stack into a single **Options** menu
   attached to the badge, fixing the mobile overlap and making room for new entries.
2. **A generalised Pond Selector + bulk operations** — turn the Repair-only canvas-selection interface
   into a reusable *Selector* (manual / All / Tree / Downstream) feeding a set of operations
   (repair, refresh, remove, wipe history, reset, sleep, kill) applied in a fixed precedence, with a
   backend primitive and matching CLI.

---

## Part A — The Options menu (top bar)

### Current state
- **Top-left** `StatusPanel` (`DagCanvas.tsx`): logo + wordmark, connection dot, **pond count**,
  **AccessBadge** (Manage | Demand | Read chips).
- **Top-right** `ControlsPanel`: *Collapse all* / *Expand all*, **Secrets** (full), **Alerts** (full),
  **Reset all** (full), plus the `SecretsMenu` / `AlertsMenu` / `ConfirmDialog` popouts.
- On mobile these two panels overlap (badge left, buttons right, both `position: absolute`).

### Target
- **Keep always visible** in the badge: catchment name + connection status. Nothing else.
- **Remove entirely**: the pond count ("N ponds") — both mobile and desktop rows.
- **Move into the Options menu**: the AccessBadge (access levels are not important enough for the
  always-on badge), Collapse-all/Expand-all, Secrets, Alerts, and a new **Pond Actions…** entry.
- **Reset all** is retired as a standalone button — it becomes the *All* selection + *Reset* operation
  in the Selector (see Part C). (The whole-catchment reset is still reachable, just via the Selector.)

### The badge → a bar
Turn the whole top-left badge into a **single horizontal bar** so it stays clean and the Options control
has a natural home (rather than an icon jammed next to the logo):

```
[ Logo | Name | Status ● | ☰ Options ]
```

The **Options** control is the right-most segment (a three-bars `☰` icon — the standard, mobile-friendly
affordance). Clicking it drops the **Options menu beneath the bar, matched to the bar's width**. The
top-right `Panel`/`ControlsPanel` is deleted outright (killing the mobile overlap). Menu items:
  - `Collapse all` / `Expand all` (always; only when there are collapsible Ponds)
  - `Secrets` (full)
  - `Alerts` (full)
  - `Pond Actions…` (full) — opens the Selector (Parts B/C)
  - Access levels, shown as the existing three chips at the foot of the menu (read-only info).

Each leaf that opens a panel (Secrets, Alerts) uses one shared presentation: **a popout beside the menu
on desktop, a full-screen modal on mobile** — the Secrets behaviour the author likes. Factor into a
small `MenuPanel` wrapper (desktop: absolutely-positioned card next to the menu; mobile:
`position: fixed; inset: 0` sheet with a header + ✕), rendering `SecretsMenu` / `AlertsMenu` through it.
`Pond Actions…` instead enters the canvas Selector mode and closes the menu.

### Files
- `frontend/src/components/DagCanvas.tsx` — delete `ControlsPanel` + the top-right `Panel`; trim
  `StatusPanel` (drop count + AccessBadge from the badge). Add an `OptionsMenu` component (new file
  `OptionsMenu.tsx`) rendered inside the top-left `Panel` next to the badge.
- `frontend/src/components/OptionsMenu.tsx` (new) — the menu + the `MenuPanel` shell; owns the
  Collapse/Secrets/Alerts/Pond-Actions/Access items and their open state (lifting the `useState`
  currently in `ControlsPanel`).
- `SecretsMenu.tsx` / `AlertsMenu.tsx` — adapt to render inside `MenuPanel`.

---

## Part B — The Selector (generalise Repair)

### Current state
Repair is a bespoke canvas-selection mode in `store.ts`:
`repairMode` / `repairScope` / `repairError`, `enterRepair` / `exitRepair` / `toggleRepair` /
`addRepairDownstream` / `submitRepair`. `PondNode.tsx` toggles a Pond in/out of `repairScope` on click
(bright-ring highlight). `App.tsx`'s `RepairBanner` is the top toolbar (count · Include downstream ·
Repair N · Cancel). Entered from the per-Pond Sidebar (`Sidebar.tsx:716`).

### Target — rename/generalise to a **Selector**
Store state `repairMode → selectorMode`, `repairScope → selectorScope`, `repairError → selectorError`,
plus a second phase for operations (Part C). Selection helpers:

- **toggle(id)** — existing manual click (kept).
- **All** — select every Pond in the Catchment (`Object.keys(ponds)`, excluding Spouts/Draws).
- **Tree** — select every Pond in the *weakly-connected component(s)* of the current selection: BFS
  over **undirected** source edges (both directions) from each currently-selected Pond. ("indirectly
  connected to any selected Pond".) New `selectTree()`.
- **Between** — add every Pond lying on a directed path *between* two selected Ponds: a node `n`
  qualifies when some selected `a` reaches `n` downstream **and** `n` reaches some selected `b`
  downstream (i.e. `n ∈ descendants(S) ∩ ancestors(S)`, selected nodes included). New `selectBetween()`.
- **Downstream** — existing directed-descendants closure (`addRepairDownstream` → `addDownstream`).
- **Clear** — empty the scope (new, handy once All/Tree/Between exist).

`PondNode.tsx`: rename the `repairMode`/`toggleRepair`/`inRepair` reads to the selector names; keep the
ring highlight.

`App.tsx`: `RepairBanner → SelectorBanner`, a two-phase top toolbar:
- **Phase 1 (select):** buttons `All` · `Tree` · `Downstream` · `Clear`, live `N selected`, then
  `Choose actions →` (disabled at 0) and `Cancel`.
- **Phase 2 (operations):** the operations panel (Part C) — either inline in the banner or via a
  `MenuPanel` modal.

Entry point: **Pond Actions…** in the Options menu (Part A) calls `enterSelector()`. The old per-Pond
Sidebar "Repair — rebuild a connected set…" button is removed (Part E).

### Files
- `frontend/src/lib/store.ts` — rename the repair block to the selector block; add `selectAll`,
  `selectTree`, `clearSelectorScope`, and the operations substate + `submitBatch` (Part C/D).
- `frontend/src/components/PondNode.tsx` — rename reads.
- `frontend/src/components/App.tsx` — `SelectorBanner`.

---

## Part C — Operations + precedence + constraints

After a scope is chosen, the operator picks one or more operations (the "second menu"):

| Op            | Meaning                                                        | Backend today |
|---------------|---------------------------------------------------------------|---------------|
| **Kill**      | Terminate the Duck, park killed                               | `kill`        |
| **Sleep**     | Clear demand (push+pull); started runs finish                 | `sleep`       |
| **Reset**     | Delete all tables/objects + rewind freshness (per-Pond reset) | `reset_pond`  |
| **Wipe**      | Clear run history only (**no data scrub**)                    | **new**       |
| **Remove**    | Retire the line (data+config+attachments; keep deploy record) | `remove_pond` |
| **Clear**     | Clear failure / unblock (no run)                              | `clear`       |
| **Repair**    | Force-rebuild the connected set now, in dependency order      | `repair`      |
| **Refresh**   | Flag next run as a cold wipe-and-rebuild (lazy)               | `refresh`     |

### Precedence (order of application)
**Kill > Sleep > Reset > Wipe > Remove > Clear > Repair > Refresh.**

Rationale: quiesce first (Kill the process, Sleep the demand → the line is idle, which `reset_pond`
*requires*), then destroy state (Reset), trim history (Wipe), retire (Remove); then the recovery ops —
Clear (drop fault state) → Repair (rebuild now) → Refresh (flag next run).

### Constraints
- **Remove implies Reset** — Reset is auto-selected (locked on) when Remove is chosen; because Remove
  already scrubs, the executor *drops* the redundant standalone Reset for removed Ponds (a no-op).
- **Repair is exclusive with Remove and Reset** — disable/uncheck Repair when either is on (and vice
  versa). Repair still combines with Refresh, Wipe, Sleep, Kill, Clear.
- **Reset and Wipe are NOT fused** — applied as two independent steps. Wipe alone (clear history logs
  without resetting data) is a valid, common case; keep them separate everywhere.
- **Remove is terminal per Pond** — once a Pond is removed, later ops (Clear, Refresh) silently skip it.
  This resolves Remove+Refresh (allowed by "others combine with anything", but a removed line has no
  next run) without a hard exclusion.
- All other ops combine freely.

### Repair over a disconnected scope
`_connectivity_gap` only rejects a **skipped intermediate** (two selected Ponds connected in the full
graph but not *within* the selection) — it does **not** reject genuinely disjoint components. All/Tree/
Between are closed under "path between two members", so they never have a gap. The batch executor makes
**one** `repair` call over the live (non-removed) scope; `repair` handles multiple disjoint components in
one plan (each component's roots release independently). A gappy *manual* selection is correctly
rejected (surfaced as a per-op error) — tell the user to use Tree/Between or include the connector.

### Wipe + Remove
Distinct from `remove(wipe=True)`: Wipe clears `pond_run`/`ripple_run` while Remove(retire) keeps the
deploy record — net: a redeploy un-retires with no history. Bulk Remove defaults to **retire**
(`wipe=False`); the standalone Wipe op composes the history clear. The purge (`remove --wipe`) stays the
single-Pond Danger action / CLI flag, out of the bulk path.

### UI (operations panel)
Checkboxes for each op with the constraints wired (disable Repair when Reset/Remove on; auto-lock Reset
under Remove). Show the resulting **precedence-ordered plan** as a sentence, e.g. *"Kill, then Sleep,
then Reset, then Wipe — 6 Ponds"*. A single **Apply** button.

### Confirmation
Any op set containing an **irreversible** op — **Reset, Wipe, Remove** — requires typing the catchment
name (enforced **server-side** in the batch endpoint, so the CLI must supply it too). In the UI, reuse
`ConfirmDialog` with `requireTyped: catchment.name` (fallback string when unnamed), the body describing
the ordered plan + affected count. Reversible-only sets (Sleep/Kill/Clear/Repair/Refresh) apply without
the typed gate (a normal confirm is fine).

---

## Part D — Backend primitive + CLI

Applying an ordered op-set to a set of Ponds is cleanest as **one backend call** (atomic precedence,
one confirm check, one partial-failure report, and CLI/UI parity — the "associated change to CLI" the
author expects). Frontend-looping would duplicate ordering/constraint logic and lose atomicity.

### New endpoint
`POST /api/ponds/batch` (`dependencies=[auth.full]`), body:
```jsonc
{
  "ponds": [{"name": "sales", "major": 1}, ...],
  "operations": ["kill", "sleep", "reset", "wipe", "remove", "clear", "repair", "refresh"],
  "confirm": "<catchment name>"   // required iff operations ∩ {reset, wipe, remove} ≠ ∅
}
```
Server: validate the op-set against the constraints (422 on repair+remove/reset, etc.); require
`confirm == catchment name` when any irreversible op is present (422 otherwise); resolve each pond to a
`name@major`; execute in precedence order (Reset and Wipe as separate steps; Remove terminal per Pond;
Repair per connected component). Return a per-op summary
(`{applied: {...}, skipped: [...], errors: [...]}`). Best-effort per Pond with collected errors (don't
abort the whole batch on one failure) — mirror how the CLI/UI want to surface partial results.

### New driver method
`Driver.batch(ponds, operations, confirm)` orchestrating the above by calling the existing
`kill` / `sleep` / `reset_pond` / `remove_pond` / `repair` methods plus the new wipe.

### New: wipe-history-only
No current path clears run history without scrubbing data (`reset_pond(clear_history=True)` also
scrubs). Add `Driver.wipe_history(pond)` → `DELETE FROM ripple_run/pond_run WHERE pond_version_id=?`
(the existing `clear_history` SQL, extracted). Expose per-Pond too if cheap
(`POST /api/ponds/{name}/wipe-history`), so the CLI has a single-Pond form.

### CLI
A new bulk command — proposed **`duckstring do`** (top-level, or under `control`):
```
duckstring do [PONDS...] [--all] [--tree] [--downstream] \
              --kill --sleep --reset --wipe --remove --repair --refresh \
              [--yes | --confirm NAME] [--catchment C] [--major M]
```
- Selection: explicit names, or `--all`, or `--tree`/`--downstream` expanding the given names
  (resolved client-side against `/api/status`, then sent as an explicit pond list — keeps the server
  endpoint dumb).
- Ops: one flag per operation; validated the same way as the UI.
- Irreversible ops prompt for the catchment name unless `--confirm NAME`/`--yes` is passed.
- Prints the returned per-op summary.

The existing single-Pond `repair` / `sleep` / `kill` / `reset` / `refresh` / `remove` CLI commands stay
(convenience); `do` is the multi-target superset.

### Files
- `src/duckstring/catchment/routes/orchestrate.py` — `POST /ponds/batch`, `POST /ponds/{name}/wipe-history`.
- `src/duckstring/catchment/driver.py` — `batch(...)`, `wipe_history(...)`; extract the `clear_history`
  SQL; component-split for Repair.
- `src/duckstring/cli/control.py` (or new `cli/bulk.py`) — the `do` command; register in the CLI app.
- `frontend/src/lib/api.ts` — `batchPonds(...)`; keep `repairPonds`/`resetCatchment` or route them
  through the batch endpoint.
- Docs: update `docs/` control/CLI pages; note in CLAUDE.md (Triggers & control section).

---

## Part E — Sidebar simplification

Per the author: operations that are "done as a collection" leave the per-Pond Sidebar; the Selector
always allows a single target.

Remove from `Sidebar.tsx`:
- The **Repair** button (`~716`, Failures section) — replaced by Options → Pond Actions.
- The **Refresh** + **Reset** grid (`~673–684`, Control section).
- The **Danger / Remove Pond** section (`~735+`).

Keep in the Sidebar:
- **Control**: Force / Wake / Sleep / Kill (single-Pond lifecycle — Sleep/Kill are *also* in the bulk
  Selector; the single-Pond buttons stay for convenience).
- **Failures**: retry budgets + Clear Failure.
- Triggers, Spouts, Alerts, TraceChart.

(This also drops `refreshPond` / `resetPond` / `removePond` / `enterRepair` reads from the Sidebar; the
`ConfirmDialog`/`setRemoveErr` plumbing there goes with the Danger section.)

---

## Settled decisions
- Menu entry is **Pond Actions…** (not "Do to Ponds").
- Selection modes: manual, **All**, **Tree**, **Between**, **Downstream**, Clear.
- **Reset and Wipe are not fused** (separate steps; Wipe-only is valid).
- **Remove implies Reset**; Repair excludes Remove/Reset; **Remove terminal per Pond** (later Clear/
  Refresh skip removed Ponds).
- **Clear** added as a bulk op.
- Badge becomes a **bar** — `Logo | Name | Status | ☰ Options` — with the menu dropping beneath at the
  bar's width.
- Spouts/Draws excluded from All/Tree/Between and bulk ops (no local output).

---

## Test plan
- `tests/test_bulk.py` (new) — `Driver.batch` precedence, constraint rejection (repair+remove → error),
  confirm gating, Reset+Wipe fusion, Repair component-split, partial-failure reporting; `wipe_history`
  clears history without touching data/state.
- Extend `tests/test_engine.py` only if new engine primitives appear (none expected — batch composes
  existing methods).
- Frontend: manual — Selector All/Tree/Downstream on the demo graph; op-constraint locking; typed
  confirm on irreversible sets; mobile Options menu + no top-bar overlap.
- `ruff check .` before finishing.
```
