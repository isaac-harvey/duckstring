# Removing a Pond (a `name@major` line)

Status: **implemented.** `Driver.remove_pond(name, major)` + `DELETE /api/ponds/{name}` +
`duckstring pond remove` + a Sidebar **Remove Pond** button (type-`name@version` confirm). Prerequisite
(alerts → `name@major` scope) landed first. Tests: `test_remove_pond_retires_line` (real-Duck: scrub +
history kept + artifact survives + Spout/scoped-alert cascade + catchment-wide-alert survives + downstream
blocks) and `test_remove_pond_rejects_with_demand`.

Retire one deployed major line — the executable object `name@major` — from a
Catchment. The main use case is dropping an old major that nothing runs any more. The sibling of
[reset](reset.md): reset scrubs a Pond's runtime and keeps it live; remove takes the Pond out of the live
system entirely, **keeping its deployment record + run history**.

## Prerequisite — alert channels become `name@major`-scoped

Today a scoped `alert_channel` is bound to a Pond **name** (it "survives version/major changes"). That is
inconsistent with every other Pond-attached construct (Spouts, windows, triggers, retry — all `name@major`)
and it means a per-major removal can't cleanly take its alerts. **Fix first:** re-scope a Pond alert
channel to `name@major` (a `major` alongside the scope name; a catchment-wide channel stays unscoped).
Once that lands, removal treats alerts exactly like Spouts. See the "prerequisite fix" note below and the
separate alerts-scoping change.

## Scope — the removable unit is `name@major`

The runtime identity is the pond key `name@major` (the `pond` row = the selected version of one major
line). A `pond_name` can have several independent live majors; each is a separate removable line. Remove
targets **one** `name@major`. It never touches other majors of the same name, and it never deletes
`pond_name` (which survives for the other majors, the history, and any future redeploy).

## History is kept (no purge, for now)

A removal is a **retire**, not a wipe. It deletes the *live* line and its runtime; it keeps `pond_name`,
the `pond_version` rows for that major, and the canonical run history (`pond_run` / `ripple_run`). Because
the engine only loads *selected* Ponds (those with a `pond` row), deleting the `pond` row makes the line
vanish from the live graph, status, and scheduling, while `/api/runs` (keyed on `pond_version`) keeps
serving its history. A later redeploy of `name@major` re-selects a `pond` row and the line is back, history
intact — retire is naturally reversible.

**Keeping history forces the FK chain.** `ripple_run` references *both* `ripple` and `pond_version`, and
`pond_run` references `pond_version`; FK enforcement is on (`PRAGMA foreign_keys = ON`). So a no-purge
removal **cannot** delete the version-scoped rows (`pond_version`, `ripple`, `ripple_to_ripple`,
`pond_version_schema`) — and shouldn't; they *are* the record. Removal only deletes the live selection +
its `pond(id)`-keyed config. (A separate future `--purge` — explicitly out of scope now — would delete the
version rows + history + the `{version}/` artifacts, gated by the `pond_to_pond.source_pond_name_id` FK,
which already forbids deleting a `pond_name` that a live sink still declares.)

## Dependents & cascade — none, by thesis

**Removal is allowed regardless of downstream dependents.** No Pond is aware of its sinks, only its
sources — so removal never inspects or refuses on dependents. A sink that pins the removed `name@major`
simply re-derives `has_missing_source` on the next `reload()` (the check is **major-aware**:
`pond_key(name, smajor)` against the deployed set, [driver.py:232](../src/duckstring/catchment/driver.py#L232))
and parks **hard-blocked** with the source named — the honest, source-driven signal. The operator then
fixes each sink on its own schedule (redeploy without the pin, repoint, or remove it too). There is **no
auto-cascade** to independent sinks.

What removal *does* clean up is the line's **own attachments** — its Spouts and its alert channels:

- **Spouts** — a Spout is a real `pond` (kind `outlet`, named `{source}#{spout}`) wired to a specific
  source major via `pond_to_pond`; it has no meaning once its source line is gone, so each Spout bound to
  this `name@major` is removed with it (itself a line removal — Spouts have no downstream).
- **Alert channels** — scoped to `name@major` (like every other Pond-attached config; this scoping is a
  **prerequisite fix**, below), so the line's channels are removed with it. A catchment-wide channel
  (unscoped) is untouched.

## Guard — idle and demand-free

Reject the removal (409) unless the line is fully quiescent:

- **not running** (`start_f == end_f`, no in-flight run), and
- **no demand** — no `has_pull`, no pending `targets`, no standing `pond_trigger` (Wave/Tide), no
  `standing_wake` (a Spout).

The remedy is one command: **`duckstring control sleep {pond}`** clears pull + targets + cancels the
standing trigger, so `sleep` then `remove` is the clean path. (This is stricter than reset, which keeps
demand — because reset re-runs the Pond, whereas remove ends it.)

## Mechanism (`Driver.remove_pond(name, major)`)

Under the driver lock (so the whole thing is atomic — no tick or event interleaves, which is why no
transient "blocked" marking is needed):

1. **Guard** — reject if running or has demand (above).
2. **Quiesce + scrub disk** — terminate the Duck (`wait=True`, freeing `registry.duckdb`), `rmtree` the
   `ponds/{name}/m{major}/` runtime tree (registry + `pond.db` ledger + local `data/`), and if a remote
   `data_root` is set, `rmtree` the line's remote data dir (reuse `reset_pond`'s disk-scrub).
3. **Remove its attachments** — each `is_spout` Pond whose `pond_to_pond` source is this `(name, major)`
   (remove that Spout line: its `pond(id)` config + `pond` row + egress worker state), and every
   `alert_channel` scoped to this `name@major` (+ its `alert_delivery` outbox rows).
4. **Delete the `pond(id)`-keyed config** — `pond_state`, `pond_target`, `pond_retry`, `pond_window`,
   `pond_trigger`, `pond_spout`, and `pond_to_pond WHERE pond_id = ?` (this line as a *sink*). FK order:
   these children first.
5. **Delete the `pond` row.** (`pond_version`, `ripple`, `ripple_run`, `pond_run`, `pond_version_schema`,
   `pond_name`, and the `{version}/` artifacts all stay — the retained record.)
6. **`reload()`** — rebuild the engine from the DB. The line is gone from `state`/`meta`/`jobs`; every sink
   that pinned it re-derives `has_missing_source` and hard-blocks with the source named.

Returns what was removed: the pond key, the Spouts taken with it, and the sinks now blocked-on-missing (so
the caller can report the blast radius).

## Surfaces

- **API**: `DELETE /api/ponds/{name}?major=M` (full-gated). 409 if running or has demand. Returns
  `{removed, spouts_removed, now_blocked}`.
- **CLI**: `duckstring pond remove {name} [--major M] [-y]` (default major = highest deployed). Prints the
  blast radius (Spouts removed, sinks now blocked) and confirms; `-y` skips. On a not-idle Pond, the error
  says to `control sleep` first.
- **UI**: a **Remove** button at the bottom of the Pond Sidebar (full-only), opening the themed
  `ConfirmDialog` with `requireTyped` = the Pond's **`name@version`** (what the node shows, e.g.
  `catalog@1.4.2`) — the type-to-confirm gate, since it's destructive and against the read-only-topology
  grain. The body states the Pond, that its data + live state go but its run history is kept, and lists the
  Spouts that go with it + any sinks that will block. The backend resolves the typed version to its major
  line. Disabled (with a hint to sleep first) when the Pond is running or has demand.

## What removal deliberately does not do

- **No `--purge`** (delete history + version rows + artifacts) — a later, separate feature.
- **No dependent refusal, no cascade** — sinks block via `has_missing_source`; the operator handles them.
- **No `pond_name` deletion** — it's forced-kept by the history FK chain and shared across majors.

## Build order

0. **Prerequisite:** re-scope Pond alert channels to `name@major` (schema + CRUD + matching + UI). Build
   and land this first.
1. `Driver.remove_pond(name, major)` — the guard + quiesce/scrub (reuse reset) + Spout/alert removal + row
   deletes + `reload()`. Return the blast radius.
2. The `DELETE` route + `pond remove` CLI.
3. The Sidebar Remove button + type-to-confirm dialog (reuse `ConfirmDialog`).

## Tests

- Real-Duck e2e: deploy the chain, remove `catalog@1` — it vanishes from status, its `m1/` disk is gone,
  its history survives (`/api/runs?pond=catalog` still returns rows), and `priced` (a sink pinning it)
  goes blocked-with `has_missing_source` naming `catalog`.
- Guard: remove is rejected (409) while running and while a standing Wave is active; succeeds after
  `sleep`.
- A Spout on the removed line is removed with it; an alert channel scoped to the line is removed too, while
  a catchment-wide (unscoped) channel survives.
- Removing `name@1` leaves `name@2` (if deployed) fully intact.
- Redeploying the removed `name@major` restores it, with the pre-removal history still present.
