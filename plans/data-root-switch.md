# Switching the data-plane target (empty, adopt, migrate)

Status: **empty+dormant switch + adopt built** (`Driver.switch_data_root(new, mode="empty"|"adopt")`,
`PUT /api/catchment/settings` with `confirm`+`mode`, the Cloud-menu Data Plane switcher with the
Empty/Adopt choice). Adopt restores each line's freshness from the target's `_trickle.json` sidecar
(`_plane_freshness`), drops the local hot state to re-hydrate from the target, and keeps demand/triggers.
Tested in `tests/test_cloud_settings.py`. **Migrate (built-in server-side copy) still to build** — step 2.

The data root was set-once (refused once a Catchment had published data or an existing root). It's now
switchable, but only in one shape: an **empty + dormant** reset. This plan adds the two shapes that make
it a real data-plane move — **adopt** (pick up a plane that already has data, no rebuild) and **migrate**
(copy the current plane somewhere else, then adopt it) — and records why they lean almost entirely on
plumbing that already exists.

## Background: where state lives (the two-layer split)

- **Hot state** — a Pond's working `registry.duckdb` + `pond.db` ledger — is **always local to the box
  running that Pond's Duck** (`registry.pond_major_dir`, never an object store), and is **reconstructable
  from the published data** (`dataplane.hydrate_registry`, the export inverse — the registry-loss recovery
  path). It is a cache, not the system of record.
- **The data plane** — what a Pond publishes — follows `data_root`: local `{root}/ponds/{name}/m{major}/data`
  when unset, or `{data_root}/{name}/m{major}/data` (s3/gs) when set. `registry.pond_data_dir` resolves the
  layout; everything goes through the `Storage` abstraction (`LocalStorage` / `ObjectStorage`).
- **Freshness** (`pond_state.start_f/end_f/changed_f`) lives in `duck.db` (the Catchment), and is
  *data-plane-specific in meaning*: "output exists up to F" implicitly means "…in the current data plane".
  Re-pointing at a different plane desyncs the ledger from the bytes — that coupling is the whole problem.

## The universal freshness sidecar (the key enabler)

Every Pond publish writes a `_trickle.json` sidecar into the data dir via `dataplane.publish_plan`
(despite the name, it is **not** Trickle-specific — it is the per-table state file for *all* output):

- plain overwrite output → `{"mode": "overwrite", "f": <run freshness>}` (dataplane.py:268)
- a Trickle table → `{"mode", "pk", "floor", "f": <run freshness>}` (dataplane.py:270)
- **objects** (non-tabular) → the sidecar's `"objects"` section, `{name: {f, is_dir, ext, size}}`
  (`objects.commit_objects`) — already `f`-stamped, same as tables.

So the freshness of **everything** a Pond publishes already lives in the data plane, per table/object,
stamped with the producing run's `f`. A cross-Pond reader already resolves an overwrite source's currency
from this file (that's what it's for). **No new state file is needed** — restore-to-freshness from the
plane alone is possible for every Pond type.

## The three shapes

`switch_data_root(new_root)` becomes `switch_data_root(new_root, *, mode)`:

### 1. `empty` (built)
Re-point → rewind all freshness to `NEVER` (cold refresh pending) → clear **all** demand (pull, push,
standing Wave/Tide triggers) → **do not** kick a rebuild. Leaves every Pond "as if its data were deleted"
and **dormant**, so the operator drives the outcome. NON-destructive: the old location is untouched.

### 2. `adopt` (to build)
Pick up whatever data already sits in the target plane (a bucket you `aws s3 sync`'d into, or one you're
returning to) and **resume incrementally, no rebuild**:

1. Quiesce Ducks; re-point `data_root` (driver + launcher `set_data_root`).
2. For each Pond line: read its sidecar from the **target** plane (`trickle_io.load_sidecar` over
   `pond_data_dir(root, name, major, new_root)`). Derive the line's freshness from the stamped `f`
   (the tables/objects of a run share the run `f`; take the max). Empty dir / no sidecar → `NEVER`.
3. Set `pond_state.start_f/end_f/changed_f` to that `f` (leave demand + triggers intact so it resumes).
4. **Drop the local registries** for each line (delete `registry.duckdb` + `pond.db`) so the next run
   **re-hydrates from the target plane** (`hydrate_registry`, the existing registry-loss path) rather
   than computing against stale hot state from the previous plane.
5. `reload()`; do **not** rewind or clear demand. The pipeline resumes from the adopted freshness.

Because it reads the truth from the data, adopt is **one mechanism for every case** — hand-copied bucket,
old bucket, or empty target (empty → `NEVER` everywhere → equivalent to a cold start, which the standing
triggers then rebuild). It is direction-agnostic: local→s3, s3→local, s3→s3, local→local all identical,
because the sidecar read + hydrate both go through `Storage`.

### 3. `migrate` (to build, = copy + adopt)
Copy the current plane to the new location, then `adopt` it. The copy is the only heavy part; two ways,
composable:

- **Bring-your-own** — the operator runs `aws s3 sync old new` (native, server-side, resumable), then
  `adopt`. Almost no new code; the honest S3-native path.
- **Built-in** — a server-side copy (boto3 `CopyObject` for s3→s3; up/download for local↔s3) so
  "Migrate" is one action. The generic `Storage` tree-copy streams *through* the Catchment, so the
  built-in path must use server-side copy where available (never route TB through the Catchment box).

**What to copy:** the `data/` subtree of each `pond_data_dir`. It is self-contained flat Parquet
(sidecars + Trickle base/bands/changelog/droplog parts + objects + `_trickle.json`). Do **not** carry the
Iceberg **catalog** across: `IcebergDataPlane._load` returns `None` when `catalog.json` is absent and
`_raw_read_select` falls back to the flat sidecar, so reads serve from the copied flat files and the next
overwrite run re-commits a fresh Iceberg catalog with correct new-location paths. (In local mode the hot
state — `registry.duckdb`/`pond.db` — sits in the *parent* `m{major}/` dir, not `data/`; don't copy it,
adopt re-hydrates it.)

## Settled nuances

- **Sidecar `f` is the last *publish* (= `changed_f`), not the pass heartbeat.** A no-change "pass"
  advances `end_f` without a Duck run, so it writes nothing to the plane. Adopt restores a Pond to "fresh
  as of its last real output change," which exactly matches the bytes and is safe — the pass-advanced
  `end_f` re-derives for free on the next tick (a pass is cheap). Arguably the more correct value.
- **Registries always dropped on adopt** (not conditionally): the target may hold data that differs from
  the current registry (old bucket), so re-hydrate-from-target is the robust choice. For the "I copied the
  current data" case the hydrate is redundant but harmless. The cost is one base read on the next run.
- **Objects** need no new work — already `f`-stamped in the sidecar; `hydrate_registry` already skips the
  `objects`/`state` sections as base tables and objects are read straight from the plane.
- **Confirmation** (`confirm == catchment name`) gates every non-first switch regardless of mode — it
  moves where all data lives.
- **Non-destructive** always: no mode scrubs the old/source location.

## Build order

1. `adopt` mode on `switch_data_root` (freshness-from-sidecar + registry drop + preserve demand) + the
   `mode` param on `PUT /api/catchment/settings` + the Cloud-menu "Adopt existing data" choice on the
   switcher. This alone makes a pre-copied bucket usable with no rebuild.
2. `migrate` = the built-in server-side copy on top (s3↔s3 `CopyObject`, local↔s3 up/down), so one action
   copies + adopts.

## Testing

- Unit: adopt reads sidecar `f` → sets ledger; empty target → `NEVER`; registries dropped; demand/triggers
  preserved (contrast the `empty` mode's cleared demand). Local↔s3 parity via a local path as the "remote"
  root (no network), as the existing `test_cloud_settings` switch tests do.
- e2e (deployed Duck): publish on plane A → `aws`-equivalent copy of the `data/` subtree to plane B →
  adopt B → assert the next run resumes incrementally (reads a delta, not a full rebuild) and the data
  matches. Reuse the Trickle demo chain.
