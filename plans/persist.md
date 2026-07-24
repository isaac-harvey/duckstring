# Plan: Persist — local-first publish, async durability, Pool-aware freshness

> **Status: phase 2 IMPLEMENTED and verified; phases 3–6 designed, not built.** Settled with the author
> 2026-07-24. Companion to `plans/s3-resident-state.md` (the *hydration* direction; this plan is the
> *publish* direction and the Pool model both rest on).
>
> **Built (phase 2):** local-first publish + the Duck-side async Persist + `persisted_f`. A Catchment-Pool
> Duck publishes to the local layout (`--persist-root`, passed by `SubprocessLauncher`; remote launchers
> keep publishing straight to the data root) and `persist_tree` mirrors it to the data root off the run's
> critical path (coalescing, replay-safe, refuses an empty local, prunes what local dropped, skips the
> local-only Iceberg catalog). Reads resolve **local-first** (`resolve_data_dir`: Duck foreign reads, the
> data viewer, serving, egress). The `persist` Duck event → migration 027 (`pond_state.persisted_f` +
> the `pond_persist` log — a separate log item from the Pond Run) → `/api/status.persisted_f`. Hydration
> falls back local → persist layer. A data-root **switch** scrubs the local publish cache when leaving a
> non-local plane (it would shadow the new plane); `migrate()` copies from the local-first source (the
> old plane's mirror may lag `persisted_f` behind `end_f`). Verified against a real ap-southeast-2
> bucket: local publish ~0.01s on the critical path, persist 0.2–0.7s off it, delta mirrors O(new parts).
> E2E `test_local_first_publish_persists_to_data_root`; units `tests/test_persist.py`.
>
> **Built (phase 3):** Pool-aware gating is LIVE. The engine carries `Pond.pool` + `Pond.async_persist`
> and `PondState.persisted_f` (tracks `end_f` exactly on completion for a durable-at-completion publish;
> advanced by `record_persist` for an async one — monotonic, never regressed by replay). The one-rule
> protocol is `source_visible_f` (engine/catchment.py): a Sink aggregates its Sources' *visible*
> freshness — `end_f` same-Pool / non-async, `persisted_f` cross-Pool — used by `pond_source_f` and the
> missing-asset re-attempt gates. The Driver resolves each Pond's Pool identity (`_pool_of`, mirroring
> the DispatchingLauncher's real routing incl. the degrade-to-local path; a remote spawn is a
> Pool-of-one in v1; misclassification is fail-safe — only ever conservative), populates the engine at
> reload, restores `persisted_f` on restart (async: the DB watermark; others: `end_f`), and drives the
> cascade on the Duck's `persist` event (`_record_persist` → engine → `_process`). The mirror uploads
> the **sidecar last** (the commit marker — mirrored data always supersets its claims, so a cross-Pool
> reader never sees a torn window). `persisted_f` on `/api/status` now reads the engine value (= `end_f`
> for durable-at-completion Ponds). theory.md carries `persistedF` + `Source.visibleF` in the state
> pseudocode. Tests: engine `test_cross_pool_sink_gates_on_persisted_f_same_pool_on_end_f` (+
> monotonicity, both completion modes), driver `test_driver_persist_event_releases_cross_pool_sink`.
>
> **Built (phase 4):** Pool-loss rollback. `Driver._reconcile_lost_publishes` (runs on every reload,
> thus every boot) reconciles each async_persist Pond's claims against surviving bytes: healthy = one
> local file read (the local sidecar backs `changed_f` — content-anchored, never `end_f`: passes publish
> nothing); suspicious = read the plane sidecar, and **the plane is truth** — it advances `persisted_f`
> if ahead (this dissolved the adopt conservatism previously noted here), and a claim neither local nor
> plane backs regresses (Pond + Ripples) to the surviving floor, failure episodes above it cleared,
> normal demand re-running the gap. Fate-sharing (argued in "The correctness core" below) is what makes
> the regression coherent. Deterministic from (DB, disk); re-runs are no-ops.
>
> **Built (phase 6):** registry eviction under disk pressure. Below `DUCKSTRING_MIN_FREE_BYTES` free
> (default 1 GiB; 0 disables; one statvfs per 30 s tick), idle demand-free Ponds shed reconstructible
> hot state LRU: the registry when a publish backs it; the local publish too only when fully mirrored
> (`persisted_f >= changed_f` — reads fall to the plane). Never touches the only copy of anything.
>
> **NOT built — phase 5** (named Pools as shared machines): the task-per-Pool launcher needs a new
> in-container component (a Pool agent supervising multiple co-resident Ducks, with spawn/stop plumbing
> from the Catchment) and **real-AWS iteration to validate** — the same gate that deferred warm-pool
> reuse before. Until it lands, a named pool spawns one machine per Pond (a Pool-of-one), which the
> phase-3 gating already models correctly; nothing is wrong, co-location off the Catchment is simply
> not yet available. This is the one remaining phase.

## The reframing

Copying data is the true bottleneck, and it only surfaces on cloud. The fastest configuration is a fully
hydrated registry persisted across runs on the Duck's own disk — the registry as a **local, idealised
cache** — with Sources' published data on the same filesystem. Today, attaching an S3 data root makes the
*publish target* S3, so a laptop Catchment with a bucket attached pays an upload + re-download for a hop
between two Ducks **on the same machine**. Pure waste: the publish is the handoff, and the handoff is local.

Key structural facts (why this plan is simpler than it first looks):

- Ducks are per-Pond; a Source and its Sink are **never the same process**. What they can share is a
  **filesystem**. There is no in-process registry-sharing problem to solve.
- Sinks never read a Source's registry — they read its **published files**. Publish *is* the handoff,
  always; the atomic tmp+replace / immutable-parts discipline already solves overwrite-while-reading. The
  entire cloud cost is *where the publish lands*.

So: split **publish** (local, the handoff, ends the run) from **Persist** (async mirror to the data root —
durability, cross-Pool consumption, serving).

## Terminology: the Pool (settled)

A **Pool is one shared filesystem** — exactly one running machine/instance/task at a time, hosting its
Ponds' Ducks as co-resident processes. Not a template, not an instance *type*: a place.

- **The Catchment is a Pool** — the box the Catchment runs on; all `catchment`-target Ducks share it.
- **A named Pool** is one shared cloud machine (one Fargate task / one EC2 instance) that multiple Ponds'
  Ducks co-reside on. This **redefines** the existing `duck_pool` construct from compute-template to place:
  the launcher becomes **task-per-Pool** (multiple Duck processes inside), not task-per-Pond. This commits
  to the previously-deferred warm-pool/instance-reuse work. `instance_type` now sizes the shared machine
  everyone in the Pool divides up; its disk is the shared registry-cache budget.
- **A Pool can only scale up, never out.** Two instances = two filesystems = not a Pool. Compute-starved →
  bigger instance_type, or split Ponds across more Pools. (Fargate tasks cannot share a filesystem; EFS is
  disqualified — NFS latency would destroy the registry-as-local-cache premise.)
- **A preset (S/M/L/XL) is NOT a Pool.** A Pond either doesn't care about compute (unspecified → the
  Catchment Pool) or cares **in isolation** (a preset → an isolated, Pond-scoped Fargate task — an
  anonymous Dedicated, a **Pool-of-one**). Presets keep the existing task-per-Pond launcher path unchanged;
  only named Pools need the new task-per-Pool launcher.
- **A Dedicated Duck is a Pool with one Pond major in scope** — the named form of the same Pool-of-one.

Every Pond resolves to exactly **one Pool identity** (the Catchment Pool, a named Pool, or its own
Pool-of-one). That identity is what freshness gating keys on.

## The model: two freshness stamps

A Pond keeps **`end_f`** (local publish complete — the Pond Run is *done*, its log entry closes) and gains
**`persisted_f`** (≤ `end_f`: the freshness through which its published output has been mirrored to the
data root). Precedent: `changed_f` is already a second per-Pond stamp with its own semantics
(plans/no-change-skip.md); this is the same shape.

**The gating rule** (the whole cross-Pool protocol, as one line): a Sink's view of `sourceF` is the
Source's `end_f` when Source and Sink share a Pool, and its `persisted_f` otherwise.

- Co-located chains flow at local speed — a bucket attached to a laptop Catchment costs those chains
  nothing.
- Cross-Pool Sinks wake only when the data is actually reachable at the data root.
- The Pond still knows nothing of its Sinks; "Duck-tagged demand" is unnecessary — the engine already
  knows every Pond's resolved compute target, so Pool identity is derivable Catchment-side.
- Flock falls out for free: DuckFlock executes against S3, so a Flock-routed run gates on its Sources'
  `persisted_f` like any cross-Pool consumer. No special casing.

## Persist mechanics

- **Local-first publish**: every run publishes to the local catchment-root layout on its Duck's Pool —
  identical to today's no-cloud path. `data_root` stops meaning "the publish target" and starts meaning
  "the durable/persist layer".
- **The Persist worker runs Duck-side** (the local data is on the Duck's Pool; only it can cheaply read
  it): an async mirror loop that reconciles the published output to the data root and reports a
  `persist_completed(f)` event to the Catchment, which records `persisted_f`. The machinery exists —
  the parts layout is idempotent-by-name, and `ObjectStoreEgressDriver.mirror` (append-mode Spouts)
  already implements "reconcile a published collection to an object store, O(new parts)". Persist is that
  pattern + the event.
- **Replayable**: a Persist that dies mid-mirror re-runs from the local published parts, idempotent by
  part name. `persisted_f` only advances on a completed mirror at that `f`.
- **A separate log item**: the Pond Run row closes at `end_f`; the Persist is its own row (its own
  timings/errors). Not a visualised graph element — Persists are infrastructure, not topology.
- **Always-eventually-persist is non-negotiable when cloud is enabled** — not only for recovery: serving,
  the data viewer (when the Catchment is a different Pool than the producer), draws, and Flock all read
  the persisted layer. Everything persists; Persist has no per-Pond configuration and no opt-out. (A
  serve-tables-only variant was considered and rejected: recovery wants everything anyway.)
- **Readers resolve by Pool identity**: same-Pool readers use the local root; cross-Pool readers use the
  data root at the persisted watermark. The viewer against a remote producer reads at `persisted_f` —
  slightly behind `end_f`, honestly so.

## The correctness core: Pool loss in the publish→persist window

The one genuinely hard problem. A run completes (`end_f` advances), co-located Sinks may already have
consumed the output locally — then the Pool dies with the delta un-persisted (`persisted_f < end_f`).

- A replacement Duck rehydrates from the data root, which only has state through `persisted_f`. The
  engine must **regress the Pond's effective freshness to `persisted_f`** and let the normal demand
  machinery re-run the gap. Freshness regression on loss is the new engine semantics this plan introduces;
  it must be designed against the four engine landmines (cold-start guards, push target sets) carefully.
- Co-located Sinks that consumed the lost window are coherent **because they share the fate**: the whole
  chain's unpersisted suffix lived on the same filesystem and dies together, regresses together, re-runs
  together. A cross-Pool Sink never consumed past `persisted_f` (the gating rule), so it never saw the
  lost data at all. This fate-sharing argument is the reason same-Pool `end_f` gating is *safe*, and it
  should be written into the engine tests.
- Liveness detection (the existing dead/silent-Duck machinery, per-Pool once Pools are real) is what
  triggers the regression.

## Disk pressure (the finite-disk strategy)

Registries are **reconstructible caches** (after plans/s3-resident-state.md step 2, a merge base is a view
— rehydration is O(bounded-hot)). The eviction policy is therefore simply: under disk pressure on a Pool,
**delete idle Ponds' registries, LRU**. Compaction naturally makes the evictable fraction large (cold data
is already at the data root), but eviction is a pressure response, not a hard "cold lives on S3" rule — a
Pool with disk to spare keeps everything hot. Per-Pool budget = the Pool's disk.

## What this deliberately does not do

- **No Duck→Duck direct transfer.** Cross-Pool movement goes through the persisted layer (the data root).
  The Duct analogy ends at "separate the run from the transfer"; literal peer-to-peer copy is complexity
  without a demonstrated win.
- **No change to Ducts or Spouts.** A Duct remains the cross-Catchment construct; a Spout remains egress
  to foreign systems. Persist is the intra-Catchment durability sibling — same family, distinct name,
  distinct (invisible) surface.
- **No Flock changes** beyond gating on `persisted_f`. Express-layer optimisations are later.

## Phases

1. **Plan** (this document) — settle the two-stamp model, Pool identity, Persist mechanics, and the
   loss-window rollback semantics.
2. **Local-first publish + Duck-side Persist + `persisted_f`** — gating still `end_f`-only for a
   Catchment-only topology (no remote Ducks). Ships the headline win: a laptop Catchment with a bucket
   attached runs its chains at local speed while everything still persists. Minimal engine risk.
3. **Pool-aware gating** — `persisted_f` for cross-Pool Sinks; serving/viewer/draw reads pinned to the
   persisted watermark; Pool identity resolution in the Driver.
4. **Pool loss rollback** — freshness regression to `persisted_f` on Pool loss, fate-sharing tests, the
   liveness → regression wiring.
5. **Named Pools as shared machines** — the task-per-Pool launcher (multiple co-resident Ducks), presets
   unchanged on task-per-Pond. Unblocks real co-location off the Catchment.
6. **Registry eviction under disk pressure** — LRU per Pool.

Phase 2 alone fixes "attached a bucket and everything got slow" end-to-end; each later phase is
independently shippable behind its own verification.
