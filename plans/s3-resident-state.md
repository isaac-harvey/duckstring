# Plan: S3-resident state — hydrate nothing, read what you touch

> **Status: step 2 (base-as-view) IMPLEMENTED and verified; steps 1/3/4 designed, not built.** The target is
> a Duck that holds no persistent state on its own disk: all durable tiers (merge base, changelog, warm bands,
> agg/acc accumulators) live on the data plane (S3), and a run reads only the slice its delta touches. The
> registry becomes per-run scratch. This supersedes the narrower "base as a reference" idea — the same
> principle applies to every tier, so the plan covers all of them, sequenced by payoff.
>
> **Done (the headline win — the TB-base concern that motivated this):** a merge main's **cold base** is now a
> **view** over the published S3 chunks, never a hydrated copy. `hydrate_registry` registers the base as a
> view; a checkpoint materialises it locally, publishes chunks, then `_review_base` re-points it at S3;
> `_table_exists` recognises views; `checkpoint` drops a base view before rebuilding the base table. Verified
> against a real ap-southeast-2 bucket: a checkpointed base hydrates as `view=1/table=0`, reconstruction
> correct. Tests: `test_base_hydrates_as_a_view_not_a_copy`, and a pre-existing Iceberg crash fixed on the way
> (`_is_incremental` now excludes warm bands — `test_merge_export_survives_warm_fold_then_checkpoint`). This
> alone solves the stated scenario — a fresh Duck facing a TB base + a small delta no longer downloads the
> base (a steady-state run never reads it).
>
> **Deferred, with reasons found during implementation (see "Implementation note" below):** the warm tier,
> the changelog, and the accumulators all use **append/accumulate** write patterns (an in-place `INSERT`
> every fold/run), unlike the base's single **replace** at checkpoint. A view can't be appended to, and the
> compute engine (`trickle/io.py`) is deliberately data-plane-agnostic (it can't read/write S3), so making
> those tiers S3-resident is a genuine restructure of their write path + the retention coupling — not the
> clean table→view swap the base allowed. They are real follow-ons, not quick wins.

## The problem

A newly-spawned (ephemeral, cloud) Duck rebuilds its registry from the published state before its first run
(`RippleExecutor`, [executor.py:148](../src/duckstring/duck/executor.py#L148) → `hydrate_registry`,
[executor.py:167](../src/duckstring/duck/executor.py#L167)). `hydrate_registry` is **full-collection**: for a
merge main it does `CREATE OR REPLACE TABLE {table} AS <read the cold base chunks>`
([dataplane.py:601](../src/duckstring/dataplane.py#L601)) — it materialises the **entire cold base** into the
local registry, plus the whole changelog, warm bands, and the whole agg/acc accumulator snapshots.

The waste is stark at scale: a merge main with a **TB cold base** fed a **100k-row Source delta** copies the
whole TB down first — even though the steady-state incremental run **never reads the base**. `apply_zset` is
explicit: *"a run only ever appends its delta here (O(change)); the base is never touched per run"*
([io.py](../src/duckstring/trickle/io.py), `apply_zset`). The base is read by exactly two operations, both
rare — a **checkpoint** (cold compaction, which rewrites the base anyway) and a **comprehensive fallback** (a
full recompute after a bootstrap / coverage-miss / over-`p` change). Neither is a normal run.

The governing principle (the author's, and the right one): **copy is the thing to avoid, not computation.**
The copy is bandwidth-bound — it is the wall we hit both cross-region and, less severely, in-region — while
the compute is local and cheap, and the bytes a delta *touches* are a rounding error against the bytes
*stored*. Reading only what you touch, even at the cost of more small S3 requests and some recompute, wins
decisively. Today we do the opposite: bulk-copy everything, then ignore almost all of it.

## Why it copies everything today: the retention coupling

The registry is currently treated as the **source of truth for what should exist on S3**. `export` mirrors the
registry back to the data plane and **prunes any published part the registry no longer holds** — that is how
retention works. `_export_parts` writes the registry's freshnesses and then
`for fi, name in existing: if fi not in reg_fs: remove` ([dataplane.py:395](../src/duckstring/dataplane.py#L395)).

So a part left un-hydrated would be pruned from S3 on the very next export — **data loss**. That single
coupling is what forces "hydrate everything": the registry must hold the full history so the next export
doesn't delete it. Break the coupling and the hydration mandate disappears with it.

## Target model: the registry is scratch, the data plane is truth

Invert the ownership. The **data plane owns the durable state**; the registry is a per-run scratchpad that
holds only what the current run computes. Concretely, a steady-state incremental run becomes:

1. read the Source deltas from S3 (already how cross-Pond reads work — `read_delta`);
2. read the *touched slice* of its own prior state from S3, predicate-pushed to the affected keys;
3. compute the delta in local temp tables;
4. append the new parts to S3.

Nothing is hydrated up front; nothing persistent is written to local disk. A run touches O(delta) bytes, not
O(history). An ephemeral Duck spawns, does O(delta) work, and dies — with **nothing to recover**, because
there was nothing to lose. In the limit the registry can be `:memory:`.

The enabling mechanical fact (verified): the reconstruction SQL references the registry **table names**
(`_reconstruct_sql_for`, [io.py:742-744](../src/duckstring/trickle/io.py#L742-L744)) — the base as
`SELECT * FROM {name}`, the changelog/warm as `_clog_union_sql`. If `{name}` is an **S3-backed view**
(`read_parquet('s3://…/{name}__base/*.parquet')`) instead of a materialised table, every consumer —
`reconstruct_current`, `checkpoint`, the comprehensive fallback — reads from S3 transparently, with DuckDB's
httpfs doing predicate/projection pushdown to the relevant row groups. No reconstruct logic changes.

## What each tier actually needs — and when

| Tier | Read by steady-state incremental run? | Read by whom, when |
|------|----------------------------------------|--------------------|
| **cold base** (`{t}__base/`) | **No** | checkpoint (rewrites it); comprehensive fallback (full recompute) |
| **changelog** (`{t}__changelog/`) | **No** — only appended to | reconstruct (checkpoint / comprehensive); a downstream consumer's window read (from S3, not the producer's registry) |
| **warm bands** (`{t}__band/`) | **No** — only appended to | same as changelog |
| **agg/acc accumulators** (`_duckstring_agg_{t}` / `_acc_`) | **Yes — but only the affected groups** | every incremental aggregate/accumulate run |

So three of the four tiers are **never read** by a normal run; they are copied down purely to satisfy the
retention coupling. The accumulators are the one genuine read-dependency — and even they are needed only for
the groups the delta touches (`apply_aggregate` folds per affected group, O(δ)), not wholesale.

## The changes, sequenced by payoff

### 1. Decouple retention from the registry (the enabling change)

Make published parts **immutable, data-plane-owned artifacts** whose lifetime is governed by an explicit
**retention policy applied against S3**, never by "the registry doesn't have it." `export` stops pruning parts
it doesn't see in the registry; a separate retention pass deletes parts by freshness/count directly on S3
(the `retain_t` / `retain_n` semantics already exist — `_apply_retention` — they just move from acting on
registry rows to acting on published part names, which carry their `f` in the filename, so no Parquet open is
needed). This is the keystone: once retention no longer keys off registry contents, nothing *requires* full
hydration.

### 2. Base as a reference (biggest single win — TB-scale)

- **Hydration** registers the base as an S3-backed view, not a table (`hydrate_registry`, the merge branch at
  [dataplane.py:596-602](../src/duckstring/dataplane.py#L596-L602)): `CREATE VIEW {name} AS
  read_parquet('{base}/*.parquet')` instead of `CREATE TABLE … AS`.
- **Export** no longer re-mirrors the base (it is already on S3, immutable between checkpoints). The base's
  sole writer becomes `checkpoint`.
- **Checkpoint** reads the old base from S3 through the view (it is rewriting it regardless — the inherent,
  amortised O(base) op), folds base ⊎ warm ⊎ hot, and writes the new chunks to S3. Because the Duck may be
  in-region, this read is fast; because k=1 keeps checkpoints rare, it is amortised.
- **Comprehensive fallback** reads the base through the view, predicate-pushed where the recompute allows.

Steady-state runs are untouched — they already never reference the base.

### 3. Changelog + warm: stop hydrating, append-only on S3

With retention decoupled (step 1), the changelog and warm bands never need to be local. A run appends a new
part to S3 directly (the compute produces the delta in scratch; publish writes the part). Reconstruction reads
them from S3 through views, same as the base. The producer never reads its own history to append to it.

### 4. Log-structured accumulators (the deep change)

The agg/acc accumulators are the one tier a normal run reads. Today they are a **registry-only snapshot**
(`_duckstring_agg_{name}`), published as a whole-companion snapshot per run
(`_export_companions` → `state/{agg|acc}/{table}/{f}.parquet`, prior snapshot pruned). To read-on-demand and
write-partially:

- **Read**: fetch only the affected groups from the S3 snapshot, predicate-pushed —
  `SELECT * FROM read_parquet('{snap}') WHERE {by} IN (K)` — where `K` is the delta's group set. DuckDB prunes
  to the relevant row groups (sort the snapshot by `by` at write time so groups cluster into few row groups).
- **Write**: emit only the changed groups. A whole-companion rewrite is O(all groups) — fatal for a
  billion-group aggregate. Give the accumulators the **same log-structured treatment as the merge main**:
  an append-only accumulator changelog (the changed groups' new state per run) + an occasional checkpoint that
  folds it into a fresh snapshot. Reconstruction of a group's accumulator = latest-per-`by` over snapshot ⊎
  changelog, exactly the merge-main pattern already implemented — reuse `reconstruct_sql` machinery keyed on
  `by`.

This is the meatiest sub-change and should land last; steps 1–3 already make a fresh Duck O(delta) for the
common (non-aggregate, or modest-cardinality) cases.

## The stateless-Duck endpoint

With all four in place, a steady-state incremental Duck holds nothing durable locally. `hydrate_registry` for
the incremental path becomes: register S3-backed views for base/changelog/warm, and (for aggregates) leave the
accumulator read to the per-run predicate-pushed fetch. The `recover = not registry_path.exists()` branch
([executor.py:148](../src/duckstring/duck/executor.py#L148)) collapses to "wire up the views" — there is no
bulk rebuild. Registry-loss recovery as a distinct, expensive path largely dissolves: a lost registry is
indistinguishable from a fresh spawn, and both are cheap. The registry can be `:memory:` for a pure
incremental run; a disk registry remains only as scratch/spill for large local joins.

## What stays O(history) — honestly

Two operations still read at scale, and should:

- **Checkpoint / cold compaction** — rewrites the base by definition; O(base) read + write, but rare (k=1
  amortisation) and in-region when the Duck is co-located.
- **Comprehensive fallback** — a bootstrap / coverage-miss / over-`p` change forces a full recompute, which
  reads the base + full changelog. This is the correctness backstop; it reads (predicate-pushed where it can)
  rather than bulk-copy-then-ignore, and it is the exception, not the per-run path.

Both read *because they must*, not to satisfy a coupling. That is the distinction the current design blurs.

The other cost being traded: more, smaller S3 requests (predicate-pushed reads issue range GETs; ~tens of ms
each in-region) in place of one bulk copy. This is a clear win whenever bytes-touched ≪ bytes-stored — i.e.
exactly the scenario that motivates the change — but it is worth measuring for small tables, where the
overhead of view setup + per-op latency could exceed a trivially-cheap copy. A size threshold (copy small
tables, reference large ones) is a reasonable safety valve if the crossover proves to matter.

## Migration & compatibility

- The published **layout is unchanged** — base chunks, changelog/warm parts, and the state snapshots keep
  their names and formats. Only *who reads/writes them and when* changes, so existing buckets are readable
  as-is and no data migration is required.
- The accumulator log-structuring (step 4) changes the `state/{agg|acc}/` layout from snapshot to
  snapshot+changelog. Since these are recomputable working state (not user data), a version bump can simply
  re-snapshot on first write under the new scheme; no back-migration of old snapshots.
- Cross-Pond draws / the poller are unaffected — they already read the flat parts from S3.

## Testing

- A **no-hydration** assertion: spawn a Duck against a published merge main with a large base + a small
  delta; assert the run reads O(delta) bytes and never materialises the base (instrument the data-plane read
  path / count bytes fetched). The counterpart of the existing hydration tests.
- **Retention on S3**: `retain_t` / `retain_n` trim published parts directly, with the registry never holding
  full history — guard against the old prune-what-the-registry-lacks path resurfacing.
- **Checkpoint from S3-backed base**: a checkpoint reads the base through the view, folds, and republishes;
  reconstruction after is correct.
- **Comprehensive fallback from S3**: force a coverage-miss on a fresh Duck; the fallback reconstructs
  correctly reading base+changelog from S3.
- **Log-structured accumulators**: an incremental aggregate over a large group space reads/writes only
  affected groups; reconstruction of a group equals the wholesale rebuild.

## Implementation note (what building step 2 taught us)

The base swapped table→view cleanly because a merge base is **only ever replaced** — `checkpoint` rewrites it
wholesale, nothing appends to it, and a steady-state run never reads it. So "hold it as a view, materialise it
only inside the rare checkpoint" was a contained change: `hydrate_registry` makes the view, `checkpoint` drops
the view and builds a fresh base table, `_review_base` re-views it after publishing. No retention change was
needed, because the base is published by `_publish_base_chunks` (token-swap), never by the registry-mirroring
`_export_parts` prune — so the retention coupling never touched it.

The **other three tiers are harder for a concrete reason**: they are written by **append/accumulate**, not
replace.

- **Warm** — `fold_warm` does `INSERT INTO {warm}` (bands accumulate); `_export_bands` publishes each new band
  incrementally. A view can't be `INSERT`ed into, so warm-as-view needs `fold_warm` to stage the new band
  elsewhere and the data plane to publish it, with the view over S3 bands covering it — a restructure of the
  fold path, in the dependency-free `trickle/io.py`.
- **Changelog** — `apply_zset` appends **every run**, and mid-run **chained** reads (`read_registry_delta`)
  read those just-appended rows back from the registry. Moving the changelog to S3-only would have the compute
  engine write S3 (it can't — data-plane-agnostic), or split it into an S3 view + a registry "pending" table
  with lifecycle/dedup/clear-after-publish — and this is the tier that **forces the retention decoupling**
  (clearing the pending table after publish is only safe once `_export_parts` stops pruning parts the registry
  no longer holds).
- **Accumulators** — snapshot-per-run today; read-affected-groups + write-changed-groups needs them
  log-structured like the merge main (a deep change to `apply_aggregate`).

So the original sequencing (retention first) is right *for the changelog*, but the base did not need it and was
the far larger win — hence it shipped first. The remaining order stands:

1. **Retention decoupling** — the keystone for the changelog; self-contained but changes prune semantics
   (correctness-sensitive: a wrong prune is data loss), so it wants its own careful pass + tests.
2. **Warm as a reference** — extends the base pattern once `fold_warm` stages-not-accumulates.
3. **Changelog no-hydration** — the involved one (retention + S3-view/pending split + mid-run reads).
4. **Log-structured accumulators** — the deep change; only large-cardinality aggregates need it.

**Practical read:** step 2 removes the unbounded cost (the cold base is the only tier that grows without
bound between the rare checkpoints). What remains hydrated after step 2 — the hot changelog (bounded by the
compact threshold before it folds) and the accumulators — is bounded, so a fresh Duck is already
O(bounded-hot), not O(history), for the motivating case. Steps 1–4 drive that last bounded cost toward zero
and are worth doing, but each is a deliberate, independently-tested change to the core incremental engine, not
a mechanical follow-through — they should land one at a time behind their own verification, not in a rush.
