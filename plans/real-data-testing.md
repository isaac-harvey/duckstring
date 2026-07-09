# Real-data demos: TPC-DS and GHArchive Trickle pipelines

Status: **implemented** (both sets shipped as `duckstring pond demo --tpcds` / `--gharchive`, with
deployed-Duck e2es in `tests/test_runtime.py`; the topology below is the built shape). Duckstring's Trickle
chain had only ever run on the synthetic
`orders`/`catalog` demo. That set proves the mechanics but not the pitch: a *big standing table, a tiny
per-run delta*, on data an evaluator already recognises. Two datasets close that gap — **TPC-DS** (the
canonical analytics star schema, generated) and **GHArchive** (a real public event stream, fetched). Both
are hundreds of MB to GB at rest with small deltas, both fan out into joins and aggregations, and both let
us demo the thing the synthetic set can't: **two independent Outlet paths off one Inlet, run at different
cadences.** They ship as new demo sets, `duckstring pond demo --tpcds` and `--gharchive`, alongside
`--ripple`/`--trickle`.

## Why these two, and why Trickle

The existing demo (`src/duckstring/demo/orders/…`) is the template: an Inlet whose `ingest` ripple lays
down a large history on the first run (`_BOOTSTRAP`) then appends a small `_BATCH` every run after, sizes
env-overridable via `DUCKSTRING_DEMO_*` so the test suite shrinks them. Both new sets follow that shape —
the delta contrast *is* the demo, so the standing tables are deliberately large and the per-run change is
small. Every transform Pond is a Trickle (append Inlets, merge dimensions, `pond.trickle(...)` builder
joins, comprehensive aggregate Outlets), mirroring `orders → catalog → priced → revenue`.

The distinguishing feature over the synthetic set is **topology**: each demo has ≥4 Ponds, a cross-Pond
join to a maintained dimension, and **two separated Outlet paths** sharing one Inlet. That structure is
what makes "run one path at a different frequency" demonstrable — a Tide/Wave with one staleness bound on
Outlet A, another on Outlet B (suggested trigger commands live in each Pond's `README.md`).

## GHArchive — real streaming deltas

GHArchive publishes one gzipped-JSON file of all public GitHub events per hour at
`https://data.gharchive.org/YYYY-MM-DD-H.json.gz` — **public, no auth**, ~100–200 MB/hour. An hour *is* a
delta, so this is genuine streamed real data, not emulation.

- **`gh_events`** (Inlet, append Trickle) — reads the next unconsumed hour via DuckDB `httpfs` +
  `read_json` and appends flattened events (`event_id`, `type`, `actor_login`, `repo_name`, `created_at`,
  plus a few payload fields). The bootstrap loads a small backfill window (a handful of hours); each run
  advances one hour. `event_id` is unique by construction → the trust-the-writer append fast path
  (`fail_on_conflict=False`), exactly like `orders`.
- **`gh_actors`** (merge Trickle) — maintains the actor dimension (`actor_login → actor_id`, display
  fields), diffed per run so only newly-seen or changed actors hit the changelog.
- **Path A → `gh_pushes`** (`pond.trickle` builder: `gh_events` PushEvents ⋈ `gh_actors`) →
  **`gh_repo_activity`** (Outlet, aggregate: commits & pushes per repo).
- **Path B → `gh_stars`** (Trickle: filter WatchEvent/ForkEvent to an **append** table — the signal is
  insert-only) → **`gh_trending`** (Outlet, aggregate: stars vs forks per repo). Path B is **entirely
  disjoint** from Path A, sharing only the `gh_events` Inlet.

Six Ponds, two Outlets, two disjoint paths sharing only `gh_events` (the `gh_actors` join is Path A only).

**Runtime-network caveat (the one real risk).** Fetching inside a ripple means the demo needs network at
run time and isn't deterministic. Mitigations, all in-pattern: the ingest source is env-pointed
(`DUCKSTRING_GHARCHIVE_BASE` — a bucket URL by default, or a local directory of `{hour}.json.gz` files),
so the test-suite e2e reads a **small committed 2-hour fixture** from a local path instead of the network
(`tests/fixtures/gharchive/`, the same way `DUCKSTRING_DEMO_*` shrinks the trickle e2e). An hour that
isn't published yet (a live-stream gap, or offline) makes the Inlet **hold** on that hour and pass — not
fail. If in practice this proves too flaky to ship as a demo, it falls back to `.sandbox/gharchive/` per
the original brief — but the no-auth public bucket makes shipping it the expected outcome.

## TPC-DS — emulated streaming, fully offline

DuckDB's `tpcds` extension generates the standard star schema in-process (`INSTALL tpcds; LOAD tpcds; CALL
dsdgen(sf=…)`), no auth and deterministic. It has no native stream, so deltas are **emulated**: bootstrap
generates the dimensions + a base fact at a scale factor; each run synthesises a small batch of new
`store_sales` rows (new sale keys, random existing item/store/customer FKs, a fresh date), and perturbs a
few item prices — the same emulation `orders`+`catalog` already do, on the real TPC-DS schema.

- **`tpcds_sales`** (Inlet, append Trickle) — bootstrap = `dsdgen` `store_sales` at `sf`; each run appends
  a `_BATCH` of synthetic sales (FKs sampled from the ranges already in the history so joins resolve).
  Env-overridable scale (`DUCKSTRING_TPCDS_SF`, `_BATCH`).
- **`tpcds_items`** (merge Trickle) — the `item` dimension with a few prices drifting per run (CDC).
- **`tpcds_stores`** (merge Trickle) — the `store` dimension; stable, so after the first run its delta is
  empty (a free stable join operand — the honest other half of the CDC story).
- **`tpcds_priced`** (`pond.trickle`: sales ⋈ item ⋈ store) — the shared 3-way builder join.
- **Outlet A → `tpcds_category_revenue`** (aggregate: revenue per category).
- **Outlet B → `tpcds_store_revenue`** (aggregate: revenue per store).

Six Ponds. Both Outlets consume the shared `tpcds_priced` builder and run on independent cadences — the
"one path at a different frequency" demo. (This is the built shape; it swaps the earlier
customer/segments Path B for a second Outlet off the shared builder — simpler, avoids the large `customer`
dimension at scale, and keeps both Outlets genuinely independent.)

## What's genuinely new engineering

- **Two demo Pond sets** under `src/duckstring/demo/` (one dir per Pond: `pond.toml` + `src/pond.py` +
  `README.md` + Trickle-shaped `puddles.py` where useful), authored to the existing conventions (SQL-side
  generation — no million-row Python `VALUES`; no replacement scans; `pond.read_table`/`pond.trickle`
  reads; `pond.f`-stamped appends).
- **CLI**: two more mutually-exclusive flags on `duckstring pond demo` (`cli/pond.py`) with
  `_TPCDS_DEMO`/`_GHARCHIVE_DEMO` set tuples beside `_RIPPLE_DEMO`/`_TRICKLE_DEMO`; the copy/confirm/echo
  loop is already generic, so this is additive.
- **Test coverage**: a deployed-Duck e2e per set mirroring `test_trickle_chain_runs_end_to_end`, driven at
  shrunk sizes via env overrides — GHArchive against the committed fixture hour, TPC-DS at `sf` small
  enough to stay under the suite's timeout.
- **The `tpcds` and `httpfs`/`read_json` DuckDB extensions** at demo runtime (the trickle demo already
  leans on core DuckDB; these are `INSTALL`-on-first-use, so no new *Python* dependency — but the TPC-DS
  path needs the extension download once, and GHArchive needs httpfs + network).

## Decisions made

- **Ship both as first-class `--tpcds` / `--gharchive` demos**, not `.sandbox/`. Neither needs auth; TPC-DS
  is deterministic and offline via the extension, GHArchive is a public no-auth bucket. `.sandbox/gharchive`
  stays the fallback *only* if the runtime-network dependency proves too flaky to ship.
- **TPC-DS streaming is emulated, GHArchive is real** — this is the honest split (TPC-DS has no stream;
  GHArchive is one). Stated plainly in each README so nobody mistakes the generated fact for a live feed.
- **Two Outlets + two disjoint paths per demo**, sharing one Inlet and one joined dimension — the minimum
  structure that makes the different-cadence story real. Trigger commands go in the READMEs, not baked in
  (cadence is operational config, like the existing demo's `trigger pulse`).
- **Sizes are env-overridable** (`DUCKSTRING_TPCDS_*` / `DUCKSTRING_GHARCHIVE_*`) so the e2e tests shrink
  them, exactly as `DUCKSTRING_DEMO_*` already does.

## Resolved during the build

- **GHArchive fixture** → two real trimmed hours (~1,530 events each, ~700 KB gzipped) in
  `tests/fixtures/gharchive/`, so the parse path runs against real GHArchive JSON shape. Two hours (not
  one) so the cursor can walk a genuine second hour.
- **TPC-DS default `sf`** → `0.1` for a scaffolded run (not `1.0` — each of the three generating Ponds runs
  its own `dsdgen`, so `1.0` × 3 is too slow for a first-touch demo; `0.1` still bootstraps a real fact in
  a few seconds). The e2e shrinks to `0.01`.
- **Shared helper vs self-contained** → fully self-contained, each Pond dir standalone (matching every
  existing demo — no cross-Pond import).
- **GHArchive gap handling** → **hold-on-gap** (retry the same hour next run), not skip-forward — correct
  live-stream behaviour (the next hour simply hasn't landed) and it keeps repeated runs safe no-ops.

## Follow-ups (not done)

- Point the GHArchive demo's default `START` at a rolling-recent window rather than a fixed historical hour
  (a fixed hour is reproducible but goes stale as "the latest" for a live demo).
- A larger-scale performance write-up (the brief's original "real performance on recognisable datasets"
  motivation) — the demos exist and run; a benchmark/blog measuring the delta-vs-standing-table contrast at
  `sf=10`+ / a full GHArchive day is the natural next step.
