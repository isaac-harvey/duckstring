# Real-data demos: TPC-DS and GHArchive Trickle pipelines

Status: **not built** (design only). Duckstring's Trickle chain has only ever run on the synthetic
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
- **Path B → `gh_stars`** (Trickle: filter WatchEvent/ForkEvent) → **`gh_trending`** (Outlet, aggregate:
  stars per repo, `.accumulate` for a running total over time).

Six Ponds, two Outlets, two disjoint paths sharing `gh_events` and the join to `gh_actors`.

**Runtime-network caveat (the one real risk).** Fetching inside a ripple means the demo needs network at
run time and isn't deterministic. Mitigations, all in-pattern: the ingest window is env-pointed
(`DUCKSTRING_GHARCHIVE_URL`/date), so the test-suite e2e reads a **small committed fixture hour** from a
local path instead of the network (the same way `DUCKSTRING_DEMO_*` shrinks the trickle e2e); an offline
run skips with a warning rather than failing. If in practice this proves too flaky to ship as a demo, it
falls back to `.sandbox/gharchive/` per the original brief — but the no-auth public bucket makes shipping
it the expected outcome.

## TPC-DS — emulated streaming, fully offline

DuckDB's `tpcds` extension generates the standard star schema in-process (`INSTALL tpcds; LOAD tpcds; CALL
dsdgen(sf=…)`), no auth and deterministic. It has no native stream, so deltas are **emulated**: bootstrap
generates the dimensions + a base fact at a scale factor; each run synthesises a small batch of new
`store_sales` rows (new sale keys, random existing item/store/customer FKs, a fresh date), and perturbs a
few item prices — the same emulation `orders`+`catalog` already do, on the real TPC-DS schema.

- **`tpcds_sales`** (Inlet, append Trickle) — bootstrap = `dsdgen` `store_sales` at `sf`; each run appends
  a `_BATCH` of synthetic sales. Env-overridable scale (`DUCKSTRING_TPCDS_SF`, `_BATCH`).
- **`tpcds_items`** (merge Trickle) — the `item` dimension with a few prices drifting per run (CDC).
- **Path A → `tpcds_priced`** (`pond.trickle`: sales ⋈ item ⋈ store) → **`tpcds_store_revenue`** (Outlet,
  aggregate: revenue per store × category).
- **Path B → `tpcds_customer_sales`** (Trickle: sales ⋈ customer) → **`tpcds_segments`** (Outlet,
  aggregate: spend & order count per customer segment).

Five Ponds, two Outlets, two disjoint paths sharing `tpcds_sales` and a join to `tpcds_items`.

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

## Open questions

- GHArchive fixture: commit one trimmed hour (a few thousand events) as the test fixture, or synthesise a
  GHArchive-shaped fixture? Leaning: a real trimmed hour, so the parse path is exercised on real JSON shape.
- TPC-DS default `sf` for a `demo`-scaffolded (non-test) run — big enough to feel real, small enough to
  bootstrap in seconds. Leaning `sf=1` (~1 GB raw, but only `store_sales` + used dims materialise).
- Whether the two demos share any helper (e.g. a fetch/generate utility) or stay fully self-contained per
  the demo convention (each Pond dir standalone). Leaning: self-contained, matching every existing demo.

**Confirm before building** — this is the plan for the brief in the original stub; check the topologies and
the ship-both-as-demos call before I scaffold anything.
