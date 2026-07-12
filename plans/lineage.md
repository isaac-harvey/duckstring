# Data lineage: from the declared DAG to column provenance

Status: **proposed (2026-07-13), unbuilt.** Written after the 2026-07 review: most of what the industry
sells as "lineage" already exists here by construction, so this plan is mostly about *surfacing* recorded
facts, plus one genuinely new capability (column-level) scoped along a boundary the codebase already draws.

## Positioning — lineage is bookkeeping here, not archaeology

Elsewhere, lineage is a reconstruction problem: pipelines are imperative code with implicit dependencies
scattered across heterogeneous systems, so the lineage product is a separate *inference* layer — parsing
query logs, scraping orchestrator metadata, reverse-engineering SQL — inherently incomplete and drifting
from reality the moment someone edits a job. The hard part was never rendering the graph; it's that the
graph was never **recorded**.

Duckstring inverted that premise at the design level. Dependencies are declarations (`pond.toml
[sources]`) that gate deploy; every cross-Pond read is mediated (`Pond.read_table`/`read_delta`); every
Trickle row is stamped with the run that produced it (`_duckstring_f`); versions and run history are
first-class. So lineage decomposes into four levels that range from *already done* to *honestly scoped*:

| Level | State today | Work |
|---|---|---|
| **Dataset (Pond)** | **Built.** `pond_to_pond` (+ major pins), `/api/status` `edges`, the recursive cross-Catchment view (`/api/view`, plans/cross-catchment-visibility.md) | none — presentation only |
| **Table** | The choke point exists (`read_table`/`read_delta` see every cross-Pond table read) but nothing records it; the capture IR + the dbt manifest carry it statically | Phase 1 (observed) — days |
| **Column** | The structured subsets are mechanically derivable (the capture IR's expressions; the builder's own `_qualify`/join-key resolution; the dbt manifest) | Phases 2–3 — the one real project |
| **Row / temporal provenance** | **Built but invisible.** `_duckstring_f` + the epoch bracket `(previous_f, f]` + run history answer "which run produced this row, from which slice of each source" | a query surface + docs |

Two principles carry over from capture and alerts:

- **Exact or absent — never inferred.** Where the structure is known (declared sources, mediated reads,
  captured plans, dbt manifests), lineage is exact. Where it is not (arbitrary Python over the relation
  API), lineage is *absent and says so* — the capture fail-loud philosophy. A lineage edge that might be
  wrong is worse than no edge; guessed lineage is how the incumbents earned their reputation.
- **Observability, not orchestration.** Like an alert channel, lineage records observe state the engine
  already computes and add **no engine state** — `theory.md`'s state machine is untouched. A lineage
  write failure must never fail a run.

## Phase 1 — observed table-level lineage (the near-free win)

**What:** record, per Ripple Run, the set of tables read (own + `source.table`) and written, from the
Pond handle's existing mediation points.

- **Recording seam:** the `Pond` handle already brokers every read (`read_table`, `read_delta`,
  `read_object`) and every write (`write_table`, `append_table`, `merge_table`, `apply_zset`, the builder
  terminals, `write_object`). Add an in-memory `pond._lineage = {"reads": set(), "writes": set()}`
  appended at each call — a one-line touch per method, zero cost when unread. The Duck's executor drains
  it per ripple and ships it on the existing `ripple` event (like `started_at`/`changed` — no new
  transport).
- **Persistence:** a `ripple_run_lineage` table keyed like `ripple_run` (`pond_version_id, ripple, f,
  retry`) with rows `(direction ∈ read|write, source_name|NULL, table_name)`. Keyed on `pond_version`
  like all history (the conventions section) — lineage is a property of *a version's run*, which is what
  makes "when did this dependency appear/disappear" answerable across deploys.
- **Why observed, not just declared:** the declared graph says `sales` reads from `products`; the
  observed record says *which tables*, per run — and catches the honest drift case (a ripple stops
  reading a table; the declaration lingers). Observed ⊆ declared is also a useful lint: a read from an
  undeclared source already fails, but a *declared source never read* is surfaceable.
- **Surface:** `GET /api/lineage?pond=&major=&table=` → the table-level graph (latest run per ripple,
  or `f=` for as-of); nest into `/api/runs?lineage_tables=true` for per-run detail. CLI:
  `duckstring lineage {pond}[.{table}]` printing the upstream/downstream table tree. UI: the canvas
  already draws Pond edges — a table picked in the data viewer highlights the Ponds/Ripples on its
  path (read-level, so it rides the existing status poll + one lineage fetch).
- The runtime dbt executor reports the same shape from the manifest (`depends_on` + sources) — no
  observation needed; dbt-mode gets Phase 1 for free.

## Phase 2 — static column lineage for the structured subsets

**What:** `output column ← {(source, table, column), …}` for every ripple whose structure the framework
already understands. No SQL parsing dependency; this is a walk over structures we already build.

- **Trickle builder ripples:** the captured plan IR (`trickle/capture.py`) holds the operator DAG, the
  join pairs, and the ordered `filter/mutate/select` expressions with `alias.col` references — and the
  builder already owns the resolution machinery (`_qualify`, `_select_items`, the join-key union-find in
  `_star_output`, `_resolve_col`). A `lineage_of(plan) → {output: {col: [(ref, col), …]}}` walks:
  leaf columns are `(ref, col)`; a join merges namespaces (equi-join keys unify — the union-find);
  `mutate`/computed `select` items take the union of the source columns their expression references
  (extracted with the same `_COLPART` scanning `_qualify` uses); bare-`*` output maps 1:1. Where an
  expression references something unresolvable, that column's provenance is marked `opaque` — exact or
  absent, per the principle.
- **dbt-mode ponds:** model-level lineage from the manifest (Phase 1); column-level joins Phase 3 (dbt
  compiles to SQL — same parser path).
- **Storage:** static column lineage is a property of the *deployed version*, not a run — captured at
  deploy (or first run, beside `_capture_schema`) into a `pond_version_column_lineage` table, invalidated
  by redeploy exactly like the schema contract rows.
- This phase also pays a second dividend: column lineage × the schema contract = **impact analysis**.
  "Which downstream columns die if I drop `orders.discount`" becomes a query — the missing half of the
  pinned-minor question (CLAUDE.md's Version-contract note): column lineage is the *observed* use
  declaration that a future column-level pin could formalise.

## Phase 3 — SQL column lineage, best-effort and opt-in

**What:** column lineage through `.sql()` escape hatches and dbt-compiled SQL, via a real SQL parser.

- The builder already holds the `.sql()` query string (the capture IR's `kind: sql` statements); dbt
  holds each model's compiled SQL. Parse with **sqlglot** (the industry hammer; DuckDB dialect) behind an
  optional extra — `duckstring[lineage]` — never a core dependency, mirroring the dbt/ibis lazy-import
  discipline.
- Coverage is honest: what sqlglot resolves is recorded; what it can't (dynamic SQL, `SELECT *` through
  opaque UDFs) is `opaque`. Raw `pond.con.execute(...)` ripples are **out of scope** — the handle hands
  out a bare DuckDB cursor and intercepting it would be guessing; those ripples show table-level lineage
  (Phase 1) with column-level marked absent. Scope arbitrary Python out *and say so* — the same boundary
  capture draws, for the same reason.

## Phase 4 — temporal provenance surface + OpenLineage emission

- **Row provenance is already recorded** — make it answerable. A docs guide (the recipe:
  `_duckstring_f` on a row → the `pond_run` at that `f` → its version + its sources' windows
  `(previous_f, f]` → recurse) plus a convenience: `duckstring lineage trace {pond}.{table} --where …`
  resolving a row's producing run, version, and per-source input windows from data already persisted.
  This is the level the graph-lineage products don't have; it deserves a headline in the docs, not just
  an API.
- **OpenLineage emission** — the integration play, not a catalog competitor. On `run_completed`, emit a
  standard OpenLineage RunEvent (job = `pond@major`, run = `f`, inputs/outputs = the Phase-1 table
  records with the Extension-2 sidecar `schema` as facets, column lineage as a facet where Phases 2–3
  recorded it). Delivery reuses the **alerts shape wholesale**: operational config (an emitter URI, an
  `${env:}`/`${secret:}` API key), an outbox + the existing worker discipline (a catalog outage never
  touches a run). One emitter slots Duckstring into whatever catalog a team already runs — the same
  philosophy as egress ("get my data where my consumers already are") applied to metadata.

## What lineage deliberately is NOT

A data catalog, a search/discovery product, a glossary, or an org-wide impact dashboard spanning
non-Duckstring systems. Those are catalog products' jobs, and OpenLineage is the honest integration
point — Duckstring **records exact lineage and emits it**; it doesn't try to be the place the whole
company browses metadata. (Same brand rule as alerts: name the gap, integrate with what teams run.)

## Sequencing & effort

Phase 1 (observed table level: handle recording + event + table + API/CLI + UI highlight) — small; the
choke points exist. Phase 2 (builder column lineage from the capture IR + deploy-time storage +
impact-analysis query) — the best value-per-effort in the plan; no new dependencies. Phase 4a (temporal
provenance docs + `lineage trace`) — small, high differentiation. Phase 3 (sqlglot) and 4b (OpenLineage)
— independent, each behind its own opt-in, in whichever order demand shows up.

## Open questions

- **Column-level *reads* for plain ripples:** `read_table` registers a view and the ripple's SQL runs
  against it unseen, so even Phase 3 sees only what the builder/`.sql()` holds. Fine (exact-or-absent),
  but note it caps "observed" column lineage at table grain for classic ripples.
- **Cross-Catchment column lineage:** Phase 1/2 records are per-Catchment; the recursive `/api/view`
  fan-out could thread column lineage the same way it threads freshness — deferred until single-Catchment
  lineage has users.
- **Retention:** per-run lineage rows grow with run history; they should ride whatever history-retention
  policy `pond_run` eventually gets (none exists today — a shared question, not a lineage one).
