# dbt-mode Ponds: deploying a dbt project as a Pond with zero user code

Status: **not built** (design only). dbt is the established way teams already define a sequence of SQL
transformations; a huge number of the mesh-pattern users Duckstring targets already have a dbt project they
don't want to rewrite. A **dbt-mode Pond** lets that project deploy as-is: `pond.toml` points at the dbt
project directory, and Duckstring interprets its model graph into Ripples entirely at deploy time — no
`@ripple`-decorated Python is ever written for that Pond.

## Positioning

Duckstring's pitch is "the pipeline is implicit in the package graph," and a dbt project already has an
implicit pipeline of its own — `ref()` gives you the model DAG for free, the same way Duckstring's ripple
edges give you the intra-Pond DAG for free. The two graphs are structurally the same shape. That similarity
is the whole opportunity: a dbt project doesn't need a Duckstring-flavored rewrite, it needs a **translator**
that reads a graph dbt already computed and re-expresses it as Ripples for freshness/failure/retry tracking.

This is deliberately **not** "convert dbt models to hand-written Ripples." That would ask a user to
transcribe a project they already have working. The no-thinking version is a Pond *kind*: point at a dbt
project, deploy, done — dbt keeps owning its own SQL and materialization logic; Duckstring only wraps it in
per-model freshness/fault-tolerance nodes and cross-Pond source pinning.

## Mechanics

**`pond.toml`**: a dbt-mode Pond declares `[pond] dbt_project = "dbt/"` instead of `ripples = "src/pond.py"`.
The two entrypoint kinds are mutually exclusive per Pond in v1 — no mixing hand-written Ripples with dbt
models in the same Pond (a hand-written Pond can still sit upstream or downstream of a dbt-mode one).

**At deploy** (`cli/deploy.py`, a new branch keyed on `dbt_project` being set):
1. Run `dbt parse` (or `dbt compile`) against the project, targeting the Pond's own registry file
   (`ponds/{name}/m{major}/registry.duckdb`) via a generated `profiles.yml` using the `dbt-duckdb` adapter.
2. Read the resulting `manifest.json`. For every node with `resource_type: model`, create a `ripple` row
   named after the model; for every entry in its `depends_on.nodes` that is also a model in this project,
   create the matching `ripple_to_ripple` edge. This is the exact same DB shape `@ripple` decorator discovery
   already produces — no schema changes.
3. A dbt `source()` that Duckstring recognizes as another Pond's published output (matched by source name /
   config, TBD exact convention) becomes a `pond_to_pond` edge instead of an intra-Pond one — the model reads
   it the way any Ripple reads a Source, via the existing `pond.read_table` cross-Pond path. A `source()` that
   isn't a known Pond is left to dbt's normal external-source resolution (an unmanaged warehouse table).
4. Store the compiled project (or enough to recompile per-model on demand) so runtime execution doesn't
   re-parse the whole project on every Ripple.

**At runtime**, a dbt-mode Ripple's executor branch (`duck/executor.py`) doesn't call a Python function — it
invokes dbt itself, scoped to one model: `dbtRunner().invoke(["run", "--select", model_name])` (the
programmatic API, not a subprocess shell-out, to avoid per-Ripple CLI startup cost) against the Pond's
registry file. dbt's own materialization logic (view/table/incremental) runs unmodified — the DuckDB file
already holds whatever prior state an incremental model's `is_incremental()` macro expects, because it's the
same persistent registry across runs. Duckstring supplies no incremental machinery here; dbt keeps doing what
it already does.

## Why not Trickle

dbt-mode Ripples are **plain overwrite nodes only** — never Trickle-native. dbt's incremental materialization
already solves the same problem Trickle solves, with its own semantics (merge/insert strategies, its own
`is_incremental()` branching), and reconciling the two would mean either reimplementing dbt's incremental
logic in terms of Z-sets (throwing away compatibility, the whole point of this feature) or bolting Trickle
onto something that already manages its own state. Accept the loss: a dbt-sourced Pond doesn't get Z-set
composition, but it also asks nothing of the user beyond a working dbt project.

## What's genuinely new engineering

- The deploy-time manifest → `ripple`/`ripple_to_ripple` translator (new code, but writes rows through the
  same path deploy already uses for Python-discovered Ripples).
- The dbt `source()` → `pond_to_pond` convention (needs a decision: a dbt `sources.yml` entry tagged somehow
  as "this is a Duckstring Pond," vs. a naming convention, vs. an explicit mapping in `pond.toml`).
- The per-model runtime executor branch (`dbtRunner().invoke(["run", "--select", ...])` against the Pond's
  registry) — a new `RippleExecutor` code path alongside the existing Python-function one.
- Generating the `profiles.yml` / `dbt_project.yml` target wiring so dbt points at the right registry file per
  major line without the user hand-maintaining a profile per environment.

## Non-goals (v1)

- Trickle/incremental-Z-set support for dbt models (see above).
- Mixing hand-written `@ripple`s and dbt models in one Pond.
- dbt tests / docs / exposures surfaced anywhere in Duckstring (a dbt project's tests stay a dbt concern; a
  failing test could map to a Ripple failure later, but that's a follow-up, not v1).
- Anything beyond `dbt-duckdb` as the execution adapter — a dbt project targeting a warehouse adapter
  natively is out of scope; the whole model depends on the compiled SQL running against the Pond's own
  DuckDB registry.

## Open questions

- The `source()` → cross-Pond convention (see above) is the one real design decision left; everything else is
  mechanical translation.
- Should per-model retry budgets (`immediate_retries`) default from somewhere dbt-native, or just inherit the
  Pond's normal defaults like any other Ripple? Leaning: no special-casing, same defaults.
- Whether to expose compiled SQL / dbt docs artifacts anywhere in the UI (Run Detail already shows a
  traceback per Ripple — a dbt model failure's compiler error should surface the same way with no new UI).
