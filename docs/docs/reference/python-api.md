---
title: Python API
description: The duckstring package — the @ripple decorator and the Pond handle.
---

# Python API Reference

The surface a Pond author touches is intentionally tiny: one decorator to register Ripples, and one handle passed into every Ripple at runtime.

```python
from duckstring import ripple
```

## `@ripple`

Registers a function as a [Ripple](../concepts/ripples.md) of the Pond. The Catchment discovers a Pond's topology at deploy time by importing `src/pond.py` (or the `ripples` path declared in [pond.toml](pond-toml.md)) and reading these registrations.

```python
@ripple
def daily_sales(pond): ...

@ripple(parents=[daily_sales])
def join_lines(pond): ...

@ripple(parents=[daily_sales], name="join_lines_v2")
def join_lines_impl(pond): ...
```

| Parameter | Default | Meaning |
|---|---|---|
| `parents` | `[]` | Ripples (the decorated function objects) that must complete, within the same Pond Run, before this one starts. Ripples with no parent relationship run in parallel. All parent edges are required. |
| `name` | the function's name | The Ripple's registered name — used in topology, run history, and `duckstring get`/`query`. |
| `always_run` | `False` | Run this Pond **every time** it is triggered, even when its Sources are unchanged (so a side effect — a notification, a heartbeat — always fires). By default a Pond whose Sources didn't change is *passed* (completed with no execution); `always_run` opts out of that. ORed up to the Pond: any always-run Ripple makes the whole Pond always run. Pair with `pond.sources_changed()` / `pond.skip()` to still skip the *data* work. |

The decorated function must accept exactly one argument: the `Pond` handle. It returns nothing — output happens through `pond.write_table`. The decorator returns the function unchanged, so parents can reference it directly.

:::note
`src/pond.py` is imported both at deploy time (to read the topology) and at execution time. Keep module level free of side effects — work belongs inside Ripple bodies.
:::

## The `Pond` handle

Each Ripple invocation receives a fresh `Pond` — the runtime handle bound to the Pond's working database.

| Attribute | Type | Meaning |
|---|---|---|
| `pond.name` | `str` | The Pond's name |
| `pond.version` | `str` | The deployed version executing |
| `pond.con` | `duckdb.DuckDBPyConnection` | A connection to the Pond's private working database |
| `pond.f` | `datetime` | The run's [freshness](../concepts/freshness.md) (tz-aware UTC) — the natural watermark/provenance stamp, stable across retries and crash recovery (see [Incremental Ripples](../guides/incremental-ripples.md)) |
| `pond.previous_f` | `datetime` | The previous successfully-completed run's freshness — the lower bound of the bracket `(previous_f, f]` for hand-rolled incremental reads. Equal to the sentinel `NEVER` (far past) on the first run, so that bracket reads everything. Stable across retries/crash recovery, like `pond.f` |
| `pond.root` | `Path` | The Catchment root (rarely needed directly) |

### `pond.read_table(ref)`

Returns a DuckDB **relation** for a table. The reference form decides where it reads from:

```python
own = pond.read_table("daily_sales")                  # this Pond's table — live, from the working DB
src = pond.read_table("transactions.transaction")     # a Source's table — its published Parquet snapshot
```

- **Bare name** — a table this Pond wrote, read live from its working database. This is how intermediate state flows between Ripples in a run, and how a Ripple builds on its own previous output (see [Incremental Ripples](../guides/incremental-ripples.md)).
- **`source_pond.table`** — a Source's *published* output: the Parquet snapshot exported by its last successful run. Reads never touch the Source's live database, so they see only consistent, completed data and never contend with the Source's execution. The table is also registered as a view under its own name, so plain SQL can reference it directly — `FROM "transaction"` after the read above.

Raises `FileNotFoundError` if a Source table has no exported snapshot yet — i.e. the Source hasn't completed a successful run.

### `pond.write_table(name, relation)`

Publishes a relation as a table of this Pond, atomically:

```python
agg = pond.con.sql('SELECT product_id, SUM(quantity) AS qty FROM "transaction" GROUP BY 1')
pond.write_table("daily_sales", agg)
```

The write is build-then-swap: the relation materialises into a temporary table which then replaces the target in one transaction. Readers see the old table or the new one, never anything in between. Concurrent write conflicts (other Ripples writing their own tables to the same database) are retried with backoff automatically — they queue rather than fail.

Each successful Pond Run ends with every table published into the Pond's `data/` directory — via the [data plane](../guides/running-a-catchment.md#the-data-plane) (Iceberg by default, Parquet optional) — and that published copy is what Sinks and [queries](../guides/querying-data.md) consume.

Column names beginning with `_duckstring_` are **reserved** for framework system columns and rejected at publish time — keep your output columns out of that namespace.

### `pond.con` — direct DuckDB

`pond.con` is an ordinary DuckDB connection, with the full SQL and Python-API surface. SQL sees every table this Pond has written, plus a view for each Source table read with `read_table`:

```python
pond.read_table("transactions.transaction")    # registers the view `transaction`
agg = pond.con.sql("""
    SELECT product_id, SUM(quantity) AS total
    FROM "transaction"
    GROUP BY product_id
""")
pond.write_table("totals", agg)
```

Relations are lazy — `pond.con.sql(...)` builds a query plan, and nothing executes until the result is consumed (here, by `write_table`). Chains of relations compose into a single optimised query, and the relation API (`.filter`, `.aggregate`, `.union`, …) composes the same way.

One DuckDB feature to avoid inside Ripples: **replacement scans** — referencing a Python *variable* as a table name in SQL (``FROM raw`` for a local named ``raw``). That resolves by scanning Python stack frames, which is unreliable under the threaded executor Ripples run in. Reference registered names as above, or compose relations with the relation API instead.

Anything that produces a DuckDB relation works as `write_table` input, which is also the bridge for non-SQL transforms:

```python
import pandas as pd

df = pd.DataFrame(fetch_from_api())                    # arbitrary Python
pond.write_table("snapshot", pond.con.from_df(df))
```

### Objects — non-tabular outputs

Not every output is a table. A trained model, a serialised vectoriser, a rendered report — these are **Objects**: named, non-tabular artifacts a Pond publishes alongside its tables, read cross-Pond by the same `source.name` addressing.

```python
import pickle

@ripple
def train(pond):
    model = fit(pond.read_table("features.daily"))
    pond.write_object("model", pickle.dumps(model))       # a single blob
    pond.write_object("checkpoint", "/tmp/run/ckpt")      # or a path — a file OR a directory
```

- **`pond.write_object(name, src)`** — publish under `name`. `src` is raw `bytes`, a file path, or a directory path (published as one unit — e.g. a HuggingFace model directory). Overwrite semantics, like `write_table`; the write is staged during the run and committed atomically when the run publishes, so a later Ripple failure leaves the last-good Object intact.
- **`pond.read_object("source.name")` → `bytes`** — a single-file Object's bytes (own `"name"` or a Source's `"source.name"`). Raises for a directory Object.
- **`pond.object_path("source.name")` → `Path`** — a local path to the Object, valid for a single file *and* a directory. A remote (object-store) Object is materialised locally once; use this for large or directory artifacts you load by path:

```python
@ripple
def score(pond):
    import pickle
    model = pickle.loads(pond.read_object("trainer.model"))          # a blob, straight to bytes
    tokenizer = AutoTokenizer.from_pretrained(pond.object_path("trainer.hf"))  # a directory, by path
```

Objects are overwrite-only and ripple-only (no history/versioning yet). They appear under the **Objects** tab of the [Data Viewer](../guides/web-ui.md), download via `duckstring get-object <pond> <name>`, and can be removed with `duckstring delete-object <pond> <name>` (or the tab's Delete button) — a deleted Object returns only if a Ripple writes it again.

### `pond.sources_changed()` → `bool` and `pond.skip()`

By default Duckstring skips a Pond Run whose Sources are all unchanged since it last ran — it *passes*, advancing freshness with no execution, so downstream skips too (see [Freshness](../concepts/freshness.md#no-change-and-passes)). Most Ponds need nothing here; the skip is automatic and the Ripple never runs.

These two methods are for a Pond that **must** run every time — declared with [`@ripple(always_run=True)`](#ripple) because it carries a side effect — but should still skip the *data* work when nothing upstream changed:

- **`pond.sources_changed()`** returns whether any Source changed its output since this Pond last ran (the engine's verdict). Always `True` in a [local Puddle run](../guides/local-testing.md).
- **`pond.skip()`** marks the current run as producing **no change** — a pass. The Pond's content freshness is held, so downstream Ponds skip. A no-op in a local Puddle run.

```python
@ripple(always_run=True)
def publish(pond):
    notify_dashboard("run started")          # the side effect — every run
    if not pond.sources_changed():
        pond.skip()                          # nothing upstream changed → don't redo the data work
        return
    pond.write_table("report", build_report(pond))
```

An Inlet that ingests from outside can likewise call `pond.skip()` when it finds no new data — every downstream Pond then passes for free.

## `@puddle` and the `Puddle` handle

Registers a function in `src/puddles.py` (or the `puddles` path in [pond.toml](pond-toml.md)) as a [Puddle](../guides/local-testing.md) — a local snapshot of the Source table it names, materialised by `duckstring pond hydrate`:

```python
from duckstring import puddle

@puddle("transactions.transaction")     # one table of a Source
def transactions(p):
    return p.con.sql("SELECT range AS id FROM range(50)")

@puddle("products")                     # a whole Source — name each table
def products(p):
    p.write_table("product", p.con.sql("SELECT 1 AS id"))
```

The handle `p`:

| Attribute | Meaning |
|---|---|
| `p.target` / `p.source` / `p.table` | The target as declared / its Source / its table (`None` for whole-Source puddles). |
| `p.con` | A scratch in-memory DuckDB connection. |
| `p.path` | The destination directory (`puddles/ponds/{source}/data/`) — write any non-table artifact there directly. |
| `p.write_table([name,] relation)` | Export a relation as a table's Parquet snapshot (atomic). Accepts anything `write_table` on a Pond accepts. |
| `p.write_path(path)` | Copy a parquet/csv file or glob in. |
| `p.write_object(name, src)` | Seed a non-tabular [Object](#objects--non-tabular-outputs) for the Source under test (`bytes`, a file, or a directory), so a Pond reading `"{source}.{name}"` resolves it. `p.read_object` / `p.object_path` read it back. |
| `p.catchment(name=None)` | A `Catchment` client bound to the Source: `.get([table])` fetches a table, `.query(sql)` runs SQL against the Source's exported tables, `.tables()` lists them. |

Returning a relation is shorthand for `p.write_table(relation)`; returning a path string for `p.write_path(path)`. Puddle code never runs on a Catchment — only `pond hydrate` imports it.

## Trickle: incremental I/O

A [Trickle](../concepts/trickle.md) is a history-preserving table, not a separate node type — there's no decorator. Inside any `@ripple`, write through `pond.append_table` / `pond.merge_table` instead of `write_table`; consumers read change-sets with `pond.read_delta`. The merge key is declared at the write. The [Incremental Processing guide](../guides/trickle.md) is the worked walkthrough; this is the surface.

### `pond.append_table(name, relation, *, pk=None, fail_on_conflict=True, retain_t=None, retain_n=None)`

Append `relation` to the insert-only history table `name`; each row is stamped with `pond.f`. No diff, no deletes. Idempotent on replay at the same freshness. The history table is both the full read and the delta source. **Returns `bool`** — whether rows were actually appended (an empty relation, or a pure same-`f` replay, is no change), so a Ripple can [`pond.skip()`](#pondsources_changed--bool-and-pondskip) a no-change run.

| Parameter | Default | Meaning |
|---|---|---|
| `pk` | — | Optional. Recorded as the table's declared key (for downstream / the data viewer); also the key the conflict check uses. |
| `fail_on_conflict` | `True` | With `pk` set, assert it is unique across the appended rows and existing history — raising before any write on a violation (the live table is untouched). Pass `False` for the trust-the-writer fast path (no check). A no-op when `pk` is unset. |
| `retain_t` / `retain_n` | `None` | Bound the kept history: a `timedelta` and/or a row count. Off by default. |

### `pond.merge_table(name, relation, *, pk, retain_t=None, retain_n=None)`

Merge the **complete current state** `relation` into the clean current-state **main** table `name`, recording the change as a Z-set in its `__changelog` companion. Duckstring diffs `relation` against the previous main as a full-row Z-set difference to derive inserts/updates/deletes — so it is always safe to hand it the whole state, and there is no way to under-merge. **Returns `bool`** — whether the state actually changed (the diff was non-empty); gate [`pond.skip()`](#pondsources_changed--bool-and-pondskip) on it for a no-change pass. (`pond.apply_zset` returns the same signal.)

| Parameter | Default | Meaning |
|---|---|---|
| `pk` | — | **Required.** The merge identity (a column name or tuple). |
| `retain_t` / `retain_n` | `None` | Bound the kept changelog: a `timedelta` and/or a run count. Off by default (keep everything); a lag SLA, never a correctness gate. |

### `pond.apply_zset(name, zset, *, pk, retain_t=None, retain_n=None)`

The low-level primitive the builder uses: apply a **Z-set** change `zset` (a relation of user columns + the `_duckstring_d` weight) directly to the output Trickle `name`. Reach for it only for hand-rolled incremental compute outside the builder; otherwise use `merge_table` (full state) or the builder.

### `pond.read_delta(ref)` → `Delta`

A Source's change over this run's window `(pond.previous_f, pond.f]`, as a Z-set. Resolves the Source's mode automatically (append history window all `+1`; merge changelog consolidated by full row; an overwrite Ripple → a full read if it advanced, else an empty delta), and falls back to a full read on a first run or a coverage miss.

| Attribute / method | Meaning |
|---|---|
| `delta.zset` | The change as a Z-set — user columns plus `_duckstring_d` (`+1` present, `-1` retraction), a DuckDB relation. |
| `delta.upserts` | Derived: the net present rows (weight `> 0`), user columns only. |
| `delta.deletes` | Derived: the removed primary keys. |
| `delta.is_full` | `True` when this is a **full read** (a bootstrap, a coverage-miss past the source's retained history, or a *changed* overwrite Ripple source) rather than a window — the whole current state at `+1`, to be absorbed comprehensively. The builder does this automatically. |

### The builder — `pond.trickle(spine_ref, *, p=0.3)`

A fluent builder that composes an incremental join from its sources' Z-set deltas and *can't forget an edge* (it sees the whole graph). Chain `.alias(name)` / `.join(pond.trickle(dim), on=…)` and an ordered `.filter(predicate)` / `.mutate(name=expr, …)` / `.select(projection)` pipeline (applied in call order, so a filter may reference an earlier mutate's column), then `.merge(name, *, pk, retain_t=None, retain_n=None)`. `.mutate()` adds computed columns while keeping the rest; `.select()` is optional (with none, the output is the bare `*`, equi-join keys deduplicated). A computed/mutated column may be the `pk` if deterministic, but it can't be a join key. A method exists *only* if the engine maintains it incrementally; everything else goes through `.sql()`.

- **`on`** is a shared column name (or list), or a `{left_col: right_col}` dict — **any** equi-join key (no FK=PK requirement); qualify as `alias.col` if a bare name is ambiguous across sources. **`how`** ∈ `inner` (default) / `left` / `right` / `full` / `semi` / `anti` — **all maintained incrementally**, including the outer joins' NULL-padded incomparables. The join operand may itself be a join DAG, so bushy `(a⋈b)⋈(c⋈d)` and snowflake shapes are expressible (each composed by a binary affected-key recompute).
- **`.merge(name, pk=…)`** — `pk` is **required** (the output identity; must be unique in the output). The output must contain the PK (via `.select`, `.mutate`, or the bare `*`); a deterministic computed column may be the PK.
- The spine is `s0`, dimensions `s1`, `s2`, … in the projection — or name each source with **`.alias(name)`** and reference that (`o.id`, `p."col"`). `s0`/`s1` stay the fallback; aliasing makes the select reorder-safe.
- **`.aggregate(by, **metrics)`** (and the Ibis-shaped **`.group_by(by).aggregate(**metrics)`**) — a grouped aggregate maintained **incrementally**: a merge Trickle keyed by `by` (`pk` defaults to `by`). Metrics are [`duckstring.agg`](#aggregate-metrics) specs — `agg.count/sum/mean/min/max/var/stddev`, the weighted family (`weight_total`/`weighted_sum`/`weighted_average`) and two-variable co-moments (`covariance`/`pearson_correlation`/`ols_slope`/`ols_intercept`), all maintained from the delta alone (raw accumulators in a registry-only companion, only changed groups emitted). Terminal-bound to `.merge()`; `.append`/further joins after it raise (do it downstream). Anything outside this metric set → `.sql()`.
- **`.sql(query)`** — the comprehensive escape hatch for anything outside the incremental op set (non-distributive aggregation, windows, `DISTINCT`, set ops). Name the builder with `.alias()`, then `.sql("… FROM that_name")`. It materialises (no incremental compute, no fast path after it — `.join`/`.select`/`.mutate`/`.filter` raise) but the terminal `.merge()` still diffs → incremental delta out. Accepts a SQL string or, if Ibis is installed, an Ibis expression (compiled lazily). See **`.to_ibis_schema()`** (and **`.schema()`**) → a `{column: type}` dict for `ibis.table(...)`.
- Any table is a valid source (Trickle or overwrite Ripple).
- **`p`** (per source, default `0.3`) is the change-fraction threshold: past that share of a source's rows the builder recomputes comprehensively for that run; `p=1.0` disables the check.
- **`ivm` / `key_filter`** on `.merge()`/`.append()` (both default `True`) are manual strategy escapes — reach for them only when you've measured the default hurts a specific build. `ivm=False` ignores deltas entirely and recomputes the whole output with plain full-table joins, diffed against the stored main (also disables the append spine-PK fast path). `key_filter=False` keeps the incremental delta composition but skips the `IN (…)` pre-filter on each join (joins the full new/old states and diffs) — for when the change is large enough to trip `p` anyway, so the filter buys nothing.
- Bootstrap / coverage-miss / changed-Ripple / over-`p` → comprehensive recompute diffed against the last-written main. The op set is closed — a join operand carrying its own `.filter()`/`.mutate()`/`.select()`/`.aggregate()`/`.sql()`, a missing merge key, an ambiguous join key, or a `*` output with an unresolvable name collision raises at build time.
- **`.append(name, *, pk=None, fail_on_conflict=True, log_drops=True, retain_t=None, retain_n=None)`** is the alternative terminal: write the result to an **append** (insert-only history) Trickle instead of a merge main+changelog — for a *monotonic* transform (output rows only added, never updated/retracted), e.g. enriching an append-only fact stream with stable/SCD dims. A retraction in ΔO, or a `+1` row whose `pk` is in history with a *different* image, is a conflict (identical-image is a benign idempotent skip); `fail_on_conflict=True` raises, `False` drops it (history wins) and — with `log_drops` — records the dropped rows in a `{name}__droplog` companion (published alongside the table like `__changelog`). `pk=None` + `fail_on_conflict=False` skips the checks (fast; sound only when duplicates/past-changes are impossible). **Spine-PK fast path:** when the output PK is a verbatim `s0.<col>` pass-through of the spine's key *and* `fail_on_conflict=False, log_drops=False`, dim deltas can't affect the result (changed facts are dropped-and-forgotten either way), so the builder skips them — computing only `new spine rows ⋈ current dims`. Auto-detected and conservative (falls back to the full, always-correct path otherwise).
- **`.merge(...)` / `.append(...)` return a builder rooted at the table just written**, so joins chain through materialised intermediates in one Ripple — `a.join(b).merge("ab", pk=…).join(c).merge("abc", pk=…)`. Each terminal stores its output's trace, so a later run that changes only `c` reuses the stored `ab` instead of recomputing `a⋈b`. The returned handle is the next **spine** (its in-run delta is threaded forward); a composed builder still can't be a *dimension*.
- **`.was_changed()`** on that returned handle reports whether the `.merge()`/`.append()` actually changed the output (the composed ΔO was non-empty). Gate [`pond.skip()`](#pondsources_changed--bool-and-pondskip) on it to pass a no-change run downstream:
  ```python
  out = pond.trickle("orders.order").join(...).merge("priced", pk="order_id")
  if not out.was_changed():
      pond.skip()
  ```

See the [guide](../guides/trickle.md#the-builder-pondtrickle).

### Aggregate metrics

`duckstring.agg` — the metric specs for `.aggregate(by, **metrics)`. Distributive/algebraic (maintained from the delta alone — min/max rescan a group only on a retraction of the supporting row):

| Spec | Result |
|---|---|
| `agg.count()` | rows in the group (`count(*)`) |
| `agg.sum(col)` | running sum (NULLs ignored; all-NULL group → NULL) |
| `agg.mean(col)` | `sum(col) / count(col)` over non-NULL values |
| `agg.min(col)` / `agg.max(col)` | extreme of `col` (NULLs ignored); inserts extend in place, a retraction of the extreme rescans the group |
| `agg.var(col, how=)` / `agg.stddev(col, how=)` | variance / std-dev over non-NULL values; `how` ∈ `"sample"` (default, Ibis-matching) / `"pop"` |
| `agg.weight_total(w)` | `Σw` over rows where `w` is non-NULL |
| `agg.weighted_sum(x, w)` | `Σ(w·x)` over rows where both are non-NULL |
| `agg.weighted_average(x, w)` | `Σ(w·x) / Σw` (NULL when `Σw` = 0) |
| `agg.covariance(x, y, how=)` | covariance over paired (both-non-NULL) rows; `how` ∈ `"sample"` (default) / `"pop"` |
| `agg.pearson_correlation(x, y)` | Pearson correlation `Cxy / √(M2x·M2y)` |
| `agg.ols_slope(x, y)` / `agg.ols_intercept(x, y)` | least-squares fit of `y` on `x` (slope `Cxy/M2x`, intercept `ȳ − slope·x̄`) |
| `agg.argmin(arg, by)` / `agg.argmax(arg, by)` | the `arg` value at the row minimising / maximising `by` (a retraction of the supporting row rescans the group) |
| `agg.bool_and(col)` / `agg.bool_or(col)` | logical AND / OR over `col` (rescan on retraction) |
| `agg.bit_and(col)` / `agg.bit_or(col)` | bitwise AND / OR over an integer `col` (rescan on retraction) |
| `agg.product(col)` | product of `col` (NULLs ignored; any 0 → 0). Retractable via log-sum-exp — returns a **float**, not bit-exact for large integer products |

`var`/`stddev`/`covariance`/`correlation`/`ols` are maintained as **centred (co-)moments** by a numerically stable parallel merge (never the cancellation-prone `Σx²−(Σx)²/n`), so they stay accurate at any value scale and under retraction.

`agg.reduce(fn, init, *, dtype=)` is a **custom order-dependent reduction** — one value per group, the final fold of the group's rows in `.along` order (`fn(state, row) -> (new_state, output)`, `row` a `{col: value}` dict). It's the reducing counterpart of [`acc.scan`](#order-dependent-scans--alongaccumulate): use `.along(col).aggregate(by, m=agg.reduce(...)).merge(name)`. Retraction-aware (a change anywhere re-folds the group); can't share an `.aggregate()` with the order-independent metrics above.

For anything outside this set (window functions, `DISTINCT`, percentiles), aggregate via `.sql()`.

### Order-dependent scans — `.along(...).accumulate(...)`

`.aggregate(...)` reductions are order-*independent*. For **order-dependent** running values — a value per row, computed in sequence — use **`.along(col).accumulate(by, **metrics)`** with `duckstring.acc` specs, finished by `.append()`:

- **`.along(col)`** declares the **monotonic order axis**: a column that is non-decreasing with freshness (each run's new rows sit at the tail). This is a *precondition*, not a generic sort — a row arriving below its group's high-water mark raises.
- **`.accumulate(by=None, **metrics)`** is a **transform, not a reduction or a terminal**: it enriches *every* row with its running value (output cardinality = input), in `.along` order within each `by` group, then returns a builder you finish with a terminal:
  - **`.append(name, pk=…)`** — append-only; the input must stay monotonic in `.along`. Maintained by a per-group carried fold-state continued from the tail (`O(new rows)`/run).
  - **`.merge(name, pk=…)`** — **retraction-aware**: an edit or out-of-order insert anywhere re-folds the affected group's sequence over its current membership and diffs against the prior main (no monotonic constraint; `O(affected membership)`/run).

The `acc.` prefix is what marks a metric as *accumulated*, so `acc.sum` is a running sum — the order-dependent counterpart of `agg.sum`.

| Spec | Result (running, in `.along` order, per `by` group) |
|---|---|
| `acc.sum(col)` | running sum |
| `acc.count()` | running row count (1, 2, 3, …) |
| `acc.min(col)` / `acc.max(col)` | running extreme so far |
| `acc.first(col)` | first non-NULL value in the group (frozen once set) |
| `acc.product(col)` | running product (float; a 0 makes it stay 0) |
| `acc.prev(col)` / `acc.lag(col, n)` | the value `n` rows back (NULL until `n` prior rows); a length-`n` FIFO buffer, so it reaches across run boundaries |
| `acc.convolution(col, kernel)` | 1-D FIR filter — dot product of `kernel` with the last `len(kernel)` values (NULL until filled; NULL inputs → 0) |
| `acc.ema(col, alpha)` | discrete EMA `α·x + (1−α)·ema_prev` (`0 < α ≤ 1`) |
| `acc.tema(col, lam)` | time-decayed EMA `α_t = 1 − exp(−lam·Δt)`, Δt the gap in the (numeric) `.along` value |
| `acc.scan(fn, init, dtype=)` | custom fold `fn(state, row) -> (new_state, output)`; `row` is a `{col: value}` dict, `state` is carried (persisted as JSON between runs), `output` (type `dtype`) is appended |

```python
from duckstring import acc
(pond.trickle("orders.order_line")
     .along("event_time")
     .accumulate(by="product_id",
                 run_total=acc.sum("qty"),
                 smoothed=acc.ema("unit_price", 0.3))
     .append("order_line_scored", pk="order_id"))
```

## Execution environment

Facts about how Ripple code runs, occasionally relevant when writing it:

- **Threads, one process.** A Pond's Ripples execute in a thread pool inside the Pond's worker process. Independent Ripples genuinely overlap (DuckDB releases the GIL for query work); module-level mutable state in `pond.py` is shared and best avoided.
- **One database per Pond.** All of a Pond's Ripples share one working database; each invocation gets its own connection to it. Cross-Pond access is only ever via `read_table("source.table")` — Parquet snapshots, not the Source's database.
- **Failures are exceptions.** A Ripple fails by raising. The exception's message and traceback are captured into [run history](../guides/fault-tolerance.md), and the [immediate-retry budget](../guides/fault-tolerance.md#the-two-retry-budgets) governs re-attempts. Write Ripples idempotently — a retry re-runs the whole function. `write_table`'s replace semantics make the common derive-and-replace case idempotent by construction, and the same atomicity makes self-read appends replay-safe when the increment is computed from the previous state (see [Incremental Ripples](../guides/incremental-ripples.md)). What needs care is anything with *external* side effects — a retry repeats them (see [External Pipelines](../guides/external-pipelines.md) for the ensure-then-poll shape).
