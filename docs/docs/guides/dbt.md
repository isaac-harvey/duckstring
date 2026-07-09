---
title: dbt Projects as Ponds
description: Deploy an existing dbt project as a Pond with zero @ripple code — each dbt model becomes a Ripple, tracked for freshness, failure and retry.
---

# dbt Projects as Ponds

If your transforms are already a dbt project, you don't have to rewrite them to put them under Duckstring. A **dbt-mode Pond** deploys the project as-is: `pond.toml` points at the dbt project directory, and Duckstring reads the model graph dbt already computes and re-expresses it as Ripples. Each dbt **model becomes a Ripple** — tracked for [freshness](../concepts/freshness.md), [failure and retry](fault-tolerance.md) — and dbt's `ref()` DAG becomes the intra-Pond ripple graph, the same way `@ripple` edges do for a Python Pond.

dbt keeps owning its own SQL and materialization logic. Duckstring only wraps each model in a per-model freshness/fault-tolerance node and handles cross-Pond wiring. There is no transcription step: you point at the project and deploy.

:::note Requires the dbt extra
dbt-mode needs dbt installed on the Catchment: `pip install 'duckstring[dbt]'` (pulls dbt-core + dbt-duckdb). A Catchment without it can still run every other kind of Pond; deploying a dbt-mode Pond without it fails with a clear message.
:::

## Declaring a dbt-mode Pond

A dbt-mode Pond's `pond.toml` sets `[pond] dbt_project` instead of shipping `@ripple` code:

```toml
[pond]
name = "shop_analytics"
version = "1.0.0"
type = "outlet"
dbt_project = "dbt"          # the dbt project directory, relative to this Pond

[sources]
shop_orders = "1.0.0"        # a dbt source() resolves to this Duckstring Source Pond
```

The two entrypoint kinds are mutually exclusive per Pond — a dbt-mode Pond has no hand-written Ripples. (A Python Pond can still sit upstream or downstream of a dbt-mode one.)

The project itself is an ordinary dbt project. Its profile must be named `duckstring` — Duckstring generates the `profiles.yml` and points that profile at the Pond's own DuckDB registry via the `dbt-duckdb` adapter, so you don't maintain a profile per environment:

```yaml
# dbt/dbt_project.yml
name: 'shop_analytics'
version: '1.0.0'
config-version: 2
profile: 'duckstring'
model-paths: ["models"]

models:
  shop_analytics:
    +materialized: table       # publish tables, not views, so other Ponds can read them
```

Models are just dbt models:

```sql
-- dbt/models/revenue_by_product.sql
select product, count(*) as sales, round(sum(amount), 2) as revenue
from {{ ref('orders_clean') }}
group by product
```

## Reading another Pond's output

A dbt `source()` whose **source name matches a Pond declared in `pond.toml [sources]`** is a cross-Pond dependency. Define it in the dbt project the normal way:

```yaml
# dbt/models/sources.yml
version: 2
sources:
  - name: shop_orders         # matches [sources] shop_orders
    schema: shop_orders
    tables:
      - name: sale
```

```sql
-- dbt/models/orders_clean.sql
select sale_id, product, amount, sale_date
from {{ source('shop_orders', 'sale') }}
where amount > 0
```

Before the models run, Duckstring materialises each such Source into the model's registry as exactly the relation dbt resolves it to (`schema.identifier`), honouring the Source's [major pin](../concepts/versioning.md) and reading its published output the same way any Ripple reads a Source. A dbt source whose name is *not* a declared Duckstring Source is left to dbt's own resolution (an unmanaged warehouse table).

## What you get

Once deployed, a dbt-mode Pond behaves like any other Pond:

- **Triggers and demand** — `pulse`/`wave`/`tide` the Pond; the models run in dependency order.
- **Per-model freshness** — each model is a Ripple with its own run history.
- **Fault tolerance** — a failing model's compiler/runtime error surfaces in [run history](web-ui.md) with its traceback, retry budgets re-run it, and downstream Ponds block on a failed upstream.
- **Cross-Pond versioning** — the Pond pins its Sources' majors in `pond.toml` like any other.

## Try it

```bash
pip install 'duckstring[dbt]'
duckstring pond demo --dbt          # scaffolds shop_orders/ + shop_analytics/
cd shop_orders   && duckstring pond deploy
cd ../shop_analytics && duckstring pond deploy
duckstring trigger pulse shop_analytics
```

The `shop_analytics` Pond deploys a three-model dbt project (`orders_clean → revenue_by_product → top_products`) reading `shop_orders` as a cross-Pond source.

## Scope (v1)

- dbt-mode models are **plain overwrite nodes** — not [Trickle](trickle.md)-native. dbt's own incremental materialization already manages its state; Duckstring doesn't layer Z-set incrementality on top of it. (Its `is_incremental()` models still work — the registry persists across runs — Duckstring just doesn't add incremental machinery.)
- Execution is via the `dbt-duckdb` adapter against the Pond's DuckDB registry. A dbt project targeting a warehouse adapter natively is out of scope.
- No mixing hand-written `@ripple`s and dbt models in one Pond.
