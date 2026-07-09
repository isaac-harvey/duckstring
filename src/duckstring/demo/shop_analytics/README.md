# shop_analytics — a dbt-mode Pond

Demo Duckstring Pond that deploys a **dbt project** as a Pond: no `@ripple` code, just dbt models. Each
model becomes a Ripple (freshness / failure / retry tracked), and dbt's `ref()` DAG becomes the intra-Pond
ripple graph. See [`plans/dbt.md`](../../../../plans/dbt.md).

```
sale (from shop_orders)  ──▶  orders_clean  ──▶  revenue_by_product  ──▶  top_products
                              └─ 3 dbt models = 3 Ripples ─────────────────────────┘
```

- `pond.toml` sets `[pond] dbt_project = "dbt"` (instead of `@ripple` code) and declares `shop_orders` as a
  Source. A dbt `source('shop_orders', 'sale')` resolves to that Source Pond's published table —
  Duckstring materialises it into the model's DuckDB registry before the models run.
- The models are plain `+materialized: table` dbt models; dbt owns its own SQL and materialization,
  Duckstring wraps each in a Ripple.

**Requires the dbt extra** to deploy or run:

```bash
pip install 'duckstring[dbt]'
duckstring pond deploy                          # in shop_orders/, then shop_analytics/
duckstring trigger pulse shop_analytics
```
