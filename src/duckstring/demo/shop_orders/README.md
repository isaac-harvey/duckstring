# shop_orders

Demo Duckstring Pond — a plain `@ripple` inlet that generates a `sale` table. It's the **Source** the
dbt-mode `shop_analytics` Pond reads: a hand-written Pond can sit upstream of a dbt one. Overwrite output,
grows a little each run. Size is env-overridable (`DUCKSTRING_DBT_DEMO_SALES`).

```bash
duckstring pond deploy
```
