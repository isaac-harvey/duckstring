# tpcds_store_revenue

Demo Duckstring Pond — an **Outlet** aggregating `tpcds_priced` revenue per store. The second, independent
Outlet off `tpcds_priced`: the same enriched sale stream rolled up a different way, on its own cadence.
Two Outlets sharing one upstream builder is the "run one path at a different frequency to another" demo
(see `tpcds_category_revenue`).

```bash
duckstring pond deploy
```
