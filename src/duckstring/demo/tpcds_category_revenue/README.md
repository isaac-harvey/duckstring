# tpcds_category_revenue

Demo Duckstring Pond — an **Outlet** aggregating `tpcds_priced` revenue per product category. Merges
comprehensively, so only the categories whose revenue actually moved reach the changelog.

One of **two independent Outlets** off `tpcds_priced`. Run them at different cadences to demo a single
upstream feeding two paths at different rates — e.g. this one hourly, `tpcds_store_revenue` daily:

```bash
duckstring pond deploy
duckstring trigger tide tpcds_category_revenue 1h
duckstring trigger tide tpcds_store_revenue 1d
```
