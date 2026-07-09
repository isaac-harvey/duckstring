# tpcds_sales

Demo Duckstring Pond — an **append Trickle** over the TPC-DS `store_sales` fact. The first run generates
the base fact with DuckDB's `tpcds` extension (`dsdgen`, offline + deterministic); every run after that
appends a small synthetic batch of new sales (streaming emulated — TPC-DS has no native stream). The
single history table is at once the full read and the delta source.

The Inlet of the TPC-DS demo chain:

```
tpcds_sales ─┐
tpcds_items ─┼─▶ tpcds_priced ─┬─▶ tpcds_category_revenue   (Outlet)
tpcds_stores ┘                 └─▶ tpcds_store_revenue      (Outlet)
```

Sizes are env-overridable: `DUCKSTRING_TPCDS_SF` (scale factor, default `0.1`), `DUCKSTRING_TPCDS_BATCH`.

Deploy to a Catchment:

```bash
duckstring pond deploy
```
