# tpcds_items

Demo Duckstring Pond — a **merge Trickle** over the TPC-DS `item` dimension. Each run emits the complete
current catalogue; `merge_table` diffs it into a Z-set changelog, so a price that drifts between runs
becomes CDC a downstream Trickle re-prices from. A few prices are perturbed each run
(`DUCKSTRING_TPCDS_PRICE_CHANGES`); everything else re-emits at its deterministic base, so the changelog
stays small however large the catalogue.

```bash
duckstring pond deploy
```
