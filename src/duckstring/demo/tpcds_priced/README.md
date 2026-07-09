# tpcds_priced

Demo Duckstring Pond — the `pond.trickle(...)` builder: a **3-way incremental star join** of the
`tpcds_sales` fact to the `tpcds_items` and `tpcds_stores` dimensions. Each changed Source's Z-set delta
flows through the join to exactly the affected output rows — a new sale, or an item whose price drifted,
re-prices only the sales it touches, never the whole fact. The output is itself a merge Trickle for the
two Outlets to consume.

```bash
duckstring pond deploy
```
