# tpcds_stores

Demo Duckstring Pond — a **merge Trickle** over the TPC-DS `store` dimension. A small, stable dimension:
generated once, re-emitted as the complete current state each run. Because the roster doesn't drift, after
the first run it diffs to nothing — a free *stable operand* for the downstream join. The honest other half
of the CDC story: not every dimension churns.

```bash
duckstring pond deploy
```
