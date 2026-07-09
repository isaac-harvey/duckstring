# gh_repo_activity

Demo Duckstring Pond — the **Outlet** at the end of Path A. Aggregates `gh_pushes` per repository: total
pushes, commits and distinct contributors. Merges comprehensively, so only repos whose activity moved
reach the changelog.

One of **two independent Outlets** off the shared `gh_events` Inlet. Run the two paths at different cadences
to demo one Inlet feeding two entirely separate paths at different rates:

```bash
duckstring pond deploy
duckstring trigger tide gh_repo_activity 15m
duckstring trigger tide gh_trending 1h
```
