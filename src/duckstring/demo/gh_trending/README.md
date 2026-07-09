# gh_trending

Demo Duckstring Pond — the **Outlet** at the end of Path B. Reads `gh_stars` and counts stars vs forks per
repo. The second independent Outlet of the GHArchive demo — run it at a different cadence from
`gh_repo_activity` to demo one Inlet feeding two separate paths at different rates (see
`gh_repo_activity`). Merges comprehensively, so only repos whose counts moved reach the changelog.

```bash
duckstring pond deploy
```
