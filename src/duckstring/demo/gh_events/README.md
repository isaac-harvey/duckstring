# gh_events

Demo Duckstring Pond — an **append Trickle** over the real [GHArchive](https://www.gharchive.org/) public
event stream (`https://data.gharchive.org/YYYY-MM-DD-H.json.gz`, no auth). An hour *is* a delta: each run
advances an hour cursor and appends that hour's GitHub events; the first run backfills a few hours. This is
genuine streamed real data, not emulation.

The shared Inlet of two entirely separate paths:

```
                 ┌─▶ gh_pushes ─▶ gh_repo_activity   (Outlet — Path A)
gh_events ─▶ … ──┤   (⋈ gh_actors)
                 └─▶ gh_stars  ─▶ gh_trending        (Outlet — Path B)
```

**Config** (env): `DUCKSTRING_GHARCHIVE_BASE` (default the public bucket — point at a local directory of
`{hour}.json.gz` files to run offline), `DUCKSTRING_GHARCHIVE_START`, `DUCKSTRING_GHARCHIVE_BACKFILL`.

```bash
duckstring pond deploy
```
