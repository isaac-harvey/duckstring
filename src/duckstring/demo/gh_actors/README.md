# gh_actors

Demo Duckstring Pond — a **merge Trickle** maintaining the actor dimension from the event stream. Reduces
`gh_events` to one row per actor (latest login + avatar, running event count) and merges comprehensively,
so only actors whose details moved reach the changelog. The dimension `gh_pushes` joins against to enrich
a push with its author's current login.

```bash
duckstring pond deploy
```
