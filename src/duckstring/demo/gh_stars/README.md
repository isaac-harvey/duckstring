# gh_stars

Demo Duckstring Pond — the head of **Path B**, entirely separate from the push path (they share only the
`gh_events` Inlet). A Trickle that keeps just `WatchEvent` (a GitHub star) and `ForkEvent` rows and projects
them to the repo they targeted. An insert-only signal, so it writes to an **append** Trickle — each run
contributes only the new star/fork events.

```bash
duckstring pond deploy
```
