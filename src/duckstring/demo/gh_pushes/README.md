# gh_pushes

Demo Duckstring Pond — the `pond.trickle(...)` builder for **Path A**: joins the `gh_events` stream to the
`gh_actors` dimension on `actor_id`, keeps only `PushEvent` rows, and projects each to its repo, author and
commit count. Each new event flows through the join as a `+1` delta. The output is a merge Trickle for the
`gh_repo_activity` Outlet.

```bash
duckstring pond deploy
```
