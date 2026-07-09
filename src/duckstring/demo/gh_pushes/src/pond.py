"""gh_pushes — the ``pond.trickle(...)`` builder: push events enriched with the actor dimension.

Path A of the GHArchive demo. Joins the ``gh_events`` stream to the ``gh_actors`` dimension on
``actor_id`` and keeps only ``PushEvent`` rows, projecting each to its repo, pushing actor's login and the
commit count. Each new event flows through the join as a ``+1`` delta; an actor whose login changed
re-labels only their pushes. The output is a merge Trickle for the ``gh_repo_activity`` Outlet.
"""

from duckstring import ripple


@ripple
def enrich(pond):
    (
        pond.trickle("gh_events.gh_event")
        .join(pond.trickle("gh_actors.actor"), on="actor_id")
        .filter("s0.type = 'PushEvent'")
        .select(
            "s0.event_id, s0.repo_name, s1.actor_login, "
            "COALESCE(s0.push_size, 0) AS commits, s0.created_at"
        )
        .merge("push", pk="event_id")
    )
