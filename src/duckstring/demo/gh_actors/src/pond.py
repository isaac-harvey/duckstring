"""gh_actors — a merge Trickle maintaining the actor dimension from the event stream.

Reads the clean current state of ``gh_events.gh_event`` and reduces it to one row per actor (their latest
login + avatar, and a running event count), then merges comprehensively — so only actors whose details or
counts actually moved this run reach the changelog. A dimension derived from the fact stream: the login a
downstream join enriches a push with, kept current as people rename or their activity grows.
"""

from duckstring import ripple


@ripple
def build(pond):
    pond.read_table("gh_events.gh_event")  # registers the Source as the view `gh_event`
    state = pond.con.sql("""
        SELECT
            actor_id,
            arg_max(actor_login, created_at)   AS actor_login,
            arg_max(actor_avatar, created_at)  AS actor_avatar,
            COUNT(*)                           AS event_count
        FROM gh_event
        WHERE actor_id IS NOT NULL
        GROUP BY actor_id
    """)
    pond.merge_table("actor", state, pk="actor_id")
