"""gh_stars — a Trickle filtering the star / fork signal out of the event stream.

The head of Path B — entirely separate from the push path, sharing only the ``gh_events`` Inlet. Keeps
just ``WatchEvent`` (a GitHub star) and ``ForkEvent`` rows and projects them to the repo they targeted.
An insert-only signal (a star event is never retracted), so it writes to an **append** Trickle — each
run contributes only the new star/fork events as a ``+1`` delta.
"""

from duckstring import ripple


@ripple
def filter_stars(pond):
    (
        pond.trickle("gh_events.gh_event")
        .filter("s0.type IN ('WatchEvent', 'ForkEvent')")
        .select("s0.event_id, s0.repo_name, s0.type AS event_type, s0.created_at")
        .append("star", pk="event_id", fail_on_conflict=False)
    )
